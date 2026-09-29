//! The E310 report a refused memory request renders, built from a real
//! ledger: its figures come from one snapshot, it names memory in the words a
//! pipeline author uses, and the limit it suggests would have granted the
//! request.
//!
//! Every refusal here is made off the run's walk, where a request is checked
//! once and never spills, so each report says no reclaim was attempted. The
//! rendering of a round's record is covered beside the walk's reclaim loop.

use clinker_exec::pipeline::memory::ledger::{Grant, Requester, Shortfall};
use clinker_exec::pipeline::memory::{
    ConsumerHandle, ConsumerId, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
};
use clinker_plan::runtime_error::{
    ConsumerLabel, HolderState, MemoryShortfallReport, MemorySurface, suggested_limit_text,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// Words for the engine's own machinery, which the E310 text must never use.
const ENGINE_WORDS: [&str; 5] = ["arena", "node_buffer", "arbitrator", "consumer", "ledger"];

/// A holder whose charge is its handle's bytes. `spillable` decides whether a
/// spill could free them; `pausable` makes it a Source-like holder whose
/// only relief is pausing.
struct Holder {
    handle: Arc<ConsumerHandle>,
    spillable: bool,
    pausable: bool,
    paused: AtomicBool,
}

impl MemoryConsumer for Holder {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }
    fn reclaimable_bytes(&self) -> u64 {
        if self.spillable && !self.pausable {
            self.handle.bytes()
        } else {
            0
        }
    }
    fn spill_priority(&self) -> i32 {
        0
    }
    fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
        Ok(0)
    }
    fn can_back_pressure(&self) -> bool {
        self.pausable
    }
    fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }
    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }
}

fn run(limit: u64) -> MemoryArbitrator {
    MemoryArbitrator::with_policy(limit, 0.80, 0.70, MemoryArbitrator::default_policy())
}

enum Kind {
    Spillable,
    CannotSpill,
    PausedSource,
}

/// Register `node`'s `surface` holding `bytes` on its handle.
fn hold(
    arbitrator: &MemoryArbitrator,
    node: &str,
    surface: MemorySurface,
    bytes: u64,
    kind: Kind,
) -> (ConsumerId, Arc<ConsumerHandle>) {
    let handle = ConsumerHandle::new();
    let holder = Arc::new(Holder {
        handle: Arc::clone(&handle),
        spillable: matches!(kind, Kind::Spillable),
        pausable: matches!(kind, Kind::PausedSource),
        paused: AtomicBool::new(false),
    });
    if matches!(kind, Kind::PausedSource) {
        holder.pause();
    }
    let id = arbitrator.register_node_consumer(
        holder,
        Arc::clone(&handle),
        ConsumerLabel {
            node: node.to_string(),
            surface,
        },
    );
    handle.set_bytes(bytes);
    (id, handle)
}

fn refuse(arbitrator: &MemoryArbitrator, bytes: u64, requester: Requester) -> Shortfall {
    arbitrator
        .reserve(bytes, requester)
        .expect_err("the request must not fit")
}

fn round_up_to_mebibyte(bytes: u64) -> u64 {
    bytes.div_ceil(MIB) * MIB
}

fn assert_rows_sum_to_charged(report: &MemoryShortfallReport) {
    let listed: u64 = report.holders.iter().map(|holder| holder.bytes).sum();
    assert_eq!(
        listed + report.other_holders_bytes + report.unattributed_bytes,
        report.charged_bytes,
        "the holder rows, the +N more line and the memory no single node holds must add up \
         to the charged figure: {report:#?}"
    );
}

/// The pipeline the rendered example describes: a join's build side that
/// cannot spill, an Aggregate asking for more group state, a paused Source,
/// buffered rows, decision state, and memory the run holds as a whole.
struct Example {
    arbitrator: MemoryArbitrator,
    totals: ConsumerId,
    _handles: Vec<Arc<ConsumerHandle>>,
    _grants: Vec<Grant>,
}

fn example() -> Example {
    let arbitrator = run(8 * MIB);
    let (enrich, enrich_handle) = hold(
        &arbitrator,
        "enrich",
        MemorySurface::JoinBuildSide,
        3 * MIB,
        Kind::CannotSpill,
    );
    let (totals, totals_handle) = hold(
        &arbitrator,
        "totals",
        MemorySurface::GroupState,
        MIB,
        Kind::Spillable,
    );
    let (_, sorted) = hold(
        &arbitrator,
        "sorted",
        MemorySurface::BufferedRows {
            from: "orders".to_string(),
            to: "sorted".to_string(),
        },
        MIB,
        Kind::Spillable,
    );
    let (_, orders) = hold(
        &arbitrator,
        "orders",
        MemorySurface::RowsRead,
        768 * KIB,
        Kind::PausedSource,
    );
    let (_, dedupe) = hold(
        &arbitrator,
        "dedupe",
        MemorySurface::DecisionState,
        256 * KIB,
        Kind::CannotSpill,
    );
    let (_, audit) = hold(
        &arbitrator,
        "audit",
        MemorySurface::HeldFailingRows,
        128 * KIB,
        Kind::Spillable,
    );
    let (_, widen) = hold(
        &arbitrator,
        "widen",
        MemorySurface::ScanMaterialization,
        64 * KIB,
        Kind::CannotSpill,
    );
    // The join's build side also holds memory granted in its name, and the
    // run holds some output staging no single node owns.
    let in_its_name = arbitrator
        .reserve(512 * KIB, Requester::for_consumer(enrich))
        .expect("fits");
    let run_wide = arbitrator
        .reserve(640 * KIB, Requester::governed())
        .expect("fits");
    Example {
        arbitrator,
        totals,
        _handles: vec![
            enrich_handle,
            totals_handle,
            sorted,
            orders,
            dedupe,
            audit,
            widen,
        ],
        _grants: vec![in_its_name, run_wide],
    }
}

/// The example's refusal: `totals` asks for 2 MiB more group state.
fn example_report(example: &Example) -> Box<MemoryShortfallReport> {
    let mut report = refuse(
        &example.arbitrator,
        2 * MIB,
        Requester::for_consumer(example.totals),
    )
    .into_report(&example.arbitrator);
    // The process's private memory is whatever this test process holds; pin
    // it so the rendering is reproducible.
    assert!(
        report.private_bytes.is_some()
            || !cfg!(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "windows"
            )),
        "supported platforms read private memory"
    );
    report.private_bytes = Some(200 * MIB);
    report
}

#[test]
fn report_figures_come_from_one_snapshot() {
    let arbitrator = Arc::new(run(8 * MIB));
    let (join, _join_handle) = hold(
        &arbitrator,
        "enrich",
        MemorySurface::JoinBuildSide,
        2 * MIB,
        Kind::CannotSpill,
    );
    let _in_its_name = arbitrator
        .reserve(512 * KIB, Requester::for_consumer(join))
        .expect("fits");
    let _small: Vec<_> = (0..6)
        .map(|index| {
            hold(
                &arbitrator,
                &format!("step{index}"),
                MemorySurface::BufferedRows {
                    from: format!("step{index}"),
                    to: "next".to_string(),
                },
                64 * KIB,
                Kind::Spillable,
            )
        })
        .collect();
    let _run_wide = arbitrator
        .reserve(256 * KIB, Requester::governed())
        .expect("fits");
    let (_, churn_handle) = hold(
        &arbitrator,
        "churn",
        MemorySurface::GroupState,
        0,
        Kind::Spillable,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let churn = {
        let arbitrator = Arc::clone(&arbitrator);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut rounds = 0u64;
            while !stop.load(Ordering::Relaxed) {
                churn_handle.set_bytes(if rounds % 2 == 0 { 128 * KIB } else { 0 });
                let grant = arbitrator.reserve(32 * KIB, Requester::governed());
                drop(grant);
                rounds += 1;
            }
            rounds
        })
    };

    let mut distinct_charged = std::collections::BTreeSet::new();
    for _ in 0..500 {
        let requested = 5 * MIB;
        let report = refuse(&arbitrator, requested, Requester::governed()).into_report(&arbitrator);
        assert_rows_sum_to_charged(&report);
        assert_eq!(
            report.suggested_limit_bytes,
            round_up_to_mebibyte(report.charged_bytes + requested)
        );
        assert!(report.suggested_limit_bytes >= report.charged_bytes + requested);
        assert_eq!(report.limit_bytes, 8 * MIB);
        assert_eq!(report.requested_bytes, requested);
        assert!(report.reclaim.is_none(), "no round runs off the walk");
        let enrich = report
            .holders
            .iter()
            .find(|holder| holder.node == "enrich")
            .expect("the largest holder is listed");
        assert_eq!(
            enrich.bytes,
            2 * MIB + 512 * KIB,
            "a holder's figure is its own charge plus the memory granted in its name"
        );
        assert_eq!(enrich.state, HolderState::CannotSpill);
        assert!(report.holders.len() <= MemoryShortfallReport::LISTED_HOLDERS);
        assert!(report.other_holders_count >= 2);
        distinct_charged.insert(report.charged_bytes);
    }
    stop.store(true, Ordering::Relaxed);
    let rounds = churn.join().expect("the churning thread");
    assert!(
        rounds > 0,
        "the other thread charged and released meanwhile"
    );
    // Not a requirement of the report, only evidence the race was exercised
    // when the scheduler interleaved the threads.
    eprintln!("distinct charged totals seen: {}", distinct_charged.len());
}

#[test]
fn report_renders_memory_no_single_node_holds() {
    let example = example();
    let report = example_report(&example);
    assert_eq!(report.unattributed_bytes, 640 * KIB);
    assert_rows_sum_to_charged(&report);
    let text = report.to_string();
    let remainder = text
        .find("\n    not held by any one node  640.0 KiB")
        .unwrap_or_else(|| panic!("the remainder line is rendered:\n{text}"));
    let last_row = text
        .find("+2 more holders")
        .unwrap_or_else(|| panic!("the +N more line is rendered:\n{text}"));
    assert!(
        remainder > last_row,
        "the remainder follows the holder rows"
    );

    // With nothing held outside a node, there is no such line.
    let arbitrator = run(4 * MIB);
    let (join, _handle) = hold(
        &arbitrator,
        "enrich",
        MemorySurface::JoinBuildSide,
        3 * MIB,
        Kind::CannotSpill,
    );
    let report =
        refuse(&arbitrator, 2 * MIB, Requester::for_consumer(join)).into_report(&arbitrator);
    assert_eq!(report.unattributed_bytes, 0);
    assert_rows_sum_to_charged(&report);
    let text = report.to_string();
    assert!(
        !text.contains("not held by any one node"),
        "no remainder line without a remainder:\n{text}"
    );
}

#[test]
fn suggested_limit_rounds_up_to_whole_mebibytes() {
    assert_eq!(suggested_limit_text(3 * MIB + 1), "4M");
    assert_eq!(suggested_limit_text(3 * MIB), "3M");
    assert_eq!(suggested_limit_text(1024 * MIB), "1G");
    assert_eq!(suggested_limit_text(1024 * MIB + 1), "1025M");

    // A report asking for one byte beside exactly 3 MiB charged suggests 4M,
    // in both spellings.
    let arbitrator = run(3 * MIB);
    let (join, _handle) = hold(
        &arbitrator,
        "enrich",
        MemorySurface::JoinBuildSide,
        3 * MIB,
        Kind::CannotSpill,
    );
    let report = refuse(&arbitrator, 1, Requester::for_consumer(join)).into_report(&arbitrator);
    assert_eq!(report.charged_bytes, 3 * MIB);
    assert_eq!(report.suggested_limit_bytes, 4 * MIB);
    let text = report.to_string();
    assert!(text.contains("memory: { limit: \"4M\" }"), "{text}");
    assert!(text.contains("or: --memory-limit 4M"), "{text}");
    assert!(
        text.contains("fix: raise the limit to at least 4M — this request needed 3.0 MiB"),
        "{text}"
    );
}

#[test]
fn oversized_request_says_spilling_cannot_help() {
    // Larger than the whole limit on its own.
    let arbitrator = run(4 * MIB);
    let (join, _handle) = hold(
        &arbitrator,
        "big_join",
        MemorySurface::JoinBuildSide,
        0,
        Kind::Spillable,
    );
    let report =
        refuse(&arbitrator, 5 * MIB, Requester::for_consumer(join)).into_report(&arbitrator);
    assert!(report.oversized);
    let text = report.to_string();
    assert!(
        text.starts_with(
            "E310 big_join: one request for join build side needs 5.0 MiB, more than \
             memory.limit 4.0 MiB can hold — spilling cannot help\n"
        ),
        "{text}"
    );
    assert!(text.contains("memory: { limit: \"5M\" }"), "{text}");

    // Within the limit, but not beside the state that cannot spill.
    let arbitrator = run(4 * MIB);
    let (_, _held) = hold(
        &arbitrator,
        "enrich",
        MemorySurface::JoinBuildSide,
        3 * MIB,
        Kind::CannotSpill,
    );
    let (totals, _totals_handle) = hold(
        &arbitrator,
        "totals",
        MemorySurface::GroupState,
        0,
        Kind::Spillable,
    );
    let report =
        refuse(&arbitrator, 2 * MIB, Requester::for_consumer(totals)).into_report(&arbitrator);
    assert!(report.oversized);
    assert_eq!(report.unspillable_bytes, 3 * MIB);
    let text = report.to_string();
    assert!(
        text.starts_with(
            "E310 totals: one request for group state needs 2.0 MiB, more than memory.limit \
             4.0 MiB can hold beside 3.0 MiB of state that cannot spill — spilling cannot help\n"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "\n  spilling cannot help: the state that fills the limit cannot be written to disk"
        ),
        "every holder is unspillable:\n{text}"
    );
    assert!(
        text.contains(
            "\n  remedy: enrich's join build side holds 3.0 MiB and cannot be spilled; \
             see \"Join build side\" in clinker explain --code E310"
        ),
        "{text}"
    );

    // A request that spilling could make room for keeps the ordinary headline.
    let example = example();
    let report = example_report(&example);
    assert!(!report.oversized);
    assert!(
        report.to_string().starts_with(
            "E310 totals: needed 2.0 MiB more for group state, but memory.limit 8.0 MiB is \
             fully held and nothing more could be spilled\n"
        ),
        "{report}"
    );
}

#[test]
fn report_text_uses_author_vocabulary() {
    let example = example();
    let report = example_report(&example);
    assert_rows_sum_to_charged(&report);
    let states: Vec<(&str, HolderState)> = report
        .holders
        .iter()
        .map(|holder| (holder.node.as_str(), holder.state))
        .collect();
    assert_eq!(
        states,
        vec![
            ("enrich", HolderState::CannotSpill),
            ("totals", HolderState::Requester),
            ("sorted", HolderState::InUse),
            ("orders", HolderState::PausedSource),
            ("dedupe", HolderState::CannotSpill),
        ]
    );
    let text = report.to_string();
    let lowered = text.to_lowercase();
    for word in ENGINE_WORDS {
        assert!(
            !lowered.contains(word),
            "the E310 text must not say {word:?}:\n{text}"
        );
    }
    insta::assert_snapshot!("e310_report", text);
}
