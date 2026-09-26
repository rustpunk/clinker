//! The two-direction memory-pressure check every retained-state consumer's
//! tests share.
//!
//! Item 6 of the Memory budget checklist in `docs/ai/32_NODE_OBLIGATIONS.md`
//! asks for two runs of the same input: one under a limit smaller than the
//! state, which must spill that state and still produce the same output, and
//! one with ample memory, which must write nothing to disk while the state's
//! bytes are charged. [`assert_arbitrated`] checks both runs at once.
//!
//! Its first check is the reason it exists. A pressure test whose state fits
//! under its own low limit proves nothing about spilling, and the cheapest
//! way to make a failing pressure test pass is to shrink its input until the
//! state fits. The helper refuses that pair outright: the node's own charged
//! peak in the ample run must exceed the low run's limit. Shrinking a test's
//! input, raising its limit or relaxing an assertion to get past this check
//! is a stop, not a deviation.
//!
//! Every figure is the node's own. [`PressureRun::from_report`] reads the
//! node's charged high-water mark (`per_node_peak_charged_bytes`, never the
//! run-wide sampled sum) and the bytes the node wrote to spill files
//! (`per_stage_spill_bytes_written`, which unlinking a run does not lower),
//! and refuses a limit that differs from the one the compiled plan runs
//! under.
//!
//! Include it with `#[path = "common/memory_pressure.rs"] mod memory_pressure;`.

use clinker_exec::executor::ExecutionReport;
use clinker_plan::plan::CompiledPlan;

/// The figures [`assert_arbitrated`] compares from one run of a pressure
/// test.
#[derive(Debug, Clone)]
pub struct PressureRun {
    /// The memory limit the run was given, in bytes.
    pub limit_bytes: u64,
    /// The highest number of bytes any one of the node's own memory
    /// consumers held charged, or `None` when the run attributes no charged
    /// state to the node (its state is not registered with the arbitrator
    /// under its name).
    pub peak_charged_bytes: Option<u64>,
    /// Bytes the node under test wrote to spill files over the run,
    /// including runs it later merged and deleted.
    pub node_spill_bytes: u64,
    /// Bytes every stage of the run wrote to spill files, including runs
    /// later deleted.
    pub total_spill_bytes: u64,
    /// The run's output, with any run-variant columns (timestamps, ids)
    /// already masked by the caller so equal data compares equal.
    pub output: Vec<u8>,
}

/// The memory limit `plan` runs under, in bytes: its `memory.limit` read by
/// the same parser the executor uses to size the run's arbitrator.
#[allow(dead_code)] // Read only through `PressureRun::from_report`, which a target may not call.
pub fn effective_limit_bytes(plan: &CompiledPlan) -> u64 {
    clinker_plan::config::utils::parse_memory_limit_bytes(
        plan.config().pipeline.memory.limit.as_deref(),
    )
    .expect("a compiled plan's memory.limit parses")
}

impl PressureRun {
    /// Read the figures of a finished run of `plan` from its report.
    ///
    /// `node` is the name of the node whose state is under test; its spill
    /// bytes are `0` when the report attributes no spill to it, and its peak
    /// is `None` when the report attributes no charged state to it.
    /// `limit_bytes` is the limit the caller believes the run was given; it
    /// must equal the plan's effective limit, so a test cannot claim one
    /// limit while running another. `output` is the run's masked output.
    ///
    /// Panics when `limit_bytes` differs from the plan's effective limit.
    #[allow(dead_code)] // A target that builds its runs by hand does not read a report.
    pub fn from_report(
        report: &ExecutionReport,
        plan: &CompiledPlan,
        node: &str,
        limit_bytes: u64,
        output: Vec<u8>,
    ) -> Self {
        let effective = effective_limit_bytes(plan);
        assert_eq!(
            limit_bytes, effective,
            "`{node}`: the caller's limit ({limit_bytes} bytes) disagrees with the plan's \
             effective limit ({effective} bytes); state the limit the pipeline's memory.limit \
             sets"
        );
        Self {
            limit_bytes,
            peak_charged_bytes: report.per_node_peak_charged_bytes.get(node).copied(),
            node_spill_bytes: report
                .per_stage_spill_bytes_written
                .get(node)
                .copied()
                .unwrap_or(0),
            total_spill_bytes: report.per_stage_spill_bytes_written.values().sum(),
            output,
        }
    }
}

/// Assert that `node`'s state was arbitrated in both directions: under the
/// `low` limit it spilled and still produced the ample run's output, and under
/// `ample` memory nothing reached disk.
///
/// Panics, naming `node` and the figures, when:
///
/// 1. either run attributes no charged state to `node`;
/// 2. the node's charged peak in the ample run does not exceed the low run's
///    limit (the state fits under the low limit, so the pair proves nothing);
/// 3. the ample run spilled anything;
/// 4. the low run did not spill `node`;
/// 5. the node's charged peak in the low run is not below the ample run's (it
///    held the whole state rather than spilling part of it);
/// 6. the two outputs differ.
#[allow(dead_code)] // A target that only reads reports may not assert a pair.
pub fn assert_arbitrated(node: &str, low: &PressureRun, ample: &PressureRun) {
    let (Some(low_peak), Some(ample_peak)) = (low.peak_charged_bytes, ample.peak_charged_bytes)
    else {
        panic!(
            "`{node}`: no charged state is attributed to `{node}` (low run {:?}, ample run \
             {:?}); register its state with register_node_consumer under the node's name",
            low.peak_charged_bytes, ample.peak_charged_bytes
        );
    };
    assert!(
        ample_peak > low.limit_bytes,
        "`{node}`: state fits under the low limit: a pressure test must hold more than its \
         low limit (ample peak charged {ample_peak} bytes, low limit {} bytes); do not shrink \
         the input or raise the limit to pass",
        low.limit_bytes
    );
    assert!(
        ample.total_spill_bytes == 0,
        "`{node}`: ample memory spilled {} bytes; the ample run must write nothing to disk",
        ample.total_spill_bytes
    );
    assert!(
        low.node_spill_bytes > 0,
        "`{node}`: the low run did not spill `{node}` (limit {} bytes, run spilled {} bytes \
         in total)",
        low.limit_bytes,
        low.total_spill_bytes
    );
    assert!(
        low_peak < ample_peak,
        "`{node}`: the low run held the whole state (low peak charged {low_peak} bytes, ample \
         peak charged {ample_peak} bytes)"
    );
    assert!(
        low.output == ample.output,
        "`{node}`: output differs across limits ({} bytes under the low limit, {} bytes with \
         ample memory)",
        low.output.len(),
        ample.output.len()
    );
}
