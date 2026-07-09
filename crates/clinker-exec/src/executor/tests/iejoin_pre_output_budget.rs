//! Hard-limit coverage for the IEJoin pre-output working-set budget gate.
//!
//! IEJoin has no spill path, so its pre-output working set — the routed
//! partition index vectors, the per-record range-key slots, and the
//! per-group L1/L2 sort arrays / permutation vectors / bit array — is what
//! the memory budget has to bound. The kernel estimates that footprint and
//! polls [`MemoryArbitrator::should_abort_local`] at partition-build
//! completion and again per equality group before the range walk. When the
//! estimate exceeds the hard limit the run aborts with a typed
//! `MemoryBudgetExceeded { source: Arena, detail: "iejoin pre-output ..." }`
//! rather than spilling.
//!
//! The gate is deliberately RSS-independent: `should_abort_local`
//! short-circuits on `local_bytes > limit` before it ever consults process
//! RSS, so a doomed band join aborts even on a host where `rss_bytes()`
//! returns `None`. This test proves that property WITHOUT a
//! `rss_bytes().is_none()` skip guard: the arbitrator runs `NoOpPolicy`
//! (never selects a spill / pause victim) and is NOT seeded with any
//! `peak_rss`, so the only signal that can fire is the local-estimate arm.
//! A hard limit well below the pre-output estimate for a 500×500 pure-range
//! join makes that arm trip at the first (partition-build) poll, before any
//! output record is materialized.
//!
//! The assertion destructures the typed variant and checks the node name
//! and the pre-output detail so a regression that trips on a different
//! surface (the output-buffer poll, a generic node-buffer charge) fails
//! loudly rather than passing on the wrong abort.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::PlanNode;
use std::collections::HashMap;
use std::sync::Arc;

/// Hard limit far below the pre-output estimate for the 500×500 pure-range
/// join below (partition state alone is ~32 KB), so the local-estimate arm
/// of `should_abort_local` trips at the first poll. Small enough that the
/// estimate clears it by a wide margin; the point is the local arm, not a
/// fine-tuned threshold.
const TIGHT_LIMIT: u64 = 8 * 1024;
const SPILL_FRAC: f64 = 0.80;

/// Arbitrator with a tight hard limit and `NoOpPolicy` so no victim is ever
/// paused or asked to spill — the only response to pressure is the kernel's
/// own pre-output abort. `peak_rss` is left unseeded on purpose: the test
/// must pass on RSS-blind hosts, where the sole trip signal is the local
/// byte estimate.
fn tight_no_op_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        TIGHT_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    ))
}

/// Pure-range predicate (two range conjuncts, no equality) so the planner
/// selects `CombineStrategy::IEJoin` and the runtime runs one virtual
/// partition — a single group whose pre-output state scales with the full
/// input, which is what the pre-output gate has to bound.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: iejoin_pre_output_budget
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: band_id, type: string }
      - { name: lo, type: int }
      - { name: hi, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.amount >= bands.lo and orders.amount < bands.hi"
    match: first
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit amount = orders.amount
      emit band_id = bands.band_id
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

/// Compile `PIPELINE_YAML` and return the strategy stamped on the `banded`
/// combine node — the guard that keeps the test honest about exercising the
/// IEJoin branch.
fn banded_strategy() -> CombineStrategy {
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let validated = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile pipeline");
    let dag = validated.dag();
    for idx in dag.graph.node_indices() {
        if let PlanNode::Combine { name, strategy, .. } = &dag.graph[idx]
            && name == "banded"
        {
            return strategy.clone();
        }
    }
    panic!("combine node \"banded\" not present in compiled plan");
}

#[test]
fn iejoin_pre_output_state_aborts_under_tight_budget_without_rss() {
    assert!(
        matches!(banded_strategy(), CombineStrategy::IEJoin),
        "the pure-range predicate must select IEJoin for this test to exercise \
         the IEJoin pre-output gate"
    );

    // 500 orders × 500 disjoint bands: every order carries a valid integer
    // range key, so all 1000 records route into the single pure-range
    // partition. The partition-fill estimate (index vectors + range-key
    // slots) alone is ~32 KB — four times the 8 KB budget — so the
    // partition-build poll aborts before the range walk allocates the L1/L2
    // sort arrays and before any output row is built.
    let mut orders = String::from("order_id,amount\n");
    for i in 0..500 {
        // Amounts spread across the band range so this is a realistic
        // low-output join (each order falls in ~one band), not a
        // pathological all-pairs blowup — the abort comes from the O(n)
        // pre-output state, not from output cardinality.
        orders.push_str(&format!("o{i},{}\n", i * 2));
    }
    let mut bands = String::from("band_id,lo,hi\n");
    for i in 0..500 {
        bands.push_str(&format!("b{i},{},{}\n", i * 2, i * 2 + 2));
    }

    let readers: crate::executor::SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            crate::executor::single_file_reader(
                "orders.csv",
                Box::new(std::io::Cursor::new(orders.into_bytes())),
            ),
        ),
        (
            "bands".to_string(),
            crate::executor::single_file_reader(
                "bands.csv",
                Box::new(std::io::Cursor::new(bands.into_bytes())),
            ),
        ),
    ]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let params = PipelineRunParams {
        execution_id: "iejoin-pre-output-budget".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let arb = tight_no_op_arbitrator();
    let err = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        Arc::clone(&arb),
    )
    .expect_err("IEJoin pre-output state over the tight budget must abort the run");

    match err {
        PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            source,
            detail,
        } => {
            assert_eq!(
                node, "banded",
                "the abort must name the IEJoin combine node"
            );
            assert_eq!(
                source,
                clinker_plan::BudgetCategory::Arena,
                "IEJoin pre-output state is arena-class memory"
            );
            assert_eq!(
                limit, TIGHT_LIMIT,
                "the reported limit must be the hard budget"
            );
            assert!(
                used > TIGHT_LIMIT,
                "the reported pre-output estimate ({used}) must exceed the budget \
                 ({TIGHT_LIMIT}) — that overflow is what tripped the local arm"
            );
            let detail = detail.expect("the pre-output abort must carry a detail string");
            assert!(
                detail.contains("iejoin pre-output"),
                "the abort must come from the IEJoin pre-output gate, not the \
                 output-buffer poll or a generic charge; got detail: {detail:?}"
            );
        }
        other => {
            panic!("expected MemoryBudgetExceeded from the IEJoin pre-output gate; got: {other:?}")
        }
    }

    // No output row was produced — the abort fired before output
    // materialization.
    assert!(
        out.as_string().lines().filter(|l| !l.is_empty()).count() <= 1,
        "the run aborted before the IEJoin emitted any matched row, so the CSV \
         sink holds at most a header line; got: {:?}",
        out.as_string()
    );

    // Error-path registry hygiene: the IEJoin branch's consumer is
    // unregistered on the abort exit, so nothing lingers in the arbitrator.
    assert_eq!(
        arb.consumer_count(),
        0,
        "the IEJoin consumer must be unregistered even when the branch aborts"
    );
}
