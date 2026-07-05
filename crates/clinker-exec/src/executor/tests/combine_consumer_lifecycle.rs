//! Combine branch consumer lifecycle.
//!
//! The inline `HashBuildProbe`, IEJoin, GraceHash, and SortMerge combine
//! branches each register a `MemoryConsumer` with the pipeline-scoped
//! arbitrator before running their kernel. Each branch must unregister that
//! consumer when it exits — on the clean path and on every `?` early-return
//! between registration and the arm's return — or the wrapper lingers in the
//! arbitrator's registry for the rest of the run, growing the
//! victim-selection surface and (once the handle carries live bytes)
//! charging finished join state into `sum_consumer_usage`.
//!
//! Each test forces one strategy through the planner (a two-range
//! predicate selects IEJoin, a single-range presorted predicate selects
//! SortMerge, a `strategy: grace_hash` hint on a pure-equi predicate
//! selects GraceHash, a pure-equi predicate with no hint selects the inline
//! HashBuildProbe branch), asserts the compiled node actually carries that
//! strategy so the run is not vacuous, then runs with an injected
//! arbitrator and asserts the registry is empty afterward. The error-exit
//! tests drive a branch to a hard error (`on_miss: error` with an unmatched
//! driver row) and assert the registry is still empty — proving the
//! unregister funnels through the error exit too.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_plan::plan::combine::CombineStrategy;
use clinker_plan::plan::execution::PlanNode;
use std::collections::HashMap;
use std::sync::Arc;

/// Generous hard limit so the spill / abort gates never trip: these tests
/// observe registry hygiene, not pressure behavior. GraceHash is forced by
/// the `strategy: grace_hash` hint, not by budget pressure, so a huge
/// budget does not suppress it.
const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

fn quiet_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        crate::pipeline::memory::MemoryArbitrator::default_policy(),
    ))
}

fn csv_reader(name: &str, body: &str) -> (String, crate::source::SourceInput) {
    (
        name.to_string(),
        crate::executor::single_file_reader(
            format!("{name}.csv"),
            Box::new(std::io::Cursor::new(body.as_bytes().to_vec())),
        ),
    )
}

fn sink_writer(name: &str) -> HashMap<String, Box<dyn std::io::Write + Send>> {
    HashMap::from([(
        name.to_string(),
        Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
    )])
}

fn run(
    yaml: &str,
    readers: crate::executor::SourceReaders,
    writers: HashMap<String, Box<dyn std::io::Write + Send>>,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
) -> Result<ExecutionReport, PipelineError> {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let params = PipelineRunParams {
        execution_id: "combine-consumer-lifecycle".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        arbitrator,
    )
}

/// Compile `yaml` and return the strategy the planner stamped on the
/// combine node named `combine_name` — the guard that keeps each test
/// honest about which branch it actually exercises.
fn compiled_combine_strategy(yaml: &str, combine_name: &str) -> CombineStrategy {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let validated = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile pipeline");
    let dag = validated.dag();
    for idx in dag.graph.node_indices() {
        if let PlanNode::Combine { name, strategy, .. } = &dag.graph[idx]
            && name == combine_name
        {
            return strategy.clone();
        }
    }
    panic!("combine node {combine_name:?} not present in compiled plan");
}

const ORDERS_RANGE_CSV: &str = "\
order_id,amount
o1,50
o2,150
";

const TAX_BRACKETS_CSV: &str = "\
bracket_id,min_amount,max_amount
b1,0,100
b2,100,1000
";

/// A two-range (`>=` and `<`) predicate routes to IEJoin. After the join
/// completes and its output drains downstream, the arbitrator registry
/// must hold no IEJoin consumer.
#[test]
fn iejoin_branch_unregisters_its_consumer_on_clean_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_iejoin
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
  name: tax_brackets
  config:
    name: tax_brackets
    type: csv
    path: tax_brackets.csv
    schema:
      - { name: bracket_id, type: string }
      - { name: min_amount, type: int }
      - { name: max_amount, type: int }
- type: combine
  name: bracketed
  input:
    orders: orders
    tax_brackets: tax_brackets
  config:
    where: "orders.amount >= tax_brackets.min_amount and orders.amount < tax_brackets.max_amount"
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit amount = orders.amount
      emit bracket_id = tax_brackets.bracket_id
    propagate_ck: driver
- type: output
  name: out
  input: bracketed
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "bracketed"),
            CombineStrategy::IEJoin
        ),
        "the two-range predicate must select IEJoin for this test to exercise that branch"
    );

    let arb = quiet_arbitrator();
    let report = run(
        yaml,
        HashMap::from([
            csv_reader("orders", ORDERS_RANGE_CSV),
            csv_reader("tax_brackets", TAX_BRACKETS_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(
        report.counters.total_count, 4,
        "both sources must ingest so the IEJoin branch actually runs"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the IEJoin branch's consumer must be unregistered after the branch exits"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

const PRODUCTS_SORTED_CSV: &str = "\
sku,price
s1,10
s2,20
s3,30
";

const PRICE_BRACKETS_SORTED_CSV: &str = "\
bracket_id,max
b1,15
b2,25
b3,100
";

/// A single-range predicate over two inputs that both declare `sort_order`
/// on the range axis routes to SortMerge. The branch's consumer must be
/// unregistered once the branch exits.
#[test]
fn sort_merge_branch_unregisters_its_consumer_on_clean_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_sort_merge
nodes:
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    sort_order:
      - field: price
    schema:
      - { name: sku, type: string }
      - { name: price, type: int }
- type: source
  name: brackets
  config:
    name: brackets
    type: csv
    path: brackets.csv
    sort_order:
      - field: max
    schema:
      - { name: bracket_id, type: string }
      - { name: max, type: int }
- type: combine
  name: assign_bracket
  input:
    products: products
    brackets: brackets
  config:
    where: "products.price < brackets.max"
    match: first
    on_miss: null_fields
    cxl: |
      emit sku = products.sku
      emit price = products.price
      emit bracket_id = brackets.bracket_id
    propagate_ck: driver
- type: output
  name: out
  input: assign_bracket
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "assign_bracket"),
            CombineStrategy::SortMerge
        ),
        "the single-range presorted predicate must select SortMerge for this test to exercise \
         that branch"
    );

    let arb = quiet_arbitrator();
    let report = run(
        yaml,
        HashMap::from([
            csv_reader("products", PRODUCTS_SORTED_CSV),
            csv_reader("brackets", PRICE_BRACKETS_SORTED_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(
        report.counters.total_count, 6,
        "both sources must ingest so the SortMerge branch actually runs"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the SortMerge branch's consumer must be unregistered after the branch exits"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

const ORDERS_EQUI_CSV: &str = "\
order_id,product_id
o1,p1
o2,p2
";

const PRODUCTS_EQUI_CSV: &str = "\
product_id,name
p1,widget
p2,gadget
";

/// A pure-equi predicate with an explicit `strategy: grace_hash` hint
/// routes to GraceHash regardless of budget. The branch's consumer must be
/// unregistered once the branch exits.
#[test]
fn grace_hash_branch_unregisters_its_consumer_on_clean_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_grace_hash
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: first
    on_miss: null_fields
    strategy: grace_hash
    cxl: |
      emit order_id = orders.order_id
      emit product_id = orders.product_id
      emit name = products.name
    propagate_ck: driver
- type: output
  name: out
  input: enriched
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "enriched"),
            CombineStrategy::GraceHash { .. }
        ),
        "the `strategy: grace_hash` hint on a pure-equi predicate must select GraceHash for this \
         test to exercise that branch"
    );

    let arb = quiet_arbitrator();
    let report = run(
        yaml,
        HashMap::from([
            csv_reader("orders", ORDERS_EQUI_CSV),
            csv_reader("products", PRODUCTS_EQUI_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(
        report.counters.total_count, 4,
        "both sources must ingest so the GraceHash branch actually runs"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the GraceHash branch's consumer must be unregistered after the branch exits"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

const ORDERS_UNMATCHED_CSV: &str = "\
order_id,amount
o1,50
o2,5000
";

/// The unregister must also cover the error exit: an IEJoin branch whose
/// `on_miss: error` fires on an unmatched driver row returns through the
/// kernel's `?`, and the branch must still unregister its consumer before
/// propagating the error. The run fails, but the retained arbitrator must
/// show an empty registry — a leaked consumer on the error path would keep
/// the finished branch summed into the registry.
#[test]
fn iejoin_branch_unregisters_its_consumer_on_error_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_iejoin_error
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
  name: tax_brackets
  config:
    name: tax_brackets
    type: csv
    path: tax_brackets.csv
    schema:
      - { name: bracket_id, type: string }
      - { name: min_amount, type: int }
      - { name: max_amount, type: int }
- type: combine
  name: bracketed
  input:
    orders: orders
    tax_brackets: tax_brackets
  config:
    where: "orders.amount >= tax_brackets.min_amount and orders.amount < tax_brackets.max_amount"
    match: first
    on_miss: error
    cxl: |
      emit order_id = orders.order_id
      emit amount = orders.amount
      emit bracket_id = tax_brackets.bracket_id
    propagate_ck: driver
- type: output
  name: out
  input: bracketed
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "bracketed"),
            CombineStrategy::IEJoin
        ),
        "the two-range predicate must select IEJoin for this test to exercise that branch"
    );

    let arb = quiet_arbitrator();
    let result = run(
        yaml,
        HashMap::from([
            csv_reader("orders", ORDERS_UNMATCHED_CSV),
            csv_reader("tax_brackets", TAX_BRACKETS_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    );

    assert!(
        matches!(result, Err(PipelineError::CombineMissingMatch { .. })),
        "the unmatched driver row under on_miss: error must fail the run, got {result:?}"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the IEJoin branch's consumer must be unregistered even when the branch exits via error"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

/// A pure-equi predicate with no `strategy` hint and inputs well under the
/// grace-hash threshold routes to the inline `HashBuildProbe` branch. After
/// the probe completes and its output drains downstream, the arbitrator
/// registry must hold no inline-combine consumer.
#[test]
fn inline_hash_branch_unregisters_its_consumer_on_clean_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_inline_hash
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit product_id = orders.product_id
      emit name = products.name
    propagate_ck: driver
- type: output
  name: out
  input: enriched
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "enriched"),
            CombineStrategy::HashBuildProbe
        ),
        "a pure-equi predicate with no strategy hint must select the inline HashBuildProbe branch \
         for this test to exercise it"
    );

    let arb = quiet_arbitrator();
    let report = run(
        yaml,
        HashMap::from([
            csv_reader("orders", ORDERS_EQUI_CSV),
            csv_reader("products", PRODUCTS_EQUI_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    )
    .expect("pipeline must run");

    assert_eq!(
        report.counters.total_count, 4,
        "both sources must ingest so the inline HashBuildProbe branch actually runs"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the inline HashBuildProbe branch's consumer must be unregistered after the branch exits"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}

const ORDERS_EQUI_DISJOINT_CSV: &str = "\
order_id,product_id
o1,p1
o2,p2
";

const PRODUCTS_EQUI_DISJOINT_CSV: &str = "\
product_id,name
p8,sprocket
p9,flange
";

/// The unregister must also cover the inline branch's error exit: an inline
/// `HashBuildProbe` combine whose `on_miss: error` fires on an unmatched
/// driver row returns through the probe kernel's `?`, and the branch must
/// still unregister its consumer before propagating the error. The build
/// side has already mirrored its hash-table bytes into the consumer handle
/// when the probe row misses, so a leaked consumer would keep that finished
/// join state summed into the registry for the rest of the run. The two
/// inputs share no join key, so the first driver row misses regardless of
/// which side the planner drives from.
#[test]
fn inline_hash_branch_unregisters_its_consumer_on_error_exit() {
    let yaml = r#"
pipeline:
  name: combine_lifecycle_inline_hash_error
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: first
    on_miss: error
    cxl: |
      emit order_id = orders.order_id
      emit product_id = orders.product_id
      emit name = products.name
    propagate_ck: driver
- type: output
  name: out
  input: enriched
  config:
    name: out
    type: csv
    path: out.csv
"#;
    assert!(
        matches!(
            compiled_combine_strategy(yaml, "enriched"),
            CombineStrategy::HashBuildProbe
        ),
        "a pure-equi predicate with no strategy hint must select the inline HashBuildProbe branch \
         for this test to exercise its error exit"
    );

    let arb = quiet_arbitrator();
    let result = run(
        yaml,
        HashMap::from([
            csv_reader("orders", ORDERS_EQUI_DISJOINT_CSV),
            csv_reader("products", PRODUCTS_EQUI_DISJOINT_CSV),
        ]),
        sink_writer("out"),
        Arc::clone(&arb),
    );

    assert!(
        matches!(result, Err(PipelineError::CombineMissingMatch { .. })),
        "the unmatched driver row under on_miss: error must fail the run, got {result:?}"
    );
    assert_eq!(
        arb.consumer_count(),
        0,
        "the inline HashBuildProbe branch's consumer must be unregistered even when the branch \
         exits via error"
    );
    assert_eq!(arb.sum_consumer_usage(), 0);
}
