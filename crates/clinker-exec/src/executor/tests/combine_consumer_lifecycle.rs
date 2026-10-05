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
        0.70,
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
pub(super) fn compiled_combine_strategy(yaml: &str, combine_name: &str) -> CombineStrategy {
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
- type: sink
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
- type: sink
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
- type: sink
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
- type: sink
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
- type: sink
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
- type: sink
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

/// An inline hash join, `enriched`, beside a branch it does not read:
/// `events` is read and buffered through `widened` before `enriched` builds,
/// and only `tagged`, which reads `enriched`'s output, reads `widened`. So
/// `widened`'s rows stay resident and spillable across `enriched`'s build.
/// `enriched` also feeds `enriched_out`, so it is not `tagged`'s streaming
/// driver and runs, whole, before `tagged`.
const INLINE_RECLAIM_YAML: &str = r#"
pipeline:
  name: inline_build_reclaim
  memory: { limit: "512M", backpressure: spill }
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
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: event_id, type: string }
      - { name: payload, type: string }
- type: transform
  name: widened
  input: events
  config:
    cxl: |
      emit event_id = event_id
      emit payload = payload
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    drive: orders
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = orders.order_id
      emit name = products.name
    propagate_ck: driver
- type: combine
  name: tagged
  input:
    enriched: enriched
    widened: widened
  config:
    where: "enriched.order_id == widened.event_id"
    drive: enriched
    match: first
    on_miss: null_fields
    cxl: |
      emit order_id = enriched.order_id
      emit name = enriched.name
      emit payload = widened.payload
    propagate_ck: driver
- type: sink
  name: out
  input: tagged
  config:
    name: out
    type: csv
    path: out.csv
- type: sink
  name: enriched_out
  input: enriched
  config:
    name: enriched_out
    type: csv
    path: enriched_out.csv
"#;

/// A CSV body generated row by row as it is read, so the input is never
/// held whole in the test process.
struct GeneratedCsv {
    rows: usize,
    next: usize,
    line: fn(usize) -> String,
    pending: Vec<u8>,
    at: usize,
}

impl GeneratedCsv {
    fn reader(
        header: &str,
        rows: usize,
        line: fn(usize) -> String,
    ) -> Box<dyn std::io::Read + Send> {
        Box::new(Self {
            rows,
            next: 0,
            line,
            pending: header.as_bytes().to_vec(),
            at: 0,
        })
    }
}

impl std::io::Read for GeneratedCsv {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.at == self.pending.len() {
            if self.next == self.rows {
                return Ok(0);
            }
            self.pending = (self.line)(self.next).into_bytes();
            self.at = 0;
            self.next += 1;
        }
        let n = buf.len().min(self.pending.len() - self.at);
        buf[..n].copy_from_slice(&self.pending[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Driver rows: at least the probe loop's 10,000-row check cadence, so a run
/// the build left past its limit meets the probe loop's check.
const RECLAIM_ORDERS: usize = 12_000;
/// Build rows; short text, so each row's charge is its fixed slot cost.
const RECLAIM_PRODUCTS: usize = 6_000;
/// Rows of the third branch, buffered in `widened` across the first join.
const RECLAIM_EVENTS: usize = 6_000;

/// The ledger capacity the low run is held to. The first join's finished
/// table does not fit beside the orders, products and `widened` rows the
/// run holds at its build (8.6 MB with the table), so it fits only by
/// spilling `widened`; once that join's state is released the second join
/// fits (7.0 MB at its peak). Below the ample run's charged peak, 10.8 MB.
const RECLAIM_CAPACITY: u64 = 8_400_000;

/// Run the fixture held to `capacity` bytes of ledger (ample when `None`),
/// reading no process memory so only the charged total can trip a limit;
/// return its report and every Output's bytes.
fn inline_reclaim_run(capacity: Option<u64>) -> Result<(ExecutionReport, String), PipelineError> {
    let config = clinker_plan::config::parse_config(INLINE_RECLAIM_YAML).expect("parse");
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("compile");
    let readers: crate::executor::SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            crate::executor::single_file_reader(
                "orders.csv",
                GeneratedCsv::reader("order_id,product_id\n", RECLAIM_ORDERS, |i| {
                    format!("o{i},p{}\n", i % RECLAIM_PRODUCTS)
                }),
            ),
        ),
        (
            "products".to_string(),
            crate::executor::single_file_reader(
                "products.csv",
                GeneratedCsv::reader("product_id,name\n", RECLAIM_PRODUCTS, |i| {
                    format!("p{i},n{i}\n")
                }),
            ),
        ),
        (
            "events".to_string(),
            crate::executor::single_file_reader(
                "events.csv",
                GeneratedCsv::reader("event_id,payload\n", RECLAIM_EVENTS, |i| {
                    format!("o{i},x{i}\n")
                }),
            ),
        ),
    ]);
    let names = ["out", "enriched_out"];
    let buffers: Vec<SharedBuffer> = names.iter().map(|_| SharedBuffer::new()).collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = names
        .iter()
        .zip(&buffers)
        .map(|(name, buffer)| {
            (
                name.to_string(),
                Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let memory_test = match capacity {
        Some(bytes) => crate::executor::MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => crate::executor::MemoryTestOverrides::default(),
    }
    .with_no_process_memory();
    let params = PipelineRunParams {
        execution_id: "inline-build-reclaim".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)?;
    let mut output = String::new();
    for (name, buffer) in names.iter().zip(&buffers) {
        output.push_str(name);
        output.push('\n');
        output.push_str(&buffer.as_string());
    }
    Ok((report, output))
}

/// The first join's finished table does not fit beside what the run holds,
/// while `widened`'s rows are resident and spillable: the join's hard-limit
/// check spills them on the walk and the run completes with the ample run's
/// output. Without that reclaim the run stops with E310 at the join's probe
/// loop, past its limit, `reclaim: none attempted`.
#[test]
fn an_inline_join_build_over_the_limit_spills_other_state_and_completes() {
    assert!(
        matches!(
            compiled_combine_strategy(INLINE_RECLAIM_YAML, "enriched"),
            CombineStrategy::HashBuildProbe
        ),
        "the pure-equi join must run the inline HashBuildProbe branch"
    );
    let (ample, ample_output) = inline_reclaim_run(None).expect("the ample run completes");
    assert!(
        ample.per_stage_spill_bytes_written.is_empty(),
        "ample memory spills nothing: {:?}",
        ample.per_stage_spill_bytes_written
    );
    assert!(
        RECLAIM_CAPACITY < ample.peak_consumer_usage_bytes,
        "the capacity {RECLAIM_CAPACITY} must sit below the ample charged peak {}",
        ample.peak_consumer_usage_bytes
    );

    let (low, low_output) = inline_reclaim_run(Some(RECLAIM_CAPACITY))
        .expect("the join's check makes room by spilling other state and the run completes");
    assert!(
        low.per_stage_spill_bytes_written
            .get("widened")
            .is_some_and(|&bytes| bytes > 0),
        "`widened`'s resident rows must be spilled to make room: {:?}",
        low.per_stage_spill_bytes_written
    );
    assert!(
        low_output == ample_output,
        "spilling another node's rows to make room must not change any Output's bytes"
    );
}
