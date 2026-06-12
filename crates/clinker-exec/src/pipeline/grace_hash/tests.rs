//! Unit tests for the grace hash join: partition assignment and the
//! spill/reload lifecycle including the block-nested-loop fallback.
//! Driven through both the public `execute_combine_grace_hash` entry
//! point and a hand-built `ReloadContext` harness for the BNL-only paths.
//! The distinct-key sketch's accuracy bounds are covered by the shared
//! `crate::sketch` tests.

use super::build::{GraceHll, MAX_HASH_BITS};
use super::spill::{
    BnlStats, PROBE_BUFFER_RESERVATION, RESULT_BATCH_SIZE, SKEW_REDUCTION_THRESHOLD, bnl_fallback,
};
use super::*;
use crate::pipeline::grace_spill::GraceSpillReader;
use clinker_record::SchemaBuilder;
use cxl::ast::Statement;
use cxl::lexer::Span as CxlSpan;
use cxl::parser::Parser;
use cxl::resolve::pass::resolve_program;
use cxl::typecheck::pass::type_check;
use cxl::typecheck::row::{QualifiedField, Row};

fn schema_with(cols: &[&str]) -> Arc<Schema> {
    let mut b = SchemaBuilder::with_capacity(cols.len());
    for c in cols {
        b = b.with_field(*c);
    }
    b.build()
}

/// Fresh exec-time statistics catalog for a grace-hash test to record its
/// build-side distinct estimate into.
fn fresh_stats_catalog()
-> std::sync::Arc<std::sync::Mutex<clinker_plan::plan::statistics::StatisticsCatalog>> {
    std::sync::Arc::new(std::sync::Mutex::new(
        clinker_plan::plan::statistics::StatisticsCatalog::new(),
    ))
}

/// Build a [`GraceStatsSink`] over a test catalog and key.
fn test_stats_sink<'a>(
    catalog: &std::sync::Arc<std::sync::Mutex<clinker_plan::plan::statistics::StatisticsCatalog>>,
    node: &'a str,
    column: &'a str,
) -> GraceStatsSink<'a> {
    GraceStatsSink {
        catalog: std::sync::Arc::clone(catalog),
        node,
        column,
    }
}

fn record_for(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
    Record::new(Arc::clone(schema), values)
}

/// Compile a single CXL key expression into the (typed_program,
/// expression) pair that `KeyExtractor::new` consumes. Mirrors
/// `pipeline::combine::tests::compile_key`.
fn compile_key(
    src: &str,
    fields: &[&str],
    row_fields: &[(&str, cxl::typecheck::Type)],
) -> (Arc<TypedProgram>, cxl::ast::Expr) {
    let parsed = Parser::parse(src);
    assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count)
        .unwrap_or_else(|d| panic!("resolve: {d:?}"));
    let mut cols: indexmap::IndexMap<QualifiedField, cxl::typecheck::Type> =
        indexmap::IndexMap::new();
    for (n, t) in row_fields {
        cols.insert(QualifiedField::bare(*n), t.clone());
    }
    let row = Row::closed(cols, CxlSpan::new(0, 0));
    let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("typecheck: {d:?}"));
    let expr = match &typed.program.statements[0] {
        Statement::Emit { expr, .. } => expr.clone(),
        _ => panic!("expected emit stmt"),
    };
    (Arc::new(typed), expr)
}

/// Budget calibrated to fire `should_spill` continuously (so the
/// largest-Building eviction loop takes effect) without firing
/// `should_abort` (which would short-circuit the build phase).
/// `limit` is 10 GiB so RSS-vs-hard-limit always falls inside;
/// `spill_threshold_pct` is set so soft limit = 1 KiB, well below
/// any host's resident set.
fn tiny_budget() -> MemoryArbitrator {
    MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.000_001, Box::new(NoOpPolicy))
}

#[test]
fn partition_assigner_alignment() {
    let a = PartitionAssigner::new(4);
    // Same hash → same partition (deterministic).
    for h in [0u64, 1, 0xDEAD_BEEF, !0] {
        assert_eq!(a.partition_for(h), a.partition_for(h));
    }
    assert_eq!(a.num_partitions(), 16);
    assert_eq!(a.hash_bits(), 4);
}

#[test]
fn partition_assigner_double_refines_uniformly() {
    // Doubling the partition count refines: every parent partition
    // splits into 2*p and 2*p + 1.
    let parent = PartitionAssigner::new(4);
    let child = parent.double().unwrap();
    assert_eq!(child.hash_bits(), 5);
    for h in (0..1024u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) {
        let pp = parent.partition_for(h) as u32;
        let cp = child.partition_for(h) as u32;
        assert!(
            cp == pp * 2 || cp == pp * 2 + 1,
            "child partition {cp} must be one of {{{}, {}}} for parent {pp}",
            pp * 2,
            pp * 2 + 1
        );
    }
}

#[test]
fn partition_assigner_caps_at_max_bits() {
    let mut a = PartitionAssigner::new(MAX_HASH_BITS);
    assert!(a.double().is_none());
    a = PartitionAssigner::new(20); // clamped down
    assert_eq!(a.hash_bits(), MAX_HASH_BITS);
}

#[test]
fn pipeline_temp_dir_owns_spill_files_on_drop() {
    // Pipeline-scoped TempDir is the owner; the executor only
    // borrows its path. Files committed via spill_partition stay
    // alive while the TempDir lives and disappear when it drops.
    let pipeline_dir = tempfile::Builder::new()
        .prefix("grace-pipeline-")
        .tempdir()
        .unwrap();
    let pipeline_path = pipeline_dir.path().to_path_buf();
    let mut exec = GraceHashExecutor::new(
        4,
        pipeline_dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
    )
    .unwrap();
    let schema = schema_with(&["k"]);
    // Deposit a record and force a spill so a file actually exists.
    let rec = record_for(&schema, vec![Value::Integer(7)]);
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
    exec.add_build_record(rec, 0, &budget).unwrap();
    exec.spill_partition(0).unwrap();
    let spilled_inside = std::fs::read_dir(&pipeline_path).unwrap().count();
    assert!(spilled_inside >= 1, "spill_partition must commit a file");
    drop(exec);
    assert!(
        pipeline_path.exists(),
        "pipeline-scoped dir must outlive the executor"
    );
    drop(pipeline_dir);
    assert!(
        !pipeline_path.exists(),
        "pipeline-scoped dir Drop must remove the spill files"
    );
}

#[test]
fn pipeline_temp_dir_cleans_on_panic_unwind() {
    // Operator-mid-spill panic leaks files unless an enclosing
    // TempDir whose lifetime outlives the operator collects them.
    // The pipeline-scoped TempDir provides that secondary sweep.
    let captured: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let pipeline_dir = tempfile::Builder::new()
            .prefix("grace-panic-")
            .tempdir()
            .unwrap();
        *captured.lock().unwrap() = Some(pipeline_dir.path().to_path_buf());
        let mut exec = GraceHashExecutor::new(
            4,
            pipeline_dir.path(),
            crate::pipeline::memory::ConsumerHandle::new(),
            true,
        )
        .unwrap();
        let schema = schema_with(&["k"]);
        let rec = record_for(&schema, vec![Value::Integer(99)]);
        let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        exec.add_build_record(rec, 0, &budget).unwrap();
        exec.spill_partition(0).unwrap();
        panic!("simulated mid-spill panic");
    }));
    assert!(result.is_err(), "panic must propagate out");
    let path = captured.lock().unwrap().clone().unwrap();
    assert!(
        !path.exists(),
        "pipeline-scoped TempDir Drop must clean spill files on panic unwind"
    );
}

/// Add records into a low-budget executor and assert that at least
/// one partition transitions to `OnDisk` before `finish_build` runs.
#[test]
fn spill_activates_under_tiny_budget() {
    let schema = schema_with(&["k", "v"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-test-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        4,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
    )
    .unwrap();
    let budget = tiny_budget();
    for i in 0..256i64 {
        let rec = record_for(
            &schema,
            vec![Value::Integer(i), Value::String(format!("row-{i}").into())],
        );
        // Synthetic hash: distribute uniformly across 16 partitions.
        let hash = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        exec.add_build_record(rec, hash, &budget).unwrap();
    }
    let on_disk = exec
        .partitions
        .iter()
        .filter(|p| matches!(p, PartitionState::OnDisk { .. }))
        .count();
    assert!(
        on_disk >= 1,
        "expected ≥1 partition spilled under tiny budget; got {on_disk}"
    );
}

/// RSS-independent backstop: grace-hash spills on the pull-mode
/// charged-byte sum even when the RSS arm of `should_spill` is inert.
/// The executor's `consumer_handle` is registered with the arbitrator
/// (mirroring production), then pre-charged above the soft limit while a
/// 1 GiB hard limit keeps the soft threshold (800 MiB) above any real
/// test-process RSS — the faithful Linux proxy for a platform where
/// `rss_bytes()` returns `None`. Adding a single build record drives
/// `add_build_record`'s `should_spill` poll, which must trip on the
/// charged bytes alone and evict a partition to disk. Without the
/// charged-byte arm the build would grow unbounded under no RSS reading.
#[test]
fn spill_activates_on_charged_bytes_without_rss() {
    let schema = schema_with(&["k", "v"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-test-")
        .tempdir()
        .unwrap();
    let consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
    let mut exec = GraceHashExecutor::new(4, dir.path(), consumer_handle.clone(), true).unwrap();

    // 1 GiB hard limit → 800 MiB soft limit, above any host's test RSS,
    // so the RSS arm of `should_spill` cannot trip. Register the handle so
    // its charged bytes flow into `sum_consumer_usage`.
    let budget = MemoryArbitrator::with_policy(1024 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
    budget.register_consumer(Arc::new(GraceHashConsumer::new(consumer_handle.clone())));
    let soft = budget.soft_limit();
    assert!(
        budget.peak_rss().is_none_or(|rss| rss < soft),
        "test invariant: real RSS must stay under the 800 MiB soft limit so only \
         the charged-byte arm can trip the spill"
    );

    // Pre-charge the handle above the soft limit but below the 1 GiB hard
    // limit (so `should_spill` trips while `should_abort` does not). One
    // real build record then carries enough Building bytes for
    // `spill_largest_building` to have a victim.
    consumer_handle.set_bytes(soft + 1);
    let rec = record_for(
        &schema,
        vec![Value::Integer(0), Value::String("row-0".into())],
    );
    exec.add_build_record(rec, 0, &budget).unwrap();

    let on_disk = exec
        .partitions
        .iter()
        .filter(|p| matches!(p, PartitionState::OnDisk { .. }))
        .count();
    assert!(
        on_disk >= 1,
        "charged bytes over the soft limit must spill a partition with the RSS arm inert; \
         got {on_disk} on-disk partitions"
    );
}

/// Lazy probe spill: a partition pre-spilled during build receives a
/// probe record and writes it to its probe-side spill file rather
/// than dropping it.
#[test]
fn lazy_probe_spill_routes_to_partition_file() {
    let schema = schema_with(&["k"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-test-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        4,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
    )
    .unwrap();
    let budget = tiny_budget();

    // Send 64 records all to partition 0 (top 4 bits = 0). Use a
    // hash with high-bits zero.
    let probe_partition_hash: u64 = 0x0000_0000_0000_1234;
    for i in 0..64i64 {
        let rec = record_for(&schema, vec![Value::Integer(i)]);
        exec.add_build_record(rec, probe_partition_hash, &budget)
            .unwrap();
    }
    // Force spill of partition 0.
    exec.spill_largest_building(&budget).unwrap();
    let p0_disk = matches!(&exec.partitions[0], PartitionState::OnDisk { .. });
    assert!(p0_disk, "partition 0 must be on disk after force-spill");

    // Probe a record into partition 0; it should write to the
    // partition's probe-side file.
    let probe = record_for(&schema, vec![Value::Integer(999)]);
    let outcome = exec
        .probe_record(&probe, &[Value::Integer(999)], probe_partition_hash)
        .unwrap();
    assert!(matches!(outcome, ProbeOutcome::Spilled));

    exec.finalize_probe_spills().unwrap();
    match &exec.partitions[0] {
        PartitionState::OnDisk {
            probe_files,
            probe_count,
            ..
        } => {
            assert_eq!(probe_files.len(), 1);
            assert_eq!(*probe_count, 1);
            assert!(probe_files[0].exists());
        }
        _ => panic!("partition 0 must remain OnDisk after probe spill"),
    }
}

/// End-to-end correctness via `execute_combine_grace_hash` with a
/// hand-built `DecomposedPredicate` and `KeyExtractor` aligned to a
/// pure-equi join `orders.k == products.k`. The output set must
/// match the cross-product filtered by the predicate, regardless
/// of which partitions get spilled.
#[test]
fn execute_grace_hash_partition_pair_correct() {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use cxl::eval::{EvalContext, StableEvalContext};

    // Driver and build schemas use distinct bare names for the
    // join key (`dk` and `bk`) so the bare-name CombineResolver
    // mapping is unambiguous.
    let driver_schema = schema_with(&["dk", "v"]);
    let build_schema = schema_with(&["bk", "name"]);

    let drivers: Vec<(Record, RecordOrder)> = (0..10i64)
        .map(|i| {
            (
                Record::new(
                    Arc::clone(&driver_schema),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                i as u64,
            )
        })
        .collect();
    let builds: Vec<Record> = (0..10i64)
        .map(|i| {
            Record::new(
                Arc::clone(&build_schema),
                vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();

    // Compose typed programs for left (driver) and right (build)
    // key expressions. Each side runs against its own row.
    let (left_tp, left_expr) =
        compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
    let (right_tp, right_expr) =
        compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);

    let decomposed = DecomposedPredicate {
        equalities: vec![EqualityConjunct {
            left_expr,
            left_input: Arc::from("orders"),
            left_program: left_tp,
            right_expr,
            right_input: Arc::from("products"),
            right_program: right_tp,
        }],
        ranges: Vec::new(),
        residual: None,
    };

    // Resolver mapping: orders.dk → driver col 0, orders.v →
    // driver col 1, products.bk → build col 0, products.name →
    // build col 1. Bare names `dk`, `v`, `bk`, `name` are
    // unambiguous because each appears on exactly one side.
    let mut mapping_q: std::collections::HashMap<
        clinker_plan::plan::row_type::QualifiedField,
        (JoinSide, u32),
    > = std::collections::HashMap::new();
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "dk"),
        (JoinSide::Probe, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "v"),
        (JoinSide::Probe, 1),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "bk"),
        (JoinSide::Build, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "name"),
        (JoinSide::Build, 1),
    );

    let mut combine_inputs: indexmap::IndexMap<String, clinker_plan::plan::combine::CombineInput> =
        indexmap::IndexMap::new();
    let mut driver_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("dk"),
        cxl::typecheck::Type::Int,
    );
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("v"),
        cxl::typecheck::Type::String,
    );
    let mut build_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("bk"),
        cxl::typecheck::Type::Int,
    );
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("name"),
        cxl::typecheck::Type::String,
    );
    combine_inputs.insert(
        "orders".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("orders"),
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));

    // Drive everything through grace hash. body_program=None so
    // the synthetic-step concatenation path is exercised; that's
    // fine for correctness because we only assert on join
    // membership, not on the body shape.
    let mut combined_schema_builder = clinker_record::SchemaBuilder::new();
    combined_schema_builder = combined_schema_builder.with_field("dk");
    combined_schema_builder = combined_schema_builder.with_field("v");
    combined_schema_builder = combined_schema_builder.with_field("bk");
    combined_schema_builder = combined_schema_builder.with_field("name");
    let combined_schema = combined_schema_builder.build();

    let dir = tempfile::Builder::new()
        .prefix("gh-e2e-")
        .tempdir()
        .unwrap();
    let stats_catalog = fresh_stats_catalog();
    let result = execute_combine_grace_hash(GraceHashExec {
        name: "grace_test",
        build_qualifier: "products",
        driver_records: drivers,
        build_records: builds,
        decomposed: &decomposed,
        body_program: None,
        resolver_mapping: &resolver_mapping,
        output_schema: Some(&combined_schema),
        match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
        on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
        partition_bits: 4,
        propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
        ctx: &ctx,
        budget: &budget,
        spill_dir: dir.path(),
        spill_compress: true,
        consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
        strategy: clinker_plan::config::ErrorStrategy::FailFast,
        stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
    })
    .expect("grace hash E2E")
    .records;

    assert_eq!(result.len(), 10, "every driver matches one build by k");
    // Verify membership: each (k, v, k, name) tuple is present.
    let mut seen: Vec<(i64, String, String)> = result
        .iter()
        .map(|(rec, _)| {
            let k = match rec.values()[0] {
                Value::Integer(i) => i,
                _ => panic!("k not int"),
            };
            let v = match &rec.values()[1] {
                Value::String(s) => s.to_string(),
                _ => panic!("v not str"),
            };
            // After widen: build columns occupy slots 2 & 3 (k_b,
            // name) but the synthetic concat writes raw values
            // positionally.
            let name = match &rec.values()[3] {
                Value::String(s) => s.to_string(),
                _ => panic!("name not str"),
            };
            (k, v, name)
        })
        .collect();
    seen.sort_by_key(|(k, _, _)| *k);
    let expected: Vec<(i64, String, String)> = (0..10)
        .map(|i| (i, format!("d-{i}"), format!("b-{i}")))
        .collect();
    assert_eq!(seen, expected);

    // Plane B lifecycle: the join routed its build-side distinct-count
    // estimate into the catalog. 10 distinct build keys over a 1024-
    // register HLL lands at or very near 10.
    let catalog = stats_catalog.lock().unwrap();
    let distinct = catalog
        .column("products", "products")
        .and_then(|c| c.distinct)
        .expect("grace hash must record a build-side distinct estimate");
    assert!(
        (8..=12).contains(&distinct.0),
        "10 distinct build keys should estimate near 10; got {}",
        distinct.0
    );
}

/// End-to-end: a tiny memory budget forces partition spill during
/// build. The reload phase rehydrates the spilled partitions and
/// emits the same join membership the in-memory path would.
#[test]
fn execute_grace_hash_spill_then_reload_correct() {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use cxl::eval::{EvalContext, StableEvalContext};

    let driver_schema = schema_with(&["dk", "v"]);
    let build_schema = schema_with(&["bk", "name"]);

    let drivers: Vec<(Record, RecordOrder)> = (0..32i64)
        .map(|i| {
            (
                Record::new(
                    Arc::clone(&driver_schema),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                i as u64,
            )
        })
        .collect();
    let builds: Vec<Record> = (0..32i64)
        .map(|i| {
            Record::new(
                Arc::clone(&build_schema),
                vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();

    let (left_tp, left_expr) =
        compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
    let (right_tp, right_expr) =
        compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);
    let decomposed = DecomposedPredicate {
        equalities: vec![EqualityConjunct {
            left_expr,
            left_input: Arc::from("orders"),
            left_program: left_tp,
            right_expr,
            right_input: Arc::from("products"),
            right_program: right_tp,
        }],
        ranges: Vec::new(),
        residual: None,
    };

    let mut mapping_q: std::collections::HashMap<
        clinker_plan::plan::row_type::QualifiedField,
        (JoinSide, u32),
    > = std::collections::HashMap::new();
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "dk"),
        (JoinSide::Probe, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "v"),
        (JoinSide::Probe, 1),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "bk"),
        (JoinSide::Build, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "name"),
        (JoinSide::Build, 1),
    );

    let mut combine_inputs: indexmap::IndexMap<String, clinker_plan::plan::combine::CombineInput> =
        indexmap::IndexMap::new();
    let mut driver_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("dk"),
        cxl::typecheck::Type::Int,
    );
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("v"),
        cxl::typecheck::Type::String,
    );
    let mut build_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("bk"),
        cxl::typecheck::Type::Int,
    );
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("name"),
        cxl::typecheck::Type::String,
    );
    combine_inputs.insert(
        "orders".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("orders"),
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);

    // Big hard limit so should_abort never fires; tiny spill
    // threshold so should_spill fires immediately (process RSS
    // far exceeds 1 KiB on any host). This decouples spill
    // activation from build abort.
    let budget =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.000_001, Box::new(NoOpPolicy));

    let mut combined_schema_builder = clinker_record::SchemaBuilder::new();
    combined_schema_builder = combined_schema_builder.with_field("dk");
    combined_schema_builder = combined_schema_builder.with_field("v");
    combined_schema_builder = combined_schema_builder.with_field("bk");
    combined_schema_builder = combined_schema_builder.with_field("name");
    let combined_schema = combined_schema_builder.build();

    let dir = tempfile::Builder::new()
        .prefix("gh-spill-e2e-")
        .tempdir()
        .unwrap();
    let stats_catalog = fresh_stats_catalog();
    let result = execute_combine_grace_hash(GraceHashExec {
        name: "grace_spill_test",
        build_qualifier: "products",
        driver_records: drivers,
        build_records: builds,
        decomposed: &decomposed,
        body_program: None,
        resolver_mapping: &resolver_mapping,
        output_schema: Some(&combined_schema),
        match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
        on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
        partition_bits: 4,
        propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
        ctx: &ctx,
        budget: &budget,
        spill_dir: dir.path(),
        spill_compress: true,
        consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
        strategy: clinker_plan::config::ErrorStrategy::FailFast,
        stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
    })
    .expect("grace hash spill E2E")
    .records;

    assert_eq!(
        result.len(),
        32,
        "every driver matches one build under spill"
    );
    let mut keys: Vec<i64> = result
        .iter()
        .map(|(rec, _)| match rec.values()[0] {
            Value::Integer(i) => i,
            _ => panic!(),
        })
        .collect();
    keys.sort();
    assert_eq!(keys, (0..32).collect::<Vec<_>>());
}

/// Disk-quota gate: a build phase that spills more than the
/// configured `max_spill_bytes` aborts with the dedicated
/// `SpillCapExceeded` (E320) surface instead of continuing to fill
/// the disk. The hard memory limit is large (so `should_abort`
/// never fires); only the disk quota can cause this combine to
/// fail, and the cap error must NOT masquerade as an out-of-memory
/// E310.
#[test]
fn execute_grace_hash_aborts_on_disk_quota_overflow() {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use cxl::eval::{EvalContext, StableEvalContext};

    let driver_schema = schema_with(&["dk", "v"]);
    let build_schema = schema_with(&["bk", "name"]);

    // Many records on the build side so the tiny spill threshold
    // forces a partition flush before the quota gate trips.
    let drivers: Vec<(Record, RecordOrder)> = (0..16i64)
        .map(|i| {
            (
                Record::new(
                    Arc::clone(&driver_schema),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                i as u64,
            )
        })
        .collect();
    let builds: Vec<Record> = (0..512i64)
        .map(|i| {
            Record::new(
                Arc::clone(&build_schema),
                vec![
                    Value::Integer(i % 16),
                    Value::String(format!("b-{i:08}-padding-padding-padding-padding").into()),
                ],
            )
        })
        .collect();

    let (left_tp, left_expr) =
        compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
    let (right_tp, right_expr) =
        compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);
    let decomposed = DecomposedPredicate {
        equalities: vec![EqualityConjunct {
            left_expr,
            left_input: Arc::from("orders"),
            left_program: left_tp,
            right_expr,
            right_input: Arc::from("products"),
            right_program: right_tp,
        }],
        ranges: Vec::new(),
        residual: None,
    };

    let mut mapping_q: std::collections::HashMap<
        clinker_plan::plan::row_type::QualifiedField,
        (JoinSide, u32),
    > = std::collections::HashMap::new();
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "dk"),
        (JoinSide::Probe, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "v"),
        (JoinSide::Probe, 1),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "bk"),
        (JoinSide::Build, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "name"),
        (JoinSide::Build, 1),
    );

    let mut combine_inputs: indexmap::IndexMap<String, clinker_plan::plan::combine::CombineInput> =
        indexmap::IndexMap::new();
    let mut driver_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("dk"),
        cxl::typecheck::Type::Int,
    );
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("v"),
        cxl::typecheck::Type::String,
    );
    let mut build_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("bk"),
        cxl::typecheck::Type::Int,
    );
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("name"),
        cxl::typecheck::Type::String,
    );
    combine_inputs.insert(
        "orders".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("orders"),
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);

    // Memory hard limit huge so should_abort never fires; spill
    // threshold tiny so spills happen; disk quota tight so the
    // first partition flush trips it.
    let budget =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.000_001, Box::new(NoOpPolicy));
    budget.set_max_spill_bytes(64);

    let combined_schema = clinker_record::SchemaBuilder::new()
        .with_field("dk")
        .with_field("v")
        .with_field("bk")
        .with_field("name")
        .build();

    let dir = tempfile::Builder::new()
        .prefix("gh-quota-")
        .tempdir()
        .unwrap();
    let stats_catalog = fresh_stats_catalog();
    let result = execute_combine_grace_hash(GraceHashExec {
        name: "grace_quota_test",
        build_qualifier: "products",
        driver_records: drivers,
        build_records: builds,
        decomposed: &decomposed,
        body_program: None,
        resolver_mapping: &resolver_mapping,
        output_schema: Some(&combined_schema),
        match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
        on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
        partition_bits: 4,
        propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
        ctx: &ctx,
        budget: &budget,
        spill_dir: dir.path(),
        spill_compress: true,
        consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
        strategy: clinker_plan::config::ErrorStrategy::FailFast,
        stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
    });

    let err = result.expect_err("disk quota must abort the combine");
    match &err {
        PipelineError::SpillCapExceeded {
            node,
            cap,
            attempted,
            current,
        } => {
            assert_eq!(node, "grace_quota_test");
            assert_eq!(*cap, 64, "reported cap must equal the configured quota");
            assert!(*attempted > 0, "the overflowing flush must report its size");
            assert!(
                *current > *cap,
                "cumulative spilled ({current}) must exceed the cap ({cap})"
            );
        }
        other => panic!("disk-quota overflow must surface SpillCapExceeded; got {other:?}"),
    }
    assert!(
        budget.cumulative_spill_bytes() > 64,
        "cumulative_spill_bytes must reflect the overflowing total"
    );
}

/// Round-trip records through the spill writer/reader by calling
/// `add_build_record`, `spill_largest_building`, then reloading via
/// `drain_spilled` + `GraceSpillReader`.
#[test]
fn build_spill_reload_records_match() {
    let schema = schema_with(&["k", "v"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-test-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        2,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
    )
    .unwrap();
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy)); // never spills via budget
    let originals: Vec<Record> = (0..16i64)
        .map(|i| {
            record_for(
                &schema,
                vec![Value::Integer(i), Value::String(format!("v-{i}").into())],
            )
        })
        .collect();
    for (i, r) in originals.iter().enumerate() {
        exec.add_build_record(r.clone(), i as u64, &budget).unwrap();
    }
    // Force-spill every partition.
    for idx in 0..exec.partitions.len() {
        let _ = exec.spill_partition(idx);
    }
    let spilled = exec.drain_spilled();
    let mut reloaded: Vec<Record> = Vec::new();
    for sp in spilled {
        for path in &sp.build_files {
            let reader = GraceSpillReader::open(path, Arc::clone(&schema)).unwrap();
            for r in reader {
                reloaded.push(r.unwrap());
            }
        }
    }
    // Sort both by k to compare independent of partition ordering.
    let mut by_k: Vec<(i64, String)> = reloaded
        .iter()
        .map(|r| {
            let k = match r.get("k") {
                Some(Value::Integer(i)) => *i,
                _ => panic!("missing k"),
            };
            let v = match r.get("v") {
                Some(Value::String(s)) => s.to_string(),
                _ => panic!("missing v"),
            };
            (k, v)
        })
        .collect();
    by_k.sort_by_key(|(k, _)| *k);
    let expected: Vec<(i64, String)> = (0..16).map(|i| (i, format!("v-{i}"))).collect();
    assert_eq!(by_k, expected);
}

// ──────────────────────────────────────────────────────────────────
// Skew / BNL / E310 hard-gate tests
//
// The BNL fallback runs inside [`process_spilled_partition`]'s
// skew-detection branch. Driving it through a full
// `execute_combine_grace_hash` would require manufacturing skew
// through the public input shape; instead we build a minimal
// [`ReloadContext`] + [`SpilledPartition`] in test code so we can
// both observe [`BnlStats`] and assert directly on the function's
// chunking / batching invariants.
// ──────────────────────────────────────────────────────────────────

/// Build a tiny harness wrapping the keyed-pair join `dk == bk` so
/// each BNL test can drive [`bnl_fallback`] without re-typing the
/// `CombineResolverMapping` boilerplate.
struct BnlHarness {
    decomposed: clinker_plan::plan::combine::DecomposedPredicate,
    resolver_mapping: crate::executor::combine::CombineResolverMapping,
    build_extractor: KeyExtractor,
    driver_extractor: KeyExtractor,
    emit: EmitArgsOwned,
    build_schema: Arc<Schema>,
    driver_schema: Arc<Schema>,
    stable: cxl::eval::StableEvalContext,
    source_file: Arc<str>,
    hash_state: ahash::RandomState,
    spill_dir: tempfile::TempDir,
}

/// Owned analogue of [`EmitArgs`] — the live struct is borrow-only,
/// so the harness keeps owned copies and reconstitutes the borrowed
/// view inside each test.
struct EmitArgsOwned {
    name: String,
    match_mode: MatchMode,
    on_miss: OnMiss,
    build_qualifier: String,
    output_schema: Arc<Schema>,
}

fn build_bnl_harness() -> BnlHarness {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use cxl::eval::StableEvalContext;

    let driver_schema = schema_with(&["dk", "v"]);
    let build_schema = schema_with(&["bk", "name"]);

    let (left_tp, left_expr) =
        compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
    let (right_tp, right_expr) =
        compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);

    let decomposed = DecomposedPredicate {
        equalities: vec![EqualityConjunct {
            left_expr: left_expr.clone(),
            left_input: Arc::from("orders"),
            left_program: Arc::clone(&left_tp),
            right_expr: right_expr.clone(),
            right_input: Arc::from("products"),
            right_program: Arc::clone(&right_tp),
        }],
        ranges: Vec::new(),
        residual: None,
    };

    let mut mapping_q: std::collections::HashMap<
        clinker_plan::plan::row_type::QualifiedField,
        (JoinSide, u32),
    > = std::collections::HashMap::new();
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "dk"),
        (JoinSide::Probe, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("orders", "v"),
        (JoinSide::Probe, 1),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "bk"),
        (JoinSide::Build, 0),
    );
    mapping_q.insert(
        clinker_plan::plan::row_type::QualifiedField::qualified("products", "name"),
        (JoinSide::Build, 1),
    );

    let mut combine_inputs: indexmap::IndexMap<String, clinker_plan::plan::combine::CombineInput> =
        indexmap::IndexMap::new();
    let mut driver_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("dk"),
        cxl::typecheck::Type::Int,
    );
    driver_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("v"),
        cxl::typecheck::Type::String,
    );
    let mut build_row_cols: indexmap::IndexMap<
        clinker_plan::plan::row_type::QualifiedField,
        cxl::typecheck::Type,
    > = indexmap::IndexMap::new();
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("bk"),
        cxl::typecheck::Type::Int,
    );
    build_row_cols.insert(
        clinker_plan::plan::row_type::QualifiedField::bare("name"),
        cxl::typecheck::Type::String,
    );
    combine_inputs.insert(
        "orders".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("orders"),
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let driver_extractor = KeyExtractor::new(vec![(left_tp, left_expr)]);
    let build_extractor = KeyExtractor::new(vec![(right_tp, right_expr)]);

    let combined_schema = SchemaBuilder::new()
        .with_field("dk")
        .with_field("v")
        .with_field("bk")
        .with_field("name")
        .build();

    BnlHarness {
        decomposed,
        resolver_mapping,
        build_extractor,
        driver_extractor,
        emit: EmitArgsOwned {
            name: "bnl_test".to_string(),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            build_qualifier: "products".to_string(),
            output_schema: combined_schema,
        },
        build_schema,
        driver_schema,
        stable: StableEvalContext::test_default(),
        source_file: Arc::from("test.csv"),
        hash_state: ahash::RandomState::new(),
        spill_dir: tempfile::Builder::new()
            .prefix("bnl-test-")
            .tempdir()
            .unwrap(),
    }
}

/// Run `f` with a freshly-built [`ReloadContext`] borrowed off
/// the harness. The closure owns the BNL invocation; lifetime
/// inference threads the harness's borrows through `f`'s
/// parameter without resorting to transmutes.
fn with_reload_context<R>(h: &BnlHarness, f: impl FnOnce(&ReloadContext<'_>) -> R) -> R {
    let emit = EmitArgs {
        name: &h.emit.name,
        decomposed: &h.decomposed,
        resolver_mapping: &h.resolver_mapping,
        output_schema: Some(&h.emit.output_schema),
        match_mode: h.emit.match_mode,
        on_miss: h.emit.on_miss,
        build_qualifier: &h.emit.build_qualifier,
        propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
        strategy: clinker_plan::config::ErrorStrategy::FailFast,
    };
    let eval_ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
    let rc = ReloadContext {
        name: &h.emit.name,
        build_extractor: &h.build_extractor,
        driver_extractor: &h.driver_extractor,
        emit: &emit,
        ctx: &eval_ctx,
        build_schema: Arc::clone(&h.build_schema),
        driver_schema: Arc::clone(&h.driver_schema),
        spill_dir: h.spill_dir.path(),
        spill_compress: true,
        hash_state: &h.hash_state,
    };
    f(&rc)
}

/// Spill `build_records` to a single file under partition_id 0 and
/// `probe_records` to a sibling probe file. Populates the
/// returned [`SpilledPartition`] and feeds the HLL.
fn spill_for_bnl(
    h: &BnlHarness,
    build_records: &[Record],
    probe_records: &[Record],
    partition_id: u16,
    hash_bits: u8,
) -> SpilledPartition {
    let mut bw = GraceSpillWriter::new(h.spill_dir.path(), hash_bits, partition_id, true).unwrap();
    let mut sketch = GraceHll::new();
    for r in build_records {
        bw.write_record(r).unwrap();
        // Feed the HLL via the build-side hash of the join key.
        let stable = cxl::eval::StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("test.csv");
        let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
        let keys = h.build_extractor.extract(&ctx, r).unwrap();
        sketch.add(hash_composite_key(&keys, &h.hash_state));
    }
    let (bpath, _b_written) = bw.finish().unwrap();
    let mut probe_files: Vec<SpillFilePath> = Vec::new();
    if !probe_records.is_empty() {
        let mut pw =
            GraceSpillWriter::new(h.spill_dir.path(), hash_bits, partition_id | 0x8000, true)
                .unwrap();
        for r in probe_records {
            pw.write_record(r).unwrap();
        }
        let (p_path, _p_written) = pw.finish().unwrap();
        probe_files.push(p_path);
    }
    SpilledPartition {
        partition_id,
        build_files: vec![bpath],
        probe_files,
        build_count: build_records.len() as u64,
        hash_bits,
        distinct_sketch: sketch,
    }
}

/// Hard-gate 1: a uniform-key build dataset (every record carries
/// the same join key) cannot be split usefully. After
/// `assigner.double()` the largest child still holds 100% of the
/// parent's records (max_child / parent ≥ 1.0 ≫ 0.8), so the
/// reload path must hand the partition off to BNL rather than
/// recursing further.
#[test]
fn test_skew_detection_triggers_bnl() {
    let h = build_bnl_harness();
    // 200 records, all keyed at 42 → identical hash, identical
    // partition no matter the assigner width.
    let builds: Vec<Record> = (0..200i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![
                    Value::Integer(42),
                    Value::String(format!("name-{i}").into()),
                ],
            )
        })
        .collect();
    let probes: Vec<Record> = (0..5i64)
        .map(|i| {
            record_for(
                &h.driver_schema,
                vec![Value::Integer(42), Value::String(format!("d-{i}").into())],
            )
        })
        .collect();

    // Use a parent assigner with a few free bits so `double()` is
    // available — that's the path that lets the skew check fire.
    let parent_bits = 4u8;
    let sp = spill_for_bnl(&h, &builds, &probes, /* partition_id */ 0, parent_bits);

    // Verify the math the executor uses: classify every build
    // record under the doubled assigner; the largest child must
    // exceed the SKEW_REDUCTION_THRESHOLD-derived ceiling.
    let parent_assigner = PartitionAssigner::new(parent_bits);
    let child = parent_assigner.double().unwrap();
    let parent_id = sp.partition_id as u64;
    let stable = cxl::eval::StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
    let mut a = 0usize;
    let mut b = 0usize;
    for r in &builds {
        let keys = h.build_extractor.extract(&ctx, r).unwrap();
        let hash = hash_composite_key(&keys, &h.hash_state);
        let cp = child.partition_for(hash) as u64;
        if cp == parent_id * 2 {
            a += 1;
        } else {
            b += 1;
        }
    }
    let max_child = a.max(b);
    let parent_count = builds.len();
    assert!(
        (max_child as f64) > (1.0 - SKEW_REDUCTION_THRESHOLD) * (parent_count as f64),
        "uniform-key partition must trip the irreducible threshold; \
             max_child={max_child}, parent={parent_count}",
    );

    // Now drive BNL directly and confirm it produces the expected
    // 5 driver × 200 build = 1000 join rows.
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            builds,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
            },
            &mut stats,
        )
        .expect("BNL fallback must run on irreducible partition");
    });
    assert_eq!(
        output.len(),
        5 * 200,
        "BNL must produce the cross-product of matching keys"
    );
    assert!(
        stats.chunks_processed >= 1,
        "BNL must process at least one chunk"
    );
}

/// Hard-gate 2: BNL output equals the in-memory hash join over the
/// same input. Tests the join correctness invariant under the
/// chunked-build path.
#[test]
fn test_bnl_fallback_correct_output() {
    let h = build_bnl_harness();
    // 50 unique keys, each carried by exactly one build and one
    // probe row. Expected result: 50 join rows.
    let builds: Vec<Record> = (0..50i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();
    let probes: Vec<Record> = (0..50i64)
        .map(|i| {
            record_for(
                &h.driver_schema,
                vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
            )
        })
        .collect();
    let sp = spill_for_bnl(&h, &builds, &probes, 0, 2);

    // Force a small chunk budget so the chunked path actually
    // splits the build into pieces (not a single chunk).
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            builds,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
            },
            &mut stats,
        )
        .expect("BNL must succeed on non-skewed input");
    });

    // Every probe joins exactly one build by `dk == bk`. The
    // synthetic-step concatenation writes (dk, v, bk, name) into
    // the combined schema.
    assert_eq!(output.len(), 50, "join must yield one row per key");
    let mut keys: Vec<i64> = output
        .iter()
        .map(|(r, _)| match r.values()[0] {
            Value::Integer(i) => i,
            _ => panic!("expected Integer at column 0"),
        })
        .collect();
    keys.sort();
    assert_eq!(keys, (0..50).collect::<Vec<_>>());

    // Each row's bk (column 2) must equal its dk (column 0).
    for (r, _) in &output {
        let dk = match r.values()[0] {
            Value::Integer(i) => i,
            _ => panic!(),
        };
        let bk = match r.values()[2] {
            Value::Integer(i) => i,
            _ => panic!(),
        };
        assert_eq!(dk, bk, "join must align dk == bk per equality conjunct");
    }
}

/// Hard-gate 3: BNL respects the `(soft_limit -
/// PROBE_BUFFER_RESERVATION) / 2` chunk budget formula. Verified
/// by feeding a small-soft-limit budget and asserting the chunk
/// budget the function resolves to lands at the expected value
/// AND that the largest observed hash-table footprint stays within
/// it. The peak observation is the in-process bound the test
/// can prove without injecting an artificial allocator.
#[test]
fn test_bnl_bounded_memory() {
    let h = build_bnl_harness();
    // Mid-sized build + probe set so chunks > 1.
    let builds: Vec<Record> = (0..400i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![Value::Integer(7), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();
    let probes: Vec<Record> = (0..50i64)
        .map(|i| {
            record_for(
                &h.driver_schema,
                vec![Value::Integer(7), Value::String(format!("d-{i}").into())],
            )
        })
        .collect();
    let sp = spill_for_bnl(&h, &builds, &probes, 0, 2);

    // Budget with hard_limit huge (so should_abort never fires) and
    // soft_limit just below PROBE_BUFFER_RESERVATION (so the chunk
    // formula's saturating_sub bottoms out at zero and the `max(1)`
    // floor kicks in). spill_threshold_pct expresses soft as a
    // fraction of hard.
    let target_soft = (PROBE_BUFFER_RESERVATION as f64) / 2.0; // ~2 MB < reservation
    let budget = MemoryArbitrator::with_policy(
        u64::MAX,
        target_soft / (u64::MAX as f64),
        Box::new(NoOpPolicy),
    );
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;

    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            builds,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
            },
            &mut stats,
        )
        .expect("BNL must succeed with bounded chunks");
    });

    // Verify the formula exactly. soft_limit = limit; the
    // saturating_sub goes to zero (limit < PROBE_BUFFER_RESERVATION),
    // saturating_div(2) stays zero, .max(1) lifts to 1.
    assert_eq!(stats.chunk_byte_budget, 1, "chunk budget formula floor");

    // With a 1-byte chunk budget every record forms its own chunk,
    // so chunks_processed == build size and peak_chunk_records
    // is 1.
    assert_eq!(
        stats.chunks_processed, 400,
        "1-byte chunk budget should make every record its own chunk"
    );
    assert_eq!(
        stats.peak_chunk_records, 1,
        "single-record chunks bound peak_chunk_records to 1"
    );
    // Now drive the same input with a soft-limit large enough for
    // one chunk and confirm the formula resolves to the expected
    // (soft - reservation) / 2 value. hard_limit stays at u64::MAX
    // so should_abort cannot fire on RSS.
    let big_soft = (PROBE_BUFFER_RESERVATION as u64) * 8;
    let big_budget = MemoryArbitrator::with_policy(
        u64::MAX,
        (big_soft as f64) / (u64::MAX as f64),
        Box::new(NoOpPolicy),
    );
    let sp2 = spill_for_bnl(
        &h,
        &(0..10i64)
            .map(|i| {
                record_for(
                    &h.build_schema,
                    vec![Value::Integer(7), Value::String(format!("b-{i}").into())],
                )
            })
            .collect::<Vec<_>>(),
        &probes,
        1,
        2,
    );
    let mut output2: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats2 = BnlStats::default();
    let mut body_eval2: Option<ProgramEvaluator> = None;
    let builds2: Vec<Record> = (0..10i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![Value::Integer(7), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp2,
            builds2,
            &mut body_eval2,
            &big_budget,
            &mut GraceEmitSink {
                records: &mut output2,
                failures: &mut Vec::new(),
            },
            &mut stats2,
        )
        .unwrap();
    });
    let expected_budget = (big_soft as usize - PROBE_BUFFER_RESERVATION) / 2;
    assert_eq!(
        stats2.chunk_byte_budget, expected_budget,
        "(soft - probe) / 2 formula"
    );
    // peak hash-table memory must not exceed soft_limit (the
    // architectural invariant — a single chunk's hash table is
    // strictly smaller than the in-flight chunk plus its expansion
    // headroom).
    assert!(
        stats2.peak_chunk_table_bytes <= big_budget.soft_limit() as usize,
        "peak chunk table {} must stay within soft_limit {}",
        stats2.peak_chunk_table_bytes,
        big_budget.soft_limit(),
    );
}

/// Hard-gate 4: BNL emits results in 10 K-record batches and polls
/// `should_abort` between them. Verified by producing enough output
/// to cross multiple batch boundaries and asserting on
/// `stats.batches_emitted`.
///
/// Strategy: a single hot key K shared by 200 build rows and 60
/// probe rows yields 200 × 60 = 12 000 join records per chunk, so
/// at least one batch boundary fires.
#[test]
fn test_bnl_result_batching() {
    let h = build_bnl_harness();
    let builds: Vec<Record> = (0..200i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![Value::Integer(99), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();
    let probes: Vec<Record> = (0..60i64)
        .map(|i| {
            record_for(
                &h.driver_schema,
                vec![Value::Integer(99), Value::String(format!("d-{i}").into())],
            )
        })
        .collect();
    let sp = spill_for_bnl(&h, &builds, &probes, 0, 2);

    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            builds,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
            },
            &mut stats,
        )
        .expect("BNL must produce output for hot-key test");
    });

    // 200 × 60 = 12 000 join rows; should cross at least one
    // 10 K boundary so batches_emitted ≥ 1.
    assert_eq!(output.len(), 12_000);
    assert!(
        stats.batches_emitted >= 1,
        "BNL must hit the {RESULT_BATCH_SIZE}-record batch boundary at least once; \
             got {} batches",
        stats.batches_emitted,
    );
}

/// Hard-gate 5: hard-limit abort surfaces E310 with the partition
/// index AND a positive HLL distinct-key estimate. The host RSS
/// trivially exceeds a 1-byte limit, so `should_abort` returns true
/// on the very first poll inside BNL.
#[test]
fn test_e310_hard_limit_abort() {
    if crate::pipeline::memory::rss_bytes().is_none() {
        // RSS measurement unavailable; should_abort() returns
        // false on this platform and the test cannot fire.
        return;
    }
    let h = build_bnl_harness();
    // Distinct keys in the build set so the HLL gives a non-zero
    // estimate (its small-range linear-counting branch reports
    // close to the true count when most registers are zero).
    let builds: Vec<Record> = (0..200i64)
        .map(|i| {
            record_for(
                &h.build_schema,
                vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
            )
        })
        .collect();
    let probes: Vec<Record> = (0..10i64)
        .map(|i| {
            record_for(
                &h.driver_schema,
                vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
            )
        })
        .collect();
    let sp = spill_for_bnl(&h, &builds, &probes, 7, 2);

    // 1-byte hard limit → should_abort fires immediately.
    let budget = MemoryArbitrator::with_policy(1, 1.0, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    let err = with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            builds,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
            },
            &mut stats,
        )
        .expect_err("1-byte hard limit must abort BNL")
    });

    let est = sp.distinct_sketch.estimate();
    assert!(est > 0, "HLL must report a positive distinct estimate");
    match &err {
        PipelineError::MemoryBudgetExceeded { source, detail, .. } => {
            assert_eq!(*source, BudgetCategory::Arena);
            let detail = detail.as_deref().unwrap_or("");
            assert!(
                detail.contains("partition 7"),
                "detail must include partition_id; got {detail:?}"
            );
            assert!(
                detail.contains(&est.to_string()),
                "detail must include approx distinct count {est}; got {detail:?}"
            );
        }
        other => {
            panic!("BNL hard-limit abort must surface MemoryBudgetExceeded; got {other:?}")
        }
    }
}
