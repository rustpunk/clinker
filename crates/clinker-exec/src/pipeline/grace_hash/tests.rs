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
use crate::pipeline::memory::walk::walk_test_support::unregistered_consumer_id;

/// The row id `crate::test_support::with_build_row_ids` gives the build
/// fixture record at `index`, for fixtures that add or spill build records
/// one at a time.
fn build_row(index: usize) -> RecordOrder {
    RecordOrder::new(
        <clinker_plan::plan::PlanNodeId as clinker_plan::plan::EntityRef>::new(1),
        index as u64 + 1,
    )
}
use clinker_record::SchemaBuilder;
use clinker_record::owned_storage::SharedStorage;
use cxl::ast::Statement;
use cxl::lexer::Span as CxlSpan;
use cxl::parser::Parser;
use cxl::resolve::pass::resolve_program;
use cxl::typecheck::pass::type_check;
use cxl::typecheck::row::{QualifiedField, Row};

fn schema_with(cols: &[&str]) -> SharedStorage<Schema> {
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

thread_local! {
    static NEXT_SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The next build arrival position on this test's thread, so the build rows
/// a test adds rise in arrival order as the build input would deliver them.
fn fresh_seq() -> crate::pipeline::combine::BuildSeq {
    NEXT_SEQ.with(|next| {
        let seq = next.get();
        next.set(seq + 1);
        crate::pipeline::combine::BuildSeq(seq)
    })
}

/// `records` with the row id [`build_row`] gives each and their build
/// arrival positions, in the order given.
fn with_build_ids(
    records: Vec<Record>,
) -> Vec<(Record, RecordOrder, crate::pipeline::combine::BuildSeq)> {
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| (record, build_row(index), fresh_seq()))
        .collect()
}

fn record_for(schema: &SharedStorage<Schema>, values: Vec<Value>) -> Record {
    Record::new(schema.clone(), values)
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
/// largest-Building eviction loop takes effect) without the hard-limit
/// check (`MemoryArbitrator::check_hard_limit`) refusing, which would
/// short-circuit the build phase.
/// `limit` is 10 GiB so RSS-vs-hard-limit always falls inside;
/// `spill_threshold_pct` is set so soft limit = 1 KiB, well below
/// any host's resident set.
fn tiny_budget() -> MemoryArbitrator {
    MemoryArbitrator::with_policy(
        10 * 1024 * 1024 * 1024,
        0.000_001,
        0.000_000_5,
        Box::new(NoOpPolicy),
    )
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
        "grace_test",
    );
    let schema = schema_with(&["k"]);
    // Deposit a record and force a spill so a file actually exists.
    let rec = record_for(&schema, vec![Value::Integer(7)]);
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
    exec.add_build_record(rec, build_row(0), fresh_seq(), 0, &budget)
        .unwrap();
    exec.spill_partition(0, &budget).unwrap();
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
            "grace_test",
        );
        let schema = schema_with(&["k"]);
        let rec = record_for(&schema, vec![Value::Integer(99)]);
        let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
        exec.add_build_record(rec, build_row(0), fresh_seq(), 0, &budget)
            .unwrap();
        exec.spill_partition(0, &budget).unwrap();
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
        "grace_test",
    );
    let budget = tiny_budget();
    for i in 0..256i64 {
        let rec = record_for(
            &schema,
            vec![Value::Integer(i), Value::String(format!("row-{i}").into())],
        );
        // Synthetic hash: distribute uniformly across 16 partitions.
        let hash = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        exec.add_build_record(rec, build_row(0), fresh_seq(), hash, &budget)
            .unwrap();
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
    let mut exec =
        GraceHashExecutor::new(4, dir.path(), consumer_handle.clone(), true, "grace_test");

    // 1 GiB hard limit → 800 MiB soft limit, above any host's test RSS,
    // so the RSS arm of `should_spill` cannot trip. Register the handle so
    // its charged bytes flow into `sum_consumer_usage`.
    let budget =
        MemoryArbitrator::with_policy(1024 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
    budget
        .register_consumer(
            Arc::new(GraceHashConsumer::new(consumer_handle.clone())),
            consumer_handle.clone(),
            clinker_plan::runtime_error::ConsumerLabel {
                node: "grace_test".to_string(),
                surface: clinker_plan::runtime_error::MemorySurface::JoinBuildSide,
            },
        )
        .expect("a fresh handle registers");
    let soft = budget.soft_limit();
    assert!(
        budget.peak_rss().is_none_or(|rss| rss < soft),
        "test invariant: real RSS must stay under the 800 MiB soft limit so only \
         the charged-byte arm can trip the spill"
    );

    // Pre-charge the handle above the soft limit but below the 1 GiB hard
    // limit (so `should_spill` trips while the hard-limit check does not). One
    // real build record then carries enough Building bytes for
    // `spill_largest_building` to have a victim.
    consumer_handle.set_bytes(soft + 1);
    let rec = record_for(
        &schema,
        vec![Value::Integer(0), Value::String("row-0".into())],
    );
    exec.add_build_record(rec, build_row(0), fresh_seq(), 0, &budget)
        .unwrap();

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
        "grace_test",
    );
    let budget = tiny_budget();

    // Send 64 records all to partition 0 (top 4 bits = 0). Use a
    // hash with high-bits zero.
    let probe_partition_hash: u64 = 0x0000_0000_0000_1234;
    for i in 0..64i64 {
        let rec = record_for(&schema, vec![Value::Integer(i)]);
        exec.add_build_record(
            rec,
            build_row(0),
            fresh_seq(),
            probe_partition_hash,
            &budget,
        )
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
        .probe_record(
            &probe,
            0.into(),
            &[Value::Integer(999)],
            probe_partition_hash,
        )
        .unwrap();
    assert!(matches!(outcome, ProbeOutcome::Spilled));

    exec.finalize_probe_spills(&budget).unwrap();
    match &exec.partitions[0] {
        PartitionState::OnDisk {
            probe_files,
            probe_count,
            ..
        } => {
            assert_eq!(probe_files.len(), 1);
            assert_eq!(*probe_count, 1);
            assert!(probe_files[0].path().exists());
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
fn combine_driver_identity_survives_grace_hash_partition_pair() {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use clinker_plan::plan::{EntityRef, PlanNodeId};
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
                    driver_schema.clone(),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                RecordOrder::new(PlanNodeId::new(21 + i as usize), 7),
            )
        })
        .collect();
    let builds: Vec<Record> = (0..10i64)
        .map(|i| {
            Record::new(
                build_schema.clone(),
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
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));

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
    // Seed the build node's plan-time row count from file metadata so the
    // membership filter is sized up front and recorded as estimate-sized
    // (the Bloom is skipped when no plan estimate exists). 12 KiB at the
    // shared ~1 KiB/row divisor seeds ~12 rows.
    stats_catalog
        .lock()
        .unwrap()
        .seed_row_count_from_bytes("products", Some(12 * 1024));
    let result = execute_combine_grace_hash(
        GraceHashExec {
            name: "grace_test",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: crate::test_support::with_build_row_ids(builds),
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
            on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
            max_output_rows: None,
            partition_bits: 4,
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget: &budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            consumer_id: unregistered_consumer_id(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
            stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
            build_input_charge: None,
            driver_input_charge: None,
        },
        crate::test_support::test_kernel_pool(),
    )
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

    for (record, identity) in &result {
        let driver_value = match &record.values()[1] {
            Value::String(value) => value.as_ref(),
            other => panic!("driver value not string: {other:?}"),
        };
        let driver_index: usize = driver_value
            .strip_prefix("d-")
            .expect("driver value prefix")
            .parse()
            .expect("driver value index");
        assert_eq!(
            *identity,
            RecordOrder::new(PlanNodeId::new(21 + driver_index), 7),
            "grace-hash output must retain the exact typed driver identity"
        );
    }

    // Plane B lifecycle: the join routed all three build-side sketches
    // into the catalog under (products, products).
    let catalog = stats_catalog.lock().unwrap();
    let stats = catalog
        .column("products", "products")
        .expect("grace hash must record build-side column statistics");

    // Distinct: 10 distinct build keys over a 1024-register HLL lands at or
    // very near 10.
    let distinct = stats
        .distinct
        .expect("grace hash must record a build-side distinct estimate");
    assert!(
        (8..=12).contains(&distinct.0),
        "10 distinct build keys should estimate near 10; got {}",
        distinct.0
    );

    // Heavy hitters: each of the 10 keys appears once, so all survive the
    // 256-counter sketch; the report caps at the top 16, so all 10 list.
    let hitters = stats
        .heavy_hitters
        .as_ref()
        .expect("grace hash must record build-side heavy hitters");
    assert_eq!(
        hitters.len(),
        10,
        "all 10 single-occurrence keys survive the Misra-Gries sketch; got {hitters:?}"
    );
    for (_, count) in hitters {
        assert_eq!(
            *count, 1,
            "each key appears once, so its lower-bound count is 1"
        );
    }
    // The representative values are the actual single-component build join
    // keys (`bk` = "0".."9"), not opaque hashes.
    let hitter_values: std::collections::HashSet<String> =
        hitters.iter().map(|(v, _)| v.to_string()).collect();
    for i in 0..10 {
        assert!(
            hitter_values.contains(&i.to_string()),
            "heavy-hitter list must carry the real join-key value {i}; got {hitter_values:?}"
        );
    }

    // Membership: a Bloom filter sized from the distinct estimate, recorded
    // as estimate-sized with a positive bit/probe budget.
    let bloom = stats
        .bloom
        .expect("grace hash must record a build-side membership filter");
    assert!(
        bloom.sized_from_estimate,
        "Bloom was sized from the HLL estimate"
    );
    assert!(bloom.bit_count > 0 && bloom.hash_count > 0);
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
                    driver_schema.clone(),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                (i as u64).into(),
            )
        })
        .collect();
    let builds: Vec<Record> = (0..32i64)
        .map(|i| {
            Record::new(
                build_schema.clone(),
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
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);

    // Big hard limit so the hard-limit check never refuses; tiny spill
    // threshold so should_spill fires immediately (process RSS
    // far exceeds 1 KiB on any host). This decouples spill
    // activation from build abort.
    let budget = MemoryArbitrator::with_policy(
        10 * 1024 * 1024 * 1024,
        0.000_001,
        0.000_000_5,
        Box::new(NoOpPolicy),
    );

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
    let result = execute_combine_grace_hash(
        GraceHashExec {
            name: "grace_spill_test",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: crate::test_support::with_build_row_ids(builds),
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
            on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
            max_output_rows: None,
            partition_bits: 4,
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget: &budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            consumer_id: unregistered_consumer_id(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
            stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
            build_input_charge: None,
            driver_input_charge: None,
        },
        crate::test_support::test_kernel_pool(),
    )
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
/// the disk. The hard memory limit is large (so the hard-limit check
/// never refuses); only the disk quota can cause this combine to
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
                    driver_schema.clone(),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                (i as u64).into(),
            )
        })
        .collect();
    let builds: Vec<Record> = (0..512i64)
        .map(|i| {
            Record::new(
                build_schema.clone(),
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
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);

    // Memory hard limit huge so the hard-limit check never refuses; spill
    // threshold tiny so spills happen; disk quota tight so the
    // first partition flush trips it.
    let budget = MemoryArbitrator::with_policy(
        10 * 1024 * 1024 * 1024,
        0.000_001,
        0.000_000_5,
        Box::new(NoOpPolicy),
    );
    budget.set_max_spill_bytes(64).unwrap();

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
    let result = execute_combine_grace_hash(
        GraceHashExec {
            name: "grace_quota_test",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: crate::test_support::with_build_row_ids(builds),
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode: clinker_plan::config::pipeline_node::MatchMode::All,
            on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
            max_output_rows: None,
            partition_bits: 4,
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget: &budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            consumer_id: unregistered_consumer_id(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
            stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
            build_input_charge: None,
            driver_input_charge: None,
        },
        crate::test_support::test_kernel_pool(),
    );

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

/// Per-commit enforcement on the build-side eviction path: a 1-byte cap
/// trips `spill_largest_building` -> `spill_partition` at the FIRST
/// partition it evicts, aborting mid-build rather than after the whole
/// build stream has been drained (the phase-boundary bug). At the method
/// boundary the old code returned `Ok` on every record and only the
/// driver's phase-end tally could fail; now the commit itself fails.
#[test]
fn build_eviction_spill_commit_trips_disk_cap_mid_stream() {
    use crate::pipeline::grace_spill::{GraceSpillError, grace_spill_error};

    let schema = schema_with(&["k", "v"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-cap-build-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        4,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
        "join_build",
    );

    // Tiny soft limit so `should_spill` fires continuously; 1-byte cap so
    // the first eviction commit overshoots; hard limit huge so the
    // hard-limit check never refuses.
    let budget = tiny_budget();
    budget.set_max_spill_bytes(1).unwrap();

    let total = 4096usize;
    let mut fed = 0usize;
    let mut hit: Option<GraceSpillError> = None;
    for i in 0..total as i64 {
        let rec = record_for(
            &schema,
            vec![
                Value::Integer(i),
                Value::String(format!("row-{i:08}-pad-pad-pad").into()),
            ],
        );
        let hash = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        match exec.add_build_record(rec, build_row(0), fresh_seq(), hash, &budget) {
            Ok(()) => fed += 1,
            Err(e) => {
                hit = Some(e);
                break;
            }
        }
    }

    let err = hit.expect("1-byte cap must abort the build-side eviction");
    match &err {
        GraceSpillError::CapExceeded {
            attempted,
            cap,
            cumulative,
        } => {
            assert!(
                *attempted > 0,
                "the overflowing commit must report its size"
            );
            assert_eq!(*cap, 1, "cap must equal the configured quota");
            assert!(
                *cumulative > *cap,
                "cumulative ({cumulative}) must exceed cap ({cap})"
            );
        }
        other => panic!("build eviction overshoot must surface CapExceeded; got {other:?}"),
    }

    // Per-commit, not phase-boundary: the abort landed before the whole
    // 4096-record stream was consumed.
    assert!(
        fed < total,
        "cap must abort mid-stream, not after the full feed ({fed} of {total})"
    );

    // Exactly one commit was charged — the crossing eviction — and it was
    // attributed to the executor's stage name. A phase-boundary check
    // would instead have let every partition's eviction accumulate first.
    let per_stage = budget.per_stage_spill_bytes();
    assert_eq!(
        per_stage.len(),
        1,
        "only the single crossing commit should be charged, got {per_stage:?}"
    );
    assert_eq!(
        budget.cumulative_spill_bytes(),
        per_stage["join_build"],
        "cumulative must equal the single crossing commit's bytes"
    );

    // Field-destructure of the mapped E320 surface, mirroring the driver's
    // `grace_spill_error` mapping.
    match grace_spill_error(err, "join_build", "build add") {
        PipelineError::SpillCapExceeded {
            node,
            cap,
            attempted,
            current,
        } => {
            assert_eq!(node, "join_build");
            assert_eq!(cap, 1);
            assert!(attempted > 0);
            assert!(current > cap);
        }
        other => panic!("mapper must yield SpillCapExceeded; got {other:?}"),
    }
}

/// Per-commit enforcement on the build-side OnDisk immediate-write path:
/// once a partition is already spilled, each subsequent record streams to
/// its own file and must charge the cap on that commit. An exhausted cap trips
/// on the FIRST such record rather than accumulating a fresh file per row
/// until the phase ends.
#[test]
fn build_ondisk_immediate_write_commit_trips_disk_cap() {
    use crate::pipeline::grace_spill::{GraceSpillError, grace_spill_error};

    let schema = schema_with(&["k", "v"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-cap-ondisk-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        4,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
        "join_ondisk",
    );

    // A budget that does NOT auto-spill (soft limit ~8 GiB) so the test
    // controls exactly when a commit happens; the cap starts unlimited so
    // the manual pre-spill lands without tripping.
    let budget =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));

    // Top 4 bits select the partition under 4 hash-bits; 0x0…1 -> 0.
    let hash_p0 = 0x0000_0000_0000_0001u64;
    // Seed partition 0 and drive it OnDisk by hand — no cap trip yet.
    exec.add_build_record(
        record_for(
            &schema,
            vec![Value::Integer(0), Value::String("seed".into())],
        ),
        build_row(0),
        fresh_seq(),
        hash_p0,
        &budget,
    )
    .unwrap();
    exec.spill_partition(0, &budget).unwrap();
    assert!(
        matches!(exec.partitions[0], PartitionState::OnDisk { .. }),
        "manual spill must place partition 0 on disk"
    );
    let after_pre_spill = budget.cumulative_spill_bytes();
    assert!(after_pre_spill > 0, "pre-spill must have committed a file");

    // Clamp the cap to the already committed bytes. The next OnDisk record
    // partition streams straight to a fresh file and charges on commit,
    // which must trip immediately.
    budget.set_max_spill_bytes(after_pre_spill).unwrap();

    let total = 8usize;
    let mut fed = 0usize;
    let mut hit: Option<GraceSpillError> = None;
    for i in 1..=total as i64 {
        let rec = record_for(
            &schema,
            vec![Value::Integer(i), Value::String(format!("row-{i}").into())],
        );
        match exec.add_build_record(rec, build_row(0), fresh_seq(), hash_p0, &budget) {
            Ok(()) => fed += 1,
            Err(e) => {
                hit = Some(e);
                break;
            }
        }
    }

    let err = hit.expect("exhausted cap must abort the OnDisk immediate write");
    match &err {
        GraceSpillError::CapExceeded {
            attempted,
            cap,
            cumulative,
        } => {
            assert!(
                *attempted > 0,
                "the immediate-write commit must report its size"
            );
            assert_eq!(*cap, after_pre_spill);
            assert!(
                *cumulative > *cap,
                "cumulative ({cumulative}) must exceed cap ({cap})"
            );
            assert!(
                *cumulative > after_pre_spill,
                "the immediate-write commit must add to the pre-spill total"
            );
        }
        other => panic!("OnDisk immediate write overshoot must surface CapExceeded; got {other:?}"),
    }
    assert_eq!(
        fed, 0,
        "the FIRST OnDisk record must trip the cap, before the rest of the stream"
    );

    match grace_spill_error(err, "join_ondisk", "build write") {
        PipelineError::SpillCapExceeded {
            node,
            cap,
            attempted,
            current,
        } => {
            assert_eq!(node, "join_ondisk");
            assert_eq!(cap, after_pre_spill);
            assert!(attempted > 0);
            assert!(current > cap);
        }
        other => panic!("mapper must yield SpillCapExceeded; got {other:?}"),
    }
}

/// Per-commit enforcement on the probe-finalize path: each partition's
/// buffered probe writer is committed and charged in turn, so an exhausted cap
/// trips on the FIRST partition finalized and leaves later partitions'
/// writers unflushed — rather than finalizing every open writer and only
/// then checking the total.
#[test]
fn probe_finalize_spill_commit_trips_disk_cap_per_partition() {
    use crate::pipeline::grace_spill::{GraceSpillError, grace_spill_error};

    let schema = schema_with(&["k"]);
    let dir = tempfile::Builder::new()
        .prefix("gh-cap-probe-")
        .tempdir()
        .unwrap();
    let mut exec = GraceHashExecutor::new(
        4,
        dir.path(),
        crate::pipeline::memory::ConsumerHandle::new(),
        true,
        "join_probe",
    );
    let budget =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));

    // Top 4 bits select the partition: 0x0…1 -> 0, 0x1000… -> 1.
    let hash_p0 = 0x0000_0000_0000_0001u64;
    let hash_p1 = 0x1000_0000_0000_0000u64;

    // Drive partitions 0 and 1 OnDisk by hand (cap unlimited during build).
    exec.add_build_record(
        record_for(&schema, vec![Value::Integer(0)]),
        build_row(0),
        fresh_seq(),
        hash_p0,
        &budget,
    )
    .unwrap();
    exec.add_build_record(
        record_for(&schema, vec![Value::Integer(1)]),
        build_row(1),
        fresh_seq(),
        hash_p1,
        &budget,
    )
    .unwrap();
    exec.spill_partition(0, &budget).unwrap();
    exec.spill_partition(1, &budget).unwrap();
    assert!(matches!(exec.partitions[0], PartitionState::OnDisk { .. }));
    assert!(matches!(exec.partitions[1], PartitionState::OnDisk { .. }));

    // Route a probe record into each OnDisk partition — buffered in the
    // partition's probe writer, not yet committed to disk.
    assert!(matches!(
        exec.probe_record(
            &record_for(&schema, vec![Value::Integer(10)]),
            0.into(),
            &[Value::Integer(10)],
            hash_p0
        )
        .unwrap(),
        ProbeOutcome::Spilled
    ));
    assert!(matches!(
        exec.probe_record(
            &record_for(&schema, vec![Value::Integer(20)]),
            1.into(),
            &[Value::Integer(20)],
            hash_p1
        )
        .unwrap(),
        ProbeOutcome::Spilled
    ));

    // Clamp the cap; finalize must commit each probe file and trip on the
    // first partition, leaving the later partition's writer open.
    let existing_spill = budget.cumulative_spill_bytes();
    assert!(
        existing_spill > 0,
        "build partitions already own spill bytes"
    );
    budget.set_max_spill_bytes(existing_spill).unwrap();
    let err = exec
        .finalize_probe_spills(&budget)
        .expect_err("exhausted cap must abort probe finalize");
    match &err {
        GraceSpillError::CapExceeded {
            attempted,
            cap,
            cumulative,
        } => {
            assert!(
                *attempted > 0,
                "the finalized probe file must report its size"
            );
            assert_eq!(*cap, existing_spill);
            assert!(
                *cumulative > *cap,
                "cumulative ({cumulative}) must exceed cap ({cap})"
            );
        }
        other => panic!("probe finalize overshoot must surface CapExceeded; got {other:?}"),
    }

    // Per-commit early abort: exactly one probe writer was finalized (its
    // file committed) and the other partition's writer is still open,
    // untouched after the loop returned on the first crossing.
    let committed = exec
        .partitions
        .iter()
        .filter(|p| {
            matches!(
                p,
                PartitionState::OnDisk { probe_writer: None, probe_files, .. }
                    if !probe_files.is_empty()
            )
        })
        .count();
    let open = exec
        .partitions
        .iter()
        .filter(|p| {
            matches!(
                p,
                PartitionState::OnDisk {
                    probe_writer: Some(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        committed, 1,
        "only the crossing partition's probe file was committed"
    );
    assert_eq!(
        open, 1,
        "the later partition's probe writer stays open — finalize aborted mid-stream"
    );

    match grace_spill_error(err, "join_probe", "probe finalize") {
        PipelineError::SpillCapExceeded {
            node,
            cap,
            attempted,
            current,
        } => {
            assert_eq!(node, "join_probe");
            assert_eq!(cap, existing_spill);
            assert!(attempted > 0);
            assert!(current > cap);
        }
        other => panic!("mapper must yield SpillCapExceeded; got {other:?}"),
    }
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
        "grace_test",
    );
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy)); // never spills via budget
    let originals: Vec<Record> = (0..16i64)
        .map(|i| {
            record_for(
                &schema,
                vec![Value::Integer(i), Value::String(format!("v-{i}").into())],
            )
        })
        .collect();
    for (i, r) in originals.iter().enumerate() {
        exec.add_build_record(r.clone(), build_row(i), fresh_seq(), i as u64, &budget)
            .unwrap();
    }
    // Force-spill every partition.
    for idx in 0..exec.partitions.len() {
        let _ = exec.spill_partition(idx, &budget);
    }
    let spilled = exec.drain_spilled();
    let mut reloaded: Vec<(Record, RecordOrder)> = Vec::new();
    for sp in spilled {
        for path in &sp.build_files {
            let reader = GraceSpillReader::open(path, schema.clone()).unwrap();
            for r in reader {
                let (record, row, _) = r.unwrap();
                reloaded.push((record, row));
            }
        }
    }
    // Sort both by k to compare independent of partition ordering. Each
    // record comes back with the build row id it was added with.
    let mut by_k: Vec<(i64, String, RecordOrder)> = reloaded
        .iter()
        .map(|(r, row)| {
            let k = match r.get("k") {
                Some(Value::Integer(i)) => *i,
                _ => panic!("missing k"),
            };
            let v = match r.get("v") {
                Some(Value::String(s)) => s.to_string(),
                _ => panic!("missing v"),
            };
            (k, v, *row)
        })
        .collect();
    by_k.sort_by_key(|(k, _, _)| *k);
    let expected: Vec<(i64, String, RecordOrder)> = (0..16)
        .map(|i| (i, format!("v-{i}"), build_row(i as usize)))
        .collect();
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
    build_schema: SharedStorage<Schema>,
    driver_schema: SharedStorage<Schema>,
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
    output_schema: SharedStorage<Schema>,
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
            producer_port: None,
            row: clinker_plan::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            producer_port: None,
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
        build_schema: h.build_schema.clone(),
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
    for (index, r) in build_records.iter().enumerate() {
        bw.write_record(r, build_row(index), fresh_seq()).unwrap();
        // Feed the HLL via the build-side hash of the join key.
        let stable = cxl::eval::StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("test.csv");
        let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
        let keys = h.build_extractor.extract(&ctx, r).unwrap();
        sketch.add(hash_composite_key(&keys, &h.hash_state));
    }
    let (bpath, _b_written) = bw.finish().unwrap();
    let mut probe_files = Vec::new();
    if !probe_records.is_empty() {
        let mut pw: crate::pipeline::spill::SpillWriter<RecordOrder> =
            crate::pipeline::spill::SpillWriter::new(
                h.driver_schema.clone(),
                Some(h.spill_dir.path()),
                true,
            )
            .unwrap();
        for (ordinal, r) in probe_records.iter().enumerate() {
            pw.write_pair(r, &RecordOrder::from(ordinal as u64))
                .unwrap();
        }
        probe_files.push(pw.finish().unwrap());
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
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
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
    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
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

    // Budget with hard_limit huge (so the hard-limit check never refuses) and
    // soft_limit just below PROBE_BUFFER_RESERVATION (so the chunk
    // formula's saturating_sub bottoms out at zero and the `max(1)`
    // floor kicks in). spill_threshold_pct expresses soft as a
    // fraction of hard.
    let target_soft = (PROBE_BUFFER_RESERVATION as f64) / 2.0; // ~2 MB < reservation
    let budget = MemoryArbitrator::with_policy(
        u64::MAX,
        target_soft / (u64::MAX as f64),
        target_soft / (u64::MAX as f64) / 2.0,
        Box::new(NoOpPolicy),
    );
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;

    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
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
    // so the hard-limit check cannot refuse on RSS.
    let big_soft = (PROBE_BUFFER_RESERVATION as u64) * 8;
    let big_budget = MemoryArbitrator::with_policy(
        u64::MAX,
        (big_soft as f64) / (u64::MAX as f64),
        (big_soft as f64) / (u64::MAX as f64) / 2.0,
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
            with_build_ids(builds2),
            &mut body_eval2,
            &big_budget,
            &mut GraceEmitSink {
                records: &mut output2,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
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

/// Hard-gate 4: BNL emits results in 10 K-record batches and runs the
/// hard-limit check (`MemoryArbitrator::check_hard_limit`) between them.
/// Verified by producing enough output to cross multiple batch boundaries
/// and asserting on `stats.batches_emitted`.
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

    let budget = MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
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

/// The hash of `record`'s build-side join key, as the join partitions it.
fn build_key_hash(h: &BnlHarness, record: &Record) -> u64 {
    let ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
    let keys = h.build_extractor.extract(&ctx, record).unwrap();
    hash_composite_key(&keys, &h.hash_state)
}

/// A build record keyed `key`, and a probe record keyed `key`.
fn keyed_build(h: &BnlHarness, key: i64, tag: usize) -> Record {
    record_for(
        &h.build_schema,
        vec![
            Value::Integer(key),
            Value::String(format!("b-{key}-{tag}").into()),
        ],
    )
}

fn keyed_probe(h: &BnlHarness, key: i64) -> Record {
    record_for(
        &h.driver_schema,
        vec![
            Value::Integer(key),
            Value::String(format!("d-{key}").into()),
        ],
    )
}

/// Run the chunked loop over `sp`'s `builds` under a 1-byte hard limit,
/// which the host's resident memory always exceeds, and return the E310
/// report it fails with.
fn chunked_loop_over_a_one_byte_limit(
    h: &BnlHarness,
    sp: &SpilledPartition,
    builds: Vec<Record>,
) -> Box<clinker_plan::runtime_error::MemoryShortfallReport> {
    let budget = MemoryArbitrator::with_policy(1, 1.0, 0.70, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut body_eval: Option<ProgramEvaluator> = None;
    let err = with_reload_context(h, |rc| {
        bnl_fallback(
            rc,
            sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
            },
            &mut BnlStats::default(),
        )
        .expect_err("a 1-byte hard limit must stop the chunked loop")
    });
    match err {
        PipelineError::MemoryBudgetExceeded { report } => report,
        other => panic!("the chunked loop must stop with E310; got {other:?}"),
    }
}

/// Reload `sp` through the full spilled-partition path under a 1-byte
/// hard limit, which the host's resident memory always exceeds, and
/// return the E310 report it fails with.
fn reload_over_a_one_byte_limit(
    h: &BnlHarness,
    sp: SpilledPartition,
) -> Box<clinker_plan::runtime_error::MemoryShortfallReport> {
    let budget = MemoryArbitrator::with_policy(1, 1.0, 0.70, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut body_eval: Option<ProgramEvaluator> = None;
    let err = with_reload_context(h, |rc| {
        process_spilled_partition(
            rc,
            sp,
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
            },
        )
        .expect_err("a 1-byte hard limit must stop the reload")
    });
    match err {
        PipelineError::MemoryBudgetExceeded { report } => report,
        other => panic!("the reload must stop with E310; got {other:?}"),
    }
}

/// One join key carries every row of a spilled partition. No
/// repartitioning separates rows that share a key, so the reload falls
/// back to the chunked loop, and the E310 it stops with says the
/// partition holds about one distinct key and why the split cannot help.
#[test]
fn a_one_key_partition_that_stops_over_the_limit_reports_about_one_distinct_key() {
    if crate::pipeline::memory::rss_bytes().is_none() {
        // Without an RSS reading a 1-byte limit never trips the abort.
        return;
    }
    let h = build_bnl_harness();
    let key = 7_654_321;
    let builds: Vec<Record> = (0..200).map(|tag| keyed_build(&h, key, tag)).collect();
    let probes: Vec<Record> = (0..5).map(|_| keyed_probe(&h, key)).collect();
    let parent_bits = 2u8;
    let partition =
        PartitionAssigner::new(parent_bits).partition_for(build_key_hash(&h, &builds[0]));
    let sp = spill_for_bnl(&h, &builds, &probes, partition, parent_bits);

    let report = reload_over_a_one_byte_limit(&h, sp);
    assert_eq!(
        report.join_partition_distinct_keys,
        Some(1),
        "a partition of one key's rows holds about one distinct key: {report:?}"
    );
    let rendered = report.to_string();
    assert!(
        rendered.contains(
            "\n  join partition: about 1 distinct key; one key's rows cannot be split across \
             partitions, so repartitioning cannot make them fit"
        ),
        "{rendered}"
    );
    assert!(
        !rendered.contains("7654321"),
        "the report never prints the key: {rendered}"
    );
}

/// A spilled partition splits once by the join key; the half holding a
/// hot key cannot be split again, so the chunked loop runs on that half.
/// The E310 counts the distinct keys of that half, as the sketch
/// rebuilt for it during the split saw them, not those of the partition it
/// was split from.
#[test]
fn a_split_partition_reports_the_distinct_keys_of_the_half_that_stopped() {
    if crate::pipeline::memory::rss_bytes().is_none() {
        return;
    }
    let h = build_bnl_harness();
    let parent = PartitionAssigner::new(2);
    let child = parent.double().unwrap();
    let grandchild = child.double().unwrap();
    let hash_of = |key: i64| build_key_hash(&h, &keyed_build(&h, key, 0));

    // A hot key whose half is the first one the split recurses into.
    let hot = (0..10_000i64)
        .find(|&key| {
            let hash = hash_of(key);
            child.partition_for(hash) == parent.partition_for(hash) * 2
        })
        .expect("half of all keys land in the first half");
    let hot_hash = hash_of(hot);
    let partition = parent.partition_for(hot_hash);
    // Keys sharing the hot key's partition: ten that follow it into its
    // half but not into its quarter, and forty that land in the other half.
    let mut with_hot = Vec::new();
    let mut other_half = Vec::new();
    for key in (0..1_000_000i64).filter(|&key| key != hot) {
        if with_hot.len() == 10 && other_half.len() == 40 {
            break;
        }
        let hash = hash_of(key);
        if parent.partition_for(hash) != partition {
            continue;
        }
        if child.partition_for(hash) != child.partition_for(hot_hash) {
            if other_half.len() < 40 {
                other_half.push(key);
            }
        } else if grandchild.partition_for(hash) != grandchild.partition_for(hot_hash)
            && with_hot.len() < 10
        {
            with_hot.push(key);
        }
    }
    assert_eq!((with_hot.len(), other_half.len()), (10, 40));

    // 100 hot rows beside 10 + 40 single-row keys: the first split leaves
    // the hot half at 110 of 150 rows, under the irreducible share, so it
    // recurses; the hot half's own split leaves 100 of 110 in one quarter,
    // over it, so that half runs the chunked loop.
    let mut builds: Vec<Record> = (0..100).map(|tag| keyed_build(&h, hot, tag)).collect();
    builds.extend(with_hot.iter().map(|&key| keyed_build(&h, key, 0)));
    builds.extend(other_half.iter().map(|&key| keyed_build(&h, key, 0)));
    let hot_half: Vec<Record> = builds[..110].to_vec();
    let irreducible_share = 1.0 - SKEW_REDUCTION_THRESHOLD;
    assert!(
        hot_half.len() as f64 <= irreducible_share * builds.len() as f64
            && 100.0 > irreducible_share * hot_half.len() as f64,
        "the first split must recurse and the second must not"
    );

    let sp = spill_for_bnl(&h, &builds, &[], partition, 2);
    let report = reload_over_a_one_byte_limit(&h, sp);
    // The half holds 11 distinct keys; the partition it was split from
    // held 51. A 64-register estimate of 11 keys can lose a few to
    // register collisions but stays far below what 51 keys give.
    let estimate = report
        .join_partition_distinct_keys
        .expect("the stopped half carries its estimate");
    assert!(
        (4..=25).contains(&estimate),
        "the half's 11 keys, not the whole partition's 51: {report:?}"
    );
}

/// A partition deep in the split (eight partition bits) holding 200
/// distinct keys. Every key in it shares the hash bits that chose the
/// partition, so the estimate must be read from other bits of the hash: it
/// reports about 200 keys, never about one.
#[test]
fn a_deep_partition_of_many_keys_does_not_report_one_key() {
    if crate::pipeline::memory::rss_bytes().is_none() {
        return;
    }
    let h = build_bnl_harness();
    let bits = 8u8;
    let assigner = PartitionAssigner::new(bits);
    let hash_of = |key: i64| build_key_hash(&h, &keyed_build(&h, key, 0));
    let partition = assigner.partition_for(hash_of(0));
    let builds: Vec<Record> = (0..i64::MAX)
        .filter(|&key| assigner.partition_for(hash_of(key)) == partition)
        .take(200)
        .map(|key| keyed_build(&h, key, 0))
        .collect();
    let sp = spill_for_bnl(&h, &builds, &[], partition, bits);

    let report = chunked_loop_over_a_one_byte_limit(&h, &sp, builds);
    let estimate = report
        .join_partition_distinct_keys
        .expect("the stopped partition carries its estimate");
    assert!(
        (100..=400).contains(&estimate),
        "200 distinct keys estimate near 200, not {estimate}: {report:?}"
    );
}

/// Hard-gate 5: hard-limit abort surfaces E310 for the combine's join
/// build side with the partition's approximate distinct-key count. The
/// host RSS trivially exceeds a 1-byte limit, so the hard-limit check
/// refuses at its very first run inside BNL.
#[test]
fn test_e310_hard_limit_abort() {
    if crate::pipeline::memory::rss_bytes().is_none() {
        // RSS measurement unavailable; the hard-limit check's process-memory
        // arm cannot refuse on this platform and the test cannot fire.
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

    // 1-byte hard limit → the hard-limit check refuses immediately.
    let budget = MemoryArbitrator::with_policy(1, 1.0, 0.70, Box::new(NoOpPolicy));
    let mut output: Vec<(Record, RecordOrder)> = Vec::new();
    let mut stats = BnlStats::default();
    let mut body_eval: Option<ProgramEvaluator> = None;
    let err = with_reload_context(&h, |rc| {
        bnl_fallback(
            rc,
            &sp,
            with_build_ids(builds),
            &mut body_eval,
            &budget,
            &mut GraceEmitSink {
                records: &mut output,
                failures: &mut Vec::new(),
                name: "grace_test",
                max_output_rows: None,
            },
            &mut stats,
        )
        .expect_err("1-byte hard limit must abort BNL")
    });

    match &err {
        PipelineError::MemoryBudgetExceeded { report } => {
            assert_eq!(
                report.requester.as_ref().map(|label| &label.surface),
                Some(&clinker_plan::runtime_error::MemorySurface::JoinBuildSide),
                "the abort names the combine's join build side: {report:?}"
            );
            assert!(
                report.requested_bytes > 0,
                "the backstop reports how far past the limit the run was: {report:?}"
            );
            let est = report
                .join_partition_distinct_keys
                .expect("the abort carries the partition's distinct-key estimate");
            assert_eq!(
                est,
                sp.distinct_sketch.estimate(),
                "the figure is the partition's own sketch"
            );
            assert!(
                (100..=400).contains(&est),
                "200 distinct keys estimate near 200, not {est}: {report:?}"
            );
            let rendered = report.to_string();
            assert!(
                rendered.contains(&format!("\n  join partition: about {est} distinct keys\n")),
                "{rendered}"
            );
        }
        other => {
            panic!("BNL hard-limit abort must surface MemoryBudgetExceeded; got {other:?}")
        }
    }
}

/// Driver keys `0..GRACE_ORDER_KEYS`; each key has [`GRACE_ORDER_PER_KEY`]
/// build rows, delivered interleaved across keys so a key's rows are not
/// adjacent in the build input.
const GRACE_ORDER_KEYS: i64 = 8;
const GRACE_ORDER_PER_KEY: i64 = 3;

/// Run a body-less grace-hash join of drivers `0..GRACE_ORDER_KEYS` against
/// builds named `b-<key>-<n>`, where `n` is the row's position among its
/// key's build rows in arrival order, under `match_mode` and `budget`.
/// Returns the output records in emitted order.
fn run_grace_arrival_order(
    match_mode: clinker_plan::config::pipeline_node::MatchMode,
    budget: &MemoryArbitrator,
) -> Vec<Record> {
    run_grace_join(GRACE_ORDER_KEYS, match_mode, budget, None, None)
}

/// [`run_grace_arrival_order`]'s join over drivers `0..driver_keys`, each
/// key past `GRACE_ORDER_KEYS` a miss, with the inputs' charges handed to
/// the join.
fn run_grace_join(
    driver_keys: i64,
    match_mode: clinker_plan::config::pipeline_node::MatchMode,
    budget: &MemoryArbitrator,
    build_input_charge: Option<TransientNodeBufferReservation>,
    driver_input_charge: Option<TransientNodeBufferReservation>,
) -> Vec<Record> {
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{DecomposedPredicate, EqualityConjunct};
    use clinker_plan::plan::types::JoinSide;
    use cxl::eval::{EvalContext, StableEvalContext};

    let driver_schema = schema_with(&["dk", "v"]);
    let build_schema = schema_with(&["bk", "name"]);
    let drivers: Vec<(Record, RecordOrder)> = (0..driver_keys)
        .map(|i| {
            (
                Record::new(
                    driver_schema.clone(),
                    vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                ),
                (i as u64).into(),
            )
        })
        .collect();
    let builds: Vec<Record> = (0..GRACE_ORDER_PER_KEY)
        .flat_map(|n| (0..GRACE_ORDER_KEYS).map(move |k| (k, n)))
        .map(|(k, n)| {
            Record::new(
                build_schema.clone(),
                vec![
                    Value::Integer(k),
                    Value::String(format!("b-{k}-{n}").into()),
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
    for (input, field, side, index) in [
        ("orders", "dk", JoinSide::Probe, 0),
        ("orders", "v", JoinSide::Probe, 1),
        ("products", "bk", JoinSide::Build, 0),
        ("products", "name", JoinSide::Build, 1),
    ] {
        mapping_q.insert(
            clinker_plan::plan::row_type::QualifiedField::qualified(input, field),
            (side, index),
        );
    }
    let row = |cols: &[(&str, cxl::typecheck::Type)]| {
        let mut row_cols: indexmap::IndexMap<
            clinker_plan::plan::row_type::QualifiedField,
            cxl::typecheck::Type,
        > = indexmap::IndexMap::new();
        for (name, ty) in cols {
            row_cols.insert(
                clinker_plan::plan::row_type::QualifiedField::bare(*name),
                ty.clone(),
            );
        }
        clinker_plan::plan::row_type::Row::closed(row_cols, CxlSpan::new(0, 0))
    };
    let mut combine_inputs: indexmap::IndexMap<String, clinker_plan::plan::combine::CombineInput> =
        indexmap::IndexMap::new();
    combine_inputs.insert(
        "orders".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("orders"),
            producer_port: None,
            row: row(&[
                ("dk", cxl::typecheck::Type::Int),
                ("v", cxl::typecheck::Type::String),
            ]),
        },
    );
    combine_inputs.insert(
        "products".to_string(),
        clinker_plan::plan::combine::CombineInput {
            upstream_name: Arc::from("products"),
            producer_port: None,
            row: row(&[
                ("bk", cxl::typecheck::Type::Int),
                ("name", cxl::typecheck::Type::String),
            ]),
        },
    );
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

    let stable = StableEvalContext::test_default();
    let source_file: Arc<str> = Arc::from("test.csv");
    let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
    let combined_schema = SchemaBuilder::new()
        .with_field("dk")
        .with_field("v")
        .with_field("bk")
        .with_field("name")
        .build();
    let dir = tempfile::Builder::new()
        .prefix("gh-arrival-order-")
        .tempdir()
        .unwrap();
    let stats_catalog = fresh_stats_catalog();
    execute_combine_grace_hash(
        GraceHashExec {
            name: "grace_arrival_order",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: crate::test_support::with_build_row_ids(builds),
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode,
            on_miss: clinker_plan::config::pipeline_node::OnMiss::Skip,
            max_output_rows: None,
            partition_bits: 2,
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            consumer_id: unregistered_consumer_id(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
            stats_sink: test_stats_sink(&stats_catalog, "products", "products"),
            build_input_charge,
            driver_input_charge,
        },
        crate::test_support::test_kernel_pool(),
    )
    .expect("grace hash arrival-order run")
    .records
    .into_iter()
    .map(|(record, _)| record)
    .collect()
}

/// Each driver's output, as `(driver key, build name)` pairs in emitted
/// order, grouped by driver key.
fn grace_pairs_by_driver(records: &[Record]) -> std::collections::BTreeMap<i64, Vec<String>> {
    let mut by_driver: std::collections::BTreeMap<i64, Vec<String>> =
        std::collections::BTreeMap::new();
    for record in records {
        let Value::Integer(key) = record.values()[0] else {
            panic!("driver key is an int: {record:?}");
        };
        let Value::String(name) = &record.values()[3] else {
            panic!("build name is a string: {record:?}");
        };
        by_driver.entry(key).or_default().push(name.to_string());
    }
    by_driver
}

/// Grace-hash takes each key's build rows in arrival order: resident, `first`
/// picks the earliest and `all` emits them in arrival order; spilled, `all`
/// still emits them in arrival order.
///
/// The spilled run's budget keeps spilling at reload, so every partition
/// reloads through the block-nested-loop fallback, which decides `first`
/// once per build chunk rather than once per driver. That fallback's
/// per-chunk decisions are a separate defect, so the spilled run asserts
/// only `all`.
#[test]
fn grace_hash_candidates_follow_build_arrival_order_resident_and_spilled() {
    use clinker_plan::config::pipeline_node::MatchMode;
    let expected = |key: i64| -> Vec<String> {
        (0..GRACE_ORDER_PER_KEY)
            .map(|n| format!("b-{key}-{n}"))
            .collect()
    };
    let resident =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
    let first = grace_pairs_by_driver(&run_grace_arrival_order(MatchMode::First, &resident));
    let all = grace_pairs_by_driver(&run_grace_arrival_order(MatchMode::All, &resident));
    assert_eq!(first.len(), GRACE_ORDER_KEYS as usize, "every driver");
    for key in 0..GRACE_ORDER_KEYS {
        assert_eq!(
            first[&key],
            [format!("b-{key}-0")],
            "resident: first picks key {key}'s earliest build row"
        );
        assert_eq!(
            all[&key],
            expected(key),
            "resident: all emits key {key}'s build rows in arrival order"
        );
    }

    let spilled = grace_pairs_by_driver(&run_grace_arrival_order(MatchMode::All, &tiny_budget()));
    for key in 0..GRACE_ORDER_KEYS {
        assert_eq!(
            spilled[&key],
            expected(key),
            "spilled: all emits key {key}'s build rows in arrival order"
        );
    }
}

// ──────────────────────────────────────────────────────────────────
// The partition table as walk-owned state
// ──────────────────────────────────────────────────────────────────

/// Free capacity a foreign request is made short of, past the charged total.
const FOREIGN_FREE: u64 = 4 * 1024;

/// A grace join's build input charge ends once its build loop has moved
/// every build row into a partition, so the probe loop never runs with it
/// on the ledger: at the probe's first memory check the build input's
/// consumer is no longer registered and the charged total excludes its
/// charge, while the driver input's charge, still standing for the rows the
/// loop is probing, remains.
#[test]
fn a_grace_join_has_released_its_build_input_before_it_probes() {
    const BUILD_INPUT: u64 = 64 * 1024 * 1024;
    const DRIVER_INPUT: u64 = 1024 * 1024;
    let budget = Arc::new(MemoryArbitrator::with_policy(
        1024 * 1024 * 1024,
        0.80,
        0.70,
        Box::new(NoOpPolicy),
    ));
    budget.read_no_process_memory();
    let reserve = |bytes, node| {
        crate::executor::node_buffer::reserve_node_buffer_materialization(bytes, &budget, node)
            .expect("the input charge fits the ledger")
    };
    let build_input_charge = reserve(BUILD_INPUT, "products");
    let driver_input_charge = reserve(DRIVER_INPUT, "orders");
    let consumers_with_inputs = budget.consumer_count();
    let seen: Rc<RefCell<Option<(usize, u64)>>> = Rc::default();
    let records = {
        let seen = Rc::clone(&seen);
        with_probe_check_observer(
            move |arbitrator| {
                *seen.borrow_mut() =
                    Some((arbitrator.consumer_count(), arbitrator.charged_bytes()));
            },
            || {
                run_grace_join(
                    MEMORY_CHECK_INTERVAL as i64 + 1,
                    clinker_plan::config::pipeline_node::MatchMode::All,
                    &budget,
                    Some(build_input_charge),
                    Some(driver_input_charge),
                )
            },
        )
    };
    assert_eq!(
        records.len(),
        (GRACE_ORDER_KEYS * GRACE_ORDER_PER_KEY) as usize,
        "each keyed driver joins each of its builds; the rest miss"
    );
    let (consumers, charged) = (*seen.borrow()).expect("the probe loop reached a memory check");
    assert_eq!(
        consumers,
        consumers_with_inputs - 1,
        "at the probe's first check only the build input's consumer has left the registry"
    );
    assert!(
        (DRIVER_INPUT..BUILD_INPUT).contains(&charged),
        "the probe runs with the driver input charged and the build input's charge \
         gone: {charged} bytes charged"
    );
    assert_eq!(budget.charged_bytes(), 0, "every charge ends with the join");
}

/// Build rows keyed `0..GRACE_ORDER_KEYS`, [`GRACE_ORDER_PER_KEY`] per key,
/// delivered interleaved across keys, and one driver per key.
fn reclaim_join_inputs(h: &BnlHarness) -> (Vec<Record>, Vec<(Record, RecordOrder)>) {
    let builds = (0..GRACE_ORDER_PER_KEY)
        .flat_map(|n| (0..GRACE_ORDER_KEYS).map(move |k| (k, n)))
        .map(|(k, n)| keyed_build(h, k, n as usize))
        .collect();
    let drivers = (0..GRACE_ORDER_KEYS)
        .map(|k| (keyed_probe(h, k), (k as u64).into()))
        .collect();
    (builds, drivers)
}

/// Node `joined`'s grace consumer and its partition table, registered as
/// the Combine dispatch and [`execute_combine_grace_hash`] register them.
fn registered_partitions(
    arbitrator: &MemoryArbitrator,
    dir: &std::path::Path,
) -> (
    crate::pipeline::memory::ConsumerId,
    Arc<crate::pipeline::memory::ConsumerHandle>,
    GracePartitions,
) {
    let (id, handle) =
        register_grace_consumer(arbitrator, "joined").expect("a fresh handle registers");
    let partitions = GracePartitions::register(
        arbitrator,
        id,
        GraceHashExecutor::new(2, dir, Arc::clone(&handle), true, "joined"),
    )
    .expect("registered");
    (id, handle, partitions)
}

/// Add `builds` to `partitions` in arrival order.
fn add_builds(
    partitions: &GracePartitions,
    h: &BnlHarness,
    builds: &[Record],
    budget: &MemoryArbitrator,
) {
    for (index, record) in builds.iter().enumerate() {
        add_build(partitions, h, record, index, budget);
    }
}

/// Add build row `index` to `partitions`, placed by the hash of its join key
/// as the kernel's build loop places it.
fn add_build(
    partitions: &GracePartitions,
    h: &BnlHarness,
    record: &Record,
    index: usize,
    budget: &MemoryArbitrator,
) {
    let ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
    let keys = h.build_extractor.extract(&ctx, record).unwrap();
    partitions
        .add_build_record(
            record.clone(),
            build_row(index),
            crate::pipeline::combine::BuildSeq(index as u64),
            hash_composite_key(&keys, &partitions.hash_state()),
            budget,
        )
        .expect("build row added");
}

/// Finish `partitions`' build, probe `drivers` and reload every spilled
/// partition, as [`execute_combine_grace_hash`] does from its build on.
/// Returns the output records in emitted order.
fn finish_and_probe(
    partitions: &GracePartitions,
    h: &BnlHarness,
    drivers: Vec<(Record, RecordOrder)>,
    budget: &MemoryArbitrator,
) -> Vec<Record> {
    let ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
    partitions
        .finish_build(&h.build_extractor, &ctx, budget, &h.emit.name)
        .expect("build finished");
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
    let hash_state = partitions.hash_state();
    let mut records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut failures = Vec::new();
    let mut keys: Vec<Value> = Vec::new();
    for (driver, rn) in drivers {
        let row_ctx = ctx.with_row(rn.ordinal());
        let resolver = CombineResolver::new(&h.resolver_mapping, &driver, None);
        keys.clear();
        h.driver_extractor
            .extract_into(&row_ctx, &resolver, &mut keys)
            .expect("probe key");
        let hash = hash_composite_key(&keys, &hash_state);
        let outcome = partitions
            .probe_record(&driver, rn, &keys, hash)
            .expect("probe routed");
        if let ProbeOutcome::InMemory(probe) = outcome {
            emit_for_probe(
                &emit,
                &driver,
                rn,
                probe.matches(),
                None,
                &row_ctx,
                &mut GraceEmitSink {
                    records: &mut records,
                    failures: &mut failures,
                    name: &h.emit.name,
                    max_output_rows: None,
                },
            )
            .expect("matches emitted");
        }
    }
    partitions
        .finalize_probe_spills(budget)
        .expect("probe spills finalized");
    let spill_dir = partitions.spill_dir_path();
    let rc = ReloadContext {
        name: &h.emit.name,
        build_extractor: &h.build_extractor,
        driver_extractor: &h.driver_extractor,
        emit: &emit,
        ctx: &ctx,
        build_schema: h.build_schema.clone(),
        spill_dir: &spill_dir,
        spill_compress: true,
        hash_state: &hash_state,
    };
    let mut body_eval: Option<ProgramEvaluator> = None;
    for sp in partitions.drain_spilled() {
        process_spilled_partition(
            &rc,
            sp,
            &mut body_eval,
            budget,
            &mut GraceEmitSink {
                records: &mut records,
                failures: &mut failures,
                name: &h.emit.name,
                max_output_rows: None,
            },
        )
        .expect("spilled partition reloaded");
    }
    assert!(failures.is_empty(), "no output row failed");
    records.into_iter().map(|(record, _)| record).collect()
}

/// The join of [`reclaim_join_inputs`] through a partition table nothing
/// spills and no pass can reach: what an unspilled run emits.
fn unspilled_join(h: &BnlHarness) -> Vec<Record> {
    let budget =
        MemoryArbitrator::with_policy(10 * 1024 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
    let dir = tempfile::Builder::new()
        .prefix("gh-unspilled-")
        .tempdir()
        .unwrap();
    let partitions = GracePartitions::register(
        &budget,
        unregistered_consumer_id(),
        GraceHashExecutor::new(
            2,
            dir.path(),
            crate::pipeline::memory::ConsumerHandle::new(),
            true,
            "joined",
        ),
    )
    .expect("an operator built with no run registers nothing");
    let (builds, drivers) = reclaim_join_inputs(h);
    add_builds(&partitions, h, &builds, &budget);
    finish_and_probe(&partitions, h, drivers, &budget)
}

/// Each partition holding build rows, by index, and whether it is on disk.
fn partitions_holding_rows(partitions: &GracePartitions) -> Vec<(usize, bool)> {
    partitions
        .cell
        .borrow()
        .executor
        .partitions
        .iter()
        .enumerate()
        .filter_map(|(index, state)| match state {
            PartitionState::Building { records, .. } if !records.is_empty() => Some((index, false)),
            PartitionState::OnDisk { .. } => Some((index, true)),
            _ => None,
        })
        .collect()
}

/// A walk request another consumer makes for more than is free, while a
/// grace-hash Combine's build holds partitions resident, is granted by
/// spilling those partitions: the grace consumer's charge falls by their
/// bytes, each is on disk, the spill is recorded under the Combine's node,
/// and finishing the build and probing yields the rows an unspilled run
/// yields, each driver's in the same order.
#[test]
fn grace_partitions_spill_when_another_walk_request_falls_short() {
    use crate::pipeline::memory::walk::walk_test_support::{
        foreign_walk_request, with_test_walk_frame,
    };
    let h = build_bnl_harness();
    let expected = unspilled_join(&h);
    let resident_limit = 10 * 1024 * 1024 * 1024;
    let arbitrator = Arc::new(MemoryArbitrator::with_policy(
        resident_limit,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    ));
    let dir = tempfile::Builder::new()
        .prefix("gh-foreign-")
        .tempdir()
        .unwrap();
    with_test_walk_frame(&arbitrator, || {
        let (id, handle, partitions) = registered_partitions(&arbitrator, dir.path());
        let (builds, drivers) = reclaim_join_inputs(&h);
        add_builds(&partitions, &h, &builds, &arbitrator);
        let holding = partitions_holding_rows(&partitions);
        assert!(
            !holding.is_empty() && holding.iter().all(|(_, on_disk)| !on_disk),
            "the build holds its partitions resident: {holding:?}"
        );
        let building: u64 = partitions
            .cell
            .borrow()
            .executor
            .partitions
            .iter()
            .map(|state| state.building_bytes() as u64)
            .sum();
        let charged_before = handle.bytes();
        assert_eq!(charged_before, building);
        arbitrator
            .set_limit(arbitrator.charged_bytes() + FOREIGN_FREE)
            .expect("limit");
        let spilled_before = arbitrator
            .per_stage_spill_bytes()
            .get("joined")
            .copied()
            .unwrap_or(0);

        let grant = foreign_walk_request(&arbitrator, FOREIGN_FREE + building / 2)
            .expect("the pass spills the build's partitions and the request fits");
        assert_eq!(
            handle.bytes(),
            charged_before - building,
            "the charge falls by the spilled partitions' bytes"
        );
        assert_eq!(
            handle.reclaimable(),
            0,
            "with every partition on disk, a spill frees nothing more"
        );
        let after = partitions_holding_rows(&partitions);
        assert_eq!(
            after.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            holding.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            "the same partitions hold the build rows"
        );
        assert!(
            after.iter().all(|(_, on_disk)| *on_disk),
            "every partition that held rows is on disk: {after:?}"
        );
        assert!(
            arbitrator
                .per_stage_spill_bytes()
                .get("joined")
                .copied()
                .unwrap_or(0)
                > spilled_before,
            "the spill is recorded under the Combine's node"
        );
        drop(grant);
        arbitrator.set_limit(resident_limit).expect("limit");

        let joined = finish_and_probe(&partitions, &h, drivers, &arbitrator);
        let expected_pairs = grace_pairs_by_driver(&expected);
        assert_eq!(expected_pairs.len(), GRACE_ORDER_KEYS as usize);
        assert_eq!(joined.len(), expected.len());
        assert_eq!(
            grace_pairs_by_driver(&joined),
            expected_pairs,
            "a build a pass spilled joins as an unspilled build does"
        );
        drop(partitions);
        arbitrator.unregister_consumer(id);
    });
}

/// The grace consumer's figure is what spilling its building partitions
/// frees now: their bytes while the build runs, and 0 once the build has
/// finished, since the probe keeps every in-memory partition. A pass another
/// request starts after the build never asks the grace consumer, and its
/// partitions stay built.
#[test]
fn grace_partitions_after_the_build_are_not_reclaimable() {
    use crate::pipeline::memory::MemoryConsumer;
    use crate::pipeline::memory::walk::walk_test_support::{
        TestWalkOwned, foreign_walk_request, with_test_walk_frame,
    };
    let h = build_bnl_harness();
    let resident_limit = 10 * 1024 * 1024 * 1024;
    let arbitrator = Arc::new(MemoryArbitrator::with_policy(
        resident_limit,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    ));
    let dir = tempfile::Builder::new()
        .prefix("gh-pinned-")
        .tempdir()
        .unwrap();
    with_test_walk_frame(&arbitrator, || {
        let (id, handle, partitions) = registered_partitions(&arbitrator, dir.path());
        let consumer = GraceHashConsumer::new(Arc::clone(&handle));
        let (builds, _drivers) = reclaim_join_inputs(&h);
        let mut building = 0u64;
        for (index, record) in builds.iter().enumerate() {
            building += estimated_build_entry_bytes(record) as u64;
            add_build(&partitions, &h, record, index, &arbitrator);
            assert_eq!(
                consumer.reclaimable_bytes(),
                building,
                "the figure is the building partitions' bytes"
            );
        }
        assert!(building > 0);

        let ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
        partitions
            .finish_build(&h.build_extractor, &ctx, &arbitrator, &h.emit.name)
            .expect("build finished");
        assert_eq!(
            consumer.reclaimable_bytes(),
            0,
            "the probe keeps every in-memory partition"
        );
        assert_eq!(handle.bytes(), building, "the partitions stay charged");
        let all_ready = || {
            partitions
                .cell
                .borrow()
                .executor
                .partitions
                .iter()
                .all(|state| matches!(state, PartitionState::Ready(_)))
        };
        assert!(all_ready(), "every partition is built for the probe");

        let other = TestWalkOwned::register(&arbitrator, "other", (0..64).collect(), FOREIGN_FREE);
        arbitrator
            .set_limit(arbitrator.charged_bytes() + FOREIGN_FREE)
            .expect("limit");
        // Within the limit, so a round runs, but more than the free room and
        // the other owner's state together: only the grace partitions could
        // make up the rest, and the probe holds them.
        let shortfall = foreign_walk_request(&arbitrator, 2 * FOREIGN_FREE + building / 2)
            .expect_err("spilling the other owner leaves too little room");
        let report = shortfall.into_report(&arbitrator);
        let round = report.reclaim.as_ref().expect("the walk ran a round");
        assert_eq!(
            round.holders_asked,
            vec!["other".to_string()],
            "the round asks the other owner and never the grace consumer"
        );
        assert!(all_ready(), "the built partitions stay in memory");
        arbitrator.set_limit(resident_limit).expect("limit");
        drop(partitions);
        arbitrator.unregister_consumer(other.id);
        arbitrator.unregister_consumer(id);
    });
}

/// A grace partition's records are already charged to the grace consumer
/// while the build turns them into a table, so the hard-limit check counts
/// only what the build adds on top of them: the index and the key cache.
/// The run is placed so the table's records part is exactly what tips it
/// over the limit if they were counted a second time; no walk frame, so a
/// refusal would come at once.
#[test]
fn a_grace_partition_build_near_the_limit_does_not_count_its_records_twice() {
    let h = build_bnl_harness();
    // Far above what the test process holds: the check samples its memory.
    let limit = 10 * 1024 * 1024 * 1024;
    let arbitrator = MemoryArbitrator::with_policy(
        limit,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    );
    let dir = tempfile::Builder::new()
        .prefix("gh-charged-records-")
        .tempdir()
        .unwrap();
    let (id, handle, partitions) = registered_partitions(&arbitrator, dir.path());
    // One key, so every row lands in one partition and its table is the
    // only one with records.
    let builds: Vec<Record> = (0..2_000).map(|n| keyed_build(&h, 7, n)).collect();
    add_builds(&partitions, &h, &builds, &arbitrator);
    let charged_records = handle.bytes();
    assert!(charged_records > 0, "the partition's rows are charged");

    // The figures of the table the build will make, from the same rows.
    let held: Vec<Record> = {
        let cell = partitions.cell.borrow();
        let mut held = Vec::new();
        for state in &cell.executor.partitions {
            if let PartitionState::Building { records, .. } = state {
                held.extend(records.iter().map(|(record, _, _)| record.clone()));
            }
        }
        held
    };
    assert_eq!(held.len(), builds.len());
    let ctx = EvalContext::test_with_file(&h.stable, &h.source_file, 0);
    let ample = MemoryArbitrator::with_policy(
        limit,
        0.80,
        0.70,
        Box::new(crate::pipeline::memory::NoOpPolicy),
    );
    let records_part = (held.len() * std::mem::size_of::<Record>()
        + held.iter().map(Record::estimated_heap_size).sum::<usize>())
        as u64;
    let rows = held.len();
    let table = CombineHashTable::build(
        held,
        &h.build_extractor,
        &ctx,
        &ample,
        "joined",
        crate::pipeline::memory::ledger::Requester::governed(),
        Some(rows),
    )
    .expect("an ample build");
    let table_bytes = table.memory_bytes() as u64;
    drop(table);
    let added = table_bytes - records_part;
    assert!(records_part > 0 && added > 0, "{records_part} {added}");

    // Charge the rest of the run so the build's own additions fit with half
    // the records part to spare: counting the records again would carry the
    // run past the limit.
    let room = added + records_part / 2;
    let (filler, filler_handle) =
        register_grace_consumer(&arbitrator, "elsewhere").expect("a fresh handle registers");
    filler_handle.set_bytes(limit - arbitrator.charged_bytes() - room);
    assert_eq!(arbitrator.charged_bytes(), limit - room);

    partitions
        .finish_build(&h.build_extractor, &ctx, &arbitrator, &h.emit.name)
        .expect("the build adds only its index and key cache to what is charged");
    assert_eq!(
        handle.bytes(),
        charged_records,
        "the partition's rows stay charged once"
    );
    drop(partitions);
    filler_handle.set_bytes(0);
    arbitrator.unregister_consumer(filler);
    arbitrator.unregister_consumer(id);
}
