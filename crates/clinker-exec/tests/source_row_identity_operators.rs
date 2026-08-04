use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_exec::aggregation::{
    AddRaw, AggregatorConfig, HashAggregator, SortRow, StreamingAggregator,
};
use clinker_exec::executor::SourceRowId;
use clinker_exec::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use clinker_exec::pipeline::spill::SpillWriter;
use clinker_plan::plan::{EntityRef, PlanNodeId};
use clinker_record::{Record, Schema, Value};
use cxl::eval::{EvalContext, ProgramEvaluator, StableEvalContext};
use cxl::parser::Parser;
use cxl::plan::{CompiledAggregate, extract_aggregates};
use cxl::resolve::pass::resolve_program;
use cxl::typecheck::pass::{AggregateMode, type_check_with_mode};
use cxl::typecheck::types::Type;
use cxl::typecheck::{Row, TypedProgram};
use indexmap::IndexMap;

struct AggregateFixture {
    compiled: Arc<CompiledAggregate>,
    typed: Arc<TypedProgram>,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    spill_schema: Arc<Schema>,
}

fn aggregate_fixture() -> AggregateFixture {
    let input_fields = [("k", Type::String)];
    let parsed = Parser::parse("emit k = k\nemit n = count(*)");
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors
    );
    let resolved = resolve_program(parsed.ast, &["k"], parsed.node_count).expect("resolve");
    let input_row = Row::closed(
        IndexMap::from([(
            cxl::typecheck::QualifiedField::bare("k"),
            input_fields[0].1.clone(),
        )]),
        cxl::lexer::Span::new(0, 0),
    );
    let group_by = vec!["k".to_string()];
    let typed = type_check_with_mode(
        resolved,
        &input_row,
        AggregateMode::GroupBy {
            group_by_fields: HashSet::from_iter(group_by.iter().cloned()),
        },
    )
    .expect("typecheck");
    let typed = Arc::new(typed);
    let compiled = Arc::new(
        extract_aggregates(&typed, &group_by, &["k".to_string()]).expect("extract aggregates"),
    );
    let output_schema = Arc::new(Schema::new(
        compiled
            .emits
            .iter()
            .map(|emit| emit.output_name.clone())
            .collect(),
    ));

    AggregateFixture {
        compiled,
        typed,
        input_schema: Arc::new(Schema::new(vec!["k".into()])),
        output_schema,
        spill_schema: Arc::new(Schema::new(vec!["k".into(), "__acc_state".into()])),
    }
}

fn hash_aggregator(
    fixture: &AggregateFixture,
    memory_budget: usize,
    spill_dir: Option<PathBuf>,
) -> HashAggregator {
    HashAggregator::new(AggregatorConfig {
        compiled: Arc::clone(&fixture.compiled),
        evaluator: ProgramEvaluator::new(Arc::clone(&fixture.typed), false),
        output_schema: Arc::clone(&fixture.output_schema),
        spill_schema: Arc::clone(&fixture.spill_schema),
        memory_budget,
        spill_dir,
        spill_compress: true,
        transform_name: "identity_aggregate".to_string(),
        consumer_handle: ConsumerHandle::new(),
        arbitrator: Arc::new(MemoryArbitrator::with_policy(
            0,
            0.8,
            0.70,
            MemoryArbitrator::default_policy(),
        )),
    })
}

fn record(schema: &Arc<Schema>, key: impl Into<Value>) -> Record {
    Record::new(Arc::clone(schema), vec![key.into()])
}

fn context<'a>(stable: &'a StableEvalContext, file: &'a Arc<str>, row: u64) -> EvalContext<'a> {
    EvalContext::test_with_file(stable, file, row)
}

fn count_and_identity(rows: &[SortRow], key: &str) -> (i64, SourceRowId) {
    let (row, identity) = rows
        .iter()
        .find(|(row, _)| row.get("k") == Some(&Value::from(key)))
        .unwrap_or_else(|| panic!("missing aggregate output for group {key:?}"));
    let count = match row.get("n") {
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected aggregate count: {other:?}"),
    };
    (count, *identity)
}

#[test]
fn aggregate_representative_resident_and_streaming_keep_typed_minimum() {
    let fixture = aggregate_fixture();
    let stable = StableEvalContext::test_default();
    let file: Arc<str> = Arc::from("aggregate.csv");
    let higher_source = SourceRowId::new(PlanNodeId::new(9), 7);
    let lower_source = SourceRowId::new(PlanNodeId::new(3), 7);
    let input = record(&fixture.input_schema, "same");

    let mut resident = hash_aggregator(&fixture, 16 * 1024 * 1024, None);
    resident
        .add_record(&input, higher_source, &context(&stable, &file, 7))
        .expect("resident add higher source");
    resident
        .add_record(&input, lower_source, &context(&stable, &file, 7))
        .expect("resident add lower source");
    let mut resident_rows = Vec::new();
    resident
        .finalize(&context(&stable, &file, 7), &mut resident_rows)
        .expect("resident finalize");

    let mut streaming = StreamingAggregator::<AddRaw>::new_for_raw(
        Arc::clone(&fixture.compiled),
        ProgramEvaluator::new(Arc::clone(&fixture.typed), false),
        Arc::clone(&fixture.output_schema),
        "identity_aggregate",
    );
    let mut streaming_rows = Vec::new();
    streaming
        .add_record(
            &input,
            higher_source,
            &context(&stable, &file, 7),
            &mut streaming_rows,
        )
        .expect("streaming add higher source");
    streaming
        .add_record(
            &input,
            lower_source,
            &context(&stable, &file, 7),
            &mut streaming_rows,
        )
        .expect("streaming add lower source");
    streaming
        .flush(&context(&stable, &file, 7), &mut streaming_rows)
        .expect("streaming flush");

    assert_eq!(
        count_and_identity(&resident_rows, "same"),
        (2, lower_source)
    );
    assert_eq!(
        count_and_identity(&streaming_rows, "same"),
        (2, lower_source)
    );
}

#[test]
fn aggregate_representative_maximum_identity_survives_resident_streaming_and_spill() {
    let fixture = aggregate_fixture();
    let stable = StableEvalContext::test_default();
    let file: Arc<str> = Arc::from("aggregate.csv");
    let maximum = SourceRowId::maximum();
    let max_record = record(&fixture.input_schema, "maximum");

    let mut resident = hash_aggregator(&fixture, 16 * 1024 * 1024, None);
    resident
        .add_record(&max_record, maximum, &context(&stable, &file, u64::MAX))
        .expect("resident add maximum identity");
    let mut resident_rows = Vec::new();
    resident
        .finalize(&context(&stable, &file, u64::MAX), &mut resident_rows)
        .expect("resident finalize");

    let mut streaming = StreamingAggregator::<AddRaw>::new_for_raw(
        Arc::clone(&fixture.compiled),
        ProgramEvaluator::new(Arc::clone(&fixture.typed), false),
        Arc::clone(&fixture.output_schema),
        "identity_aggregate",
    );
    let mut streaming_rows = Vec::new();
    streaming
        .add_record(
            &max_record,
            maximum,
            &context(&stable, &file, u64::MAX),
            &mut streaming_rows,
        )
        .expect("streaming add maximum identity");
    streaming
        .flush(&context(&stable, &file, u64::MAX), &mut streaming_rows)
        .expect("streaming flush");

    let spill_root = tempfile::tempdir().expect("spill tempdir");
    let mut spilled = hash_aggregator(&fixture, 10_000, Some(spill_root.path().to_path_buf()));
    spilled
        .add_record(&max_record, maximum, &context(&stable, &file, u64::MAX))
        .expect("spill add maximum identity");
    for ordinal in 1..=500u64 {
        let row = record(&fixture.input_schema, format!("other_{ordinal:04}"));
        spilled
            .add_record(
                &row,
                SourceRowId::new(PlanNodeId::new(1), ordinal),
                &context(&stable, &file, ordinal),
            )
            .expect("spill pressure add");
    }
    assert!(
        !spilled.spill_files().is_empty(),
        "fixture must force at least one aggregate spill"
    );
    let mut spilled_rows = Vec::new();
    spilled
        .finalize(&context(&stable, &file, 500), &mut spilled_rows)
        .expect("spill finalize");

    assert_eq!(count_and_identity(&resident_rows, "maximum"), (1, maximum));
    assert_eq!(count_and_identity(&streaming_rows, "maximum"), (1, maximum));
    assert_eq!(count_and_identity(&spilled_rows, "maximum"), (1, maximum));
}

#[test]
fn combine_driver_identity_survives_resident_collect_fanout_and_spill_carriers() {
    let schema = Arc::new(Schema::new(vec!["driver".into()]));
    let first = SourceRowId::new(PlanNodeId::new(21), 7);
    let second = SourceRowId::new(PlanNodeId::new(22), 7);
    let drivers = [
        (record(&schema, "first"), first),
        (record(&schema, "second"), second),
    ];

    let resident: Vec<SourceRowId> = drivers.iter().map(|(_, identity)| *identity).collect();
    assert_eq!(resident, vec![first, second]);

    let fanout: Vec<(Record, SourceRowId)> = drivers
        .iter()
        .flat_map(|(driver, identity)| [(driver.clone(), *identity), (driver.clone(), *identity)])
        .collect();
    assert_eq!(
        fanout
            .iter()
            .map(|(_, identity)| *identity)
            .collect::<Vec<_>>(),
        vec![first, first, second, second]
    );

    let collected: Vec<(Record, SourceRowId)> = drivers
        .iter()
        .map(|(driver, identity)| (driver.clone(), *identity))
        .collect();
    assert_eq!(
        collected
            .iter()
            .map(|(_, identity)| *identity)
            .collect::<Vec<_>>(),
        vec![first, second]
    );

    let spill_dir = tempfile::tempdir().expect("combine spill tempdir");
    let mut writer =
        SpillWriter::<SourceRowId>::new(Arc::clone(&schema), Some(spill_dir.path()), true)
            .expect("open combine spill");
    for (driver, identity) in &fanout {
        writer
            .write_pair(driver, identity)
            .expect("write combine driver identity");
    }
    let spill = writer.finish().expect("finish combine spill");
    let reloaded = spill
        .reader()
        .expect("open combine spill reader")
        .map(|pair| pair.expect("read combine spill pair").1)
        .collect::<Vec<_>>();
    assert_eq!(reloaded, vec![first, first, second, second]);
}
