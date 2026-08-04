//! End-to-end coverage for declared source-type failures.
//!
//! These tests pin the boundary where decoded source values become typed
//! records. A rejected value must become exactly one record error before any
//! downstream node can observe a raw or substituted value.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clinker_bench_support::io::{SharedBuffer, slow_reader};
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
};
use clinker_exec::pipeline::schema_coerce::CoercingReader;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::source::multi_file::FileSlot;
use clinker_format::Column;
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::error::{DeclaredTypeFailure, FormatError};
use clinker_format::traits::FormatReader;
use clinker_plan::config::pipeline_node::OnUnmapped;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use clinker_record::{Record, Schema, Value};
use cxl::typecheck::Type;
use indexmap::IndexMap;
use rust_decimal::Decimal;

struct NativeReader {
    schema: Arc<Schema>,
    rows: std::vec::IntoIter<Result<Vec<Value>, FormatError>>,
}

impl FormatReader for NativeReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        match self.rows.next() {
            Some(Ok(values)) => Ok(Some(Record::new(Arc::clone(&self.schema), values))),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

fn native_coercer(
    input_columns: &[&str],
    rows: Vec<Result<Vec<Value>, FormatError>>,
    declarations: &[Column],
    pretyped: bool,
) -> CoercingReader {
    let schema = Arc::new(Schema::new(
        input_columns.iter().map(|name| Box::from(*name)).collect(),
    ));
    CoercingReader::new(
        Box::new(NativeReader {
            schema,
            rows: rows.into_iter(),
        }),
        declarations,
        OnUnmapped::Drop,
        "native",
        pretyped,
    )
    .expect("native fixture must initialize")
}

fn admit_native(declared_type: Type, value: Value) -> Result<Record, FormatError> {
    let declaration = [Column::bare("value", declared_type)];
    native_coercer(&["value"], vec![Ok(vec![value])], &declaration, false)
        .next_record()
        .map(|record| record.expect("native fixture must contain one row"))
}

fn reject_native(declaration: Column, value: Value) -> DeclaredTypeFailure {
    let original = value.clone();
    let error = native_coercer(&["value"], vec![Ok(vec![value])], &[declaration], false)
        .next_record()
        .expect_err("native value must violate its declared type");
    let FormatError::DeclaredType(failure) = error else {
        panic!("expected declared-type failure, got {error:?}");
    };
    assert_eq!(failure.original_value, original);
    *failure
}

fn native_values() -> Vec<Value> {
    let mut map = IndexMap::new();
    map.insert("key".into(), Value::Integer(9));
    vec![
        Value::Null,
        Value::Bool(true),
        Value::Integer(42),
        Value::Float(42.5),
        Value::Decimal(Decimal::new(425, 1)),
        Value::String("text".into()),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 7, 30).unwrap()),
        Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 7, 30)
                .unwrap()
                .and_hms_opt(6, 30, 0)
                .unwrap(),
        ),
        Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        Value::Map(Box::new(map)),
    ]
}

fn run(
    csv: &str,
    schema: &str,
    error_handling: &str,
) -> Result<(ExecutionReport, String), PipelineError> {
    let (result, output) = execute(csv, schema, error_handling);
    result.map(|report| (report, output.as_string()))
}

fn execute(
    csv: &str,
    schema: &str,
    error_handling: &str,
) -> (Result<ExecutionReport, PipelineError>, SharedBuffer) {
    execute_with_source_options(csv, schema, error_handling, "")
}

fn execute_with_source_options(
    csv: &str,
    schema: &str,
    error_handling: &str,
    source_options: &str,
) -> (Result<ExecutionReport, PipelineError>, SharedBuffer) {
    let yaml = format!(
        r#"
pipeline:
  name: source_type_errors
{error_handling}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
{schema}
{source_options}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#
    );
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            PathBuf::from("input.csv"),
            Box::new(Cursor::new(csv.as_bytes().to_vec())),
        )]),
    )]);
    execute_yaml(
        &yaml,
        readers,
        PipelineRunParams {
            execution_id: "source-type-test".into(),
            batch_id: "batch".into(),
            ..Default::default()
        },
    )
}

fn execute_yaml(
    yaml: &str,
    readers: SourceReaders,
    params: PipelineRunParams,
) -> (Result<ExecutionReport, PipelineError>, SharedBuffer) {
    let config = parse_config(yaml).expect("fixture must parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture must compile");
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(output.clone()) as _)]);
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params);
    (report, output)
}

fn continuing() -> &'static str {
    r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv"#
}

#[test]
fn declared_native_type_matrix() {
    let mut native_map = IndexMap::new();
    native_map.insert("key".into(), Value::String("value".into()));
    let date = chrono::NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
    let date_time = date.and_hms_opt(6, 30, 0).unwrap();
    let cases = [
        (Type::Null, Value::Null),
        (Type::Bool, Value::Bool(true)),
        (Type::Int, Value::Integer(i64::MAX)),
        (Type::Float, Value::Float(f64::MAX)),
        (Type::Decimal, Value::Decimal(Decimal::new(1234, 2))),
        (Type::String, Value::String("native".into())),
        (Type::Date, Value::Date(date)),
        (Type::DateTime, Value::DateTime(date_time)),
        (
            Type::Array,
            Value::Array(vec![Value::Integer(1), Value::String("two".into())]),
        ),
        (Type::Map, Value::Map(Box::new(native_map))),
        (Type::Numeric, Value::Integer(i64::MIN)),
        (Type::Numeric, Value::Float(-0.5)),
    ];

    for (declared_type, value) in cases {
        let record = admit_native(declared_type.clone(), value.clone())
            .unwrap_or_else(|error| panic!("{declared_type} rejected {value:?}: {error}"));
        assert_eq!(record.get("value"), Some(&value));
    }

    let missing_declaration = [Column::bare("value", Type::String)];
    let missing_error = native_coercer(&[], vec![Ok(Vec::new())], &missing_declaration, false)
        .next_record()
        .expect_err("a missing non-nullable string must reject as null");
    assert!(matches!(missing_error, FormatError::DeclaredType(_)));

    let nullable_string = Type::nullable(Type::String);
    for value in [Value::Null, Value::String(String::new().into())] {
        let record = admit_native(nullable_string.clone(), value.clone())
            .expect("nullable string must retain null and empty string distinctly");
        assert_eq!(record.get("value"), Some(&value));
    }

    let nullable_int = Type::nullable(Type::Int);
    let record = admit_native(nullable_int, Value::String(String::new().into()))
        .expect("empty text for a nullable non-string must become null");
    assert_eq!(record.get("value"), Some(&Value::Null));

    let multiple = Column {
        multiple: Some(true),
        ..Column::bare("value", Type::String)
    };
    let original_array = Value::Array(vec![
        Value::String("valid".into()),
        Value::Integer(7),
        Value::String("unreached".into()),
    ]);
    let failure = reject_native(multiple, original_array.clone());
    assert_eq!(failure.original_value, original_array);
    assert_eq!(
        failure.original_record.get("value"),
        Some(&failure.original_value)
    );
    assert!(failure.message.contains("element 2"), "{}", failure.message);
}

#[test]
fn declared_string_and_null_reject_wrong_native_values() {
    for value in native_values() {
        if !matches!(value, Value::String(_)) {
            let failure = reject_native(Column::bare("value", Type::String), value.clone());
            assert_eq!(failure.field, "value");
            assert_eq!(failure.column, 1);
            assert_eq!(failure.declared_type, "String");
            assert_eq!(failure.original_record.get("value"), Some(&value));
        }
        if !matches!(value, Value::Null) {
            let failure = reject_native(Column::bare("value", Type::Null), value.clone());
            assert_eq!(failure.field, "value");
            assert_eq!(failure.column, 1);
            assert_eq!(failure.declared_type, "Null");
            assert_eq!(failure.original_record.get("value"), Some(&value));
        }
    }
}

#[test]
fn declared_any_accepts_every_value() {
    for value in native_values() {
        let record = admit_native(Type::Any, value.clone())
            .unwrap_or_else(|error| panic!("Any rejected {value:?}: {error}"));
        assert_eq!(record.get("value"), Some(&value));
    }
}

#[test]
fn declared_type_edge_matrix() {
    let empty = Value::String(String::new().into());
    assert_eq!(
        admit_native(Type::String, empty.clone())
            .expect("empty string remains a string")
            .get("value"),
        Some(&empty)
    );
    reject_native(Column::bare("value", Type::Null), empty.clone());
    assert_eq!(
        admit_native(Type::nullable(Type::String), empty.clone())
            .expect("nullable string keeps empty text distinct from null")
            .get("value"),
        Some(&empty)
    );
    assert_eq!(
        admit_native(Type::nullable(Type::Int), empty)
            .expect("nullable non-string maps empty text to null")
            .get("value"),
        Some(&Value::Null)
    );

    let min = admit_native(Type::Int, Value::Float(i64::MIN as f64))
        .expect("the exactly representable integer minimum must be admitted");
    assert_eq!(min.get("value"), Some(&Value::Integer(i64::MIN)));
    reject_native(
        Column::bare("value", Type::Int),
        Value::Float(i64::MAX as f64),
    );
    let exactly_representable = 1_i64 << f64::MANTISSA_DIGITS;
    let exact_float = admit_native(Type::Float, Value::Integer(exactly_representable))
        .expect("the exact binary-float integer boundary must be admitted");
    assert_eq!(
        exact_float.get("value"),
        Some(&Value::Float(exactly_representable as f64))
    );
    reject_native(
        Column::bare("value", Type::Float),
        Value::Integer(exactly_representable + 1),
    );
    for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        reject_native(Column::bare("value", Type::Float), Value::Float(non_finite));
    }
    reject_native(
        Column::bare("value", Type::Numeric),
        Value::String("NaN".into()),
    );

    let decimal = Column {
        precision: Some(4),
        scale: Some(2),
        ..Column::bare("value", Type::Decimal)
    };
    let exact_decimal = native_coercer(
        &["value"],
        vec![Ok(vec![Value::String("99.99".into())])],
        std::slice::from_ref(&decimal),
        false,
    )
    .next_record()
    .expect("exact decimal must be readable")
    .expect("exact decimal fixture must contain a row");
    assert_eq!(
        exact_decimal.get("value"),
        Some(&Value::Decimal(Decimal::new(9999, 2)))
    );
    reject_native(decimal.clone(), Value::String("100.00".into()));
    reject_native(decimal, Value::String("1.234".into()));

    let alias = Column {
        source_name: Some("physical".into()),
        ..Column::bare("logical", Type::String)
    };
    let alias_error = native_coercer(
        &["physical"],
        vec![Ok(vec![Value::Integer(7)])],
        &[alias],
        false,
    )
    .next_record()
    .expect_err("an alias must apply exact admission to its physical value");
    let FormatError::DeclaredType(alias_failure) = alias_error else {
        panic!("expected alias declared-type failure, got {alias_error:?}");
    };
    assert_eq!(alias_failure.field, "logical");
    assert_eq!(alias_failure.column, 1);
    assert_eq!(alias_failure.original_value, Value::Integer(7));
    assert_eq!(
        alias_failure.original_record.get("physical"),
        Some(&Value::Integer(7))
    );

    let bytes = b"value\n\xff\n";
    let reader = CsvReader::from_reader(Cursor::new(bytes), CsvReaderConfig::default());
    let declaration = [Column::bare("value", Type::String)];
    let mut coercing = CoercingReader::new(
        Box::new(reader),
        &declaration,
        OnUnmapped::Drop,
        "encoded",
        false,
    )
    .expect("valid UTF-8 header must initialize");
    let encoding_error = coercing
        .next_record()
        .expect_err("invalid UTF-8 input must fail before string admission");
    assert!(
        matches!(encoding_error, FormatError::Charset(message) if message.contains("not valid UTF-8"))
    );

    let raw = format!("{}\n\tprivate-tail", "x".repeat(300));
    let csv = format!("id,quantity\nbad,\"{raw}\"\n");
    let (report, _) = run(
        &csv,
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        continuing(),
    )
    .expect("continuing strategy must retain the complete rejected payload");
    let entry = &report.dlq_entries[0];
    assert_eq!(
        entry.original_record.get("quantity"),
        Some(&Value::String(raw.clone().into()))
    );
    assert!(entry.error_message.contains("original_bytes=314"));
    assert_eq!(entry.error_message.lines().count(), 1);
    assert!(!entry.error_message.contains("private-tail"));
}

#[test]
fn coercion_malformed_value_enters_one_error_population() {
    let (report, output) = run(
        "id,quantity\nbad,not-an-int\ngood,42\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        continuing(),
    )
    .expect("continue must route the bad row and finish");

    assert_eq!(report.counters.dlq_count, 1);
    assert_eq!(report.dlq_entries.len(), 1);
    assert!(output.contains("good,42"), "valid row missing: {output}");
    assert!(
        !output.contains("not-an-int"),
        "raw failure leaked: {output}"
    );
}

#[test]
fn coercion_empty_values_follow_declared_nullability() {
    let (report, output) = run(
        "id,text,optional_count,required_count\n1,,,3\n2,,,\n",
        concat!(
            "        - { name: id, type: int }\n",
            "        - { name: text, type: string }\n",
            "        - { name: optional_count, type: { nullable: int } }\n",
            "        - { name: required_count, type: int }",
        ),
        continuing(),
    )
    .expect("continue must retain the valid empty values");

    assert_eq!(report.counters.dlq_count, 1);
    assert!(output.lines().any(|line| line == "1,,,3"), "{output}");
    assert!(
        !output.lines().any(|line| line.starts_with("2,")),
        "{output}"
    );
}

#[test]
fn coercion_precision_overflow_rounding_and_date_bounds_are_fail_closed() {
    let (report, output) = run(
        concat!(
            "id,whole,amount,day\n",
            "min,-9223372036854775808,12.34,0001-01-01\n",
            "max,9223372036854775807,99.99,9999-12-31\n",
            "overflow,9223372036854775808,12.34,2024-02-29\n",
            "rounding,7,12.345,2024-02-29\n",
            "bad-date,7,12.34,2023-02-29\n",
        ),
        concat!(
            "        - { name: id, type: string }\n",
            "        - { name: whole, type: int }\n",
            "        - { name: amount, type: decimal, precision: 8, scale: 2 }\n",
            "        - { name: day, type: date }",
        ),
        continuing(),
    )
    .expect("continue must preserve valid boundary rows");

    assert_eq!(report.counters.dlq_count, 3);
    assert!(
        output.lines().any(|line| line.starts_with("min,")),
        "{output}"
    );
    assert!(
        output.lines().any(|line| line.starts_with("max,")),
        "{output}"
    );
    assert!(!output.contains("overflow"), "{output}");
    assert!(!output.contains("rounding"), "{output}");
    assert!(!output.contains("bad-date"), "{output}");
}

#[test]
fn coercion_rfc3339_datetimes_normalize_offsets_and_reject_invalid_values() {
    let (report, output) = run(
        concat!(
            "id,opened_at\n",
            "utc,2026-01-31T08:27:00Z\n",
            "offset,2026-01-31T10:57:00+02:30\n",
            "fractional,2026-01-31T08:27:00.123456789Z\n",
            "malformed,2026-02-30T08:27:00Z\n",
            "out-of-range,10000-01-01T00:00:00Z\n",
            "padded,\" 2026-01-31T08:27:00Z\"\n",
        ),
        concat!(
            "        - { name: id, type: string }\n",
            "        - { name: opened_at, type: date_time }",
        ),
        continuing(),
    )
    .expect("continue must retain valid RFC 3339 datetimes");

    assert_eq!(report.counters.dlq_count, 3);
    assert_eq!(report.dlq_entries.len(), 3);
    assert!(
        output.lines().any(|line| line == "utc,2026-01-31T08:27:00"),
        "{output}",
    );
    assert!(
        output
            .lines()
            .any(|line| line == "offset,2026-01-31T08:27:00"),
        "offset was not normalized to UTC: {output}",
    );
    assert!(
        output
            .lines()
            .any(|line| line == "fractional,2026-01-31T08:27:00.123456789"),
        "{output}",
    );
    for rejected in ["malformed", "out-of-range", "padded"] {
        assert!(!output.contains(rejected), "rejected row leaked: {output}");
    }
}

#[test]
fn threshold_allows_exact_ratio_and_aborts_just_above() {
    let at_boundary = r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
  type_error_threshold: 0.5"#;
    let (report, _) = run(
        "id,quantity\ngood,1\nbad,nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        at_boundary,
    )
    .expect("an exact threshold ratio does not exceed the limit");
    assert_eq!(report.counters.dlq_count, 1);

    let (report, _) = run(
        "id,quantity\ngood-a,1\ngood-b,2\nbad,nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        at_boundary,
    )
    .expect("a ratio below the threshold does not exceed the limit");
    assert_eq!(report.counters.dlq_count, 1);

    let error = run(
        "id,quantity\ngood,1\nbad-a,nope\nbad-b,still-nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        at_boundary,
    )
    .expect_err("the next type error raises the ratio above the limit");
    assert!(
        error.to_string().contains("type error threshold"),
        "{error}"
    );

    let zero = r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
  type_error_threshold: 0.0"#;
    let error = run(
        "id,quantity\nbad,nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        zero,
    )
    .expect_err("the first type error exceeds a zero threshold");
    assert!(
        error.to_string().contains("(1/1 source records)"),
        "{error}"
    );

    let one = r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
  type_error_threshold: 1.0"#;
    let (report, _) = run(
        "id,quantity\nbad-a,nope\nbad-b,still-nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        one,
    )
    .expect("a threshold of one admits an all-error population");
    assert_eq!(report.counters.dlq_count, 2);

    let fail_fast_one = one.replace("continue", "fail_fast");
    run(
        "id,quantity\nbad,nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        &fail_fast_one,
    )
    .expect_err("fail_fast aborts before a permissive threshold can continue");
}

#[test]
fn ordered_attempt_staging_preserves_complete_population_identity_and_payload() {
    let handling = r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
  type_error_threshold: 0.5"#;
    let (result, output) = execute_with_source_options(
        "id,quantity\nb,2\nbad,not-an-int\na,1\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        handling,
        "      sort_order: [id]",
    );

    let report = result.expect(
        "the ordered file's complete 1/3 rejected population must be admitted before effects",
    );
    assert_eq!(report.dlq_entries.len(), 1);
    let rejected = &report.dlq_entries[0];
    assert_eq!(rejected.source_row.ordinal(), 2);
    assert_eq!(
        rejected.original_record.get("id"),
        Some(&Value::String("bad".into()))
    );
    assert_eq!(
        rejected.original_record.get("quantity"),
        Some(&Value::String("not-an-int".into()))
    );

    let output = output.as_string();
    let rows = output.lines().skip(1).collect::<Vec<_>>();
    assert_eq!(rows, vec!["a,1", "b,2"]);
}

fn ordered_threshold_handling() -> &'static str {
    r#"error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
  type_error_threshold: 0.1"#
}

fn ten_attempt_csv(rejected: usize) -> String {
    let mut csv = String::from("id,quantity\n");
    for ordinal in 1..=10 {
        if ordinal <= rejected {
            csv.push_str(&format!("bad-{ordinal},not-an-int\n"));
        } else {
            csv.push_str(&format!("good-{ordinal},{ordinal}\n"));
        }
    }
    csv
}

#[test]
fn ordered_threshold_uses_complete_attempt_population() {
    let schema = "        - { name: id, type: string }\n        - { name: quantity, type: int }";
    let (at_boundary, output) = execute_with_source_options(
        &ten_attempt_csv(1),
        schema,
        ordered_threshold_handling(),
        "      sort_order: [id]",
    );
    let report = at_boundary.expect("the complete ordered 1/10 population must be admitted");
    assert_eq!(report.per_source_record_counts.get("src"), Some(&10));
    assert_eq!(report.counters.dlq_count, 1);
    assert_eq!(output.as_string().lines().skip(1).count(), 9);

    let (just_over, output) = execute_with_source_options(
        &ten_attempt_csv(2),
        schema,
        ordered_threshold_handling(),
        "      sort_order: [id]",
    );
    let error = just_over.expect_err("the complete ordered 2/10 population must abort");
    assert!(
        error.to_string().contains("(2/10 source records)"),
        "{error}"
    );
    assert!(
        output.as_string().is_empty(),
        "a rejected population must produce no downstream output"
    );
}

#[test]
fn ordered_threshold_exact_boundary_matches_fused_transform_and_merge() {
    let transform_yaml = format!(
        r#"
pipeline:
  name: ordered_threshold_transform
{}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: quantity, type: int }}
      sort_order: [id]
  - type: transform
    name: projected
    input: src
    config:
      cxl: |
        emit id = id
        emit quantity = quantity
  - type: output
    name: out
    input: projected
    config:
      name: out
      type: csv
      path: output.csv
"#,
        ordered_threshold_handling()
    );
    let transform_readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            PathBuf::from("input.csv"),
            Box::new(Cursor::new(ten_attempt_csv(1).into_bytes())),
        )]),
    )]);
    let (transform, output) = execute_yaml(
        &transform_yaml,
        transform_readers,
        PipelineRunParams::default(),
    );
    let transform = transform.expect("fused Transform must admit the same exact 1/10 boundary");
    assert_eq!(transform.per_source_record_counts.get("src"), Some(&10));
    assert_eq!(transform.counters.dlq_count, 1);
    assert_eq!(output.as_string().lines().skip(1).count(), 9);

    let merge_yaml = format!(
        r#"
pipeline:
  name: ordered_threshold_merge
{}
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: quantity, type: int }}
      sort_order: [id]
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: quantity, type: int }}
      sort_order: [id]
  - type: merge
    name: merged
    inputs: [src_a, src_b]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: output.csv
"#,
        ordered_threshold_handling()
    );
    let merge_readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("a.csv"),
                Box::new(Cursor::new(ten_attempt_csv(1).into_bytes())),
            )]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("b.csv"),
                Box::new(Cursor::new(b"id,quantity\nz,99\n".to_vec())),
            )]),
        ),
    ]);
    let (merge, output) = execute_yaml(&merge_yaml, merge_readers, PipelineRunParams::default());
    let merge = merge.expect("fused Merge must apply each ordered population exactly once");
    assert_eq!(merge.per_source_record_counts.get("src_a"), Some(&10));
    assert_eq!(merge.per_source_record_counts.get("src_b"), Some(&1));
    assert_eq!(merge.counters.dlq_count, 1);
    assert_eq!(output.as_string().lines().skip(1).count(), 10);
}

#[test]
fn ordered_attempt_spill_parity() {
    fn run_with_limit(limit: &str) -> (ExecutionReport, String) {
        let yaml = format!(
            r#"
pipeline:
  name: ordered_attempt_spill
  memory: {{ limit: "{limit}", backpressure: spill }}
{}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: quantity, type: int }}
      on_unmapped:
        mode: reject
      sort_order: [id]
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#,
            ordered_threshold_handling()
        );
        let mut csv = String::from("id,quantity\n");
        for ordinal in (1..=180).rev() {
            if ordinal % 20 == 0 {
                csv.push_str(&format!("id-{ordinal:03},not-an-int\n"));
            } else {
                csv.push_str(&format!("id-{ordinal:03},{ordinal}\n"));
            }
        }
        let readers: SourceReaders = HashMap::from([(
            "src".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("input.csv"),
                Box::new(Cursor::new(csv.into_bytes())),
            )]),
        )]);
        let (report, output) = execute_yaml(&yaml, readers, PipelineRunParams::default());
        (
            report.expect("ordered attempt spill run"),
            output.as_string(),
        )
    }

    let resident = run_with_limit("1G");
    let spilled = run_with_limit("48K");
    assert_eq!(resident.0.counters.dlq_count, spilled.0.counters.dlq_count);
    assert_eq!(
        resident.0.per_source_record_counts,
        spilled.0.per_source_record_counts
    );
    assert_eq!(resident.1, spilled.1);
}

#[test]
fn ordered_attempt_interrupt_cleanup() {
    let yaml = r#"
pipeline:
  name: ordered_attempt_interrupt
  memory: { limit: "1K", backpressure: spill }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: payload, type: string }
      sort_order: [id]
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#;
    let mut csv = String::from("id,payload\n");
    for ordinal in (1..=400).rev() {
        csv.push_str(&format!("{ordinal},{}\n", "x".repeat(256)));
    }
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        SourceInput::Files(vec![FileSlot::new(
            PathBuf::from("input.csv"),
            slow_reader(&csv, Duration::from_millis(1)),
        )]),
    )]);
    let token = ShutdownToken::detached();
    let params = PipelineRunParams {
        shutdown_token: Some(token.clone()),
        ..Default::default()
    };
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (report, output) = execute_yaml(yaml, readers, params);
        let _ = done_tx.send((report, output.as_string()));
    });

    std::thread::sleep(Duration::from_millis(30));
    token.request();
    let prompt = done_rx.recv_timeout(Duration::from_millis(200));
    let completed_promptly = prompt.is_ok();
    let (report, output) = match prompt {
        Ok(result) => result,
        Err(_) => done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("ordered ingest must eventually unwind after shutdown"),
    };
    worker.join().expect("ordered ingest worker panicked");

    assert!(
        completed_promptly,
        "ordered staging ignored the shutdown request"
    );
    let report = report.expect("shutdown is a graceful interruption");
    assert!(report.interrupted);
    assert_eq!(report.cumulative_spill_bytes, 0);
    assert!(
        report
            .per_stage_spill_bytes
            .values()
            .all(|bytes| *bytes == 0)
    );
    assert!(
        output.is_empty(),
        "interrupted staged rows leaked to output"
    );
}

#[test]
fn strategies_fail_closed_and_continue_keeps_full_dlq_evidence() {
    let schema = "        - { name: id, type: string }\n        - { name: quantity, type: int }";
    let csv = "id,quantity\nbad,not-an-int\ngood,42\n";

    let (failed, fail_fast_output) = execute(csv, schema, "");
    let message = failed
        .expect_err("fail_fast must abort on the first declared-type error")
        .to_string();
    assert!(message.contains("E126"), "{message}");
    assert!(!fail_fast_output.as_string().contains("not-an-int"));

    let (report, output) = run(csv, schema, continuing()).expect("continue strategy finishes");
    assert_eq!(report.dlq_entries.len(), 1);
    let entry = &report.dlq_entries[0];
    assert_eq!(
        entry.original_record.get("quantity"),
        Some(&clinker_record::Value::String("not-an-int".into()))
    );
    assert_eq!(
        entry.triggering_value,
        Some(clinker_record::Value::String("not-an-int".into()))
    );
    assert!(entry.error_message.contains("source=\"src\""));
    assert!(entry.error_message.contains("file=\"input.csv\""));
    assert!(entry.error_message.contains("row=1"));
    assert!(entry.error_message.contains("column=2"));
    assert!(entry.error_message.contains("field=\"quantity\""));
    assert!(entry.error_message.contains("declared_type=Int"));
    assert!(!output.contains("not-an-int"), "{output}");
    assert!(output.contains("good,42"), "{output}");
}

#[test]
fn diagnostic_preview_respects_255_256_257_byte_boundaries_without_raw_tail() {
    let schema = "        - { name: quantity, type: int }";
    for length in [255usize, 256] {
        let value = "x".repeat(length);
        let csv = format!("quantity\n{value}\n");
        let (report, _) = run(&csv, schema, continuing()).expect("continue routes error");
        let diagnostic = &report.dlq_entries[0].error_message;
        assert!(
            diagnostic.contains(&format!("preview=\"{value}\" original_bytes={length}")),
            "{diagnostic}"
        );
    }

    let value = "x".repeat(257);
    let csv = format!("quantity\n{value}\n");
    let (report, _) = run(&csv, schema, continuing()).expect("continue routes error");
    let diagnostic = &report.dlq_entries[0].error_message;
    let expected = format!("preview=\"{}…\" original_bytes=257", "x".repeat(253));
    assert!(diagnostic.contains(&expected), "{diagnostic}");
    assert!(
        !diagnostic.contains(&value),
        "full raw value escaped the bounded preview: {diagnostic}"
    );
}

#[test]
fn diagnostic_preview_is_single_line_and_escapes_control_and_delimiter_tokens() {
    let value = "bad\r\n\t\\[]{}<>|\u{0085}\u{202e}";
    let csv = format!("quantity\n\"{value}\"\n");
    let (report, _) = run(
        &csv,
        "        - { name: quantity, type: int }",
        continuing(),
    )
    .expect("continue routes error");
    let diagnostic = &report.dlq_entries[0].error_message;

    assert_eq!(diagnostic.lines().count(), 1, "{diagnostic:?}");
    for token in [
        "\\r",
        "\\n",
        "\\t",
        "\\\\",
        "\\u{005B}",
        "\\u{005D}",
        "\\u{007B}",
        "\\u{007D}",
        "\\u{003C}",
        "\\u{003E}",
        "\\u{007C}",
        "\\u{0085}",
        "\\u{202E}",
    ] {
        assert!(
            diagnostic.contains(token),
            "missing {token:?}: {diagnostic:?}"
        );
    }
    assert!(!diagnostic.contains('\u{202e}'), "{diagnostic:?}");
}

#[test]
fn strategy_fail_fast_aborts_without_emitting_a_replacement() {
    let (result, output) = execute(
        "id,quantity\nbad,not-an-int\ngood,42\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        "",
    );
    let error = result.expect_err("fail_fast must abort on the first declared-type error");
    let message = error.to_string();
    assert!(message.contains("[E126]"), "{message}");
    assert!(message.contains("source=\"src\""), "{message}");
    assert!(message.contains("file=\"input.csv\""), "{message}");
    assert!(message.contains("row=1"), "{message}");
    assert!(message.contains("column=2"), "{message}");
    assert!(message.contains("declared_type=Int"), "{message}");
    assert!(!output.as_string().contains("not-an-int"));
}

#[test]
fn strategy_continue_preserves_full_dlq_evidence_only() {
    let strategy = "continue";
    let error_handling =
        format!("error_handling:\n  strategy: {strategy}\n  dlq:\n    path: rejected.csv");
    let (report, output) = run(
        "id,quantity,context\nbad,not-an-int,full evidence\ngood,42,ok\n",
        concat!(
            "        - { name: id, type: string }\n",
            "        - { name: quantity, type: int }\n",
            "        - { name: context, type: string }",
        ),
        &error_handling,
    )
    .unwrap_or_else(|error| panic!("{strategy} must finish through the DLQ: {error}"));

    assert_eq!(report.counters.dlq_count, 1, "{strategy}");
    let entry = &report.dlq_entries[0];
    assert_eq!(entry.source_row.ordinal(), 1, "{strategy}");
    assert_eq!(entry.source_name.as_ref(), "src", "{strategy}");
    assert_eq!(entry.triggering_field.as_deref(), Some("quantity"));
    assert_eq!(
        entry.original_record.get("quantity"),
        Some(&clinker_record::Value::String("not-an-int".into()))
    );
    assert_eq!(
        entry.original_record.get("context"),
        Some(&clinker_record::Value::String("full evidence".into()))
    );
    assert!(!output.contains("not-an-int"), "{strategy}: {output}");
    assert!(output.contains("good,42,ok"), "{strategy}: {output}");
}

#[test]
fn strategy_continuing_type_error_requires_an_explicit_dlq() {
    let error = run(
        "id,quantity\nbad,nope\n",
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        "error_handling:\n  strategy: continue",
    )
    .expect_err("continuing a rejected typed row without a DLQ is unsafe");
    assert!(error.to_string().contains("requires `error_handling.dlq`"));
}

#[test]
fn preview_is_bounded_without_leaking_the_raw_coercion_message() {
    let raw = "x".repeat(400);
    let csv = format!("id,quantity\nbad,{raw}\n");
    let (report, _) = run(
        &csv,
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        continuing(),
    )
    .expect("continue must retain full evidence only in the DLQ record");
    let entry = &report.dlq_entries[0];
    assert_eq!(
        entry.original_record.get("quantity"),
        Some(&clinker_record::Value::String(raw.clone().into()))
    );
    assert!(entry.error_message.contains("original_bytes=400"));
    assert!(entry.error_message.contains('…'));
    assert!(
        !entry.error_message.contains(&raw),
        "the unbounded raw input leaked outside the DLQ record"
    );
}

#[test]
fn preview_escapes_controls_bidi_delimiters_and_backslashes() {
    let raw = "a\t\u{0085}\u{202e}[x]\\z";
    let csv = format!("id,quantity\nbad,\"{raw}\"\n");
    let (report, _) = run(
        &csv,
        "        - { name: id, type: string }\n        - { name: quantity, type: int }",
        continuing(),
    )
    .expect("continue must route the escaped diagnostic");
    let message = &report.dlq_entries[0].error_message;
    assert!(message.contains("\\t"), "{message}");
    assert!(message.contains("\\u{0085}"), "{message}");
    assert!(message.contains("\\u{202E}"), "{message}");
    assert!(message.contains("\\u{005B}x\\u{005D}"), "{message}");
    assert!(message.contains("\\\\z"), "{message}");
    assert!(!message.contains('\u{202e}'), "{message}");
}
