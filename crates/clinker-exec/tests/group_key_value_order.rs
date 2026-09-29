//! Records group by exact value, under the same rule the value order ties by.
//!
//! A group key is equal to another exactly when the two values tie in the one
//! value order: integers, floats and decimals by exact value (so `5` and `5.0`
//! are one group, and two integers above 2^53 stay apart), `-0.0` with `0.0`,
//! and every NaN, whatever its sign, as one group apart from the null group.
//! A group reports the value of its first-arriving row, so an integer column is
//! written as integers.
//!
//! The in-memory aggregate table compares keys, while its spilled merge and
//! the streaming aggregate compare byte keys, so each grouping here is checked
//! with ample memory, at a ledger capacity that makes the node spill, and
//! (for Aggregate) under the streaming strategy.
//!
//! NaN values and columns mixing integers with floats or decimals come from an
//! upstream Transform (`"NaN".to_float()` and its negation; an `if` whose
//! branches are an integer and a float or a decimal), because a typed Source
//! rejects non-finite floats and holds one type per column. A Transform's
//! output loses its Source's declared order and no author-level node restores
//! it, so those keys cannot reach a streaming Aggregate through a pipeline;
//! their streaming case drives `StreamingAggregator` with the same rows. The
//! large-integer and signed-zero keys come from a typed Source that declares
//! its order, so their streaming case runs as a pipeline.
//!
//! A hash Aggregate writes its groups in hash-table order, which differs
//! between two runs with ample memory, so its outputs are compared as sets of
//! lines. Cull and Reshape write in a deterministic order and are compared
//! byte for byte.

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

#[path = "common/memory_pressure.rs"]
mod memory_pressure;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::aggregation::{AddRaw, SortRow, StreamingAggregator};
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams, SourceRowId,
};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::{EntityRef, PlanNodeId};
use clinker_record::owned_storage::SharedStorage;
use clinker_record::{Record, Schema, Value};
use cxl::eval::{EvalContext, ProgramEvaluator, StableEvalContext};
use cxl::parser::Parser;
use cxl::plan::extract_aggregates;
use cxl::resolve::pass::resolve_program;
use cxl::typecheck::Row;
use cxl::typecheck::pass::{AggregateMode, type_check_with_mode};
use cxl::typecheck::types::Type;
use indexmap::IndexMap;
use memory_pressure::{assert_capacity_below_ample_peak, assert_spill_engaged};

/// The `memory.limit` every run is configured with; a spilling run is held to
/// a smaller ledger capacity through the test override.
const AMPLE_LIMIT: &str = "512M";

/// Ledger capacity of the spilling runs of the typed and mixed-number
/// Aggregate fixtures: 140 KiB (143,360 bytes).
///
/// With ample memory each fixture charges 176,852 bytes at its peak (177,204
/// for the large-integer input). Each completes at 116,262 bytes and is
/// refused at 104,635, where the Source's 16,384-byte document admission falls
/// short. 140 KiB lies between the two, and at it the hash table spills. The
/// fixtures are new, so there is no earlier limit to stay under; every run's
/// `memory.limit` is 512M.
const AGGREGATE_SPILL_CAPACITY: u64 = 140 * 1024;

/// Ledger capacity of the spilling NaN Aggregate run: 124 KiB (126,976
/// bytes). With ample memory it charges 136,680 bytes at its peak; it
/// completes at 117,185 bytes and is refused at 111,325, where the Source's
/// document admission falls short.
const NAN_AGGREGATE_SPILL_CAPACITY: u64 = 124 * 1024;

/// Ledger capacity of the spilling NaN Cull run: 464 KiB (475,136 bytes).
/// With ample memory it charges 547,320 bytes at its peak; it completes at
/// 423,505 bytes and is refused at 402,329 with E310 naming the Cull's
/// drop-decision state.
const NAN_CULL_SPILL_CAPACITY: u64 = 464 * 1024;

/// Ledger capacity of the spilling NaN Reshape run: 264 KiB (270,336 bytes).
/// With ample memory it charges 311,640 bytes at its peak; it completes at
/// 229,083 bytes and is refused at 217,628, where the Source's document
/// admission falls short.
const NAN_RESHAPE_SPILL_CAPACITY: u64 = 264 * 1024;

/// Distinct filler groups, so a node's state outgrows the spill capacities
/// above.
const FILLER_GROUPS: usize = 400;

const TWO_POW_53: i64 = 9_007_199_254_740_992;

// ---- running pipelines ----------------------------------------------------

/// One run's report and each Sink's bytes, by Sink name.
struct Run {
    report: ExecutionReport,
    outputs: HashMap<String, String>,
}

impl Run {
    fn out(&self, sink: &str) -> &str {
        &self.outputs[sink]
    }
}

/// Run a one-Source pipeline over `csv` fed to the Source `src`, capturing
/// every Sink named in `sinks`, held to `capacity` bytes of ledger when one
/// is given.
fn try_run(
    yaml: &str,
    csv: &str,
    sinks: &[&str],
    capacity: Option<u64>,
) -> Result<Run, PipelineError> {
    let mut config = parse_config(yaml).expect("fixture parses");
    let context = CompileContext::default();
    resource_fixtures::add_csv_workspace(&mut config, &context);
    let plan = config.compile(&context).expect("fixture compiles");
    let readers = HashMap::from([(
        "src".to_string(),
        resource_fixtures::predecoded_csv_source(&config, &context, "src", &[("in.csv", csv)]),
    )]);
    let buffers: Vec<(String, SharedBuffer)> = sinks
        .iter()
        .map(|name| (name.to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(name, buf)| {
            (
                name.clone(),
                Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "group-key-value-order".to_string(),
        batch_id: "b".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)?;
    let outputs = buffers
        .into_iter()
        .map(|(name, buf)| (name, buf.as_string()))
        .collect();
    Ok(Run { report, outputs })
}

fn run(yaml: &str, csv: &str, sinks: &[&str], capacity: Option<u64>) -> Run {
    try_run(yaml, csv, sinks, capacity).expect("the pipeline runs to completion")
}

/// Run `yaml` with ample memory and at `capacity`, asserting that the
/// capacity run spilled `node`'s state and that the ample run's charged peak
/// lies above the capacity. Returns `(ample, spilled)`.
fn run_ample_and_spilled(
    yaml: &str,
    csv: &str,
    sinks: &[&str],
    capacity: u64,
    node: &str,
) -> (Run, Run) {
    let ample = run(yaml, csv, sinks, None);
    let spilled = run(yaml, csv, sinks, Some(capacity));
    assert_spill_engaged(&spilled.report);
    assert_capacity_below_ample_peak(capacity, &ample.report);
    assert!(
        spilled
            .report
            .per_stage_spill_bytes_written
            .get(node)
            .is_some_and(|bytes| *bytes > 0),
        "`{node}` itself must spill at {capacity} bytes: {:?}",
        spilled.report.per_stage_spill_bytes_written
    );
    (ample, spilled)
}

/// The Sink's lines, sorted, so a comparison does not depend on emit order.
fn sorted_lines(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().map(str::to_string).collect();
    lines.sort();
    lines
}

fn sorted(mut lines: Vec<String>) -> Vec<String> {
    lines.sort();
    lines
}

// ---- Aggregate fixtures ---------------------------------------------------

/// The Sinks every non-NaN Aggregate fixture writes: CSV and newline-delimited
/// JSON (JSON has no NaN, so the NaN fixture writes CSV only).
const AGG_SINKS: &[&str] = &["csv", "json"];

fn agg_sinks_yaml(input: &str) -> String {
    format!(
        r#"
  - type: sink
    name: csv
    input: {input}
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: sink
    name: json
    input: {input}
    config:
      name: json
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: true
"#
    )
}

/// A typed Source `src` with one column `k` of `k_type` and an Aggregate
/// `grouped` counting rows per `k`. When `streaming` is set, the Source
/// declares ascending order on `k` and the Aggregate requires the streaming
/// strategy.
fn typed_aggregate_yaml(name: &str, k_type: &str, streaming: bool) -> String {
    let sort_order = if streaming {
        "\n      sort_order:\n        - { field: k, order: asc, null_order: first }"
    } else {
        ""
    };
    let strategy = if streaming {
        "\n      strategy: streaming"
    } else {
        ""
    };
    format!(
        r#"
pipeline:
  name: {name}
  memory: {{ limit: "{AMPLE_LIMIT}", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv{sort_order}
      schema:
        - {{ name: k, type: {k_type} }}
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [k]{strategy}
      cxl: |
        emit n = count(*)
{sinks}"#,
        sinks = agg_sinks_yaml("grouped")
    )
}

/// A Source `src` of string columns `id`, `kind`, `num`, a Transform `vals`
/// emitting `id` and `v = <v_expr>`, and an Aggregate `grouped` counting rows
/// per `v`.
fn transform_aggregate_yaml(name: &str, v_expr: &str) -> String {
    format!(
        r#"
pipeline:
  name: {name}
  memory: {{ limit: "{AMPLE_LIMIT}", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: kind, type: string }}
        - {{ name: num, type: string }}
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit id = id
        emit v = {v_expr}
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [v]
      cxl: |
        emit n = count(*)
{sinks}"#,
        sinks = agg_sinks_yaml("grouped")
    )
}

/// An integer or a float, by `kind`: the branches unify to a float column
/// whose rows still hold integers.
const INT_OR_FLOAT: &str = r#"if kind == "f" then num.to_float() else num.to_int()"#;

/// An integer or a decimal, by `kind`: the branches unify to a decimal column
/// whose rows still hold integers.
const INT_OR_DECIMAL: &str = r#"if kind == "d" then num.to_decimal() else num.to_int()"#;

/// [`FILLER_GROUPS`] small integers, then 2^53 once and 2^53 + 1 twice, in
/// ascending order so the same rows can feed a streaming Aggregate.
fn large_integer_rows() -> String {
    let mut csv = String::from("k\n");
    for i in 0..FILLER_GROUPS {
        csv.push_str(&format!("{i}\n"));
    }
    csv.push_str(&format!(
        "{}\n{}\n{}\n",
        TWO_POW_53,
        TWO_POW_53 + 1,
        TWO_POW_53 + 1
    ));
    csv
}

/// `-0.0` and `0.0` in `zero_order`, then [`FILLER_GROUPS`] ascending
/// positive floats.
fn signed_zero_rows(zero_order: [&str; 2]) -> String {
    let mut csv = format!("k\n{}\n{}\n", zero_order[0], zero_order[1]);
    for i in 0..FILLER_GROUPS {
        csv.push_str(&format!("{i}.5\n"));
    }
    csv
}

/// The value 5 in `first` form (`i` integer, `f` float, `d` decimal), then
/// [`FILLER_GROUPS`] integer groups, then 5 in `second` form, so a spilled
/// run meets the two halves of the group in different spill files.
fn mixed_rows(first: &str, second: &str) -> String {
    let mut csv = format!("id,kind,num\nfirst,{first},5\n");
    for i in 0..FILLER_GROUPS {
        csv.push_str(&format!("f{i},i,{}\n", 1000 + i));
    }
    csv.push_str(&format!("second,{second},5\n"));
    csv
}

/// Expected Aggregate output lines for `column`: one line per filler group,
/// rendered by `filler`, plus the `special` `(csv, json)` group lines.
fn expected_aggregate(
    column: &str,
    filler: impl Fn(usize) -> String,
    special: &[(&str, &str)],
) -> (Vec<String>, Vec<String>) {
    let mut csv = vec![format!("{column},n")];
    let mut json = Vec::new();
    for i in 0..FILLER_GROUPS {
        csv.push(format!("{},1", filler(i)));
        json.push(format!(r#"{{"{column}":{},"n":1}}"#, filler(i)));
    }
    for (c, j) in special {
        csv.push((*c).to_string());
        json.push((*j).to_string());
    }
    (sorted(csv), sorted(json))
}

/// Assert that `run` wrote exactly `expected` (as sets of lines) to both
/// Sinks.
fn assert_aggregate_output(label: &str, run: &Run, expected: &(Vec<String>, Vec<String>)) {
    assert_eq!(
        sorted_lines(run.out("csv")),
        expected.0,
        "{label}: CSV groups or written keys differ"
    );
    assert_eq!(
        sorted_lines(run.out("json")),
        expected.1,
        "{label}: JSON groups or written keys differ"
    );
}

// ---- driving StreamingAggregator directly ---------------------------------

/// Stream `keys`, in order, through a `StreamingAggregator` grouping a
/// one-column input `k` of type `k_type` and emitting `n = count(*)`, and
/// return each output group's key (as its `Debug` form, so an integer and a
/// float of equal value stay distinguishable) and count.
fn stream_groups(k_type: Type, keys: &[Value]) -> Vec<(String, i64)> {
    let parsed = Parser::parse("emit k = k\nemit n = count(*)");
    assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
    let resolved = resolve_program(parsed.ast, &["k"], parsed.node_count).expect("resolve");
    let input_row = Row::closed(
        IndexMap::from([(cxl::typecheck::QualifiedField::bare("k"), k_type)]),
        cxl::lexer::Span::new(0, 0),
    );
    let group_by = vec!["k".to_string()];
    let typed = Arc::new(
        type_check_with_mode(
            resolved,
            &input_row,
            AggregateMode::GroupBy {
                group_by_fields: HashSet::from_iter(group_by.iter().cloned()),
            },
        )
        .expect("typecheck"),
    );
    let compiled = Arc::new(
        extract_aggregates(&typed, &group_by, &["k".to_string()]).expect("extract aggregates"),
    );
    let output_schema = SharedStorage::from_arc(Arc::new(Schema::new(
        compiled
            .emits
            .iter()
            .map(|emit| emit.output_name.clone().into())
            .collect(),
    )));
    let input_schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["k".into()])));
    let mut aggregator = StreamingAggregator::<AddRaw>::new_for_raw(
        compiled,
        ProgramEvaluator::new(typed, false),
        output_schema,
        "grouped",
    );
    let stable = StableEvalContext::test_default();
    let file: Arc<str> = Arc::from("in.csv");
    let mut rows: Vec<SortRow> = Vec::new();
    for (ordinal, key) in keys.iter().enumerate() {
        let record = Record::new(input_schema.clone(), vec![key.clone()]);
        let row = SourceRowId::new(PlanNodeId::new(0), ordinal as u64);
        aggregator
            .add_record(
                &record,
                row,
                &EvalContext::test_with_file(&stable, &file, ordinal as u64),
                &mut rows,
            )
            .expect("a sorted key streams");
    }
    aggregator
        .flush(&EvalContext::test_with_file(&stable, &file, 0), &mut rows)
        .expect("flush");
    rows.iter()
        .map(|(record, _)| {
            let count = match record.get("n") {
                Some(Value::Integer(n)) => *n,
                other => panic!("count(*) is an integer, got {other:?}"),
            };
            (
                format!("{:?}", record.get("k").expect("group key column")),
                count,
            )
        })
        .collect()
}

fn debug(v: Value) -> String {
    format!("{v:?}")
}

// ---- tests ----------------------------------------------------------------

/// An Aggregate grouping a typed integer column writes each group's key as
/// the integer it read, and keeps 2^53 and 2^53 + 1, which the same `f64`
/// holds, as two groups.
#[test]
fn integer_group_key_is_emitted_as_an_integer() {
    let yaml = r#"
pipeline:
  name: integer_group_key
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: k, type: int }
  - type: aggregate
    name: by_k
    input: src
    config:
      group_by: [k]
      cxl: |
        emit n = count(*)
  - type: sink
    name: out
    input: by_k
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: true
"#;
    let csv = "k\n42\n9007199254740993\n42\n9007199254740992\n9007199254740993\n";
    let output = run(yaml, csv, &["out"], None);
    assert_eq!(
        sorted_lines(output.out("out")),
        vec![
            r#"{"k":42,"n":2}"#.to_string(),
            r#"{"k":9007199254740992,"n":1}"#.to_string(),
            r#"{"k":9007199254740993,"n":2}"#.to_string(),
        ],
        "integer keys are exact and written as integers; output was:\n{}",
        output.out("out")
    );
}

/// Aggregate groups numbers by exact value, and writes each group's first
/// arriving value, identically with ample memory, when its hash table
/// spills, and under the streaming strategy:
///
/// - 2^53 and 2^53 + 1 (typed integers) are two groups, written exactly;
/// - `-0.0` and `0.0` (typed floats) are one group, in either arrival order;
/// - `5` and `5.0` (an `if` of an integer and a float) are one group written
///   as whichever arrived first, and the same for `5` and the decimal `5`.
#[test]
fn aggregate_groups_numbers_by_exact_value_at_every_capacity() {
    // Large integers.
    let expected = expected_aggregate(
        "k",
        |i| i.to_string(),
        &[
            ("9007199254740992,1", r#"{"k":9007199254740992,"n":1}"#),
            ("9007199254740993,2", r#"{"k":9007199254740993,"n":2}"#),
        ],
    );
    let (ample, spilled) = run_ample_and_spilled(
        &typed_aggregate_yaml("large_integers", "int", false),
        &large_integer_rows(),
        AGG_SINKS,
        AGGREGATE_SPILL_CAPACITY,
        "grouped",
    );
    assert_aggregate_output("large integers, ample", &ample, &expected);
    assert_aggregate_output("large integers, spilled", &spilled, &expected);
    let streamed = run(
        &typed_aggregate_yaml("large_integers_streaming", "int", true),
        &large_integer_rows(),
        AGG_SINKS,
        None,
    );
    assert_aggregate_output("large integers, streaming", &streamed, &expected);

    // Signed zeros, in both arrival orders. A key holds `0.0` for either
    // zero, so the group writes `0.0` whichever arrived first.
    let expected = expected_aggregate("k", |i| format!("{i}.5"), &[("0,2", r#"{"k":0.0,"n":2}"#)]);
    for zeros in [["-0.0", "0.0"], ["0.0", "-0.0"]] {
        let rows = signed_zero_rows(zeros);
        let (ample, spilled) = run_ample_and_spilled(
            &typed_aggregate_yaml("signed_zeros", "float", false),
            &rows,
            AGG_SINKS,
            AGGREGATE_SPILL_CAPACITY,
            "grouped",
        );
        assert_aggregate_output(&format!("zeros {zeros:?}, ample"), &ample, &expected);
        assert_aggregate_output(&format!("zeros {zeros:?}, spilled"), &spilled, &expected);
        let streamed = run(
            &typed_aggregate_yaml("signed_zeros_streaming", "float", true),
            &rows,
            AGG_SINKS,
            None,
        );
        assert_aggregate_output(&format!("zeros {zeros:?}, streaming"), &streamed, &expected);
    }

    // Integer and float, then integer and decimal, each in both arrival
    // orders: one group whose written value is the first arrival's.
    let value_of = |form: &str| match form {
        "f" => Value::Float(5.0),
        "d" => Value::Decimal(5.into()),
        _ => Value::Integer(5),
    };
    let cases: [(&str, [&str; 2], &str, Type); 4] = [
        (INT_OR_FLOAT, ["i", "f"], r#"{"v":5,"n":2}"#, Type::Float),
        (INT_OR_FLOAT, ["f", "i"], r#"{"v":5.0,"n":2}"#, Type::Float),
        (
            INT_OR_DECIMAL,
            ["i", "d"],
            r#"{"v":5,"n":2}"#,
            Type::Decimal,
        ),
        (
            INT_OR_DECIMAL,
            ["d", "i"],
            r#"{"v":"5","n":2}"#,
            Type::Decimal,
        ),
    ];
    for (expr, [first, second], json_five, k_type) in cases {
        let expected = expected_aggregate("v", |i| (1000 + i).to_string(), &[("5,2", json_five)]);
        let label = format!("{expr} arriving {first} then {second}");
        let (ample, spilled) = run_ample_and_spilled(
            &transform_aggregate_yaml("mixed_numbers", expr),
            &mixed_rows(first, second),
            AGG_SINKS,
            AGGREGATE_SPILL_CAPACITY,
            "grouped",
        );
        assert_aggregate_output(&format!("{label}, ample"), &ample, &expected);
        assert_aggregate_output(&format!("{label}, spilled"), &spilled, &expected);

        // Streaming, over the same values in key order: 5 sorts below the
        // filler, and the two fives tie, so either arrival order is sorted.
        let mut keys = vec![value_of(first), value_of(second)];
        keys.extend((0..3).map(|i| Value::Integer(1000 + i)));
        assert_eq!(
            stream_groups(k_type, &keys),
            vec![
                (debug(value_of(first)), 2),
                (debug(Value::Integer(1000)), 1),
                (debug(Value::Integer(1001)), 1),
                (debug(Value::Integer(1002)), 1),
            ],
            "{label}, streaming"
        );
    }
}

/// Source `src` with string columns `id`, `kind`, `num`, a Transform `vals`
/// turning `num` into the float `v` (negated when `kind` is `neg`, null when
/// `num` is empty), and `node_yaml` consuming `vals`.
fn nan_pipeline(name: &str, node_yaml: &str) -> String {
    format!(
        r#"
pipeline:
  name: {name}
  memory: {{ limit: "{AMPLE_LIMIT}", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: kind, type: string }}
        - {{ name: num, type: string }}
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit id = id
        emit v = if num == "" then null else if kind == "neg" then -(num.to_float()) else num.to_float()
{node_yaml}"#
    )
}

/// Rows `nan1` (NaN), `nan2` (-NaN) and `nan3` (NaN), `nulls` null rows and
/// [`FILLER_GROUPS`] finite groups of `filler_rows` rows each, the NaN and
/// null rows spread through the input so a spilled run holds them in
/// different spill files.
fn nan_rows(filler_rows: usize, nulls: usize) -> String {
    let mut csv = String::from("id,kind,num\nnan1,pos,NaN\n");
    let mut filler = Vec::new();
    for i in 0..FILLER_GROUPS {
        for r in 0..filler_rows {
            filler.push(format!("g{i}r{r},pos,{i}.25\n"));
        }
    }
    let third = filler.len() / 3;
    for (n, line) in filler.iter().enumerate() {
        if n == third {
            csv.push_str("nan2,neg,NaN\n");
            for k in 0..nulls {
                csv.push_str(&format!("null{k},pos,\n"));
            }
        }
        if n == 2 * third {
            csv.push_str("nan3,pos,NaN\n");
        }
        csv.push_str(line);
    }
    csv
}

const NAN_AGGREGATE: &str = r#"
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [v]
      cxl: |
        emit n = count(*)
  - type: sink
    name: out
    input: grouped
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

const NAN_CULL: &str = r#"
  - type: cull
    name: culled
    input: vals
    config:
      partition_by: [v]
      removed_to: removed
      rules:
        - name: drop_big_groups
          drop_group_when: "count(*) > 2"
  - type: sink
    name: out
    input: culled
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
    name: audit
    input: culled.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

/// A Reshape whose rule never fires, ordering each group by `id` descending:
/// groups are written in first-arrival order, each group's rows together, so
/// the output shows which rows formed one group.
const NAN_RESHAPE: &str = r#"
  - type: reshape
    name: reshaped
    input: vals
    config:
      partition_by: [v]
      order_by:
        - { field: id, order: desc }
      rules:
        - name: never
          when: "id == \"none\""
          mutate:
            set:
              id: "id"
  - type: sink
    name: out
    input: reshaped
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// Every NaN key, whatever its sign, is one group, and that group is not the
/// null group: in Aggregate (ample, spilled and streaming), Cull and Reshape
/// (ample and spilled, byte-identical).
#[test]
fn nan_keys_form_one_group_distinct_from_null() {
    // Aggregate: one NaN group of three rows and one null group of two.
    let mut expected = vec!["v,n".to_string(), "NaN,3".to_string(), ",2".to_string()];
    expected.extend((0..FILLER_GROUPS).map(|i| format!("{i}.25,1")));
    let expected = sorted(expected);
    let (ample, spilled) = run_ample_and_spilled(
        &nan_pipeline("nan_aggregate", NAN_AGGREGATE),
        &nan_rows(1, 2),
        &["out"],
        NAN_AGGREGATE_SPILL_CAPACITY,
        "grouped",
    );
    assert_eq!(sorted_lines(ample.out("out")), expected, "Aggregate, ample");
    assert_eq!(
        sorted_lines(spilled.out("out")),
        expected,
        "Aggregate, spilled"
    );
    // Streaming, over the same kinds of keys in order: nulls first, NaN of
    // either sign above every finite value.
    let nan = f64::NAN;
    assert_eq!(
        stream_groups(
            Type::Float,
            &[
                Value::Null,
                Value::Null,
                Value::Float(1.25),
                Value::Float(nan),
                Value::Float(-nan),
                Value::Float(nan),
            ],
        ),
        vec![
            (debug(Value::Null), 2),
            (debug(Value::Float(1.25)), 1),
            (debug(Value::Float(nan)), 3),
        ],
        "Aggregate, streaming"
    );

    // Cull: `count(*) > 2` removes the three NaN rows only; the filler groups
    // (two rows each) and the lone null row stay.
    let (ample, spilled) = run_ample_and_spilled(
        &nan_pipeline("nan_cull", NAN_CULL),
        &nan_rows(2, 1),
        &["out", "audit"],
        NAN_CULL_SPILL_CAPACITY,
        "culled",
    );
    let removed: Vec<String> = ample
        .out("audit")
        .lines()
        .skip(1)
        .map(str::to_string)
        .collect();
    assert_eq!(
        sorted(removed),
        vec![
            "nan1,pos,NaN,NaN".to_string(),
            "nan2,neg,NaN,NaN".to_string(),
            "nan3,pos,NaN,NaN".to_string(),
        ],
        "Cull removes exactly the NaN group"
    );
    let kept: Vec<&str> = ample.out("out").lines().skip(1).collect();
    assert_eq!(kept.len(), 2 * FILLER_GROUPS + 1);
    assert!(
        kept.contains(&"null0,pos,,"),
        "the null row is its own group"
    );
    assert_eq!(spilled.out("out"), ample.out("out"), "Cull kept rows");
    assert_eq!(
        spilled.out("audit"),
        ample.out("audit"),
        "Cull removed rows"
    );

    // Reshape: the NaN rows of both signs are one group and the null rows
    // another, each written together in descending `id`.
    let (ample, spilled) = run_ample_and_spilled(
        &nan_pipeline("nan_reshape", NAN_RESHAPE),
        &nan_rows(1, 2),
        &["out"],
        NAN_RESHAPE_SPILL_CAPACITY,
        "reshaped",
    );
    let lines: Vec<&str> = ample.out("out").lines().collect();
    let group_at = |first: &str, len: usize| -> Vec<&str> {
        let start = lines
            .iter()
            .position(|l| *l == first)
            .unwrap_or_else(|| panic!("missing {first:?}"));
        lines[start..start + len].to_vec()
    };
    assert_eq!(
        group_at("nan3,pos,NaN,NaN", 3),
        vec!["nan3,pos,NaN,NaN", "nan2,neg,NaN,NaN", "nan1,pos,NaN,NaN"],
        "the NaN rows of both signs form one Reshape group"
    );
    assert_eq!(
        group_at("null1,pos,,", 2),
        vec!["null1,pos,,", "null0,pos,,"],
        "the null rows form their own Reshape group"
    );
    assert_eq!(lines.len(), 1 + FILLER_GROUPS + 3 + 2);
    assert_eq!(spilled.out("out"), ample.out("out"), "Reshape output");
}

/// Cull keys partitions by exact value: 2^53 and 2^53 + 1 are two groups of
/// one row each, which a `count(*) > 1` rule keeps, while a real two-row
/// group is removed.
#[test]
fn cull_partitions_numbers_by_exact_value() {
    let yaml = format!(
        r#"
pipeline:
  name: cull_exact_numbers
  memory: {{ limit: "{AMPLE_LIMIT}", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: k, type: int }}
  - type: cull
    name: culled
    input: src
    config:
      partition_by: [k]
      removed_to: removed
      rules:
        - name: drop_repeated_keys
          drop_group_when: "count(*) > 1"
  - type: sink
    name: out
    input: culled
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
    name: audit
    input: culled.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#
    );
    let csv = format!("id,k\na,{}\nb,{}\nc,7\nd,7\n", TWO_POW_53, TWO_POW_53 + 1);
    let output = run(&yaml, &csv, &["out", "audit"], None);
    assert_eq!(
        output.out("out"),
        format!("id,k\na,{}\nb,{}\n", TWO_POW_53, TWO_POW_53 + 1),
        "integers that share an f64 are two groups"
    );
    assert_eq!(output.out("audit"), "id,k\nc,7\nd,7\n");
}
