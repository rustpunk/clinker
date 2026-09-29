//! Records group by exact value, under the same rule the value order ties by.
//!
//! A group key is equal to another exactly when the two values tie in the one
//! value order: integers, floats and decimals by exact value (so `5` and `5.0`
//! are one group, and two integers above 2^53 stay apart), `-0.0` with `0.0`,
//! and every NaN, whatever its sign, as one group apart from the null group.
//! A group reports the value of its first-arriving row, so an integer column is
//! written as integers.
//!
//! NaN values come from an upstream Transform (`"NaN".to_float()` and its
//! negation), because a typed Source rejects non-finite floats.

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};

/// Run a one-Source, one-Sink pipeline over `csv` fed to the Source `src`,
/// returning the report and the Sink `out`'s bytes.
fn run(yaml: &str, csv: &str) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("fixture parses");
    let context = CompileContext::default();
    let plan = config.compile(&context).expect("fixture compiles");
    let readers = HashMap::from([(
        "src".to_string(),
        resource_fixtures::predecoded_csv_source(&config, &context, "src", &[("in.csv", csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "group-key-value-order".to_string(),
        batch_id: "b".to_string(),
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("the pipeline runs to completion");
    (report, buf.as_string())
}

/// The Sink's lines, sorted, so a comparison does not depend on emit order.
fn sorted_lines(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().map(str::to_string).collect();
    lines.sort();
    lines
}

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
    let (_, output) = run(yaml, csv);
    assert_eq!(
        sorted_lines(&output),
        vec![
            r#"{"k":42,"n":2}"#.to_string(),
            r#"{"k":9007199254740992,"n":1}"#.to_string(),
            r#"{"k":9007199254740993,"n":2}"#.to_string(),
        ],
        "integer keys are exact and written as integers; output was:\n{output}"
    );
}

/// Source `src` with string columns `id`, `kind`, `num`, a Transform `vals`
/// turning `num` into the float `v` (negated when `kind` is `neg`, null when
/// `num` is empty), and `node_yaml` consuming `vals` and feeding the CSV Sink.
fn nan_pipeline(name: &str, node_yaml: &str) -> String {
    format!(
        r#"
pipeline:
  name: {name}
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
{node_yaml}
  - type: sink
    name: out
    input: grouped
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// Three NaN rows of both signs, two null rows and one finite row.
const NAN_ROWS: &str = "id,kind,num\na,pos,NaN\nb,neg,NaN\nc,pos,\nd,pos,1.5\ne,neg,NaN\nf,pos,\n";

/// Every NaN key, whatever its sign, is one group, and that group is not the
/// null group.
#[test]
fn nan_keys_form_one_group_distinct_from_null() {
    let aggregate = r#"
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [v]
      cxl: |
        emit n = count(*)
"#;
    let (_, output) = run(&nan_pipeline("nan_aggregate", aggregate), NAN_ROWS);
    assert_eq!(
        sorted_lines(&output),
        vec![
            ",2".to_string(),
            "1.5,1".to_string(),
            "NaN,3".to_string(),
            "v,n".to_string(),
        ],
        "one NaN group and one null group; output was:\n{output}"
    );
}
