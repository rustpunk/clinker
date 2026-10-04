//! Each test pastes exactly what a diagnostic printed into the pipeline that
//! raised it, and runs the result.
//!
//! A diagnostic's fix is part of the author-facing surface: an author copies
//! the backticked form into their YAML or CXL and reruns. So every test here
//! takes the form from the compile diagnostic or the run's error text, not
//! from a copy of the wording, pastes it in place of the authored form, and
//! checks the rerun's output against a hand-derived value. A change to a
//! printed fix changes what these tests run.
//!
//! Every pipeline runs under the default `fail_fast` strategy, so a failed
//! group stops the run and its error's text is read from the run's result.

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

/// A run's outcome and its `csv` Sink's bytes.
struct Run {
    result: Result<(), PipelineError>,
    csv: String,
}

impl Run {
    /// The run's error text, for a run that must fail.
    fn error(&self) -> String {
        match &self.result {
            Ok(()) => panic!("the run must fail; it wrote:\n{}", self.csv),
            Err(e) => e.to_string(),
        }
    }

    /// The `csv` Sink's lines, sorted, for a run that must succeed.
    fn lines(&self) -> Vec<String> {
        if let Err(e) = &self.result {
            panic!("the run must succeed, got: {e}");
        }
        let mut lines: Vec<String> = self.csv.lines().map(str::to_string).collect();
        lines.sort();
        lines
    }
}

/// Run a one-Source pipeline over `csv` fed to the Source `src`, capturing
/// the Sink `csv`. The pipeline must compile.
fn run(yaml: &str, csv: &str) -> Run {
    let mut config = parse_config(yaml).expect("fixture parses");
    let context = CompileContext::default();
    resource_fixtures::add_csv_workspace(&mut config, &context);
    let plan = config.compile(&context).expect("fixture compiles");
    let readers = HashMap::from([(
        "src".to_string(),
        resource_fixtures::predecoded_csv_source(&config, &context, "src", &[("in.csv", csv)]),
    )]);
    let buffer = SharedBuffer::new();
    // Every Sink other than `csv` writes into a buffer the test never reads.
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = config
        .sink_configs()
        .map(|sink| {
            let target = if sink.name == "csv" {
                buffer.clone()
            } else {
                SharedBuffer::new()
            };
            (
                sink.name.clone(),
                Box::new(target) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "aggregate-error-fixes".to_string(),
        batch_id: "b".to_string(),
        ..Default::default()
    };
    let result = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .map(|_| ());
    Run {
        result,
        csv: buffer.as_string(),
    }
}

/// Every E200 message `yaml` fails to compile with.
fn e200_messages(yaml: &str) -> Vec<String> {
    let config = parse_config(yaml).expect("fixture parses");
    let diagnostics = config
        .compile(&CompileContext::default())
        .expect_err("the pipeline must fail to compile");
    let messages: Vec<String> = diagnostics
        .iter()
        .filter(|d| d.code == "E200")
        .map(|d| d.message.clone())
        .collect();
    assert!(
        !messages.is_empty(),
        "an E200, got: {:?}",
        diagnostics
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
    messages
}

/// The one E200 message `yaml` fails to compile with.
fn e200(yaml: &str) -> String {
    let mut messages = e200_messages(yaml);
    assert_eq!(messages.len(), 1, "exactly one E200, got: {messages:?}");
    messages.remove(0)
}

/// The single backticked span in `text` that starts with `prefix`. A
/// diagnostic prints one fix, so a second match is a failure.
fn printed_span(text: &str, prefix: &str) -> String {
    let spans: Vec<&str> = text
        .split('`')
        .skip(1)
        .step_by(2)
        .filter(|span| span.starts_with(prefix))
        .collect();
    assert_eq!(
        spans.len(),
        1,
        "exactly one backticked span starting `{prefix}` in: {text}"
    );
    spans[0].to_string()
}

/// `text` with its one occurrence of `from` replaced by `to`.
fn replace_once(text: &str, from: &str, to: &str) -> String {
    assert_eq!(
        text.matches(from).count(),
        1,
        "exactly one `{from}` to replace in:\n{text}"
    );
    text.replacen(from, to, 1)
}

/// `yaml` with `from` replaced by `to` on the one line that holds `line`,
/// exactly once.
fn replace_on_line(yaml: &str, line: &str, from: &str, to: &str) -> String {
    let lines: Vec<&str> = yaml.lines().filter(|l| l.contains(line)).collect();
    assert_eq!(lines.len(), 1, "exactly one line holding `{line}`");
    let fixed = replace_once(lines[0], from, to);
    replace_once(yaml, lines[0], &fixed)
}

/// The schema change a decimal/float join's E200 prints: the backticked
/// `type:` span before "in place of", and the one after it.
fn printed_schema_change(message: &str) -> (String, String) {
    let (before, after) = message
        .split_once(" in place of ")
        .unwrap_or_else(|| panic!("the message prints a schema change: {message}"));
    (printed_span(before, "type:"), printed_span(after, "type:"))
}

// ---- the decimal/float join (E200) -----------------------------------------

/// An Aggregate and a Transform reading the decimal `amount` or the float
/// `price` by `flag`, with the price column's type `{price_type}`.
fn join_yaml(price_type: &str) -> String {
    format!(
        r#"
pipeline:
  name: decimal_float_join
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: g, type: string }}
        - {{ name: flag, type: bool }}
        - {{ name: amount, type: decimal }}
        - {{ name: price, type: {price_type} }}
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [g]
      cxl: |
        emit total = sum(if flag then amount else price)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: transform
    name: chosen
    input: src
    config:
      cxl: |
        emit v = if flag then amount else price
  - type: sink
    name: rows
    input: chosen
    config:
      name: rows
      type: csv
      path: rows.csv
      include_unmapped: true
"#
    )
}

/// The total of the chosen cells is 0.10 + 0.20 + 0.10. Read as decimals it
/// is exactly `0.40`; added as floats, 0.1 + 0.2 is 0.30000000000000004 and
/// adding 0.1 gives 0.4000000000000001.
const JOIN_ROWS: &str = "\
g,flag,amount,price
a,true,0.10,9.99
a,false,1.00,0.20
a,false,1.00,0.10
";

/// The E200 for a decimal and a float column joined by an `if` prints the
/// Source schema change for the float column. Pasting it over the column's
/// declared type makes both branches decimals, and the total is exact.
#[test]
fn the_printed_schema_fix_for_a_decimal_float_join_gives_an_exact_total() {
    for price_type in ["float", "{ nullable: float }"] {
        let authored = join_yaml(price_type);
        // The Aggregate's `sum` and the Transform's `emit` each fail.
        let messages = e200_messages(&authored);
        assert_eq!(messages.len(), 2, "one E200 per node: {messages:?}");
        let (replacement, original) = printed_schema_change(&messages[0]);
        assert_eq!(
            printed_schema_change(&messages[1]),
            (replacement.clone(), original.clone()),
            "both nodes print the same schema change"
        );
        let fixed = replace_on_line(&authored, "name: price", &original, &replacement);

        let run = run(&fixed, JOIN_ROWS);
        assert_eq!(
            run.lines(),
            vec!["a,0.40".to_string(), "g,total".to_string()],
            "price type {price_type}, fixed with {replacement}"
        );
    }
}

/// A Transform whose `else` branch is a computed float, `price * 2.0`.
const COMPUTED_JOIN_YAML: &str = r#"
pipeline:
  name: computed_float_join
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: g, type: string }
        - { name: flag, type: bool }
        - { name: amount, type: decimal }
        - { name: price, type: float }
  - type: transform
    name: chosen
    input: src
    config:
      cxl: |
        emit v = if flag then amount else price * 2.0
  - type: sink
    name: csv
    input: chosen
    config:
      name: csv
      type: csv
      path: out.csv
"#;

/// A computed float has no Source column to retype, so the E200 prints the
/// conversion of the decimal side. Pasted over `amount`, the branches are
/// floats: 0.10 becomes the float 0.1, and the prices doubled are 0.4 and
/// 0.2 (doubling a float is exact), each printed in its shortest form. A
/// Transform passes its input's columns through, so each line also holds the
/// row's Source cells, the float `price` printed the same way.
#[test]
fn the_printed_conversion_for_a_computed_float_branch_compiles_and_runs() {
    let message = e200(COMPUTED_JOIN_YAML);
    let conversion = printed_span(&message, "amount.");
    assert!(
        !message.contains(" in place of ") && !message.contains("to_decimal"),
        "a computed float gets one fix, the decimal side's conversion: {message}"
    );
    let fixed = replace_once(
        COMPUTED_JOIN_YAML,
        "then amount else",
        &format!("then {conversion} else"),
    );

    let run = run(&fixed, JOIN_ROWS);
    assert_eq!(
        run.lines(),
        vec![
            "a,false,1.00,0.1,0.2".to_string(),
            "a,false,1.00,0.2,0.4".to_string(),
            "a,true,0.10,9.99,0.1".to_string(),
            "g,flag,amount,price,v".to_string(),
        ]
    );
}

// ---- the accumulators' run-time errors -------------------------------------

/// The numeric-typed branch the typechecker cannot see through: `clamp`
/// gives `numeric`, so a group of decimals and floats reaches the run.
const MIXED_GROUP_YAML: &str = r#"
pipeline:
  name: mixed_decimal_float
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: category, type: string }
        - { name: kind, type: string }
        - { name: amount, type: decimal }
        - { name: price, type: float }
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [category]
      cxl: |
        emit total = sum(if kind == "d" then amount.clamp(0, 100) else price)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// One group: the decimal 1.50 and the floats 0.10 and 0.20. Read as
/// decimals the total is exactly `1.80`; as floats it would be
/// 1.8000000000000003.
const MIXED_GROUP_ROWS: &str = "\
category,kind,amount,price
a,d,1.50,0
a,f,0,0.10
a,f,0,0.20
";

/// The mixed-group error prints the Source schema type that makes the float
/// column a decimal. Pasted over `price`'s `type: float`, the group is all
/// decimals and the total is exact.
#[test]
fn the_printed_schema_fix_for_a_mixed_group_gives_an_exact_total() {
    let error = run(MIXED_GROUP_YAML, MIXED_GROUP_ROWS).error();
    assert!(
        error.contains("decimal and float in one group"),
        "the mixed-group error, got: {error}"
    );
    let schema_type = printed_span(&error, "type:");
    let fixed = replace_on_line(MIXED_GROUP_YAML, "name: price", "type: float", &schema_type);

    let run = run(&fixed, MIXED_GROUP_ROWS);
    assert_eq!(
        run.lines(),
        vec!["a,1.80".to_string(), "category,total".to_string()]
    );
}

/// A one-emit Aggregate over the column `amount` of the given type.
fn one_emit_yaml(amount_type: &str, emit: &str) -> String {
    format!(
        r#"
pipeline:
  name: one_emit
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: g, type: string }}
        - {{ name: amount, type: {amount_type} }}
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [g]
      cxl: |
        emit total = {emit}
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// `i64::MAX` twice overflows an integer sum. Summed as decimals the total
/// is exact: 2 × 9223372036854775807 = 18446744073709551614.
#[test]
fn the_printed_decimal_sum_fixes_an_integer_overflow() {
    let authored = one_emit_yaml("int", "sum(amount)");
    let rows = "g,amount\na,9223372036854775807\na,9223372036854775807\n";
    let error = run(&authored, rows).error();
    assert!(
        error.contains("integer sum overflow"),
        "the integer overflow, got: {error}"
    );
    let fix = printed_span(&error, "sum(");
    let fixed = replace_once(&authored, "sum(amount)", &fix);

    assert_eq!(
        run(&fixed, rows).lines(),
        vec!["a,18446744073709551614".to_string(), "g,total".to_string()]
    );
}

/// 7e28 twice is outside the decimal range. As floats each is the double
/// nearest 7e28, and their exact sum, twice that double, is the double
/// nearest 1.4e29, which the writer prints in full without an exponent.
#[test]
fn the_printed_float_sum_fixes_a_decimal_total_out_of_range() {
    let authored = one_emit_yaml("decimal", "sum(amount)");
    let rows = "g,amount\na,70000000000000000000000000000\na,70000000000000000000000000000\n";
    let error = run(&authored, rows).error();
    assert!(
        error.contains("decimal total out of range"),
        "the decimal range error, got: {error}"
    );
    let fix = printed_span(&error, "sum(");
    let fixed = replace_once(&authored, "sum(amount)", &fix);

    assert_eq!(
        run(&fixed, rows).lines(),
        vec![
            "a,140000000000000000000000000000".to_string(),
            "g,total".to_string()
        ]
    );
}

/// An Aggregate over the decimals `price` and `qty` whose one emit is `emit`.
fn weighted_yaml(emit: &str) -> String {
    format!(
        r#"
pipeline:
  name: weighted
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: g, type: string }}
        - {{ name: price, type: decimal }}
        - {{ name: qty, type: decimal }}
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [g]
      cxl: |
        emit wa = {emit}
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// 1e15 × 1e15 = 1e30 is outside the decimal range. In floats the weighted
/// average of the one row is 1e30 / 1e15 = 1e15 exactly, printed in full.
#[test]
fn the_printed_float_average_fixes_a_product_out_of_range() {
    let authored = weighted_yaml("weighted_avg(price, qty)");
    let rows = "g,price,qty\na,1000000000000000,1000000000000000\n";
    let error = run(&authored, rows).error();
    assert!(
        error.contains("decimal product out of range"),
        "the product range error, got: {error}"
    );
    let fix = printed_span(&error, "weighted_avg(");
    let fixed = replace_once(&authored, "weighted_avg(price, qty)", &fix);

    assert_eq!(
        run(&fixed, rows).lines(),
        vec!["a,1000000000000000".to_string(), "g,wa".to_string()]
    );
}

/// Group `a`'s weights are all zero; group `b`'s are 1 and 3.
const ZERO_WEIGHT_ROWS: &str = "\
g,price,qty
a,2.50,0
a,4.00,0
b,1.00,1
b,3.00,3
";

/// Group `b`'s rows alone.
const GROUP_B_ROWS: &str = "\
g,price,qty
b,1.00,1
b,3.00,3
";

/// A group whose weights are all zero has no weighted average. The error
/// prints a Transform config that drops zero-weight rows; inserted before
/// the Aggregate, it removes group `a` and leaves group `b` as it was:
/// (1.00 × 1 + 3.00 × 3) / (1 + 3) = 10 / 4 = 2.5.
#[test]
fn the_printed_filter_fixes_a_group_whose_weights_are_all_zero() {
    let authored = weighted_yaml("weighted_avg(price, qty)");
    let error = run(&authored, ZERO_WEIGHT_ROWS).error();
    assert!(
        error.contains("zero total weight"),
        "the zero-weight error, got: {error}"
    );
    let config = printed_span(&error, "config:");
    let with_filter = replace_once(
        &authored,
        "  - type: aggregate\n    name: grouped\n    input: src\n",
        &format!(
            "  - type: transform\n    name: nonzero\n    input: src\n    {config}\n  \
             - type: aggregate\n    name: grouped\n    input: nonzero\n"
        ),
    );

    let fixed = run(&with_filter, ZERO_WEIGHT_ROWS).lines();
    let b_alone = run(&authored, GROUP_B_ROWS).lines();
    assert_eq!(fixed, b_alone, "group b is unchanged and group a is gone");
    assert_eq!(fixed.len(), 2, "the header and group b: {fixed:?}");
    let b = fixed
        .iter()
        .find_map(|line| line.strip_prefix("b,"))
        .expect("group b is written");
    assert_eq!(b.parse::<f64>().expect("a number"), 2.5, "group b: {b}");
}

/// The values' products total 15800000000000000000000000000 × 0.50 + 0 ×
/// -0.45 = 7.9e27 and the weights 0.50 - 0.45 = 0.05, so the quotient,
/// 1.58e29, is outside the decimal range although both totals are inside.
const QUOTIENT_YAML: &str = r#"
pipeline:
  name: quotient
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: g, type: string }
        - { name: value, type: decimal }
        - { name: weight, type: decimal }
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [g]
      cxl: |
        emit wa = weighted_avg(value, weight)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#;

const QUOTIENT_ROWS: &str = "\
g,value,weight
a,15800000000000000000000000000,0.50
a,0,-0.45
";

/// The quotient error prints two emits to inspect the totals. Replacing the
/// `weighted_avg` emit with them runs and writes both totals.
#[test]
fn the_printed_inspection_step_runs_for_a_quotient_out_of_range() {
    let error = run(QUOTIENT_YAML, QUOTIENT_ROWS).error();
    assert!(
        error.contains("decimal average out of range"),
        "the quotient error, got: {error}"
    );
    let products = printed_span(&error, "sum(value");
    let weights = printed_span(&error, "sum(weight");
    let fixed = replace_once(
        QUOTIENT_YAML,
        "emit wa = weighted_avg(value, weight)",
        &format!("emit products = {products}\n        emit weights = {weights}"),
    );

    assert_eq!(
        run(&fixed, QUOTIENT_ROWS).lines(),
        vec![
            "a,7900000000000000000000000000.0,0.05".to_string(),
            "g,products,weights".to_string()
        ]
    );
}

// ---- a Cull rule ------------------------------------------------------------

/// A Cull whose rule sums an integer column.
const CULL_YAML: &str = r#"
pipeline:
  name: cull_overflow
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
  - type: cull
    name: drop_positive
    input: src
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: positive_total
          drop_group_when: "sum(amount) > 0"
  - type: sink
    name: csv
    input: drop_positive
    config:
      name: csv
      type: csv
      path: out.csv
  - type: sink
    name: audit
    input: drop_positive.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

/// A Cull rule's accumulator failure names the `drop_group_when` rule and
/// the Cull, and never the engine's synthetic label for the rule's
/// aggregate or its internal prefix.
#[test]
fn a_cull_rule_failure_names_drop_group_when_not_an_engine_label() {
    let rows = "account,amount\na,9223372036854775807\na,9223372036854775807\n";
    let error = run(CULL_YAML, rows).error();
    for wanted in ["drop_group_when", "drop_positive", "integer sum overflow"] {
        assert!(error.contains(wanted), "lacks {wanted}: {error}");
    }
    for engine in ["__cull_drop_decision__", "cull:"] {
        assert!(!error.contains(engine), "names {engine}: {error}");
    }
}
