//! A Cull or Reshape `order_by` orders the rows of each group exactly as a
//! Sink `sort_order` orders rows.
//!
//! Numbers compare by exact value across integers, floats and decimals,
//! every `NaN` is one value after every number, `-0.0` equals `0.0`, and
//! arrival order breaks ties. A null goes where `null_order` puts it, and
//! `last` is the default for `asc` and `desc` alike. The expected id
//! sequences below are written from that rule, not recorded from a run.
//!
//! The refusal of `null_order: drop` on these fields offers an author two
//! replacements and an upstream filter; each one is applied here and its
//! result checked, so the message cannot promise something the engine does
//! not do.

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};

/// Headroom well above anything these fixtures hold, so nothing spills and
/// the order under test is the group sort's alone.
const AMPLE_LIMIT: &str = "256MB";

// ---- fixtures -------------------------------------------------------------

/// Null placement: group `a` holds 3, null, 1, null, 2 and group `b` holds
/// null, 2, 1, in arrival order.
const FIXTURE_P: &str = "id,g,kind,num\n\
     a1,a,i,3\n\
     b1,b,i,\n\
     a2,a,i,\n\
     a3,a,i,1\n\
     b2,b,i,2\n\
     a4,a,i,\n\
     b3,b,i,1\n\
     a5,a,i,2\n";

/// The value order: `a3` is NaN and `a5` is -NaN, `a6` is the integer 0 and
/// `a7` is -0.0, `a9` is the integer 2^53 + 1 and `a10` the float 2^53.
const FIXTURE_V: &str = "id,g,kind,num\n\
     a1,a,i,3\n\
     a2,a,f,2.5\n\
     b1,b,f,1.5\n\
     a3,a,f,NaN\n\
     a4,a,i,1\n\
     b2,b,i,1\n\
     a5,a,n,NaN\n\
     a6,a,i,0\n\
     a7,a,n,0.0\n\
     a8,a,i,2\n\
     a9,a,i,9007199254740993\n\
     a10,a,f,9007199254740992\n";

/// Source `src` with four string columns, a Transform `vals` turning `num`
/// into the key column `k` (an integer for `i`, a float for `f`, a negated
/// float for `n`, null for an empty cell: the branches unify to a float
/// column whose rows still hold integers), and `node_yaml` consuming `vals`.
fn pipeline(name: &str, node_yaml: &str) -> String {
    format!(
        r#"
pipeline:
  name: {name}
  memory: {{ limit: "{AMPLE_LIMIT}" }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: g, type: string }}
        - {{ name: kind, type: string }}
        - {{ name: num, type: string }}
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit id = id
        emit g = g
        emit k = if num == "" then null else if kind == "i" then num.to_int() else if kind == "n" then -(num.to_float()) else num.to_float()
{node_yaml}"#
    )
}

/// A Cull over `vals` whose one rule removes group `b` whatever its size, so
/// group `a` leaves on the main port and group `b` on `removed`. `order_by`
/// and the node's input are spliced in as YAML text.
fn cull_yaml(order_by: &str, input: &str) -> String {
    pipeline(
        "cull_order",
        &format!(
            r#"  - type: cull
    name: culled
    input: {input}
    config:
      partition_by: [g]
      order_by: {order_by}
      removed_to: removed
      rules:
        - name: drop_b
          drop_group_when: "max(g) == \"b\""
  - type: sink
    name: out
    input: culled
    config: {{ name: out, type: csv, path: out.csv }}
  - type: sink
    name: audit
    input: culled.removed
    config: {{ name: audit, type: csv, path: audit.csv }}
"#
        ),
    )
}

/// A Reshape over `vals` whose one rule never fires, so its single output
/// holds every row, each group's rows together in first-arrival group order.
fn reshape_yaml(order_by: &str, input: &str) -> String {
    pipeline(
        "reshape_order",
        &format!(
            r#"  - type: reshape
    name: reshaped
    input: {input}
    config:
      partition_by: [g]
      order_by: {order_by}
      rules:
        - name: never
          when: "id == \"none\""
          mutate:
            set:
              id: "id"
  - type: sink
    name: out
    input: reshaped
    config: {{ name: out, type: csv, path: out.csv }}
"#
        ),
    )
}

/// A Transform between `vals` and the node under test that keeps only the
/// rows whose key is not null: the filter the `drop` refusal offers.
const NON_NULL_FILTER: &str = r#"  - type: transform
    name: kept
    input: vals
    config:
      cxl: |
        filter not k.is_null()
        emit id = id
        emit g = g
        emit k = k
"#;

// ---- running --------------------------------------------------------------

/// Run a one-Source pipeline over `csv`, returning each named Sink's rendered
/// bytes by Sink name.
fn run(yaml: &str, csv: &str, sinks: &[&str]) -> HashMap<String, String> {
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
    let params = PipelineRunParams {
        execution_id: "group-order".to_string(),
        batch_id: "group-order".to_string(),
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("fixture run");
    buffers
        .into_iter()
        .map(|(name, buf)| (name, buf.as_string()))
        .collect()
}

/// The `id` column of a rendered CSV output, in written order, space
/// separated.
fn ids(csv: &str) -> String {
    let mut lines = csv.lines();
    let header: Vec<&str> = lines
        .next()
        .expect("an output has a header line")
        .split(',')
        .collect();
    let id = header
        .iter()
        .position(|column| *column == "id")
        .expect("an output carries the id column");
    lines
        .map(|line| line.split(',').nth(id).expect("every row carries id"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Both ports of a Cull run over `csv` with the given `order_by`, as id
/// sequences.
fn cull_ids(order_by: &str, csv: &str) -> (String, String) {
    let outputs = run(&cull_yaml(order_by, "vals"), csv, &["out", "audit"]);
    (ids(&outputs["out"]), ids(&outputs["audit"]))
}

/// The one output of a Reshape run over `csv` with the given `order_by`, as
/// an id sequence.
fn reshape_ids(order_by: &str, csv: &str) -> String {
    let outputs = run(&reshape_yaml(order_by, "vals"), csv, &["out"]);
    ids(&outputs["out"])
}

/// The six `order_by` spellings every placement case needs: both directions
/// with `null_order` omitted, `last` and `first`.
const PLACEMENT_CASES: [(&str, &str); 6] = [
    ("asc omitted", "[{ field: k, order: asc }]"),
    ("asc last", "[{ field: k, order: asc, null_order: last }]"),
    ("asc first", "[{ field: k, order: asc, null_order: first }]"),
    ("desc omitted", "[{ field: k, order: desc }]"),
    ("desc last", "[{ field: k, order: desc, null_order: last }]"),
    (
        "desc first",
        "[{ field: k, order: desc, null_order: first }]",
    ),
];

/// The expected main-port and removed-port sequences of fixture P, one entry
/// per [`PLACEMENT_CASES`] entry, in the same order.
const PLACEMENT_EXPECTED: [(&str, &str); 6] = [
    ("a3 a5 a1 a2 a4", "b3 b2 b1"),
    ("a3 a5 a1 a2 a4", "b3 b2 b1"),
    ("a2 a4 a3 a5 a1", "b1 b3 b2"),
    ("a1 a5 a3 a2 a4", "b2 b3 b1"),
    ("a1 a5 a3 a2 a4", "b2 b3 b1"),
    ("a2 a4 a1 a5 a3", "b1 b2 b3"),
];

/// Report every differing case at once, so one RED run names them all.
fn assert_cases(actual: &[(String, String)], expected: &[(String, String)]) {
    let differing: Vec<String> = actual
        .iter()
        .zip(expected)
        .filter(|(got, want)| got != want)
        .map(|(got, want)| format!("got {got:?}, expected {want:?}"))
        .collect();
    assert!(
        differing.is_empty(),
        "{} of {} cases differ:\n{}",
        differing.len(),
        expected.len(),
        differing.join("\n")
    );
}

// ---- null placement -------------------------------------------------------

#[test]
fn cull_order_by_places_nulls_where_null_order_says() {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for ((case, order_by), (main, removed)) in PLACEMENT_CASES.iter().zip(PLACEMENT_EXPECTED) {
        let (got_main, got_removed) = cull_ids(order_by, FIXTURE_P);
        actual.push((
            format!("{case} main: {got_main}"),
            format!("{case} removed: {got_removed}"),
        ));
        expected.push((
            format!("{case} main: {main}"),
            format!("{case} removed: {removed}"),
        ));
    }
    assert_cases(&actual, &expected);
}

#[test]
fn reshape_order_by_places_nulls_where_null_order_says() {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for ((case, order_by), (group_a, group_b)) in PLACEMENT_CASES.iter().zip(PLACEMENT_EXPECTED) {
        let got = reshape_ids(order_by, FIXTURE_P);
        actual.push((format!("{case}: {got}"), String::new()));
        expected.push((format!("{case}: {group_a} {group_b}"), String::new()));
    }
    assert_cases(&actual, &expected);
}

// ---- the value order ------------------------------------------------------

/// Ascending and descending sequences of fixture V: group `a` on the main
/// port, group `b` on `removed`.
const VALUE_ORDER_EXPECTED: [(&str, &str, &str); 2] = [
    ("asc", "a6 a7 a4 a8 a2 a1 a10 a9 a3 a5", "b2 b1"),
    ("desc", "a3 a5 a9 a10 a1 a2 a8 a4 a6 a7", "b1 b2"),
];

#[test]
fn cull_order_by_follows_the_value_order() {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (direction, group_a, group_b) in VALUE_ORDER_EXPECTED {
        let order_by = format!("[{{ field: k, order: {direction} }}]");
        let (got_main, got_removed) = cull_ids(&order_by, FIXTURE_V);
        actual.push((
            format!("{direction} main: {got_main}"),
            format!("{direction} removed: {got_removed}"),
        ));
        expected.push((
            format!("{direction} main: {group_a}"),
            format!("{direction} removed: {group_b}"),
        ));
    }
    assert_cases(&actual, &expected);
}

#[test]
fn reshape_order_by_follows_the_value_order() {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (direction, group_a, group_b) in VALUE_ORDER_EXPECTED {
        let order_by = format!("[{{ field: k, order: {direction} }}]");
        let got = reshape_ids(&order_by, FIXTURE_V);
        actual.push((format!("{direction}: {got}"), String::new()));
        expected.push((format!("{direction}: {group_a} {group_b}"), String::new()));
    }
    assert_cases(&actual, &expected);
}

// ---- the refusal's own fixes ----------------------------------------------

/// The group-ordering refusal for field `k`, written out in full so a change
/// to the wording is a visible change to this test.
const GROUP_DROP_TEXT: &str = "`null_order: drop` is not allowed on `order_by` for field \
     'k': `order_by` only orders rows within a group and cannot remove them. Use \
     `null_order: first` or `null_order: last`; to exclude rows whose 'k' is null, add a \
     Transform before this node with `filter not k.is_null()`.";

/// The one `null_order: drop` diagnostic a fixture raises.
fn drop_message(yaml: &str) -> String {
    let config = parse_config(yaml).expect("fixture parses");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("`null_order: drop` on an ordering-only field must be refused");
    let matching: Vec<&clinker_core_types::Diagnostic> = diags
        .iter()
        .filter(|diag| diag.message.contains("null_order: drop"))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one `null_order: drop` diagnostic, got {diags:?}"
    );
    assert_eq!(matching[0].code, "E200", "{:?}", matching[0].message);
    matching[0].message.clone()
}

#[test]
fn drop_refusal_fixes_do_what_the_message_says() {
    let dropped = "[{ field: k, null_order: drop }]";
    assert_eq!(
        drop_message(&cull_yaml(dropped, "vals")),
        format!("cull \"culled\": {GROUP_DROP_TEXT}")
    );
    assert_eq!(
        drop_message(&reshape_yaml(dropped, "vals")),
        format!("reshape \"reshaped\": {GROUP_DROP_TEXT}")
    );

    // The first fix the message offers: place the nulls instead of removing
    // them. Every row is still written, where the placement says.
    let first = "[{ field: k, order: asc, null_order: first }]";
    let last = "[{ field: k, order: asc, null_order: last }]";
    assert_eq!(
        cull_ids(first, FIXTURE_P),
        ("a2 a4 a3 a5 a1".to_string(), "b1 b3 b2".to_string()),
        "the `null_order: first` the refusal offers must place the nulls first"
    );
    assert_eq!(
        cull_ids(last, FIXTURE_P),
        ("a3 a5 a1 a2 a4".to_string(), "b3 b2 b1".to_string()),
        "the `null_order: last` the refusal offers must place the nulls last"
    );
    assert_eq!(reshape_ids(first, FIXTURE_P), "a2 a4 a3 a5 a1 b1 b3 b2");
    assert_eq!(reshape_ids(last, FIXTURE_P), "a3 a5 a1 a2 a4 b3 b2 b1");

    // The second fix: the upstream filter. It removes the null-keyed rows,
    // which is what `drop` was reaching for.
    let filtered_cull = cull_yaml("[k]", "kept");
    let filtered_cull = filtered_cull.replace(
        "  - type: cull",
        &format!("{NON_NULL_FILTER}  - type: cull"),
    );
    let outputs = run(&filtered_cull, FIXTURE_P, &["out", "audit"]);
    assert_eq!(
        (ids(&outputs["out"]), ids(&outputs["audit"])),
        ("a3 a5 a1".to_string(), "b3 b2".to_string()),
        "the filter the refusal offers must write exactly the non-null rows"
    );
    let filtered_reshape = reshape_yaml("[k]", "kept");
    let filtered_reshape = filtered_reshape.replace(
        "  - type: reshape",
        &format!("{NON_NULL_FILTER}  - type: reshape"),
    );
    let outputs = run(&filtered_reshape, FIXTURE_P, &["out"]);
    assert_eq!(ids(&outputs["out"]), "a3 a5 a1 b3 b2");
}
