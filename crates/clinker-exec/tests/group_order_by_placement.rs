//! A Cull or Reshape `order_by` orders the rows of each group exactly as a
//! Sink `sort_order` orders rows.
//!
//! Numbers compare by exact value across integers, floats and decimals,
//! every `NaN` is one value after every number, `-0.0` equals `0.0`, and
//! arrival order breaks ties. A null goes where `null_order` puts it, and
//! `last` is the default for `asc` and `desc` alike. The expected id
//! sequences below are written from that rule, not recorded from a run.
//!
//! The refusal of `null_order: drop` on an ordering-only field gives one
//! fix: delete the setting and add a Transform holding the `config:` line it
//! prints, before the node or after a Source. For a field CXL cannot name,
//! its one next step is the `source_name:` line it prints. The tests here
//! take each printed line from the diagnostic itself, paste it, run the
//! pipeline and check the rows, at every site that refuses `drop`, so the
//! message cannot promise something the engine does not do.

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

/// Insert, before the first line equal to `before`, a Transform `not_null`
/// reading `input` whose whole config is `printed`, pasted exactly as the
/// refusal printed it.
fn paste_filter(yaml: &str, input: &str, before: &str, printed: &str) -> String {
    assert!(yaml.contains(before), "no {before:?} line in:\n{yaml}");
    let transform =
        format!("  - type: transform\n    name: not_null\n    input: {input}\n    {printed}\n");
    yaml.replacen(before, &format!("{transform}{before}"), 1)
}

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
     \"k\": `order_by` only orders the rows of a group, placing nulls `first` or `last`, and \
     cannot remove a row. To remove the rows whose \"k\" is null, delete `null_order: drop` and \
     add a Transform before this node with `config: { cxl: \"filter not k.is_null()\" }`.";

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

/// The one backticked span of `message` that starts with `prefix`: a line
/// the refusal prints for the author to paste. Fails unless there is
/// exactly one, so a refusal that offers a menu cannot pass.
fn printed_span(message: &str, prefix: &str) -> String {
    let spans: Vec<&str> = message
        .split('`')
        .skip(1)
        .step_by(2)
        .filter(|span| span.starts_with(prefix))
        .collect();
    assert_eq!(
        spans.len(),
        1,
        "expected exactly one printed `{prefix}...` line in: {message}"
    );
    spans[0].to_string()
}

/// The `config:` line the refusal prints for the filter Transform.
fn printed_config_line(message: &str) -> String {
    printed_span(message, "config: ")
}

/// The `source_name:` line the refusal prints for a field CXL cannot name.
fn printed_source_name_line(message: &str) -> String {
    printed_span(message, "source_name: ")
}

#[test]
fn drop_refusal_fixes_do_what_the_message_says() {
    let dropped = "[{ field: k, null_order: drop }]";
    let cull_message = drop_message(&cull_yaml(dropped, "vals"));
    assert_eq!(cull_message, format!("cull \"culled\": {GROUP_DROP_TEXT}"));
    let reshape_message = drop_message(&reshape_yaml(dropped, "vals"));
    assert_eq!(
        reshape_message,
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

    // The fix itself: delete `null_order: drop` and paste the printed line
    // as a Transform before the node. It removes the null-keyed rows, which
    // is what `drop` was reaching for.
    let fixed = dropped.replace(", null_order: drop", "");
    let filtered_cull = paste_filter(
        &cull_yaml(&fixed, "not_null"),
        "vals",
        "  - type: cull",
        &printed_config_line(&cull_message),
    );
    let outputs = run(&filtered_cull, FIXTURE_P, &["out", "audit"]);
    assert_eq!(
        (ids(&outputs["out"]), ids(&outputs["audit"])),
        ("a3 a5 a1".to_string(), "b3 b2".to_string()),
        "the printed filter must write exactly the non-null rows"
    );
    let filtered_reshape = paste_filter(
        &reshape_yaml(&fixed, "not_null"),
        "vals",
        "  - type: reshape",
        &printed_config_line(&reshape_message),
    );
    let outputs = run(&filtered_reshape, FIXTURE_P, &["out"]);
    assert_eq!(ids(&outputs["out"]), "a3 a5 a1 b3 b2");
}

// ---- the printed fix at every other site ----------------------------------

/// Source `src` over `id` and a nullable integer `k`, declared sorted by
/// `sort_order`, feeding one Sink that reads `sink_input`.
fn sorted_source_yaml(sort_order: &str, sink_input: &str) -> String {
    format!(
        r#"
pipeline:
  name: sorted_source
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
        - {{ name: k, type: {{ nullable: int }} }}
      sort_order: {sort_order}
  - type: sink
    name: out
    input: {sink_input}
    config: {{ name: out, type: csv, path: out.csv }}
"#
    )
}

/// Sorted by `k` ascending with the null keys last, the order a Source
/// declares once `null_order: drop` is deleted (`last` is the default).
const FIXTURE_S: &str = "id,k\n\
     s1,1\n\
     s2,3\n\
     s3,3\n\
     s4,8\n\
     s5,\n\
     s6,\n";

#[test]
fn the_printed_fix_after_a_source_writes_only_the_non_null_rows() {
    let dropped = "[{ field: k, null_order: drop }]";
    let message = drop_message(&sorted_source_yaml(dropped, "src"));
    let fixed = dropped.replace(", null_order: drop", "");

    // Control: the fixed Source without the filter writes every row in file
    // order, the null-keyed ones last.
    let outputs = run(&sorted_source_yaml(&fixed, "src"), FIXTURE_S, &["out"]);
    assert_eq!(ids(&outputs["out"]), "s1 s2 s3 s4 s5 s6");

    // The fix: the printed line pasted as a Transform after the Source.
    let filtered = paste_filter(
        &sorted_source_yaml(&fixed, "not_null"),
        "src",
        "  - type: sink",
        &printed_config_line(&message),
    );
    let outputs = run(&filtered, FIXTURE_S, &["out"]);
    assert_eq!(
        ids(&outputs["out"]),
        "s1 s2 s3 s4",
        "the printed filter after a Source must write exactly the non-null rows"
    );
}

/// Source `src` over `id`, `dept` and a nullable integer `amount`, a
/// windowed Transform `running` reading `input` and partitioned by `dept`
/// with the given `sort_by`, and one Sink.
fn window_yaml(sort_by: &str, input: &str) -> String {
    format!(
        r#"
pipeline:
  name: window_sort
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
        - {{ name: dept, type: string }}
        - {{ name: amount, type: {{ nullable: int }} }}
  - type: transform
    name: running
    input: {input}
    config:
      analytic_window:
        group_by: [dept]
        sort_by: {sort_by}
      cxl: |
        emit id = id
        emit dept = dept
        emit amount = amount
        emit total = $window.sum(amount)
        emit n = $window.count()
  - type: sink
    name: out
    input: running
    config: {{ name: out, type: csv, path: out.csv }}
"#
    )
}

/// Partition `a` holds 10, null, 30, 20 and partition `b` holds 5, null, 7.
const FIXTURE_W: &str = "id,dept,amount\n\
     w1,a,10\n\
     w2,a,\n\
     w3,b,5\n\
     w4,a,30\n\
     w5,b,\n\
     w6,b,7\n\
     w7,a,20\n";

/// After the filter, partition `a` is w1, w4, w7 (10 + 30 + 20 = 60 over 3
/// rows) and partition `b` is w3, w6 (5 + 7 = 12 over 2 rows). With the null
/// rows still in, each `count()` would be one higher (4 and 3) and w2 and w5
/// would be written. Rows are listed by id, since only the set and the
/// window values are under test. A sum of integers is a float, written
/// without a fraction when it has none.
const WINDOW_FILTERED: &str = "w1,a,10,60,3\n\
     w3,b,5,12,2\n\
     w4,a,30,60,3\n\
     w6,b,7,12,2\n\
     w7,a,20,60,3";

#[test]
fn the_printed_fix_before_a_window_leaves_the_null_rows_out_of_every_partition() {
    let dropped = "[{ field: amount, null_order: drop }]";
    let message = drop_message(&window_yaml(dropped, "src"));
    let fixed = dropped.replace(", null_order: drop", "");
    let filtered = paste_filter(
        &window_yaml(&fixed, "not_null"),
        "src",
        "  - type: transform\n    name: running",
        &printed_config_line(&message),
    );
    let outputs = run(&filtered, FIXTURE_W, &["out"]);
    let mut lines = outputs["out"].lines();
    assert_eq!(lines.next(), Some("id,dept,amount,total,n"));
    let mut rows: Vec<&str> = lines.collect();
    rows.sort_unstable();
    assert_eq!(
        rows.join("\n"),
        WINDOW_FILTERED,
        "the printed filter before a window must leave the null rows out of every partition"
    );
}

/// Source `src` over `id`, `g` and the schema entry `column`, a Cull
/// reading `input` partitioned by `g` with the given `order_by`, whose one
/// rule removes group `b`, and its two Sinks.
fn renamed_cull_yaml(column: &str, order_by: &str, input: &str) -> String {
    format!(
        r#"
pipeline:
  name: renamed_cull
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
        - {column}
  - type: cull
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
    )
}

#[test]
fn the_printed_rename_then_the_printed_filter_remove_the_null_rows() {
    // Each column name CXL cannot write bare, the line the refusal must
    // print for it, and the identifier the author picks.
    let cases = [
        ("order id", r#"source_name: "order id""#, "order_id"),
        ("filter", r#"source_name: "filter""#, "filter_value"),
        (
            "Address.City",
            r#"source_name: "Address.City""#,
            "address_city",
        ),
    ];
    for (field, expected_line, renamed) in cases {
        // The input file keeps its column name. Group `a` holds 3, null,
        // 1, 2 and group `b` holds null, 2; the filter leaves a1, a3, a4
        // ordered a3 a4 a1, and b2 on the removed port.
        let csv = format!(
            "id,g,{field}\n\
             a1,a,3\n\
             b1,b,\n\
             a2,a,\n\
             a3,a,1\n\
             b2,b,2\n\
             a4,a,2\n"
        );
        let column = format!(r#"{{ name: "{field}", type: {{ nullable: int }} }}"#);
        let order_by = format!(r#"[{{ field: "{field}", null_order: drop }}]"#);
        let message = drop_message(&renamed_cull_yaml(&column, &order_by, "src"));
        assert!(
            !message.contains("config: "),
            "{field}: a field CXL cannot name must get no filter to paste: {message}"
        );
        let printed = printed_source_name_line(&message);
        assert_eq!(printed, expected_line, "{field}: {message}");

        // The rename: the printed line on the schema entry, a new
        // identifier as its `name`, and the new name in `order_by`.
        let column = format!("{{ name: {renamed}, type: {{ nullable: int }}, {printed} }}");
        let dropped = format!("[{{ field: {renamed}, null_order: drop }}]");
        let message = drop_message(&renamed_cull_yaml(&column, &dropped, "src"));
        let config_line = printed_config_line(&message);
        assert_eq!(
            config_line,
            format!(r#"config: {{ cxl: "filter not {renamed}.is_null()" }}"#),
            "{field}: planning again must print the filter on the new name"
        );

        // The filter: delete `null_order: drop`, paste the printed line.
        let fixed = dropped.replace(", null_order: drop", "");
        let filtered = paste_filter(
            &renamed_cull_yaml(&column, &fixed, "not_null"),
            "src",
            "  - type: cull",
            &config_line,
        );
        let outputs = run(&filtered, &csv, &["out", "audit"]);
        assert_eq!(
            (ids(&outputs["out"]), ids(&outputs["audit"])),
            ("a3 a4 a1".to_string(), "b2".to_string()),
            "{field}: the rename then the filter must write exactly the non-null rows"
        );
    }
}
