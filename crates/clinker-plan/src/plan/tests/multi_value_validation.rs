//! Plan-time gates on the multi-value surface.
//!
//! E358 rejects a malformed `split_to_rows` / `split_values` declaration on a
//! source; E359 rejects a `multiple: true` column reaching an output whose
//! format has no multi-value encoding. Both used to be either a runtime string
//! error at reader construction (XML only) or nothing at all, so these pin the
//! move to compile time with a diagnostic code and a source span.

use clinker_core_types::Diagnostic;

use crate::config::{CompileContext, parse_config};

/// A single-source pipeline: `source_body` is spliced under the source's
/// `config:` block, and the output writes `output_format`.
fn pipeline(source_body: &str, output_format: &str) -> String {
    format!(
        r#"
pipeline:
  name: multi_value_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      path: ./in.xml
      options:
        record_path: Orders/Order
{source_body}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: {output_format}
      path: out.txt
"#
    )
}

/// The same shape as [`pipeline`] over a JSON source, for the rules that turn
/// on the source's format.
fn json_pipeline(source_body: &str, output_format: &str) -> String {
    format!(
        r#"
pipeline:
  name: multi_value_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
{source_body}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: {output_format}
      path: out.txt
"#
    )
}

fn compile_err(yaml: &str) -> Vec<Diagnostic> {
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect_err("compile must fail")
}

fn compile_ok(yaml: &str) {
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .unwrap_or_else(|d| panic!("compile must succeed, got: {d:?}"));
}

/// Every diagnostic carrying `code`, as `(message, has_span)`.
fn coded(diags: &[Diagnostic], code: &str) -> Vec<(String, bool)> {
    diags
        .iter()
        .filter(|d| d.code == code)
        .map(|d| {
            (
                d.message.clone(),
                d.primary.span != clinker_core_types::span::Span::SYNTHETIC,
            )
        })
        .collect()
}

#[test]
fn duplicate_split_to_rows_field_is_rejected_with_a_span() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Item
      schema:
        - { name: "Item.name", type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("more than once"), "{}", found[0].0);
    assert!(found[0].1, "E358 must carry a source span");
}

#[test]
fn nested_split_to_rows_fields_are_rejected() {
    // This ran at XML reader construction as an unspanned string error; it
    // now fails at compile, before any input is opened.
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Item.part
      schema:
        - { name: "Item.part.code", type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("nest"), "{}", found[0].0);
}

/// On JSON the two entries compose only when the outer keeps the group's
/// dotted path. `mode: extract` — the default — lifts the occurrence's own keys
/// to the top level, so `orders.items` stops existing, the inner entry matches
/// nothing, and `keep_empty`'s default passes the record through unfanned: one
/// record where the author asked for one per line item.
#[test]
fn nested_split_to_rows_under_extract_mode_is_rejected_on_json() {
    let yaml = json_pipeline(
        r#"      split_to_rows:
        - orders
        - orders.items
      schema:
        - { name: sku, type: string }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(
        found[0].0.contains("nest") && found[0].0.contains("mode: extract"),
        "{}",
        found[0].0
    );
    assert!(found[0].1, "E358 must carry a source span");
}

/// The pairing the reader really does expand two levels deep.
#[test]
fn nested_split_to_rows_under_split_mode_compiles_on_json() {
    let yaml = json_pipeline(
        r#"      split_to_rows:
        - { field: orders, mode: split }
        - { field: orders.items, mode: split }
      schema:
        - { name: "orders.items.sku", type: string }"#,
        "json",
    );
    compile_ok(&yaml);
}

/// XML tracks occurrence membership positionally, so nesting is ambiguous there
/// whatever the mode — `split` does not rescue it the way it does on JSON.
#[test]
fn nested_split_to_rows_under_split_mode_is_still_rejected_on_xml() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - { field: Item, mode: split }
        - { field: Item.part, mode: split }
      schema:
        - { name: "Item.part.code", type: string }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(found[0].0.contains("xml source"), "{}", found[0].0);
}

#[test]
fn disjoint_split_to_rows_fields_compile() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Tag
      schema:
        - { name: "Item.name", type: string }
        - { name: Tag, type: string }"#,
        "csv",
    );
    compile_ok(&yaml);
}

#[test]
fn split_values_on_a_single_valued_column_is_rejected() {
    // Splitting text on a delimiter produces several values, which only a
    // `multiple: true` column can hold.
    let yaml = pipeline(
        r#"      split_values:
        - field: tags
      schema:
        - { name: tags, type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("multiple: true"), "{}", found[0].0);
}

#[test]
fn duplicate_split_values_field_is_rejected() {
    let yaml = pipeline(
        r#"      split_values:
        - tags
        - field: tags
          delimiter: "|"
      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("more than once"), "{}", found[0].0);
}

#[test]
fn multi_value_column_into_a_json_output_compiles() {
    // JSON is the one output format that can encode a multi-value field
    // today, so the E359 capability table lets it through.
    let yaml = pipeline(
        r#"      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    compile_ok(&yaml);
}

#[test]
fn multi_value_column_into_a_non_encoding_output_is_rejected_with_a_span() {
    for format in ["csv", "xml", "fixed_width"] {
        let yaml = pipeline(
            r#"      schema:
        - { name: tags, type: string, multiple: true }"#,
            format,
        );
        let found = coded(&compile_err(&yaml), "E359");
        assert_eq!(found.len(), 1, "expected one E359 for {format}");
        assert!(
            found[0].0.contains("'tags'") && found[0].0.contains(format),
            "{format}: {}",
            found[0].0
        );
        assert!(found[0].1, "E359 must carry a source span for {format}");
    }
}

#[test]
fn empty_split_values_delimiter_is_rejected() {
    let yaml = pipeline(
        r#"      split_values:
        - field: tags
          delimiter: ""
      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("empty delimiter"), "{}", found[0].0);
}

#[test]
fn two_entries_writing_one_position_column_are_rejected() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - field: Item
          position_column: line_no
        - field: Tag
          position_column: line_no
      schema:
        - { name: line_no, type: int }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("position column"), "{}", found[0].0);
}

#[test]
fn a_field_in_both_blocks_is_rejected() {
    // Fanning a field out consumes it; parsing it in-cell keeps it and makes
    // it multi-valued. Declaring both leaves no single shape for the column.
    let yaml = pipeline(
        r#"      split_to_rows:
        - tags
      split_values:
        - tags
      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    // Two independent faults: the field is in both blocks, and the column it
    // names is `multiple: true` while also being fanned out.
    assert_eq!(found.len(), 2, "expected two E358: {found:?}");
    assert!(
        found.iter().any(|(message, _)| message.contains("both")),
        "no both-blocks fault: {found:?}"
    );
    assert!(
        found
            .iter()
            .any(|(message, _)| message.contains("multiple: true")),
        "no multiple-and-fanned-out fault: {found:?}"
    );
}

#[test]
fn split_values_naming_an_undeclared_field_says_so() {
    // A typo must not be reported as "add `multiple: true` to the 'ghost'
    // column" — there is no such column to add it to.
    let yaml = pipeline(
        r#"      split_values:
        - ghost
      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(
        found[0].0.contains("does not declare at all"),
        "{}",
        found[0].0
    );
}

/// A multi-value column reaching a non-encoding output through an intervening
/// node, rather than on a direct source→output edge.
#[test]
fn multi_value_column_is_traced_through_an_intervening_transform() {
    let yaml = r#"
pipeline:
  name: through_transform
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      schema:
        - { name: id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: transform
    name: pick
    input: src
    config:
      cxl: |
        emit id = id
  - type: output
    name: out
    input: pick
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let found = coded(&compile_err(yaml), "E359");
    assert_eq!(found.len(), 1, "expected one E359 through the transform");
}

/// Nothing requires a node to be declared after its producer, and the gate has
/// to hold whichever order the author wrote. A single declaration-order pass
/// leaves the output's reaching-source set empty and the run dies at the CSV
/// writer on record 1 instead.
#[test]
fn multi_value_column_is_caught_with_the_output_declared_before_its_producer() {
    let yaml = r#"
pipeline:
  name: out_before_producer
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      schema:
        - { name: id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: output
    name: out
    input: pick
    config:
      name: out
      type: csv
      path: out.csv
  - type: transform
    name: pick
    input: src
    config:
      cxl: |
        emit id = id
"#;
    let diags = compile_err(yaml);
    let found = coded(&diags, "E359");
    assert_eq!(
        found.len(),
        1,
        "declaration order must not decide whether the gate fires: {diags:?}"
    );
}

/// The output's own `schema:` block is the same `Column` type and accepts
/// `multiple: true`, but no writer honors it — the fixed-width writer's
/// `Column -> FieldDef` conversion drops the attribute outright. It is the
/// surface an author is most likely to write the declaration on.
#[test]
fn multi_value_column_on_an_outputs_own_schema_is_rejected() {
    let yaml = r#"
pipeline:
  name: output_schema_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      schema:
        - { name: id, type: string }
        - { name: codes, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      schema:
        - { name: id, type: string }
        - { name: codes, type: string, multiple: true }
"#;
    let found = coded(&compile_err(yaml), "E359");
    assert_eq!(found.len(), 1, "expected one E359: {found:?}");
    assert!(
        found[0].0.contains("its own `schema:`") && found[0].0.contains("'codes'"),
        "{}",
        found[0].0
    );
    assert!(found[0].1, "E359 must carry a source span");
}

/// The same declaration on a `json` output is honored, so it compiles.
#[test]
fn multi_value_column_on_a_json_outputs_own_schema_compiles() {
    let yaml = r#"
pipeline:
  name: output_schema_json
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      schema:
        - { name: id, type: string }
        - { name: codes, type: string, multiple: true }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: json
      path: out.json
      schema:
        - { name: id, type: string }
        - { name: codes, type: string, multiple: true }
"#;
    compile_ok(yaml);
}

/// A combine projects columns off its REFERENCE input, not just its driver, so
/// reachability for this gate must union every input — the `$doc` attribution
/// walk narrows a combine to its driving input and would miss this.
#[test]
fn multi_value_column_on_a_combines_reference_input_is_caught() {
    let yaml = r#"
pipeline:
  name: combine_reference
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./orders.json
      schema:
        - { name: id, type: string }
  - type: source
    name: events
    config:
      name: events
      type: json
      path: ./events.json
      schema:
        - { name: id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: combine
    name: enriched
    input:
      orders: orders
      events: events
    config:
      where: "orders.id == events.id"
      match: first
      on_miss: null_fields
      cxl: |
        emit id = orders.id
        emit tags = events.tags
      propagate_ck: driver
  - type: output
    name: report
    input: enriched
    config:
      name: report
      type: csv
      path: out.csv
"#;
    let found = coded(&compile_err(yaml), "E359");
    assert_eq!(
        found.len(),
        1,
        "expected one E359 for the combine's reference input"
    );
    assert!(found[0].0.contains("events"), "{}", found[0].0);
}

/// `multiple: true` inside a multi-record schema's record types is the same
/// `Column` attribute and reaches the same superset, so the gate must see it.
#[test]
fn multi_value_column_inside_a_multi_record_schema_is_caught() {
    let yaml = r#"
pipeline:
  name: multi_record
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: ./in.csv
      schema:
        discriminator: { field: kind }
        records:
          - id: header
            tag: H
            columns:
              - { name: kind, type: string }
              - { name: batch, type: string }
          - id: detail
            tag: D
            columns:
              - { name: kind, type: string }
              - { name: tags, type: string, multiple: true }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let found = coded(&compile_err(yaml), "E359");
    assert_eq!(
        found.len(),
        1,
        "expected one E359 for the record-type column"
    );
    assert!(found[0].0.contains("'tags'"), "{}", found[0].0);
}

#[test]
fn a_source_with_no_multi_value_column_is_unaffected() {
    let yaml = pipeline(
        r#"      schema:
        - { name: tags, type: string }"#,
        "csv",
    );
    compile_ok(&yaml);
}

/// A node's `name:` and its nested `config.name:` are independent, and the
/// reachability walk is built from node names. Keying the gate on the config
/// name instead left every pipeline that names them differently uncovered.
#[test]
fn e359_fires_when_the_node_name_and_the_config_name_differ() {
    let yaml = r#"
pipeline:
  name: name_mismatch
nodes:
  - type: source
    name: orders_src
    config:
      name: orders
      type: json
      path: ./orders.json
      schema:
        - { name: id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: output
    name: report_sink
    input: orders_src
    config:
      name: report
      type: csv
      path: out.csv
"#;
    let found = coded(&compile_err(yaml), "E359");
    assert_eq!(found.len(), 1, "expected one E359: {found:?}");
    assert!(found[0].0.contains("'tags'"), "{}", found[0].0);
    assert!(found[0].1, "E359 must carry a source span");
}

/// The gates read declared columns, which an unresolved external schema has
/// none of. They must therefore see the file's contents, not the `File`
/// placeholder that stands in for it until later in compilation.
#[test]
fn e359_sees_a_schema_declared_in_an_external_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema_path = dir.path().join("orders.schema.yaml");
    std::fs::write(
        &schema_path,
        "- { name: id, type: string }\n- { name: tags, type: string, multiple: true }\n",
    )
    .expect("write schema file");
    let yaml = format!(
        r#"
pipeline:
  name: external_schema
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./orders.json
      schema: {}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#,
        schema_path.display()
    );
    let found = coded(&compile_err(&yaml), "E359");
    assert_eq!(found.len(), 1, "expected one E359: {found:?}");
    assert!(found[0].0.contains("'tags'"), "{}", found[0].0);
}

#[test]
fn e358_sees_a_schema_declared_in_an_external_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema_path = dir.path().join("orders.schema.yaml");
    std::fs::write(&schema_path, "- { name: tags, type: string }\n").expect("write schema file");
    let yaml = format!(
        r#"
pipeline:
  name: external_schema
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./orders.json
      split_values:
        - tags
      schema: {}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: json
      path: out.json
"#,
        schema_path.display()
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(found[0].0.contains("multiple: true"), "{}", found[0].0);
}

/// The disjointness rule is the XML reader's positional occurrence tracking,
/// not a property of the declaration. The JSON reader applies entries in
/// declaration order over the flattened record, where the outer fan-out leaves
/// the inner field addressable for the next entry.
#[test]
fn nested_split_to_rows_fields_compile_on_a_json_source() {
    let yaml = json_pipeline(
        r#"      split_to_rows:
        - { field: orders, mode: split }
        - { field: orders.items, mode: split }
      schema:
        - { name: "orders.items.sku", type: string }"#,
        "json",
    );
    compile_ok(&yaml);
}

/// `split_values` runs against the names the DOCUMENT carries, so an aliased
/// column is addressable only by its `source_name`. Accepting the exposed name
/// compiled clean and then never matched at run time.
#[test]
fn split_values_naming_an_aliased_columns_exposed_name_is_rejected() {
    let yaml = pipeline(
        r#"      split_values:
        - cost_centres
      schema:
        - { name: cost_centres, source_name: CostCentres, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(found[0].0.contains("exposed name"), "{}", found[0].0);
    assert!(found[0].0.contains("CostCentres"), "{}", found[0].0);
}

#[test]
fn split_values_naming_the_physical_field_of_an_aliased_column_compiles() {
    let yaml = pipeline(
        r#"      split_values:
        - CostCentres
      schema:
        - { name: cost_centres, source_name: CostCentres, type: string, multiple: true }"#,
        "json",
    );
    compile_ok(&yaml);
}

#[test]
fn split_to_rows_field_path_with_an_empty_segment_is_rejected() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Items..Item
      schema:
        - { name: sku, type: string }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(found[0].0.contains("empty path segment"), "{}", found[0].0);
}

/// `multiple: true` collects a field's occurrences into one array; a fan-out
/// spends them one per record. Declaring both asks for two shapes at once.
#[test]
fn fanning_out_a_multiple_true_column_is_rejected() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Tag
      schema:
        - { name: Tag, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358: {found:?}");
    assert!(found[0].0.contains("multiple: true"), "{}", found[0].0);
}

/// A source config is flattened into its node, which rules out rejecting
/// unknown keys, so the retired key is parsed only to be rejected here — a
/// pipeline that kept it would otherwise run and emit one record per document
/// where it used to emit one per array element.
#[test]
fn the_retired_array_paths_key_is_rejected_with_a_span() {
    let yaml = json_pipeline(
        r#"      array_paths:
        - path: line_items
          mode: explode
      schema:
        - { name: sku, type: string }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E360");
    assert_eq!(found.len(), 1, "expected one E360: {found:?}");
    assert!(found[0].0.contains("array_paths"), "{}", found[0].0);
    assert!(found[0].1, "E360 must carry a source span");
}

/// A `multiple: true` column binds as an array, and an array carries no event
/// time. Reading the declared element type instead let the pipeline compile
/// and then leave the watermark frozen with no diagnostic.
#[test]
fn a_watermark_on_a_multi_value_column_is_rejected() {
    let yaml = json_pipeline(
        r#"      watermark:
        column: event_ts
      schema:
        - { name: id, type: string }
        - { name: event_ts, type: date_time, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E155");
    assert_eq!(found.len(), 1, "expected one E155: {found:?}");
    assert!(found[0].0.contains("event_ts"), "{}", found[0].0);
}

#[test]
fn a_watermark_on_a_single_valued_date_column_still_compiles() {
    let yaml = json_pipeline(
        r#"      watermark:
        column: event_ts
      schema:
        - { name: id, type: string }
        - { name: event_ts, type: date_time }"#,
        "json",
    );
    compile_ok(&yaml);
}

/// A pipeline over `source_format`, whose source body is `source_body`, writing
/// JSON so nothing but the read-side gate can fire.
fn source_format_pipeline(source_format: &str, source_body: &str) -> String {
    format!(
        r#"
pipeline:
  name: multi_value_input_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: {source_format}
      path: ./in.dat
{source_body}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: json
      path: out.json
"#
    )
}

/// The read-side arrow of E359: a `multiple: true` column binds as an array
/// for every format, so a source whose reader cannot produce one would hand
/// CXL a scalar where it typechecked an array.
#[test]
fn e361_fires_on_a_csv_source_with_a_bare_multi_value_column() {
    let yaml = source_format_pipeline(
        "csv",
        r#"      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }"#,
    );
    let found = coded(&compile_err(&yaml), "E361");
    assert_eq!(found.len(), 1, "expected one E361: {found:?}");
    assert!(
        found[0].0.contains("'tags'") && found[0].0.contains("csv"),
        "{}",
        found[0].0
    );
    assert!(found[0].1, "E361 must carry a source span");
}

/// A `split_values` entry now rescues a `multiple: true` column on a
/// single-schema delimited-cell source: the CSV and fixed-width readers parse
/// the cell into the array the column holds (#930), so the pair that used to
/// fault E361 compiles clean. The JSON output encodes the resulting array.
#[test]
fn e361_accepts_a_delimited_cell_column_a_split_values_entry_covers() {
    let csv = source_format_pipeline(
        "csv",
        r#"      split_values:
        - field: tags
          delimiter: ";"
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }"#,
    );
    compile_ok(&csv);

    let fixed_width = source_format_pipeline(
        "fixed_width",
        r#"      split_values:
        - field: tags
          delimiter: ";"
      schema:
        - { name: order_id, type: string, start: 0, width: 4 }
        - { name: tags, type: string, start: 4, width: 20, multiple: true }"#,
    );
    compile_ok(&fixed_width);
}

/// Coverage is per column: an entry rescues the column it names and no other.
/// A source with two `multiple: true` columns and one `split_values` entry
/// accepts the covered column and still faults the uncovered one.
#[test]
fn e361_faults_only_the_delimited_cell_columns_no_split_values_entry_covers() {
    let yaml = source_format_pipeline(
        "csv",
        r#"      split_values:
        - field: tags
          delimiter: ";"
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }"#,
    );
    let found = coded(&compile_err(&yaml), "E361");
    assert_eq!(found.len(), 1, "expected one E361: {found:?}");
    assert!(
        found[0].0.contains("'codes'"),
        "the uncovered column must fault: {}",
        found[0].0
    );
    assert!(
        !found[0].0.contains("'tags'"),
        "the covered column must not fault: {}",
        found[0].0
    );
}

/// The other half of the same hole: a declaration a format's reader never
/// receives is a silent no-op, reported on its own rather than left to be
/// inferred from the E361 above. Each declaration kind has its own capability —
/// the delimited-cell formats read `split_values` but not `split_to_rows`, so a
/// `split_to_rows` block on `csv` is the no-op here even though `split_values`
/// is not.
#[test]
fn e358_rejects_multi_value_declarations_a_format_never_receives() {
    for (format, key, body) in [
        (
            "csv",
            "split_to_rows",
            r#"      split_to_rows:
        - line_items
      schema:
        - { name: sku, type: string }"#,
        ),
        (
            "fixed_width",
            "split_to_rows",
            r#"      split_to_rows:
        - line_items
      schema:
        - { name: sku, type: string }"#,
        ),
        (
            "x12",
            "split_to_rows",
            r#"      split_to_rows:
        - line_items
      schema:
        - { name: sku, type: string }"#,
        ),
    ] {
        let yaml = source_format_pipeline(format, body);
        let found = coded(&compile_err(&yaml), "E358");
        assert_eq!(found.len(), 1, "expected one E358 for {format}: {found:?}");
        assert!(
            found[0].0.contains(key) && found[0].0.contains("silent no-op"),
            "{format}: {}",
            found[0].0
        );
        assert!(found[0].1, "{format}: E358 must carry a source span");
    }
}

/// `split_values` is consumed only by the single-schema CSV and fixed-width
/// readers; a multi-record source of either format runs a different backend that
/// never receives the block, so declaring it there is the same silent no-op E358
/// rejects on any other unreading format. The guard keeps the gate flip from
/// opening a hole the multi-record path cannot honor.
#[test]
fn split_values_on_a_multi_record_csv_source_is_a_no_op() {
    let yaml = r#"
pipeline:
  name: multi_record_split_values
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: ./in.csv
      split_values:
        - field: tags
          delimiter: ";"
      schema:
        discriminator: { field: kind }
        records:
          - id: header
            tag: H
            columns:
              - { name: kind, type: string }
          - id: detail
            tag: D
            columns:
              - { name: kind, type: string }
              - { name: tags, type: string, multiple: true }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: json
      path: out.json
"#;
    let found = coded(&compile_err(yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358 no-op: {found:?}");
    assert!(
        found[0].0.contains("split_values") && found[0].0.contains("silent no-op"),
        "{}",
        found[0].0
    );
}

/// Repetition in a segment format is a positional coordinate, not a list, so no
/// `split_values` entry rescues the declaration — the help must not promise one.
#[test]
fn e361_fires_on_every_segment_format_and_offers_no_in_cell_route() {
    for format in ["edifact", "x12", "hl7", "swift"] {
        let yaml = source_format_pipeline(
            format,
            r#"      split_values:
        - field: tags
          delimiter: ";"
      schema:
        - { name: tags, type: string, multiple: true }"#,
        );
        let diags = compile_err(&yaml);
        let found = coded(&diags, "E361");
        assert_eq!(found.len(), 1, "expected one E361 for {format}: {found:?}");
        assert!(found[0].0.contains("'tags'"), "{format}: {}", found[0].0);
        let help = diags
            .iter()
            .find(|d| d.code == "E361")
            .and_then(|d| d.help.clone())
            .unwrap_or_else(|| panic!("{format}: E361 must carry help"));
        assert!(
            help.contains("positional axis"),
            "{format} help must name the positional axis: {help}"
        );
        assert!(
            !help.contains("split_values"),
            "{format} help must not dangle an in-cell route: {help}"
        );
    }
}

/// HL7 is the one segment format that exposes the positional axes as its own
/// declaration, so its help names that rather than only saying no.
#[test]
fn e361_points_an_hl7_source_at_split_fields() {
    let yaml = source_format_pipeline(
        "hl7",
        r#"      schema:
        - { name: tags, type: string, multiple: true }"#,
    );
    let help = compile_err(&yaml)
        .iter()
        .find(|d| d.code == "E361")
        .and_then(|d| d.help.clone())
        .expect("E361 with help");
    assert!(help.contains("split_fields"), "{help}");
}

#[test]
fn e361_does_not_fire_on_json_or_xml_sources() {
    for format in ["json", "xml"] {
        let yaml = source_format_pipeline(
            format,
            r#"      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }"#,
        );
        compile_ok(&yaml);
    }
}

/// A source with no multi-value column is untouched whatever its format.
#[test]
fn e361_is_silent_without_a_multi_value_column() {
    for format in ["csv", "x12", "json"] {
        let yaml = source_format_pipeline(
            format,
            r#"      schema:
        - { name: order_id, type: string }"#,
        );
        compile_ok(&yaml);
    }
}
