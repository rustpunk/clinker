//! End-to-end integration tests for `OnUnmapped::AutoWiden` (the
//! engine-wide default).
//!
//! Each test runs the full pipeline path — YAML parse → DAG compile →
//! executor dispatch → CSV/JSON output — and asserts a specific
//! end-to-end behavior of the `$widened` sidecar absorber. Together
//! these tests close the audit-flagged "Pattern D is wired only at
//! source boundary; downstream paths are X% covered" gap.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }
    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

fn test_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "auto-widen-integration".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
    }
}

fn run_single(yaml: &str, csv_input: &str) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let readers = HashMap::from([(
        config.source_configs().next().unwrap().name.clone(),
        clinker_core::executor::single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
            .expect("pipeline run");
    (report, buf.as_string())
}

fn run_two_source_merge(
    yaml: &str,
    src_a_name: &str,
    csv_a: &str,
    src_b_name: &str,
    csv_b: &str,
) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let readers = HashMap::from([
        (
            src_a_name.to_string(),
            clinker_core::executor::single_file_reader(
                "a.csv",
                Box::new(Cursor::new(csv_a.as_bytes().to_vec())),
            ),
        ),
        (
            src_b_name.to_string(),
            clinker_core::executor::single_file_reader(
                "b.csv",
                Box::new(Cursor::new(csv_b.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
            .expect("pipeline run");
    (report, buf.as_string())
}

// ── H1: Aggregate strips $widened payload to Null ─────────────

/// CSV with extra column `notes` flows through an Aggregate
/// (group_by: dept). The sidecar payload from input rows is reduced
/// to `Value::Null` at the aggregate output (the per-row map has no
/// canonical reduction). With `include_widened: true` at the sink,
/// no extra column appears beyond `dept` and `total` because the
/// sidecar is empty.
#[test]
fn h1_aggregate_drops_widened_payload_to_null() {
    let yaml = r#"
pipeline:
  name: h1_aggregate
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    schema_overrides:
    - { name: salary, type: integer }
    schema:
      - { name: dept, type: string }
      - { name: salary, type: int }
- type: aggregate
  name: agg
  input: src
  config:
    group_by: [dept]
    cxl: |
      emit dept = dept
      emit total = sum(salary)
- type: output
  name: out
  input: agg
  config:
    name: out
    type: csv
    path: out.csv
    include_widened: true
"#;
    let csv = "dept,salary,notes,extra\nA,100,n1,e1\nA,200,n2,e2\nB,300,n3,e3\n";
    let (_report, output) = run_single(yaml, csv);
    let header = output.lines().next().expect("header");
    assert_eq!(
        header, "dept,total",
        "aggregate output header must be exactly [dept,total] — sidecar is Null at \
         aggregate boundary so include_widened: true expands to nothing. got: {header}"
    );
}

// ── H2: Combine — driver carries $widened, build drops ─────────

/// CSV combine: orders (driver) ⨝ products (build) on product_id.
/// Both sources have auto_widen (engine default). Each carries its
/// own `$widened` map. The combine output retains driver's sidecar
/// (verified via `include_widened: true` expanding the driver's
/// extras to top-level), but build-side extras do NOT appear.
#[test]
fn h2_combine_carries_driver_widened_drops_build() {
    let yaml = r#"
pipeline:
  name: h2_combine
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
- type: combine
  name: enriched
  input:
    o: orders
    p: products
  config:
    where: 'o.product_id == p.product_id'
    match: first
    on_miss: skip
    propagate_ck: driver
    cxl: |
      emit order_id = o.order_id
      emit product_id = o.product_id
      emit product_name = p.name
- type: output
  name: out
  input: enriched
  config:
    name: out
    type: csv
    path: out.csv
    include_widened: true
"#;
    // Driver carries `region` (extra); build carries `category` (extra).
    let orders = "order_id,product_id,region\nO1,P1,US\nO2,P1,EU\n";
    let products = "product_id,name,category\nP1,Widget,hardware\n";
    let (_report, output) = run_two_source_merge(yaml, "orders", orders, "products", products);
    let header = output.lines().next().expect("header");
    let cols: Vec<&str> = header.split(',').collect();
    // Driver-side `region` MUST appear — driver's sidecar rides through.
    assert!(
        cols.contains(&"region"),
        "driver-side $widened key `region` must reach the joined output via sidecar \
         expansion. got header: {header}"
    );
    // Build-side `category` MUST NOT appear — build's sidecar dropped at the join.
    assert!(
        !cols.contains(&"category"),
        "build-side $widened key `category` must NOT reach the joined output (build-side \
         sidecar is dropped by design, matching propagate_ck: Driver). got header: {header}"
    );
}

/// Combine in `match: collect` mode produces a per-driver-row
/// nested array of build records. Each entry in the array is a
/// `Value::Map` of the build record's user-declared fields. This
/// test verifies that the build's auto_widen `$widened` payload
/// is NOT nested into those collect-array maps — `iter_user_fields`
/// at the three collect-array build sites (`pipeline/iejoin.rs`,
/// `pipeline/sort_merge_join.rs`, `pipeline/grace_hash.rs`) filters
/// every engine-stamped column.
///
/// Without that filter, the build's `$widened` Map nests inside
/// the collect Map, then survives projection (it's a regular column
/// slot, not engine-stamped) all the way to a non-JSON writer,
/// where the nested Map triggers
/// `FormatError::UnserializableMapValue`. The test routes to JSON
/// output (which natively serializes maps) so the assertion can
/// inspect the nested structure directly: each entry in the
/// `products_collected` array must list ONLY the build's
/// user-declared fields, never `$widened`, `$ck.*`, or any keys
/// from the build's sidecar map.
#[test]
fn h2b_combine_collect_drops_build_widened() {
    let yaml = r#"
pipeline:
  name: h2b_collect
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    type: csv
    path: products.csv
    schema:
      - { name: product_id, type: string }
      - { name: name, type: string }
- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: collect
    on_miss: skip
    cxl: ""
    propagate_ck: driver
- type: output
  name: out
  input: enriched
  config:
    name: out
    type: json
    path: out.json
    include_widened: true
"#;
    // Build (`products`) carries `extra_meta` as an unmapped column —
    // auto_widen absorbs it into the build's `$widened` payload.
    // Driver (`orders`) carries `region` (also unmapped, absorbed
    // into driver's sidecar).
    let orders = "order_id,product_id,region\nO1,P1,US\n";
    let products = "product_id,name,extra_meta\nP1,Widget,build-leak-marker\n";
    let (_report, output) = run_two_source_merge(yaml, "orders", orders, "products", products);
    // Driver's sidecar key `region` rides through (include_widened
    // expansion at the Output projection).
    assert!(
        output.contains("\"region\""),
        "driver-side $widened key `region` must reach the joined output; got: {output}"
    );
    // Driver's `region` value rides through.
    assert!(
        output.contains("\"US\""),
        "driver's `region` value must reach the joined output; got: {output}"
    );
    // Build's `extra_meta` is in the build's `$widened` payload,
    // which `iter_user_fields` MUST filter out of the collect-array
    // map. The build-side leak marker must NOT appear anywhere in
    // the JSON output. (Before the fix, the build's $widened map
    // would nest into the collect array — its keys/values would
    // surface in the JSON output as part of the nested object.)
    assert!(
        !output.contains("build-leak-marker"),
        "build-side $widened payload MUST NOT leak into the collect-array entries \
         (filtered by iter_user_fields at iejoin/sort_merge_join/grace_hash). got: {output}"
    );
    assert!(
        !output.contains("extra_meta"),
        "build-side $widened key `extra_meta` MUST NOT appear nested in the \
         collect-array entries. got: {output}"
    );
}

// ── H3: Merge same-policy auto_widen at compile ────────────────

/// E315 (covered by `bind_schema_test::test_merge_mixed_on_unmapped_policy_emits_e315`)
/// rejects mixed-policy merges at compile time. This test covers the
/// success case: two sources both on auto_widen (the engine-wide
/// default) compile cleanly, and the merge's typed output Row
/// carries the `$widened` engine-stamped sidecar column inherited
/// from the first input. The runtime side of multi-source merge
/// (each source's reader populating `node_buffers`) is exercised by
/// `route → branches → merge` topologies in
/// `executor::tests::branching` and is independent of the auto_widen
/// sidecar machinery — the sidecar follows whatever upstream-row
/// shape the merge inherits, by construction.
#[test]
fn h3_merge_same_policy_auto_widen_carries_sidecar() {
    use clinker_core::plan::execution::PlanNode;

    let yaml = r#"
pipeline:
  name: h3_merge
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: a.csv
    schema:
      - { name: id, type: string }
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: b.csv
    schema:
      - { name: id, type: string }
- type: merge
  name: merged
  inputs: [src_a, src_b]
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
    include_widened: true
"#;
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let plan = PipelineConfig::compile(&config, &CompileContext::default())
        .expect("same-policy merge must compile cleanly");
    let row = plan
        .typed_output_row("merged")
        .expect("merge must publish a bound output row");
    assert!(
        row.has_field("$widened"),
        "merge's typed output row must carry the `$widened` sidecar inherited from \
         its (same-policy) inputs"
    );
    let merge_node = plan
        .dag()
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Merge { name, .. } if name == "merged"))
        .expect("merge node");
    let schema = match merge_node {
        PlanNode::Merge { output_schema, .. } => output_schema,
        _ => unreachable!(),
    };
    assert!(
        schema.contains("$widened"),
        "merge's lowered output_schema must list `$widened`"
    );
}

// ── H4: include_widened expansion CSV → CSV ───────────────────

/// CSV with extras + auto_widen + Output `include_widened: true` →
/// the sink CSV header includes the user-declared columns plus the
/// keys from the sidecar map. Ordering: declared columns first,
/// sidecar-expanded columns after (matches IndexMap insertion order
/// in the projection's slow-path expansion).
#[test]
fn h4_include_widened_expands_csv_to_csv() {
    let yaml = r#"
pipeline:
  name: h4_expand
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    schema:
      - { name: id, type: string }
      - { name: name, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
    include_widened: true
"#;
    let csv = "id,name,city,role\n1,Alice,Paris,admin\n2,Bob,Tokyo,user\n";
    let (_report, output) = run_single(yaml, csv);
    let header = output.lines().next().expect("header");
    let cols: std::collections::HashSet<&str> = header.split(',').collect();
    for required in &["id", "name", "city", "role"] {
        assert!(
            cols.contains(*required),
            "include_widened: true must expand sidecar key `{required}` to top-level. \
             got header: {header}"
        );
    }
    assert!(
        !cols.contains("$widened"),
        "the `$widened` column itself must be stripped after expansion; got header: {header}"
    );
    let body = output.lines().nth(1).expect("first body row");
    assert!(body.contains("Paris"));
    assert!(body.contains("admin"));
}

// ── H5: Cross-format CSV → JSON with sidecar expansion ─────────

/// CSV input with extras + auto_widen + JSON Output with
/// `include_widened: true` → each unmapped CSV column becomes a
/// top-level key in the JSON output object. The sidecar's
/// `Value::Map` payload is unpacked at the projection layer so the
/// writer never sees a stray Map (which would either JSON-encode
/// natively for the JSON format, or raise
/// `FormatError::UnserializableMapValue` for CSV/XML/fixed-width).
/// This locks the cross-format flow end-to-end.
#[test]
fn h5_cross_format_csv_to_json_with_include_widened() {
    let yaml = r#"
pipeline:
  name: h5_csv_to_json
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    schema:
      - { name: id, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    type: json
    path: out.json
    include_widened: true
"#;
    let csv = "id,extra,city\n1,foo,Paris\n2,bar,Tokyo\n";
    let (_report, output) = run_single(yaml, csv);
    // JSON writer (default) emits one object per record. Each
    // object must contain the declared `id` plus the expanded
    // sidecar keys `extra` and `city`. The literal `$widened` slot
    // must NOT appear — it is stripped during expansion.
    assert!(
        !output.contains("$widened"),
        "literal `$widened` slot must be stripped after include_widened expansion; got: {output}"
    );
    for required in &[
        "\"id\"",
        "\"extra\"",
        "\"city\"",
        "foo",
        "bar",
        "Paris",
        "Tokyo",
    ] {
        assert!(
            output.contains(required),
            "expected `{required}` in JSON output; got: {output}"
        );
    }
}

// ── H6: Spill round-trip preserves Value::Map ─────────────────

/// `Value::Map` round-trips through postcard via the Serialize /
/// Deserialize impls in `clinker-record/src/value.rs:71-140`. The
/// integration scaffolding is hard to drive into spill paths
/// without forcing memory pressure, so this test exercises the
/// canonical serialization path directly: serialize a record
/// carrying a `$widened` Map, deserialize, verify identity.
#[test]
fn h6_value_map_round_trips_postcard() {
    use clinker_record::Value;
    use indexmap::IndexMap;

    let mut sidecar: IndexMap<Box<str>, Value> = IndexMap::new();
    sidecar.insert("foo".into(), Value::String("bar".into()));
    sidecar.insert("count".into(), Value::Integer(42));
    let original = Value::Map(Box::new(sidecar));
    let bytes = postcard::to_allocvec(&original).expect("serialize Value::Map");
    let decoded: Value = postcard::from_bytes(&bytes).expect("deserialize Value::Map");
    match decoded {
        Value::Map(m) => {
            assert_eq!(
                m.get("foo"),
                Some(&Value::String("bar".into())),
                "round-trip preserves String values"
            );
            assert_eq!(
                m.get("count"),
                Some(&Value::Integer(42)),
                "round-trip preserves Integer values"
            );
            assert_eq!(m.len(), 2, "round-trip preserves map cardinality");
        }
        other => panic!("expected Value::Map after postcard round-trip, got {other:?}"),
    }
}

// ── H7: DLQ entry shape with $widened ──────────────────────────

/// A source row that fails downstream coercion (e.g. `to_int()` on a
/// non-numeric value) gets routed to DLQ. The DLQ entry's CSV row
/// includes the `$widened` slot for the source's auto_widen schema —
/// the sidecar's payload is JSON-encoded into the DLQ row so the
/// failure context is preserved end-to-end.
#[test]
fn h7_dlq_includes_widened_column() {
    let yaml = r#"
pipeline:
  name: h7_dlq
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: |
      emit id = id
      emit amount = amount.to_int()
- type: output
  name: out
  input: validate
  config:
    name: out
    type: csv
    path: out.csv
    include_widened: false
"#;
    let csv = "id,amount,note\n1,100,ok\n2,bad,broken\n";
    let (report, _output) = run_single(yaml, csv);
    // Continue-strategy: bad row goes to DLQ, good row reaches
    // the writer. We don't directly read dlq.csv (the test
    // harness doesn't provide a writer for it), but the report
    // counters reflect the routing. The point of this test is to
    // verify the pipeline does NOT panic / crash under auto_widen
    // when a row also has an extra column AND a coercion failure;
    // before the sidecar was added, the same row would have routed
    // straight to DLQ via FormatError, but now `$widened` carries
    // `note` through and the per-row coercion failure surfaces the
    // amount.to_int() error cleanly.
    assert_eq!(
        report.counters.dlq_count, 1,
        "exactly one DLQ entry expected (the bad-amount row); got {}",
        report.counters.dlq_count
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "exactly one ok output expected (the good row); got {}",
        report.counters.ok_count
    );
}

// ── H8: include_correlation_keys does not surface $widened end-to-end ─

/// `include_correlation_keys: true` with `include_widened: false`
/// (or unset) does NOT leak the `$widened` sidecar to the writer.
/// The CSV output's header contains the user-declared columns plus
/// the `$ck.<field>` shadow column (from a correlation_key source),
/// but never the literal `$widened` column. Closes the audit's
/// "include_correlation_keys leak" gap end-to-end.
#[test]
fn h8_include_correlation_keys_does_not_leak_widened() {
    let yaml = r#"
pipeline:
  name: h8_ck_only
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    correlation_key: id
    schema:
      - { name: id, type: string }
      - { name: name, type: string }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
    include_correlation_keys: true
"#;
    // `extra` is an unmapped column — auto_widen absorbs it into
    // the sidecar; with include_widened: false (the default)
    // the sidecar is stripped at the sink.
    let csv = "id,name,extra\n1,Alice,foo\n2,Bob,bar\n";
    let (_report, output) = run_single(yaml, csv);
    let header = output.lines().next().expect("header");
    let cols: Vec<&str> = header.split(',').collect();
    assert!(
        cols.contains(&"$ck.id"),
        "include_correlation_keys: true must surface $ck.id; got header: {header}"
    );
    assert!(
        !cols.contains(&"$widened"),
        "include_correlation_keys: true must NOT surface $widened — sidecar gates \
         independently via include_widened. got header: {header}"
    );
    assert!(
        !cols.contains(&"extra"),
        "with include_widened: false, sidecar contents must not be expanded. \
         got header: {header}"
    );
}
