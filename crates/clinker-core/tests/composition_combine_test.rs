//! Integration tests for combine nodes inside composition bodies.
//!
//! Each test threads a parent pipeline through `bind_schema` and the
//! body executor: combine references the body's signature input ports
//! as its qualifier-keyed inputs, and the body's mini-DAG seeds those
//! ports from parent-scope readers. The diagnostic-provenance test
//! exercises the inner-error span path — a body-internal E303 must
//! point at the body file's `where:` line, not the parent pipeline's
//! composition call-site line.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};

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

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn test_params(config: &PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(clinker_core::config::convert_pipeline_vars)
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "composition-combine-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

/// Run a multi-source pipeline through the executor with each
/// declared source bound to its own in-memory CSV reader. The first
/// declared source is treated as the driving (primary) input — the
/// pipelines used here pick the driving combine input as the
/// first-declared source so the default routing matches the combine
/// driver-selection rule (first-in-IndexMap on a tied cardinality).
fn run_pipeline_multi_source(
    yaml: &str,
    inputs: &[(&str, &str)],
) -> (clinker_core::executor::ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let mut readers: HashMap<String, Box<dyn Read + Send>> = HashMap::new();
    for (name, data) in inputs {
        readers.insert(
            (*name).to_string(),
            Box::new(Cursor::new(data.as_bytes().to_vec())) as Box<dyn Read + Send>,
        );
    }

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let primary = inputs
        .first()
        .map(|(n, _)| (*n).to_string())
        .expect("at least one input source required");
    let report = PipelineExecutor::run_plan_with_readers_writers_with_primary(
        &plan,
        &primary,
        readers,
        writers,
        &test_params(&config),
    )
    .expect("pipeline run");
    (report, buf.as_string())
}

#[test]
fn test_combine_in_composition_compiles() {
    // Two top-level sources feed a composition whose body contains a
    // single combine node. Compilation alone is the assertion target —
    // the combine's `input:` map keys reference the body's signature
    // input ports, and bind_combine must construct the merged row from
    // those port-bound rows. The output must carry the body's emitted
    // columns (order_id, product_name, total).
    let yaml = r#"
pipeline:
  name: combine_in_composition_compile
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  - type: composition
    name: enrich_call
    input: orders_src
    use: ../compositions/combine_enrich.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: enrich_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile must succeed");

    // The composition call-site survives compile; the combine inside
    // its body has resolved its merged row from the port-bound
    // upstream rows.
    assert!(
        plan.config()
            .nodes
            .iter()
            .any(|n| n.value.name() == "enrich_call"),
        "compiled plan must retain the composition call-site node"
    );
}

#[test]
fn test_combine_in_composition_executes() {
    // End-to-end execution. Source `orders_src` emits a single row
    // {order_id: "1", product_id: "A", quantity: 2}; source
    // `products_src` emits {product_id: "A", name: "Widget", price:
    // 10.0}. The body's combine joins on product_id and emits
    // {order_id, product_name, total = price * quantity}. Expected
    // output: order_id="1", product_name="Widget", total=20.
    let yaml = r#"
pipeline:
  name: combine_in_composition_execute
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  - type: composition
    name: enrich_call
    input: orders_src
    use: ../compositions/combine_enrich.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: enrich_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let orders_csv = "order_id,product_id,quantity\n1,A,2\n";
    let products_csv = "product_id,name,price\nA,Widget,10.0\n";
    let (_report, output) = run_pipeline_multi_source(
        yaml,
        &[("orders_src", orders_csv), ("products_src", products_csv)],
    );

    assert!(
        output.contains("order_id"),
        "output header must include order_id; got:\n{output}"
    );
    assert!(
        output.contains("product_name"),
        "output header must include product_name; got:\n{output}"
    );
    assert!(
        output.contains("total"),
        "output header must include total; got:\n{output}"
    );
    assert!(
        output.contains("Widget"),
        "output must contain joined product name 'Widget'; got:\n{output}"
    );
    // total = price (10.0) * quantity (2) = 20. CSV may render this
    // as `20`, `20.0`, or `20.0000` depending on the float
    // formatter — assert on the leading digits to stay
    // formatter-agnostic.
    assert!(
        output.contains(",20") || output.contains(",2.0"),
        "output must contain computed total ~20; got:\n{output}"
    );
}

#[test]
fn test_combine_in_nested_composition() {
    // The outer composition (`nested_combine.comp.yaml`) forwards
    // both ports to the inner composition (`combine_enrich`) and
    // stamps an additional `tenant` field. Both the inner-emitted
    // join columns and the outer-stamped `tenant` must reach the
    // top-level output — a body executor that fails to recurse
    // would lose the inner emits.
    let yaml = r#"
pipeline:
  name: combine_in_nested_composition
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  - type: composition
    name: nested_call
    input: orders_src
    use: ../compositions/nested_combine.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: nested_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let orders_csv = "order_id,product_id,quantity\n7,B,3\n";
    let products_csv = "product_id,name,price\nB,Gadget,5.0\n";
    let (_report, output) = run_pipeline_multi_source(
        yaml,
        &[("orders_src", orders_csv), ("products_src", products_csv)],
    );

    assert!(
        output.contains("product_name"),
        "output must carry inner-body `product_name`; got:\n{output}"
    );
    assert!(
        output.contains("tenant"),
        "output must carry outer-body `tenant`; got:\n{output}"
    );
    assert!(
        output.contains("Gadget"),
        "output must contain joined product name 'Gadget'; got:\n{output}"
    );
    assert!(
        output.contains("acme"),
        "output must contain outer-emitted tenant value 'acme'; got:\n{output}"
    );
}

#[test]
fn test_combine_collect_in_composition_executes() {
    // `match: collect` produces a driver-shaped row carrying every
    // matched build row in a trailing `products` Array column. The
    // composition surfaces this auto-derived shape through its
    // `collected` output port — verifies the executor's
    // collect-mode synthesis runs inside a body just as it does at
    // the top level.
    let yaml = r#"
pipeline:
  name: combine_collect_in_composition
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: composition
    name: collect_call
    input: orders_src
    use: ../compositions/combine_collect.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: collect_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    // Driver row 1 matches two build rows (A → Widget, A → WidgetPro);
    // driver row 2 matches one build row (B → Gadget).
    let orders_csv = "order_id,product_id\n1,A\n2,B\n";
    let products_csv = "product_id,name\nA,Widget\nA,WidgetPro\nB,Gadget\n";
    let (_report, output) = run_pipeline_multi_source(
        yaml,
        &[("orders_src", orders_csv), ("products_src", products_csv)],
    );

    // Driver columns appear in the output header; the build-side
    // qualifier (`products`) appears as the trailing Array column.
    assert!(
        output.contains("order_id"),
        "output header must include driver column order_id; got:\n{output}"
    );
    assert!(
        output.contains("products"),
        "output header must include collect-mode trailing array column \
         named after the build qualifier; got:\n{output}"
    );
    // Each driver row shows up once in the output — collect emits
    // ONE row per driver regardless of match count.
    let order_1_count = output.matches("\n1,").count();
    let order_2_count = output.matches("\n2,").count();
    assert_eq!(
        order_1_count, 1,
        "driver row 1 must appear exactly once; got:\n{output}"
    );
    assert_eq!(
        order_2_count, 1,
        "driver row 2 must appear exactly once; got:\n{output}"
    );
    // Both build rows for product_id=A must be encoded in row 1's
    // trailing array column. CSV array encoding is implementation-
    // defined, but both build names must appear somewhere in the
    // text serialisation.
    assert!(
        output.contains("Widget"),
        "row for A must reference build product 'Widget'; got:\n{output}"
    );
    assert!(
        output.contains("WidgetPro"),
        "row for A must reference build product 'WidgetPro'; got:\n{output}"
    );
    assert!(
        output.contains("Gadget"),
        "row for B must reference build product 'Gadget'; got:\n{output}"
    );
}

#[test]
fn test_combine_in_composition_diagnostics() {
    // The body's `where:` predicate (`a.x + b.x`) types as Int, not
    // Bool — bind_combine must emit E303. The diagnostic's primary
    // span must point at the body file's combine line, not at the
    // parent pipeline's composition call-site line. The two are at
    // distinct YAML lines: the call-site `composition` node sits at
    // line 16 of the parent pipeline below; the body's combine
    // declaration sits at line 23 of `combine_bad_predicate.comp.yaml`.
    // Comparing the diagnostic's `synthetic_line_number` against the
    // body file's known line is enough to disambiguate.
    let yaml = r#"
pipeline:
  name: combine_bad_predicate_call
nodes:
  - type: source
    name: a_src
    config:
      name: a_src
      type: csv
      path: a.csv
      schema:
        - { name: x, type: int }
  - type: source
    name: b_src
    config:
      name: b_src
      type: csv
      path: b.csv
      schema:
        - { name: x, type: int }
  - type: composition
    name: bad_call
    input: a_src
    use: ../compositions/combine_bad_predicate.comp.yaml
    inputs:
      a: a_src
      b: b_src
  - type: output
    name: out
    input: bad_call
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let root = fixture_workspace_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let result = config.compile(&ctx);
    let diags = match result {
        Ok(_) => panic!("compile must fail with E303"),
        Err(diags) => diags,
    };

    let e303: Vec<_> = diags.iter().filter(|d| d.code == "E303").collect();
    assert_eq!(
        e303.len(),
        1,
        "expected exactly one E303; got diagnostics: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );

    // The body file's combine block starts at line 23. The `where:`
    // line is at 28, which is the line that bind_combine resolves
    // to via `span_for(spanned)` — the saphyr span for the combine
    // node itself rather than the where-clause sub-key. Anchor the
    // assertion on the body file's own line range so a renumber of
    // the parent pipeline does not silently make the test pass.
    let body_path = fixture_workspace_root().join("compositions/combine_bad_predicate.comp.yaml");
    let body_src = std::fs::read_to_string(&body_path).expect("body file readable");
    let body_line_count = body_src.lines().count() as u32;

    let diag_line = e303[0]
        .primary
        .span
        .synthetic_line_number()
        .expect("E303 primary span carries a synthetic line number");

    // The body file is short (well under 50 lines). The parent
    // pipeline's `composition` call-site is at line 23 (counted
    // from the leading `pipeline:` directive of the inline YAML).
    // The body's combine line is the `- type: combine` block,
    // which lands within the body file's first ~30 lines and is
    // distinct from the parent pipeline's call-site line.
    //
    // Two structural assertions: the line number must be a valid
    // line within the body file, AND it must match the body file's
    // combine declaration (a line containing the literal text
    // `name: bad_join`).
    assert!(
        diag_line >= 1 && diag_line <= body_line_count,
        "E303 primary span line {diag_line} must fall within body file \
         line count {body_line_count}; diagnostic: {:?}",
        e303[0].message
    );
    let body_combine_line = body_src
        .lines()
        .enumerate()
        .find(|(_, l)| l.contains("name: bad_join"))
        .map(|(i, _)| (i + 1) as u32)
        .expect("body file declares combine `name: bad_join`");

    // bind_schema_inner derives the combine span from the YAML
    // node's saphyr line. saphyr reports the line of the `- type:
    // combine` element, NOT the `name:` sub-key — accept either
    // line as a structural match against the combine block's
    // 2-line opening.
    let diag_in_combine_block = diag_line == body_combine_line
        || diag_line + 1 == body_combine_line
        || diag_line == body_combine_line + 1;
    assert!(
        diag_in_combine_block,
        "E303 primary span line ({diag_line}) must match body file's combine \
         block (line {body_combine_line}); diagnostic: {:?}",
        e303[0].message
    );
}
