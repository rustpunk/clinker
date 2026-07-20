//! Pre-compile structural overlay pass wired into `compile_with_diagnostics`.
//!
//! These tests exercise the seam that splices a resolved channel/group
//! `overrides:` op stream into the base AST *before* schema binding, so the
//! effective (post-overlay) DAG is what gets typechecked and lowered. They
//! pin three behaviors from the epic's normative resolution semantics:
//!
//! - a base pipeline plus a group op plus a channel override compiles to the
//!   expected effective plan (the injected node and the overridden logic both
//!   land in the compiled `CompiledPlan`);
//! - an op that cannot apply (here an orphaning `remove`) fails with a
//!   diagnostic anchored to the offending op's span, not the base pipeline;
//! - ill-typed CXL introduced by an op is caught by the ordinary `bind_schema`
//!   pass, still anchored to the op because the engine stamps each spliced node
//!   with the op's source location;
//! - a plain pipeline with no overlay ops is unaffected (the pass is a no-op).

use crate::config::pipeline_node::PipelineNode;
use crate::config::{CompileContext, parse_config};
use crate::overlay_ops::{LayeredOp, OverlayLayer, OverlayOp};
use crate::plan::CompiledPlan;
use crate::yaml::Spanned;

/// A three-node linear pipeline: `orders -> normalize -> sink`.
const BASE: &str = r#"
pipeline:
  name: overlay_base
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: "emit order_id = order_id\nemit amount = amount"
  - type: output
    name: sink
    input: normalize
    config:
      name: sink
      type: csv
      path: out.csv
"#;

/// Parse one `Spanned<OverlayOp>` from a bare op mapping.
fn parse_op(yaml: &str) -> Spanned<OverlayOp> {
    crate::yaml::from_str::<Spanned<OverlayOp>>(yaml).expect("parse op")
}

/// Compile `BASE` with the given resolved overlay op stream.
fn compile_with_ops(
    ops: Vec<LayeredOp>,
) -> Result<CompiledPlan, Vec<clinker_core_types::Diagnostic>> {
    let config = parse_config(BASE).expect("parse base config");
    let ctx = CompileContext {
        overlay_ops: ops,
        ..CompileContext::default()
    };
    config.compile(&ctx)
}

/// Node names of the compiled plan's effective (post-overlay) config, in order.
fn effective_names(plan: &CompiledPlan) -> Vec<String> {
    plan.config()
        .nodes
        .iter()
        .map(|n| n.value.name().to_string())
        .collect()
}

/// The CXL source of a named transform in the effective config.
fn transform_cxl(plan: &CompiledPlan, name: &str) -> String {
    let node = plan
        .config()
        .nodes
        .iter()
        .find(|n| n.value.name() == name)
        .unwrap_or_else(|| panic!("node {name:?} present"));
    match &node.value {
        PipelineNode::Transform { config, .. } => config.cxl.source.clone(),
        other => panic!(
            "node {name:?} is a {} node, not a transform",
            other.type_tag()
        ),
    }
}

/// Direct input names of a named node in the effective config.
fn inputs_of(plan: &CompiledPlan, name: &str) -> Vec<String> {
    plan.config()
        .nodes
        .iter()
        .find(|n| n.value.name() == name)
        .unwrap_or_else(|| panic!("node {name:?} present"))
        .value
        .direct_input_names()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

// ── end-to-end: base + group op + channel override ────────────────────────

#[test]
fn base_plus_group_add_and_channel_set_compiles_effective_plan() {
    // A group-layer `add` splices `enrich` after `normalize`; a
    // channel-per-target `set` rewrites `normalize`'s CXL. Both must land in
    // the compiled effective plan, and the higher-precedence channel op is
    // applied to the same base node the group op read from.
    let group_add = LayeredOp::new(
        OverlayLayer::Group { priority: 10 },
        parse_op(
            r#"
op: add
node:
  type: transform
  name: enrich
  input: normalize
  config:
    cxl: "emit order_id = order_id\nemit amount = amount"
after: normalize
"#,
        ),
    );
    let channel_set = LayeredOp::new(
        OverlayLayer::ChannelPerTarget,
        parse_op(
            r#"
op: set
target: normalize
field: config.cxl
value: "emit order_id = order_id\nemit amount = amount * 2"
"#,
        ),
    );

    let plan = compile_with_ops(vec![group_add, channel_set]).expect("effective plan compiles");

    // `enrich` was spliced in after `normalize`, and `sink` was rewired onto it.
    assert_eq!(
        effective_names(&plan),
        vec!["orders", "normalize", "enrich", "sink"],
    );
    assert_eq!(inputs_of(&plan, "enrich"), vec!["normalize"]);
    assert_eq!(inputs_of(&plan, "sink"), vec!["enrich"]);

    // The channel `set` override replaced `normalize`'s logic wholesale.
    assert_eq!(
        transform_cxl(&plan, "normalize"),
        "emit order_id = order_id\nemit amount = amount * 2",
    );
    // The group-added node kept the logic the op declared.
    assert_eq!(
        transform_cxl(&plan, "enrich"),
        "emit order_id = order_id\nemit amount = amount",
    );
}

// ── ill-typed op: structural failure anchored to the op ───────────────────

#[test]
fn orphaning_remove_errors_with_op_spanned_diagnostic() {
    // Removing `normalize` without rewiring `sink` leaves a dangling reference;
    // the engine rejects it and the diagnostic must anchor to the op's line,
    // never to the base pipeline's `normalize` declaration.
    let op = parse_op(
        r#"
op: remove
target: normalize
"#,
    );
    let op_line = op.referenced.line() as u32;
    assert!(op_line > 0, "op should carry a real source line");

    let diags = compile_with_ops(vec![LayeredOp::new(OverlayLayer::ChannelPerTarget, op)])
        .expect_err("orphaning remove must fail compilation");

    let e114 = diags
        .iter()
        .find(|d| d.code == "E114")
        .expect("overlay-op failure reported as E114");
    assert_eq!(
        e114.primary.span.synthetic_line_number(),
        Some(op_line),
        "the diagnostic must be anchored to the offending op, not the base pipeline",
    );
    assert!(
        e114.message.contains("normalize"),
        "message should name the dangling target: {}",
        e114.message,
    );
}

// ── ill-typed op: bad CXL caught by bind_schema, anchored to the op ────────

#[test]
fn spliced_bad_cxl_errors_anchored_to_op() {
    // The op engine does not typecheck CXL, so a spliced node with a bad body
    // applies structurally and then fails in the ordinary `bind_schema` pass.
    // The body references an unknown column, so it fails name resolution
    // (E203). Because the engine stamps the injected node with the op's
    // location, that diagnostic still points at the op, not the base.
    let op = parse_op(
        r#"
op: add
node:
  type: transform
  name: broken
  input: normalize
  config:
    cxl: "emit oops = nonexistent_field"
after: normalize
"#,
    );
    let op_line = op.referenced.line() as u32;
    assert!(op_line > 0, "op should carry a real source line");

    let diags = compile_with_ops(vec![LayeredOp::new(OverlayLayer::ChannelPerTarget, op)])
        .expect_err("spliced bad CXL must fail compilation");

    let cxl_err = diags
        .iter()
        .find(|d| d.code == "E203" && d.primary.span.synthetic_line_number() == Some(op_line))
        .unwrap_or_else(|| {
            panic!("expected an op-spanned CXL diagnostic at line {op_line}, got: {diags:?}")
        });
    assert!(
        cxl_err.message.contains("broken"),
        "diagnostic should name the spliced node: {}",
        cxl_err.message,
    );
}

// ── no overlay: plain pipeline unaffected ─────────────────────────────────

#[test]
fn plain_pipeline_without_overlay_is_unaffected() {
    // With no overlay ops the pre-compile pass is skipped entirely: the
    // compiled plan carries exactly the declared nodes, no injections.
    let plan = compile_with_ops(Vec::new()).expect("plain pipeline compiles");
    assert_eq!(effective_names(&plan), vec!["orders", "normalize", "sink"]);
    assert_eq!(
        transform_cxl(&plan, "normalize"),
        "emit order_id = order_id\nemit amount = amount",
    );
    assert_eq!(inputs_of(&plan, "sink"), vec!["normalize"]);
}

// ── per-attribute schema provenance ───────────────────────────────────────

/// Guard against value/provenance drift: for every attribute of every resolved
/// single-record source column, the bound-schema value must equal the schema
/// provenance winning value. This is the invariant the whole side-table exists
/// to uphold, checked over an arbitrary compiled plan.
fn assert_schema_provenance_invariant(plan: &CompiledPlan) {
    use crate::config::composition::{SchemaAttr, column_attrs};
    use clinker_format::SourceSchema;

    let db = plan.schema_provenance();
    for (source, schema) in plan.bound_schemas() {
        let SourceSchema::Columns(cols) = schema else {
            continue;
        };
        for col in cols {
            for (attr, json) in column_attrs(col) {
                let resolved = db.get(source, &col.name, attr).unwrap_or_else(|| {
                    panic!(
                        "missing schema provenance leaf {source}.{}.{attr}",
                        col.name
                    )
                });
                assert_eq!(
                    resolved.value,
                    SchemaAttr::Value(json),
                    "value/provenance drift at {source}.{}.{attr}",
                    col.name,
                );
            }
        }
    }
}

#[test]
fn plain_compile_seeds_base_and_upholds_invariant() {
    let plan = compile_with_ops(Vec::new()).expect("plain pipeline compiles");
    assert_schema_provenance_invariant(&plan);
    // With no overlay, every attribute is attributed to Base.
    let db = plan.schema_provenance();
    let amount_type = db
        .get("orders", "amount", "type")
        .expect("amount.type leaf");
    assert_eq!(
        amount_type.winning_layer().unwrap().kind,
        crate::config::composition::SchemaLayer::Base,
    );
}

/// A channel-wide `patch_schema` op that retypes `amount` to float and adds a
/// `scale` — the two edits must both land, attribute to Channel, and keep the
/// value/provenance invariant.
#[test]
fn channel_patch_schema_attributes_to_channel_and_records_span() {
    use crate::config::composition::SchemaLayer;

    let op = parse_op(
        r#"
op: patch_schema
target: orders
schema:
  amount: { type: float, scale: 2 }
"#,
    );
    let op_line = op.referenced.line() as u32;
    assert!(op_line > 0, "op should carry a real source line");

    let plan = compile_with_ops(vec![LayeredOp::new(OverlayLayer::ChannelWide, op)])
        .expect("overlay plan compiles");

    // Resolved schema reflects both edits.
    let db = plan.schema_provenance();
    let ty = db.get("orders", "amount", "type").expect("amount.type");
    assert_eq!(ty.winning_layer().unwrap().kind, SchemaLayer::Channel);
    assert_eq!(
        ty.winning_layer().unwrap().span.synthetic_line_number(),
        Some(op_line)
    );
    // The shadowed Base value is still the original declared type.
    assert_eq!(
        ty.layer_value(SchemaLayer::Base),
        Some(&crate::config::composition::SchemaAttr::Value(
            serde_json::to_value(cxl::typecheck::Type::Int).unwrap()
        ))
    );

    let scale = db.get("orders", "amount", "scale").expect("amount.scale");
    assert_eq!(scale.winning_layer().unwrap().kind, SchemaLayer::Channel);

    // The whole plan still upholds the value/provenance invariant.
    assert_schema_provenance_invariant(&plan);
}

/// `explain --field <source>.<column>.<attribute>` renders the winning layer,
/// its value, and its source span.
#[test]
fn explain_field_renders_schema_attribute_provenance() {
    let op = parse_op(
        r#"
op: patch_schema
target: orders
schema:
  amount: { type: float }
"#,
    );
    let op_line = op.referenced.line() as u32;
    let plan = compile_with_ops(vec![LayeredOp::new(OverlayLayer::ChannelWide, op)])
        .expect("overlay plan compiles");

    let out =
        crate::plan::explain_provenance::explain_field_provenance(&plan, "orders.amount.type")
            .expect("schema field resolves");
    assert!(out.contains("orders.amount.type"), "{out}");
    assert!(out.contains("[WON] Channel"), "winner layer shown: {out}");
    assert!(out.contains("Base"), "shadowed base shown: {out}");
    assert!(
        out.contains(&format!("line {op_line}")),
        "span shown: {out}"
    );
}

/// A `remove` op must not leave a stale resolved value for the removed column:
/// its attribute leaves are dropped, and `explain --field` reports the attribute
/// as absent rather than a live type from the shadowed Base seed.
#[test]
fn removed_column_has_no_stale_provenance() {
    // A source with an unreferenced `order_notes` column the transform never
    // emits, so removing it does not break downstream CXL.
    const WITH_SPARE: &str = r#"
pipeline:
  name: overlay_remove
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
        - { name: order_notes, type: string }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: "emit order_id = order_id\nemit amount = amount"
  - type: output
    name: sink
    input: normalize
    config:
      name: sink
      type: csv
      path: out.csv
"#;
    let config = parse_config(WITH_SPARE).expect("parse config");
    let op = parse_op(
        r#"
op: patch_schema
target: orders
schema:
  order_notes: remove
"#,
    );
    let ctx = CompileContext {
        overlay_ops: vec![LayeredOp::new(OverlayLayer::ChannelWide, op)],
        ..CompileContext::default()
    };
    let plan = config.compile(&ctx).expect("overlay plan compiles");

    // The removed column carries no resolved attribute leaf...
    assert!(
        plan.schema_provenance()
            .get("orders", "order_notes", "type")
            .is_none(),
        "removed column must not carry a resolved type leaf",
    );
    // ...and `explain --field` on it errors rather than reporting a live value.
    assert!(
        crate::plan::explain_provenance::explain_field_provenance(&plan, "orders.order_notes.type")
            .is_err(),
        "explain must not report a value for a removed column",
    );
    // The invariant still holds over the surviving columns.
    assert_schema_provenance_invariant(&plan);
}

// ── numeric enforcement ───────────────────────────────────────────────────

#[test]
fn numeric_source_column_type_is_rejected() {
    const NUMERIC_SRC: &str = r#"
pipeline:
  name: numeric_reject
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: amount, type: numeric }
  - type: output
    name: sink
    input: orders
    config:
      name: sink
      type: csv
      path: out.csv
"#;
    let config = parse_config(NUMERIC_SRC).expect("parse numeric config");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("a `numeric` source column must be rejected");
    assert!(
        diags.iter().any(|d| d.code == "E158"),
        "expected E158 for non-concrete numeric, got: {diags:?}",
    );
}
