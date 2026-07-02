//! Tests for the rename-node refactor engine.

use super::*;

/// Parse a YAML string to a value tree (the DOM the rewriters mutate).
fn dom(yaml: &str) -> Value {
    clinker_plan::yaml::from_str(yaml).expect("parse yaml")
}

// ── Reference rewriting ──────────────────────────────────────────────

#[test]
fn rewrite_ref_handles_plain_and_port_forms() {
    assert_eq!(
        rewrite_ref("orders", "orders", "purchases").as_deref(),
        Some("purchases")
    );
    assert_eq!(
        rewrite_ref("orders.domestic", "orders", "purchases").as_deref(),
        Some("purchases.domestic")
    );
    // Non-matching node part is left alone.
    assert_eq!(rewrite_ref("events", "orders", "purchases"), None);
    assert_eq!(rewrite_ref("split.orders", "orders", "purchases"), None);
}

#[test]
fn validate_node_name_rejects_dots_and_empties() {
    assert!(validate_node_name("purchases").is_ok());
    assert!(validate_node_name("a_b_1").is_ok());
    assert!(validate_node_name("").is_err());
    assert!(validate_node_name("a.b").is_err());
    assert!(validate_node_name("a b").is_err());
}

// ── CXL-aware rewriting ──────────────────────────────────────────────

#[test]
fn cxl_rewrites_only_source_qualifiers_not_method_receivers() {
    // `orders.id` is a source qualifier → rewritten. `region.contains(...)` is a
    // method receiver → left alone. A bare field `orders` (no dot) is a column,
    // not a qualifier → left alone.
    let src = "emit id = orders.id\nemit tag = region.contains(\"x\")\nemit raw = amt";
    match rewrite_cxl(src, "orders", "purchases", false) {
        CxlRewrite::Changed(out) => {
            assert!(out.contains("purchases.id"), "{out}");
            assert!(out.contains("region.contains"), "receiver untouched: {out}");
        }
        other => panic!(
            "expected change, got {:?}",
            matches!(other, CxlRewrite::Unchanged)
        ),
    }
}

#[test]
fn cxl_predicate_rewrite_maps_spans_back_to_source() {
    // A Combine `where:` is a bare predicate; the rewrite must land on the
    // original (unwrapped) string.
    match rewrite_cxl("orders.id == events.id", "orders", "purchases", true) {
        CxlRewrite::Changed(out) => assert_eq!(out, "purchases.id == events.id"),
        _ => panic!("expected predicate rewrite"),
    }
}

#[test]
fn cxl_unchanged_when_qualifier_absent() {
    assert!(matches!(
        rewrite_cxl("emit id = events.id", "orders", "purchases", false),
        CxlRewrite::Unchanged
    ));
}

#[test]
fn cxl_parse_error_is_reported_not_silently_dropped() {
    assert!(matches!(
        rewrite_cxl("emit = = orders.", "orders", "purchases", false),
        CxlRewrite::ParseError(_)
    ));
}

// ── Node rewriting ───────────────────────────────────────────────────

#[test]
fn rename_single_input_consumer() {
    let mut node =
        dom("type: transform\nname: norm\ninput: orders\nconfig: { cxl: \"emit id = id\" }");
    let changed = rename_in_node(&mut node, "orders", "purchases").unwrap();
    assert!(changed);
    assert_eq!(node["input"], Value::from("purchases"));
    // The transform's own name is not `orders`, so it stays.
    assert_eq!(node["name"], Value::from("norm"));
}

#[test]
fn rename_combine_mirrored_qualifier_rewrites_key_value_and_cxl() {
    let mut node = dom(
        "type: combine\nname: joined\ninput:\n  orders: orders\n  events: events\nconfig:\n  where: \"orders.id == events.id\"\n  drive: orders\n  cxl: |\n    emit id = orders.id\n    emit e = events.id\n",
    );
    let changed = rename_in_node(&mut node, "orders", "purchases").unwrap();
    assert!(changed);

    // Qualifier key + upstream value both renamed.
    let input = node["input"].as_object().unwrap();
    assert!(input.contains_key("purchases"), "key renamed: {input:?}");
    assert!(!input.contains_key("orders"));
    assert_eq!(input["purchases"], Value::from("purchases"));
    // events untouched.
    assert_eq!(input["events"], Value::from("events"));

    let cfg = node["config"].as_object().unwrap();
    assert_eq!(cfg["where"], Value::from("purchases.id == events.id"));
    assert_eq!(cfg["drive"], Value::from("purchases"));
    let cxl = cfg["cxl"].as_str().unwrap();
    assert!(cxl.contains("purchases.id"), "{cxl}");
    assert!(
        cxl.contains("events.id"),
        "events qualifier untouched: {cxl}"
    );
}

#[test]
fn rename_combine_value_only_when_alias_differs() {
    // Qualifier `o` draws from node `orders`. Renaming the node rewrites the
    // upstream value but NOT the alias, and does NOT touch the CXL (which is
    // written against the alias `o`, not the node name).
    let mut node = dom(
        "type: combine\nname: joined\ninput:\n  o: orders\n  e: events\nconfig:\n  where: \"o.id == e.id\"\n  cxl: \"emit id = o.id\"\n",
    );
    let changed = rename_in_node(&mut node, "orders", "purchases").unwrap();
    assert!(changed);
    let input = node["input"].as_object().unwrap();
    assert_eq!(input["o"], Value::from("purchases"), "value renamed");
    assert!(input.contains_key("o"), "alias key preserved");
    let cfg = node["config"].as_object().unwrap();
    // CXL references the alias `o`, unaffected by the node rename.
    assert_eq!(cfg["where"], Value::from("o.id == e.id"));
    assert_eq!(cfg["cxl"], Value::from("emit id = o.id"));
}

#[test]
fn rename_merge_inputs_list() {
    let mut node = dom("type: merge\nname: m\ninputs:\n  - orders\n  - events\n  - orders.late\n");
    assert!(rename_in_node(&mut node, "orders", "purchases").unwrap());
    let inputs = node["inputs"].as_array().unwrap();
    assert_eq!(inputs[0], Value::from("purchases"));
    assert_eq!(inputs[1], Value::from("events"));
    assert_eq!(inputs[2], Value::from("purchases.late"));
}

#[test]
fn rename_envelope_ports() {
    let mut node =
        dom("type: envelope\nname: env\nbody: orders\nheader: orders\ntrailer: events\n");
    assert!(rename_in_node(&mut node, "orders", "purchases").unwrap());
    assert_eq!(node["body"], Value::from("purchases"));
    assert_eq!(node["header"], Value::from("purchases"));
    assert_eq!(node["trailer"], Value::from("events"));
}

#[test]
fn rename_renames_the_node_itself() {
    let mut node =
        dom("type: source\nname: orders\nconfig: { name: orders, type: csv, path: o.csv }");
    assert!(rename_in_node(&mut node, "orders", "purchases").unwrap());
    assert_eq!(node["name"], Value::from("purchases"));
    // The source's inner `config.name` is a format-layer field, not a node
    // reference, and is intentionally left alone.
    assert_eq!(node["config"]["name"], Value::from("orders"));
}

// ── Overlay op rewriting ─────────────────────────────────────────────

#[test]
fn config_dotted_keys_are_reprefixed() {
    let mut cfg = dom("orders.threshold: 0.9\nother.knob: 1\norders.mode: fast")
        .as_object()
        .unwrap()
        .clone();
    assert!(rename_config_keys(&mut cfg, "orders", "purchases").unwrap());
    assert!(cfg.contains_key("purchases.threshold"));
    assert!(cfg.contains_key("purchases.mode"));
    assert!(cfg.contains_key("other.knob"));
    assert!(!cfg.keys().any(|k| k.starts_with("orders.")));
}

#[test]
fn op_target_anchor_alias_and_rewire_rewritten() {
    let mut op = dom(
        "op: remove\ntarget: orders\nrewire:\n  orders.input: upstream\n  sink.input: orders\n",
    );
    assert!(rename_in_op(&mut op, "orders", "purchases").unwrap());
    assert_eq!(op["target"], Value::from("purchases"));
    let rewire = op["rewire"].as_object().unwrap();
    assert!(
        rewire.contains_key("purchases.input"),
        "key renamed: {rewire:?}"
    );
    assert_eq!(
        rewire["sink.input"],
        Value::from("purchases"),
        "value renamed"
    );
}

#[test]
fn add_op_anchor_and_inline_node_rewritten() {
    let mut op = dom(
        "op: add\nafter: orders\nnode:\n  type: transform\n  name: stamp\n  input: orders\n  config: { cxl: \"emit id = id\" }\n",
    );
    assert!(rename_in_op(&mut op, "orders", "purchases").unwrap());
    assert_eq!(op["after"], Value::from("purchases"));
    assert_eq!(op["node"]["input"], Value::from("purchases"));
}

#[test]
fn set_config_cxl_value_is_cxl_rewritten() {
    let mut op = dom("op: set\ntarget: joined\nfield: config.cxl\nvalue: \"emit id = orders.id\"");
    assert!(rename_in_op(&mut op, "orders", "purchases").unwrap());
    assert_eq!(op["value"], Value::from("emit id = purchases.id"));
}

#[test]
fn config_key_collision_is_hard_error() {
    // Both `orders.threshold` and `purchases.threshold` present: renaming the
    // former onto the latter would silently drop a value, so it errors.
    let mut cfg = dom("orders.threshold: 0.9\npurchases.threshold: 0.5")
        .as_object()
        .unwrap()
        .clone();
    assert!(rename_config_keys(&mut cfg, "orders", "purchases").is_err());
}

#[test]
fn rewire_key_collision_is_hard_error() {
    let mut op = dom("op: remove\ntarget: x\nrewire:\n  orders.input: a\n  purchases.input: b\n");
    assert!(rename_in_op(&mut op, "orders", "purchases").is_err());
}

#[test]
fn combine_cxl_parse_failure_aborts() {
    // The qualifier `orders` mirrors the renamed node, so the malformed `where`
    // must be rewritten to stay consistent — but it does not parse, so the
    // whole node rewrite errors rather than writing a half-renamed node.
    let mut node = dom(
        "type: combine\nname: joined\ninput:\n  orders: orders\n  events: events\nconfig:\n  where: \"orders.id === broken\"\n  cxl: \"emit id = orders.id\"\n",
    );
    assert!(rename_in_node(&mut node, "orders", "purchases").is_err());
}

#[test]
fn file_stem_reduces_target_paths() {
    assert_eq!(
        file_stem_of(std::path::Path::new("pipeline/order.yaml")),
        "order"
    );
    assert_eq!(
        file_stem_of(std::path::Path::new("../../pipeline/score.comp.yaml")),
        "score"
    );
    // A per-target overlay's `channel.target` and the base path reduce equal.
    let overlay = dom("channel:\n  target: ../../pipeline/order.yaml\n");
    assert_eq!(overlay_target_stem(&overlay).as_deref(), Some("order"));
}

// ── Discovery / guards ───────────────────────────────────────────────

#[test]
fn base_node_names_reads_pipelines_and_compositions() {
    let p = dom(
        "pipeline:\n  name: p\nnodes:\n  - { type: source, name: a, config: {name: a, type: csv, path: x} }\n  - { type: output, name: b, input: a, config: {name: b, type: csv, path: y} }",
    );
    let names = base_node_names(&p).unwrap();
    assert!(names.contains("a") && names.contains("b"));

    let c = dom("_compose:\n  name: C\ntransformations:\n  - { name: t1, cxl: \"emit x = y\" }");
    let names = base_node_names(&c).unwrap();
    assert!(names.contains("t1"));
}

// ── Unified diff ─────────────────────────────────────────────────────

#[test]
fn unified_diff_marks_changed_lines() {
    let before = "a\nb\nc\n";
    let after = "a\nB\nc\n";
    let d = unified_diff(before, after, "f.yaml");
    assert!(d.contains("- b"), "{d}");
    assert!(d.contains("+ B"), "{d}");
    assert!(d.contains("  a"), "context: {d}");
}

// ── End-to-end fixture ───────────────────────────────────────────────

/// Build a fixture workspace: a pipeline with a Combine that draws from the
/// renamed source under a same-named qualifier, plus a group (op `target`) and a
/// channel (per-target `set config.cxl`) that reference it.
fn write_fixture(root: &std::path::Path) {
    let w = |rel: &str, body: &str| {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    };

    w(
        "clinker.toml",
        "[channel]\nroot = \"channel\"\n[group]\nroot = \"group\"\n",
    );

    w(
        "pipeline/order.yaml",
        "pipeline:\n  name: order\nnodes:\n  - type: source\n    name: orders\n    config:\n      name: orders\n      type: csv\n      path: ./orders.csv\n      schema:\n        - { name: id, type: string }\n        - { name: amt, type: float }\n  - type: source\n    name: events\n    config:\n      name: events\n      type: csv\n      path: ./events.csv\n      schema:\n        - { name: id, type: string }\n  - type: combine\n    name: joined\n    input:\n      orders: orders\n      events: events\n    config:\n      where: \"orders.id == events.id\"\n      match: first\n      on_miss: skip\n      propagate_ck: driver\n      cxl: |\n        emit id = orders.id\n        emit amt = orders.amt\n  - type: output\n    name: out\n    input: joined\n    config:\n      name: out\n      type: csv\n      path: ./out.csv\n",
    );

    // Group: a structural op targeting the renamed source.
    w(
        "group/enterprise.group.yaml",
        "group:\n  name: enterprise\n  match: 'tier == \"enterprise\"'\n  priority: 20\noverrides:\n  - op: patch_schema\n    target: orders\n    schema:\n      region: { add: { type: string } }\n",
    );

    // Channel: manifest labels select the group; per-target overlay overrides the
    // combine's CXL, whose value references the renamed source qualifier.
    w(
        "channel/acme/channel.cfg.yaml",
        "channel:\n  name: acme\nlabels:\n  tier: enterprise\n",
    );
    w(
        "channel/acme/order.channel.yaml",
        "channel:\n  target: ../../pipeline/order.yaml\noverrides:\n  - op: set\n    target: joined\n    field: config.cxl\n    value: |\n      emit id = orders.id\n      emit amt = orders.amt\n",
    );

    // A DIFFERENT pipeline that also has a node named `orders`, with its own
    // per-target overlay referencing it under a channel that does NOT select the
    // enterprise group (so the group's target-agnostic op never composes against
    // it). Renaming `orders` in `order.yaml` must leave this overlay untouched
    // (per-target scoping keyed on the overlay's target).
    w(
        "pipeline/other.yaml",
        "pipeline:\n  name: other\nnodes:\n  - type: source\n    name: orders\n    config:\n      name: orders\n      type: csv\n      path: ./orders.csv\n      schema:\n        - { name: id, type: string }\n  - type: output\n    name: sink\n    input: orders\n    config:\n      name: sink\n      type: csv\n      path: ./sink.csv\n",
    );
    w(
        "channel/beta/channel.cfg.yaml",
        "channel:\n  name: beta\nlabels:\n  tier: basic\n",
    );
    w(
        "channel/beta/other.channel.yaml",
        "channel:\n  target: ../../pipeline/other.yaml\noverrides:\n  - op: patch_schema\n    target: orders\n    schema:\n      region: { add: { type: string } }\n",
    );

    // Source data files are declared with inline schemas, but create them so any
    // path check during compilation is satisfied.
    w("pipeline/orders.csv", "id,amt\n");
    w("pipeline/events.csv", "id\n");
}

fn args(root: &std::path::Path, dry_run: bool) -> crate::RenameNodeArgs {
    crate::RenameNodeArgs {
        target: root.join("pipeline/order.yaml"),
        old: "orders".to_string(),
        new: "purchases".to_string(),
        dry_run,
        base_dir: root.to_path_buf(),
    }
}

#[test]
fn rename_node_dry_run_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(root);

    let files = [
        "pipeline/order.yaml",
        "group/enterprise.group.yaml",
        "channel/acme/order.channel.yaml",
        "channel/acme/channel.cfg.yaml",
    ];
    let before: Vec<String> = files
        .iter()
        .map(|f| std::fs::read_to_string(root.join(f)).unwrap())
        .collect();

    let code = run_rename_node(&args(root, true)).expect("dry run ok");
    assert_eq!(code, 0);

    for (f, b) in files.iter().zip(&before) {
        assert_eq!(
            &std::fs::read_to_string(root.join(f)).unwrap(),
            b,
            "{f} must be untouched"
        );
    }
}

#[test]
fn rename_node_real_run_propagates_and_relints_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(root);

    let code = run_rename_node(&args(root, false)).expect("real run ok");
    assert_eq!(code, 0, "re-lint must be clean after the rename");

    let pipeline = std::fs::read_to_string(root.join("pipeline/order.yaml")).unwrap();
    // Graph-level node identity moved orders -> purchases (the source's inner
    // `config.name` dataset label is a format-layer field, not a graph
    // reference, and is intentionally preserved).
    let names = base_node_names(&dom(&pipeline)).unwrap();
    assert!(names.contains("purchases"), "source renamed:\n{pipeline}");
    assert!(
        !names.contains("orders"),
        "no stray `orders` node:\n{pipeline}"
    );
    assert!(
        pipeline.contains("purchases.id == events.id"),
        "where rewritten:\n{pipeline}"
    );
    assert!(
        pipeline.contains("emit id = purchases.id"),
        "cxl rewritten:\n{pipeline}"
    );

    let group = std::fs::read_to_string(root.join("group/enterprise.group.yaml")).unwrap();
    assert!(
        group.contains("target: purchases"),
        "op target propagated:\n{group}"
    );

    let overlay = std::fs::read_to_string(root.join("channel/acme/order.channel.yaml")).unwrap();
    assert!(
        overlay.contains("emit id = purchases.id"),
        "set cxl propagated:\n{overlay}"
    );

    // The manifest carries no node reference, so it must be left byte-for-byte.
    let manifest = std::fs::read_to_string(root.join("channel/acme/channel.cfg.yaml")).unwrap();
    assert_eq!(
        manifest,
        "channel:\n  name: acme\nlabels:\n  tier: enterprise\n"
    );

    // A per-target overlay for a *different* pipeline that also has an `orders`
    // node must be left untouched — the rename is scoped to `order.yaml`.
    let other = std::fs::read_to_string(root.join("channel/beta/other.channel.yaml")).unwrap();
    assert!(
        other.contains("target: orders"),
        "a different pipeline's overlay must be untouched:\n{other}"
    );
}

#[test]
fn rename_node_guards_missing_and_colliding_names() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_fixture(root);

    // Old name absent.
    let mut a = args(root, true);
    a.old = "ghost".to_string();
    assert!(run_rename_node(&a).is_err(), "missing old name must error");

    // New name already exists.
    let mut a = args(root, true);
    a.new = "events".to_string();
    assert!(
        run_rename_node(&a).is_err(),
        "colliding new name must error"
    );

    // Identical names.
    let mut a = args(root, true);
    a.new = "orders".to_string();
    assert!(run_rename_node(&a).is_err(), "no-op rename must error");
}
