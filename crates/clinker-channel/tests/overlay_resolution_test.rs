//! Channel/group overlay resolution over the folder layout.
//!
//! Drives the whole seam a CLI invocation resolves through: build a workspace
//! on disk (`pipeline/`, `composition/`, `channel/<id>/`, `group/`), resolve the
//! overlay stack by computed path, then apply its value surface to a compiled
//! plan. Coverage spans the three surfaces a per-target overlay carries:
//!
//! - **config clobber** — layer precedence (pipeline default < group <
//!   channel-wide < per-target), the unmatched-key hard error, and the
//!   content-hash channel identity a channel stamps onto the plan;
//! - **var overlay** — the four scoped registries (`$vars` / `$pipeline` /
//!   `$source` / `$record`) with their reserved-name, unknown-source,
//!   type-mismatch, and composition-target guards;
//! - **source patches** — the `sources:` carrier that reshapes a source node's
//!   schema and per-format options before compile.
//!
//! Each layer resolves through the same value machinery, so exercising it from
//! the folder layout guards the resolution contract end to end rather than any
//! single resolver in isolation.

use std::fs;
use std::path::{Path, PathBuf};

use clinker_channel::{ChannelOverlayResult, GroupSource, OverlayResolution, resolve};
use clinker_core_types::{Diagnostic, Severity};
use clinker_plan::config::composition::LayerKind;
use clinker_plan::config::{
    ChannelLayout, CompileContext, GroupLayout, InputFormat, PipelineConfig, ShardScheme,
    apply_source_patches,
};
use clinker_plan::plan::CompiledPlan;
use clinker_record::Value;
use serde_json::json;

/// Base pipeline: a CSV source, an enrich transform declaring one var per
/// scope, a `risk` composition whose `threshold` is the per-tenant knob, and an
/// output. Overlays clobber `risk.threshold` and the declared vars.
const BASE_PIPELINE: &str = r#"
pipeline:
  name: base
  vars:
    fuzzy_threshold: { type: float, default: 0.85 }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      options: { has_header: true }
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: transform
    name: enrich
    input: orders
    config:
      declares:
        - { name: cutoff_date, scope: pipeline, type: date, default: "2024-01-01" }
        - { name: ingest_label, scope: source, type: string, default: "prod" }
        - { name: tier, scope: record, type: string, default: "bronze" }
      cxl: |
        emit order_id = order_id
        emit amount = amount
  - type: composition
    name: risk
    input: enrich
    use: ../composition/risk.comp.yaml
    inputs: { inp: enrich }
    config: { threshold: 0.5 }
  - type: output
    name: out
    input: risk
    config: { name: out, type: csv, path: out.csv }
"#;

/// The `risk` scoring composition the base pipeline calls as node `risk`.
const RISK_COMPOSITION: &str = r#"
_compose:
  name: risk_score
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  outputs:
    out: scored
  config_schema:
    threshold: { type: float, default: 0.5, range: [0.0, 1.0] }
nodes:
  - type: transform
    name: scored
    input: inp
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
"#;

// ── Fixtures / helpers ──────────────────────────────────────────────────

/// A workspace with the base pipeline and its composition already on disk. Each
/// test adds the channel/group files its scenario needs.
fn workspace() -> tempfile::TempDir {
    let ws = tempfile::tempdir().expect("tempdir");
    write(&ws.path().join("pipeline/base.yaml"), BASE_PIPELINE);
    write(
        &ws.path().join("composition/risk.comp.yaml"),
        RISK_COMPOSITION,
    );
    ws
}

/// Write `contents` to `path`, creating parent directories first.
fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create dirs");
    }
    fs::write(path, contents).expect("write file");
}

fn channel_layout() -> ChannelLayout {
    ChannelLayout {
        root: PathBuf::from("channel"),
        shard: ShardScheme::None,
    }
}

fn group_layout() -> GroupLayout {
    GroupLayout {
        root: PathBuf::from("group"),
    }
}

/// Parse the base pipeline config from disk (unvalidated, uncompiled).
fn parse_base(root: &Path) -> PipelineConfig {
    let yaml = fs::read_to_string(root.join("pipeline/base.yaml")).expect("read base pipeline");
    clinker_plan::yaml::from_str(&yaml).expect("parse base pipeline")
}

/// Compile the base pipeline. The pipeline dir is `pipeline`, so the
/// composition `use: ../composition/risk.comp.yaml` resolves against the root.
fn compile_base(root: &Path, config: &PipelineConfig) -> CompiledPlan {
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipeline"));
    PipelineConfig::compile(config, &ctx).expect("compile base pipeline")
}

/// Resolve an overlay stack, then apply its config + vars over a freshly
/// compiled base plan. Returns the resolution alongside the mutated plan and
/// the runtime var maps.
fn resolve_apply(
    root: &Path,
    target: &str,
    channel_id: Option<&str>,
    groups: &[String],
    auto_groups: bool,
) -> (OverlayResolution, CompiledPlan, ChannelOverlayResult) {
    let config = parse_base(root);
    let mut plan = compile_base(root, &config);
    let res = resolve(
        root,
        &channel_layout(),
        &group_layout(),
        target,
        channel_id,
        groups,
        auto_groups,
    )
    .expect("resolve overlay stack");
    let result = res.apply_config_and_vars(&mut plan, &config);
    (res, plan, result)
}

fn errors_with_code<'a>(diags: &'a [Diagnostic], code: &str) -> Vec<&'a Diagnostic> {
    diags
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error) && d.code == code)
        .collect()
}

fn has_no_errors(diags: &[Diagnostic]) -> bool {
    diags.iter().all(|d| !matches!(d.severity, Severity::Error))
}

/// A per-target overlay file body targeting the base pipeline, with an
/// arbitrary trailing block (`config:` / `vars:` / `sources:`).
fn per_target_overlay(body: &str) -> String {
    format!("channel:\n  target: ../../pipeline/base.yaml\n{body}")
}

// ── Config clobber ──────────────────────────────────────────────────────

#[test]
fn per_target_config_clobbers_base() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    let resolved = plan
        .provenance()
        .get("risk", "threshold")
        .expect("risk.threshold tracked in provenance");
    let winner = resolved.winning_layer().expect("a layer won");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
    assert_eq!(resolved.value, json!(0.95));
}

#[test]
fn layer_precedence_group_below_channel_wide_below_per_target() {
    let ws = workspace();
    write(
        &ws.path().join("group/enterprise.group.yaml"),
        "group:\n  name: enterprise\n  match: 'tier == \"enterprise\"'\n  priority: 20\nconfig: { risk.threshold: 0.8 }\n",
    );
    write(
        &ws.path().join("channel/globex/channel.cfg.yaml"),
        "channel:\n  name: globex\nlabels: { tier: enterprise }\nconfig: { risk.threshold: 0.9 }\n",
    );
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    // The enterprise group was derived from the channel's `tier` label.
    assert!(
        res.applied_groups()
            .iter()
            .any(|g| g.name == "enterprise" && g.source == GroupSource::Derived),
        "enterprise group derived: {:?}",
        res.applied_groups()
    );

    let resolved = plan
        .provenance()
        .get("risk", "threshold")
        .expect("risk.threshold tracked in provenance");

    // Highest layer wins the value...
    let winner = resolved.winning_layer().expect("a layer won");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
    assert_eq!(resolved.value, json!(0.95));

    // ...and every lower layer is recorded in the provenance chain.
    let kinds: Vec<LayerKind> = resolved.provenance.iter().map(|l| l.kind).collect();
    assert!(
        kinds.iter().any(|k| matches!(k, LayerKind::Group { .. })),
        "group layer present: {kinds:?}"
    );
    assert!(kinds.contains(&LayerKind::ChannelWide), "{kinds:?}");
    assert!(kinds.contains(&LayerKind::ChannelPerTarget), "{kinds:?}");
}

#[test]
fn unmatched_config_key_is_e113() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { nonexistent.param: 1 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);

    let errors: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error))
        .collect();
    assert_eq!(errors.len(), 1, "one error: {:?}", result.diagnostics);
    assert_eq!(errors[0].code, "E113");

    // Identity is still stamped so downstream diagnostics can name the channel.
    assert!(plan.channel_identity().is_some());
}

#[test]
fn channel_identity_none_before_overlay_then_stamped() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    // A bare compiled plan carries no channel identity.
    let config = parse_base(ws.path());
    let mut plan = compile_base(ws.path(), &config);
    assert!(plan.channel_identity().is_none());

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("globex"),
        &[],
        true,
    )
    .expect("resolve");
    res.apply_config_and_vars(&mut plan, &config);

    let identity = plan.channel_identity().expect("identity stamped");
    assert_eq!(identity.name, "globex");
    assert_ne!(identity.content_hash, [0u8; 32]);
}

#[test]
fn channel_identity_stable_across_reruns() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (_r1, plan1, _v1) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    let (_r2, plan2, _v2) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);

    let id1 = plan1.channel_identity().expect("identity 1");
    let id2 = plan2.channel_identity().expect("identity 2");
    assert_eq!(id1.name, id2.name);
    assert_eq!(id1.content_hash, id2.content_hash);
    assert_ne!(id1.content_hash, [0u8; 32]);
}

// ── Fixed lock (per-value config lock) ──────────────────────────────────

/// A channel manifest body (`channel.cfg.yaml`) for channel `id`, with an
/// arbitrary trailing block (`config:` / `fixed:` / `labels:`).
fn manifest(id: &str, body: &str) -> String {
    format!("channel:\n  name: {id}\n{body}")
}

#[test]
fn channel_wide_fixed_locks_out_per_target() {
    let ws = workspace();
    // Channel-wide (a *lower* layer) marks the value fixed; the per-target
    // overlay (the highest layer) tries to override it non-fixed.
    write(
        &ws.path().join("channel/globex/channel.cfg.yaml"),
        &manifest("globex", "fixed: { risk.threshold: 0.6 }\n"),
    );
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    let resolved = plan
        .provenance()
        .get("risk", "threshold")
        .expect("risk.threshold tracked in provenance");
    let winner = resolved.winning_layer().expect("a layer won");
    // The fixed lower layer holds against the higher per-target layer.
    assert_eq!(winner.kind, LayerKind::ChannelWide);
    assert!(winner.fixed, "winning layer is marked fixed");
    assert_eq!(resolved.value, json!(0.6));
}

#[test]
fn non_fixed_channel_wide_is_overridden_by_per_target() {
    // Control for `channel_wide_fixed_locks_out_per_target`: without the lock,
    // plain precedence makes the higher per-target layer win.
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/channel.cfg.yaml"),
        &manifest("globex", "config: { risk.threshold: 0.6 }\n"),
    );
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    let resolved = plan.provenance().get("risk", "threshold").expect("tracked");
    let winner = resolved.winning_layer().expect("a layer won");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
    assert!(!winner.fixed);
    assert_eq!(resolved.value, json!(0.95));
}

#[test]
fn group_fixed_locks_out_channel_layers() {
    let ws = workspace();
    // The lowest overlay layer (a derived group) locks the value; both channel
    // layers above try to override it non-fixed.
    write(
        &ws.path().join("group/enterprise.group.yaml"),
        "group:\n  name: enterprise\n  match: 'tier == \"enterprise\"'\n  priority: 20\nfixed: { risk.threshold: 0.7 }\n",
    );
    write(
        &ws.path().join("channel/globex/channel.cfg.yaml"),
        &manifest(
            "globex",
            "labels: { tier: enterprise }\nconfig: { risk.threshold: 0.9 }\n",
        ),
    );
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let (res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert!(
        res.applied_groups()
            .iter()
            .any(|g| g.name == "enterprise" && g.source == GroupSource::Derived),
        "enterprise group derived: {:?}",
        res.applied_groups()
    );

    let resolved = plan.provenance().get("risk", "threshold").expect("tracked");
    let winner = resolved.winning_layer().expect("a layer won");
    assert!(
        matches!(winner.kind, LayerKind::Group { .. }),
        "fixed group layer wins: {:?}",
        winner.kind
    );
    assert!(winner.fixed);
    assert_eq!(resolved.value, json!(0.7));
}

#[test]
fn fixed_and_config_same_key_in_layer_fixed_wins() {
    // A key present in both `config:` and `fixed:` of one file resolves to the
    // fixed value (fixed is clobbered last within the layer).
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.5 }\nfixed: { risk.threshold: 0.9 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    let resolved = plan.provenance().get("risk", "threshold").expect("tracked");
    let winner = resolved.winning_layer().expect("a layer won");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
    assert!(winner.fixed, "the fixed entry wins within the layer");
    assert_eq!(resolved.value, json!(0.9));
}

#[test]
fn per_target_fixed_marks_provenance() {
    // Even at the highest layer, a `fixed:` value records the lock in provenance.
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("fixed: { risk.threshold: 0.42 }\n"),
    );

    let (_res, plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );

    let resolved = plan.provenance().get("risk", "threshold").expect("tracked");
    let winner = resolved.winning_layer().expect("a layer won");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
    assert!(winner.fixed);
    assert_eq!(resolved.value, json!(0.42));
}

#[test]
fn unmatched_fixed_key_is_e113() {
    // A `fixed:` key is validated exactly like a `config:` key: an unknown
    // parameter is the same hard E113 error.
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("fixed: { nonexistent.param: 1 }\n"),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E113").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

#[test]
fn effective_config_overrides_fold_honors_fixed() {
    // The `$config.<param>` pre-compile fold agrees with the provenance winner:
    // a fixed lower layer wins the folded value too, so the executed body and
    // the rendered `[WON]` layer never disagree.
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/channel.cfg.yaml"),
        &manifest("globex", "fixed: { risk.threshold: 0.6 }\n"),
    );
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.95 }\n"),
    );

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("globex"),
        &[],
        true,
    )
    .expect("resolve");
    let overrides = res.effective_config_overrides();
    assert_eq!(
        overrides.get("risk").and_then(|m| m.get("threshold")),
        Some(&json!(0.6)),
        "fold honors the fixed lower layer: {overrides:?}"
    );
}

// ── Var overlay: $vars (static) ─────────────────────────────────────────

#[test]
fn static_var_override() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  static:\n    fuzzy_threshold:\n      type: float\n      default: 0.92\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert_eq!(
        result.static_vars.get("fuzzy_threshold"),
        Some(&Value::Float(0.92))
    );
}

#[test]
fn static_var_add() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay("vars:\n  static:\n    new_knob:\n      type: int\n      default: 7\n"),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert_eq!(result.static_vars.get("new_knob"), Some(&Value::Integer(7)));
}

#[test]
fn static_var_type_mismatch_e107() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  static:\n    fuzzy_threshold:\n      type: string\n      default: \"wrong\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E107").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

// ── Var overlay: $pipeline ──────────────────────────────────────────────

#[test]
fn pipeline_var_override() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  pipeline:\n    cutoff_date:\n      type: date\n      default: \"2026-01-01\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert_eq!(
        result.pipeline_vars.get("cutoff_date"),
        Some(&Value::from("2026-01-01"))
    );
}

#[test]
fn pipeline_reserved_name_e110() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  pipeline:\n    execution_id:\n      type: string\n      default: \"x\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E110").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

// ── Var overlay: $source ────────────────────────────────────────────────

#[test]
fn source_var_override_per_name() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  source:\n    orders:\n      ingest_label:\n        type: string\n        default: \"staging\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    let inner = result
        .source_vars
        .get("orders")
        .expect("orders source overrides");
    assert_eq!(inner.get("ingest_label"), Some(&Value::from("staging")));
}

#[test]
fn source_unknown_name_e111() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  source:\n    not_a_source:\n      x:\n        type: int\n        default: 1\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E111").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

#[test]
fn source_reserved_name_e110() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  source:\n    orders:\n      path:\n        type: string\n        default: \"/tmp/x\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E110").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

// ── Var overlay: $record ────────────────────────────────────────────────

#[test]
fn record_var_override() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n  record:\n    tier:\n      type: string\n      default: \"platinum\"\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert_eq!(
        result.record_vars.get("tier"),
        Some(&Value::from("platinum"))
    );
}

// ── Var overlay: cross-cutting ──────────────────────────────────────────

#[test]
fn multiple_scopes_in_one_overlay() {
    let ws = workspace();
    write(
        &ws.path().join("channel/globex/base.channel.yaml"),
        &per_target_overlay(
            "vars:\n\
             \x20 static:\n    fuzzy_threshold: { type: float, default: 0.99 }\n\
             \x20 pipeline:\n    cutoff_date: { type: date, default: \"2030-01-01\" }\n\
             \x20 source:\n    orders:\n      ingest_label: { type: string, default: \"yolo\" }\n\
             \x20 record:\n    tier: { type: string, default: \"diamond\" }\n",
        ),
    );

    let (_res, _plan, result) = resolve_apply(ws.path(), "base", Some("globex"), &[], true);
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
    assert_eq!(result.static_vars.len(), 1);
    assert_eq!(result.pipeline_vars.len(), 1);
    assert_eq!(result.source_vars.get("orders").expect("orders").len(), 1);
    assert_eq!(result.record_vars.len(), 1);
}

#[test]
fn no_overlay_empty_maps() {
    let ws = workspace();

    let (res, _plan, result) = resolve_apply(ws.path(), "base", None, &[], false);
    assert!(res.is_empty(), "a channel-less resolution is empty");
    assert!(result.static_vars.is_empty());
    assert!(result.pipeline_vars.is_empty());
    assert!(result.source_vars.is_empty());
    assert!(result.record_vars.is_empty());
    assert!(
        has_no_errors(&result.diagnostics),
        "{:?}",
        result.diagnostics
    );
}

#[test]
fn composition_overlay_with_vars_e109() {
    let ws = workspace();

    // Compile the base plan before the composition overlay file exists: the
    // guard under test is in `apply_config_and_vars` (post-compile), while the
    // base compile runs a workspace-wide `*.comp.yaml` signature scan that would
    // otherwise try to parse this overlay file as a composition.
    let config = parse_base(ws.path());
    let mut plan = compile_base(ws.path(), &config);

    // A per-target overlay whose target is the composition, carrying vars: var
    // overrides are not supported on a composition overlay.
    write(
        &ws.path().join("channel/compch/risk.comp.yaml"),
        "channel:\n  target: ../../composition/risk.comp.yaml\nvars:\n  static:\n    foo:\n      type: int\n      default: 1\n",
    );

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "risk",
        Some("compch"),
        &[],
        false,
    )
    .expect("resolve");
    let result = res.apply_config_and_vars(&mut plan, &config);
    assert_eq!(
        errors_with_code(&result.diagnostics, "E109").len(),
        1,
        "{:?}",
        result.diagnostics
    );
}

// ── Source patches (`sources:` carrier) ─────────────────────────────────

#[test]
fn per_target_source_schema_patch_applies() {
    let ws = workspace();
    write(
        &ws.path().join("channel/patch/base.channel.yaml"),
        &per_target_overlay(
            "sources:\n  orders:\n    schema:\n      region: { add: { type: string } }\n",
        ),
    );
    // A channel overlay carrying no `sources:` block at all.
    write(
        &ws.path().join("channel/plain/base.channel.yaml"),
        &per_target_overlay("config: { risk.threshold: 0.9 }\n"),
    );

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("patch"),
        &[],
        true,
    )
    .expect("resolve");
    let patches = res
        .source_patches()
        .expect("per-target overlay carries patches");
    assert!(!patches.is_empty(), "sources: block yields a patch");

    let mut config = parse_base(ws.path());
    apply_source_patches(&mut config, patches).expect("apply source patches");
    let body = config.source_bodies().next().expect("one source");
    let columns = body.schema.as_columns().expect("single-record schema");
    assert!(
        columns.iter().any(|c| c.name == "region"),
        "region column added: {:?}",
        columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // An overlay with no `sources:` block resolves to an empty patch map, not
    // an absent one.
    let res_plain = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("plain"),
        &[],
        true,
    )
    .expect("resolve");
    assert!(
        res_plain
            .source_patches()
            .expect("per-target overlay present")
            .is_empty()
    );
}

#[test]
fn per_target_source_options_patch_applies() {
    let ws = workspace();
    write(
        &ws.path().join("channel/patch/base.channel.yaml"),
        &per_target_overlay("sources:\n  orders:\n    options:\n      delimiter: \"|\"\n"),
    );

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "base",
        Some("patch"),
        &[],
        true,
    )
    .expect("resolve");
    let patches = res
        .source_patches()
        .expect("per-target overlay carries patches");

    let mut config = parse_base(ws.path());
    apply_source_patches(&mut config, patches).expect("apply source patches");
    let source = config.source_configs().next().expect("one source");
    match &source.format {
        InputFormat::Csv(Some(opts)) => assert_eq!(opts.delimiter.as_deref(), Some("|")),
        other => panic!("expected csv options, got {other:?}"),
    }
}

/// A pipeline carrying the format-structure patch surfaces: an X12 source
/// with a typed `GS` declaration and an HL7 source with one composite-field
/// split.
const EDI_PIPELINE: &str = r#"
pipeline:
  name: edi
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      path: po.x12
      options:
        group_section:
          name: grp
          fields:
            e06: string
      schema:
        - { name: seg_id, type: string }
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      path: msgs.hl7
      options:
        split_fields:
          - { field: f03, components: 5 }
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out_x12
    input: interchange
    config: { name: out_x12, type: csv, path: out1.csv }
  - type: output
    name: out_hl7
    input: messages
    config: { name: out_hl7, type: csv, path: out2.csv }
"#;

#[test]
fn per_target_source_format_structure_patch_applies() {
    let ws = workspace();
    write(&ws.path().join("pipeline/edi.yaml"), EDI_PIPELINE);
    write(
        &ws.path().join("channel/patch/edi.channel.yaml"),
        "channel:\n  target: ../../pipeline/edi.yaml\n\
         sources:\n\
         \x20 interchange:\n\
         \x20   group_section:\n\
         \x20     name: fg\n\
         \x20     fields:\n\
         \x20       e04: int\n\
         \x20   set_section:\n\
         \x20     name: txn\n\
         \x20     fields:\n\
         \x20       e01: string\n\
         \x20 messages:\n\
         \x20   split_fields:\n\
         \x20     f08: { components: 3 }\n\
         \x20     f03: remove\n",
    );

    let res = resolve(
        ws.path(),
        &channel_layout(),
        &group_layout(),
        "edi",
        Some("patch"),
        &[],
        true,
    )
    .expect("resolve");
    let patches = res
        .source_patches()
        .expect("per-target overlay carries patches");

    let yaml = fs::read_to_string(ws.path().join("pipeline/edi.yaml")).expect("read edi pipeline");
    let mut config: PipelineConfig = clinker_plan::yaml::from_str(&yaml).expect("parse");
    apply_source_patches(&mut config, patches).expect("apply source patches");

    let x12 = config
        .source_configs()
        .find(|s| s.name == "interchange")
        .expect("x12 source");
    match &x12.format {
        InputFormat::X12(Some(opts)) => {
            // The keyed modify renamed the GS declaration and added a typed
            // field alongside the base one.
            let group = opts.group_section.as_ref().expect("group_section kept");
            assert_eq!(group.name, "fg");
            assert_eq!(
                serde_json::to_value(&group.fields).expect("serialize fields"),
                json!({ "e06": "string", "e04": "int" })
            );
            // The set op created the previously-undeclared ST declaration.
            let set = opts.set_section.as_ref().expect("set_section created");
            assert_eq!(set.name, "txn");
            assert_eq!(
                serde_json::to_value(&set.fields).expect("serialize fields"),
                json!({ "e01": "string" })
            );
        }
        other => panic!("expected x12 options, got {other:?}"),
    }

    let hl7 = config
        .source_configs()
        .find(|s| s.name == "messages")
        .expect("hl7 source");
    match &hl7.format {
        InputFormat::Hl7(Some(opts)) => {
            let splits = opts.split_fields.as_deref().expect("splits declared");
            assert_eq!(splits.len(), 1, "f03 removed, f08 added: {splits:?}");
            assert_eq!(splits[0].field, "f08");
            assert_eq!(
                (
                    splits[0].components,
                    splits[0].subcomponents,
                    splits[0].repetitions
                ),
                (3, 1, 1)
            );
        }
        other => panic!("expected hl7 options, got {other:?}"),
    }
}
