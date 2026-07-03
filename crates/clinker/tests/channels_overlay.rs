//! End-to-end coverage for the channel/group overlay CLI (CH-9/CH-13/CH-14).
//!
//! Exercises a small multi-tenant workspace through the four surfaces the
//! overlay system exposes: `run --group` (standalone group application),
//! `channels resolve` (effective plan + provenance, channel-only and
//! group-only), `channels lint` (full-tree scan), and the plain-pipeline path
//! (which must stay unaffected by the overlay wiring).
//!
//! The workspace pairs a base pipeline that uses a composition `scorer` (with a
//! `threshold` config knob) against:
//!   - a group `enterprise` (selector `tier == "enterprise"`, priority 20) that
//!     splices a `screen` composition into the consumed path and clobbers
//!     `scorer.threshold`;
//!   - a channel `globex` (labels `tier=enterprise`) whose manifest and
//!     per-target overlay clobber `scorer.threshold` at the higher layers.

use std::path::Path;
use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

const CLINKER_TOML: &str = "[channel]\nroot = \"channel\"\n\n[group]\nroot = \"group\"\n";
const ORDERS_CSV: &str = "order_id,amount\na1,150.0\na2,20.0\n";

const COMPOSITION: &str = r#"_compose:
  name: score
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  outputs:
    out: scored
  config_schema:
    threshold:
      type: float
      default: 0.5
nodes:
  - type: transform
    name: scored
    input: inp
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
"#;

const PIPELINE: &str = r#"pipeline:
  name: order_fulfillment
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
  - type: composition
    name: scorer
    input: normalize
    use: ../composition/score.comp.yaml
    inputs:
      inp: normalize
    config:
      threshold: 0.5
  - type: output
    name: out
    input: scorer
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// The screening composition the `enterprise` group splices into the consumed
/// data path (issue #768): it zeroes any amount at or above 100 and passes the
/// rest through. Its input port `inp` is bound to the splice anchor, and its
/// output feeds the base pipeline's `scorer` composition.
const SCREEN_COMPOSITION: &str = r#"_compose:
  name: screen
  inputs:
    inp:
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  outputs:
    out: screened
nodes:
  - type: transform
    name: screened
    input: inp
    config:
      cxl: |
        emit order_id = order_id
        emit amount = if amount >= 100.0 then 0.0 else amount
"#;

const GROUP: &str = r#"group:
  name: enterprise
  match: 'tier == "enterprise"'
  priority: 20
config:
  scorer.threshold: 0.8
overrides:
  - op: add
    composition: ../composition/screen.comp.yaml
    alias: screen
    after: normalize
"#;

const GLOBEX_MANIFEST: &str = r#"channel:
  name: globex
labels: { tier: enterprise, region: west }
config:
  scorer.threshold: 0.9
"#;

const GLOBEX_OVERLAY: &str = r#"channel:
  target: ../../pipeline/order_fulfillment.yaml
config:
  scorer.threshold: 0.95
"#;

/// A manifest declaring a non-enterprise `tier` label. A channel's manifest is
/// its label contract: without a declared `tier` the `enterprise` selector
/// would error on the unresolved identifier rather than cleanly not match, so
/// even the "broken" channels carry a manifest.
fn basic_manifest(name: &str) -> String {
    format!("channel:\n  name: {name}\nlabels: {{ tier: basic, region: west }}\n")
}

/// A per-target overlay whose `add` op splices after a node that does not
/// exist — the dangling splice anchor lint must surface (E114).
const DANGLING_OVERLAY: &str = r#"channel:
  target: ../../pipeline/order_fulfillment.yaml
overrides:
  - op: add
    node:
      type: transform
      name: dangling
      input: ghost_node
      config:
        cxl: "emit order_id = order_id"
    after: ghost_node
"#;

/// A per-target overlay with a config key matching no composition parameter —
/// the broken-overlay lint must surface (E113).
const BADKEY_OVERLAY: &str = r#"channel:
  target: ../../pipeline/order_fulfillment.yaml
config:
  scorer.bogus_param: 0.1
"#;

/// Build the valid workspace (globex only). With `broken`, add two channels
/// carrying intentionally invalid overlays for the lint failure test.
fn build_workspace(root: &Path, broken: bool) {
    write(root, "clinker.toml", CLINKER_TOML);
    write(root, "pipeline/orders.csv", ORDERS_CSV);
    write(root, "pipeline/order_fulfillment.yaml", PIPELINE);
    write(root, "composition/score.comp.yaml", COMPOSITION);
    write(root, "composition/screen.comp.yaml", SCREEN_COMPOSITION);
    write(root, "group/enterprise.group.yaml", GROUP);
    write(root, "channel/globex/channel.cfg.yaml", GLOBEX_MANIFEST);
    write(
        root,
        "channel/globex/order_fulfillment.channel.yaml",
        GLOBEX_OVERLAY,
    );
    if broken {
        write(
            root,
            "channel/dangling/channel.cfg.yaml",
            &basic_manifest("dangling"),
        );
        write(
            root,
            "channel/dangling/order_fulfillment.channel.yaml",
            DANGLING_OVERLAY,
        );
        write(
            root,
            "channel/badkey/channel.cfg.yaml",
            &basic_manifest("badkey"),
        );
        write(
            root,
            "channel/badkey/order_fulfillment.channel.yaml",
            BADKEY_OVERLAY,
        );
    }
}

fn pipeline_path(root: &Path) -> std::path::PathBuf {
    root.join("pipeline/order_fulfillment.yaml")
}

#[test]
fn plain_run_is_unaffected_by_overlay_wiring() {
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), false);

    let out = Command::new(clinker_bin())
        .arg("run")
        .arg(pipeline_path(tmp.path()))
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .args(["--dry-run", "-n", "2"])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "plain run must succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    // No overlay was requested, so the run must not announce one.
    assert!(
        !stderr.contains("applied overlay"),
        "a plain run must not apply any overlay.\nstderr: {stderr}"
    );
}

#[test]
fn run_with_group_splices_composition_into_consumed_path() {
    // The `enterprise` group splices the `screen` composition in after
    // `normalize`, so the effective path is `orders → normalize → screen →
    // scorer → out`. This is a real run (not `--dry-run`): the output proves the
    // injected composition's transform actually executes and its rows flow
    // downstream through `scorer`, the fix for issue #768. Before the fix the
    // injected composition was left unfed and bypassed — `scorer` read
    // `normalize` directly, so `a1` would keep its unscreened amount `150`.
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), false);

    // Run from the workspace dir so the output node's relative `out.csv` path
    // (resolved against the process cwd) lands inside the tempdir.
    let out = Command::new(clinker_bin())
        .current_dir(tmp.path())
        .arg("run")
        .arg(pipeline_path(tmp.path()))
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .args(["--group", "enterprise"])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "run --group must succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("applied overlay") && stderr.contains("enterprise"),
        "run --group must report the applied group.\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("2 written"),
        "the overlaid run must process both records.\nstdout: {stdout}"
    );

    // The output path resolves against the base dir; `screen` zeroed the
    // at-or-above-100 amount (`a1`: 150 → 0) and passed the rest through
    // (`a2`: 20). Both prove the injected composition ran in the consumed path.
    let output = std::fs::read_to_string(tmp.path().join("out.csv")).expect("read out.csv");
    assert!(
        output.contains("a1,0"),
        "screen must zero a1's amount (150 → 0) in the consumed path.\nout.csv:\n{output}"
    );
    assert!(
        output.contains("a2,20"),
        "screen must pass a2's amount through unchanged.\nout.csv:\n{output}"
    );
    assert!(
        !output.contains("a1,150"),
        "a1's unscreened amount must not survive — screen would be bypassed.\nout.csv:\n{output}"
    );
}

/// Split a `channels resolve` stdout at the effective-DAG boundary, keeping the
/// deterministic overlay report (the DAG text below carries volatile stats).
fn overlay_report(stdout: &str) -> String {
    stdout
        .split("\nEffective DAG:")
        .next()
        .unwrap_or(stdout)
        .to_string()
}

#[test]
fn channels_resolve_channel_renders_provenance() {
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), false);

    let out = Command::new(clinker_bin())
        .args(["channels", "resolve"])
        .arg(pipeline_path(tmp.path()))
        .args(["--channel", "globex"])
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "channels resolve --channel must succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );

    let report = overlay_report(&stdout);
    // Group derived from the channel's `tier=enterprise` label.
    assert!(
        report.contains("enterprise (priority 20, derived)"),
        "{report}"
    );
    // The group injected the screening composition.
    assert!(report.contains("screen <- enterprise"), "{report}");
    // The per-target overlay (0.95) won the 4-layer clobber over base 0.5.
    assert!(
        report.contains("scorer.threshold = 0.95") && report.contains("ChannelPerTarget"),
        "{report}"
    );
    // The deterministic overlay report snapshots cleanly.
    insta::assert_snapshot!("resolve_channel_globex", report);
}

#[test]
fn channels_resolve_group_standalone() {
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), false);

    let out = Command::new(clinker_bin())
        .args(["channels", "resolve"])
        .arg(pipeline_path(tmp.path()))
        .args(["--group", "enterprise"])
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "group-only resolve must succeed.\n{stdout}"
    );
    let report = overlay_report(&stdout);
    // No channel; the group is force-included by name.
    assert!(report.contains("channel: <none>"), "{report}");
    assert!(
        report.contains("enterprise (priority 20, explicit)"),
        "{report}"
    );
    // With no higher layer, the group's value wins.
    assert!(report.contains("scorer.threshold = 0.8"), "{report}");
    assert!(report.contains("screen <- enterprise"), "{report}");
}

#[test]
fn channels_lint_passes_on_valid_workspace() {
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), false);

    let out = Command::new(clinker_bin())
        .args(["channels", "lint"])
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "lint must pass on the valid workspace.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("OK"),
        "lint should report OK.\nstdout: {stdout}"
    );
}

#[test]
fn channels_lint_surfaces_broken_overlays() {
    let tmp = tempfile::tempdir().unwrap();
    build_workspace(tmp.path(), true);

    let out = Command::new(clinker_bin())
        .args(["channels", "lint"])
        .args(["--base-dir", tmp.path().to_str().unwrap()])
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "lint must fail when an overlay is broken.\nstdout: {stdout}\nstderr: {stderr}"
    );
    // A config key matching no composition parameter (E113).
    assert!(
        stderr.contains("E113") && stderr.contains("scorer.bogus_param"),
        "lint must surface the bad config key.\nstderr: {stderr}"
    );
    // A splice anchor naming a node that does not exist (E114).
    assert!(
        stderr.contains("E114") && stderr.contains("ghost_node"),
        "lint must surface the dangling splice anchor.\nstderr: {stderr}"
    );
}
