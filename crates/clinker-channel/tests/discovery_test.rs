//! Channel/group discovery + computed-path overlay resolution.

use std::fs;
use std::path::{Path, PathBuf};

use clinker_channel::discovery::{
    DiscoveredChannel, OverlayKind, channel_folder_path, resolve_channel_overlay, scan_channels,
    scan_groups,
};
use clinker_channel::error::ChannelError;
use clinker_plan::config::{ChannelLayout, GroupLayout, ShardScheme};

/// A workspace whose channel/group roots default to `channel`/`group`.
fn workspace() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn channel_layout(shard: ShardScheme) -> ChannelLayout {
    ChannelLayout {
        root: PathBuf::from("channel"),
        shard,
    }
}

fn group_layout() -> GroupLayout {
    GroupLayout {
        root: PathBuf::from("group"),
    }
}

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create dirs");
    }
    fs::write(path, contents).expect("write file");
}

/// Create a tenant folder for `id` at the sharded computed path and return it.
fn tenant_dir(ws: &Path, shard: ShardScheme, id: &str) -> PathBuf {
    let dir = channel_folder_path(&ws.join("channel"), shard, id);
    fs::create_dir_all(&dir).expect("create tenant dir");
    dir
}

fn overlay_yaml(target: &str) -> String {
    format!("channel:\n  target: {target}\nconfig: {{ fraud_check.threshold: 0.9 }}\n")
}

// ── Computed-path resolution ────────────────────────────────────────────

#[test]
fn resolves_channel_suffixed_overlay_by_tenant_id() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &dir.join("orders.channel.yaml"),
        &overlay_yaml("../../pipeline/orders.yaml"),
    );

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect("resolution should succeed")
    .expect("overlay should be found");

    assert_eq!(resolved.kind, OverlayKind::Pipeline);
    assert_eq!(resolved.path, dir.join("orders.channel.yaml"));
    assert_eq!(
        resolved.overlay.channel.target,
        "../../pipeline/orders.yaml"
    );
}

#[test]
fn resolves_bare_yaml_overlay() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &dir.join("orders.yaml"),
        &overlay_yaml("../../pipeline/orders.yaml"),
    );

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect("resolution should succeed")
    .expect("overlay should be found");

    // Bare filename: kind comes from the authoritative target path.
    assert_eq!(resolved.kind, OverlayKind::Pipeline);
}

#[test]
fn resolves_composition_overlay() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &dir.join("fraud_check.comp.yaml"),
        &overlay_yaml("../../composition/fraud_check.comp.yaml"),
    );

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "fraud_check",
    )
    .expect("resolution should succeed")
    .expect("overlay should be found");

    assert_eq!(resolved.kind, OverlayKind::Composition);
}

#[test]
fn resolves_under_first_char_shard() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::FirstChar, "globex");
    // FirstChar places the tenant under channel/g/globex/.
    assert_eq!(dir, ws.path().join("channel").join("g").join("globex"));
    write(
        &dir.join("orders.channel.yaml"),
        &overlay_yaml("../../../pipeline/orders.yaml"),
    );

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::FirstChar),
        ws.path(),
        "globex",
        "orders",
    )
    .expect("resolution should succeed")
    .expect("overlay should be found");
    assert_eq!(resolved.kind, OverlayKind::Pipeline);
}

#[test]
fn resolves_under_hash_shard() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::Hash, "globex");
    write(
        &dir.join("orders.channel.yaml"),
        &overlay_yaml("../../../pipeline/orders.yaml"),
    );

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::Hash),
        ws.path(),
        "globex",
        "orders",
    )
    .expect("resolution should succeed")
    .expect("overlay should be found");
    assert_eq!(resolved.kind, OverlayKind::Pipeline);
}

#[test]
fn missing_overlay_resolves_to_none() {
    let ws = workspace();
    tenant_dir(ws.path(), ShardScheme::None, "globex");

    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect("resolution should succeed");
    assert!(resolved.is_none());
}

#[test]
fn absent_tenant_folder_resolves_to_none() {
    let ws = workspace();
    // No tenant folder created at all.
    let resolved = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "nobody",
        "orders",
    )
    .expect("resolution should succeed");
    assert!(resolved.is_none());
}

// ── Ambiguity + disagreement errors ─────────────────────────────────────

#[test]
fn two_candidate_files_for_one_target_is_ambiguous() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &dir.join("orders.channel.yaml"),
        &overlay_yaml("../../pipeline/orders.yaml"),
    );
    write(
        &dir.join("orders.yaml"),
        &overlay_yaml("../../pipeline/orders.yaml"),
    );

    let err = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect_err("two candidates must be ambiguous");
    match err {
        ChannelError::AmbiguousOverlay {
            channel_id,
            target,
            candidates,
        } => {
            assert_eq!(channel_id, "globex");
            assert_eq!(target, "orders");
            assert_eq!(candidates.len(), 2);
        }
        other => panic!("expected AmbiguousOverlay, got {other:?}"),
    }
}

#[test]
fn filename_kind_disagreeing_with_target_errors() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    // `.comp.yaml` filename but the target names a pipeline.
    write(
        &dir.join("orders.comp.yaml"),
        &overlay_yaml("../../pipeline/orders.yaml"),
    );

    let err = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect_err("filename/target kind mismatch must error");
    assert!(matches!(err, ChannelError::OverlayTargetMismatch { .. }));
}

#[test]
fn filename_stem_disagreeing_with_target_errors() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    // Filename stem `orders` but the target's stem is `shipping`.
    write(
        &dir.join("orders.channel.yaml"),
        &overlay_yaml("../../pipeline/shipping.yaml"),
    );

    let err = resolve_channel_overlay(
        &channel_layout(ShardScheme::None),
        ws.path(),
        "globex",
        "orders",
    )
    .expect_err("filename/target stem mismatch must error");
    assert!(matches!(err, ChannelError::OverlayTargetMismatch { .. }));
}

// ── Lint scans ──────────────────────────────────────────────────────────

fn ids(channels: &[DiscoveredChannel]) -> Vec<String> {
    let mut v: Vec<String> = channels.iter().map(|c| c.id.clone()).collect();
    v.sort();
    v
}

#[test]
fn scan_channels_enumerates_tenant_folders_and_loads_manifests() {
    let ws = workspace();
    let globex = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &globex.join("channel.cfg.yaml"),
        "channel:\n  name: globex\nlabels: { region: west }\n",
    );
    // acme has no manifest — still a discovered channel.
    tenant_dir(ws.path(), ShardScheme::None, "acme");

    let channels =
        scan_channels(&channel_layout(ShardScheme::None), ws.path()).expect("scan should succeed");
    assert_eq!(
        ids(&channels),
        vec!["acme".to_string(), "globex".to_string()]
    );

    let globex = channels.iter().find(|c| c.id == "globex").unwrap();
    let manifest = globex.manifest.as_ref().expect("globex has a manifest");
    assert_eq!(manifest.channel.name, "globex");

    let acme = channels.iter().find(|c| c.id == "acme").unwrap();
    assert!(acme.manifest.is_none());
}

#[test]
fn scan_channels_enumerates_under_first_char_shard() {
    let ws = workspace();
    tenant_dir(ws.path(), ShardScheme::FirstChar, "globex");
    tenant_dir(ws.path(), ShardScheme::FirstChar, "acme");

    let channels = scan_channels(&channel_layout(ShardScheme::FirstChar), ws.path())
        .expect("scan should succeed");
    assert_eq!(
        ids(&channels),
        vec!["acme".to_string(), "globex".to_string()]
    );
}

#[test]
fn scan_channels_bad_manifest_fails_with_e121() {
    let ws = workspace();
    let dir = tenant_dir(ws.path(), ShardScheme::None, "globex");
    write(
        &dir.join("channel.cfg.yaml"),
        "channel:\n  not_name: oops\n",
    );

    let diags =
        scan_channels(&channel_layout(ShardScheme::None), ws.path()).expect_err("bad manifest");
    assert!(diags.iter().any(|d| d.code == "E121"));
}

#[test]
fn scan_channels_absent_root_is_empty() {
    let ws = workspace();
    let channels =
        scan_channels(&channel_layout(ShardScheme::None), ws.path()).expect("scan should succeed");
    assert!(channels.is_empty());
}

#[test]
fn scan_groups_enumerates_group_files() {
    let ws = workspace();
    write(
        &ws.path().join("group").join("enterprise.group.yaml"),
        "group:\n  name: enterprise\n  match: 'tier == \"enterprise\"'\n  priority: 20\n",
    );
    write(
        &ws.path().join("group").join("baseline.group.yaml"),
        "group:\n  name: baseline\n",
    );
    // A non-group file is ignored.
    write(&ws.path().join("group").join("README.md"), "not a group\n");

    let groups = scan_groups(&group_layout(), ws.path()).expect("scan should succeed");
    let mut names: Vec<String> = groups.iter().map(|g| g.name.clone()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["baseline".to_string(), "enterprise".to_string()]
    );

    let enterprise = groups.iter().find(|g| g.name == "enterprise").unwrap();
    assert_eq!(
        enterprise.selector.as_deref(),
        Some("tier == \"enterprise\"")
    );
    assert_eq!(enterprise.priority, 20);

    let baseline = groups.iter().find(|g| g.name == "baseline").unwrap();
    assert!(baseline.selector.is_none());
}

#[test]
fn scan_groups_bad_file_fails_with_e123() {
    let ws = workspace();
    write(
        &ws.path().join("group").join("broken.group.yaml"),
        "group:\n  nope: 1\n",
    );
    let diags = scan_groups(&group_layout(), ws.path()).expect_err("bad group");
    assert!(diags.iter().any(|d| d.code == "E123"));
}

#[test]
fn scan_groups_absent_root_is_empty() {
    let ws = workspace();
    let groups = scan_groups(&group_layout(), ws.path()).expect("scan should succeed");
    assert!(groups.is_empty());
}
