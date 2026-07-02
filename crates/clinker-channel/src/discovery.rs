//! Channel/group discovery and computed-path overlay resolution.
//!
//! This module replaces the old whole-workspace `*.channel.yaml` glob-scan
//! with the channel-centric layout the multi-tenant overlay system uses:
//!
//! - **Per-target overlay resolution is a computed path, not a scan.** Given a
//!   channel id and a target name, [`resolve_channel_overlay`] builds the exact
//!   on-disk folder (`<channel-root>/<shard>/<id>/`) and probes the three
//!   candidate overlay filenames directly, so `--channel <id>` resolves in
//!   O(1) directory lookups regardless of how many tenants exist. Enumerating
//!   ~3,000 channels with a workspace glob (the old model, capped at 50 files)
//!   never has to happen on the run path.
//! - **The full scan survives only for `channels lint`.** [`scan_channels`]
//!   and [`scan_groups`] walk the channel and group roots to enumerate every
//!   tenant folder and group definition. They keep the bounded, symlink-free
//!   walk pattern of `scan_workspace_signatures` in clinker-plan, with a
//!   budget sized for the lint use case rather than the run path.
//!
//! Roots and the directory-sharding scheme come from CH-1's
//! [`ChannelLayout`]/[`GroupLayout`] (`clinker.toml`). A relative root is
//! resolved against the workspace root here (the layout stores it verbatim).
//!
//! ## Overlay candidate filenames and ambiguity
//!
//! A per-target overlay may be written with a suffix that documents its kind
//! (`<target>.channel.yaml` for a pipeline, `<target>.comp.yaml` for a
//! composition) or as a bare `<target>.yaml`. The suffix is optional: the
//! overlay's `channel.target:` field is authoritative. Resolution therefore
//! treats two situations as hard errors rather than silently picking one:
//!
//! - **Multiple candidate files** for one target (e.g. both
//!   `orders.channel.yaml` and `orders.yaml`) — [`ChannelError::AmbiguousOverlay`].
//! - **Filename disagrees with `target:`** — a `.comp.yaml` file whose target
//!   names a pipeline, or a filename stem that does not match the target file
//!   stem — [`ChannelError::OverlayTargetMismatch`].

use std::path::{Path, PathBuf};

use clinker_core_types::{Diagnostic, LabeledSpan, Span};
use clinker_plan::config::{ChannelLayout, GroupLayout, ShardScheme};

use crate::error::ChannelError;
use crate::group::Group;
use crate::manifest::{ChannelManifest, OverlayFile};

/// Filename of the optional per-channel manifest inside each tenant folder.
const CHANNEL_MANIFEST_FILE: &str = "channel.cfg.yaml";

/// Filename suffix marking a group definition file.
const GROUP_FILE_SUFFIX: &str = ".group.yaml";

/// Maximum number of channel folders the lint scan enumerates before failing.
///
/// The run path never scans (it resolves by computed path), so this budget
/// only bounds `channels lint`. It is sized well above the epic's ~3,000
/// channels-per-pipeline target so a real workspace never trips it, while a
/// pathological tree still cannot exhaust resources.
const CHANNEL_SCAN_BUDGET: usize = 100_000;

/// Maximum number of group files the lint scan enumerates before failing.
const GROUP_SCAN_BUDGET: usize = 10_000;

/// Maximum filesystem depth for the group-root walk. Groups live directly
/// under the group root, but a bounded recursive walk tolerates light
/// sub-foldering without following symlink loops or depth bombs.
const GROUP_WALK_MAX_DEPTH: usize = 16;

// ── Overlay kind ────────────────────────────────────────────────────────

/// What a resolved per-target overlay overlays: a pipeline or a composition.
///
/// Derived from the overlay's authoritative `channel.target:` path (a
/// `.comp.yaml` target is a composition; anything else is a pipeline), then
/// cross-checked against the optional filename suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlayKind {
    /// The overlay targets a base pipeline (`<target>.channel.yaml` / bare).
    Pipeline,
    /// The overlay targets a composition (`<target>.comp.yaml` / bare).
    Composition,
}

// ── Discovered channel ──────────────────────────────────────────────────

/// One tenant folder found by [`scan_channels`].
///
/// The folder name *is* the channel id; the manifest is optional (present only
/// when the channel carries labels or channel-wide overlays).
#[derive(Debug, Clone)]
pub struct DiscoveredChannel {
    /// Channel id — the tenant folder name.
    pub id: String,
    /// Absolute (root-resolved) path to the tenant folder.
    pub dir: PathBuf,
    /// Parsed `channel.cfg.yaml`, or `None` when the folder has no manifest.
    pub manifest: Option<ChannelManifest>,
}

/// A per-target overlay resolved by [`resolve_channel_overlay`].
#[derive(Debug, Clone)]
pub struct ResolvedOverlay {
    /// Path to the overlay file that was loaded.
    pub path: PathBuf,
    /// Whether the overlay targets a pipeline or a composition (from the
    /// authoritative `channel.target:`).
    pub kind: OverlayKind,
    /// The parsed overlay body.
    pub overlay: OverlayFile,
}

// ── Root / folder path computation ──────────────────────────────────────

/// Resolve a layout root against the workspace root: absolute roots are used
/// verbatim, relative roots are joined onto the workspace root.
fn resolve_root(root: &Path, workspace_root: &Path) -> PathBuf {
    if root.is_absolute() {
        root.to_path_buf()
    } else {
        workspace_root.join(root)
    }
}

/// Compute the on-disk folder for a channel id under a shard scheme.
///
/// This is the canonical id→folder mapping the whole overlay system resolves
/// through; folder materialization must place channels at exactly these paths.
///
/// - `None` — `<root>/<id>/`
/// - `FirstChar` — `<root>/<first-char>/<id>/`
/// - `Hash` — `<root>/<bucket>/<id>/`, where `bucket` is the first BLAKE3 byte
///   of the id rendered as two lowercase hex digits (256 buckets), spreading
///   ids evenly regardless of prefix skew.
///
/// See [`ShardScheme`] for the scheme definitions.
pub fn channel_folder_path(channel_root: &Path, shard: ShardScheme, channel_id: &str) -> PathBuf {
    match shard {
        ShardScheme::None => channel_root.join(channel_id),
        ShardScheme::FirstChar => match channel_id.chars().next() {
            Some(first) => channel_root.join(first.to_string()).join(channel_id),
            None => channel_root.join(channel_id),
        },
        ShardScheme::Hash => {
            let bucket = format!("{:02x}", blake3::hash(channel_id.as_bytes()).as_bytes()[0]);
            channel_root.join(bucket).join(channel_id)
        }
    }
}

/// Depth (relative to the channel root) at which tenant folders sit for a
/// shard scheme: flat layouts put them one level down, sharded layouts two.
fn channel_folder_depth(shard: ShardScheme) -> usize {
    match shard {
        ShardScheme::None => 1,
        ShardScheme::FirstChar | ShardScheme::Hash => 2,
    }
}

// ── Overlay filename / target classification ────────────────────────────

/// Classify a per-target overlay filename by its suffix. A suffixed filename
/// pins a kind; a bare `<target>.yaml` returns `None` (kind unconstrained by
/// the filename, taken from `target:` alone).
fn classify_filename_kind(file_name: &str) -> Option<OverlayKind> {
    if file_name.ends_with(".comp.yaml") {
        Some(OverlayKind::Composition)
    } else if file_name.ends_with(".channel.yaml") {
        Some(OverlayKind::Pipeline)
    } else {
        None
    }
}

/// Classify the authoritative `channel.target:` path: a `.comp.yaml` target is
/// a composition, anything else a pipeline.
fn classify_target_kind(target: &str) -> OverlayKind {
    if target.ends_with(".comp.yaml") {
        OverlayKind::Composition
    } else {
        OverlayKind::Pipeline
    }
}

/// The file stem of a `channel.target:` path — its file name with the
/// `.comp.yaml` or `.yaml` suffix stripped.
fn target_stem(target: &str) -> &str {
    let file = Path::new(target)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(target);
    file.strip_suffix(".comp.yaml")
        .or_else(|| file.strip_suffix(".yaml"))
        .unwrap_or(file)
}

// ── Computed-path overlay resolution ────────────────────────────────────

/// Resolve a channel's overlay for one target by computed path.
///
/// Builds `<channel-root>/<shard>/<id>/` and probes the three candidate
/// filenames for `target_name` — `<target>.channel.yaml`, `<target>.comp.yaml`,
/// then bare `<target>.yaml`. Returns:
///
/// - `Ok(None)` when the tenant has no overlay for this target (a channel need
///   not override every pipeline);
/// - `Ok(Some(_))` for exactly one candidate whose `channel.target:` agrees
///   with its filename (kind and stem);
/// - `Err(ChannelError::AmbiguousOverlay)` when more than one candidate exists;
/// - `Err(ChannelError::OverlayTargetMismatch)` when the sole candidate's
///   filename suffix or stem disagrees with its `channel.target:`.
///
/// `target_name` is the bare target stem (no extension), e.g.
/// `order_fulfillment`.
pub fn resolve_channel_overlay(
    layout: &ChannelLayout,
    workspace_root: &Path,
    channel_id: &str,
    target_name: &str,
) -> Result<Option<ResolvedOverlay>, ChannelError> {
    let root = resolve_root(&layout.root, workspace_root);
    let dir = channel_folder_path(&root, layout.shard, channel_id);

    // Ordered by suffix specificity; ordering only affects which candidate a
    // future single-match convention would prefer — with more than one present
    // we error regardless of order.
    let candidates = [
        dir.join(format!("{target_name}.channel.yaml")),
        dir.join(format!("{target_name}.comp.yaml")),
        dir.join(format!("{target_name}.yaml")),
    ];

    let mut present: Vec<PathBuf> = candidates.into_iter().filter(|p| p.is_file()).collect();

    match present.len() {
        0 => Ok(None),
        1 => {
            let path = present.remove(0);
            let overlay = OverlayFile::load(&path)?;
            let kind = verify_overlay_agreement(&path, &overlay, target_name)?;
            Ok(Some(ResolvedOverlay {
                path,
                kind,
                overlay,
            }))
        }
        _ => Err(ChannelError::AmbiguousOverlay {
            channel_id: channel_id.to_string(),
            target: target_name.to_string(),
            candidates: present,
        }),
    }
}

/// Verify the sole candidate's filename agrees with its authoritative
/// `channel.target:` and return the target's kind.
///
/// A suffixed filename must match the target kind (`.comp.yaml`↔composition,
/// `.channel.yaml`↔pipeline); a bare `.yaml` file constrains nothing by
/// suffix. In all cases the filename stem must equal the target file stem, so
/// `orders.channel.yaml` cannot silently overlay a differently-named pipeline.
fn verify_overlay_agreement(
    path: &Path,
    overlay: &OverlayFile,
    target_name: &str,
) -> Result<OverlayKind, ChannelError> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let declared = overlay.channel.target.as_str();
    let target_kind = classify_target_kind(declared);

    if let Some(filename_kind) = classify_filename_kind(file_name)
        && filename_kind != target_kind
    {
        return Err(ChannelError::OverlayTargetMismatch {
            path: path.to_path_buf(),
            declared: declared.to_string(),
            reason: format!(
                "filename suffix marks a {} overlay but target: names a {}",
                kind_label(filename_kind),
                kind_label(target_kind),
            ),
        });
    }

    let stem = target_stem(declared);
    if stem != target_name {
        return Err(ChannelError::OverlayTargetMismatch {
            path: path.to_path_buf(),
            declared: declared.to_string(),
            reason: format!("filename stem {target_name:?} does not match target stem {stem:?}"),
        });
    }

    Ok(target_kind)
}

fn kind_label(kind: OverlayKind) -> &'static str {
    match kind {
        OverlayKind::Pipeline => "pipeline",
        OverlayKind::Composition => "composition",
    }
}

// ── Lint scans ──────────────────────────────────────────────────────────

/// Enumerate every tenant folder under the channel root, loading each folder's
/// optional `channel.cfg.yaml` manifest.
///
/// This is the `channels lint` enumeration path; the run path resolves by
/// computed path instead (see [`resolve_channel_overlay`]). Tenant folders sit
/// at [`channel_folder_depth`] under the root for the configured shard scheme.
/// The walk rejects symlinks and is bounded by depth and [`CHANNEL_SCAN_BUDGET`].
///
/// A nonexistent channel root yields an empty list (a workspace may not have a
/// channel tree yet). A manifest parse error fails the scan with `E121`; a
/// budget overrun fails with `E120`.
pub fn scan_channels(
    layout: &ChannelLayout,
    workspace_root: &Path,
) -> Result<Vec<DiscoveredChannel>, Vec<Diagnostic>> {
    use walkdir::WalkDir;

    let root = resolve_root(&layout.root, workspace_root);
    if !root.exists() {
        return Ok(Vec::new());
    }

    let depth = channel_folder_depth(layout.shard);
    let mut channels = Vec::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    let walker = WalkDir::new(&root)
        .follow_links(false)
        .max_depth(depth)
        .into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            // Broken symlink / permission denied on a candidate: skip it, the
            // scan itself is not fatal on per-entry IO errors.
            Err(_) => continue,
        };

        // Only leaf tenant folders at the shard depth are channels; reject
        // symlinks explicitly (belt-and-suspenders with follow_links(false)).
        if entry.depth() != depth || entry.file_type().is_symlink() || !entry.file_type().is_dir() {
            continue;
        }

        let dir = entry.path();
        let Some(id) = dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        if channels.len() >= CHANNEL_SCAN_BUDGET {
            diagnostics.push(Diagnostic::error(
                "E120",
                format!(
                    "channel folder budget exceeded: more than {CHANNEL_SCAN_BUDGET} \
                     tenant folders under {}",
                    root.display()
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diagnostics);
        }

        let manifest_path = dir.join(CHANNEL_MANIFEST_FILE);
        let manifest = if manifest_path.is_file() {
            match ChannelManifest::load(&manifest_path) {
                Ok(m) => Some(m),
                Err(e) => {
                    diagnostics.push(Diagnostic::error(
                        "E121",
                        format!("failed to parse {}: {e}", manifest_path.display()),
                        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                    ));
                    return Err(diagnostics);
                }
            }
        } else {
            None
        };

        channels.push(DiscoveredChannel {
            id: id.to_string(),
            dir: dir.to_path_buf(),
            manifest,
        });
    }

    Ok(channels)
}

/// Enumerate every `*.group.yaml` definition under the group root.
///
/// Mirrors [`scan_channels`]: bounded, symlink-free walk. A nonexistent group
/// root yields an empty list. A parse error fails with `E123`; a budget
/// overrun fails with `E122`.
pub fn scan_groups(
    layout: &GroupLayout,
    workspace_root: &Path,
) -> Result<Vec<Group>, Vec<Diagnostic>> {
    use walkdir::WalkDir;

    let root = resolve_root(&layout.root, workspace_root);
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut groups = Vec::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    let walker = WalkDir::new(&root)
        .follow_links(false)
        .max_depth(GROUP_WALK_MAX_DEPTH)
        .into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let file_type = entry.file_type();
        if file_type.is_symlink() || !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let is_group = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(GROUP_FILE_SUFFIX))
            .unwrap_or(false);
        if !is_group {
            continue;
        }

        if groups.len() >= GROUP_SCAN_BUDGET {
            diagnostics.push(Diagnostic::error(
                "E122",
                format!(
                    "group file budget exceeded: more than {GROUP_SCAN_BUDGET} \
                     {GROUP_FILE_SUFFIX} files under {}",
                    root.display()
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diagnostics);
        }

        match Group::load(path) {
            Ok(group) => groups.push(group),
            Err(e) => {
                diagnostics.push(Diagnostic::error(
                    "E123",
                    format!("failed to parse {}: {e}", path.display()),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diagnostics);
            }
        }
    }

    Ok(groups)
}
