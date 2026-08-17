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
//! Roots and the directory-sharding scheme come from the
//! [`ChannelLayout`]/[`GroupLayout`] sections of `clinker.toml`. A relative
//! root is resolved against the workspace root here (the layout stores it
//! verbatim).
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

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clinker_core_types::{Diagnostic, FileId, LabeledSpan, Span};
use clinker_plan::config::composition::CompositionFile;
use clinker_plan::config::composition::WORKSPACE_COMPOSITION_BUDGET;
use clinker_plan::config::{ChannelLayout, GroupLayout, PipelineConfig, PipelineNode, ShardScheme};
use clinker_plan::overlay_ops::OverlayOp;
use clinker_plan::plan::bind_schema::MAX_COMPOSITION_DEPTH;
use clinker_plan::resources::{CatalogResourceKind, LogicalResourceId, WorkspaceCatalog};

use crate::error::ChannelError;
use crate::group::Group;
use crate::manifest::{ChannelManifest, OverlayFile};

/// Filename of the optional per-channel manifest inside each tenant folder.
pub const CHANNEL_MANIFEST_FILE: &str = "channel.cfg.yaml";

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

/// Identity of the exact bytes retained by a contained, single-open load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LayerSourceIdentity {
    pub(crate) path: PathBuf,
    pub(crate) byte_len: u64,
    pub(crate) content_hash: [u8; 32],
}

/// A parsed layer paired with the identity of the exact bytes that produced it.
#[derive(Debug)]
pub(crate) struct LoadedLayer<T> {
    pub(crate) value: T,
    pub(crate) identity: LayerSourceIdentity,
}

fn layer_error(context: &str, path: &Path, reason: impl std::fmt::Display) -> ChannelError {
    ChannelError::InvalidChannelResource {
        channel: context.to_string(),
        reason: format!("layer `{}`: {reason}", path.display()),
    }
}

fn path_has_parent(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
}

fn same_open_file(
    opened_file: &File,
    opened: &Metadata,
    current_path: &Path,
    current: &Metadata,
) -> bool {
    #[cfg(unix)]
    {
        let _ = (opened_file, current_path);
        use std::os::unix::fs::MetadataExt;
        opened.dev() == current.dev() && opened.ino() == current.ino()
    }
    #[cfg(windows)]
    {
        let _ = (opened, current);
        let Ok(current_file) = File::open(current_path) else {
            return false;
        };
        match (
            win_file_identity::query(opened_file),
            win_file_identity::query(&current_file),
        ) {
            (Ok(opened), Ok(current)) => opened == current,
            _ => false,
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (opened_file, current_path);
        opened.len() == current.len() && opened.modified().ok() == current.modified().ok()
    }
}

#[cfg(windows)]
mod win_file_identity {
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[repr(C)]
    struct ByHandleFileInformation {
        attributes: u32,
        creation_time: FileTime,
        last_access_time: FileTime,
        last_write_time: FileTime,
        volume_serial: u32,
        file_size_high: u32,
        file_size_low: u32,
        number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(
            file: *mut c_void,
            information: *mut ByHandleFileInformation,
        ) -> i32;
    }

    pub(super) fn query(file: &File) -> io::Result<(u32, u64)> {
        let mut information = MaybeUninit::<ByHandleFileInformation>::uninit();
        // SAFETY: `file` owns a valid Windows handle for the duration of this
        // call, and `information` points to writable storage for the exact
        // structure the API initializes on success.
        let ok = unsafe {
            GetFileInformationByHandle(
                file.as_raw_handle().cast::<c_void>(),
                information.as_mut_ptr(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call initializes the complete structure.
        let information = unsafe { information.assume_init() };
        let file_index =
            (u64::from(information.file_index_high) << 32) | u64::from(information.file_index_low);
        Ok((information.volume_serial, file_index))
    }
}

fn reject_symlink_components(root: &Path, path: &Path, context: &str) -> Result<(), ChannelError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| layer_error(context, path, "resolves outside its owned root"))?;
    if path_has_parent(relative) {
        return Err(layer_error(
            context,
            path,
            "contains parent traversal outside the admitted layer path",
        ));
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&current)
            .map_err(|error| layer_error(context, &current, format!("metadata failed: {error}")))?;
        if metadata.file_type().is_symlink() {
            return Err(layer_error(
                context,
                &current,
                "symlink entries are not admitted",
            ));
        }
    }
    Ok(())
}

/// Canonicalize, contain, open, bound, read, parse, and hash one layer.
///
/// Parsing and identity always consume the same buffer from one open handle.
pub(crate) fn read_contained_layer<T>(
    owned_root: &Path,
    path: &Path,
    context: &str,
    parse: impl FnOnce(&[u8], PathBuf) -> Result<T, ChannelError>,
) -> Result<LoadedLayer<T>, ChannelError> {
    let root = owned_root.canonicalize().map_err(|error| {
        layer_error(
            context,
            owned_root,
            format!("root cannot be opened: {error}"),
        )
    })?;
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    reject_symlink_components(&root, &candidate, context)?;
    let canonical = candidate.canonicalize().map_err(|error| {
        layer_error(
            context,
            &candidate,
            format!("canonicalization failed: {error}"),
        )
    })?;
    if !canonical.starts_with(&root) {
        return Err(layer_error(
            context,
            &candidate,
            "resolves outside its canonical owned root",
        ));
    }

    let file = File::open(&canonical)
        .map_err(|error| layer_error(context, &canonical, format!("open failed: {error}")))?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| layer_error(context, &canonical, format!("metadata failed: {error}")))?;
    if !opened_metadata.is_file() {
        return Err(layer_error(context, &canonical, "is not a regular file"));
    }
    reject_symlink_components(&root, &candidate, context)?;
    let current_canonical = candidate.canonicalize().map_err(|error| {
        layer_error(
            context,
            &candidate,
            format!("post-open canonicalization failed: {error}"),
        )
    })?;
    if current_canonical != canonical || !current_canonical.starts_with(&root) {
        return Err(layer_error(
            context,
            &candidate,
            "changed canonical identity while loading",
        ));
    }
    let current_metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|error| layer_error(context, &candidate, format!("metadata failed: {error}")))?;
    if current_metadata.file_type().is_symlink() {
        return Err(layer_error(
            context,
            &candidate,
            "became a symlink while loading",
        ));
    }
    if !same_open_file(&file, &opened_metadata, &candidate, &current_metadata) {
        return Err(layer_error(
            context,
            &candidate,
            "changed between containment validation and open",
        ));
    }

    let max = clinker_plan::yaml::MAX_INPUT_BYTES;
    let mut bytes = Vec::with_capacity(
        usize::try_from(opened_metadata.len())
            .unwrap_or(max)
            .min(max),
    );
    file.take((max as u64) + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| layer_error(context, &canonical, format!("read failed: {error}")))?;
    if bytes.len() > max {
        return Err(layer_error(
            context,
            &canonical,
            format!("exceeds the {max}-byte layer limit"),
        ));
    }
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let byte_len = bytes.len() as u64;
    let value = parse(&bytes, canonical.clone())
        .map_err(|error| layer_error(context, &canonical, format!("parse failed: {error}")))?;
    Ok(LoadedLayer {
        value,
        identity: LayerSourceIdentity {
            path: canonical,
            byte_len,
            content_hash,
        },
    })
}

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
    pub(crate) source_identity: LayerSourceIdentity,
}

/// A catalog pipeline plus the catalog compositions admitted by its closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTarget {
    /// Catalog identity of the selected pipeline.
    pub pipeline: LogicalResourceId,
    /// Catalog composition identities admitted by the selected closure when
    /// those identities are available from the catalog.
    pub compositions: BTreeSet<LogicalResourceId>,
    pub(crate) composition_paths: BTreeSet<PathBuf>,
}

impl ChannelTarget {
    /// Construct a pipeline-only target. Discovery fills its composition
    /// closure when validating structural target operations.
    pub fn pipeline(value: &str) -> Result<Self, clinker_plan::resources::ResourceError> {
        Ok(Self {
            pipeline: LogicalResourceId::parse(value)?,
            compositions: BTreeSet::new(),
            composition_paths: BTreeSet::new(),
        })
    }
}

/// Immutable, per-target view of one catalog channel resource.
#[derive(Debug, Clone)]
pub struct ChannelResource {
    /// Catalog identity of the selected channel.
    pub channel_id: LogicalResourceId,
    /// Selected pipeline and its admitted composition closure.
    pub target: ChannelTarget,
    /// Parsed channel-wide manifest.
    pub manifest: Arc<ChannelManifest>,
    pub(crate) manifest_identity: LayerSourceIdentity,
    /// Parsed pipeline-specific channel file.
    pub overlay: Arc<OverlayFile>,
    /// Resolved path of the pipeline-specific channel file.
    pub overlay_path: PathBuf,
    pub(crate) overlay_identity: LayerSourceIdentity,
}

struct DiscoveredTargetLayer {
    overlay: Arc<OverlayFile>,
    path: PathBuf,
    identity: LayerSourceIdentity,
}

/// Load a catalog channel folder, validate every declared target/file, and
/// return the overlay for `selected_pipeline` by logical identity.
pub fn discover_channel_resource(
    catalog: &WorkspaceCatalog,
    channel_id: &str,
    selected_pipeline: &str,
) -> Result<ChannelResource, ChannelError> {
    let channel_id = LogicalResourceId::parse(channel_id).map_err(|error| {
        ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: error.to_string(),
        }
    })?;
    let selected_pipeline = LogicalResourceId::parse(selected_pipeline).map_err(|error| {
        ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: error.to_string(),
        }
    })?;
    let directory = catalog
        .resolve(CatalogResourceKind::Channel, &channel_id)
        .map_err(|error| ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: error.to_string(),
        })?;
    if !directory.is_dir() {
        return Err(ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: "the catalog entry must name a folder containing `channel.cfg.yaml`"
                .to_string(),
        });
    }

    let manifest_path = directory.join(CHANNEL_MANIFEST_FILE);
    let manifest = read_contained_layer(
        directory,
        &manifest_path,
        &format!("channel `{channel_id}` manifest"),
        ChannelManifest::from_yaml_bytes,
    )?;
    if manifest.value.channel.name != channel_id.as_str() {
        return Err(ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!(
                "manifest declares `{}`; change `channel.name` to `{channel_id}`",
                manifest.value.channel.name
            ),
        });
    }

    let mut declared = BTreeSet::new();
    for target in &manifest.value.channel.targets {
        let id = LogicalResourceId::parse(&target.value).map_err(|error| {
            ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "invalid target at line {}: {error}",
                    target.referenced.line()
                ),
            }
        })?;
        catalog
            .resolve(CatalogResourceKind::Pipeline, &id)
            .map_err(|error| ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!("target at line {}: {error}", target.referenced.line()),
            })?;
        if !declared.insert(id.clone()) {
            return Err(ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "duplicate target `{id}` at line {}; remove the repeated identity",
                    target.referenced.line()
                ),
            });
        }
    }
    if !declared.contains(&selected_pipeline) {
        return Err(ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!(
                "selected pipeline `{selected_pipeline}` is not listed in `channel.targets`"
            ),
        });
    }

    let mut entries = std::fs::read_dir(directory)
        .map_err(|error| ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!("cannot enumerate channel folder: {error}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!("cannot enumerate channel folder: {error}"),
        })?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    let mut overlays: BTreeMap<LogicalResourceId, DiscoveredTargetLayer> = BTreeMap::new();
    for entry in entries {
        let path = entry.path();
        let file_type =
            entry
                .file_type()
                .map_err(|error| ChannelError::InvalidChannelResource {
                    channel: channel_id.to_string(),
                    reason: format!(
                        "cannot inspect channel folder entry `{}`: {error}",
                        path.display()
                    ),
                })?;
        if file_type.is_symlink() {
            return Err(ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "target layer `{}` is a symlink; use a contained regular file",
                    path.display()
                ),
            });
        }
        if !file_type.is_file() {
            continue;
        }
        let display_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "channel folder contains a non-UTF-8 file name at `{}`",
                    path.display()
                ),
            })?
            .to_string();
        if display_name == CHANNEL_MANIFEST_FILE
            || path.extension().and_then(|extension| extension.to_str()) != Some("yaml")
        {
            continue;
        }
        let overlay = read_contained_layer(
            directory,
            &path,
            &format!("channel `{channel_id}` target candidate `{display_name}`"),
            OverlayFile::from_yaml_bytes,
        )?;
        let target = LogicalResourceId::parse(&overlay.value.channel.target).map_err(|error| {
            ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!("target file `{display_name}`: {error}"),
            }
        })?;
        if !declared.contains(&target) {
            return Err(ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "target file declares `{target}`, which is not listed in `channel.targets`"
                ),
            });
        }
        catalog
            .resolve(CatalogResourceKind::Pipeline, &target)
            .map_err(|error| ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!("target file `{display_name}`: {error}"),
            })?;
        if overlays
            .insert(
                target.clone(),
                DiscoveredTargetLayer {
                    overlay: Arc::new(overlay.value),
                    path: path.clone(),
                    identity: overlay.identity,
                },
            )
            .is_some()
        {
            return Err(ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "duplicate target files declare `{target}`; keep exactly one file per logical pipeline"
                ),
            });
        }
    }

    let mut selected_target = None;
    for target_id in &declared {
        let target_layer = overlays.get(target_id).ok_or_else(|| {
            ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "`channel.targets` declares `{target_id}` but no target file names that identity"
                ),
            }
        })?;
        let pipeline_path = catalog
            .resolve(CatalogResourceKind::Pipeline, target_id)
            .map_err(|error| ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!("target `{target_id}`: {error}"),
            })?;
        let closure = load_target_closure(catalog, pipeline_path, &channel_id, target_id)?;
        validate_target_file_scope(
            &target_layer.overlay,
            pipeline_path,
            &closure,
            &channel_id,
            target_layer
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("target.yaml"),
        )?;
        if target_id == &selected_pipeline {
            selected_target = Some(channel_target_from_closure(
                catalog,
                selected_pipeline.clone(),
                &closure,
            ));
        }
    }
    let selected_layer = overlays.remove(&selected_pipeline).ok_or_else(|| {
        ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!("selected target `{selected_pipeline}` disappeared after validation"),
        }
    })?;
    let target = selected_target.ok_or_else(|| ChannelError::InvalidChannelResource {
        channel: channel_id.to_string(),
        reason: format!("selected target `{selected_pipeline}` was not validated"),
    })?;
    Ok(ChannelResource {
        channel_id,
        target,
        manifest: Arc::new(manifest.value),
        manifest_identity: manifest.identity,
        overlay: selected_layer.overlay,
        overlay_path: selected_layer.path,
        overlay_identity: selected_layer.identity,
    })
}

#[derive(Default)]
struct TargetClosure {
    compositions: BTreeSet<PathBuf>,
    nodes: BTreeSet<String>,
    sources: BTreeSet<String>,
}

pub(crate) fn discover_channel_target(
    catalog: &WorkspaceCatalog,
    pipeline: LogicalResourceId,
) -> Result<ChannelTarget, ChannelError> {
    let pipeline_path = catalog
        .resolve(CatalogResourceKind::Pipeline, &pipeline)
        .map_err(|error| ChannelError::InvalidChannelResource {
            channel: "standalone".to_string(),
            reason: error.to_string(),
        })?;
    let closure = load_target_closure(catalog, pipeline_path, &pipeline, &pipeline)?;
    Ok(channel_target_from_closure(catalog, pipeline, &closure))
}

fn channel_target_from_closure(
    catalog: &WorkspaceCatalog,
    pipeline: LogicalResourceId,
    closure: &TargetClosure,
) -> ChannelTarget {
    let compositions = closure
        .compositions
        .iter()
        .filter_map(|path| {
            catalog
                .identify(CatalogResourceKind::Composition, path)
                .cloned()
        })
        .collect();
    ChannelTarget {
        pipeline,
        compositions,
        composition_paths: closure.compositions.clone(),
    }
}

fn load_target_closure(
    catalog: &WorkspaceCatalog,
    pipeline_path: &Path,
    channel_id: &LogicalResourceId,
    target_id: &LogicalResourceId,
) -> Result<TargetClosure, ChannelError> {
    let root = pipeline_path
        .parent()
        .ok_or_else(|| ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!("target `{target_id}` pipeline has no owned parent directory"),
        })?;
    let pipeline = read_contained_layer(
        root,
        pipeline_path,
        &format!("channel `{channel_id}` target `{target_id}` pipeline"),
        |bytes, path| {
            let yaml = std::str::from_utf8(bytes).map_err(|source| ChannelError::Utf8 {
                path: path.clone(),
                source,
            })?;
            clinker_plan::yaml::from_str::<PipelineConfig>(yaml).map_err(|source| {
                ChannelError::Yaml {
                    path,
                    source: Box::new(source.0),
                }
            })
        },
    )?;
    let mut closure = TargetClosure::default();
    collect_nodes(
        catalog,
        pipeline_path.parent().unwrap_or_else(|| Path::new(".")),
        &pipeline.value.nodes,
        &mut closure,
        channel_id,
        target_id,
        0,
    )?;
    Ok(closure)
}

fn collect_nodes(
    catalog: &WorkspaceCatalog,
    base_dir: &Path,
    nodes: &[clinker_plan::yaml::Spanned<PipelineNode>],
    closure: &mut TargetClosure,
    channel_id: &LogicalResourceId,
    target_id: &LogicalResourceId,
    depth: usize,
) -> Result<(), ChannelError> {
    if depth > MAX_COMPOSITION_DEPTH as usize {
        return Err(ChannelError::InvalidChannelResource {
            channel: channel_id.to_string(),
            reason: format!(
                "composition closure exceeds the maximum depth of {MAX_COMPOSITION_DEPTH}"
            ),
        });
    }
    for node in nodes {
        closure.nodes.insert(node.value.name().to_string());
        if matches!(node.value, PipelineNode::Source { .. }) {
            closure.sources.insert(node.value.name().to_string());
        }
        if let PipelineNode::Composition { r#use, .. } = &node.value {
            let requested = base_dir.join(r#use);
            let path = requested.canonicalize().map_err(|error| {
                ChannelError::InvalidChannelResource {
                    channel: channel_id.to_string(),
                    reason: format!(
                        "target `{target_id}` composition `{}` cannot be canonicalized: {error}",
                        requested.display()
                    ),
                }
            })?;
            let composition_id = catalog
                .identify(CatalogResourceKind::Composition, &path)
                .ok_or_else(|| ChannelError::InvalidChannelResource {
                    channel: channel_id.to_string(),
                    reason: format!(
                        "target `{target_id}` composition `{}` is outside the admitted catalog composition boundary",
                        requested.display()
                    ),
                })?;
            if !closure.compositions.insert(path.clone()) {
                continue;
            }
            if closure.compositions.len() > WORKSPACE_COMPOSITION_BUDGET {
                return Err(ChannelError::InvalidChannelResource {
                    channel: channel_id.to_string(),
                    reason: format!(
                        "target `{target_id}` composition closure exceeds the workspace budget of {WORKSPACE_COMPOSITION_BUDGET}"
                    ),
                });
            }
            let root = path
                .parent()
                .ok_or_else(|| ChannelError::InvalidChannelResource {
                    channel: channel_id.to_string(),
                    reason: format!("catalog composition `{composition_id}` has no owned parent"),
                })?;
            let composition = read_contained_layer(
                root,
                &path,
                &format!(
                    "channel `{channel_id}` target `{target_id}` composition `{composition_id}`"
                ),
                |bytes, source_path| {
                    let yaml = std::str::from_utf8(bytes).map_err(|source| ChannelError::Utf8 {
                        path: source_path.clone(),
                        source,
                    })?;
                    CompositionFile::parse(
                        yaml,
                        FileId::new(std::num::NonZeroU32::MIN),
                        source_path,
                    )
                    .map_err(|error| ChannelError::InvalidChannelResource {
                        channel: channel_id.to_string(),
                        reason: format!("composition `{composition_id}` is invalid: {error}"),
                    })
                },
            )?;
            collect_nodes(
                catalog,
                path.parent().unwrap_or_else(|| Path::new(".")),
                &composition.value.nodes,
                closure,
                channel_id,
                target_id,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

fn validate_target_file_scope(
    overlay: &OverlayFile,
    pipeline_path: &Path,
    closure: &TargetClosure,
    channel_id: &LogicalResourceId,
    file_name: &str,
) -> Result<(), ChannelError> {
    for operation in &overlay.overrides {
        let target = match &operation.value {
            OverlayOp::Remove(value) => Some(value.target.as_str()),
            OverlayOp::Replace(value) => Some(value.target.as_str()),
            OverlayOp::Set(value) => Some(value.target.as_str()),
            OverlayOp::Bypass(value) => Some(value.target.as_str()),
            OverlayOp::PatchSchema(value) => Some(value.target.as_str()),
            OverlayOp::BindResource(value) => Some(value.target.as_str()),
            OverlayOp::Add(value) => {
                if let Some(composition) = &value.composition {
                    let path = pipeline_path
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .join(composition)
                        .canonicalize()
                        .map_err(|error| ChannelError::InvalidChannelResource {
                            channel: channel_id.to_string(),
                            reason: format!(
                                "target file `{file_name}` composition at line {} cannot be opened: {error}",
                                operation.referenced.line()
                            ),
                        })?;
                    if !closure.compositions.contains(&path) {
                        return Err(ChannelError::InvalidChannelResource {
                            channel: channel_id.to_string(),
                            reason: format!(
                                "target file `{file_name}` composition at line {} is outside the selected pipeline closure; move the operation to the pipeline that owns it",
                                operation.referenced.line()
                            ),
                        });
                    }
                }
                value
                    .after
                    .as_deref()
                    .or(value.before.as_deref())
                    .or_else(|| value.input.as_ref().map(|input| input.name()))
            }
        };
        if let Some(target) = target
            && !closure.nodes.contains(target)
        {
            return Err(ChannelError::InvalidChannelResource {
                channel: channel_id.to_string(),
                reason: format!(
                    "target file `{file_name}` operation at line {} names `{target}` outside the selected pipeline and composition closure",
                    operation.referenced.line()
                ),
            });
        }
    }
    Ok(())
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

pub(crate) fn contained_layout_root(
    configured_root: &Path,
    workspace_root: &Path,
    context: &str,
) -> Result<Option<PathBuf>, ChannelError> {
    if path_has_parent(configured_root) {
        return Err(layer_error(
            context,
            configured_root,
            "contains parent traversal",
        ));
    }
    // An explicit absolute root is itself the admitted boundary. This is a
    // supported catalog layout (for example, a separately mounted catalog),
    // so it is not required to sit below the workspace. Relative roots remain
    // workspace-owned and retain the stricter workspace containment check.
    let workspace = if configured_root.is_absolute() {
        None
    } else {
        Some(workspace_root.canonicalize().map_err(|error| {
            layer_error(
                context,
                workspace_root,
                format!("workspace root cannot be opened: {error}"),
            )
        })?)
    };
    let candidate = match &workspace {
        Some(workspace) => resolve_root(configured_root, workspace),
        None => configured_root.to_path_buf(),
    };
    if !candidate.try_exists().map_err(|error| {
        layer_error(
            context,
            &candidate,
            format!("existence check failed: {error}"),
        )
    })? {
        return Ok(None);
    }
    if let Some(workspace) = &workspace {
        reject_symlink_components(workspace, &candidate, context)?;
    }
    let canonical = candidate.canonicalize().map_err(|error| {
        layer_error(
            context,
            &candidate,
            format!("canonicalization failed: {error}"),
        )
    })?;
    if let Some(workspace) = &workspace
        && !canonical.starts_with(workspace)
    {
        return Err(layer_error(
            context,
            &candidate,
            "resolves outside its admitted root",
        ));
    }
    Ok(Some(canonical))
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

/// The on-disk tenant folder for `channel_id` under a channel layout, resolved
/// against the workspace root.
///
/// This is the folder [`resolve_channel_overlay`] probes for per-target
/// overlays and where the optional [`CHANNEL_MANIFEST_FILE`] manifest lives.
/// It composes [`resolve_root`] (workspace-relative-or-absolute root) with
/// [`channel_folder_path`] (the shard-aware id→folder mapping), so callers get
/// the same path both the run-path computed lookup and the lint scan land on.
pub fn channel_dir(layout: &ChannelLayout, workspace_root: &Path, channel_id: &str) -> PathBuf {
    let root = resolve_root(&layout.root, workspace_root);
    channel_folder_path(&root, layout.shard, channel_id)
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
    let Some(root) = contained_layout_root(&layout.root, workspace_root, "channel root")? else {
        return Ok(None);
    };
    let dir = channel_folder_path(&root, layout.shard, channel_id);

    // Ordered by suffix specificity; ordering only affects which candidate a
    // future single-match convention would prefer — with more than one present
    // we error regardless of order.
    let candidates = [
        dir.join(format!("{target_name}.channel.yaml")),
        dir.join(format!("{target_name}.comp.yaml")),
        dir.join(format!("{target_name}.yaml")),
    ];

    let mut present = Vec::new();
    for candidate in candidates {
        match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(layer_error(
                    &format!("channel `{channel_id}` target `{target_name}`"),
                    &candidate,
                    "symlink entries are not admitted",
                ));
            }
            Ok(metadata) if metadata.is_file() => present.push(candidate),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(layer_error(
                    &format!("channel `{channel_id}` target `{target_name}`"),
                    &candidate,
                    format!("metadata failed: {error}"),
                ));
            }
        }
    }

    match present.len() {
        0 => Ok(None),
        1 => {
            let path = present.remove(0);
            let loaded = read_contained_layer(
                &root,
                &path,
                &format!("channel `{channel_id}` target `{target_name}`"),
                OverlayFile::from_yaml_bytes,
            )?;
            let kind = verify_overlay_agreement(&path, &loaded.value, target_name)?;
            Ok(Some(ResolvedOverlay {
                path,
                kind,
                overlay: loaded.value,
                source_identity: loaded.identity,
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

    let root = contained_layout_root(&layout.root, workspace_root, "channel scan root").map_err(
        |error| {
            vec![Diagnostic::error(
                "E121",
                error.to_string(),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            )]
        },
    )?;
    let Some(root) = root else {
        return Ok(Vec::new());
    };

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
            Err(error) => {
                diagnostics.push(Diagnostic::error(
                    "E121",
                    format!("channel scan failed under {}: {error}", root.display()),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diagnostics);
            }
        };

        // Only leaf tenant folders at the shard depth are channels; reject
        // symlinks explicitly (belt-and-suspenders with follow_links(false)).
        if entry.file_type().is_symlink() {
            diagnostics.push(Diagnostic::error(
                "E121",
                format!(
                    "channel scan rejected symlink entry `{}`",
                    entry.path().display()
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diagnostics);
        }
        if entry.depth() != depth || !entry.file_type().is_dir() {
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
        let manifest = match std::fs::symlink_metadata(&manifest_path) {
            Ok(_) => {
                match read_contained_layer(
                    dir,
                    &manifest_path,
                    &format!("channel `{id}` manifest"),
                    ChannelManifest::from_yaml_bytes,
                ) {
                    Ok(loaded) => Some(loaded.value),
                    Err(e) => {
                        diagnostics.push(Diagnostic::error(
                            "E121",
                            format!("failed to parse {}: {e}", manifest_path.display()),
                            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                        ));
                        return Err(diagnostics);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                diagnostics.push(Diagnostic::error(
                    "E121",
                    format!("cannot inspect {}: {error}", manifest_path.display()),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diagnostics);
            }
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
    scan_groups_with_identity(layout, workspace_root)
        .map(|groups| groups.into_iter().map(|loaded| loaded.value).collect())
}

pub(crate) fn scan_groups_with_identity(
    layout: &GroupLayout,
    workspace_root: &Path,
) -> Result<Vec<LoadedLayer<Group>>, Vec<Diagnostic>> {
    use walkdir::WalkDir;

    let root = contained_layout_root(&layout.root, workspace_root, "group scan root").map_err(
        |error| {
            vec![Diagnostic::error(
                "E123",
                error.to_string(),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            )]
        },
    )?;
    let Some(root) = root else {
        return Ok(Vec::new());
    };

    let mut groups = Vec::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    let walker = WalkDir::new(&root)
        .follow_links(false)
        .max_depth(GROUP_WALK_MAX_DEPTH)
        .into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(error) => {
                diagnostics.push(Diagnostic::error(
                    "E123",
                    format!("group scan failed under {}: {error}", root.display()),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
                return Err(diagnostics);
            }
        };

        let file_type = entry.file_type();
        if file_type.is_symlink() {
            diagnostics.push(Diagnostic::error(
                "E123",
                format!(
                    "group scan rejected symlink entry `{}`",
                    entry.path().display()
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            ));
            return Err(diagnostics);
        }
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            vec![Diagnostic::error(
                "E123",
                format!(
                    "group scan found a non-UTF-8 file name at `{}`",
                    path.display()
                ),
                LabeledSpan::primary(Span::SYNTHETIC, String::new()),
            )]
        })?;
        if !file_name.ends_with(GROUP_FILE_SUFFIX) {
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

        match read_contained_layer(
            &root,
            path,
            &format!("group candidate `{file_name}`"),
            Group::from_yaml_bytes,
        ) {
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
