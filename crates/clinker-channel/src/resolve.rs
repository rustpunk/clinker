//! End-to-end channel/group overlay resolution for one CLI invocation.
//!
//! This is the seam the CLI (`run`, `explain`, `channels resolve`,
//! `channels lint`) resolves an overlay stack through. It ties the discovery
//! ([`crate::discovery`]), group derivation ([`crate::derivation`]), and the
//! two application surfaces — the structural op stream ([`OverlayOp`], applied
//! pre-compile via `CompileContext::overlay_ops`) and the `config`/`vars` value
//! clobber ([`crate::overlay`], applied post-compile over the plan's
//! `ProvenanceDb`) — into one resolution.
//!
//! # Two-phase application
//!
//! The two surfaces apply at different points in the compile pipeline, so a
//! caller uses a [`OverlayResolution`] in two steps:
//!
//! 1. **Pre-compile:** feed [`OverlayResolution::op_stream`] into
//!    `CompileContext::overlay_ops` so the structural ops splice into the base
//!    AST before schema binding — the *effective* DAG is what compiles.
//! 2. **Post-compile:** call [`OverlayResolution::apply_config_and_vars`] on the
//!    compiled plan to clobber `config` onto its `ProvenanceDb` and resolve
//!    `vars` into runtime values for `PipelineRunParams`.
//!
//! # Layer stack
//!
//! Both surfaces resolve through the fixed semantic order
//! `pipeline-default < group(s) by priority < channel-wide < channel-per-target`.
//! Groups are enumerated once (they are few, unlike the ~3,000-tenant channel
//! tree) and either derived from the channel's `labels` or force-included by
//! name; the channel manifest and per-target overlay supply the two channel
//! layers. A plain invocation (no channel id, no groups) resolves to an empty
//! stack, so the compile path is byte-identical to a bare pipeline.

use std::path::Path;

use clinker_core_types::{Diagnostic, LabeledSpan, Span};

use clinker_plan::config::composition::LayerKind;
use clinker_plan::config::pipeline_node::PipelineNode;
use clinker_plan::config::{ChannelLayout, GroupLayout, PipelineConfig, SourceConfigPatch};
use clinker_plan::overlay_ops::{LayeredOp, OverlayLayer, OverlayOp};
use clinker_plan::plan::{ChannelIdentity, CompiledPlan};
use clinker_plan::yaml::Spanned;

use crate::derivation::{derive_groups, group_layer};
use crate::discovery::{
    CHANNEL_MANIFEST_FILE, OverlayKind, ResolvedOverlay, channel_dir, resolve_channel_overlay,
};
use crate::error::ChannelError;
use crate::group::Group;
use crate::manifest::{ChannelManifest, ChannelVars};
use crate::overlay::{
    ChannelOverlayResult, EffectiveConfig, apply_config_clobber, resolve_vars_layer,
    validate_config_keys,
};
use crate::{apply_group_config, scan_groups};

/// Why a group joined the overlay stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSource {
    /// The group's selector matched the channel's labels.
    Derived,
    /// The group was force-included by name (`--group`), regardless of selector.
    Explicit,
}

impl GroupSource {
    /// A short label for reporting.
    pub fn label(self) -> &'static str {
        match self {
            GroupSource::Derived => "derived",
            GroupSource::Explicit => "explicit",
        }
    }
}

/// One group applied to the stack, carrying the layer identities both surfaces
/// resolve through plus the owned group for the post-compile phase.
#[derive(Debug, Clone)]
pub struct AppliedGroup {
    /// Group name.
    pub name: String,
    /// Authored priority (higher wins among matching groups).
    pub priority: i64,
    /// Why this group was applied.
    pub source: GroupSource,
    /// Config-clobber layer (carries priority + declaration seq).
    layer: LayerKind,
    /// Op-stream precedence layer.
    op_layer: OverlayLayer,
    /// The parsed group (for `config`/`vars` application post-compile).
    group: Group,
}

/// A node injected by an `add` op, attributed to the overlay layer / source
/// that introduced it — the "which group added which node" report.
#[derive(Debug, Clone)]
pub struct InjectedNode {
    /// Name of the injected node.
    pub node: String,
    /// The overlay layer whose op added it.
    pub layer: OverlayLayer,
    /// Human-readable source (group or channel name).
    pub source: String,
}

/// The resolved channel context: id plus its optional manifest and per-target
/// overlay.
#[derive(Debug, Clone)]
struct ChannelContext {
    id: String,
    manifest: Option<ChannelManifest>,
    per_target: Option<ResolvedOverlay>,
    /// BLAKE3 over the channel id plus the raw bytes of its manifest and
    /// per-target overlay files. Content-sensitive so a changed overlay yields
    /// a distinct [`ChannelIdentity`], rather than a bare id hash that would
    /// collide across edits.
    content_hash: [u8; 32],
}

/// A fully resolved overlay stack for one (target, channel?, groups)
/// invocation. See the module docs for the two-phase application contract.
#[derive(Debug, Clone)]
pub struct OverlayResolution {
    channel: Option<ChannelContext>,
    applied_groups: Vec<AppliedGroup>,
    injected_nodes: Vec<InjectedNode>,
    op_stream: Vec<LayeredOp>,
}

impl OverlayResolution {
    /// Whether this resolution changes anything. An empty resolution means the
    /// compile + run path is identical to a bare pipeline.
    pub fn is_empty(&self) -> bool {
        self.channel.is_none() && self.applied_groups.is_empty() && self.op_stream.is_empty()
    }

    /// The concatenated, layer-tagged structural op stream. Feed this into
    /// `CompileContext::overlay_ops` before compiling so the effective DAG is
    /// what binds and lowers.
    pub fn op_stream(&self) -> &[LayeredOp] {
        &self.op_stream
    }

    /// Nodes injected by `add` ops, each attributed to its source layer.
    pub fn injected_nodes(&self) -> &[InjectedNode] {
        &self.injected_nodes
    }

    /// The applied groups, in ascending precedence (highest-priority last).
    pub fn applied_groups(&self) -> &[AppliedGroup] {
        &self.applied_groups
    }

    /// The resolved channel id, if a channel was selected.
    pub fn channel_id(&self) -> Option<&str> {
        self.channel.as_ref().map(|c| c.id.as_str())
    }

    /// Path to the resolved per-target overlay file, if the channel overlays
    /// this target.
    pub fn overlay_path(&self) -> Option<&Path> {
        self.channel
            .as_ref()
            .and_then(|c| c.per_target.as_ref())
            .map(|o| o.path.as_path())
    }

    /// Per-source config patches carried by the resolved per-target overlay,
    /// keyed by source-node name (empty when the channel has no per-target
    /// overlay, or the overlay declares no `sources:` block).
    ///
    /// Applied to the parsed pipeline config *before* validation/compile via
    /// [`apply_source_patches`](clinker_plan::config::apply_source_patches), so
    /// the effective plan observes the patched source shape (schema column ops,
    /// `array_paths`, and per-format `options`). Per-target scoping keeps the
    /// source-node keys resolvable against exactly the overlaid pipeline.
    pub fn source_patches(&self) -> Option<&indexmap::IndexMap<String, SourceConfigPatch>> {
        self.channel
            .as_ref()
            .and_then(|c| c.per_target.as_ref())
            .map(|o| &o.overlay.sources)
    }

    /// Resolve the winning composition `config:` value per node for the
    /// pre-compile `$config.<param>` fold, keyed
    /// `composition-node-name -> param -> value`.
    ///
    /// Applies the same layers in the same ascending-precedence order as
    /// [`Self::apply_config_and_vars`] (groups by priority → channel-wide →
    /// per-target), so the folded value matches the layer the `ProvenanceDb`
    /// renders as `[WON]`. The map feeds
    /// [`CompileContext::config_overrides`](clinker_plan::config::CompileContext);
    /// the provenance chain is still recorded separately by
    /// `apply_config_and_vars`, so this does not double-apply the override.
    pub fn effective_config_overrides(&self) -> clinker_plan::config::ConfigOverrides {
        let mut eff = EffectiveConfig::default();
        for applied in &self.applied_groups {
            eff.apply(&applied.group.config, applied.layer, false);
        }
        if let Some(ctx) = &self.channel {
            if let Some(manifest) = &ctx.manifest {
                eff.apply(&manifest.config, LayerKind::ChannelWide, false);
            }
            if let Some(overlay) = &ctx.per_target {
                eff.apply(&overlay.overlay.config, LayerKind::ChannelPerTarget, false);
            }
        }
        eff.into_overrides()
    }

    /// Apply the value-clobber surface (`config` + `vars`) of every layer over a
    /// compiled plan's `ProvenanceDb`, resolving `vars` into runtime values.
    ///
    /// Layers apply in ascending precedence (groups by priority, then
    /// channel-wide, then channel-per-target) so the highest layer wins each
    /// key. An unknown `config` key surfaces as an `E113` diagnostic (never a
    /// silent no-op); callers refuse to execute if any `Error` diagnostic is
    /// present. Returns the merged runtime `vars` maps for `PipelineRunParams`.
    pub fn apply_config_and_vars(
        &self,
        plan: &mut CompiledPlan,
        config: &PipelineConfig,
    ) -> ChannelOverlayResult {
        let mut result = ChannelOverlayResult::default();

        // Groups: ascending precedence. Config clobbers at each group's layer;
        // vars merge with later-wins.
        for applied in &self.applied_groups {
            if let Err(e) = apply_group_config(
                plan.provenance_mut(),
                &applied.group,
                applied.layer,
                &mut result.diagnostics,
            ) {
                push_config_error(&mut result.diagnostics, &applied.name, &e);
            }
            resolve_vars_layer(&applied.name, &applied.group.vars, config, &mut result);
        }

        if let Some(ctx) = &self.channel {
            // Channel-wide (manifest): applies to every pipeline this channel
            // runs.
            if let Some(manifest) = &ctx.manifest {
                clobber_config(
                    plan,
                    &manifest.config,
                    LayerKind::ChannelWide,
                    &ctx.id,
                    &mut result,
                );
                resolve_vars_layer(&ctx.id, &manifest.vars, config, &mut result);
            }
            // Per-target overlay: the highest layer.
            if let Some(overlay) = &ctx.per_target {
                clobber_config(
                    plan,
                    &overlay.overlay.config,
                    LayerKind::ChannelPerTarget,
                    &ctx.id,
                    &mut result,
                );
                apply_overlay_vars(overlay, &ctx.id, config, &mut result);
            }
            plan.set_channel_identity(ChannelIdentity {
                name: ctx.id.clone(),
                content_hash: ctx.content_hash,
            });
        }

        result
    }
}

/// Clobber one raw `config` map (string keys validated into dotted paths) onto
/// a plan's provenance at `layer`. A malformed key is a hard error surfaced as
/// a diagnostic; an unknown-but-well-formed key is `E113` from the clobber.
fn clobber_config(
    plan: &mut CompiledPlan,
    config: &indexmap::IndexMap<String, serde_json::Value>,
    layer: LayerKind,
    source_label: &str,
    result: &mut ChannelOverlayResult,
) {
    match validate_config_keys(config) {
        Ok(validated) => apply_config_clobber(
            plan.provenance_mut(),
            &validated,
            layer,
            false,
            source_label,
            &mut result.diagnostics,
        ),
        Err(e) => push_config_error(&mut result.diagnostics, source_label, &e),
    }
}

/// Resolve a per-target overlay's `vars`. Var overrides are not supported on a
/// composition overlay (E109): a composition overlay carrying vars raises the
/// diagnostic instead of silently resolving them.
fn apply_overlay_vars(
    overlay: &ResolvedOverlay,
    source_label: &str,
    config: &PipelineConfig,
    result: &mut ChannelOverlayResult,
) {
    if overlay.kind == OverlayKind::Composition && vars_present(&overlay.overlay.vars) {
        result.diagnostics.push(Diagnostic::error(
            "E109",
            format!(
                "channel {source_label:?}: var overrides not supported on composition overlay {}",
                overlay.path.display(),
            ),
            LabeledSpan::primary(Span::SYNTHETIC, String::new()),
        ));
        return;
    }
    resolve_vars_layer(source_label, &overlay.overlay.vars, config, result);
}

/// Whether any of the four var scopes carries an entry.
fn vars_present(vars: &ChannelVars) -> bool {
    !vars.static_scope.is_empty()
        || !vars.pipeline.is_empty()
        || !vars.source.is_empty()
        || !vars.record.is_empty()
}

/// Push a hard config-key error (malformed dotted path) as an `E113`
/// diagnostic, consistent with the unknown-key path.
fn push_config_error(diagnostics: &mut Vec<Diagnostic>, source_label: &str, err: &ChannelError) {
    diagnostics.push(Diagnostic::error(
        "E113",
        format!("overlay {source_label:?}: {err}"),
        LabeledSpan::primary(Span::SYNTHETIC, String::new()),
    ));
}

/// A failure resolving an overlay stack before compile.
#[derive(Debug)]
pub enum ResolveError {
    /// A discovery scan (groups) failed with diagnostics.
    Scan(Vec<Diagnostic>),
    /// A channel manifest / per-target overlay failed to load or was
    /// ambiguous.
    Channel(ChannelError),
    /// One or more group selectors failed to compile or evaluate (e.g. an
    /// unknown label). Each entry is `(group_name, reason)`.
    Selector(Vec<(String, String)>),
    /// An explicit `--group <name>` named no group under the group root.
    UnknownGroup {
        /// The requested group name.
        name: String,
        /// Group names that do exist, for a "did you mean" hint.
        known: Vec<String>,
    },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Scan(diags) => {
                write!(f, "group discovery failed: ")?;
                for (i, d) in diags.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "{}: {}", d.code, d.message)?;
                }
                Ok(())
            }
            ResolveError::Channel(e) => write!(f, "{e}"),
            ResolveError::Selector(errs) => {
                write!(f, "group selector error: ")?;
                for (i, (name, reason)) in errs.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "group {name:?}: {reason}")?;
                }
                Ok(())
            }
            ResolveError::UnknownGroup { name, known } => {
                write!(f, "no group named {name:?} under the group root")?;
                if !known.is_empty() {
                    write!(f, " (known: {})", known.join(", "))?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ResolveError {}

/// Resolve the overlay stack for one target.
///
/// `target_name` is the bare pipeline/composition stem (no extension), e.g.
/// `order_fulfillment`. `channel_id` selects a tenant by computed path;
/// `explicit_groups` force-includes groups by name; `auto_groups` enables
/// selector-derived group membership (suppressed by `--no-auto-groups`).
///
/// Auto-derivation needs a channel manifest's `labels`, so it is a no-op when
/// no channel is selected — explicit `--group` still applies, standalone.
pub fn resolve(
    workspace_root: &Path,
    channel_layout: &ChannelLayout,
    group_layout: &GroupLayout,
    target_name: &str,
    channel_id: Option<&str>,
    explicit_groups: &[String],
    auto_groups: bool,
) -> Result<OverlayResolution, ResolveError> {
    // Groups are few (bounded well below the tenant tree), so enumerating them
    // once is fine off the channel hot path. Explicit lookup and selector
    // derivation both read this set. Sort by name so the derivation `seq` (which
    // breaks equal-priority ties) is a stable, portable order rather than the
    // filesystem-dependent `scan_groups` walk order.
    let mut all_groups = scan_groups(group_layout, workspace_root).map_err(ResolveError::Scan)?;
    all_groups.sort_by(|a, b| a.name.cmp(&b.name));

    // Channel context: manifest (labels + channel-wide overlay) + per-target
    // overlay, both by computed path.
    let channel = match channel_id {
        Some(id) => {
            let dir = channel_dir(channel_layout, workspace_root, id);
            let manifest_path = dir.join(CHANNEL_MANIFEST_FILE);
            let manifest = if manifest_path.is_file() {
                Some(ChannelManifest::load(&manifest_path).map_err(ResolveError::Channel)?)
            } else {
                None
            };
            let per_target =
                resolve_channel_overlay(channel_layout, workspace_root, id, target_name)
                    .map_err(ResolveError::Channel)?;
            // Content-sensitive identity: id plus the raw bytes of the channel's
            // own overlay files (manifest + per-target). A changed overlay must
            // yield a distinct hash so any identity-keyed cache invalidates.
            let mut hasher = blake3::Hasher::new();
            hasher.update(id.as_bytes());
            if let Ok(bytes) = std::fs::read(&manifest_path) {
                hasher.update(&bytes);
            }
            if let Some(overlay) = &per_target
                && let Ok(bytes) = std::fs::read(&overlay.path)
            {
                hasher.update(&bytes);
            }
            Some(ChannelContext {
                id: id.to_string(),
                manifest,
                per_target,
                content_hash: *hasher.finalize().as_bytes(),
            })
        }
        None => None,
    };

    // Labels drive selector derivation; absent manifest ⇒ no labels.
    let labels = channel
        .as_ref()
        .and_then(|c| c.manifest.as_ref())
        .map(|m| m.labels.clone())
        .unwrap_or_default();

    // Choose group indices (into `all_groups`) with the source that pulled each
    // in. Auto-derived first (transparent; a selector error is fatal), then
    // explicit force-includes (deduped against the derived set).
    let mut chosen: Vec<(usize, GroupSource)> = Vec::new();

    if auto_groups && channel.is_some() {
        let derivation = derive_groups(&all_groups, &labels);
        if derivation.has_errors() {
            let errs = derivation
                .errors()
                .map(|(g, e)| (g.name.clone(), e.to_string()))
                .collect();
            return Err(ResolveError::Selector(errs));
        }
        for selection in derivation.selected() {
            // `seq` is the group's index in `all_groups` (assigned by
            // `derive_groups`' enumerate), so map back by index — not by name,
            // which would collapse two same-named group files onto one.
            let idx = selection.seq as usize;
            chosen.push((idx, GroupSource::Derived));
        }
    }

    for name in explicit_groups {
        let idx = all_groups
            .iter()
            .position(|g| &g.name == name)
            .ok_or_else(|| ResolveError::UnknownGroup {
                name: name.clone(),
                known: all_groups.iter().map(|g| g.name.clone()).collect(),
            })?;
        if !chosen.iter().any(|(i, _)| *i == idx) {
            chosen.push((idx, GroupSource::Explicit));
        }
    }

    // Materialize applied groups with their layer identities, then order by
    // ascending precedence so the highest-priority group applies last.
    let mut applied_groups: Vec<AppliedGroup> = chosen
        .into_iter()
        .map(|(idx, source)| {
            let group = all_groups[idx].clone();
            // Declaration-order seq = scan index; breaks equal-priority ties.
            let seq = u32::try_from(idx).unwrap_or(u32::MAX);
            let layer = group_layer(&group, seq);
            // Tag the op stream with the *same* (saturated) priority the config
            // layer carries, so both surfaces order groups whose priorities
            // saturate to the same value identically.
            let op_priority = match layer {
                LayerKind::Group { priority, .. } => i64::from(priority),
                _ => group.priority,
            };
            AppliedGroup {
                name: group.name.clone(),
                priority: group.priority,
                source,
                layer,
                op_layer: OverlayLayer::Group {
                    priority: op_priority,
                },
                group,
            }
        })
        .collect();
    applied_groups.sort_by_key(|a| a.layer);

    // Concatenate the structural op streams in ascending precedence, tagging
    // each op with its layer and recording every injected node.
    let mut op_stream = Vec::new();
    let mut injected_nodes = Vec::new();
    for applied in &applied_groups {
        push_ops(
            &mut op_stream,
            &mut injected_nodes,
            &applied.group.overrides,
            applied.op_layer,
            &applied.name,
        );
    }
    if let Some(ctx) = &channel {
        if let Some(manifest) = &ctx.manifest {
            push_ops(
                &mut op_stream,
                &mut injected_nodes,
                &manifest.overrides,
                OverlayLayer::ChannelWide,
                &ctx.id,
            );
        }
        if let Some(overlay) = &ctx.per_target {
            push_ops(
                &mut op_stream,
                &mut injected_nodes,
                &overlay.overlay.overrides,
                OverlayLayer::ChannelPerTarget,
                &ctx.id,
            );
        }
    }

    Ok(OverlayResolution {
        channel,
        applied_groups,
        injected_nodes,
        op_stream,
    })
}

/// Tag each op in `ops` with `layer`, push it onto the stream, and record any
/// node an `add` op introduces.
fn push_ops(
    op_stream: &mut Vec<LayeredOp>,
    injected: &mut Vec<InjectedNode>,
    ops: &[Spanned<OverlayOp>],
    layer: OverlayLayer,
    source: &str,
) {
    for op in ops {
        if let Some(name) = injected_node_name(&op.value) {
            injected.push(InjectedNode {
                node: name,
                layer,
                source: source.to_string(),
            });
        }
        op_stream.push(LayeredOp::new(layer, op.clone()));
    }
}

/// The name an `add` op introduces: the inline node's own name, or the
/// composition add's `alias`. Non-`add` ops introduce no node.
fn injected_node_name(op: &OverlayOp) -> Option<String> {
    match op {
        OverlayOp::Add(add) => add
            .node
            .as_ref()
            .map(|n: &PipelineNode| n.name().to_string())
            .or_else(|| add.alias.clone()),
        _ => None,
    }
}
