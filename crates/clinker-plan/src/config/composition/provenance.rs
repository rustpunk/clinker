//! Provenance tracking for resolved configuration values.
//!
//! Each config value in a compiled pipeline carries a [`ResolvedValue`] wrapper
//! that records which configuration layer contributed the winning value. The
//! provenance chain is stored in a side-table [`ProvenanceDb`], separate from
//! [`CompiledPlan`](crate::plan::compiled::CompiledPlan).
//!
//! # Layer stack
//!
//! The composition overlay resolves values through a fixed *semantic* order of
//! layers (never lexical or positional), encoded by [`LayerKind`]:
//!
//! ```text
//! PipelineDefault  <  Group(s) by priority  <  ChannelWide  <  ChannelPerTarget
//! ```
//!
//! Groups are *dynamic*: any number of groups may match a channel, so `Group`
//! carries its `priority` (higher wins) and a declaration-order source id
//! (`seq`, later declaration wins ties). Every group is a distinct layer.
//!
//! A layer may additionally be marked **`fixed`**: a fixed value locks the
//! resolution against every higher-precedence layer — nothing downstream may
//! override it. When several layers are fixed, the lowest-precedence fixed
//! layer wins, because it locked the value first.
//!
//! # Generic over the layer kind
//!
//! [`ResolvedValue`] and [`ProvenanceLayer`] are generic over the layer-kind
//! type `L` (any `Copy + Ord`). The composition overlay instantiates them with
//! [`LayerKind`]; a future schema-attribute resolver can reuse the *same*
//! implementation with a `SchemaLayer` kind and zero duplication. Precedence,
//! `fixed`-lock semantics, and single-winner selection are all defined purely
//! in terms of `L: Ord`.

use std::collections::HashMap;
use std::fmt;

use clinker_core_types::span::Span;

// Keep ProvenanceLayer at or under 64 bytes — the provenance side-table
// is sized for this footprint. Checked for the composition instantiation
// (`ProvenanceLayer<LayerKind>`), the widest layer kind in the crate.
const _: () = assert!(std::mem::size_of::<ProvenanceLayer>() <= 64);

/// Which configuration layer contributed a value.
///
/// Precedence is encoded via the derived `Ord`, which orders enum variants by
/// declaration order and struct-variant fields top-to-bottom:
///
/// ```text
/// PipelineDefault < Group { priority, seq } < ChannelWide < ChannelPerTarget
/// ```
///
/// Among `Group` variants, `priority` is compared first (higher priority is a
/// greater layer, so it wins), then `seq` (the group's declaration-order source
/// id; a later declaration is greater and wins ties). Higher-precedence layers
/// win over lower-precedence ones during [`ResolvedValue::apply_layer`], unless
/// a lower layer is `fixed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LayerKind {
    /// Base layer: a pipeline's own authored defaults / composition call-site
    /// values.
    PipelineDefault,
    /// A matching group overlay. `priority` (higher wins) then `seq` (the
    /// group's declaration-order source id, later wins) determine ordering
    /// among multiple matching groups.
    Group {
        /// Group priority; higher wins among multiple matching groups.
        priority: i32,
        /// Declaration-order source id of the group; breaks priority ties with
        /// later-declared groups winning.
        seq: u32,
    },
    /// Channel-wide overrides (`channel.cfg.yaml`), applied to every pipeline
    /// the channel runs.
    ChannelWide,
    /// Channel per-target overrides (`<target>.channel.yaml`).
    ChannelPerTarget,
}

impl fmt::Display for LayerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LayerKind::PipelineDefault => write!(f, "PipelineDefault"),
            LayerKind::Group { priority, seq } => {
                write!(f, "Group #{seq} (priority {priority})")
            }
            LayerKind::ChannelWide => write!(f, "ChannelWide"),
            LayerKind::ChannelPerTarget => write!(f, "ChannelPerTarget"),
        }
    }
}

/// A single provenance record: one layer's contribution to a config value.
///
/// The `won` flag marks the layer whose value was selected. Exactly one layer
/// in a [`ResolvedValue`]'s provenance chain has `won == true`.
///
/// Generic over the layer-kind `L`; the composition overlay uses the default
/// [`LayerKind`].
#[derive(Debug, Clone)]
pub struct ProvenanceLayer<L = LayerKind> {
    /// Source span of the value's origin in the YAML file. File identity
    /// is resolved via `SourceDb.path(span.file)` at render time.
    pub span: Span,
    /// Which configuration layer this value came from.
    pub kind: L,
    /// Whether this layer locks its value against every higher-precedence
    /// layer. A `fixed` layer cannot be overridden by anything downstream.
    pub fixed: bool,
    /// Whether this layer's value was selected as the winner.
    pub won: bool,
}

/// A config value together with its full provenance chain.
///
/// The `won` flag makes the winning layer explicit for tooling inspectors
/// without requiring the inspector to re-run priority logic.
///
/// Generic over the value type `T` and the layer-kind `L` (defaulting to
/// [`LayerKind`]). The provenance chain length is *not* fixed: it grows with
/// the number of matching group layers.
#[derive(Debug, Clone)]
pub struct ResolvedValue<T, L = LayerKind> {
    /// The winning value after all layers have been applied.
    pub value: T,
    /// Ordered provenance chain. Each entry records a layer's contribution.
    /// Exactly one entry has `won == true`.
    pub provenance: Vec<ProvenanceLayer<L>>,
    /// Per-layer values in application order. Stores the value contributed by
    /// each layer so `--explain --field` can display shadowed values. Kept as
    /// an ordered collection (not a fixed-size map) so multiple `Group` layers
    /// each keep their own entry, and separate from [`ProvenanceLayer`] to
    /// preserve its 64-byte size.
    pub layer_values: Vec<(L, T)>,
}

impl<T: Clone, L: Copy + Ord> ResolvedValue<T, L> {
    /// Create a new resolved value with a single provenance layer.
    /// The initial layer is always the winner.
    pub fn new(value: T, kind: L, span: Span) -> Self {
        Self::with_fixed(value, kind, span, false)
    }

    /// Create a new resolved value whose sole layer is `fixed` — it locks the
    /// value against any higher-precedence layer applied later.
    pub fn new_fixed(value: T, kind: L, span: Span) -> Self {
        Self::with_fixed(value, kind, span, true)
    }

    fn with_fixed(value: T, kind: L, span: Span, fixed: bool) -> Self {
        Self {
            provenance: vec![ProvenanceLayer {
                span,
                kind,
                fixed,
                won: true,
            }],
            layer_values: vec![(kind, value.clone())],
            value,
        }
    }

    /// Returns the layer that won (the one with `won == true`).
    pub fn winning_layer(&self) -> Option<&ProvenanceLayer<L>> {
        self.provenance.iter().find(|l| l.won)
    }

    /// Apply a new (non-fixed) layer on top. The new layer wins if it is the
    /// highest-precedence layer and no lower layer is `fixed`.
    ///
    /// Same-kind layers replace in place: the span/value are updated. A `Group`
    /// with a distinct `(priority, seq)` is a distinct layer and is appended.
    pub fn apply_layer(&mut self, value: T, kind: L, span: Span) {
        self.apply_layer_inner(value, kind, span, false);
    }

    /// Apply a new `fixed` layer on top. A fixed layer locks its value against
    /// every higher-precedence layer applied afterwards.
    pub fn apply_layer_fixed(&mut self, value: T, kind: L, span: Span) {
        self.apply_layer_inner(value, kind, span, true);
    }

    fn apply_layer_inner(&mut self, value: T, kind: L, span: Span, fixed: bool) {
        // Record this layer's value (same-kind replace-in-place).
        match self.layer_values.iter_mut().find(|(k, _)| *k == kind) {
            Some(slot) => slot.1 = value,
            None => self.layer_values.push((kind, value)),
        }

        // Record/replace the provenance entry for this kind.
        match self.provenance.iter_mut().find(|l| l.kind == kind) {
            Some(existing) => {
                existing.span = span;
                existing.fixed = fixed;
            }
            None => self.provenance.push(ProvenanceLayer {
                span,
                kind,
                fixed,
                won: false,
            }),
        }

        self.recompute_winner();
    }

    /// The winning layer kind: the lowest-precedence `fixed` layer if any layer
    /// is fixed (it locked the value against everything downstream), otherwise
    /// the highest-precedence layer. Deterministic from the layer set alone —
    /// independent of application order.
    fn winner_kind(&self) -> L {
        if let Some(locked) = self
            .provenance
            .iter()
            .filter(|l| l.fixed)
            .map(|l| l.kind)
            .min()
        {
            return locked;
        }
        self.provenance
            .iter()
            .map(|l| l.kind)
            .max()
            .expect("provenance chain is non-empty")
    }

    /// Recompute `won` flags and the resolved `value` from the current layer
    /// set. Exactly one layer wins (layer kinds are unique per value).
    fn recompute_winner(&mut self) {
        let winner = self.winner_kind();
        for layer in &mut self.provenance {
            layer.won = layer.kind == winner;
        }
        if let Some((_, value)) = self.layer_values.iter().find(|(k, _)| *k == winner) {
            self.value = value.clone();
        }
    }

    /// Look up the value contributed by a specific layer kind.
    pub fn layer_value(&self, kind: L) -> Option<&T> {
        self.layer_values
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, v)| v)
    }
}

/// Side-table mapping `(node_name, param_name)` to provenance-tracked config values.
///
/// Kept separate from [`CompileArtifacts`](crate::plan::bind_schema::CompileArtifacts)
/// to avoid polluting the hot typecheck path. Only populated for
/// `PipelineNode::Composition` nodes that have config params.
///
/// Part of the `CompileOutput { plan, provenance }` separation.
#[derive(Debug, Default, Clone)]
pub struct ProvenanceDb {
    entries: HashMap<(String, String), ResolvedValue<serde_json::Value>>,
}

impl ProvenanceDb {
    /// Insert a provenance entry for `(node_name, param_name)`.
    pub fn insert(
        &mut self,
        node_name: String,
        param_name: String,
        resolved: ResolvedValue<serde_json::Value>,
    ) {
        self.entries.insert((node_name, param_name), resolved);
    }

    /// Look up provenance for a specific `(node_name, param_name)`.
    pub fn get(
        &self,
        node_name: &str,
        param_name: &str,
    ) -> Option<&ResolvedValue<serde_json::Value>> {
        self.entries
            .get(&(node_name.to_owned(), param_name.to_owned()))
    }

    /// Mutable lookup for `(node_name, param_name)`. Used by the channel overlay
    /// to apply `ChannelWide`/`ChannelPerTarget` layers.
    pub fn get_mut(
        &mut self,
        node_name: &str,
        param_name: &str,
    ) -> Option<&mut ResolvedValue<serde_json::Value>> {
        self.entries
            .get_mut(&(node_name.to_owned(), param_name.to_owned()))
    }

    /// Iterate over all provenance entries.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&(String, String), &ResolvedValue<serde_json::Value>)> {
        self.entries.iter()
    }

    /// List all param names tracked for a given node.
    pub fn params_for_node(&self, node_name: &str) -> Vec<&str> {
        self.entries
            .keys()
            .filter(|(n, _)| n == node_name)
            .map(|(_, p)| p.as_str())
            .collect()
    }

    /// List all tracked node names.
    pub fn node_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .entries
            .keys()
            .map(|(n, _)| n.as_str())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        names.sort();
        names
    }

    /// Number of tracked entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the provenance table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(line: u32) -> Span {
        Span::line_only(line)
    }

    fn group(priority: i32, seq: u32) -> LayerKind {
        LayerKind::Group { priority, seq }
    }

    // ── LayerKind precedence ─────────────────────────────────────────────

    #[test]
    fn layer_kind_total_order_across_stack() {
        assert!(LayerKind::PipelineDefault < group(0, 0));
        assert!(group(1000, 999) < LayerKind::ChannelWide);
        assert!(LayerKind::ChannelWide < LayerKind::ChannelPerTarget);
    }

    #[test]
    fn group_ordering_priority_then_declaration() {
        // Higher priority is a greater layer.
        assert!(group(10, 5) < group(20, 0));
        // Equal priority: later declaration (higher seq) is greater.
        assert!(group(20, 1) < group(20, 2));
    }

    // ── Precedence resolution ────────────────────────────────────────────

    #[test]
    fn higher_layer_wins_over_lower() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelWide, span(2));
        assert_eq!(rv.value, 2);
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::ChannelWide);
    }

    #[test]
    fn full_stack_precedence_channel_per_target_wins() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, group(20, 0), span(2));
        rv.apply_layer(3, LayerKind::ChannelWide, span(3));
        rv.apply_layer(4, LayerKind::ChannelPerTarget, span(4));
        assert_eq!(rv.value, 4);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    #[test]
    fn lower_layer_applied_later_does_not_win() {
        // Applying a lower-precedence layer after a higher one must not flip
        // the winner — precedence is semantic, not positional.
        let mut rv = ResolvedValue::new(1, LayerKind::ChannelPerTarget, span(1));
        rv.apply_layer(2, LayerKind::PipelineDefault, span(2));
        assert_eq!(rv.value, 1);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    // ── Multiple groups ──────────────────────────────────────────────────

    #[test]
    fn multiple_groups_highest_priority_wins() {
        let mut rv = ResolvedValue::new(0, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(10, group(10, 0), span(2));
        rv.apply_layer(30, group(30, 1), span(3));
        rv.apply_layer(20, group(20, 2), span(4));
        assert_eq!(rv.value, 30, "priority 30 group must win");
        assert_eq!(rv.winning_layer().unwrap().kind, group(30, 1));
        // Every group keeps its own shadowed value.
        assert_eq!(rv.layer_value(group(10, 0)), Some(&10));
        assert_eq!(rv.layer_value(group(20, 2)), Some(&20));
        assert_eq!(rv.layer_value(group(30, 1)), Some(&30));
    }

    #[test]
    fn groups_same_priority_later_declaration_wins() {
        let mut rv = ResolvedValue::new(0, LayerKind::PipelineDefault, span(1));
        // Apply in reverse declaration order to prove ordering is by seq, not
        // application order.
        rv.apply_layer(200, group(20, 2), span(2));
        rv.apply_layer(100, group(20, 1), span(3));
        assert_eq!(rv.value, 200, "later-declared group (seq 2) must win");
        assert_eq!(rv.winning_layer().unwrap().kind, group(20, 2));
    }

    // ── Fixed lock ───────────────────────────────────────────────────────

    #[test]
    fn fixed_lower_layer_wins_over_higher() {
        let mut rv = ResolvedValue::new_fixed(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.value, 1, "fixed PipelineDefault must not be overridden");
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::PipelineDefault);
    }

    #[test]
    fn fixed_applied_mid_stack_locks_downstream() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelWide, span(2));
        rv.apply_layer(3, LayerKind::ChannelPerTarget, span(3));
        assert_eq!(rv.value, 2, "fixed ChannelWide locks out ChannelPerTarget");
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::ChannelWide);
    }

    #[test]
    fn lowest_fixed_layer_wins_among_several_fixed() {
        let mut rv = ResolvedValue::new_fixed(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelWide, span(2));
        assert_eq!(
            rv.value, 1,
            "lowest-precedence fixed layer locked the value first"
        );
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::PipelineDefault);
    }

    #[test]
    fn non_fixed_higher_still_wins_when_lower_not_fixed() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.value, 2);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    // ── Exactly-one-winner invariant ─────────────────────────────────────

    #[test]
    fn exactly_one_winning_layer() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, group(10, 0), span(2));
        rv.apply_layer_fixed(3, group(20, 1), span(3));
        rv.apply_layer(4, LayerKind::ChannelWide, span(4));
        rv.apply_layer(5, LayerKind::ChannelPerTarget, span(5));
        let winners = rv.provenance.iter().filter(|l| l.won).count();
        assert_eq!(winners, 1, "exactly one layer must win");
        // The fixed group locks out both channel layers.
        assert_eq!(rv.value, 3);
        assert_eq!(rv.winning_layer().unwrap().kind, group(20, 1));
    }

    #[test]
    fn same_kind_replaces_in_place() {
        let mut rv = ResolvedValue::new(1, LayerKind::ChannelWide, span(1));
        rv.apply_layer(2, LayerKind::ChannelWide, span(9));
        assert_eq!(rv.value, 2);
        // A single ChannelWide provenance entry, with the updated span.
        let entries: Vec<_> = rv
            .provenance
            .iter()
            .filter(|l| l.kind == LayerKind::ChannelWide)
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].span, span(9));
    }

    // ── Shadowed value retention ─────────────────────────────────────────

    #[test]
    fn shadowed_layer_values_are_retained() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.layer_value(LayerKind::PipelineDefault), Some(&1));
        assert_eq!(rv.layer_value(LayerKind::ChannelPerTarget), Some(&2));
    }

    // ── Generic reuse over a different layer kind ────────────────────────

    #[test]
    fn generic_over_arbitrary_layer_kind() {
        // A stand-in for the future SchemaLayer proves the implementation is
        // reused with zero duplication for any `Copy + Ord` layer kind.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum SchemaLayer {
            Base,
            Pipeline,
            Group,
            Channel,
        }

        let mut rv = ResolvedValue::<&str, SchemaLayer>::new("base", SchemaLayer::Base, span(1));
        rv.apply_layer("pipeline", SchemaLayer::Pipeline, span(2));
        rv.apply_layer_fixed("group", SchemaLayer::Group, span(3));
        rv.apply_layer("channel", SchemaLayer::Channel, span(4));
        assert_eq!(rv.value, "group", "fixed group layer locks out channel");
        assert_eq!(rv.winning_layer().unwrap().kind, SchemaLayer::Group);
        assert_eq!(rv.provenance.iter().filter(|l| l.won).count(), 1);
    }

    #[test]
    fn provenance_layer_fits_in_cache_budget() {
        assert!(
            std::mem::size_of::<ProvenanceLayer>() <= 64,
            "ProvenanceLayer must fit in 64 bytes, got {}",
            std::mem::size_of::<ProvenanceLayer>()
        );
    }
}
