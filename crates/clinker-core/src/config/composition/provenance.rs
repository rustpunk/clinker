//! Provenance tracking for resolved configuration values.
//!
//! Each config value in a compiled pipeline carries a [`ResolvedValue`] wrapper
//! that records which configuration layer (composition default, channel default,
//! channel fixed, inspector edit) contributed the winning value. The provenance
//! chain is stored in a side-table [`ProvenanceDb`], separate from
//! [`CompiledPlan`](crate::plan::compiled::CompiledPlan).

use std::collections::HashMap;
use std::fmt;

use crate::span::Span;

// Keep ProvenanceLayer at or under 64 bytes — the provenance side-table
// is sized for this footprint.
const _: () = assert!(std::mem::size_of::<ProvenanceLayer>() <= 64);

/// Which configuration layer contributed a value.
///
/// Priority is encoded in discriminant order via `PartialOrd`/`Ord` derives:
/// `CompositionDefault (0) < ChannelDefault (1) < ChannelFixed (2) < InspectorEdit (3)`.
/// Higher-priority layers win over lower-priority ones during [`ResolvedValue::apply_layer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LayerKind {
    CompositionDefault = 0,
    ChannelDefault = 1,
    ChannelFixed = 2,
    InspectorEdit = 3,
}

impl fmt::Display for LayerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LayerKind::CompositionDefault => write!(f, "CompositionDefault"),
            LayerKind::ChannelDefault => write!(f, "ChannelDefault"),
            LayerKind::ChannelFixed => write!(f, "ChannelFixed"),
            LayerKind::InspectorEdit => write!(f, "InspectorEdit"),
        }
    }
}

/// A single provenance record: one layer's contribution to a config value.
///
/// The `won` flag marks the layer whose value was selected. Exactly one layer
/// in a [`ResolvedValue`]'s provenance chain has `won == true`.
#[derive(Debug, Clone)]
pub struct ProvenanceLayer {
    /// Source span of the value's origin in the YAML file. File identity
    /// is resolved via `SourceDb.path(span.file)` at render time.
    pub span: Span,
    /// Which configuration layer this value came from.
    pub kind: LayerKind,
    /// Whether this layer's value was selected as the winner.
    pub won: bool,
}

/// A config value together with its full provenance chain.
///
/// Inspired by figment's `Metadata` model. The `won` flag is Clinker's
/// addition — it makes the winning layer explicit for the Kiln inspector
/// panel without requiring the inspector to re-run priority logic.
///
/// The provenance chain is bounded at 4 entries (one per [`LayerKind`]).
#[derive(Debug, Clone)]
pub struct ResolvedValue<T> {
    /// The winning value after all layers have been applied.
    pub value: T,
    /// Ordered provenance chain. Each entry records a layer's contribution.
    /// At most 4 entries (one per [`LayerKind`]). Exactly one has `won == true`.
    pub provenance: Vec<ProvenanceLayer>,
    /// Per-layer values keyed by [`LayerKind`]. Stores the value contributed
    /// by each layer so `--explain --field` can display shadowed values.
    /// Kept separate from [`ProvenanceLayer`] to preserve its 64-byte size.
    pub layer_values: HashMap<LayerKind, T>,
}

impl<T: Clone> ResolvedValue<T> {
    /// Create a new resolved value with a single provenance layer.
    /// The initial layer is always the winner.
    pub fn new(value: T, kind: LayerKind, span: Span) -> Self {
        let mut layer_values = HashMap::new();
        layer_values.insert(kind, value.clone());
        Self {
            value,
            provenance: vec![ProvenanceLayer {
                span,
                kind,
                won: true,
            }],
            layer_values,
        }
    }

    /// Returns the layer that won (the one with `won == true`).
    pub fn winning_layer(&self) -> Option<&ProvenanceLayer> {
        self.provenance.iter().find(|l| l.won)
    }

    /// Apply a new layer on top. The new layer wins if its [`LayerKind`]
    /// is >= the current winner's kind (higher or equal priority wins).
    ///
    /// Same-kind layers replace in place: the span is updated and
    /// the value is replaced if the new layer wins.
    pub fn apply_layer(&mut self, value: T, kind: LayerKind, span: Span) {
        debug_assert!(
            self.provenance.len() <= 4,
            "provenance chain exceeds 4-layer bound"
        );

        // Store this layer's value before any winner computation.
        self.layer_values.insert(kind, value.clone());

        // Find existing entry for this kind (same-kind replace-in-place).
        let existing_idx = self.provenance.iter().position(|l| l.kind == kind);

        if let Some(idx) = existing_idx {
            // Replace in place: update span, recalculate winner.
            self.provenance[idx].span = span;
        } else {
            // New kind: push a new entry.
            self.provenance.push(ProvenanceLayer {
                span,
                kind,
                won: false,
            });
        }

        // Recompute winner: highest LayerKind wins.
        let winning_kind = self
            .provenance
            .iter()
            .map(|l| l.kind)
            .max()
            .expect("provenance is non-empty");

        let new_layer_wins = kind >= winning_kind;

        for layer in &mut self.provenance {
            layer.won = layer.kind == winning_kind;
        }

        // Update value if the new/replaced layer is the winner.
        if new_layer_wins {
            self.value = value;
        }
    }

    /// Look up the value contributed by a specific layer kind.
    pub fn layer_value(&self, kind: LayerKind) -> Option<&T> {
        self.layer_values.get(&kind)
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

    /// Mutable lookup for `(node_name, param_name)`. Used by channel overlay
    /// to apply `ChannelDefault`/`ChannelFixed` layers.
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
