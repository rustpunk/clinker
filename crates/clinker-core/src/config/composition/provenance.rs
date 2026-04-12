//! Provenance tracking for resolved configuration values (Phase 16c.3).
//!
//! Each config value in a compiled pipeline carries a [`ResolvedValue`] wrapper
//! that records which configuration layer (composition default, channel default,
//! channel fixed, inspector edit) contributed the winning value. The provenance
//! chain is stored in a side-table [`ProvenanceDb`], separate from
//! [`CompiledPlan`](crate::plan::compiled::CompiledPlan).

use crate::span::Span;

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

/// A single provenance record: one layer's contribution to a config value.
///
/// The `won` flag marks the layer whose value was selected. Exactly one layer
/// in a [`ResolvedValue`]'s provenance chain has `won == true`.
#[derive(Debug, Clone)]
pub struct ProvenanceLayer {
    /// Source span of the value's origin in the YAML file (LD-003).
    /// File identity resolved via `SourceDb.path(span.file)` at render time.
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
/// The provenance chain is bounded at 4 entries (one per [`LayerKind`])
/// by the LD-16c-8 no-layering decision.
#[derive(Debug, Clone)]
pub struct ResolvedValue<T> {
    /// The winning value after all layers have been applied.
    pub value: T,
    /// Ordered provenance chain. Each entry records a layer's contribution.
    /// At most 4 entries (one per [`LayerKind`]). Exactly one has `won == true`.
    pub provenance: Vec<ProvenanceLayer>,
}

impl<T> ResolvedValue<T> {
    /// Create a new resolved value with a single provenance layer.
    /// The initial layer is always the winner (V-7-3).
    pub fn new(value: T, kind: LayerKind, span: Span) -> Self {
        Self {
            value,
            provenance: vec![ProvenanceLayer {
                span,
                kind,
                won: true,
            }],
        }
    }

    /// Returns the layer that won (the one with `won == true`).
    pub fn winning_layer(&self) -> Option<&ProvenanceLayer> {
        self.provenance.iter().find(|l| l.won)
    }

    /// Apply a new layer on top. The new layer wins if its [`LayerKind`]
    /// is >= the current winner's kind (higher or equal priority wins).
    ///
    /// Same-kind layers replace in place (V-7-4): the span is updated and
    /// the value is replaced if the new layer wins.
    pub fn apply_layer(&mut self, value: T, kind: LayerKind, span: Span) {
        debug_assert!(
            self.provenance.len() <= 4,
            "provenance chain exceeds 4-layer bound (LD-16c-8)"
        );

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
}
