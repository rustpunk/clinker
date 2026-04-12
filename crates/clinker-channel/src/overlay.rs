//! Channel overlay merge (Phase 16c.4 Task 16c.4.3).
//!
//! Applies a [`ChannelBinding`] to a [`CompiledPlan`], overlaying config
//! values as `ChannelDefault` or `ChannelFixed` provenance layers on the
//! plan's [`ProvenanceDb`]. Stamps a [`ChannelIdentity`] for cache-keying.

use clinker_core::config::composition::LayerKind;
use clinker_core::error::{Diagnostic, LabeledSpan};
use clinker_core::plan::{ChannelIdentity, CompiledPlan};
use clinker_core::span::Span;

use crate::binding::{ChannelBinding, DottedPath};

/// Apply a channel binding's config overlays to a compiled plan.
///
/// For each key in `config_default`, applies a `ChannelDefault` provenance
/// layer. For each key in `config_fixed`, applies a `ChannelFixed` layer.
/// Returns diagnostics for bindings that reference nodes/params not present
/// in the plan's provenance DB.
///
/// Stamps `ChannelIdentity` on the plan after overlay.
pub fn apply_channel_overlay(plan: &mut CompiledPlan, binding: &ChannelBinding) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    apply_config_layer(
        plan,
        &binding.config_default,
        LayerKind::ChannelDefault,
        &binding.name,
        &mut diagnostics,
    );
    apply_config_layer(
        plan,
        &binding.config_fixed,
        LayerKind::ChannelFixed,
        &binding.name,
        &mut diagnostics,
    );

    plan.set_channel_identity(ChannelIdentity {
        name: binding.name.clone(),
        content_hash: binding.channel_hash,
    });

    diagnostics
}

fn apply_config_layer(
    plan: &mut CompiledPlan,
    config: &indexmap::IndexMap<DottedPath, serde_json::Value>,
    kind: LayerKind,
    channel_name: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let provenance = plan.provenance_mut();

    for (dotted_path, value) in config {
        let (node_name, param_name) = match dotted_path.segments() {
            (Some(alias), param) => (alias, param),
            (None, param) => ("", param),
        };

        match provenance.get_mut(node_name, param_name) {
            Some(resolved) => {
                resolved.apply_layer(value.clone(), kind, Span::SYNTHETIC);
            }
            None => {
                diagnostics.push(Diagnostic::warning(
                    "W103",
                    format!(
                        "channel {:?}: config key {:?} does not match any \
                         composition parameter in the compiled plan",
                        channel_name,
                        dotted_path.as_str(),
                    ),
                    LabeledSpan::primary(Span::SYNTHETIC, String::new()),
                ));
            }
        }
    }
}
