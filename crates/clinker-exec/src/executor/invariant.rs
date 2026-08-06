//! Typed construction helpers for runtime dispatcher invariants.

use clinker_plan::error::PipelineError;

/// Construct a dispatcher-kind mismatch from fixed tags and one logical node
/// identity. The retained node name is bounded before it reaches the error
/// payload so machine and text edges cannot scale with authored input size.
pub(crate) fn dispatch_mismatch(
    dispatcher: &'static str,
    expected_kind: &'static str,
    actual_kind: &'static str,
    node: &str,
) -> PipelineError {
    PipelineError::DispatchMismatch {
        dispatcher,
        expected_kind,
        actual_kind,
        node: PipelineError::bounded_dispatch_node_name(node),
    }
}
