//! Per-window arena+index runtime, scoped over the top-level DAG and
//! exact composition-body execution scope.
//!
//! A `WindowRuntime` is the bare minimum the windowed Transform arm
//! needs to look up: an `Arc<Arena>` carrying the projected source-side
//! columns and an `Arc<SecondaryIndex>` carrying the partition slices.
//! The arena and index are reference-counted so the buffer-recompute
//! path can keep them alive across the commit phase via `Arc::clone`
//! without forcing the dispatcher to rebuild either at retract time.
//!
//! `WindowRuntimeRegistry` is the executor-context handle. Slot `i` in
//! `top` corresponds to `plan.indices_to_build[i]`; body runtimes are
//! keyed by the full `(body_scope, window, input_root)` identity. No
//! numeric slot fallback crosses a composition boundary.

use std::collections::HashMap;
use std::sync::Arc;

use crate::pipeline::arena::Arena;
use crate::pipeline::index::SecondaryIndex;
use clinker_plan::plan::composition_body::{BodyScopeId, CompositionBodyId, WindowRuntimeKey};

/// Resolved arena + secondary index for one windowed Transform.
///
/// Both fields are `Arc`-shared across the dispatcher's per-record
/// evaluation, the buffer-recompute path's retain pool, and the
/// orchestrator's recompute-window phase. The arena is the canonical
/// retraction buffer for buffer-recompute mode — there is no separate
/// shadow buffer.
pub(crate) struct WindowRuntime {
    pub(crate) arena: Arc<Arena>,
    pub(crate) index: Arc<SecondaryIndex>,
}

impl Clone for WindowRuntime {
    fn clone(&self) -> Self {
        Self {
            arena: Arc::clone(&self.arena),
            index: Arc::clone(&self.index),
        }
    }
}

/// Registry of top-level slot runtimes and exactly keyed body runtimes.
pub(crate) struct WindowRuntimeRegistry {
    /// Top-level DAG window runtimes; sized to
    /// `plan.indices_to_build.len()` at executor start. Slot `i`
    /// corresponds to `plan.indices_to_build[i]`. Slots remain `None`
    /// until their owner operator (Source materialization, or an
    /// upstream operator's dispatch-arm finalize) populates them.
    pub(crate) top: Vec<Option<WindowRuntime>>,
    /// Body runtimes keyed by stable scope and node identities.
    pub(crate) bodies: HashMap<WindowRuntimeKey, WindowRuntime>,
    /// Stack of currently-active body IDs, top-of-stack last. Other
    /// executor subsystems use this for body-local buffer namespaces.
    pub(crate) active_stack: Vec<CompositionBodyId>,
}

impl WindowRuntimeRegistry {
    /// Create a registry sized to the top-level plan's index count.
    /// `top` slots start `None`; operators populate them at their
    /// dispatch-arm finalize, including Source arms (which anchor
    /// their windows at the Source's own `NodeIndex`).
    pub(crate) fn new(specs: &[clinker_plan::plan::index::IndexSpec]) -> Self {
        Self {
            top: (0..specs.len()).map(|_| None).collect(),
            bodies: HashMap::new(),
            active_stack: Vec::new(),
        }
    }

    pub(crate) fn resolve_top(&self, idx: usize) -> Option<&WindowRuntime> {
        self.top.get(idx).and_then(|slot| slot.as_ref())
    }

    pub(crate) fn resolve_body(&self, key: &WindowRuntimeKey) -> Option<&WindowRuntime> {
        self.bodies.get(key)
    }

    pub(crate) fn install_top(&mut self, slot_idx: usize, runtime: WindowRuntime) -> bool {
        if let Some(slot) = self.top.get_mut(slot_idx) {
            *slot = Some(runtime);
            return true;
        }
        false
    }

    pub(crate) fn install_body(&mut self, key: WindowRuntimeKey, runtime: WindowRuntime) {
        self.bodies.insert(key, runtime);
    }

    pub(crate) fn remove_body_scope(&mut self, scope: BodyScopeId) {
        self.bodies.retain(|key, _| key.body_scope != scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::plan::index::{IndexSpec, PlanIndexRoot};
    use clinker_plan::plan::{BodyScopeId, EntityRef, PlanNodeId, WindowRuntimeKey};
    use clinker_record::Schema;
    use petgraph::graph::NodeIndex;

    fn empty_runtime() -> WindowRuntime {
        let arena = Arc::new(Arena::empty(Arc::new(Schema::new(Vec::new()))));
        let index = Arc::new(SecondaryIndex {
            groups: HashMap::new(),
        });
        WindowRuntime { arena, index }
    }

    #[test]
    fn body_lookup_does_not_fall_back_to_same_numbered_top_slot() {
        let schema = Arc::new(Schema::new(Vec::new()));
        let spec = IndexSpec {
            root: PlanIndexRoot::Node {
                upstream: NodeIndex::new(0),
                anchor_schema: schema,
            },
            group_by: Vec::new(),
            sort_by: Vec::new(),
            arena_fields: Vec::new(),
            already_sorted: false,
            requires_buffer_recompute: false,
        };
        let mut registry = WindowRuntimeRegistry::new(&[spec]);
        registry.top[0] = Some(empty_runtime());
        registry.active_stack.push(CompositionBodyId(7));
        let key = WindowRuntimeKey {
            body_scope: BodyScopeId(7),
            window: PlanNodeId::new(12),
            input_root: PlanNodeId::new(4),
        };

        assert!(
            registry.resolve_body(&key).is_none(),
            "a missing body runtime must not alias a populated top-level slot"
        );
        assert!(registry.resolve_top(0).is_some());
    }

    #[test]
    fn removing_one_body_scope_preserves_sibling_runtime() {
        let mut registry = WindowRuntimeRegistry::new(&[]);
        let left = WindowRuntimeKey {
            body_scope: BodyScopeId(7),
            window: PlanNodeId::new(12),
            input_root: PlanNodeId::new(4),
        };
        let right = WindowRuntimeKey {
            body_scope: BodyScopeId(8),
            ..left
        };
        registry.install_body(left, empty_runtime());
        registry.install_body(right, empty_runtime());

        registry.remove_body_scope(left.body_scope);

        assert!(registry.resolve_body(&left).is_none());
        assert!(registry.resolve_body(&right).is_some());
    }
}
