//! Per-window arena+index runtime, scoped over the top-level DAG and
//! every active composition body.
//!
//! A `WindowRuntime` is the bare minimum the windowed Transform arm
//! needs to look up: an `Arc<Arena>` carrying the projected source-side
//! columns and an `Arc<SecondaryIndex>` carrying the partition slices.
//! The arena and index are reference-counted so the buffer-recompute
//! path can keep them alive across the commit phase via `Arc::clone`
//! without forcing the dispatcher to rebuild either at retract time.
//!
//! `WindowRuntimeRegistry` is the executor-context handle. Slot `i` in
//! `top` corresponds to `plan.indices_to_build[i]`; body executors
//! push a fresh per-body vec onto `bodies` at recursion entry and pop
//! it at exit. `active_stack` records the currently-executing body
//! chain so `resolve` walks the body's overlay first and falls back to
//! `top` for `ParentNode` roots that escape the body's mini-DAG.

use std::collections::HashMap;
use std::sync::Arc;

use crate::pipeline::arena::Arena;
use crate::pipeline::index::SecondaryIndex;
use crate::plan::composition_body::CompositionBodyId;
use crate::plan::index::PlanIndexRoot;

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

/// Body-scoped registry of `WindowRuntime`s indexed by
/// `IndexSpec` slot.
///
/// The registry resolves a window's runtime from a `(PlanIndexRoot,
/// idx)` pair — one structure handles top-level DAG windows
/// (`Source` / `Node`) and composition-body windows
/// (`ParentNode` overlays + body-internal `Node`/`Source` slots).
pub(crate) struct WindowRuntimeRegistry {
    /// Top-level DAG window runtimes; sized to
    /// `plan.indices_to_build.len()` at executor start. Slot `i`
    /// corresponds to `plan.indices_to_build[i]`. Slots remain `None`
    /// until their owner operator (Source materialization, or an
    /// upstream operator's dispatch-arm finalize) populates them.
    pub(crate) top: Vec<Option<WindowRuntime>>,
    /// One body-scoped vector per active composition body, keyed by
    /// `CompositionBodyId`. Body executors insert a fresh vec on
    /// recursion entry, populate it as body operators finalize, and
    /// remove it on exit. Slot `i` corresponds to
    /// `bound_body.indices_to_build[i]` — body NodeIndex space, not
    /// parent-DAG space.
    pub(crate) bodies: HashMap<CompositionBodyId, Vec<Option<WindowRuntime>>>,
    /// Stack of currently-active body IDs, top-of-stack last. The
    /// active body's vec receives node-rooted writes; resolution
    /// walks this stack from top to bottom for `ParentNode` lookups
    /// before falling back to `top`.
    pub(crate) active_stack: Vec<CompositionBodyId>,
    /// Source name → top-level slot index, populated at executor
    /// start by walking `plan.indices_to_build` and extracting every
    /// `PlanIndexRoot::Source(name)` slot. `resolve` consults this
    /// map for source-rooted lookups so a multi-source pipeline can
    /// materialize each source's arena into its own slot independent
    /// of declaration order.
    pub(crate) source_name_to_top_idx: HashMap<String, Vec<usize>>,
}

impl WindowRuntimeRegistry {
    /// Create a registry sized to the top-level plan's index count.
    /// `top` slots start `None`; operators populate them at their
    /// dispatch-arm finalize. `source_name_to_top_idx` is populated
    /// from the spec list so source-rooted lookups never miss a slot
    /// because of multi-source declaration order.
    pub(crate) fn new(specs: &[crate::plan::index::IndexSpec]) -> Self {
        let mut source_name_to_top_idx: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, spec) in specs.iter().enumerate() {
            if let PlanIndexRoot::Source(name) = &spec.root {
                source_name_to_top_idx
                    .entry(name.clone())
                    .or_default()
                    .push(i);
            }
        }
        Self {
            top: (0..specs.len()).map(|_| None).collect(),
            bodies: HashMap::new(),
            active_stack: Vec::new(),
            source_name_to_top_idx,
        }
    }

    /// Resolve a runtime by `(root, idx)`. `idx` is the spec's
    /// position in `plan.indices_to_build` for top-level lookups, or
    /// the body's spec position for body-internal lookups.
    ///
    /// Resolution rules:
    /// - `Source(name)`: consult `source_name_to_top_idx` for the
    ///   slot list, return the first populated slot. Falls through
    ///   to direct `top[idx]` if the side map miss is silent (e.g.
    ///   stale `idx` from a body context).
    /// - `Node { .. }`: prefer the active body's vec[idx] if
    ///   present and populated; otherwise `top[idx]`.
    /// - `ParentNode { .. }`: walk `active_stack` for any body
    ///   whose vec[idx] is populated; ultimately fall back to
    ///   `top[idx]` (the parent-DAG runtime that the
    ///   ParentNode rooting points at).
    pub(crate) fn resolve(&self, root: &PlanIndexRoot, idx: usize) -> Option<&WindowRuntime> {
        match root {
            PlanIndexRoot::Source(name) => {
                if let Some(slot_idxs) = self.source_name_to_top_idx.get(name)
                    && let Some(&slot) = slot_idxs
                        .iter()
                        .find(|&&s| self.top.get(s).map(|o| o.is_some()).unwrap_or(false))
                {
                    return self.top.get(slot).and_then(|o| o.as_ref());
                }
                self.top.get(idx).and_then(|o| o.as_ref())
            }
            PlanIndexRoot::Node { .. } => {
                if let Some(&body_id) = self.active_stack.last()
                    && let Some(body_vec) = self.bodies.get(&body_id)
                    && let Some(slot) = body_vec.get(idx).and_then(|o| o.as_ref())
                {
                    return Some(slot);
                }
                self.top.get(idx).and_then(|o| o.as_ref())
            }
            PlanIndexRoot::ParentNode { .. } => {
                // Body's overlay first — body-executor entry is what
                // installed the runtime via `Arc::clone` from the
                // parent's `top` slot, so a populated body vec[idx]
                // is the correct hit.
                for &body_id in self.active_stack.iter().rev() {
                    if let Some(body_vec) = self.bodies.get(&body_id)
                        && let Some(slot) = body_vec.get(idx).and_then(|o| o.as_ref())
                    {
                        return Some(slot);
                    }
                }
                // Fallback: the parent's runtime list. Reached when a
                // body is freshly entered and its overlay has not yet
                // been populated — `idx` is then a body-spec index,
                // not a top-level index, so this fallback is only
                // correct when the body's spec table mirrors the
                // parent's at this slot.
                self.top.get(idx).and_then(|o| o.as_ref())
            }
        }
    }

    /// Write a runtime to `slot_idx` of the active scope. The active
    /// body's vec wins when `active_stack` is non-empty; otherwise
    /// the top-level vec is the target.
    ///
    /// Returns `true` if the write landed; `false` if the slot was
    /// out of bounds (a planner-pass invariant violation — the spec
    /// list size and the registry's vec sizing are kept in lockstep
    /// at construction time).
    pub(crate) fn install(&mut self, slot_idx: usize, runtime: WindowRuntime) -> bool {
        if let Some(&body_id) = self.active_stack.last()
            && let Some(body_vec) = self.bodies.get_mut(&body_id)
        {
            if let Some(slot) = body_vec.get_mut(slot_idx) {
                *slot = Some(runtime);
                return true;
            }
            return false;
        }
        if let Some(slot) = self.top.get_mut(slot_idx) {
            *slot = Some(runtime);
            return true;
        }
        false
    }
}
