//! The contract a compiled plan exposes to the memory arbitrator so it can
//! pick which runnable node to dispatch by predicted memory impact.

use petgraph::graph::NodeIndex;

/// Per-node memory predictions plus a stable ordering key, supplied to the
/// memory arbitrator so it can choose which of several currently-runnable
/// nodes to dispatch by predicted memory impact.
///
/// The plan is the natural implementer: the byte predictions are the
/// plan-time volume estimates carried on each node's `NodeProperties`,
/// and the stable index is the node's position in the plan's canonical
/// topological order. The arbitrator treats this as a read-only view —
/// it never mutates the plan and calls each method at most once per
/// candidate per invocation.
///
/// All three methods key off [`NodeIndex`]. The caller guarantees the
/// hint knows every index in the `runnable` slice it passes; an unknown
/// index is the caller's contract violation, and an implementer should
/// answer `0` / a stable fallback rather than panic.
pub trait SchedulingHint: Send + Sync {
    /// Predicted peak live bytes for `id` — the volume the node holds at
    /// once at its peak, in the same units as the arbitrator's soft limit.
    /// `0` means "unknown" (no file-size seed reached the node); an
    /// unknown node participates normally in the tiebreaks but never
    /// trips the headroom filter, which is what keeps behavior unchanged
    /// when no statistics are available.
    fn predicted_peak_bytes(&self, id: NodeIndex) -> u64;

    /// Predicted bytes `id` releases back to the budget when it finishes
    /// draining (a blocking operator drops its accumulated state on its
    /// last emit; a streaming node frees nothing). Used as the higher-
    /// priority freed tiebreak: a node that frees memory the instant it
    /// completes (a ready blocking operator) reclaims headroom *now*, so
    /// it is preferred over one that merely unlocks an eventual reclaim.
    fn predicted_freed_bytes_on_complete(&self, id: NodeIndex) -> u64;

    /// Predicted largest reclaimable footprint anywhere in the subtree
    /// rooted at `id` — the headroom that running this node's chain to
    /// completion eventually unlocks (for a fresh Source, its downstream
    /// blocking operator's accumulated state). The lower-priority freed
    /// tiebreak: it distinguishes candidates that free nothing *now* but
    /// whose chains differ in eventual reclaim, so the scheduler launches
    /// the independent chain whose completion frees the most first. `0`
    /// when nothing downstream accumulates, so it never disturbs the
    /// "no-estimates == today's order" floor.
    fn predicted_subtree_reclaim_bytes(&self, id: NodeIndex) -> u64;

    /// Position of `id` in the plan's canonical topological order — the
    /// deterministic final tiebreak. This must be the SAME order the
    /// executor walks today (front-to-back over `topo_order`), so that
    /// when every candidate's `predicted_peak_bytes` is `0` the chosen
    /// node is exactly the one a plain topo walk would dispatch first.
    /// That equivalence is the load-bearing "no-estimates == today's
    /// order" invariant the downstream dispatch integration relies on.
    fn stable_index(&self, id: NodeIndex) -> usize;
}
