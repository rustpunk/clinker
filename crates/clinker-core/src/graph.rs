//! Pure graph utilities for pipeline DAG validation.
//!
//! Cycle detection uses [`petgraph::algo::tarjan_scc`] (battle-tested, O(V+E),
//! correctly handles diamonds). Topological sort uses Kahn's algorithm via
//! [`petgraph::algo::toposort`].
//!
//! These utilities operate on a name-keyed adjacency representation built
//! from `PipelineNode` headers — they do **not** know about
//! `PlanNode`/`ExecutionPlanDag`. The lowering pipeline calls these to fail
//! fast on duplicates / self-loops / cycles before any CXL parsing happens.

use std::collections::BTreeMap;

use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};

/// A name-keyed adjacency view of a pipeline DAG.
///
/// Each node is identified by its (case-sensitive) string name. Edges run
/// from producer to consumer.
#[derive(Debug, Default, Clone)]
pub struct NameGraph {
    inner: DiGraph<String, ()>,
    by_name: BTreeMap<String, NodeIndex>,
}

impl NameGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a node by name. If the name is already present the existing
    /// `NodeIndex` is returned; the caller is responsible for the duplicate-
    /// name detection pass that runs *before* this graph is built.
    pub fn add_node(&mut self, name: &str) -> NodeIndex {
        if let Some(&idx) = self.by_name.get(name) {
            return idx;
        }
        let idx = self.inner.add_node(name.to_string());
        self.by_name.insert(name.to_string(), idx);
        idx
    }

    /// Add a directed edge `producer -> consumer`. Both names must already
    /// have been inserted via [`add_node`].
    pub fn add_edge(&mut self, producer: &str, consumer: &str) {
        let p = self.by_name[producer];
        let c = self.by_name[consumer];
        self.inner.add_edge(p, c, ());
    }

    /// Look up a node index by name.
    pub fn index_of(&self, name: &str) -> Option<NodeIndex> {
        self.by_name.get(name).copied()
    }

    /// Borrow the underlying petgraph for advanced consumers.
    pub fn graph(&self) -> &DiGraph<String, ()> {
        &self.inner
    }

    /// Detect cycles via [`tarjan_scc`]. Returns the *first* SCC of size > 1
    /// (or any node listed in a self-loop SCC) as a `Vec<String>` of node
    /// names in the order Tarjan returned them.
    ///
    /// Self-loops show up as single-node SCCs that have a self-edge — those
    /// are filtered out here because the dedicated self-loop pass (E002)
    /// reports them with a more actionable message.
    pub fn detect_cycle(&self) -> Option<Vec<String>> {
        for scc in tarjan_scc(&self.inner) {
            if scc.len() > 1 {
                return Some(scc.into_iter().map(|idx| self.inner[idx].clone()).collect());
            }
        }
        None
    }

    /// Topological sort. Returns names in topological order, or `Err(cycle)`
    /// if the graph is cyclic. The error path is the same one
    /// [`detect_cycle`] would return.
    pub fn topo_sort(&self) -> Result<Vec<String>, Vec<String>> {
        match petgraph::algo::toposort(&self.inner, None) {
            Ok(order) => Ok(order
                .into_iter()
                .map(|idx| self.inner[idx].clone())
                .collect()),
            Err(_) => Err(self.detect_cycle().unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> NameGraph {
        let mut g = NameGraph::new();
        for n in nodes {
            g.add_node(n);
        }
        for (a, b) in edges {
            g.add_edge(a, b);
        }
        g
    }

    #[test]
    fn test_graph_detect_cycle_linear_ok() {
        // a -> b -> c
        let g = build(&["a", "b", "c"], &[("a", "b"), ("b", "c")]);
        assert!(g.detect_cycle().is_none());
        let order = g.topo_sort().expect("linear sorts");
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_graph_detect_cycle_diamond_ok() {
        // a -> b, a -> c, b -> d, c -> d
        let g = build(
            &["a", "b", "c", "d"],
            &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
        );
        assert!(g.detect_cycle().is_none());
        let order = g.topo_sort().expect("diamond sorts");
        // a comes first, d comes last; b/c order is implementation-defined
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn test_graph_detect_cycle_simple_cycle() {
        // a -> b -> a
        let g = build(&["a", "b"], &[("a", "b"), ("b", "a")]);
        let cycle = g.detect_cycle().expect("expected cycle");
        assert_eq!(cycle.len(), 2);
        assert!(cycle.contains(&"a".to_string()));
        assert!(cycle.contains(&"b".to_string()));
        assert!(g.topo_sort().is_err());
    }

    #[test]
    fn test_graph_detect_cycle_long_cycle() {
        // a -> b -> c -> d -> b
        let g = build(
            &["a", "b", "c", "d"],
            &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "b")],
        );
        let cycle = g.detect_cycle().expect("expected cycle");
        // The cycle SCC must contain b, c, d (a is acyclic feeder).
        assert!(cycle.contains(&"b".to_string()));
        assert!(cycle.contains(&"c".to_string()));
        assert!(cycle.contains(&"d".to_string()));
        assert!(!cycle.contains(&"a".to_string()));
    }
}
