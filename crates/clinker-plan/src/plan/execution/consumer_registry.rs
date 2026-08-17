//! Compiled producer-port consumer registry.
//!
//! The finalized plan graph is lowered once into stable producer and consumer
//! identities. Runtime fan-out uses this registry instead of independently
//! recounting Output nodes and Merge/Combine predecessor readers.

use std::collections::{BTreeMap, BTreeSet};

use petgraph::Direction;
use petgraph::graph::DiGraph;
use petgraph::visit::EdgeRef;
use serde::Serialize;

use super::{DependencyType, PlanEdge, PlanNode};
use crate::plan::PlanNodeId;

/// Stable identity of one producer output port.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ProducerPortKey {
    pub producer: PlanNodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_port: Option<String>,
}

impl ProducerPortKey {
    pub fn new(producer: PlanNodeId, producer_port: Option<&str>) -> Self {
        Self {
            producer,
            producer_port: producer_port.map(str::to_owned),
        }
    }
}

/// How a consumer receives the producer port at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerSlotBehavior {
    /// The consumer drains or re-reads the producer's shared port slot.
    ReadsProducerPort,
    /// The producer pre-forks this edge into a slot owned by the consumer.
    ReadsOwnSlot,
}

/// Whether the consumer crosses the physical writer boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompiledConsumerKind {
    Computational,
    PhysicalWriterBoundary,
}

/// Stable identity and runtime delivery classification for one graph consumer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct CompiledConsumer {
    pub node: PlanNodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_port: Option<String>,
    pub slot_behavior: ConsumerSlotBehavior,
    pub kind: CompiledConsumerKind,
}

/// Complete deterministic consumer list for every finalized producer port.
#[derive(Debug, Clone, Default)]
pub struct CompiledConsumerRegistry {
    by_port: BTreeMap<ProducerPortKey, Vec<CompiledConsumer>>,
}

impl CompiledConsumerRegistry {
    pub(crate) fn compile(graph: &DiGraph<PlanNode, PlanEdge>) -> Self {
        let mut unique: BTreeMap<ProducerPortKey, BTreeSet<CompiledConsumer>> = BTreeMap::new();

        for producer_idx in graph.node_indices() {
            let producer = &graph[producer_idx];
            for edge in graph.edges_directed(producer_idx, Direction::Outgoing) {
                if !matches!(edge.weight().dependency_type, DependencyType::Data) {
                    continue;
                }
                let target = &graph[edge.target()];
                let slot_behavior =
                    if matches!(producer, PlanNode::Route { .. } | PlanNode::Cull { .. })
                        && !matches!(target, PlanNode::Merge { .. } | PlanNode::Combine { .. })
                    {
                        ConsumerSlotBehavior::ReadsOwnSlot
                    } else {
                        ConsumerSlotBehavior::ReadsProducerPort
                    };
                let key =
                    ProducerPortKey::new(producer.id(), edge.weight().producer_port.as_deref());
                unique.entry(key).or_default().insert(CompiledConsumer {
                    node: target.id(),
                    input_port: edge.weight().port.clone(),
                    slot_behavior,
                    kind: if matches!(target, PlanNode::Sink { .. }) {
                        CompiledConsumerKind::PhysicalWriterBoundary
                    } else {
                        CompiledConsumerKind::Computational
                    },
                });
            }
        }

        Self {
            by_port: unique
                .into_iter()
                .map(|(key, consumers)| (key, consumers.into_iter().collect()))
                .collect(),
        }
    }

    pub fn consumers(&self, key: &ProducerPortKey) -> &[CompiledConsumer] {
        self.by_port.get(key).map_or(&[], Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ProducerPortKey, &[CompiledConsumer])> {
        self.by_port
            .iter()
            .map(|(key, consumers)| (key, consumers.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompileContext, parse_config};
    use crate::plan::EntityRef;
    use clinker_core_types::span::Span;

    fn compile(yaml: &str) -> crate::plan::CompiledPlan {
        parse_config(yaml)
            .expect("registry fixture parses")
            .compile(&CompileContext::default())
            .expect("registry fixture compiles")
    }

    fn node_id(dag: &super::super::ExecutionPlanDag, name: &str) -> PlanNodeId {
        dag.graph
            .node_indices()
            .find_map(|idx| (dag.graph[idx].name() == name).then(|| dag.graph[idx].id()))
            .unwrap_or_else(|| panic!("missing node {name}"))
    }

    #[test]
    fn consumer_registry_counts_output_and_sort_on_one_producer() {
        let mut graph = DiGraph::new();
        let source_id = PlanNodeId::new(0);
        let output_id = PlanNodeId::new(1);
        let sort_id = PlanNodeId::new(2);
        let source = graph.add_node(PlanNode::Source {
            name: "shared".to_string(),
            id: source_id,
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: clinker_record::SchemaBuilder::new().build(),
        });
        let output = graph.add_node(PlanNode::Sink {
            name: "direct".to_string(),
            id: output_id,
            span: Span::SYNTHETIC,
            resolved: None,
        });
        let sort = graph.add_node(PlanNode::Sort {
            name: "__sort_for_sorted".to_string(),
            id: sort_id,
            span: Span::SYNTHETIC,
            sort_fields: Vec::new(),
        });
        let edge = || PlanEdge {
            dependency_type: DependencyType::Data,
            port: None,
            producer_port: None,
        };
        graph.add_edge(source, output, edge());
        graph.add_edge(source, sort, edge());

        let registry = CompiledConsumerRegistry::compile(&graph);
        let consumers = registry.consumers(&ProducerPortKey::new(source_id, None));

        assert_eq!(consumers.len(), 2);
        assert!(consumers.iter().any(|consumer| consumer.node == output_id));
        assert!(consumers.iter().any(|consumer| consumer.node == sort_id));
        assert!(
            consumers
                .iter()
                .all(|consumer| consumer.slot_behavior == ConsumerSlotBehavior::ReadsProducerPort)
        );
    }

    #[test]
    fn consumer_registry_contains_output_and_merge_once_in_stable_order() {
        let plan = compile(
            r#"
pipeline: { name: registry_mixed }
nodes:
  - type: source
    name: shared
    config:
      name: shared
      type: csv
      path: shared.csv
      schema: [{ name: id, type: string }]
  - type: source
    name: other
    config:
      name: other
      type: csv
      path: other.csv
      schema: [{ name: id, type: string }]
  - type: output
    name: direct
    input: shared
    config: { name: direct, type: csv, path: direct.csv }
  - type: merge
    name: joined
    inputs: [shared, other]
  - type: output
    name: merged
    input: joined
    config: { name: merged, type: csv, path: merged.csv }
"#,
        );
        let dag = plan.dag();
        let key = ProducerPortKey::new(node_id(dag, "shared"), None);
        let consumers = dag.consumer_registry.consumers(&key);

        assert_eq!(consumers.len(), 2);
        assert_eq!(consumers[0].node, node_id(dag, "direct"));
        assert_eq!(
            consumers[0].kind,
            CompiledConsumerKind::PhysicalWriterBoundary
        );
        assert_eq!(consumers[1].node, node_id(dag, "joined"));
        assert_eq!(consumers[1].kind, CompiledConsumerKind::Computational);
        assert!(
            consumers
                .iter()
                .all(|consumer| consumer.slot_behavior == ConsumerSlotBehavior::ReadsProducerPort)
        );
    }

    #[test]
    fn consumer_registry_keeps_route_producer_ports_distinct() {
        let plan = compile(
            r#"
pipeline: { name: registry_ports }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
        - { name: side, type: string }
  - type: route
    name: split
    input: src
    config:
      mode: exclusive
      conditions: { left: "side == 'left'" }
      default: right
  - type: output
    name: left
    input: split.left
    config: { name: left, type: csv, path: left.csv }
  - type: output
    name: right
    input: split.right
    config: { name: right, type: csv, path: right.csv }
"#,
        );
        let dag = plan.dag();
        let split = node_id(dag, "split");
        let ports: Vec<_> = dag
            .consumer_registry
            .iter()
            .filter(|(key, _)| key.producer == split)
            .map(|(key, consumers)| (key.producer_port.as_deref(), consumers))
            .collect();

        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].0, Some("left"));
        assert_eq!(ports[1].0, Some("right"));
        assert!(ports.iter().all(|(_, consumers)| {
            consumers.len() == 1 && consumers[0].slot_behavior == ConsumerSlotBehavior::ReadsOwnSlot
        }));
    }
}
