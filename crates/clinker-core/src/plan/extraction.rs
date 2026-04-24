//! Extract-as-composition boundary analysis and YAML serialization.
//!
//! Provides [`analyze_extraction_boundary`] to identify crossing edges when
//! extracting a subgraph into a composition, and [`write_extracted_composition`]
//! to serialize the result as a `.comp.yaml` file.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::visit::EdgeRef;
use serde::Serialize;

use crate::config::PipelineConfig;
use crate::plan::execution::{DependencyType, ExecutionPlanDag, PlanNode};

/// A port detected at the extraction boundary.
#[derive(Debug, Clone)]
pub struct ExtractedPort {
    /// Auto-derived port name (from the external node's name).
    pub name: String,
    /// Name of the node on the external side of the boundary.
    pub external_node: String,
    /// Name of the node on the internal side of the boundary.
    pub internal_node: String,
    /// Type of the crossing edge.
    pub dependency_type: DependencyType,
}

/// A literal config value candidate for parameterization.
#[derive(Debug, Clone)]
pub struct ConfigCandidate {
    /// Name of the composition node that owns this config value.
    pub node_name: String,
    /// Name of the config parameter.
    pub param_name: String,
    /// The resolved winning value.
    pub value: serde_json::Value,
}

/// Result of extraction boundary analysis.
#[derive(Debug)]
pub struct ExtractionBoundary {
    /// Edges crossing INTO the selection — data flowing in from external nodes.
    pub input_ports: IndexMap<String, ExtractedPort>,
    /// Edges crossing OUT of the selection — data flowing to external nodes.
    pub output_ports: IndexMap<String, ExtractedPort>,
    /// Names of all internal (selected) nodes.
    pub internal_nodes: Vec<String>,
    /// Literal config candidates from composition nodes in the selection.
    pub config_candidates: Vec<ConfigCandidate>,
    /// Warnings accumulated during analysis (e.g. zero output ports).
    pub warnings: Vec<String>,
}

/// Error from extraction boundary analysis.
#[derive(Debug)]
pub struct ExtractionError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ExtractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ExtractionError {}

/// Analyze the extraction boundary for a set of selected nodes.
///
/// Given a DAG and a set of selected node names, identifies:
/// - Input ports: edges from external nodes into the selection
/// - Output ports: edges from the selection to external nodes
/// - Config candidates: provenance-tracked config values on composition nodes
///
/// Returns `Err` if any selected name doesn't exist in the DAG (E110).
pub fn analyze_extraction_boundary(
    dag: &ExecutionPlanDag,
    selected_names: &HashSet<String>,
    provenance: &crate::config::composition::ProvenanceDb,
) -> Result<ExtractionBoundary, Vec<ExtractionError>> {
    let mut errors = Vec::new();

    // Build name → NodeIndex map.
    let name_to_idx: HashMap<&str, petgraph::graph::NodeIndex> = dag
        .topo_order
        .iter()
        .map(|&idx| (dag.graph[idx].name(), idx))
        .collect();

    // Validate all selected names exist.
    for name in selected_names {
        if !name_to_idx.contains_key(name.as_str()) {
            errors.push(ExtractionError {
                code: "E110".to_owned(),
                message: format!("selected node '{name}' not found in the execution plan DAG"),
            });
        }
    }

    // Scope boundary check: if a selected node is a Composition, the
    // entire composition is selected as an opaque unit. Selecting nodes
    // from different scope levels (e.g. half top-level + half body) is
    // refused. Since body nodes live in CompileArtifacts (not the top-level
    // DAG), we only need to verify all selected names exist in the top-level
    // graph — which we already did above.

    if !errors.is_empty() {
        return Err(errors);
    }

    let selected_indices: HashSet<petgraph::graph::NodeIndex> = selected_names
        .iter()
        .filter_map(|n| name_to_idx.get(n.as_str()).copied())
        .collect();

    let mut input_ports = IndexMap::new();
    let mut output_ports = IndexMap::new();
    let mut warnings = Vec::new();

    for &idx in &selected_indices {
        let internal_name = dag.graph[idx].name().to_owned();

        // Walk incoming edges: source outside → input port.
        for edge in dag.graph.edges_directed(idx, Direction::Incoming) {
            let source_idx = edge.source();
            if !selected_indices.contains(&source_idx) {
                let external_name = dag.graph[source_idx].name().to_owned();
                let port_name = format!("in_{external_name}");
                input_ports
                    .entry(port_name.clone())
                    .or_insert(ExtractedPort {
                        name: port_name,
                        external_node: external_name,
                        internal_node: internal_name.clone(),
                        dependency_type: edge.weight().dependency_type,
                    });
            }
        }

        // Walk outgoing edges: target outside → output port.
        for edge in dag.graph.edges_directed(idx, Direction::Outgoing) {
            let target_idx = edge.target();
            if !selected_indices.contains(&target_idx) {
                let external_name = dag.graph[target_idx].name().to_owned();
                // For Route nodes, include the branch target in the port name
                // to produce distinct named output ports per branch.
                let port_name = if matches!(dag.graph[idx], PlanNode::Route { .. }) {
                    format!("out_{internal_name}_to_{external_name}")
                } else {
                    format!("out_{internal_name}")
                };
                output_ports
                    .entry(port_name.clone())
                    .or_insert(ExtractedPort {
                        name: port_name,
                        external_node: external_name,
                        internal_node: internal_name.clone(),
                        dependency_type: edge.weight().dependency_type,
                    });
            }
        }
    }

    if output_ports.is_empty() {
        warnings.push(
            "selected nodes have no downstream consumers — extracted composition is a sink"
                .to_owned(),
        );
    }

    // Config candidates: scan provenance DB for entries matching selected nodes.
    let mut config_candidates = Vec::new();
    for (key, resolved) in provenance.iter() {
        let (node_name, param_name) = key;
        if selected_names.contains(node_name) {
            config_candidates.push(ConfigCandidate {
                node_name: node_name.clone(),
                param_name: param_name.clone(),
                value: resolved.value.clone(),
            });
        }
    }

    Ok(ExtractionBoundary {
        input_ports,
        output_ports,
        internal_nodes: selected_names.iter().cloned().collect(),
        config_candidates,
        warnings,
    })
}

// =========================================================================
// Composition writer
// =========================================================================

/// Error from writing an extracted composition.
#[derive(Debug)]
pub enum ExtractionWriteError {
    /// Output path already exists and force=false.
    OutputExists(String),
    /// No nodes matched the internal_nodes list in the pipeline config.
    NoMatchingNodes(String),
    /// YAML serialization failed.
    Serialization(String),
    /// IO error writing the file.
    Io(std::io::Error),
}

impl std::fmt::Display for ExtractionWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtractionWriteError::OutputExists(p) => {
                write!(
                    f,
                    "output path '{p}' already exists (use force=true to overwrite)"
                )
            }
            ExtractionWriteError::NoMatchingNodes(msg) => write!(f, "{msg}"),
            ExtractionWriteError::Serialization(msg) => write!(f, "YAML serialization: {msg}"),
            ExtractionWriteError::Io(e) => write!(f, "IO error: {e}"),
        }
    }
}

impl std::error::Error for ExtractionWriteError {}

/// Serializable port declaration for the `_compose.inputs` block.
#[derive(Debug, Serialize)]
struct SerializablePortDecl {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Serializable output alias for the `_compose.outputs` block.
#[derive(Debug, Serialize)]
struct SerializableOutputAlias {
    /// Internal node reference (e.g. "transform_name.out").
    #[serde(rename = "ref")]
    internal_ref: String,
}

/// Serializable config param declaration for `_compose.config_schema`.
#[derive(Debug, Serialize)]
struct SerializableParamDecl {
    #[serde(rename = "type")]
    param_type: String,
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<serde_json::Value>,
}

/// Top-level serializable composition file wrapper.
#[derive(Debug, Serialize)]
struct SerializableCompositionFile {
    #[serde(rename = "_compose")]
    compose: SerializableSignature,
    nodes: Vec<serde_json::Value>,
}

/// Serializable signature block.
#[derive(Debug, Serialize)]
struct SerializableSignature {
    name: String,
    inputs: IndexMap<String, SerializablePortDecl>,
    outputs: IndexMap<String, SerializableOutputAlias>,
    config_schema: IndexMap<String, SerializableParamDecl>,
}

/// Write an extracted composition to a `.comp.yaml` file.
///
/// Builds a composition file from the boundary analysis result and the
/// original pipeline config. Returns the call-site YAML snippet string
/// to replace the selected nodes in the parent pipeline.
pub fn write_extracted_composition(
    boundary: &ExtractionBoundary,
    config: &PipelineConfig,
    name: &str,
    output_path: &Path,
    force: bool,
) -> Result<String, ExtractionWriteError> {
    // Check output path existence.
    if output_path.exists() && !force {
        return Err(ExtractionWriteError::OutputExists(
            output_path.display().to_string(),
        ));
    }

    // Extract matching PipelineNode entries from the config.
    let internal_set: HashSet<&str> = boundary.internal_nodes.iter().map(|s| s.as_str()).collect();
    let selected_nodes: Vec<serde_json::Value> = config
        .nodes
        .iter()
        .filter(|spanned| internal_set.contains(spanned.value.name()))
        .filter_map(|spanned| serde_json::to_value(&spanned.value).ok())
        .collect();

    if selected_nodes.is_empty() {
        return Err(ExtractionWriteError::NoMatchingNodes(format!(
            "no pipeline nodes matched the internal node names: {:?}",
            boundary.internal_nodes
        )));
    }

    // Build signature from boundary analysis.
    let mut inputs = IndexMap::new();
    for (port_name, _port) in &boundary.input_ports {
        inputs.insert(
            port_name.clone(),
            SerializablePortDecl { description: None },
        );
    }

    let mut outputs = IndexMap::new();
    for (port_name, port) in &boundary.output_ports {
        outputs.insert(
            port_name.clone(),
            SerializableOutputAlias {
                internal_ref: format!("{}.out", port.internal_node),
            },
        );
    }

    let mut config_schema = IndexMap::new();
    for candidate in &boundary.config_candidates {
        let param_type = infer_json_type(&candidate.value);
        config_schema.insert(
            candidate.param_name.clone(),
            SerializableParamDecl {
                param_type,
                required: false,
                default: Some(candidate.value.clone()),
            },
        );
    }

    let comp_file = SerializableCompositionFile {
        compose: SerializableSignature {
            name: name.to_owned(),
            inputs,
            outputs,
            config_schema,
        },
        nodes: selected_nodes,
    };

    // Serialize to YAML.
    let yaml_content =
        crate::yaml::to_string(&comp_file).map_err(ExtractionWriteError::Serialization)?;

    // Write to output path.
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(ExtractionWriteError::Io)?;
    }
    std::fs::write(output_path, &yaml_content).map_err(ExtractionWriteError::Io)?;

    // Build call-site snippet as a YAML mapping (without the `- ` list prefix,
    // so it can be parsed standalone). The caller wraps it in a list item
    // when inserting into the pipeline YAML.
    let mut snippet = String::new();
    snippet.push_str("type: composition\n");
    snippet.push_str(&format!("name: {name}\n"));
    snippet.push_str(&format!("use: {}\n", output_path.display()));
    if !boundary.input_ports.is_empty() {
        snippet.push_str("inputs:\n");
        for (port_name, port) in &boundary.input_ports {
            snippet.push_str(&format!("  {port_name}: {}\n", port.external_node));
        }
    }
    if !boundary.config_candidates.is_empty() {
        snippet.push_str("config:\n");
        for candidate in &boundary.config_candidates {
            let value_str = match &candidate.value {
                serde_json::Value::String(s) => format!("\"{s}\""),
                other => other.to_string(),
            };
            snippet.push_str(&format!("  {}: {value_str}\n", candidate.param_name));
        }
    }

    Ok(snippet)
}

/// Infer a YAML config_schema type string from a JSON value.
fn infer_json_type(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(_) => "bool".to_owned(),
        serde_json::Value::Number(n) if n.is_f64() => "float".to_owned(),
        serde_json::Value::Number(_) => "int".to_owned(),
        serde_json::Value::String(_) => "string".to_owned(),
        _ => "string".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RouteMode;
    use crate::config::composition::ProvenanceDb;
    use crate::plan::execution::{
        DependencyType, ExecutionPlanDag, NodeExecutionReqs, ParallelismClass, PlanEdge, PlanNode,
        SourceTier,
    };
    use crate::span::Span;
    use petgraph::graph::DiGraph;
    use std::collections::BTreeSet;

    fn source(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_owned(),
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: clinker_record::SchemaBuilder::new().build(),
        }
    }

    fn transform(name: &str) -> PlanNode {
        PlanNode::Transform {
            name: name.to_owned(),
            span: Span::SYNTHETIC,
            resolved: None,
            parallelism_class: ParallelismClass::Stateless,
            tier: 0,
            execution_reqs: NodeExecutionReqs::Streaming,
            window_index: None,
            partition_lookup: None,
            write_set: BTreeSet::new(),
            has_distinct: false,
            output_schema: clinker_record::SchemaBuilder::new().build(),
        }
    }

    fn output(name: &str) -> PlanNode {
        PlanNode::Output {
            name: name.to_owned(),
            span: Span::SYNTHETIC,
            resolved: None,
        }
    }

    fn route(name: &str) -> PlanNode {
        PlanNode::Route {
            name: name.to_owned(),
            span: Span::SYNTHETIC,
            mode: RouteMode::Exclusive,
            branches: vec![],
            default: String::new(),
        }
    }

    fn data_edge() -> PlanEdge {
        PlanEdge {
            dependency_type: DependencyType::Data,
        }
    }

    fn make_dag(graph: DiGraph<PlanNode, PlanEdge>) -> ExecutionPlanDag {
        let topo = petgraph::algo::toposort(&graph, None).unwrap();
        let sources: Vec<String> = topo
            .iter()
            .filter(|&&idx| matches!(graph[idx], PlanNode::Source { .. }))
            .map(|&idx| graph[idx].name().to_owned())
            .collect();
        ExecutionPlanDag {
            graph,
            topo_order: topo,
            source_dag: vec![SourceTier { sources }],
            indices_to_build: vec![],
            output_projections: vec![],
            parallelism: crate::plan::execution::ParallelismProfile {
                per_transform: vec![],
                worker_threads: 1,
            },
            correlation_sort_note: None,
            node_properties: Default::default(),
        }
    }

    /// Build a simple 4-node linear pipeline: src → t1 → t2 → out
    fn build_4_node_dag() -> ExecutionPlanDag {
        let mut graph = DiGraph::new();
        let src = graph.add_node(source("src"));
        let t1 = graph.add_node(transform("t1"));
        let t2 = graph.add_node(transform("t2"));
        let out_idx = graph.add_node(output("out"));
        graph.add_edge(src, t1, data_edge());
        graph.add_edge(t1, t2, data_edge());
        graph.add_edge(t2, out_idx, data_edge());
        make_dag(graph)
    }

    /// Build a DAG with a Route node: src → route → [branch_a, branch_b]
    fn build_route_dag() -> ExecutionPlanDag {
        let mut graph = DiGraph::new();
        let src = graph.add_node(source("src"));
        let router = graph.add_node(route("router"));
        let a = graph.add_node(output("branch_a"));
        let b = graph.add_node(output("branch_b"));
        graph.add_edge(src, router, data_edge());
        graph.add_edge(router, a, data_edge());
        graph.add_edge(router, b, data_edge());
        make_dag(graph)
    }

    #[test]
    fn test_extract_boundary_identifies_input_ports_from_crossing_edges() {
        let dag = build_4_node_dag();
        let prov = ProvenanceDb::default();
        let selected: HashSet<String> = ["t1", "t2"].iter().map(|s| s.to_string()).collect();

        let boundary = analyze_extraction_boundary(&dag, &selected, &prov).unwrap();

        // src → t1 is a crossing edge → input port
        assert_eq!(boundary.input_ports.len(), 1);
        let port = boundary.input_ports.values().next().unwrap();
        assert_eq!(port.external_node, "src");
        assert_eq!(port.internal_node, "t1");
    }

    #[test]
    fn test_extract_boundary_identifies_output_ports() {
        let dag = build_4_node_dag();
        let prov = ProvenanceDb::default();
        let selected: HashSet<String> = ["t1", "t2"].iter().map(|s| s.to_string()).collect();

        let boundary = analyze_extraction_boundary(&dag, &selected, &prov).unwrap();

        // t2 → out is a crossing edge → output port
        assert_eq!(boundary.output_ports.len(), 1);
        let port = boundary.output_ports.values().next().unwrap();
        assert_eq!(port.external_node, "out");
        assert_eq!(port.internal_node, "t2");
    }

    #[test]
    fn test_extract_boundary_route_branch_produces_multiple_outputs() {
        let dag = build_route_dag();
        let prov = ProvenanceDb::default();
        // Select only the route node (src is external, branches are external).
        let selected: HashSet<String> = ["router"].iter().map(|s| s.to_string()).collect();

        let boundary = analyze_extraction_boundary(&dag, &selected, &prov).unwrap();

        // router → branch_a and router → branch_b are two crossing edges.
        // Route node produces named ports per branch.
        assert_eq!(
            boundary.output_ports.len(),
            2,
            "Route node should produce 2 named output ports, got: {:?}",
            boundary.output_ports.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_extract_boundary_literal_config_candidates() {
        let dag = build_4_node_dag();
        let mut prov = ProvenanceDb::default();

        // Simulate a provenance entry for a composition config param on t1.
        use crate::config::composition::{LayerKind, ResolvedValue};
        prov.insert(
            "t1".to_owned(),
            "fuzzy_threshold".to_owned(),
            ResolvedValue::new(
                serde_json::json!(0.85),
                LayerKind::CompositionDefault,
                Span::line_only(10),
            ),
        );

        let selected: HashSet<String> = ["t1", "t2"].iter().map(|s| s.to_string()).collect();
        let boundary = analyze_extraction_boundary(&dag, &selected, &prov).unwrap();

        assert_eq!(boundary.config_candidates.len(), 1);
        assert_eq!(boundary.config_candidates[0].param_name, "fuzzy_threshold");
        assert_eq!(boundary.config_candidates[0].value, serde_json::json!(0.85));
    }

    #[test]
    fn test_extract_boundary_zero_output_ports_warns() {
        let dag = build_4_node_dag();
        let prov = ProvenanceDb::default();
        // Select only the output node — it has no downstream consumers.
        let selected: HashSet<String> = ["out"].iter().map(|s| s.to_string()).collect();

        let boundary = analyze_extraction_boundary(&dag, &selected, &prov).unwrap();

        assert!(boundary.output_ports.is_empty());
        assert!(!boundary.warnings.is_empty());
        assert!(boundary.warnings[0].contains("sink"));
    }

    #[test]
    fn test_extract_boundary_unknown_node_returns_e110() {
        let dag = build_4_node_dag();
        let prov = ProvenanceDb::default();
        let selected: HashSet<String> = ["t1", "nonexistent"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let errors = analyze_extraction_boundary(&dag, &selected, &prov).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "E110");
        assert!(errors[0].message.contains("nonexistent"));
    }

    // ---- Writer tests ----

    /// Helper: build a PipelineConfig from a YAML fixture path.
    fn load_fixture_config(fixture_name: &str) -> PipelineConfig {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let yaml_path = root.join(fixture_name);
        let yaml = std::fs::read_to_string(&yaml_path).expect("read fixture");
        crate::yaml::from_str(&yaml).expect("parse fixture")
    }

    #[test]
    fn test_write_extracted_composition_produces_valid_comp_yaml() {
        let config = load_fixture_config("pipelines/composition_pipeline.yaml");
        let tmp_dir = std::env::temp_dir().join("clinker_extract_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let output_path = tmp_dir.join("test_extracted.comp.yaml");
        let _ = std::fs::remove_file(&output_path);

        // Build a boundary manually for the test.
        let boundary = ExtractionBoundary {
            input_ports: {
                let mut m = IndexMap::new();
                m.insert(
                    "in_customers_src".to_owned(),
                    ExtractedPort {
                        name: "in_customers_src".to_owned(),
                        external_node: "customers_src".to_owned(),
                        internal_node: "enrich1".to_owned(),
                        dependency_type: DependencyType::Data,
                    },
                );
                m
            },
            output_ports: {
                let mut m = IndexMap::new();
                m.insert(
                    "out_enrich1".to_owned(),
                    ExtractedPort {
                        name: "out_enrich1".to_owned(),
                        external_node: "enriched_out".to_owned(),
                        internal_node: "enrich1".to_owned(),
                        dependency_type: DependencyType::Data,
                    },
                );
                m
            },
            internal_nodes: vec!["enrich1".to_owned()],
            config_candidates: vec![],
            warnings: vec![],
        };

        let snippet =
            write_extracted_composition(&boundary, &config, "test_comp", &output_path, false)
                .expect("write should succeed");

        // The output file must exist and be valid YAML.
        assert!(output_path.exists(), "output file must exist");
        let written = std::fs::read_to_string(&output_path).expect("read output");
        assert!(!written.is_empty(), "output must not be empty");

        // Must deserialize as valid YAML (not as CompositionFile — that
        // requires _compose parsing, but the YAML itself must be well-formed).
        let parsed: serde_json::Value =
            crate::yaml::from_str(&written).expect("output must be valid YAML");
        assert!(parsed.get("_compose").is_some(), "must have _compose block");
        assert!(parsed.get("nodes").is_some(), "must have nodes block");

        // Clean up.
        let _ = std::fs::remove_file(&output_path);
        let _ = std::fs::remove_dir(&tmp_dir);

        // snippet must not be empty
        assert!(!snippet.is_empty());
    }

    #[test]
    fn test_write_extracted_composition_callsite_snippet_is_valid_yaml() {
        let config = load_fixture_config("pipelines/composition_pipeline.yaml");
        let tmp_dir = std::env::temp_dir().join("clinker_extract_test2");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let output_path = tmp_dir.join("test_extracted2.comp.yaml");
        let _ = std::fs::remove_file(&output_path);

        let boundary = ExtractionBoundary {
            input_ports: {
                let mut m = IndexMap::new();
                m.insert(
                    "in_customers_src".to_owned(),
                    ExtractedPort {
                        name: "in_customers_src".to_owned(),
                        external_node: "customers_src".to_owned(),
                        internal_node: "enrich1".to_owned(),
                        dependency_type: DependencyType::Data,
                    },
                );
                m
            },
            output_ports: IndexMap::new(),
            internal_nodes: vec!["enrich1".to_owned()],
            config_candidates: vec![ConfigCandidate {
                node_name: "enrich1".to_owned(),
                param_name: "threshold".to_owned(),
                value: serde_json::json!(0.85),
            }],
            warnings: vec![],
        };

        let snippet =
            write_extracted_composition(&boundary, &config, "my_comp", &output_path, false)
                .expect("write should succeed");

        // The snippet must be valid YAML.
        let parsed: serde_json::Value =
            crate::yaml::from_str(&snippet).expect("snippet must be valid YAML");

        // Must have the composition type.
        assert_eq!(
            parsed.get("type").and_then(|v| v.as_str()),
            Some("composition"),
            "snippet must have type: composition"
        );

        // Must have the name.
        assert_eq!(
            parsed.get("name").and_then(|v| v.as_str()),
            Some("my_comp"),
            "snippet must have name: my_comp"
        );

        // Must have inputs block.
        assert!(parsed.get("inputs").is_some(), "snippet must have inputs");

        // Must have config block with threshold.
        let config_block = parsed.get("config").expect("snippet must have config");
        assert!(
            config_block.get("threshold").is_some(),
            "snippet config must have threshold"
        );

        // Clean up.
        let _ = std::fs::remove_file(&output_path);
        let _ = std::fs::remove_dir(&tmp_dir);
    }
}
