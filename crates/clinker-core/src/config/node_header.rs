//! Shared header types for the unified `nodes:` enum (Phase 16b).
//!
//! These are the building blocks for `PipelineNode` variants. Task 16b.2
//! will wire them into the enum itself; for now they stand alone so the
//! foundation tests can lock their shapes in.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// A reference to a producer node from a consumer's `input:` field.
///
/// Accepts either a bare node name (`"clean"`) or a `node.port` pair
/// (`"split.domestic"`). Because node names cannot contain dots (enforced
/// by `config/mod.rs` validate), the split is unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeInput {
    Single(String),
    Port { node: String, port: String },
}

impl Serialize for NodeInput {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let rendered = match self {
            NodeInput::Single(name) => name.clone(),
            NodeInput::Port { node, port } => format!("{node}.{port}"),
        };
        s.serialize_str(&rendered)
    }
}

impl<'de> Deserialize<'de> for NodeInput {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        parse_node_input(&s).map_err(serde::de::Error::custom)
    }
}

fn parse_node_input(s: &str) -> Result<NodeInput, String> {
    if s.is_empty() {
        return Err("empty input ref".to_string());
    }
    let parts: Vec<&str> = s.split('.').collect();
    match parts.as_slice() {
        [single] if !single.is_empty() => Ok(NodeInput::Single((*single).to_string())),
        [node, port] if !node.is_empty() && !port.is_empty() => Ok(NodeInput::Port {
            node: (*node).to_string(),
            port: (*port).to_string(),
        }),
        _ => Err(format!("invalid input ref: {s:?}")),
    }
}

/// Header shared by every consumer variant (transform, route, aggregate,
/// output). Every field is validated via `deny_unknown_fields` so typos like
/// `inputs:` on a consumer node surface as parse errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub input: NodeInput,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

/// Header for source nodes — no `input:` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

/// Header for merge nodes — takes a list of `inputs:` instead of a single `input:`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub inputs: Vec<NodeInput>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

/// Header for combine nodes — takes a named map of inputs.
///
/// Unlike [`MergeHeader`] (which uses `Vec<NodeInput>`), Combine uses an
/// [`IndexMap<String, NodeInput>`]: each input has a qualifier name used
/// in the body's CXL `where:` / `cxl:` expressions (e.g. `orders.id ==
/// products.id`). Insertion order is preserved and determines the default
/// driving input (first entry) when no explicit `drive:` is set on the
/// body — see `CombineBody::drive` in `config/pipeline_node.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CombineHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Named input map: key = qualifier name, value = upstream node ref.
    /// Order is preserved (IndexMap) and determines the default driving
    /// input (first entry) when no explicit `drive:` is set.
    pub input: IndexMap<String, NodeInput>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_input_parses_single() {
        assert_eq!(
            parse_node_input("clean").unwrap(),
            NodeInput::Single("clean".to_string())
        );
    }

    #[test]
    fn test_node_input_parses_port() {
        assert_eq!(
            parse_node_input("split.domestic").unwrap(),
            NodeInput::Port {
                node: "split".to_string(),
                port: "domestic".to_string()
            }
        );
    }

    #[test]
    fn test_node_input_rejects_double_dot() {
        assert!(parse_node_input("a.b.c").is_err());
    }

    #[test]
    fn test_node_input_rejects_empty() {
        assert!(parse_node_input("").is_err());
    }

    #[test]
    fn test_node_input_rejects_leading_or_trailing_dot() {
        assert!(parse_node_input(".port").is_err());
        assert!(parse_node_input("node.").is_err());
    }

    #[test]
    fn test_node_header_round_trips() {
        let yaml = r#"
name: clean
description: "trim + uppercase"
input: raw
"#;
        let hdr: NodeHeader = crate::yaml::from_str(yaml).expect("parse");
        assert_eq!(hdr.name, "clean");
        assert_eq!(hdr.description.as_deref(), Some("trim + uppercase"));
        assert!(matches!(hdr.input, NodeInput::Single(ref s) if s == "raw"));

        // Port-form input.
        let yaml_port = r#"
name: enrich
input: split.domestic
"#;
        let hdr: NodeHeader = crate::yaml::from_str(yaml_port).expect("parse");
        assert!(matches!(
            hdr.input,
            NodeInput::Port { ref node, ref port } if node == "split" && port == "domestic"
        ));
    }

    #[test]
    fn test_source_header_rejects_input_field() {
        let yaml = r#"
name: orders
input: upstream
"#;
        let err = crate::yaml::from_str::<SourceHeader>(yaml)
            .expect_err("SourceHeader must reject `input:`");
        let msg = err.to_string();
        assert!(msg.contains("input"), "unexpected error: {msg}");
    }

    #[test]
    fn test_merge_header_requires_inputs_list() {
        // Empty list is structurally valid (graph validation will reject it later).
        let yaml_empty = r#"
name: combined
inputs: []
"#;
        let hdr: MergeHeader = crate::yaml::from_str(yaml_empty).expect("parse empty inputs");
        assert_eq!(hdr.name, "combined");
        assert!(hdr.inputs.is_empty());

        // Missing `inputs:` entirely is a parse error.
        let yaml_missing = r#"
name: combined
"#;
        assert!(crate::yaml::from_str::<MergeHeader>(yaml_missing).is_err());

        // Populated list round-trips.
        let yaml_full = r#"
name: combined
inputs:
  - domestic
  - intl.priority
"#;
        let hdr: MergeHeader = crate::yaml::from_str(yaml_full).expect("parse full");
        assert_eq!(hdr.inputs.len(), 2);
        assert!(matches!(&hdr.inputs[0], NodeInput::Single(s) if s == "domestic"));
        assert!(matches!(
            &hdr.inputs[1],
            NodeInput::Port { node, port } if node == "intl" && port == "priority"
        ));
    }
}
