//! Shared header types for the unified `nodes:` enum.
//!
//! These are the building blocks for `PipelineNode` variants.
//!
//! # Span-preserving input references
//!
//! Consumer headers carry their `input` / `inputs` references wrapped in
//! [`crate::yaml::Spanned`] so field-level diagnostics (specifically on
//! combine inputs referencing undeclared upstreams) can point at the
//! exact YAML line/column of the offending reference. A custom-Deserialize
//! on `PipelineNode` drives serde-saphyr's native path so
//! `Spanned<NodeInput>` captures real locations even at
//! `IndexMap<String, _>` value position (spike validated in
//! `yaml::tests::test_spanned_at_indexmap_value_position_captures_real_spans`).

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::yaml::Spanned;

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
///
/// `input` is wrapped in [`Spanned`] so consumers (E307-style diagnostics,
/// DAG-edge wiring for future field-level messaging) can recover the exact
/// YAML line/column of the reference. The `Serialize` impl is hand-rolled
/// to skip the span and emit the underlying `NodeInput` verbatim — so the
/// YAML round-trip shape is unchanged.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub input: Spanned<NodeInput>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

impl Serialize for NodeHeader {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = s.serialize_struct("NodeHeader", 4)?;
        state.serialize_field("name", &self.name)?;
        if self.description.is_some() {
            state.serialize_field("description", &self.description)?;
        } else {
            state.skip_field("description")?;
        }
        state.serialize_field("input", &self.input.value)?;
        if self.notes.is_some() {
            state.serialize_field("_notes", &self.notes)?;
        } else {
            state.skip_field("_notes")?;
        }
        state.end()
    }
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
///
/// Each input reference carries a [`Spanned`] wrapper so diagnostics
/// targeting an individual entry (e.g. an undeclared upstream in a multi-
/// input merge) can point at the exact YAML line/column. The `Serialize`
/// impl is hand-rolled to drop the spans and emit the underlying
/// `NodeInput` sequence verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub inputs: Vec<Spanned<NodeInput>>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

impl Serialize for MergeHeader {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeSeq, SerializeStruct};
        let mut state = s.serialize_struct("MergeHeader", 4)?;
        state.serialize_field("name", &self.name)?;
        if self.description.is_some() {
            state.serialize_field("description", &self.description)?;
        } else {
            state.skip_field("description")?;
        }
        // Emit `inputs` as a plain sequence of NodeInput (no span).
        struct InputsSeq<'a>(&'a [Spanned<NodeInput>]);
        impl Serialize for InputsSeq<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(Some(self.0.len()))?;
                for entry in self.0 {
                    seq.serialize_element(&entry.value)?;
                }
                seq.end()
            }
        }
        state.serialize_field("inputs", &InputsSeq(&self.inputs))?;
        if self.notes.is_some() {
            state.serialize_field("_notes", &self.notes)?;
        } else {
            state.skip_field("_notes")?;
        }
        state.end()
    }
}

/// Header for combine nodes — takes a named map of inputs.
///
/// Unlike [`MergeHeader`] (which uses `Vec<Spanned<NodeInput>>`), Combine
/// uses an [`IndexMap<String, Spanned<NodeInput>>`]: each input has a
/// qualifier name used in the body's CXL `where:` / `cxl:` expressions
/// (e.g. `orders.id == products.id`). Insertion order is preserved and
/// determines the default driving input (first entry) when no explicit
/// `drive:` is set on the body — see `CombineBody::drive` in
/// `config/pipeline_node.rs`.
///
/// Wrapping each map value in [`Spanned`] gives E307 diagnostics a real
/// `(line, column)` at the offending reference (the promise of the
/// pre-C.1 span-preserving deser refactor). The `Serialize` impl is hand-
/// rolled to drop spans and emit the underlying map verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CombineHeader {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Named input map: key = qualifier name, value = upstream node ref.
    /// Order is preserved (IndexMap) and determines the default driving
    /// input (first entry) when no explicit `drive:` is set.
    pub input: IndexMap<String, Spanned<NodeInput>>,
    #[serde(default, rename = "_notes")]
    pub notes: Option<serde_json::Value>,
}

impl Serialize for CombineHeader {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeStruct};
        let mut state = s.serialize_struct("CombineHeader", 4)?;
        state.serialize_field("name", &self.name)?;
        if self.description.is_some() {
            state.serialize_field("description", &self.description)?;
        } else {
            state.skip_field("description")?;
        }
        // Emit `input` as a plain map from qualifier to NodeInput (no span).
        struct InputsMap<'a>(&'a IndexMap<String, Spanned<NodeInput>>);
        impl Serialize for InputsMap<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut map = s.serialize_map(Some(self.0.len()))?;
                for (k, v) in self.0 {
                    map.serialize_entry(k, &v.value)?;
                }
                map.end()
            }
        }
        state.serialize_field("input", &InputsMap(&self.input))?;
        if self.notes.is_some() {
            state.serialize_field("_notes", &self.notes)?;
        } else {
            state.skip_field("_notes")?;
        }
        state.end()
    }
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
        assert!(matches!(hdr.input.value, NodeInput::Single(ref s) if s == "raw"));

        // Port-form input.
        let yaml_port = r#"
name: enrich
input: split.domestic
"#;
        let hdr: NodeHeader = crate::yaml::from_str(yaml_port).expect("parse");
        assert!(matches!(
            hdr.input.value,
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
        assert!(matches!(&hdr.inputs[0].value, NodeInput::Single(s) if s == "domestic"));
        assert!(matches!(
            &hdr.inputs[1].value,
            NodeInput::Port { node, port } if node == "intl" && port == "priority"
        ));
    }
}
