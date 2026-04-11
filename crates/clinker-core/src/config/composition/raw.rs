//! Serde-derived raw shapes for `.comp.yaml` deserialization (Task 16c.1.2).
//!
//! Separated from the canonical public types in `mod.rs` because the canonical
//! [`crate::config::composition::CompositionSignature`] holds
//! [`crate::span::Span`] values (LD-003). serde-saphyr natively produces
//! [`serde_saphyr::Span`], so we deserialize into this raw layer, capture
//! saphyr spans via [`serde_saphyr::Spanned`], then convert to the canonical
//! form via [`Span::from_saphyr`] at the YAML chokepoint boundary — exactly
//! the pattern prescribed by LD-003 and CONVENTIONS.md §Span usage.
//!
//! Nothing in this module is exported outside the `composition` module.

use super::{
    CompositionFile, CompositionSignature, OutputAlias, ParamDecl, ParamType, PortDecl,
    ResourceDecl, ResourceKind, SourceMap, SpannedNodeRef,
};
use crate::config::pipeline_node::{PipelineNode, SchemaDecl};
use crate::span::{FileId, Span};
use crate::yaml::Spanned;
use indexmap::IndexMap;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawCompositionFile {
    #[serde(rename = "_compose")]
    pub compose: RawCompositionSignature,
    #[serde(default)]
    pub nodes: Vec<Spanned<PipelineNode>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawCompositionSignature {
    pub name: String,
    #[serde(default)]
    pub inputs: IndexMap<String, RawPortDecl>,
    #[serde(default)]
    pub outputs: IndexMap<String, RawOutputAlias>,
    #[serde(default)]
    pub config_schema: IndexMap<String, RawParamDecl>,
    #[serde(default)]
    pub resources_schema: IndexMap<String, RawResourceDecl>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawPortDecl {
    /// Minimum-required column set (LD-16c-2). YAML form: a list of
    /// `{ name, type }` entries matching the 16b `SchemaDecl` convention.
    /// Absence means accept-any.
    #[serde(default)]
    pub schema: Option<SchemaDecl>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
}

/// Port output alias — accepts either a string shorthand or an object form.
#[derive(Debug)]
pub(super) struct RawOutputAlias {
    pub internal_ref: Spanned<String>,
    pub description: Option<String>,
}

impl<'de> Deserialize<'de> for RawOutputAlias {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawOutputAliasVisitor;

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LongForm {
            #[serde(rename = "ref")]
            internal_ref: Spanned<String>,
            #[serde(default)]
            description: Option<String>,
        }

        impl<'de> Visitor<'de> for RawOutputAliasVisitor {
            type Value = RawOutputAlias;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a node-channel reference string or an object with 'ref' and optional 'description'")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                // Short form has no span hook at this layer; callers fall
                // back to the enclosing map span when emitting diagnostics.
                Ok(RawOutputAlias {
                    internal_ref: Spanned::new(
                        v.to_owned(),
                        serde_saphyr::Location::UNKNOWN,
                        serde_saphyr::Location::UNKNOWN,
                    ),
                    description: None,
                })
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RawOutputAlias {
                    internal_ref: Spanned::new(
                        v,
                        serde_saphyr::Location::UNKNOWN,
                        serde_saphyr::Location::UNKNOWN,
                    ),
                    description: None,
                })
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let long = LongForm::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(RawOutputAlias {
                    internal_ref: long.internal_ref,
                    description: long.description,
                })
            }
        }

        deserializer.deserialize_any(RawOutputAliasVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawParamDecl {
    #[serde(rename = "type")]
    pub param_type: Spanned<ParamType>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default, rename = "enum")]
    pub enum_values: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub range: Option<(f64, f64)>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawResourceDecl {
    pub kind: Spanned<ResourceKind>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
}

// ---------------------------------------------------------------------
// Conversion (raw → canonical)
// ---------------------------------------------------------------------

impl RawCompositionFile {
    pub(super) fn finalize(
        self,
        file_id: FileId,
        source_path: std::path::PathBuf,
    ) -> CompositionFile {
        let signature = self.compose.finalize(file_id, source_path);
        CompositionFile {
            signature,
            nodes: self.nodes,
        }
    }
}

impl RawCompositionSignature {
    fn finalize(self, file_id: FileId, source_path: std::path::PathBuf) -> CompositionSignature {
        let mut source_spans: SourceMap = IndexMap::new();

        let inputs = self
            .inputs
            .into_iter()
            .map(|(name, raw)| (name, raw.finalize()))
            .collect();

        let outputs = self
            .outputs
            .into_iter()
            .map(|(name, raw)| {
                let alias = raw.finalize(file_id);
                source_spans.insert(format!("outputs.{name}"), alias.internal_ref.span);
                (name, alias)
            })
            .collect();

        let config_schema = self
            .config_schema
            .into_iter()
            .map(|(name, raw)| {
                let decl = raw.finalize(file_id);
                source_spans.insert(format!("config_schema.{name}"), decl.span);
                (name, decl)
            })
            .collect();

        let resources_schema = self
            .resources_schema
            .into_iter()
            .map(|(name, raw)| {
                let decl = raw.finalize(file_id);
                source_spans.insert(format!("resources_schema.{name}"), decl.span);
                (name, decl)
            })
            .collect();

        CompositionSignature {
            name: self.name,
            inputs,
            outputs,
            config_schema,
            resources_schema,
            source_path,
            source_spans,
        }
    }
}

impl RawPortDecl {
    fn finalize(self) -> PortDecl {
        PortDecl {
            schema: self.schema,
            description: self.description,
            required: self.required,
        }
    }
}

impl RawOutputAlias {
    fn finalize(self, file_id: FileId) -> OutputAlias {
        OutputAlias {
            internal_ref: SpannedNodeRef {
                span: Span::from_saphyr(file_id, self.internal_ref.referenced.span()),
                value: self.internal_ref.value,
            },
            description: self.description,
        }
    }
}

impl RawParamDecl {
    fn finalize(self, file_id: FileId) -> ParamDecl {
        ParamDecl {
            param_type: self.param_type.value,
            required: self.required,
            default: self.default,
            enum_values: self.enum_values,
            range: self.range,
            description: self.description,
            span: Span::from_saphyr(file_id, self.param_type.referenced.span()),
        }
    }
}

impl RawResourceDecl {
    fn finalize(self, file_id: FileId) -> ResourceDecl {
        ResourceDecl {
            kind: self.kind.value,
            required: self.required,
            description: self.description,
            span: Span::from_saphyr(file_id, self.kind.referenced.span()),
        }
    }
}

// ---------------------------------------------------------------------
// Deserialize impls for enum wrappers in the canonical types
// ---------------------------------------------------------------------

// ParamType and ResourceKind are defined in mod.rs; serde derives live here
// so the canonical types stay free of serde machinery. serde's orphan rules
// require a newtype or in-crate derive — the derive is in-crate, so it's fine.
//
// These `impl Deserialize` blocks duplicate the `#[serde(rename_all = ...)]`
// shape via a small shim to avoid putting derives on the canonical types.

impl<'de> Deserialize<'de> for ParamType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "string" => Ok(ParamType::String),
            "int" => Ok(ParamType::Int),
            "float" => Ok(ParamType::Float),
            "bool" => Ok(ParamType::Bool),
            "path" => Ok(ParamType::Path),
            other => Err(de::Error::unknown_variant(
                other,
                &["string", "int", "float", "bool", "path"],
            )),
        }
    }
}

impl<'de> Deserialize<'de> for ResourceKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "file" => Ok(ResourceKind::File),
            other => Err(de::Error::unknown_variant(other, &["file"])),
        }
    }
}
