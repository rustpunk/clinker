//! OpenLineage `ColumnLineageDatasetFacet`: per-column field lineage.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Column-level lineage for one dataset.
///
/// DIRECT (value-derivation) lineage is keyed per output column in
/// [`fields`](ColumnLineageDatasetFacet::fields); whole-dataset INDIRECT
/// (influence via filter / join / group / sort) lineage is collected once in
/// [`dataset`](ColumnLineageDatasetFacet::dataset) rather than duplicated across
/// every column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    /// URI of the software that produced this facet.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// Schema URL of the facet spec this conforms to.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    /// Output column name -> the input fields its value derives from. A
    /// `BTreeMap` keeps the serialized key order deterministic.
    pub fields: BTreeMap<String, FieldLineage>,
    /// Input fields that influence the dataset as a whole (INDIRECT lineage).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub dataset: Vec<InputField>,
}

/// The input fields one output column derives from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldLineage {
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<InputField>,
}

/// A reference to one input dataset column plus how it reaches the output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputField {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub transformations: Vec<Transformation>,
}

/// How an input field reaches an output: its derivation/influence class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")]
    pub transformation_type: TransformationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<TransformationSubtype>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Set when the transformation obfuscates the value (e.g. a hash).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub masking: Option<bool>,
}

/// Whether an output value is derived from the input (`DIRECT`) or merely
/// influenced by it (`INDIRECT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransformationType {
    Direct,
    Indirect,
}

/// The specific derivation/influence kind under a [`TransformationType`].
///
/// The OpenLineage spec leaves `subtype` free-form; this crate is a producer and
/// emits only the documented vocabulary below. Parsing foreign lineage carrying
/// an out-of-set subtype is intentionally not supported here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransformationSubtype {
    Identity,
    Transformation,
    Aggregation,
    Join,
    GroupBy,
    Filter,
    Sort,
    Window,
    Conditional,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtype_casing_matches_spec() {
        let cases = [
            (TransformationSubtype::Identity, "IDENTITY"),
            (TransformationSubtype::Transformation, "TRANSFORMATION"),
            (TransformationSubtype::Aggregation, "AGGREGATION"),
            (TransformationSubtype::Join, "JOIN"),
            (TransformationSubtype::GroupBy, "GROUP_BY"),
            (TransformationSubtype::Filter, "FILTER"),
            (TransformationSubtype::Sort, "SORT"),
            (TransformationSubtype::Window, "WINDOW"),
            (TransformationSubtype::Conditional, "CONDITIONAL"),
        ];
        for (variant, wire) in cases {
            assert_eq!(serde_json::to_value(variant).unwrap(), wire);
        }
        assert_eq!(
            serde_json::to_value(TransformationType::Direct).unwrap(),
            "DIRECT"
        );
        assert_eq!(
            serde_json::to_value(TransformationType::Indirect).unwrap(),
            "INDIRECT"
        );
    }
}
