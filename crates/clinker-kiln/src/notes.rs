/// Helpers for reading and writing `_notes` metadata in `PipelineConfig`.
///
/// Notes are stored as `serde_json::Value` in the `_notes` field of each
/// config struct. This module provides typed accessors.
///
/// YAML format (spec §A5A.5):
/// ```yaml
/// _notes:
///   stage: |
///     Freeform markdown note about this stage.
///   fields:
///     expr: "Annotation on the expr field."
///     path: "Annotation on the path field."
/// ```
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Parsed notes for a single stage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StageNotes {
    /// Freeform markdown note about the stage.
    pub stage_note: String,
    /// Per-field annotations keyed by field path.
    pub field_annotations: HashMap<String, String>,
}

/// Extract `StageNotes` from a `_notes` JSON value.
pub fn parse_notes(notes: Option<&Value>) -> StageNotes {
    let Some(Value::Object(map)) = notes else {
        return StageNotes::default();
    };

    let stage_note = map
        .get("stage")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let field_annotations = map
        .get("fields")
        .and_then(|v| v.as_object())
        .map(|fields| {
            fields
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    StageNotes {
        stage_note,
        field_annotations,
    }
}

/// Serialize `StageNotes` back to a `serde_json::Value` for storage in the
/// `_notes` field. Returns `None` if there are no notes (omit the field).
pub fn serialize_notes(notes: &StageNotes) -> Option<Value> {
    let has_stage = !notes.stage_note.is_empty();
    let has_fields = !notes.field_annotations.is_empty();

    if !has_stage && !has_fields {
        return None;
    }

    let mut map = Map::new();

    if has_stage {
        map.insert("stage".to_string(), Value::String(notes.stage_note.clone()));
    }

    if has_fields {
        let fields: Map<String, Value> = notes
            .field_annotations
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        map.insert("fields".to_string(), Value::Object(fields));
    }

    Some(Value::Object(map))
}
