/// Per-item fallback parser for graceful degradation.
///
/// When the standard all-or-nothing `serde_saphyr::from_str` parse fails,
/// this module provides a two-pass approach:
/// 1. Parse the YAML into a `serde_json::Value` tree (requires valid YAML syntax).
/// 2. Walk the tree and deserialize each section/item individually.
///
/// Items that parse successfully become `PartialItem::Ok(T)`, failures become
/// `PartialItem::Err` with the error message. The canvas can then render
/// error nodes at the failure positions.

use crate::composition::RawTransformEntry;
use crate::config::{interpolate_env_vars, ErrorHandlingConfig, InputConfig, OutputConfig, PipelineMeta};

/// An item that was either successfully parsed or failed with an error.
#[derive(Debug, Clone)]
pub enum PartialItem<T> {
    Ok(T),
    Err {
        /// Zero-based index within the YAML array.
        index: usize,
        /// Human-readable parse error message.
        message: String,
    },
}

/// A pipeline config where individual items may have failed to parse.
///
/// Produced by `parse_partial_config` when the YAML is syntactically valid
/// but one or more items fail semantic deserialization.
#[derive(Debug, Clone)]
pub struct PartialPipelineConfig {
    pub pipeline: Result<PipelineMeta, String>,
    pub inputs: Vec<PartialItem<InputConfig>>,
    pub transformations: Vec<PartialItem<RawTransformEntry>>,
    pub outputs: Vec<PartialItem<OutputConfig>>,
    pub error_handling: Result<ErrorHandlingConfig, String>,
    pub notes: Option<serde_json::Value>,
    /// All error messages collected (for the parse_errors signal).
    pub errors: Vec<String>,
}

/// Parse a YAML string with per-item fallback.
///
/// Returns `Ok(partial)` if the YAML is syntactically valid (even if some items
/// fail to deserialize). Returns `Err` only when the YAML itself is broken
/// (bad indentation, unclosed quotes, etc.).
pub fn parse_partial_config(yaml: &str) -> Result<PartialPipelineConfig, String> {
    let interpolated = interpolate_env_vars(yaml, &[]).map_err(|e| e.to_string())?;

    let tree: serde_json::Value =
        serde_saphyr::from_str(&interpolated).map_err(|e| format!("YAML syntax error: {e}"))?;

    let obj = tree
        .as_object()
        .ok_or_else(|| "expected top-level YAML mapping".to_string())?;

    let mut errors = Vec::new();

    // Pipeline metadata (singleton)
    let pipeline = match obj.get("pipeline") {
        Some(v) => serde_json::from_value::<PipelineMeta>(v.clone()).map_err(|e| {
            let msg = format!("pipeline: {e}");
            errors.push(msg.clone());
            msg
        }),
        None => {
            let msg = "missing required key: pipeline".to_string();
            errors.push(msg.clone());
            Err(msg)
        }
    };

    // Inputs array
    let inputs = parse_array_section::<InputConfig>(obj, "inputs", &mut errors);

    // Transformations array (uses RawTransformEntry for _import support)
    let transformations =
        parse_array_section::<RawTransformEntry>(obj, "transformations", &mut errors);

    // Outputs array
    let outputs = parse_array_section::<OutputConfig>(obj, "outputs", &mut errors);

    // Error handling (singleton, optional with default)
    let error_handling = match obj.get("error_handling") {
        Some(v) => serde_json::from_value::<ErrorHandlingConfig>(v.clone()).map_err(|e| {
            let msg = format!("error_handling: {e}");
            errors.push(msg.clone());
            msg
        }),
        None => Ok(ErrorHandlingConfig::default()),
    };

    // Notes (optional, pass-through)
    let notes = obj.get("_notes").cloned();

    Ok(PartialPipelineConfig {
        pipeline,
        inputs,
        transformations,
        outputs,
        error_handling,
        notes,
        errors,
    })
}

/// Parse an array section with per-item fallback.
fn parse_array_section<T: serde::de::DeserializeOwned + Clone>(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    errors: &mut Vec<String>,
) -> Vec<PartialItem<T>> {
    let Some(value) = obj.get(key) else {
        return Vec::new();
    };

    let Some(arr) = value.as_array() else {
        let msg = format!("{key}: expected array");
        errors.push(msg.clone());
        return vec![PartialItem::Err { index: 0, message: msg }];
    };

    arr.iter()
        .enumerate()
        .map(|(i, item)| match serde_json::from_value::<T>(item.clone()) {
            Ok(v) => PartialItem::Ok(v),
            Err(e) => {
                let msg = format!("{key}[{i}]: {e}");
                errors.push(msg.clone());
                PartialItem::Err { index: i, message: msg }
            }
        })
        .collect()
}
