/// Per-item fallback parser for graceful degradation.
///
/// When the standard all-or-nothing `crate::yaml::from_str` parse fails,
/// this module provides a two-pass approach:
/// 1. Parse the YAML into a `serde_json::Value` tree (requires valid YAML syntax).
/// 2. Walk the tree and deserialize each section/item individually.
///
/// Items that parse successfully become `PartialItem::Ok(T)`, failures become
/// `PartialItem::Err` with the error message. The canvas can then render
/// error nodes at the failure positions.
use crate::config::{
    ErrorHandlingConfig, OutputConfig, PipelineMeta, SourceConfig, TransformConfig,
    interpolate_env_vars,
};

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
    pub inputs: Vec<PartialItem<SourceConfig>>,
    pub transformations: Vec<PartialItem<TransformConfig>>,
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
        crate::yaml::from_str(&interpolated).map_err(|e| format!("YAML syntax error: {e}"))?;

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

    // Phase 16b Wave 4ab Checkpoint B: partial config is sourced exclusively
    // from the unified `nodes:` array. The legacy top-level
    // `inputs:`/`transformations:`/`outputs:` YAML sections are no longer
    // consulted here; the full parser still accepts them via the lift shim
    // but the partial path walks nodes only.
    let mut inputs: Vec<PartialItem<SourceConfig>> = Vec::new();
    let mut transformations: Vec<PartialItem<TransformConfig>> = Vec::new();
    let mut outputs: Vec<PartialItem<OutputConfig>> = Vec::new();

    if let Some(nodes_value) = obj.get("nodes")
        && let Some(nodes_arr) = nodes_value.as_array()
    {
        project_partial_nodes(
            nodes_arr,
            &mut inputs,
            &mut transformations,
            &mut outputs,
            &mut errors,
        );
    }

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

/// Phase 16b Wave 3 — per-node partial projection shim.
///
/// Walks each element of the YAML `nodes:` array and, based on its
/// `type:` tag, synthesizes an entry in the legacy partial vectors
/// that Kiln's `pipeline_view` already consumes. Parse errors on any
/// single node degrade to an error tile in the target section (the
/// position the node would occupy). Merge and Composition variants
/// have no legacy analogue in Wave 3 and are elided (they still show
/// up in the raw node count but do not render tiles).
fn project_partial_nodes(
    nodes_arr: &[serde_json::Value],
    inputs: &mut Vec<PartialItem<SourceConfig>>,
    transformations: &mut Vec<PartialItem<TransformConfig>>,
    outputs: &mut Vec<PartialItem<OutputConfig>>,
    errors: &mut Vec<String>,
) {
    for (i, node) in nodes_arr.iter().enumerate() {
        let Some(obj) = node.as_object() else {
            let msg = format!("nodes[{i}]: expected mapping");
            errors.push(msg.clone());
            continue;
        };
        let type_tag = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        let config = obj
            .get("config")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        match type_tag {
            "source" => match serde_json::from_value::<SourceConfig>(config) {
                Ok(src) => inputs.push(PartialItem::Ok(src)),
                Err(e) => {
                    let msg = format!("nodes[{i}] ({name}): {e}");
                    errors.push(msg.clone());
                    inputs.push(PartialItem::Err {
                        index: i,
                        message: msg,
                    });
                }
            },
            "output" => match serde_json::from_value::<OutputConfig>(config) {
                Ok(out) => outputs.push(PartialItem::Ok(out)),
                Err(e) => {
                    let msg = format!("nodes[{i}] ({name}): {e}");
                    errors.push(msg.clone());
                    outputs.push(PartialItem::Err {
                        index: i,
                        message: msg,
                    });
                }
            },
            "transform" | "aggregate" | "route" => {
                // Synthesize a minimally-valid TransformConfig JSON view
                // and push it through serde so notes/log/etc. round-trip.
                let mut synth = serde_json::Map::new();
                synth.insert("name".into(), serde_json::Value::String(name.clone()));
                if let Some(desc) = obj.get("description") {
                    synth.insert("description".into(), desc.clone());
                }
                if let Some(inp) = obj.get("input") {
                    synth.insert("input".into(), inp.clone());
                }
                if let Some(notes) = obj.get("_notes") {
                    synth.insert("_notes".into(), notes.clone());
                }
                let config_obj = config.as_object().cloned().unwrap_or_default();
                match type_tag {
                    "transform" => {
                        if let Some(v) = config_obj.get("cxl") {
                            synth.insert("cxl".into(), v.clone());
                        }
                        if let Some(v) = config_obj.get("analytic_window") {
                            synth.insert("local_window".into(), v.clone());
                        }
                        if let Some(v) = config_obj.get("log") {
                            synth.insert("log".into(), v.clone());
                        }
                        if let Some(v) = config_obj.get("validations") {
                            synth.insert("validations".into(), v.clone());
                        }
                    }
                    "aggregate" => {
                        let mut agg = serde_json::Map::new();
                        if let Some(v) = config_obj.get("group_by") {
                            agg.insert("group_by".into(), v.clone());
                        } else {
                            agg.insert("group_by".into(), serde_json::Value::Array(vec![]));
                        }
                        if let Some(v) = config_obj.get("cxl") {
                            agg.insert("cxl".into(), v.clone());
                        }
                        if let Some(v) = config_obj.get("strategy") {
                            agg.insert("strategy".into(), v.clone());
                        }
                        synth.insert("aggregate".into(), serde_json::Value::Object(agg));
                    }
                    "route" => {
                        synth.insert("cxl".into(), serde_json::Value::String(String::new()));
                        // Translate BTreeMap-shaped `conditions` + `default`
                        // into the legacy `{branches: [..], default}` shape.
                        let mut route = serde_json::Map::new();
                        if let Some(v) = config_obj.get("mode") {
                            route.insert("mode".into(), v.clone());
                        }
                        let mut branches = Vec::new();
                        if let Some(conds) =
                            config_obj.get("conditions").and_then(|v| v.as_object())
                        {
                            for (bname, cond) in conds {
                                let mut branch = serde_json::Map::new();
                                branch.insert(
                                    "name".into(),
                                    serde_json::Value::String(bname.clone()),
                                );
                                branch.insert("condition".into(), cond.clone());
                                branches.push(serde_json::Value::Object(branch));
                            }
                        }
                        route.insert("branches".into(), serde_json::Value::Array(branches));
                        if let Some(v) = config_obj.get("default") {
                            route.insert("default".into(), v.clone());
                        }
                        synth.insert("route".into(), serde_json::Value::Object(route));
                    }
                    _ => unreachable!(),
                }
                let synth_val = serde_json::Value::Object(synth);
                match serde_json::from_value::<TransformConfig>(synth_val) {
                    Ok(t) => transformations.push(PartialItem::Ok(t)),
                    Err(e) => {
                        let msg = format!("nodes[{i}] ({name}): {e}");
                        errors.push(msg.clone());
                        transformations.push(PartialItem::Err {
                            index: i,
                            message: msg,
                        });
                    }
                }
            }
            "merge" | "composition" => {
                // No legacy partial analogue in Wave 3; Wave 4 rewrites
                // PartialPipelineConfig to the IndexMap+PartialItem shape
                // and these render natively.
            }
            other => {
                let msg = format!("nodes[{i}] ({name}): unknown type '{other}'");
                errors.push(msg);
            }
        }
    }
}
