//! Per-item fallback parser for graceful degradation (Kiln IDE).
//!
//! Phase 16b Task 16b.7: rewritten to walk the unified `nodes:` array
//! directly. Sources and outputs deserialize into `SourceConfig`/`OutputConfig`
//! via the Body wrappers; transforms/aggregates/routes produce a
//! lightweight [`PartialTransform`] tile with only the fields the canvas
//! renders.

use crate::config::{
    ErrorHandlingConfig, OutputConfig, PipelineMeta, SourceConfig, interpolate_env_vars,
};

#[derive(Debug, Clone)]
pub enum PartialItem<T> {
    Ok(T),
    Err { index: usize, message: String },
}

/// Kiln IDE partial view of a transform-like node (Transform, Aggregate,
/// Route). Only the fields the canvas renders are populated.
#[derive(Debug, Clone)]
pub struct PartialTransform {
    pub name: String,
    pub description: Option<String>,
    pub cxl: String,
}

impl PartialTransform {
    pub fn cxl_source(&self) -> &str {
        &self.cxl
    }
}

#[derive(Debug, Clone)]
pub struct PartialPipelineConfig {
    pub pipeline: Result<PipelineMeta, String>,
    pub inputs: Vec<PartialItem<SourceConfig>>,
    pub transformations: Vec<PartialItem<PartialTransform>>,
    pub outputs: Vec<PartialItem<OutputConfig>>,
    pub error_handling: Result<ErrorHandlingConfig, String>,
    pub notes: Option<serde_json::Value>,
    pub errors: Vec<String>,
}

pub fn parse_partial_config(yaml: &str) -> Result<PartialPipelineConfig, String> {
    let interpolated = interpolate_env_vars(yaml, &[]).map_err(|e| e.to_string())?;

    let tree: serde_json::Value =
        crate::yaml::from_str(&interpolated).map_err(|e| format!("YAML syntax error: {e}"))?;

    let obj = tree
        .as_object()
        .ok_or_else(|| "expected top-level YAML mapping".to_string())?;

    let mut errors = Vec::new();

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

    let mut inputs: Vec<PartialItem<SourceConfig>> = Vec::new();
    let mut transformations: Vec<PartialItem<PartialTransform>> = Vec::new();
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

    let error_handling = match obj.get("error_handling") {
        Some(v) => serde_json::from_value::<ErrorHandlingConfig>(v.clone()).map_err(|e| {
            let msg = format!("error_handling: {e}");
            errors.push(msg.clone());
            msg
        }),
        None => Ok(ErrorHandlingConfig::default()),
    };

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

fn project_partial_nodes(
    nodes_arr: &[serde_json::Value],
    inputs: &mut Vec<PartialItem<SourceConfig>>,
    transformations: &mut Vec<PartialItem<PartialTransform>>,
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
        let description = obj
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let config = obj
            .get("config")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        match type_tag {
            "source" => {
                match serde_json::from_value::<SourceConfig>(merge_name_into(&config, &name)) {
                    Ok(src) => inputs.push(PartialItem::Ok(src)),
                    Err(e) => {
                        let msg = format!("nodes[{i}] ({name}): {e}");
                        errors.push(msg.clone());
                        inputs.push(PartialItem::Err {
                            index: i,
                            message: msg,
                        });
                    }
                }
            }
            "output" => {
                match serde_json::from_value::<OutputConfig>(merge_name_into(&config, &name)) {
                    Ok(out) => outputs.push(PartialItem::Ok(out)),
                    Err(e) => {
                        let msg = format!("nodes[{i}] ({name}): {e}");
                        errors.push(msg.clone());
                        outputs.push(PartialItem::Err {
                            index: i,
                            message: msg,
                        });
                    }
                }
            }
            "transform" | "aggregate" | "route" => {
                let cxl = match type_tag {
                    "transform" | "aggregate" => config
                        .get("cxl")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    _ => String::new(),
                };
                transformations.push(PartialItem::Ok(PartialTransform {
                    name: name.clone(),
                    description: description.clone(),
                    cxl,
                }));
            }
            "merge" | "composition" => {}
            other => {
                let msg = format!("nodes[{i}] ({name}): unknown type '{other}'");
                errors.push(msg);
            }
        }
    }
}

fn merge_name_into(config: &serde_json::Value, name: &str) -> serde_json::Value {
    let mut map = config.as_object().cloned().unwrap_or_default();
    map.entry("name".to_string())
        .or_insert(serde_json::Value::String(name.to_string()));
    serde_json::Value::Object(map)
}
