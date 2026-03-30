/// Auto-generated stage descriptions and structural metadata.
///
/// All content is derived deterministically from the `PipelineConfig` —
/// never from run results. Description templates follow spec §A8.2.

use clinker_core::config::{PipelineConfig, TransformConfig};

/// Structural documentation for a single pipeline stage.
#[derive(Clone, Debug)]
pub struct StageDoc {
    /// Auto-generated description of what this stage does.
    pub description: String,
    /// Key-value metadata entries (Type, Pass, Path, etc.).
    pub metadata: Vec<(&'static str, String)>,
    /// Columns added by this stage (CXL `emit` statements).
    pub columns_added: Vec<String>,
}

/// Generate documentation for the stage with the given name.
pub fn generate_stage_doc(config: &PipelineConfig, stage_name: &str) -> Option<StageDoc> {
    // Check inputs
    if let Some(input) = config.inputs.iter().find(|i| i.name == stage_name) {
        let format = format!("{:?}", input.r#type).to_lowercase();
        let has_header = input.options.as_ref()
            .and_then(|o| o.has_header)
            .unwrap_or(true);
        let header_str = if has_header { "present" } else { "absent" };

        return Some(StageDoc {
            description: format!(
                "Reads {} data from {}. Header row {}.",
                format, input.path, header_str,
            ),
            metadata: vec![
                ("TYPE", "source".to_string()),
                ("FORMAT", format),
                ("PATH", input.path.clone()),
                ("HAS HEADER", header_str.to_string()),
            ],
            columns_added: vec![],
        });
    }

    // Check transforms
    if let Some(transform) = config.transformations.iter().find(|t| t.name == stage_name) {
        let emits = extract_emit_names(&transform.cxl);
        let emit_count = emits.len();

        let description = if emit_count > 0 {
            let field_list = emits.join(", ");
            format!(
                "Computes {} derived field(s): {}. Row count is preserved.",
                emit_count, field_list,
            )
        } else {
            "Evaluates CXL expressions. Row count is preserved.".to_string()
        };

        let mut metadata = vec![
            ("TYPE", "transform".to_string()),
            ("EXPRESSIONS", format!("{} field derivation(s)", emit_count)),
            ("PRESERVES ROWS", "yes".to_string()),
        ];

        if let Some(ref desc) = transform.description {
            metadata.insert(1, ("DESCRIPTION", desc.clone()));
        }

        return Some(StageDoc {
            description,
            metadata,
            columns_added: emits,
        });
    }

    // Check outputs
    if let Some(output) = config.outputs.iter().find(|o| o.name == stage_name) {
        let format = format!("{:?}", output.r#type).to_lowercase();

        let mut metadata = vec![
            ("TYPE", "output".to_string()),
            ("FORMAT", format.clone()),
            ("PATH", output.path.clone()),
            ("INCLUDE UNMAPPED", output.include_unmapped.to_string()),
        ];

        if let Some(ref mapping) = output.mapping {
            metadata.push(("FIELD MAPPINGS", format!("{} mapping(s)", mapping.len())));
        }

        if let Some(ref exclude) = output.exclude {
            metadata.push(("EXCLUDED FIELDS", exclude.join(", ")));
        }

        return Some(StageDoc {
            description: format!("Writes {} output to {}.", format, output.path),
            metadata,
            columns_added: vec![],
        });
    }

    None
}

/// Extract field names from `emit` statements in CXL source.
/// Looks for lines matching `emit <name> = ...`
fn extract_emit_names(cxl: &str) -> Vec<String> {
    cxl.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("emit ") {
                let rest = &trimmed[5..].trim_start();
                // Extract the name before the `=`
                rest.split('=').next()
                    .map(|name| name.trim().to_string())
                    .filter(|name| !name.is_empty())
            } else {
                None
            }
        })
        .collect()
}
