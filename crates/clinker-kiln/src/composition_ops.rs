/// Extract-as-composition logic: pull consecutive transforms from a pipeline
/// into a reusable `.comp.yaml` file and replace them with an `_import` directive.
use std::path::{Path, PathBuf};

use indexmap::IndexMap;

use clinker_core::composition::{
    CompositionConfig, CompositionMeta, ImportDirective, RawPipelineConfig, RawTransformEntry,
};

/// Result of extracting transforms into a composition.
#[allow(dead_code)]
pub struct ExtractionResult {
    pub composition_yaml: String,
    pub composition_path: PathBuf,
    pub updated_pipeline: RawPipelineConfig,
}

/// Extract consecutive transforms from a pipeline into a `.comp.yaml` file.
///
/// The selected transforms must be consecutive in the pipeline's transformations
/// array. They are removed and replaced with a single `_import` directive.
#[allow(dead_code)]
pub fn extract_composition(
    raw: &RawPipelineConfig,
    selected_names: &[String],
    comp_name: &str,
    comp_description: &str,
    comp_version: &str,
    workspace_root: &Path,
) -> Result<ExtractionResult, String> {
    // Find indices of selected transforms (must be consecutive)
    let indices: Vec<usize> = selected_names
        .iter()
        .filter_map(|name| {
            raw.transformations
                .iter()
                .position(|entry| matches!(entry, RawTransformEntry::Inline(t) if t.name == *name))
        })
        .collect();

    if indices.is_empty() {
        return Err("No matching transforms found".to_string());
    }

    // Verify consecutive
    for w in indices.windows(2) {
        if w[1] != w[0] + 1 {
            return Err("Selected transforms must be consecutive".to_string());
        }
    }

    // Extract the transforms
    let transforms: Vec<_> = indices
        .iter()
        .filter_map(|&i| match &raw.transformations[i] {
            RawTransformEntry::Inline(t) => Some(t.clone()),
            _ => None,
        })
        .collect();

    // Build composition config
    let comp = CompositionConfig {
        compose: CompositionMeta {
            name: comp_name.to_string(),
            description: Some(comp_description.to_string()),
            version: Some(comp_version.to_string()),
            contract: None, // User can add contract later
        },
        transformations: transforms
            .iter()
            .map(|t| RawTransformEntry::Inline(t.clone()))
            .collect(),
    };

    let composition_yaml = serde_saphyr::to_string(&comp)
        .map_err(|e| format!("Failed to serialize composition: {e}"))?;

    // Build the import directive to replace the extracted transforms
    let slug = comp_name.to_lowercase().replace(' ', "_");
    let relative_path = format!("compositions/{slug}.comp.yaml");
    let composition_path = workspace_root.join(&relative_path);

    let import = RawTransformEntry::Import(ImportDirective {
        path: relative_path,
        overrides: IndexMap::new(),
    });

    // Build updated pipeline: replace the range of indices with the import
    let mut new_transforms = Vec::new();
    let start = indices[0];
    let end = *indices.last().unwrap();

    for (i, entry) in raw.transformations.iter().enumerate() {
        if i == start {
            new_transforms.push(import.clone());
        } else if i > start && i <= end {
            continue; // skip replaced transforms
        } else {
            new_transforms.push(entry.clone());
        }
    }

    let updated_pipeline = RawPipelineConfig {
        pipeline: raw.pipeline.clone(),
        inputs: raw.inputs.clone(),
        outputs: raw.outputs.clone(),
        transformations: new_transforms,
        error_handling: raw.error_handling.clone(),
        notes: raw.notes.clone(),
    };

    Ok(ExtractionResult {
        composition_yaml,
        composition_path,
        updated_pipeline,
    })
}
