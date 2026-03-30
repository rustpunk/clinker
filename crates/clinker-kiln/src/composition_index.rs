/// Composition index: discovers and indexes all `.comp.yaml` files in a workspace.
///
/// Scans the `compositions/` directory, parses each file, and collects metadata
/// for the composition browser panel.

use std::path::PathBuf;

use clinker_core::composition::{load_composition, CompositionMeta, Contract};

use crate::workspace::Workspace;

/// Index of all discovered compositions in the workspace.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompositionIndex {
    pub compositions: Vec<CompositionEntry>,
}

/// A single composition entry with parsed metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositionEntry {
    pub path: PathBuf,
    pub meta: CompositionMeta,
    pub transform_count: usize,
    pub contract: Option<Contract>,
    pub used_by: Vec<PathBuf>,
}

impl CompositionIndex {
    pub fn len(&self) -> usize {
        self.compositions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.compositions.is_empty()
    }
}

/// Build composition index by scanning the compositions directory.
pub fn build_composition_index(workspace: &Workspace) -> CompositionIndex {
    // Get compositions directory from manifest or default to "compositions"
    let comp_dir = workspace.root.join("compositions");
    if !comp_dir.exists() {
        return CompositionIndex::default();
    }

    let mut entries = Vec::new();
    // Scan for .comp.yaml files
    if let Ok(dir) = std::fs::read_dir(&comp_dir) {
        for dir_entry in dir.flatten() {
            let path = dir_entry.path();
            if path.extension().is_some_and(|e| e == "yaml")
                && path
                    .file_stem()
                    .is_some_and(|s| s.to_string_lossy().ends_with(".comp"))
            {
                if let Ok(comp) = load_composition(&path) {
                    let transform_count = comp.transformations.len();
                    let contract = comp.compose.contract.clone();
                    entries.push(CompositionEntry {
                        path,
                        meta: comp.compose,
                        transform_count,
                        contract,
                        used_by: Vec::new(), // TODO: scan pipelines for _import refs
                    });
                }
            }
        }
    }

    entries.sort_by(|a, b| a.meta.name.cmp(&b.meta.name));
    CompositionIndex { compositions: entries }
}
