/// Per-tab data model: each open pipeline gets its own signals and file state.
///
/// `TabEntry` holds the reactive signals that downstream components read via
/// `AppState` context. When the user switches tabs, `ActiveTabContent` remounts
/// with the new tab's signals — no double-indirection needed.

use std::fmt;
use std::path::{Path, PathBuf};

use clinker_core::config::PipelineConfig;
use dioxus::prelude::*;
use uuid::Uuid;

use crate::file_ops::compute_hash;
use crate::sync::{parse_yaml, EditSource};

/// Scaffold YAML for new untitled pipelines.
const SCAFFOLD_YAML: &str = r#"source:
  format: csv
  path: ""
stages: []
sink:
  format: csv
  path: ""
"#;

/// Stable identity for a tab — survives reordering and state changes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TabId(Uuid);

impl TabId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for TabId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One open pipeline tab with its own reactive signals and file state.
///
/// The signal handles here are the same types that `AppState` exposes.
/// When this tab is active, `ActiveTabContent` provides these via context.
pub struct TabEntry {
    pub id: TabId,
    /// `None` for unsaved / untitled tabs.
    pub file_path: Option<PathBuf>,
    /// Display name for untitled tabs (e.g. "untitled.yaml", "untitled-2.yaml").
    untitled_name: Option<String>,
    /// Raw YAML text shown in the sidebar editor.
    pub yaml_text: Signal<String>,
    /// Parsed pipeline config (`None` if YAML is invalid).
    pub pipeline: Signal<Option<PipelineConfig>>,
    /// Parse error messages (empty when valid).
    pub parse_errors: Signal<Vec<String>>,
    /// Which view last edited the model (sync-loop prevention).
    pub edit_source: Signal<EditSource>,
    /// Currently selected stage on canvas / inspector.
    pub selected_stage: Signal<Option<String>>,
    /// Blake3 hash of the YAML at last save/open. `None` for never-saved tabs.
    content_hash: Option<[u8; 32]>,
}

impl TabEntry {
    /// Create a new untitled tab with scaffold YAML.
    ///
    /// Pass the current list of tabs to generate a unique display name
    /// (untitled.yaml, untitled-2.yaml, untitled-3.yaml, ...).
    pub fn new_untitled(existing_tabs: &[TabEntry]) -> Self {
        // Count existing untitled tabs to pick the next number
        let untitled_count = existing_tabs
            .iter()
            .filter(|t| t.file_path.is_none())
            .count();

        let name = if untitled_count == 0 {
            "untitled.yaml".to_string()
        } else {
            format!("untitled-{}.yaml", untitled_count + 1)
        };

        Self {
            id: TabId::new(),
            file_path: None,
            untitled_name: Some(name),
            yaml_text: Signal::new(SCAFFOLD_YAML.to_string()),
            pipeline: Signal::new(parse_yaml(SCAFFOLD_YAML).ok()),
            parse_errors: Signal::new(Vec::new()),
            edit_source: Signal::new(EditSource::None),
            selected_stage: Signal::new(None),
            content_hash: None, // never saved → always dirty
        }
    }

    /// Create a tab from a file on disk.
    pub fn from_file(path: PathBuf, yaml: String) -> Self {
        let hash = compute_hash(&yaml);
        let pipeline = parse_yaml(&yaml);
        let (config, errors) = match pipeline {
            Ok(c) => (Some(c), Vec::new()),
            Err(e) => (None, e),
        };

        Self {
            id: TabId::new(),
            file_path: Some(path),
            untitled_name: None,
            yaml_text: Signal::new(yaml),
            pipeline: Signal::new(config),
            parse_errors: Signal::new(errors),
            edit_source: Signal::new(EditSource::None),
            selected_stage: Signal::new(None),
            content_hash: Some(hash),
        }
    }

    /// Create a tab from a file, pre-loading with the demo YAML.
    pub fn new_demo(yaml: &str) -> Self {
        let pipeline = parse_yaml(yaml);
        let (config, errors) = match pipeline {
            Ok(c) => (Some(c), Vec::new()),
            Err(e) => (None, e),
        };

        Self {
            id: TabId::new(),
            file_path: None,
            untitled_name: Some("demo.yaml".to_string()),
            yaml_text: Signal::new(yaml.to_string()),
            pipeline: Signal::new(config),
            parse_errors: Signal::new(errors),
            edit_source: Signal::new(EditSource::None),
            selected_stage: Signal::new(None),
            content_hash: None,
        }
    }

    /// Whether the tab has unsaved changes.
    ///
    /// Untitled tabs (no `content_hash`) are always dirty.
    /// Saved tabs compare current YAML against the last-saved hash.
    pub fn is_dirty(&self) -> bool {
        let Some(saved_hash) = self.content_hash else {
            return true;
        };
        let current = compute_hash(&(self.yaml_text)());
        current != saved_hash
    }

    /// Mark the current YAML as saved (updates the content hash).
    pub fn mark_saved(&mut self, path: PathBuf) {
        self.content_hash = Some(compute_hash(&(self.yaml_text)()));
        self.file_path = Some(path);
    }

    /// Display name for the tab label.
    pub fn display_name(&self) -> String {
        match &self.file_path {
            Some(p) => p
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "untitled.yaml".to_string()),
            None => self
                .untitled_name
                .clone()
                .unwrap_or_else(|| "untitled.yaml".to_string()),
        }
    }

    /// Full file path as a string (for tooltips).
    pub fn full_path(&self) -> Option<String> {
        self.file_path.as_ref().map(|p| p.display().to_string())
    }
}
