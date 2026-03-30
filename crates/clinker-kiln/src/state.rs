/// App-level reactive state and context types.
///
/// `AppState` is the per-tab context consumed by all downstream components.
/// Its shape is unchanged from the single-pipeline era — components don't
/// know about tabs.
///
/// `TabManagerState` is the global context for tab/file operations.

use clinker_core::config::PipelineConfig;
use clinker_schema::SchemaIndex;
use dioxus::prelude::*;

use crate::recent_files::RecentFileEntry;
use crate::sync::EditSource;
use crate::tab::{TabEntry, TabId};
use crate::workspace::Workspace;

/// Which left-side panel is currently open (280px slide-in slot).
///
/// Only one panel can be open at a time. Search and Schemas share the same
/// slot. `None` means the slot is collapsed.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum LeftPanel {
    #[default]
    None,
    Search,
    Schemas,
}

/// Pipeline canvas layout preset.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LayoutPreset {
    CanvasFocus,
    Hybrid,
    EditorFocus,
    Schematics,
}

impl LayoutPreset {
    pub fn as_data_attr(self) -> &'static str {
        match self {
            LayoutPreset::CanvasFocus => "canvas-focus",
            LayoutPreset::Hybrid => "hybrid",
            LayoutPreset::EditorFocus => "editor-focus",
            LayoutPreset::Schematics => "schematics",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            LayoutPreset::CanvasFocus => "Canvas",
            LayoutPreset::Hybrid => "Hybrid",
            LayoutPreset::EditorFocus => "Editor",
            LayoutPreset::Schematics => "Schematic",
        }
    }
}

/// Per-tab reactive state — consumed by canvas, inspector, YAML sidebar, etc.
///
/// Shape is identical to the original single-pipeline `AppState`.
/// Downstream components call `use_context::<AppState>()` and get the
/// active tab's signals transparently.
#[derive(Clone, Copy)]
pub struct AppState {
    pub layout: Signal<LayoutPreset>,
    pub run_log_expanded: Signal<bool>,
    pub selected_stage: Signal<Option<String>>,
    #[allow(dead_code)]
    pub inspector_width: Signal<f32>,
    /// Raw YAML text shown in the sidebar editor.
    pub yaml_text: Signal<String>,
    /// Parsed pipeline config (None if YAML is invalid).
    pub pipeline: Signal<Option<PipelineConfig>>,
    /// Parse error messages (empty when YAML is valid).
    pub parse_errors: Signal<Vec<String>>,
    /// Which view last edited the model (sync loop prevention).
    pub edit_source: Signal<EditSource>,
}

/// Read the current `AppState` from context.
///
/// The context holds a `Signal<AppState>` which is updated when the active
/// tab changes. This helper reads through the signal so callers get the
/// current tab's state.
pub fn use_app_state() -> AppState {
    let sig = use_context::<Signal<AppState>>();
    *sig.read()
}

/// Global tab management context — used by tab bar, title bar, keyboard handlers.
#[derive(Clone, Copy)]
pub struct TabManagerState {
    pub tabs: Signal<Vec<TabEntry>>,
    pub active_tab_id: Signal<Option<TabId>>,
    pub recent_files: Signal<Vec<RecentFileEntry>>,
    pub workspace: Signal<Option<Workspace>>,
    /// Which left panel is currently open (Search or Schemas).
    pub left_panel: Signal<LeftPanel>,
    /// Workspace schema index — populated on workspace load, refreshed on file changes.
    pub schema_index: Signal<SchemaIndex>,
    /// Whether the template gallery overlay is visible.
    pub show_template_gallery: Signal<bool>,
}
