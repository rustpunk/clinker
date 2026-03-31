/// App-level reactive state and context types.
///
/// `AppState` is the per-tab context consumed by all downstream components.
/// Its shape is unchanged from the single-pipeline era — components don't
/// know about tabs.
///
/// `TabManagerState` is the global context for tab/file operations.
///
/// Navigation uses a two-level model:
/// - `NavigationContext` — top-level activity (Pipeline, Channels, Git, Docs, Runs)
/// - `PipelineLayoutMode` — view within Pipeline context only (Canvas, Hybrid, Editor)

use clinker_core::composition::{ContractWarning, RawPipelineConfig, ResolvedComposition};
use clinker_core::config::PipelineConfig;
use clinker_core::partial::PartialPipelineConfig;
use clinker_git::RepoStatus;
use dioxus::prelude::*;
use serde::{Deserialize, Serialize};

use crate::composition_index::CompositionIndex;
use crate::recent_files::RecentFileEntry;
use crate::sync::EditSource;
use crate::tab::{TabEntry, TabId};
use crate::workspace::Workspace;

/// Which left-side panel is currently open (280px slide-in slot).
///
/// Only one panel can be open at a time. Search, Schemas, and Compositions
/// share the same slot. `None` means the slot is collapsed.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum LeftPanel {
    #[default]
    None,
    Search,
    Schemas,
    Compositions,
}

/// Top-level navigation context — the activity the user is performing.
///
/// Each context is a distinct page with its own layout, content, and purpose.
/// Switching contexts changes *what you're doing*. State is preserved per
/// context when switching away.
#[derive(Clone, Copy, PartialEq, Debug, Default, Serialize, Deserialize)]
pub enum NavigationContext {
    /// Pipeline editing — canvas, YAML editor, inspector, compositions.
    #[default]
    Pipeline,
    /// Channel management — identity card, pipeline override grid, health.
    Channels,
    /// Version control — staged/unstaged files, diff view, commit form.
    Git,
    /// Documentation — pipeline docs, transformation summaries, PDF export.
    Docs,
    /// Run history — chronological run list, filterable, expandable entries.
    Runs,
}

impl NavigationContext {
    /// CSS data attribute value for the content area.
    pub fn as_data_attr(self) -> &'static str {
        match self {
            Self::Pipeline => "pipeline",
            Self::Channels => "channels",
            Self::Git => "git",
            Self::Docs => "docs",
            Self::Runs => "runs",
        }
    }

    /// Full display label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Pipeline => "Pipeline",
            Self::Channels => "Channels",
            Self::Git => "Version Control",
            Self::Docs => "Documentation",
            Self::Runs => "Run History",
        }
    }

    /// Short label for the activity bar (max 4 chars).
    pub fn short_label(self) -> &'static str {
        match self {
            Self::Pipeline => "Pipe",
            Self::Channels => "Chan",
            Self::Git => "Git",
            Self::Docs => "Docs",
            Self::Runs => "Runs",
        }
    }

    /// Unicode icon character for the activity bar.
    pub fn icon_char(self) -> &'static str {
        match self {
            Self::Pipeline => "◇",
            Self::Channels => "◈",
            Self::Git => "⟠",
            Self::Docs => "◆",
            Self::Runs => "◎",
        }
    }

    /// Keyboard shortcut hint for display.
    pub fn keyboard_hint(self) -> &'static str {
        match self {
            Self::Pipeline => "Ctrl+Shift+E",
            Self::Channels => "Ctrl+Shift+C",
            Self::Git => "Ctrl+Shift+G",
            Self::Docs => "Ctrl+Shift+D",
            Self::Runs => "Ctrl+Shift+R",
        }
    }

    /// All contexts in display order.
    pub const ALL: [NavigationContext; 5] = [
        Self::Pipeline,
        Self::Channels,
        Self::Git,
        Self::Docs,
        Self::Runs,
    ];
}

/// Pipeline view mode — how you see the pipeline content.
///
/// Only applies within the Pipeline context. Switching layout modes changes
/// *how you see* the pipeline, not what you're doing. All three modes share
/// the same state (selected transformation, cursor position, inspector).
#[derive(Clone, Copy, PartialEq, Debug, Default, Serialize, Deserialize)]
pub enum PipelineLayoutMode {
    /// Canvas takes full width, YAML sidebar hidden.
    Canvas,
    /// Canvas ~62% + YAML sidebar ~38% (360px). Primary authoring mode.
    #[default]
    Hybrid,
    /// YAML editor takes full width, canvas and inspector hidden.
    Editor,
}

impl PipelineLayoutMode {
    /// CSS data attribute value for layout switching.
    pub fn as_data_attr(self) -> &'static str {
        match self {
            Self::Canvas => "canvas",
            Self::Hybrid => "hybrid",
            Self::Editor => "editor",
        }
    }

    /// Display label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Canvas => "Canvas",
            Self::Hybrid => "Hybrid",
            Self::Editor => "Editor",
        }
    }

    /// All layout modes in display order.
    pub const ALL: [PipelineLayoutMode; 3] = [Self::Canvas, Self::Hybrid, Self::Editor];
}

/// Per-tab reactive state — consumed by canvas, inspector, YAML sidebar, etc.
///
/// Downstream components call `use_context::<AppState>()` and get the
/// active tab's signals transparently.
#[derive(Clone, Copy)]
pub struct AppState {
    pub active_context: Signal<NavigationContext>,
    pub pipeline_layout: Signal<PipelineLayoutMode>,
    pub run_log_expanded: Signal<bool>,
    pub selected_stage: Signal<Option<String>>,
    #[allow(dead_code)]
    pub inspector_width: Signal<f32>,
    /// Raw YAML text shown in the sidebar editor.
    pub yaml_text: Signal<String>,
    /// Parsed pipeline config (None if YAML is invalid).
    pub pipeline: Signal<Option<PipelineConfig>>,
    /// Raw pipeline config preserving `_import` directives (for serialization).
    pub raw_pipeline: Signal<Option<RawPipelineConfig>>,
    /// Resolved composition metadata (for canvas rendering and override editing).
    pub compositions: Signal<Vec<ResolvedComposition>>,
    /// Contract validation warnings from composition imports.
    pub contract_warnings: Signal<Vec<ContractWarning>>,
    /// Partial pipeline from graceful degradation (set when full parse fails but YAML syntax is valid).
    pub partial_pipeline: Signal<Option<PartialPipelineConfig>>,
    /// Parse error messages (empty when YAML is valid).
    pub parse_errors: Signal<Vec<String>>,
    /// Which view last edited the model (sync loop prevention).
    pub edit_source: Signal<EditSource>,
    /// Schema validation warnings for the current pipeline.
    pub schema_warnings: Signal<Vec<SchemaWarning>>,
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
    /// Git repository status — branch, ahead/behind, file changes.
    pub git_state: Signal<Option<RepoStatus>>,
    /// Whether the command palette overlay is visible.
    pub show_command_palette: Signal<bool>,
    /// Workspace composition index — populated on workspace load, refreshed on file changes.
    pub composition_index: Signal<CompositionIndex>,
    /// Whether the settings overlay is visible.
    pub show_settings: Signal<bool>,
    /// Whether the activity bar is visible (focus mode toggle).
    pub activity_bar_visible: Signal<bool>,
    /// Navigation history stack for back-navigation (capped at 50).
    pub nav_history: Signal<Vec<NavigationContext>>,
}

use clinker_schema::{SchemaIndex, SchemaWarning};
