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
use std::path::PathBuf;
use std::sync::Arc;

use clinker_core::config::PipelineConfig;
use clinker_core::partial::PartialPipelineConfig;
use clinker_core::plan::CompiledPlan;
use dioxus::prelude::*;
use serde::{Deserialize, Serialize};

use crate::sync::EditSource;
use crate::tab::{TabEntry, TabId};
use crate::workspace::Workspace;

/// Visual theme — switches the entire UI between sub-aesthetics.
///
/// Oxide is the default dark Rustpunk theme (warm charred surfaces).
/// Enamel is a light industrial theme (porcelain-fused-to-steel data plates).
/// The active theme drives CSS custom property overrides via `data-theme` on the root element.
#[derive(Clone, Copy, PartialEq, Debug, Default, Serialize, Deserialize)]
pub enum KilnTheme {
    #[default]
    Oxide,
    Enamel,
}

impl KilnTheme {
    /// CSS `data-theme` attribute value.
    pub fn as_data_attr(self) -> &'static str {
        match self {
            Self::Oxide => "oxide",
            Self::Enamel => "enamel",
        }
    }

    /// Toggle to the opposite theme.
    pub fn toggle(self) -> Self {
        match self {
            Self::Oxide => Self::Enamel,
            Self::Enamel => Self::Oxide,
        }
    }

    /// Parse from persisted string, defaulting to Oxide.
    pub fn from_str_or_default(s: &str) -> Self {
        match s {
            "enamel" => Self::Enamel,
            _ => Self::Oxide,
        }
    }
}

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
    /// Documentation — Kiln user guide, CXL reference, pipeline authoring docs.
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
            Self::Docs => "Technical Guide",
            Self::Runs => "Run History",
        }
    }

    /// Short label for the activity bar (max 4 chars).
    pub fn short_label(self) -> &'static str {
        match self {
            Self::Pipeline => "Pipe",
            Self::Channels => "Chan",
            Self::Git => "Git",
            Self::Docs => "Guide",
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
            Self::Docs => "",
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
    /// Pipeline autodoc — full scrollable documentation view (Blueprint aesthetic).
    Schematics,
}

impl PipelineLayoutMode {
    /// CSS data attribute value for layout switching.
    pub fn as_data_attr(self) -> &'static str {
        match self {
            Self::Canvas => "canvas",
            Self::Hybrid => "hybrid",
            Self::Editor => "editor",
            Self::Schematics => "schematics",
        }
    }

    /// Display label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Canvas => "Canvas",
            Self::Hybrid => "Hybrid",
            Self::Editor => "Editor",
            Self::Schematics => "Schematics",
        }
    }

    /// All layout modes in display order.
    pub const ALL: [PipelineLayoutMode; 4] =
        [Self::Canvas, Self::Hybrid, Self::Editor, Self::Schematics];
}

/// Canvas data source mode — switches between raw pipeline config and
/// channel-resolved compiled plan.
///
/// `Raw` shows the pipeline as authored (PipelineConfig from YAML).
/// `Resolved` shows the pipeline after channel overlay application
/// (CompiledPlan with provenance-tracked config values).
///
/// Orthogonal to composition drill depth — a user can be drilled into
/// a composition at depth 2 and toggle between Raw and Resolved.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum ChannelViewMode {
    /// Show pipeline config from YAML (state.pipeline).
    #[default]
    Raw,
    /// Show resolved config from channel overlay (compiled_plan).
    Resolved,
}

/// A frame in the composition drill-in stack.
///
/// Each frame represents one level of drill-in. The body_id indexes into
/// `CompileArtifacts::composition_bodies` to get the sub-canvas nodes.
#[derive(Clone, Debug, PartialEq)]
pub struct CompositionDrillFrame {
    /// Body ID for the composition's bound body (not an alias prefix string).
    pub body_id: clinker_core::plan::composition_body::CompositionBodyId,
    /// Display label for the breadcrumb (composition alias or name).
    pub alias: String,
    /// Path to the `.comp.yaml` file (for display in breadcrumb tooltip).
    pub use_path: std::path::PathBuf,
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
    /// Raw YAML text shown in the sidebar editor.
    pub yaml_text: Signal<String>,
    /// Parsed pipeline config (None if YAML is invalid).
    pub pipeline: Signal<Option<PipelineConfig>>,
    /// Partial pipeline from graceful degradation.
    pub partial_pipeline: Signal<Option<PartialPipelineConfig>>,
    /// Parse error messages (empty when YAML is valid).
    pub parse_errors: Signal<Vec<String>>,
    /// Which view last edited the model (sync loop prevention).
    pub edit_source: Signal<EditSource>,
    /// Schema validation warnings for the current pipeline.
    pub schema_warnings: Signal<Vec<SchemaWarning>>,
    /// Canvas data source mode (Raw = pipeline config, Resolved = compiled plan).
    pub channel_view_mode: Signal<ChannelViewMode>,
    /// Composition drill-in stack. Empty = top-level pipeline view.
    /// Each frame holds a body ID for rendering the sub-canvas.
    pub composition_drill_stack: Signal<Vec<CompositionDrillFrame>>,
    /// Compiled plan with channel overlay applied. None when no channel is
    /// loaded or in Raw mode. Wrapped in Arc because CompiledPlan is not Clone.
    pub compiled_plan: Signal<Option<Arc<CompiledPlan>>>,
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
    pub workspace: Signal<Option<Workspace>>,
    /// Which left panel is currently open (Search or Schemas).
    pub left_panel: Signal<LeftPanel>,
    /// Workspace schema index — populated on workspace load, refreshed on file changes.
    pub schema_index: Signal<SchemaIndex>,
    /// Whether the template gallery overlay is visible.
    pub show_template_gallery: Signal<bool>,
    /// Git repository status — branch, ahead/behind, file changes.
    /// On web builds, this is always None (git not available in WASM).
    #[cfg(not(target_arch = "wasm32"))]
    pub git_state: Signal<Option<clinker_git::RepoStatus>>,
    #[cfg(target_arch = "wasm32")]
    pub git_state: Signal<Option<()>>,
    /// Whether the command palette overlay is visible.
    pub show_command_palette: Signal<bool>,
    /// Whether the settings overlay is visible.
    pub show_settings: Signal<bool>,
    /// Whether the activity bar is visible (focus mode toggle).
    pub activity_bar_visible: Signal<bool>,
    /// Navigation history stack for back-navigation (capped at 50).
    pub nav_history: Signal<Vec<NavigationContext>>,
    /// Channel state discovered from clinker.toml (None if no clinker.toml found).
    pub channel_state: Signal<Option<ChannelState>>,
    /// Active visual theme (Oxide dark / Enamel light).
    pub theme: Signal<KilnTheme>,
}

use clinker_schema::{SchemaIndex, SchemaWarning};

// ── Channel state ──────────────────────────────────────────────────────

/// Discovered channel workspace state based on the 16c ChannelBinding model.
///
/// Populated when a workspace has `.channel.yaml` files in its channels
/// directory. None when no workspace is loaded or no channels are found.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelState {
    /// Discovered channel binding summaries.
    pub channels: Vec<ChannelBindingSummary>,
    /// Currently selected channel name (None = run base pipeline).
    pub active_channel: Option<String>,
    /// Recently selected channel names (most recent first, max 10).
    pub recent_channels: Vec<String>,
}

/// Summary of a discovered `.channel.yaml` binding file.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelBindingSummary {
    /// Channel name (from the binding's `name` field).
    pub name: String,
    /// Path to the `.channel.yaml` file on disk.
    pub source_path: PathBuf,
    /// Display string for the channel's target (pipeline or composition path).
    pub target: String,
}
