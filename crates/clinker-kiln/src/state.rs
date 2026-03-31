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

use std::collections::HashSet;
use std::path::PathBuf;

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
    /// Channel-resolved pipeline (None when no channel is active).
    pub channel_pipeline: Signal<Option<crate::channel_resolve::ChannelResolution>>,
    /// Current view mode when a channel is active (Base/Resolved/Diff).
    pub channel_view_mode: Signal<ChannelViewMode>,
    /// Composition paths that are currently inline-expanded on the canvas.
    pub expanded_compositions: Signal<HashSet<String>>,
    /// Drill-in stack for navigating into large compositions (5+ transforms).
    pub composition_drill_stack: Signal<Vec<DrillInEntry>>,
}

/// An entry in the composition drill-in navigation stack.
#[derive(Clone, Debug, PartialEq)]
pub struct DrillInEntry {
    /// Path to the `.comp.yaml` file.
    pub path: String,
    /// Display name of the composition.
    pub name: String,
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
    /// Channel state discovered from clinker.toml (None if no clinker.toml found).
    pub channel_state: Signal<Option<ChannelState>>,
}

use clinker_schema::{SchemaIndex, SchemaWarning};

// ── Channel state ──────────────────────────────────────────────────────

/// Discovered channel workspace state. Extracted from `clinker-channel`
/// types into Clone + PartialEq structs safe for Dioxus signals.
///
/// Populated when a workspace has a `clinker.toml` with a channels directory.
/// None when no `clinker.toml` is found (channel features degrade gracefully).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelState {
    /// Root directory containing clinker.toml.
    pub workspace_root: PathBuf,
    /// Channel directory name (from clinker.toml `[workspace] channels_dir`).
    pub channels_dir: String,
    /// Group directory name (from clinker.toml `[workspace] groups_dir`).
    pub groups_dir: String,
    /// Default channel ID (from clinker.toml `[workspace] default_channel`).
    pub default_channel: Option<String>,
    /// Whether the channels directory exists on disk.
    pub dir_exists: bool,
    /// Discovered channels with summary metadata.
    pub channels: Vec<ChannelSummary>,
    /// Discovered groups with summary metadata.
    pub groups: Vec<GroupSummary>,
    /// Currently selected channel ID (None = run base pipeline).
    pub active_channel: Option<String>,
    /// Recently selected channel IDs (most recent first, max 10).
    pub recent_channels: Vec<String>,
}

impl ChannelState {
    /// Get the summary for the active channel, if any.
    pub fn active_summary(&self) -> Option<&ChannelSummary> {
        self.active_channel.as_ref().and_then(|id| {
            self.channels.iter().find(|c| c.id == *id)
        })
    }

    /// All unique tier names across discovered channels.
    pub fn tier_names(&self) -> Vec<String> {
        let mut tiers: Vec<String> = self.channels
            .iter()
            .filter_map(|c| c.tier.clone())
            .collect();
        tiers.sort();
        tiers.dedup();
        tiers
    }

    /// Set active channel and push to recent list (max 10, deduped).
    pub fn select_channel(&mut self, id: Option<String>) {
        if let Some(ref channel_id) = id {
            self.recent_channels.retain(|r| r != channel_id);
            self.recent_channels.insert(0, channel_id.clone());
            self.recent_channels.truncate(10);
        }
        self.active_channel = id;
    }
}

/// Summary of a discovered channel (extracted from `ChannelManifest`).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelSummary {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub contact: Option<String>,
    pub tier: Option<String>,
    pub tags: Vec<String>,
    pub active: bool,
    /// Group IDs this channel inherits from.
    pub inherits: Vec<String>,
    /// Number of `.channel.yaml` override files in this channel's directory.
    pub override_count: usize,
    /// Number of variables defined in `channel.yaml`.
    pub variable_count: usize,
}

/// Summary of a discovered group (from `_groups/` directory).
#[derive(Clone, Debug, PartialEq)]
pub struct GroupSummary {
    pub id: String,
    /// Number of `.channel.yaml` override files in this group's directory.
    pub override_count: usize,
    /// Channel IDs that inherit from this group.
    pub inheritor_ids: Vec<String>,
}

/// Which view mode to show when a channel is active in Pipeline context.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum ChannelViewMode {
    /// Show the base pipeline (no overrides applied).
    #[default]
    Base,
    /// Show the resolved pipeline (base + channel overrides merged).
    Resolved,
    /// Show/edit the channel override `.channel.yaml` file.
    Channel,
}

impl ChannelViewMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Base => "Base",
            Self::Resolved => "Resolved",
            Self::Channel => "Channel",
        }
    }

    pub const ALL: [ChannelViewMode; 3] = [Self::Base, Self::Resolved, Self::Channel];
}