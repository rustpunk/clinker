use clinker_core::config::PipelineConfig;
use dioxus::prelude::*;

use crate::sync::EditSource;

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

/// App-level reactive state — struct of independent Signal handles (AP-4).
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
