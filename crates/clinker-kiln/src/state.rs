use dioxus::prelude::*;

/// Pipeline canvas layout preset.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LayoutPreset {
    CanvasFocus,
    Hybrid,
    EditorFocus,
}

impl LayoutPreset {
    /// CSS `data-layout` attribute value for this preset.
    pub fn as_data_attr(self) -> &'static str {
        match self {
            LayoutPreset::CanvasFocus => "canvas-focus",
            LayoutPreset::Hybrid => "hybrid",
            LayoutPreset::EditorFocus => "editor-focus",
        }
    }

    /// Short label used in the title-bar layout switcher.
    pub fn label(self) -> &'static str {
        match self {
            LayoutPreset::CanvasFocus => "Canvas",
            LayoutPreset::Hybrid => "Hybrid",
            LayoutPreset::EditorFocus => "Editor",
        }
    }
}

/// App-level reactive state — a struct of independent Signal handles, not a
/// `Signal<AppState>`. Each field is a separate signal so that writing to
/// `layout` does not trigger re-renders in components subscribed only to
/// `run_log_expanded`, etc. (avoids the AP-4 monolithic-state anti-pattern.)
///
/// `Signal<T>` is `Copy`, so `AppState` is `Copy` and cheap to pass around.
/// Provided at the app root via `use_context_provider` and consumed in
/// descendant components via `use_context::<AppState>()`.
#[derive(Clone, Copy)]
pub struct AppState {
    /// Current layout preset driving panel widths.
    pub layout: Signal<LayoutPreset>,
    /// Whether the run-log drawer is expanded (220 px) or collapsed (28 px).
    pub run_log_expanded: Signal<bool>,
    /// Whether the node-inspector panel is open (Phase 2+, stubbed in Phase 1).
    #[allow(dead_code)]
    pub inspector_open: Signal<bool>,
    /// Inspector panel width in pixels; range 260–520, default 340.
    #[allow(dead_code)]
    pub inspector_width: Signal<f32>,
}
