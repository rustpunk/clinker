use std::cell::RefCell;
use std::rc::Rc;

use dioxus::html::geometry::WheelDelta;
use dioxus::prelude::*;

use crate::pipeline_view::{derive_pipeline_view, derive_partial_pipeline_view, derive_composition_drill_view};
use crate::state::use_app_state;

use super::breadcrumb::BreadcrumbBar;
use super::connector::Connector;
use super::group_frame::CompositionGroupFrame;
use super::node::CanvasNode;

// ── Canvas transform constants ───────────────────────────────────────────────
const ZOOM_MIN: f32 = 0.25;
const ZOOM_MAX: f32 = 4.0;
/// Zoom factor applied per scroll-wheel "tick" (for Line/Page delta modes).
const ZOOM_STEP_LINE: f32 = 1.10;
/// Zoom factor per pixel of scroll delta (for Pixel mode).
const ZOOM_STEP_PIXEL: f32 = 0.001;

// ── Drag state — non-reactive, stored in Rc<RefCell<>> to avoid a signal write
// (and therefore a re-render) on every pointer-move event. ───────────────────
#[derive(Default)]
struct DragState {
    /// Whether a pan drag is currently active.
    active: bool,
    /// Client-space X where the drag began.
    start_x: f32,
    /// Client-space Y where the drag began.
    start_y: f32,
    /// Pan X at drag start — restored as offset when computing current pan.
    start_pan_x: f32,
    /// Pan Y at drag start.
    start_pan_y: f32,
}

/// The infinite-canvas panel rendering the pipeline node graph.
///
/// Pan: left-click-drag anywhere on the canvas background.
/// Zoom: scroll wheel (zoom anchored to cursor position, range 25 %–400 %).
/// Fit-to-view: double-click empty canvas (Phase 3, stub here as reset to origin).
///
/// Visual layers (back to front):
///   1. Dot grid background (CSS radial-gradient, does NOT transform with content)
///   2. Noise + scanline overlays (CSS ::before / ::after, fixed to panel)
///   3. `kiln-canvas-viewport` div — receives CSS transform(translate + scale)
///      a. SVG connector overlay (absolute, inset: 0, overflow: visible)
///      b. Node cards (absolute, world-space coordinates)
///
/// Doc: spec §4.1 — Viewport.
#[component]
pub fn CanvasPanel() -> Element {
    let state = use_app_state();

    // Derive canvas stages from the pipeline model, falling back to partial parse.
    let compositions_read = (state.compositions).read();
    let expanded = (state.expanded_compositions)();
    let drill_stack = (state.composition_drill_stack)();
    let is_drilled = !drill_stack.is_empty();

    let pipeline_view = if is_drilled {
        // Drill-in: render the top composition's transforms
        let drill_entry = drill_stack.last().unwrap();
        match &*(state.pipeline).read() {
            Some(config) => derive_composition_drill_view(config, &compositions_read, &drill_entry.path),
            None => crate::pipeline_view::PipelineView {
                stages: Vec::new(),
                composition_groups: Vec::new(),
            },
        }
    } else {
        // Normal pipeline view
        match &*(state.pipeline).read() {
            Some(config) => derive_pipeline_view(config, &compositions_read, &expanded),
            None => match &*(state.partial_pipeline).read() {
                Some(partial) => derive_partial_pipeline_view(partial),
                None => crate::pipeline_view::PipelineView {
                    stages: Vec::new(),
                    composition_groups: Vec::new(),
                },
            },
        }
    };
    let stages = pipeline_view.stages;
    let composition_groups = pipeline_view.composition_groups;
    let connections: Vec<_> = stages.windows(2).map(|w| (w[0].clone(), w[1].clone())).collect();

    // ── Transform state (local — only the canvas needs these) ────────────────
    let mut pan_x = use_signal(|| 0.0_f32);
    let mut pan_y = use_signal(|| 0.0_f32);
    let mut zoom = use_signal(|| 1.0_f32);

    // Reset pan/zoom when entering or leaving drill-in so the content is centered.
    let drill_depth = drill_stack.len();
    let mut prev_drill_depth = use_signal(|| 0_usize);
    if drill_depth != *prev_drill_depth.peek() {
        prev_drill_depth.set(drill_depth);
        pan_x.set(0.0);
        pan_y.set(0.0);
        zoom.set(1.0);
    }

    // ── Non-reactive drag state — hot path, no re-renders during drag ─────────
    let drag = use_hook(|| Rc::new(RefCell::new(DragState::default())));

    // ── Event handler closures ────────────────────────────────────────────────

    let drag_down = {
        let drag = drag.clone();
        move |e: MouseEvent| {
            // Only initiate pan on left-button (button 0) or middle-button (1).
            // Right-click is reserved for the future context menu (Phase 3).
            if e.trigger_button() == Some(dioxus::html::input_data::MouseButton::Primary)
                || e.trigger_button() == Some(dioxus::html::input_data::MouseButton::Auxiliary)
            {
                let pos = e.client_coordinates();
                let mut d = drag.borrow_mut();
                d.active = true;
                d.start_x = pos.x as f32;
                d.start_y = pos.y as f32;
                d.start_pan_x = *pan_x.peek();
                d.start_pan_y = *pan_y.peek();
            }
        }
    };

    let drag_move = {
        let drag = drag.clone();
        move |e: MouseEvent| {
            let d = drag.borrow();
            if d.active {
                let pos = e.client_coordinates();
                let dx = pos.x as f32 - d.start_x;
                let dy = pos.y as f32 - d.start_y;
                pan_x.set(d.start_pan_x + dx);
                pan_y.set(d.start_pan_y + dy);
            }
        }
    };

    let drag_up = {
        let drag = drag.clone();
        move |_: MouseEvent| {
            drag.borrow_mut().active = false;
        }
    };

    let on_wheel = move |e: WheelEvent| {
        // Compute a zoom multiplier from the wheel delta.
        // Positive delta_y = scroll down = zoom out (< 1).
        // Negative delta_y = scroll up   = zoom in  (> 1).
        let factor = match e.delta() {
            WheelDelta::Pixels(data) => {
                let dy = data.y as f32;
                if dy == 0.0 { return; }
                1.0 - dy * ZOOM_STEP_PIXEL
            }
            WheelDelta::Lines(data) => {
                let dy = data.y as f32;
                if dy == 0.0 { return; }
                if dy < 0.0 { ZOOM_STEP_LINE } else { 1.0 / ZOOM_STEP_LINE }
            }
            WheelDelta::Pages(data) => {
                let dy = data.y as f32;
                if dy == 0.0 { return; }
                if dy < 0.0 { ZOOM_STEP_LINE * ZOOM_STEP_LINE } else { 1.0 / (ZOOM_STEP_LINE * ZOOM_STEP_LINE) }
            }
        };

        let old_z = *zoom.peek();
        let new_z = (old_z * factor).clamp(ZOOM_MIN, ZOOM_MAX);
        if (new_z - old_z).abs() < 0.0001 { return; }

        // Anchor zoom to cursor position (cursor stays fixed in world space).
        let cursor = e.client_coordinates();
        let cx = cursor.x as f32;
        let cy = cursor.y as f32;
        let old_px = *pan_x.peek();
        let old_py = *pan_y.peek();
        let ratio = new_z / old_z;

        pan_x.set(cx - (cx - old_px) * ratio);
        pan_y.set(cy - (cy - old_py) * ratio);
        zoom.set(new_z);
    };

    // SVG overlay bounds — large enough to cover the world space used by demo nodes.
    let svg_w = 1200.0_f32;
    let svg_h = 400.0_f32;

    rsx! {
        div {
            class: "kiln-canvas-column",

            // Breadcrumb bar + scope indicator (visible during drill-in)
            if is_drilled {
                BreadcrumbBar {
                    drill_stack: drill_stack.clone(),
                }
            }

            div {
                class: "kiln-canvas-panel",
            // Events on the outer panel — pointer capture would be added in Phase 3.
            onmousedown: drag_down,
            onmousemove: drag_move,
            onmouseup: drag_up,
            // Cancel drag if pointer leaves the panel entirely.
            onmouseleave: move |_| { drag.borrow_mut().active = false; },
            onwheel: on_wheel,
            // Clicking empty canvas deselects any selected node.
            // Node clicks call stop_propagation(), so this only fires on empty space.
            onclick: move |_| {
                let mut sel = state.selected_stage;
                sel.set(None);
            },

            // ── Transformed viewport ──────────────────────────────────────
            div {
                class: "kiln-canvas-viewport",
                style: "transform: translate({pan_x}px, {pan_y}px) scale({zoom});",

                // SVG connector overlay — rendered first (lower z-index).
                svg {
                    class: "kiln-canvas-svg",
                    width: "{svg_w}",
                    height: "{svg_h}",
                    for (from, to) in connections {
                        Connector {
                            key: "{from.id}-{to.id}",
                            from,
                            to,
                        }
                    }
                }

                // Composition group frames (inline-expanded compositions)
                for group in &composition_groups {
                    CompositionGroupFrame {
                        key: "{group.path}",
                        group: group.clone(),
                    }
                }

                // Node cards
                for stage in stages {
                    CanvasNode {
                        key: "{stage.id}",
                        stage,
                    }
                }
            }
        }
        }
    }
}
