/// Derives canvas-renderable stage data from a `PipelineConfig`.
///
/// The canvas renders `StageView` nodes, not raw `PipelineConfig` structs.
/// This module converts inputs → source nodes, transforms → transform nodes,
/// outputs → sink nodes, and auto-layouts them horizontally.

use clinker_core::config::PipelineConfig;

/// Node height constant (matches CSS `.kiln-node` rendered height).
pub const NODE_HEIGHT: f32 = 92.0;
/// Node width constant (matches CSS `.kiln-node` width).
pub const NODE_WIDTH: f32 = 160.0;

/// What kind of pipeline stage this node represents.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StageKind {
    Source,
    Transform,
    Output,
}

impl StageKind {
    /// Rustpunk accent colour hex for this stage kind.
    pub fn accent_color(self) -> &'static str {
        match self {
            StageKind::Source => "#43B3AE",    // verdigris
            StageKind::Transform => "#C75B2A", // ember
            StageKind::Output => "#B7410E",    // oxide-red
        }
    }

    /// Short badge label for the node card.
    pub fn badge_label(self) -> &'static str {
        match self {
            StageKind::Source => "SOURCE",
            StageKind::Transform => "TRANSFORM",
            StageKind::Output => "OUTPUT",
        }
    }
}

/// A canvas-renderable pipeline stage derived from `PipelineConfig`.
#[derive(Clone, Debug, PartialEq)]
pub struct StageView {
    /// Unique ID (the stage name from the config). Used as RSX key.
    pub id: String,
    /// Display label (same as id for now).
    pub label: String,
    /// What kind of stage (determines accent colour and badge).
    pub kind: StageKind,
    /// Subtitle shown below the label (path for sources/outputs, CXL preview for transforms).
    pub subtitle: String,
    /// Canvas world-space X position.
    pub canvas_x: f32,
    /// Canvas world-space Y position.
    pub canvas_y: f32,
    /// CXL source text (only for Transform stages).
    pub cxl_source: Option<String>,
    /// Optional description from the config.
    pub description: Option<String>,
}

impl StageView {
    /// Centre of the right-side output port — connector source point.
    pub fn port_out(&self) -> (f32, f32) {
        (self.canvas_x + NODE_WIDTH, self.canvas_y + NODE_HEIGHT / 2.0)
    }

    /// Centre of the left-side input port — connector target point.
    pub fn port_in(&self) -> (f32, f32) {
        (self.canvas_x, self.canvas_y + NODE_HEIGHT / 2.0)
    }
}

/// Horizontal spacing between nodes in the auto-layout.
const NODE_GAP: f32 = 80.0;
/// Left margin for the first node.
const LEFT_MARGIN: f32 = 60.0;
/// Base Y position for nodes. Staggered slightly for visual interest.
const BASE_Y: f32 = 120.0;
/// Y stagger amount (alternates +/- for adjacent nodes).
const STAGGER_Y: f32 = 20.0;

/// Derive a flat list of canvas nodes from a `PipelineConfig`.
///
/// Layout: inputs first, then transforms in order, then outputs.
/// Horizontal left-to-right, evenly spaced, with slight Y stagger.
pub fn derive_pipeline_view(config: &PipelineConfig) -> Vec<StageView> {
    let mut stages = Vec::new();
    let mut x = LEFT_MARGIN;
    let mut idx = 0;

    // Inputs → Source nodes
    for input in &config.inputs {
        let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
        stages.push(StageView {
            id: input.name.clone(),
            label: input.name.clone(),
            kind: StageKind::Source,
            subtitle: input.path.clone(),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: None,
        });
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    // Transforms → Transform nodes
    for transform in &config.transformations {
        let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
        // Subtitle: first line of CXL, truncated
        let subtitle = transform
            .cxl
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("")
            .trim()
            .chars()
            .take(30)
            .collect::<String>();
        stages.push(StageView {
            id: transform.name.clone(),
            label: transform.name.clone(),
            kind: StageKind::Transform,
            subtitle,
            canvas_x: x,
            canvas_y: y,
            cxl_source: Some(transform.cxl.clone()),
            description: transform.description.clone(),
        });
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    // Outputs → Output nodes
    for output in &config.outputs {
        let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
        stages.push(StageView {
            id: output.name.clone(),
            label: output.name.clone(),
            kind: StageKind::Output,
            subtitle: output.path.clone(),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: None,
        });
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    stages
}
