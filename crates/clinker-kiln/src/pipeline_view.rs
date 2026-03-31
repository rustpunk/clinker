/// Derives canvas-renderable stage data from a `PipelineConfig`.
///
/// The canvas renders `StageView` nodes, not raw `PipelineConfig` structs.
/// This module converts inputs → source nodes, transforms → transform nodes,
/// outputs → sink nodes, and auto-layouts them horizontally.
///
/// Compositions are rendered in two modes:
/// - **Inline** (≤4 transforms): individual transform nodes with `from_composition` metadata
///   and a `CompositionGroup` boundary marker.
/// - **Collapsed** (5+ transforms): a single `StageKind::Composition` node that can drill in.

use clinker_core::composition::{CompositionOrigin, ResolvedComposition};
use clinker_core::config::PipelineConfig;

/// Node height constant (matches CSS `.kiln-node` rendered height).
pub const NODE_HEIGHT: f32 = 92.0;
/// Node width constant (matches CSS `.kiln-node` width).
pub const NODE_WIDTH: f32 = 160.0;

/// What kind of pipeline stage this node represents.
#[derive(Clone, Debug, PartialEq)]
pub enum StageKind {
    Source,
    Transform,
    Output,
    /// A collapsed composition node (5+ transforms).
    Composition(CompositionNodeMeta),
    /// A node that failed to parse — shown as a red error placeholder.
    Error,
}

impl StageKind {
    /// Rustpunk accent colour hex for this stage kind.
    pub fn accent_color(&self) -> &'static str {
        match self {
            StageKind::Source => "#43B3AE",         // verdigris
            StageKind::Transform => "#C75B2A",      // ember
            StageKind::Output => "#B7410E",         // oxide-red
            StageKind::Composition(_) => "#43B3AE", // verdigris (same as source)
            StageKind::Error => "#CC3333",          // danger red
        }
    }

    /// Short badge label for the node card.
    pub fn badge_label(&self) -> &'static str {
        match self {
            StageKind::Source => "SOURCE",
            StageKind::Transform => "TRANSFORM",
            StageKind::Output => "OUTPUT",
            StageKind::Composition(_) => "COMPOSITION",
            StageKind::Error => "ERROR",
        }
    }
}

/// Metadata for a collapsed composition node on the canvas.
#[derive(Clone, Debug, PartialEq)]
pub struct CompositionNodeMeta {
    /// Path to the `.comp.yaml` file (relative to workspace root).
    pub path: String,
    /// Display name from composition metadata.
    pub name: String,
    /// Version string from composition metadata.
    pub version: Option<String>,
    /// Number of transformations in this composition.
    pub transform_count: usize,
    /// Number of overrides applied from the `_import` directive.
    pub override_count: usize,
}

/// Inline composition group boundary — marks a range of stages that came
/// from the same composition (≤4 transforms, rendered inline).
#[derive(Clone, Debug, PartialEq)]
pub struct CompositionGroup {
    /// Composition path.
    pub path: String,
    /// Display name.
    pub name: String,
    /// Version string.
    pub version: Option<String>,
    /// Number of overrides.
    pub override_count: usize,
    /// Canvas X position of the group boundary (left edge of first node).
    pub x: f32,
    /// Canvas Y position of the group boundary.
    pub y: f32,
    /// Total width of the group (all nodes + gaps).
    pub width: f32,
    /// Index range of stages in the `StageView` vec that belong to this group.
    pub stage_range: (usize, usize),
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
    /// If this transform came from a composition (for grouping, badges, and override editing).
    pub from_composition: Option<CompositionOrigin>,
    /// Error message for `StageKind::Error` nodes (partial parse failures).
    pub error_message: Option<String>,
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

/// Maximum transforms in a composition that can be inline-expanded.
/// Compositions with more than this many transforms use drill-in instead.
pub const INLINE_THRESHOLD: usize = 4;

/// Result of deriving a pipeline view with composition awareness.
pub struct PipelineView {
    pub stages: Vec<StageView>,
    pub composition_groups: Vec<CompositionGroup>,
}

/// Derive canvas nodes from a `PipelineConfig` with composition metadata.
///
/// Layout: inputs first, then transforms in order, then outputs.
/// Horizontal left-to-right, evenly spaced, with slight Y stagger.
///
/// All compositions are collapsed by default. Compositions whose path is in
/// `expanded` are rendered inline with a group boundary.
pub fn derive_pipeline_view(
    config: &PipelineConfig,
    compositions: &[ResolvedComposition],
    expanded: &std::collections::HashSet<String>,
) -> PipelineView {
    let mut stages = Vec::new();
    let mut groups = Vec::new();
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
            from_composition: None,
            error_message: None,
        });
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    // Transforms → Transform nodes (with composition awareness)
    let mut transform_idx = 0;
    while transform_idx < config.transformations.len() {
        let transform = &config.transformations[transform_idx];

        // Check if this transform belongs to a composition
        if let Some((comp, origin_idx)) = find_composition(transform, compositions) {
            if expanded.contains(&comp.path) {
                // Inline-expanded: emit individual nodes inside a group boundary
                let group_start_idx = stages.len();
                let group_start_x = x;

                for i in 0..comp.transform_names.len() {
                    let t = &config.transformations[transform_idx + i];
                    let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
                    let subtitle = cxl_subtitle(&t.cxl);
                    let origin = comp.origins.get(origin_idx + i).cloned();

                    stages.push(StageView {
                        id: t.name.clone(),
                        label: t.name.clone(),
                        kind: StageKind::Transform,
                        subtitle,
                        canvas_x: x,
                        canvas_y: y,
                        cxl_source: Some(t.cxl.clone()),
                        description: t.description.clone(),
                        from_composition: origin,
                        error_message: None,
                    });
                    x += NODE_WIDTH + NODE_GAP;
                    idx += 1;
                }

                let group_end_idx = stages.len() - 1;
                let group_width =
                    (comp.transform_names.len() as f32) * (NODE_WIDTH + NODE_GAP) - NODE_GAP;

                groups.push(CompositionGroup {
                    path: comp.path.clone(),
                    name: comp.meta.name.clone(),
                    version: comp.meta.version.clone(),
                    override_count: comp.override_count,
                    x: group_start_x,
                    y: BASE_Y - 20.0,
                    width: group_width,
                    stage_range: (group_start_idx, group_end_idx),
                });

                transform_idx += comp.transform_names.len();
            } else {
                // Collapsed: single composition node
                let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
                stages.push(StageView {
                    id: format!("_comp_{}", comp.path),
                    label: comp.meta.name.clone(),
                    kind: StageKind::Composition(CompositionNodeMeta {
                        path: comp.path.clone(),
                        name: comp.meta.name.clone(),
                        version: comp.meta.version.clone(),
                        transform_count: comp.transform_names.len(),
                        override_count: comp.override_count,
                    }),
                    subtitle: format!(
                        "{} transforms{}",
                        comp.transform_names.len(),
                        if comp.override_count > 0 {
                            format!(", {} override(s)", comp.override_count)
                        } else {
                            String::new()
                        }
                    ),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: comp.meta.description.clone(),
                    from_composition: None,
                    error_message: None,
                });
                x += NODE_WIDTH + NODE_GAP;
                idx += 1;
                transform_idx += comp.transform_names.len();
            }
        } else {
            // Regular inline transform
            let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
            let subtitle = cxl_subtitle(&transform.cxl);
            stages.push(StageView {
                id: transform.name.clone(),
                label: transform.name.clone(),
                kind: StageKind::Transform,
                subtitle,
                canvas_x: x,
                canvas_y: y,
                cxl_source: Some(transform.cxl.clone()),
                description: transform.description.clone(),
                from_composition: None,
                error_message: None,
            });
            x += NODE_WIDTH + NODE_GAP;
            idx += 1;
            transform_idx += 1;
        }
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
            from_composition: None,
            error_message: None,
        });
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    PipelineView {
        stages,
        composition_groups: groups,
    }
}

/// Derive a drill-in view for a composition — renders its transforms as
/// top-level stages for the canvas to display.
pub fn derive_composition_drill_view(
    config: &PipelineConfig,
    compositions: &[ResolvedComposition],
    composition_path: &str,
) -> PipelineView {
    let mut stages = Vec::new();
    let mut x = LEFT_MARGIN;

    // Find the composition and its transforms
    let comp = compositions.iter().find(|c| c.path == composition_path);
    let Some(comp) = comp else {
        return PipelineView { stages: Vec::new(), composition_groups: Vec::new() };
    };

    let transforms: Vec<_> = config.transforms().collect();

    // Find the first transform in this composition
    let start_idx = transforms.iter().position(|t| {
        comp.transform_names.first().map(|n| n == &t.name).unwrap_or(false)
    });

    if let Some(start) = start_idx {
        for (i, name) in comp.transform_names.iter().enumerate() {
            let t_idx = start + i;
            if t_idx >= transforms.len() { break; }
            let t = transforms[t_idx];
            if t.name != *name { break; }

            let y = BASE_Y + if i % 2 == 0 { 0.0 } else { STAGGER_Y };
            let subtitle = cxl_subtitle(&t.cxl);
            let origin = comp.origins.get(i).cloned();

            stages.push(StageView {
                id: t.name.clone(),
                label: t.name.clone(),
                kind: StageKind::Transform,
                subtitle,
                canvas_x: x,
                canvas_y: y,
                cxl_source: Some(t.cxl.clone()),
                description: t.description.clone(),
                from_composition: origin,
                error_message: None,
            });
            x += NODE_WIDTH + NODE_GAP;
        }
    }

    PipelineView { stages, composition_groups: Vec::new() }
}

/// Find which composition (if any) a transform belongs to, and its index within that composition.
fn find_composition<'a>(
    transform: &clinker_core::config::TransformConfig,
    compositions: &'a [ResolvedComposition],
) -> Option<(&'a ResolvedComposition, usize)> {
    for comp in compositions {
        if let Some(idx) = comp
            .transform_names
            .iter()
            .position(|n| n == &transform.name)
        {
            // Verify by checking the origin's transform name matches
            if let Some(origin) = comp.origins.get(idx) {
                if origin.transform_name == transform.name {
                    return Some((comp, idx));
                }
            }
        }
    }
    None
}

/// Derive canvas nodes from a `PartialPipelineConfig` (graceful degradation).
///
/// Renders successfully-parsed items as normal nodes and failed items as
/// `StageKind::Error` nodes. Composition grouping is skipped — import
/// directives are rendered as plain transform nodes.
pub fn derive_partial_pipeline_view(
    partial: &clinker_core::partial::PartialPipelineConfig,
) -> PipelineView {
    use clinker_core::composition::RawTransformEntry;
    use clinker_core::partial::PartialItem;

    let mut stages = Vec::new();
    let mut x = LEFT_MARGIN;
    let mut idx = 0;

    // Helper to compute staggered Y
    let y_for = |i: usize| BASE_Y + if i % 2 == 0 { 0.0 } else { STAGGER_Y };

    // Inputs
    for item in &partial.inputs {
        let y = y_for(idx);
        match item {
            PartialItem::Ok(input) => {
                stages.push(StageView {
                    id: input.name.clone(),
                    label: input.name.clone(),
                    kind: StageKind::Source,
                    subtitle: input.path.clone(),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: None,
                });
            }
            PartialItem::Err { index, message } => {
                stages.push(StageView {
                    id: format!("_err_input_{index}"),
                    label: format!("input #{}", index + 1),
                    kind: StageKind::Error,
                    subtitle: truncate_error(message),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: Some(message.clone()),
                });
            }
        }
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    // Transformations
    for item in &partial.transformations {
        let y = y_for(idx);
        match item {
            PartialItem::Ok(entry) => match entry {
                RawTransformEntry::Inline(t) => {
                    let subtitle = cxl_subtitle(&t.cxl);
                    stages.push(StageView {
                        id: t.name.clone(),
                        label: t.name.clone(),
                        kind: StageKind::Transform,
                        subtitle,
                        canvas_x: x,
                        canvas_y: y,
                        cxl_source: Some(t.cxl.clone()),
                        description: t.description.clone(),
                        from_composition: None,
                        error_message: None,
                    });
                }
                RawTransformEntry::Import(directive) => {
                    stages.push(StageView {
                        id: format!("_import_{}", directive.path),
                        label: directive.path.clone(),
                        kind: StageKind::Transform,
                        subtitle: format!("_import: {}", directive.path),
                        canvas_x: x,
                        canvas_y: y,
                        cxl_source: None,
                        description: None,
                        from_composition: None,
                        error_message: None,
                    });
                }
            },
            PartialItem::Err { index, message } => {
                stages.push(StageView {
                    id: format!("_err_transform_{index}"),
                    label: format!("transform #{}", index + 1),
                    kind: StageKind::Error,
                    subtitle: truncate_error(message),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: Some(message.clone()),
                });
            }
        }
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    // Outputs
    for item in &partial.outputs {
        let y = y_for(idx);
        match item {
            PartialItem::Ok(output) => {
                stages.push(StageView {
                    id: output.name.clone(),
                    label: output.name.clone(),
                    kind: StageKind::Output,
                    subtitle: output.path.clone(),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: None,
                });
            }
            PartialItem::Err { index, message } => {
                stages.push(StageView {
                    id: format!("_err_output_{index}"),
                    label: format!("output #{}", index + 1),
                    kind: StageKind::Error,
                    subtitle: truncate_error(message),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: Some(message.clone()),
                });
            }
        }
        x += NODE_WIDTH + NODE_GAP;
        idx += 1;
    }

    PipelineView {
        stages,
        composition_groups: Vec::new(),
    }
}

/// Truncate an error message for display as a node subtitle.
fn truncate_error(msg: &str) -> String {
    if msg.len() <= 40 {
        msg.to_string()
    } else {
        format!("{}...", &msg[..37])
    }
}

/// Extract first non-empty line of CXL, truncated to 30 chars.
fn cxl_subtitle(cxl: &str) -> String {
    cxl.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .chars()
        .take(30)
        .collect()
}
