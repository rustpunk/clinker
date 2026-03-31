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
        (
            self.canvas_x + NODE_WIDTH,
            self.canvas_y + NODE_HEIGHT / 2.0,
        )
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
/// Vertical gap between stacked output nodes.
const STACK_GAP: f32 = 24.0;
/// Vertical offset for secondary inputs above the transform chain.
const INPUT_Y_OFFSET: f32 = 30.0;

/// Maximum transforms in a composition that can be inline-expanded.
/// Compositions with more than this many transforms use drill-in instead.
pub const INLINE_THRESHOLD: usize = 4;

/// Result of deriving a pipeline view with composition awareness.
pub struct PipelineView {
    pub stages: Vec<StageView>,
    pub composition_groups: Vec<CompositionGroup>,
    /// Explicit connections between stages: `(from_idx, to_idx)` into `stages`.
    pub connections: Vec<(usize, usize)>,
}

/// Derive canvas nodes from a `PipelineConfig` with composition metadata.
///
/// Flowchart layout: transforms form the horizontal spine; inputs float
/// above-left of the first transform that uses them; outputs fan out to the
/// right of the last transform.
///
/// All compositions are collapsed by default. Compositions whose path is in
/// `expanded` are rendered inline with a group boundary.
pub fn derive_pipeline_view(
    config: &PipelineConfig,
    compositions: &[ResolvedComposition],
    expanded: &std::collections::HashSet<String>,
) -> PipelineView {
    let mut transform_stages = Vec::new();
    let mut groups = Vec::new();

    // ── Phase A: Layout transforms (horizontal spine) ───────────────────────
    // Leave room for the primary input at LEFT_MARGIN.
    let transform_x_start = if config.inputs.is_empty() {
        LEFT_MARGIN
    } else {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    };
    let mut x = transform_x_start;
    let mut idx = 0;

    let transforms: Vec<_> = config.transforms().collect();
    let mut transform_idx = 0;
    while transform_idx < transforms.len() {
        let transform = transforms[transform_idx];

        if let Some((comp, origin_idx)) = find_composition(transform, compositions) {
            if expanded.contains(&comp.path) {
                let group_start_idx = transform_stages.len();
                let group_start_x = x;

                for i in 0..comp.transform_names.len() {
                    let t = transforms[transform_idx + i];
                    let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
                    let subtitle = cxl_subtitle(&t.cxl);
                    let origin = comp.origins.get(origin_idx + i).cloned();

                    transform_stages.push(StageView {
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

                let group_end_idx = transform_stages.len() - 1;
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
                    // Will be adjusted after inputs are prepended to stages vec.
                    stage_range: (group_start_idx, group_end_idx),
                });

                transform_idx += comp.transform_names.len();
            } else {
                let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
                transform_stages.push(StageView {
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
            let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
            let subtitle = cxl_subtitle(&transform.cxl);
            transform_stages.push(StageView {
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

    // ── Phase B: Map each input to its target transform ─────────────────────
    // First input → first transform (primary stream).
    // Others → scan transforms for local_window.source match.
    let input_targets = map_inputs_to_transforms(config, &transforms);

    // ── Phase C: Position inputs ────────────────────────────────────────────
    let mut input_stages = Vec::new();
    // Track how many secondary inputs already target each transform (for stacking).
    let mut secondary_stack_count: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();

    for (input_idx, input) in config.inputs.iter().enumerate() {
        let target_t_idx = input_targets[input_idx];

        let (ix, iy) = if input_idx == 0 && !transform_stages.is_empty() {
            // Primary input: on the main line, left of the first transform.
            (LEFT_MARGIN, transform_stages[0].canvas_y)
        } else if !transform_stages.is_empty() && target_t_idx < transform_stages.len() {
            // Secondary input: above-left of its target transform.
            let target = &transform_stages[target_t_idx];
            let stack_n = secondary_stack_count.entry(target_t_idx).or_insert(0);
            let iy = target.canvas_y
                - NODE_HEIGHT
                - INPUT_Y_OFFSET
                - (*stack_n as f32) * (NODE_HEIGHT + STACK_GAP);
            *stack_n += 1;
            (target.canvas_x - NODE_WIDTH - NODE_GAP / 2.0, iy)
        } else {
            // No transforms — just place at LEFT_MARGIN.
            (
                LEFT_MARGIN,
                BASE_Y + (input_idx as f32) * (NODE_HEIGHT + STACK_GAP),
            )
        };

        input_stages.push(StageView {
            id: input.name.clone(),
            label: input.name.clone(),
            kind: StageKind::Source,
            subtitle: input.path.clone(),
            canvas_x: ix,
            canvas_y: iy,
            cxl_source: None,
            description: None,
            from_composition: None,
            error_message: None,
        });
    }

    // ── Phase D: Position outputs ───────────────────────────────────────────
    let mut output_stages = Vec::new();
    let output_x = if transform_stages.is_empty() {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    } else {
        let last = transform_stages.last().unwrap();
        last.canvas_x + NODE_WIDTH + NODE_GAP
    };
    let output_count = config.outputs.len();
    let center_y = BASE_Y + NODE_HEIGHT / 2.0 + STAGGER_Y / 2.0;
    let output_ys = stack_y_positions(output_count, center_y);

    for (i, output) in config.outputs.iter().enumerate() {
        output_stages.push(StageView {
            id: output.name.clone(),
            label: output.name.clone(),
            kind: StageKind::Output,
            subtitle: output.path.clone(),
            canvas_x: output_x,
            canvas_y: output_ys[i],
            cxl_source: None,
            description: None,
            from_composition: None,
            error_message: None,
        });
    }

    // ── Phase E: Build connections ──────────────────────────────────────────
    let input_count = input_stages.len();
    let transform_count = transform_stages.len();
    let connections = build_connections(input_count, &input_targets, transform_count, output_count);

    // ── Phase F: Assemble stages vec (inputs ++ transforms ++ outputs) ──────
    // Adjust composition group stage_range indices (they were relative to
    // transform_stages, now offset by input_count).
    for group in &mut groups {
        group.stage_range.0 += input_count;
        group.stage_range.1 += input_count;
    }

    let mut stages = Vec::with_capacity(input_count + transform_count + output_count);
    stages.extend(input_stages);
    stages.extend(transform_stages);
    stages.extend(output_stages);

    PipelineView {
        stages,
        composition_groups: groups,
        connections,
    }
}

/// Map each input to the index of the transform stage it should connect to.
///
/// First input → index 0 (first transform, primary stream).
/// Other inputs → scan transforms for `local_window.source` match; default to 0.
fn map_inputs_to_transforms(
    config: &PipelineConfig,
    transforms: &[&clinker_core::config::TransformConfig],
) -> Vec<usize> {
    config
        .inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            if i == 0 || transforms.is_empty() {
                return 0;
            }
            // Scan transforms for local_window.source matching this input name.
            transforms
                .iter()
                .position(|t| {
                    t.local_window
                        .as_ref()
                        .and_then(|lw| lw.get("source"))
                        .and_then(|v| v.as_str())
                        == Some(&input.name)
                })
                .unwrap_or(0)
        })
        .collect()
}

/// Compute Y positions for a vertical stack of `count` nodes centered on `center_y`.
fn stack_y_positions(count: usize, center_y: f32) -> Vec<f32> {
    if count == 0 {
        return Vec::new();
    }
    let total_h = count as f32 * NODE_HEIGHT + (count as f32 - 1.0) * STACK_GAP;
    let top = center_y - total_h / 2.0;
    (0..count)
        .map(|i| top + i as f32 * (NODE_HEIGHT + STACK_GAP))
        .collect()
}

/// Build connection pairs given stage layout.
///
/// Stages vec order: `[inputs..., transforms..., outputs...]`.
fn build_connections(
    input_count: usize,
    input_targets: &[usize],
    transform_count: usize,
    output_count: usize,
) -> Vec<(usize, usize)> {
    let mut conns = Vec::new();
    let t_base = input_count; // first transform index in stages vec

    if transform_count > 0 {
        // Each input → its target transform.
        for (i, &target) in input_targets.iter().enumerate() {
            let target_stage = t_base + target.min(transform_count - 1);
            conns.push((i, target_stage));
        }
        // Sequential transform chain.
        for i in 0..transform_count - 1 {
            conns.push((t_base + i, t_base + i + 1));
        }
        // Last transform → each output.
        let last_t = t_base + transform_count - 1;
        let o_base = t_base + transform_count;
        for j in 0..output_count {
            conns.push((last_t, o_base + j));
        }
    } else {
        // No transforms — connect each input to each output directly.
        let o_base = input_count;
        for i in 0..input_count {
            for j in 0..output_count {
                conns.push((i, o_base + j));
            }
        }
    }

    conns
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

    let comp = compositions.iter().find(|c| c.path == composition_path);
    let Some(comp) = comp else {
        return PipelineView {
            stages: Vec::new(),
            composition_groups: Vec::new(),
            connections: Vec::new(),
        };
    };

    let transforms: Vec<_> = config.transforms().collect();

    let start_idx = transforms.iter().position(|t| {
        comp.transform_names
            .first()
            .map(|n| n == &t.name)
            .unwrap_or(false)
    });

    if let Some(start) = start_idx {
        for (i, name) in comp.transform_names.iter().enumerate() {
            let t_idx = start + i;
            if t_idx >= transforms.len() {
                break;
            }
            let t = transforms[t_idx];
            if t.name != *name {
                break;
            }

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

    // Sequential chain connections for drill-in transforms.
    let connections: Vec<_> = (0..stages.len().saturating_sub(1))
        .map(|i| (i, i + 1))
        .collect();

    PipelineView {
        stages,
        composition_groups: Vec::new(),
        connections,
    }
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
            if let Some(origin) = comp.origins.get(idx)
                && origin.transform_name == transform.name
            {
                return Some((comp, idx));
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
///
/// Uses the same flowchart layout as `derive_pipeline_view`: inputs are
/// positioned above-left of their target transforms (primary input on the
/// main line), outputs fan out right of the last transform.
/// Since partial mode may lack `local_window` data, all inputs default to
/// the first transform.
pub fn derive_partial_pipeline_view(
    partial: &clinker_core::partial::PartialPipelineConfig,
) -> PipelineView {
    use clinker_core::composition::RawTransformEntry;
    use clinker_core::partial::PartialItem;

    let input_count = partial.inputs.len();
    let transform_count = partial.transformations.len();
    let output_count = partial.outputs.len();

    // Helper to compute staggered Y
    let y_for = |i: usize| BASE_Y + if i.is_multiple_of(2) { 0.0 } else { STAGGER_Y };

    // ── Transforms (horizontal spine) ───────────────────────────────────────
    let transform_x_start = if input_count == 0 {
        LEFT_MARGIN
    } else {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    };
    let mut x = transform_x_start;
    let mut transform_stages = Vec::new();

    for (idx, item) in partial.transformations.iter().enumerate() {
        let y = y_for(idx);
        match item {
            PartialItem::Ok(entry) => match entry {
                RawTransformEntry::Inline(t) => {
                    let subtitle = cxl_subtitle(&t.cxl);
                    transform_stages.push(StageView {
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
                    transform_stages.push(StageView {
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
                transform_stages.push(StageView {
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
    }

    // ── Inputs (all target first transform in partial mode) ─────────────────
    let mut input_stages = Vec::new();
    // In partial mode we can't reliably detect local_window.source, so all
    // inputs default to the first transform.
    let input_targets: Vec<usize> = vec![0; input_count];

    for (input_idx, item) in partial.inputs.iter().enumerate() {
        let (ix, iy) = if input_idx == 0 && !transform_stages.is_empty() {
            (LEFT_MARGIN, transform_stages[0].canvas_y)
        } else if !transform_stages.is_empty() {
            let target = &transform_stages[0];
            let stack_n = input_idx - 1; // 0-based for secondary inputs
            let iy = target.canvas_y
                - NODE_HEIGHT
                - INPUT_Y_OFFSET
                - (stack_n as f32) * (NODE_HEIGHT + STACK_GAP);
            (target.canvas_x - NODE_WIDTH - NODE_GAP / 2.0, iy)
        } else {
            (
                LEFT_MARGIN,
                BASE_Y + (input_idx as f32) * (NODE_HEIGHT + STACK_GAP),
            )
        };

        match item {
            PartialItem::Ok(input) => {
                input_stages.push(StageView {
                    id: input.name.clone(),
                    label: input.name.clone(),
                    kind: StageKind::Source,
                    subtitle: input.path.clone(),
                    canvas_x: ix,
                    canvas_y: iy,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: None,
                });
            }
            PartialItem::Err { index, message } => {
                input_stages.push(StageView {
                    id: format!("_err_input_{index}"),
                    label: format!("input #{}", index + 1),
                    kind: StageKind::Error,
                    subtitle: truncate_error(message),
                    canvas_x: ix,
                    canvas_y: iy,
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: Some(message.clone()),
                });
            }
        }
    }

    // ── Outputs (fan out right of last transform) ───────────────────────────
    let mut output_stages = Vec::new();
    let output_x = if transform_stages.is_empty() {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    } else {
        let last = transform_stages.last().unwrap();
        last.canvas_x + NODE_WIDTH + NODE_GAP
    };
    let center_y = BASE_Y + NODE_HEIGHT / 2.0 + STAGGER_Y / 2.0;
    let output_ys = stack_y_positions(output_count, center_y);

    for (i, item) in partial.outputs.iter().enumerate() {
        match item {
            PartialItem::Ok(output) => {
                output_stages.push(StageView {
                    id: output.name.clone(),
                    label: output.name.clone(),
                    kind: StageKind::Output,
                    subtitle: output.path.clone(),
                    canvas_x: output_x,
                    canvas_y: output_ys[i],
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: None,
                });
            }
            PartialItem::Err { index, message } => {
                output_stages.push(StageView {
                    id: format!("_err_output_{index}"),
                    label: format!("output #{}", index + 1),
                    kind: StageKind::Error,
                    subtitle: truncate_error(message),
                    canvas_x: output_x,
                    canvas_y: output_ys[i],
                    cxl_source: None,
                    description: None,
                    from_composition: None,
                    error_message: Some(message.clone()),
                });
            }
        }
    }

    // ── Connections & assembly ───────────────────────────────────────────────
    let connections = build_connections(input_count, &input_targets, transform_count, output_count);

    let mut stages = Vec::with_capacity(input_count + transform_count + output_count);
    stages.extend(input_stages);
    stages.extend(transform_stages);
    stages.extend(output_stages);

    PipelineView {
        stages,
        composition_groups: Vec::new(),
        connections,
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
