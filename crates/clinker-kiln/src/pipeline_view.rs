/// Derives canvas-renderable stage data from a `PipelineConfig`.
///
/// Phase 16b: composition system removed. The canvas renders simple
/// source/transform/output nodes laid out horizontally.
use clinker_core::config::{NodeHeader, PipelineConfig, PipelineNode, TransformBody};

pub const NODE_HEIGHT: f32 = 92.0;
pub const NODE_WIDTH: f32 = 160.0;

#[derive(Clone, Debug, PartialEq)]
pub enum StageKind {
    Source,
    Transform,
    Output,
    Error,
}

impl StageKind {
    pub fn kind_attr(&self) -> &'static str {
        match self {
            StageKind::Source => "source",
            StageKind::Transform => "transform",
            StageKind::Output => "output",
            StageKind::Error => "error",
        }
    }

    #[allow(dead_code)]
    pub fn accent_color(&self) -> &'static str {
        match self {
            StageKind::Source => "#43B3AE",
            StageKind::Transform => "#C75B2A",
            StageKind::Output => "#B7410E",
            StageKind::Error => "#CC3333",
        }
    }

    pub fn badge_label(&self) -> &'static str {
        match self {
            StageKind::Source => "SOURCE",
            StageKind::Transform => "TRANSFORM",
            StageKind::Output => "OUTPUT",
            StageKind::Error => "ERROR",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StageView {
    pub id: String,
    pub label: String,
    pub kind: StageKind,
    pub subtitle: String,
    pub canvas_x: f32,
    pub canvas_y: f32,
    pub cxl_source: Option<String>,
    pub description: Option<String>,
    pub error_message: Option<String>,
}

impl StageView {
    pub fn port_out(&self) -> (f32, f32) {
        (
            self.canvas_x + NODE_WIDTH,
            self.canvas_y + NODE_HEIGHT / 2.0,
        )
    }

    pub fn port_in(&self) -> (f32, f32) {
        (self.canvas_x, self.canvas_y + NODE_HEIGHT / 2.0)
    }
}

const NODE_GAP: f32 = 80.0;
const LEFT_MARGIN: f32 = 60.0;
const BASE_Y: f32 = 120.0;
const STAGGER_Y: f32 = 20.0;
const STACK_GAP: f32 = 24.0;
const INPUT_Y_OFFSET: f32 = 30.0;

pub struct PipelineView {
    pub stages: Vec<StageView>,
    /// Explicit connections between stages: `(from_idx, to_idx)`.
    pub connections: Vec<(usize, usize)>,
}

pub fn derive_pipeline_view(config: &PipelineConfig) -> PipelineView {
    let transforms: Vec<(&NodeHeader, &TransformBody)> = config
        .nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Transform {
                header,
                config: body,
            } => Some((header, body)),
            _ => None,
        })
        .collect();
    let source_configs: Vec<_> = config.source_configs().collect();
    let output_configs: Vec<_> = config.output_configs().collect();
    let input_count = source_configs.len();
    let transform_count = transforms.len();
    let output_count = output_configs.len();

    let transform_x_start = if input_count == 0 {
        LEFT_MARGIN
    } else {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    };

    let mut transform_stages = Vec::new();
    for (idx, (header, body)) in transforms.iter().enumerate() {
        let x = transform_x_start + (idx as f32) * (NODE_WIDTH + NODE_GAP);
        let y = BASE_Y + if idx % 2 == 0 { 0.0 } else { STAGGER_Y };
        let cxl_src: &str = body.cxl.as_ref();
        transform_stages.push(StageView {
            id: header.name.clone(),
            label: header.name.clone(),
            kind: StageKind::Transform,
            subtitle: cxl_subtitle(cxl_src),
            canvas_x: x,
            canvas_y: y,
            cxl_source: Some(cxl_src.to_string()),
            description: header.description.clone(),
            error_message: None,
        });
    }

    let input_targets = map_inputs_to_transforms(&source_configs, &transforms);

    let mut input_stages = Vec::new();
    let mut secondary_stack_count: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    for (input_idx, input) in source_configs.iter().enumerate() {
        let target_t_idx = input_targets[input_idx];
        let (ix, iy) = if input_idx == 0 && !transform_stages.is_empty() {
            (LEFT_MARGIN, transform_stages[0].canvas_y)
        } else if !transform_stages.is_empty() && target_t_idx < transform_stages.len() {
            let target = &transform_stages[target_t_idx];
            let stack_n = secondary_stack_count.entry(target_t_idx).or_insert(0);
            let iy = target.canvas_y
                - NODE_HEIGHT
                - INPUT_Y_OFFSET
                - (*stack_n as f32) * (NODE_HEIGHT + STACK_GAP);
            *stack_n += 1;
            (target.canvas_x - NODE_WIDTH - NODE_GAP / 2.0, iy)
        } else {
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
            error_message: None,
        });
    }

    let mut output_stages = Vec::new();
    let output_x = if transform_stages.is_empty() {
        LEFT_MARGIN + NODE_WIDTH + NODE_GAP
    } else {
        let last = transform_stages.last().unwrap();
        last.canvas_x + NODE_WIDTH + NODE_GAP
    };
    let center_y = BASE_Y + NODE_HEIGHT / 2.0 + STAGGER_Y / 2.0;
    let output_ys = stack_y_positions(output_count, center_y);
    for (i, output) in output_configs.iter().enumerate() {
        output_stages.push(StageView {
            id: output.name.clone(),
            label: output.name.clone(),
            kind: StageKind::Output,
            subtitle: output.path.clone(),
            canvas_x: output_x,
            canvas_y: output_ys[i],
            cxl_source: None,
            description: None,
            error_message: None,
        });
    }

    let connections = build_connections(input_count, &input_targets, transform_count, output_count);

    let mut stages = Vec::with_capacity(input_count + transform_count + output_count);
    stages.extend(input_stages);
    stages.extend(transform_stages);
    stages.extend(output_stages);

    PipelineView {
        stages,
        connections,
    }
}

fn map_inputs_to_transforms(
    source_configs: &[&clinker_core::config::SourceConfig],
    transforms: &[(&NodeHeader, &TransformBody)],
) -> Vec<usize> {
    source_configs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            if i == 0 || transforms.is_empty() {
                return 0;
            }
            transforms
                .iter()
                .position(|(_, body)| {
                    body.analytic_window
                        .as_ref()
                        .and_then(|lw| lw.get("source"))
                        .and_then(|v| v.as_str())
                        == Some(&input.name)
                })
                .unwrap_or(0)
        })
        .collect()
}

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

fn build_connections(
    input_count: usize,
    input_targets: &[usize],
    transform_count: usize,
    output_count: usize,
) -> Vec<(usize, usize)> {
    let mut conns = Vec::new();
    let t_base = input_count;

    if transform_count > 0 {
        for (i, &target) in input_targets.iter().enumerate() {
            let target_stage = t_base + target.min(transform_count - 1);
            conns.push((i, target_stage));
        }
        for i in 0..transform_count - 1 {
            conns.push((t_base + i, t_base + i + 1));
        }
        let last_t = t_base + transform_count - 1;
        let o_base = t_base + transform_count;
        for j in 0..output_count {
            conns.push((last_t, o_base + j));
        }
    } else {
        let o_base = input_count;
        for i in 0..input_count {
            for j in 0..output_count {
                conns.push((i, o_base + j));
            }
        }
    }

    conns
}

/// Derive canvas nodes from a `PartialPipelineConfig` (graceful degradation).
pub fn derive_partial_pipeline_view(
    partial: &clinker_core::partial::PartialPipelineConfig,
) -> PipelineView {
    use clinker_core::partial::PartialItem;

    let input_count = partial.inputs.len();
    let transform_count = partial.transformations.len();
    let output_count = partial.outputs.len();

    let y_for = |i: usize| BASE_Y + if i.is_multiple_of(2) { 0.0 } else { STAGGER_Y };

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
            PartialItem::Ok(t) => {
                transform_stages.push(StageView {
                    id: t.name.clone(),
                    label: t.name.clone(),
                    kind: StageKind::Transform,
                    subtitle: cxl_subtitle(t.cxl_source()),
                    canvas_x: x,
                    canvas_y: y,
                    cxl_source: Some(t.cxl_source().to_string()),
                    description: t.description.clone(),
                    error_message: None,
                });
            }
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
                    error_message: Some(message.clone()),
                });
            }
        }
        x += NODE_WIDTH + NODE_GAP;
    }

    let input_targets: Vec<usize> = vec![0; input_count];
    let mut input_stages = Vec::new();
    for (input_idx, item) in partial.inputs.iter().enumerate() {
        let (ix, iy) = if input_idx == 0 && !transform_stages.is_empty() {
            (LEFT_MARGIN, transform_stages[0].canvas_y)
        } else if !transform_stages.is_empty() {
            let target = &transform_stages[0];
            let stack_n = input_idx - 1;
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
                    error_message: Some(message.clone()),
                });
            }
        }
    }

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
                    error_message: Some(message.clone()),
                });
            }
        }
    }

    let connections = build_connections(input_count, &input_targets, transform_count, output_count);

    let mut stages = Vec::with_capacity(input_count + transform_count + output_count);
    stages.extend(input_stages);
    stages.extend(transform_stages);
    stages.extend(output_stages);

    PipelineView {
        stages,
        connections,
    }
}

fn truncate_error(msg: &str) -> String {
    if msg.len() <= 40 {
        msg.to_string()
    } else {
        format!("{}...", &msg[..37])
    }
}

fn cxl_subtitle(cxl: &str) -> String {
    cxl.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .chars()
        .take(30)
        .collect()
}
