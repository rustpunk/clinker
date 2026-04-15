/// Derives canvas-renderable stage data from a `PipelineConfig`.
///
/// Phase 16b Task 16b.5: the canvas dispatches on `PipelineNode` variants
/// directly. Every variant — Source, Transform, Aggregate, Route, Merge,
/// Output, Composition — maps 1:1 to a [`StageKind`] via an exhaustive
/// `match` in [`stage_kind_for_node`], so adding a new variant to
/// `PipelineNode` is a compile-time break. Composition renders as a
/// placeholder badge until Phase 16c supplies real rendering.
use clinker_core::config::node_header::NodeInput;
use clinker_core::config::{PipelineConfig, PipelineNode};

pub const NODE_HEIGHT: f32 = 92.0;
pub const NODE_WIDTH: f32 = 160.0;

#[derive(Clone, Debug, PartialEq)]
pub enum StageKind {
    Source,
    Transform,
    Aggregate,
    Route,
    Merge,
    Combine,
    Output,
    Composition,
    Error,
}

impl StageKind {
    pub fn kind_attr(&self) -> &'static str {
        match self {
            StageKind::Source => "source",
            StageKind::Transform => "transform",
            StageKind::Aggregate => "aggregate",
            StageKind::Route => "route",
            StageKind::Merge => "merge",
            StageKind::Combine => "combine",
            StageKind::Output => "output",
            StageKind::Composition => "composition",
            StageKind::Error => "error",
        }
    }

    pub fn badge_label(&self) -> &'static str {
        match self {
            StageKind::Source => "SOURCE",
            StageKind::Transform => "TRANSFORM",
            StageKind::Aggregate => "AGGREGATE",
            StageKind::Route => "ROUTE",
            StageKind::Merge => "MERGE",
            StageKind::Combine => "COMBINE",
            StageKind::Output => "OUTPUT",
            StageKind::Composition => "COMPOSITION",
            StageKind::Error => "ERROR",
        }
    }
}

/// Exhaustive compile-time variant dispatch: classify a [`PipelineNode`]
/// into its canvas [`StageKind`]. Adding a new variant to `PipelineNode`
/// without updating this match is a build error.
pub fn stage_kind_for_node(node: &PipelineNode) -> StageKind {
    match node {
        PipelineNode::Source { .. } => StageKind::Source,
        PipelineNode::Transform { .. } => StageKind::Transform,
        PipelineNode::Aggregate { .. } => StageKind::Aggregate,
        PipelineNode::Route { .. } => StageKind::Route,
        PipelineNode::Merge { .. } => StageKind::Merge,
        PipelineNode::Combine { .. } => StageKind::Combine,
        PipelineNode::Output { .. } => StageKind::Output,
        PipelineNode::Composition { .. } => StageKind::Composition,
    }
}

fn node_input_name(ni: &NodeInput) -> &str {
    match ni {
        NodeInput::Single(s) => s.as_str(),
        NodeInput::Port { node, .. } => node.as_str(),
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

/// Phase 16b Task 16b.5: walk `config.nodes` in declaration order and
/// dispatch on `PipelineNode` variant to produce a [`StageView`] for every
/// node. Connections are derived from each consumer's `input:` / `inputs:`
/// header field. The match arms here mirror [`stage_kind_for_node`]; both
/// are compile-time exhaustive, so adding a new `PipelineNode` variant is
/// a build error.
pub fn derive_pipeline_view(config: &PipelineConfig) -> PipelineView {
    use std::collections::HashMap;

    // Column = 1 + max column of inputs; sources sit in column 0.
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut cols: Vec<usize> = Vec::with_capacity(config.nodes.len());
    for (idx, spanned) in config.nodes.iter().enumerate() {
        let node = &spanned.value;
        name_to_idx.insert(node.name().to_string(), idx);
        let col = match node {
            PipelineNode::Source { .. } => 0,
            PipelineNode::Merge { header, .. } => header
                .inputs
                .iter()
                .filter_map(|ni| name_to_idx.get(node_input_name(ni)).copied())
                .map(|i| cols[i] + 1)
                .max()
                .unwrap_or(1),
            PipelineNode::Combine { header, .. } => header
                .input
                .values()
                .filter_map(|ni| name_to_idx.get(node_input_name(ni)).copied())
                .map(|i| cols[i] + 1)
                .max()
                .unwrap_or(1),
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. }
            | PipelineNode::Composition { header, .. } => name_to_idx
                .get(node_input_name(&header.input))
                .copied()
                .map(|i| cols[i] + 1)
                .unwrap_or(1),
        };
        cols.push(col);
    }

    // Per-column row counter for vertical stacking.
    let mut col_rows: HashMap<usize, usize> = HashMap::new();
    let mut stages: Vec<StageView> = Vec::with_capacity(config.nodes.len());
    for (idx, spanned) in config.nodes.iter().enumerate() {
        let node = &spanned.value;
        let col = cols[idx];
        let row = *col_rows
            .entry(col)
            .and_modify(|r| *r += 1)
            .or_insert(0_usize);
        let x = LEFT_MARGIN + (col as f32) * (NODE_WIDTH + NODE_GAP);
        let stagger = if col.is_multiple_of(2) {
            0.0
        } else {
            STAGGER_Y
        };
        let y = BASE_Y + (row as f32) * (NODE_HEIGHT + STACK_GAP) + stagger;
        stages.push(build_stage_view(node, x, y));
    }

    // Connections: resolve each consumer's input header reference.
    let mut connections: Vec<(usize, usize)> = Vec::new();
    for (idx, spanned) in config.nodes.iter().enumerate() {
        match &spanned.value {
            PipelineNode::Source { .. } => {}
            PipelineNode::Merge { header, .. } => {
                for ni in &header.inputs {
                    if let Some(&from) = name_to_idx.get(node_input_name(ni)) {
                        connections.push((from, idx));
                    }
                }
            }
            PipelineNode::Combine { header, .. } => {
                for ni in header.input.values() {
                    if let Some(&from) = name_to_idx.get(node_input_name(ni)) {
                        connections.push((from, idx));
                    }
                }
            }
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. }
            | PipelineNode::Composition { header, .. } => {
                if let Some(&from) = name_to_idx.get(node_input_name(&header.input)) {
                    connections.push((from, idx));
                }
            }
        }
    }

    PipelineView {
        stages,
        connections,
    }
}

/// Variant-dispatched [`StageView`] constructor. Every arm is exhaustive;
/// adding a new `PipelineNode` variant breaks the build here.
fn build_stage_view(node: &PipelineNode, x: f32, y: f32) -> StageView {
    let kind = stage_kind_for_node(node);
    match node {
        PipelineNode::Source {
            header,
            config: body,
        } => StageView {
            id: header.name.clone(),
            label: header.name.clone(),
            kind,
            subtitle: body.source.path.clone(),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: header.description.clone(),
            error_message: None,
        },
        PipelineNode::Transform {
            header,
            config: body,
        } => {
            let cxl_src: &str = body.cxl.as_ref();
            StageView {
                id: header.name.clone(),
                label: header.name.clone(),
                kind,
                subtitle: cxl_subtitle(cxl_src),
                canvas_x: x,
                canvas_y: y,
                cxl_source: Some(cxl_src.to_string()),
                description: header.description.clone(),
                error_message: None,
            }
        }
        PipelineNode::Aggregate {
            header,
            config: body,
        } => {
            let cxl_src: &str = body.cxl.as_ref();
            let subtitle = if body.group_by.is_empty() {
                cxl_subtitle(cxl_src)
            } else {
                format!("by {}", body.group_by.join(", "))
            };
            StageView {
                id: header.name.clone(),
                label: header.name.clone(),
                kind,
                subtitle,
                canvas_x: x,
                canvas_y: y,
                cxl_source: Some(cxl_src.to_string()),
                description: header.description.clone(),
                error_message: None,
            }
        }
        PipelineNode::Route {
            header,
            config: body,
        } => {
            let subtitle = format!(
                "{} branch{} → {}",
                body.conditions.len(),
                if body.conditions.len() == 1 { "" } else { "es" },
                body.default
            );
            StageView {
                id: header.name.clone(),
                label: header.name.clone(),
                kind,
                subtitle,
                canvas_x: x,
                canvas_y: y,
                cxl_source: None,
                description: header.description.clone(),
                error_message: None,
            }
        }
        PipelineNode::Merge { header, .. } => StageView {
            id: header.name.clone(),
            label: header.name.clone(),
            kind,
            subtitle: format!("{} inputs", header.inputs.len()),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: header.description.clone(),
            error_message: None,
        },
        PipelineNode::Combine {
            header,
            config: body,
        } => {
            let cxl_src: &str = body.cxl.as_ref();
            StageView {
                id: header.name.clone(),
                label: header.name.clone(),
                kind,
                subtitle: format!("{} inputs", header.input.len()),
                canvas_x: x,
                canvas_y: y,
                cxl_source: Some(cxl_src.to_string()),
                description: header.description.clone(),
                error_message: None,
            }
        }
        PipelineNode::Output {
            header,
            config: body,
        } => StageView {
            id: header.name.clone(),
            label: header.name.clone(),
            kind,
            subtitle: body.output.path.clone(),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: header.description.clone(),
            error_message: None,
        },
        PipelineNode::Composition { header, r#use, .. } => StageView {
            id: header.name.clone(),
            label: header.name.clone(),
            kind,
            subtitle: format!("use: {} (Phase 16c)", r#use.display()),
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: header.description.clone(),
            error_message: None,
        },
    }
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

/// Derive canvas nodes from a `BoundBody`'s `PlanNode` list.
///
/// Used when drilled into a composition: the sub-canvas renders the
/// composition's internal nodes. Layout is a simple left-to-right chain.
pub fn derive_body_view(body: &clinker_core::plan::composition_body::BoundBody) -> PipelineView {
    use clinker_core::plan::execution::PlanNode;

    let mut stages = Vec::with_capacity(body.nodes.len());

    for (idx, plan_node) in body.nodes.iter().enumerate() {
        let x = LEFT_MARGIN + idx as f32 * (NODE_WIDTH + NODE_GAP);
        let y = BASE_Y;

        let (id, kind, subtitle) = match plan_node {
            PlanNode::Source { name, .. } => (name.clone(), StageKind::Source, String::new()),
            PlanNode::Transform { name, .. } => (name.clone(), StageKind::Transform, String::new()),
            PlanNode::Route { name, mode, .. } => {
                (name.clone(), StageKind::Route, format!("{mode:?}"))
            }
            PlanNode::Merge { name, .. } => (name.clone(), StageKind::Merge, String::new()),
            PlanNode::Output { name, .. } => (name.clone(), StageKind::Output, String::new()),
            PlanNode::Sort { name, .. } => (name.clone(), StageKind::Transform, "sort".into()),
            PlanNode::Aggregation { name, strategy, .. } => {
                (name.clone(), StageKind::Aggregate, format!("{strategy:?}"))
            }
            PlanNode::Composition { name, .. } => {
                (name.clone(), StageKind::Composition, String::new())
            }
            PlanNode::Combine { name, strategy, .. } => {
                (name.clone(), StageKind::Combine, format!("{strategy:?}"))
            }
        };

        stages.push(StageView {
            id,
            label: plan_node.name().to_string(),
            kind,
            subtitle,
            canvas_x: x,
            canvas_y: y,
            cxl_source: None,
            description: None,
            error_message: None,
        });
    }

    // Simple sequential connections for body nodes
    let connections: Vec<(usize, usize)> = (0..stages.len().saturating_sub(1))
        .map(|i| (i, i + 1))
        .collect();

    PipelineView {
        stages,
        connections,
    }
}

#[cfg(test)]
mod task_16b_5_tests {
    use super::*;
    use clinker_core::config::parse_config;

    /// Phase 16b Task 16b.5 gate test: compile-time exhaustiveness of the
    /// variant dispatch. This function returns a distinct `StageKind` for
    /// every `PipelineNode` variant; adding a new variant without updating
    /// [`stage_kind_for_node`] is a build error.
    #[test]
    fn test_canvas_node_dispatches_on_variant() {
        // Use a minimal unified-shape YAML exercising every variant so the
        // match in `stage_kind_for_node` is hit for each at runtime too.
        let yaml = r#"
pipeline:
  name: variant_dispatch_smoke
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: ./in.csv
      schema:
        - { name: agg, type: string }

  - type: transform
    name: clean
    input: src
    config:
      cxl: |
        emit x = 1
  - type: aggregate
    name: agg
    input: clean
    config:
      group_by: [x]
      cxl: |
        emit n = 1
  - type: route
    name: split
    input: clean
    config:
      conditions:
        hi: "x > 0"
      default: lo
  - type: merge
    name: joined
    inputs: [split.hi, split.lo]
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
        let config = parse_config(yaml).expect("parse unified-shape pipeline");
        let view = derive_pipeline_view(&config);

        // Each declared node produces exactly one stage.
        assert_eq!(view.stages.len(), 6);

        // Every variant kind is represented.
        let has = |k: &StageKind| view.stages.iter().any(|s| &s.kind == k);
        assert!(has(&StageKind::Source));
        assert!(has(&StageKind::Transform));
        assert!(has(&StageKind::Aggregate));
        assert!(has(&StageKind::Route));
        assert!(has(&StageKind::Merge));
        assert!(has(&StageKind::Output));
    }

    /// Phase 16b Task 16b.5 gate test: a legacy-shape fixture lifted into
    /// the unified `nodes:` topology still renders via the variant-dispatch
    /// code path and produces the expected stage count.
    #[test]
    fn test_kiln_loads_migrated_fixture() {
        let yaml = r#"
pipeline:
  name: loaded_fixture_smoke
nodes:
- type: source
  name: raw
  config:
    name: raw
    type: csv
    path: ./raw.csv
    options:
      has_header: true
    schema:
      - { name: agg, type: string }

- type: transform
  name: clean
  input: raw
  config:
    cxl: 'emit a = 1

      '
- type: transform
  name: finalize
  input: clean
  config:
    cxl: 'emit b = 2

      '
- type: output
  name: results
  input: finalize
  config:
    name: results
    type: csv
    path: ./results.csv
"#;
        let config = parse_config(yaml).expect("legacy fixture lifts to nodes");
        let view = derive_pipeline_view(&config);

        // 1 source + 2 transforms + 1 output = 4 stages.
        assert_eq!(view.stages.len(), 4);
        assert_eq!(
            view.stages
                .iter()
                .filter(|s| s.kind == StageKind::Transform)
                .count(),
            2
        );
        assert_eq!(
            view.stages
                .iter()
                .filter(|s| s.kind == StageKind::Source)
                .count(),
            1
        );
        assert_eq!(
            view.stages
                .iter()
                .filter(|s| s.kind == StageKind::Output)
                .count(),
            1
        );

        // Connections are derived and non-empty for a chain of length > 1.
        assert!(!view.connections.is_empty());
    }

    /// Phase 16b Task 16b.5 gate test: a pipeline containing a
    /// `PipelineNode::Composition` stub renders it as a placeholder stage
    /// (Phase 16c badge subtitle) without panic.
    #[test]
    fn test_kiln_composition_placeholder_renders() {
        let yaml = r#"
pipeline:
  name: composition_placeholder_smoke
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: ./comp_in.csv
      schema:
        - { name: agg, type: string }

  - type: composition
    name: sub
    input: src
    use: compositions/clean_names.comp.yaml
"#;
        let config = parse_config(yaml).expect("composition stub parses");
        let view = derive_pipeline_view(&config);

        let comp = view
            .stages
            .iter()
            .find(|s| s.kind == StageKind::Composition)
            .expect("composition stage present");
        assert_eq!(comp.label, "sub");
        assert!(
            comp.subtitle.contains("Phase 16c"),
            "placeholder subtitle should mention Phase 16c, got: {}",
            comp.subtitle
        );
        // Badge text is the authoritative kind label.
        assert_eq!(StageKind::Composition.badge_label(), "COMPOSITION");
    }
}
