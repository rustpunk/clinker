/// Hardcoded demo pipeline for Phase 1. Replaced in Phase 2 by a real
/// `Signal<Pipeline>` model parsed from YAML via serde-saphyr.

/// Pass assignment for a pipeline stage.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Pass {
    /// Pass 1 — scan phase (schema detection, index building).
    Scan,
    /// Pass 2 — transform phase (CXL expression evaluation, output).
    Transform,
}

impl Pass {
    pub fn label(self) -> &'static str {
        match self {
            Pass::Scan => "P1",
            Pass::Transform => "P2",
        }
    }
}

/// Stage type, determining the accent colour and badge label.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum StepType {
    Source,
    Filter,
    Map,
    Sink,
}

impl StepType {
    /// Rustpunk accent colour hex string for this stage type.
    pub fn accent_color(self) -> &'static str {
        match self {
            StepType::Source => "#43B3AE",   // verdigris
            StepType::Filter => "#E8A524",   // hazard
            StepType::Map => "#C75B2A",      // ember
            StepType::Sink => "#B7410E",     // oxide-red
        }
    }

    /// Short badge label shown on the node card.
    pub fn badge_label(self) -> &'static str {
        match self {
            StepType::Source => "SOURCE",
            StepType::Filter => "FILTER",
            StepType::Map => "MAP",
            StepType::Sink => "SINK",
        }
    }
}

/// A single pipeline stage as rendered in the Phase 1 static canvas.
#[derive(Clone, Debug, PartialEq)]
pub struct DemoStage {
    /// Stable string ID used as the RSX iterator key. Never index-based.
    pub id: &'static str,
    /// Human-readable node label (Chakra Petch heading).
    pub label: &'static str,
    pub step_type: StepType,
    pub pass: Pass,
    /// Secondary subtitle line (JetBrains Mono, iron colour).
    pub subtitle: &'static str,
    /// Canvas world-space X position (top-left of the node card).
    pub canvas_x: f32,
    /// Canvas world-space Y position (top-left of the node card).
    pub canvas_y: f32,
}

/// Approximate rendered node height in px (used for port-centre calculations).
pub const NODE_HEIGHT: f32 = 92.0;
/// Fixed node card width in px, per spec §4.2.
pub const NODE_WIDTH: f32 = 160.0;

impl DemoStage {
    /// Centre of the right-side output port — connector source point.
    pub fn port_out(&self) -> (f32, f32) {
        (self.canvas_x + NODE_WIDTH, self.canvas_y + NODE_HEIGHT / 2.0)
    }

    /// Centre of the left-side input port — connector target point.
    pub fn port_in(&self) -> (f32, f32) {
        (self.canvas_x, self.canvas_y + NODE_HEIGHT / 2.0)
    }
}

/// Returns the four-stage demo pipeline.
pub fn demo_pipeline() -> Vec<DemoStage> {
    vec![
        DemoStage {
            id: "demo-source",
            label: "csv_reader",
            step_type: StepType::Source,
            pass: Pass::Scan,
            subtitle: "customers_*.csv",
            canvas_x: 60.0,
            canvas_y: 100.0,
        },
        DemoStage {
            id: "demo-filter",
            label: "active_only",
            step_type: StepType::Filter,
            pass: Pass::Transform,
            subtitle: "status == \"active\"",
            canvas_x: 300.0,
            canvas_y: 140.0,
        },
        DemoStage {
            id: "demo-map",
            label: "enrich",
            step_type: StepType::Map,
            pass: Pass::Transform,
            subtitle: "full_name, email_lower",
            canvas_x: 540.0,
            canvas_y: 80.0,
        },
        DemoStage {
            id: "demo-sink",
            label: "json_out",
            step_type: StepType::Sink,
            pass: Pass::Transform,
            subtitle: "output/customers.json",
            canvas_x: 780.0,
            canvas_y: 120.0,
        },
    ]
}

/// Demo pipeline YAML shown in the sidebar. Intentionally realistic to exercise
/// the static syntax tokeniser.
pub const DEMO_YAML: &str = r#"# Clinker Pipeline — Customer ETL
name: customer_etl
version: 2

source:
  csv_reader:
    format: csv
    path: ./data/customers_*.csv
    csv:
      header: true
      null_value: ""

stages:
  active_only:
    step: filter
    expr: 'status == "active"'

  enrich:
    step: map
    fields:
      - name: full_name
        expr: 'first_name + " " + last_name'
      - name: email_lower
        expr: lower(email)
      - name: region_label
        expr: map("region_labels", region)

destination:
  json_out:
    format: json
    path: ./output/customers.json
"#;
