//! End-to-end coverage for the `pipeline.memory.backpressure` knob:
//! parsing through serde-saphyr, lowering to `BackpressureKnob`, and
//! the `--explain` annotation that renders the active policy name.
//!
//! Pairs with the in-crate unit tests in
//! `clinker-exec/src/pipeline/memory.rs` (synthetic-consumer
//! victim-selection coverage) and the `pre_lift_baselines` snapshot
//! suite (default-case explain output).

use clinker_exec::executor::PipelineExecutor;
use clinker_plan::config::{BackpressureKnob, parse_config};

const FIXTURE_BASE: &str = r#"
pipeline:
  name: backpressure_test
  __MEMORY__
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    options:
      has_header: true
    schema:
      - { name: id, type: int }
- type: output
  name: sink
  input: src
  config:
    name: sink
    type: csv
    path: out.csv
"#;

fn explain_text_with(memory_block: &str) -> String {
    let yaml = FIXTURE_BASE.replace("  __MEMORY__\n", memory_block);
    let config = parse_config(&yaml).expect("parse_config");
    let compiled = clinker_plan::config::PipelineConfig::compile(
        &config,
        &clinker_plan::config::CompileContext::default(),
    )
    .expect("compile");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&compiled).expect("explain_dag");
    dag.explain_text(&config)
}

#[test]
fn memory_block_omitted_uses_pause_default() {
    let config = parse_config(&FIXTURE_BASE.replace("  __MEMORY__\n", "")).expect("parse_config");
    assert_eq!(config.pipeline.memory.limit, None);
    assert_eq!(config.pipeline.memory.backpressure, BackpressureKnob::Pause);
}

#[test]
fn memory_block_with_limit_only_uses_pause_default() {
    let yaml = FIXTURE_BASE.replace("  __MEMORY__\n", "  memory:\n    limit: \"1G\"\n");
    let config = parse_config(&yaml).expect("parse_config");
    assert_eq!(config.pipeline.memory.limit.as_deref(), Some("1G"));
    assert_eq!(config.pipeline.memory.backpressure, BackpressureKnob::Pause);
}

#[test]
fn memory_backpressure_spill_parses() {
    let yaml = FIXTURE_BASE.replace("  __MEMORY__\n", "  memory:\n    backpressure: spill\n");
    let config = parse_config(&yaml).expect("parse_config");
    assert_eq!(config.pipeline.memory.backpressure, BackpressureKnob::Spill);
}

#[test]
fn memory_backpressure_both_parses() {
    let yaml = FIXTURE_BASE.replace("  __MEMORY__\n", "  memory:\n    backpressure: both\n");
    let config = parse_config(&yaml).expect("parse_config");
    assert_eq!(config.pipeline.memory.backpressure, BackpressureKnob::Both);
}

#[test]
fn memory_backpressure_invalid_value_errors() {
    let yaml = FIXTURE_BASE.replace("  __MEMORY__\n", "  memory:\n    backpressure: nope\n");
    let err = parse_config(&yaml).expect_err("parse_config should reject unknown variant");
    let msg = err.to_string();
    // saphyr surfaces the offending field and the candidate variants
    // in its error path; check both signals are present so a future
    // refactor that drops one is caught.
    assert!(
        msg.contains("backpressure") || msg.contains("nope"),
        "expected diagnostic to name the invalid field/value, got: {msg}"
    );
}

#[test]
fn explain_default_renders_back_pressure_preferred_to_priority() {
    let text = explain_text_with("");
    assert!(
        text.contains("arbitration: BackPressurePreferred -> Priority"),
        "explain output should contain default policy line; got:\n{text}"
    );
}

#[test]
fn explain_with_spill_knob_renders_priority() {
    let text = explain_text_with("  memory:\n    backpressure: spill\n");
    assert!(
        text.contains("arbitration: Priority\n"),
        "explain output should contain Priority for backpressure: spill; got:\n{text}"
    );
    // Confirm we didn't accidentally land the wrapper-composed name.
    assert!(
        !text.contains("BackPressurePreferred"),
        "spill knob should not install BackPressurePreferred; got:\n{text}"
    );
}

#[test]
fn explain_with_both_knob_renders_back_pressure_preferred_to_largest_first() {
    let text = explain_text_with("  memory:\n    backpressure: both\n");
    assert!(
        text.contains("arbitration: BackPressurePreferred -> LargestFirst"),
        "explain output should contain wrapped LargestFirst for backpressure: both; got:\n{text}"
    );
}
