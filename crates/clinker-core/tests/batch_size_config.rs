//! Config-surface tests for the streaming-handoff `batch_size` knob:
//! the pipeline-level `pipeline.batch_size`, the per-Transform override
//! on a Transform's `config.batch_size`, and the shared "must be >= 1"
//! validation guarding both against a never-flushing zero batch.

use clinker_core::config::{PipelineNode, parse_config};

const PIPELINE_LEVEL: &str = r#"
pipeline:
  name: batch_knob
  batch_size: 512
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: t
    input: src
    config:
      batch_size: 64
      cxl: |
        emit id = id
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn pipeline_and_transform_batch_size_parse() {
    let config = parse_config(PIPELINE_LEVEL).expect("valid batch_size config parses");
    assert_eq!(config.pipeline.batch_size, Some(512));
    let t_batch = config
        .nodes
        .iter()
        .find_map(|spanned| match &spanned.value {
            PipelineNode::Transform { header, config } if header.name == "t" => {
                Some(config.batch_size)
            }
            _ => None,
        })
        .expect("transform 't' present");
    assert_eq!(t_batch, Some(64));
}

#[test]
fn omitted_batch_size_is_none() {
    let yaml = r#"
pipeline:
  name: no_batch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: t
    input: src
    config:
      cxl: |
        emit id = id
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("config without batch_size parses");
    assert_eq!(
        config.pipeline.batch_size, None,
        "omitted pipeline.batch_size resolves to None (engine applies its default)"
    );
}

#[test]
fn zero_pipeline_batch_size_is_rejected() {
    let yaml = r#"
pipeline:
  name: zero_batch
  batch_size: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = parse_config(yaml).expect_err("batch_size: 0 must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("batch_size") && msg.contains(">= 1"),
        "expected a 'batch_size must be >= 1' validation error, got: {msg}"
    );
}

#[test]
fn zero_transform_batch_size_is_rejected() {
    let yaml = r#"
pipeline:
  name: zero_transform_batch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: t
    input: src
    config:
      batch_size: 0
      cxl: |
        emit id = id
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = parse_config(yaml).expect_err("per-Transform batch_size: 0 must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("batch_size") && msg.contains(">= 1"),
        "expected a per-Transform 'batch_size must be >= 1' validation error, got: {msg}"
    );
    assert!(
        msg.contains('t'),
        "error should name the offending transform 't', got: {msg}"
    );
}
