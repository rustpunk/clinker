//! Hard-limit overshoot coverage for `node_buffers` admission at a
//! composition's input-port boundary.
//!
//! A composition body shares the parent pipeline's `MemoryArbitrator`
//! (bodies do not get their own budget). When the body's port-source
//! Source arm admits the records seeded from the parent producer, the
//! admission runs through the same `admit_node_buffer` disk-spill-quota
//! gate every other slot uses. Because that admission happens *inside*
//! the body's topo walk, an overflow there bubbles through the walk's
//! `?` and the executor wraps it as
//! `PipelineError::CompositionBodyError { composition_name, inner }` —
//! the user-visible failure names the composition call-site, while the
//! inner error names the body-internal port-source node.
//!
//! This pins the boundary case distinctly from a deeper body operator
//! (covered by `nested_composition_overshoot`): the inner `node` is the
//! body's first node, the port-source, demonstrating that the very first
//! admission a body performs is already enveloped by the call-site
//! arbitrator and wrapped under the call-site name.
//!
//! The arbitrator is seeded above the soft limit (spill active) with a
//! one-byte disk quota so the first body admission overflows; the
//! assertion destructures both the wrapper and the inner typed variant.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

fn spill_tripped_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        Box::new(crate::pipeline::memory::Priority),
    );
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(1);
    Arc::new(arb)
}

const PIPELINE_YAML: &str = r#"
pipeline:
  name: composition_port_admission_overshoot
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: src.csv
    schema:
      - { name: id, type: string }
- type: composition
  name: port_enrich_call
  input: src
  use: ../compositions/port_passthrough.comp.yaml
  inputs:
    data: src
- type: output
  name: out
  input: port_enrich_call
  config:
    name: out
    type: csv
    path: out.csv
"#;

const BODY_YAML: &str = r#"_compose:
  name: port_passthrough
  inputs:
    data:
      schema:
        - { name: id, type: string }
  outputs:
    out: add_tag
  config_schema: {}
nodes:
  - type: transform
    name: add_tag
    input: data
    config:
      cxl: |
        emit id = id
"#;

#[test]
fn port_admission_overshoot_is_wrapped_naming_the_port_source() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(comp_dir.join("port_passthrough.comp.yaml"), BODY_YAML)
        .expect("write composition body fixture");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(
        workspace.path().to_path_buf(),
        PathBuf::from("pipelines"),
    );
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");

    let mut csv = String::from("id\n");
    for i in 0..30 {
        csv.push_str(&format!("id_{i}\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "src.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "composition-port-admission-overshoot".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let err = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        ctx,
        spill_tripped_arbitrator(),
    )
    .expect_err("one-byte spill quota must abort the body's port-source admission");

    match err {
        PipelineError::CompositionBodyError {
            composition_name,
            inner,
        } => {
            assert_eq!(
                composition_name, "port_enrich_call",
                "the wrapper must name the user-visible composition call-site",
            );
            match *inner {
                PipelineError::MemoryBudgetExceeded {
                    node,
                    source,
                    used,
                    limit,
                    detail,
                } => {
                    assert_eq!(
                        node, "data",
                        "the boundary overflow must name the body's port-source node",
                    );
                    assert!(
                        matches!(source, clinker_plan::BudgetCategory::NodeBuffer),
                        "port admission must surface under NodeBuffer; got {source:?}",
                    );
                    assert_eq!(
                        detail.as_deref(),
                        Some("spill quota exceeded"),
                        "the body admission uses the same disk-spill-quota gate as every slot",
                    );
                    assert!(
                        used > limit,
                        "reported used ({used}) must exceed the spill quota ({limit})",
                    );
                }
                other => panic!("expected inner MemoryBudgetExceeded; got: {other:?}"),
            }
        }
        other => panic!("expected outer CompositionBodyError; got: {other:?}"),
    }
}
