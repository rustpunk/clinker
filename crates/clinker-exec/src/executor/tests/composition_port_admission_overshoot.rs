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
//! The arbitrator is seeded above the soft limit (spill active) with enough
//! disk quota for the parent Source slot but not both that slot and the body
//! port Source. The assertion destructures both the wrapper and the inner
//! typed variant.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;
const PORT_SPILL_CAP: u64 = 512;

fn spill_tripped_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(PORT_SPILL_CAP).unwrap();
    Arc::new(arb)
}

/// Seeded above the hard limit with unlimited disk quota. Every materialized
/// slot on this port path is spill-eligible, so the finite pipeline completes
/// through spill instead of taking the legacy non-spillable E310 gate.
fn forced_spill_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    arb.set_peak_rss_for_test(150 * 1024 * 1024 * 1024);
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
- type: sink
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
    .expect_err("the cumulative spill quota must abort the body's port-source admission");

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
                PipelineError::SpillCapExceeded {
                    node,
                    cap,
                    attempted,
                    current,
                } => {
                    assert_eq!(
                        node, "data",
                        "the boundary overflow must name the body's port-source node",
                    );
                    assert_eq!(cap, PORT_SPILL_CAP, "reported cap must equal the quota");
                    assert!(attempted > 0, "the overflowing flush must report its size");
                    assert!(
                        current > cap,
                        "reported cumulative spilled ({current}) must exceed the cap ({cap})",
                    );
                }
                other => panic!("expected inner SpillCapExceeded; got: {other:?}"),
            }
        }
        other => panic!("expected outer CompositionBodyError; got: {other:?}"),
    }
}

/// A composition input port is spill-eligible. Even when the seeded pressure
/// is above the hard limit, every finite slot spills and each body/parent
/// boundary opens a fresh sequential scan over immutable backing. The run
/// therefore completes exactly instead of taking the removed non-spillable
/// Arena gate.
#[test]
fn port_feeder_over_hard_limit_completes_through_spill() {
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
        execution_id: "composition-port-forced-spill".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    let report = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        ctx,
        forced_spill_arbitrator(),
    )
    .expect("a finite composition port path must complete through spill");

    assert!(
        report.cumulative_spill_bytes > 0,
        "the run must exercise spill"
    );
    let rendered = out.as_string();
    let rows: Vec<&str> = rendered.lines().skip(1).collect();
    assert_eq!(rows.len(), 30);
    for i in 0..30 {
        assert!(rows.contains(&format!("id_{i}").as_str()));
    }
}

/// A composition port whose producer's rows were spilled re-charges them at
/// the call site when the call takes them into memory. Refused, its E310
/// names what it was reserving: the rows buffered from the producer into
/// the call site, not rows collected for a full scan.
///
/// The run is held to a 256 KiB test capacity, reading no process memory so
/// only the charged total can refuse. The Source's 4,000 rows of about
/// 100 bytes spill as they are buffered for the call, and loading them back
/// needs more than the whole capacity, so the re-charge itself is refused.
#[test]
fn a_composition_port_recharge_shortfall_names_the_rows_it_buffers() {
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
    for i in 0..4000 {
        csv.push_str(&format!("id_{i:0100}\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "src.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
    )]);
    let capacity = 256 * 1024;
    let params = PipelineRunParams {
        execution_id: "composition-port-recharge".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test: crate::executor::MemoryTestOverrides::default()
            .with_ledger_capacity(capacity)
            .with_no_process_memory(),
        ..Default::default()
    };

    let err = PipelineExecutor::run_with_readers_writers_in_context(
        &config,
        readers,
        writers.into(),
        &params,
        ctx,
    )
    .expect_err("the port's rows cannot be loaded back within the capacity");

    // Refused at the call site, the report is bare: the call-site name is
    // its requester.
    let PipelineError::MemoryBudgetExceeded { report } = &err else {
        panic!("the port re-charge must fail with a bare E310; got {err:?}");
    };
    assert_eq!(
        report.requester,
        Some(clinker_plan::runtime_error::ConsumerLabel {
            node: "port_enrich_call".to_string(),
            surface: clinker_plan::runtime_error::MemorySurface::BufferedRows {
                from: "src".to_string(),
                to: vec!["port_enrich_call".to_string()],
            },
        }),
        "the requester names the rows buffered from the producer into the call: {report:?}"
    );
    assert!(
        report.requested_bytes > capacity,
        "the refused request is the port's re-charge, more than the capacity: {report:?}"
    );
    let rendered = err.to_string();
    assert!(
        rendered.contains("rows buffered between \"src\" and \"port_enrich_call\""),
        "{rendered}"
    );
    assert!(
        !rendered.contains("rows collected for a full scan"),
        "the port's rows are not a full scan: {rendered}"
    );
}
