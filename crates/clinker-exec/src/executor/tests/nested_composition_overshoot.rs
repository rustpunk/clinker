//! Hard-limit overshoot coverage for a body-interior `node_buffers`
//! admission, surfacing the
//! `PipelineError::CompositionBodyError { composition_name, inner }`
//! wrapping shape.
//!
//! A composition body shares the parent pipeline's `MemoryArbitrator`.
//! When an admission inside the body's topo walk overflows the
//! disk-spill quota, the error bubbles through the walk's `?` and the
//! executor wraps it under the composition's call-site name so the
//! user-visible failure location is the `Composition` node they wrote,
//! not a body-internal operator they never named in their YAML. The
//! test destructures both layers: the wrapper carries the call-site
//! name, the inner carries the body-internal node and the dedicated
//! `SpillCapExceeded` (E320) disk-cap surface.
//!
//! Reuses the multi-node body fixture
//! `tests/fixtures/compositions/issue_123_nested_hard_fail.comp.yaml`,
//! whose schema-widening Transform and two-branch fan-out mirror the
//! top-level diamond inside a body. The arbitrator is seeded above the
//! soft limit (spill active) with a one-byte disk quota so the body's
//! first admission overflows deterministically — independent of the
//! test process's real RSS, which pull-mode would otherwise race.

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
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(1);
    Arc::new(arb)
}

const PIPELINE_YAML: &str = r#"
pipeline:
  name: nested_composition_overshoot
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
      - { name: value, type: int }
- type: composition
  name: enrich_call
  input: events
  use: ../compositions/issue_123_nested_hard_fail.comp.yaml
  inputs:
    records: events
- type: output
  name: out
  input: enrich_call
  config:
    name: out
    type: csv
    path: out.csv
"#;

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

#[test]
fn body_interior_overshoot_is_wrapped_under_the_call_site() {
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");
    let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(
        fixture_workspace_root(),
        PathBuf::from("pipelines"),
    );

    let mut csv = String::from("id,region,value\n");
    for i in 0..30 {
        let region = if i % 2 == 0 { "a" } else { "b" };
        csv.push_str(&format!("id_{i},{region},{i}\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "nested-composition-overshoot".to_string(),
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
    .expect_err("one-byte spill quota must abort a body-interior admission");

    match err {
        PipelineError::CompositionBodyError {
            composition_name,
            inner,
        } => {
            assert_eq!(
                composition_name, "enrich_call",
                "the wrapper must name the call-site, not the body interior",
            );
            match *inner {
                PipelineError::SpillCapExceeded {
                    node,
                    cap,
                    attempted,
                    current,
                } => {
                    assert!(
                        !node.is_empty(),
                        "the inner error must name the body-internal node that overflowed",
                    );
                    assert_eq!(cap, 1, "reported cap must equal the one-byte quota");
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
