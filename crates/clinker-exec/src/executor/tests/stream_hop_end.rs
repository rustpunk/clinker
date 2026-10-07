//! A producer streaming into a step on another thread hands that step every
//! row it produced.
//!
//! A bounded preview runs a Transform apart from its Source, so the Sources
//! drain in a fixed order, while the Sink keeps the streaming writer the
//! compiled plan gave it. The Transform's rows reach that writer, not a
//! buffer nobody reads.
//!
//! The writers are raw in-memory buffers, so anything a step finished is
//! visible here even though a production run would leave it staged and
//! unpublished.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

struct Run {
    result: Result<ExecutionReport, PipelineError>,
    outputs: HashMap<&'static str, String>,
}

/// Run `yaml` over `readers`, writing each of `sinks` to an in-memory
/// buffer and reading no process memory, under `policy` when given.
fn run_with(
    yaml: &str,
    readers: crate::executor::SourceReaders,
    sinks: &[&'static str],
    params: PipelineRunParams,
    policy: Option<RunPolicy>,
) -> Run {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let buffers: Vec<(&'static str, SharedBuffer)> = sinks
        .iter()
        .map(|sink| (*sink, SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(sink, buffer)| {
            (
                sink.to_string(),
                Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "stream-hop-end".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test: crate::executor::MemoryTestOverrides::default().with_no_process_memory(),
        ..params
    };
    let result = match policy {
        // A preview never publishes, so its writers are not committed.
        Some(policy) => PipelineExecutor::run_with_readers_writers_in_context_and_activation(
            &config,
            readers,
            WriterRegistry {
                auto_commit_staged: false,
                ..WriterRegistry::from(writers)
            },
            &params,
            policy,
            clinker_plan::config::CompileContext::default(),
            None,
        ),
        None => {
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
        }
    };
    Run {
        result,
        outputs: buffers
            .into_iter()
            .map(|(sink, buffer)| (sink, buffer.as_string()))
            .collect(),
    }
}

fn csv_reader(name: &str, csv: String) -> (String, crate::source::SourceInput) {
    (
        name.to_string(),
        crate::executor::single_file_reader(
            format!("{name}.csv"),
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )
}

fn pipeline(name: &str, nodes: &[&str]) -> String {
    format!("pipeline:\n  name: {name}\nnodes:{}", nodes.concat())
}

const ROWS_SOURCE: &str = r#"
- type: source
  name: src
  config:
    name: src
    type: csv
    path: src.csv
    schema:
      - { name: grp, type: string }
      - { name: id, type: string }
"#;

/// Source -> Transform -> Sink, the Transform fusable with its Source.
fn fusable_transform_sink() -> String {
    pipeline(
        "fusable_transform_sink",
        &[
            ROWS_SOURCE,
            r#"
- type: transform
  name: pass
  input: src
  config:
    cxl: |
      emit grp = grp
      emit id = id
- type: sink
  name: rows_out
  input: pass
  config:
    name: rows_out
    type: csv
    path: rows_out.csv
"#,
        ],
    )
}

/// A bounded preview reads at most its limit from each Source and writes
/// those rows through every step, a Sink fed by a Transform that a full
/// run would fuse with its Source included.
#[test]
fn a_preview_of_a_fusable_transform_chain_writes_its_rows() {
    let csv = "grp,id\ng0,a\ng1,b\ng2,c\ng3,d\ng0,e\n".to_string();
    let run = run_with(
        &fusable_transform_sink(),
        HashMap::from([csv_reader("src", csv)]),
        &["rows_out"],
        PipelineRunParams::default(),
        Some(RunPolicy::new(
            std::num::NonZeroUsize::MIN,
            PreviewPolicy::RecordsPerSource(std::num::NonZeroU64::new(2).unwrap()),
        )),
    );
    run.result.as_ref().expect("a bounded preview succeeds");
    assert_eq!(run.outputs["rows_out"], "grp,id\ng0,a\ng1,b\n");
}
