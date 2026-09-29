//! Rows a relaxed-key pipeline parks for its deferred step: a Source, Route
//! branch, Cull port or composition-body edge that crosses into a deferred
//! consumer keeps its rows until the commit, and every retraction iteration of
//! that commit reads them again, unchanged and in arrival order.

#![cfg(feature = "test-utils")]

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;
use std::io::Write;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams, SourceReaders,
};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;

/// A relaxed-key aggregate (`group_by` omits the correlation key) feeds a
/// Combine whose build side is a second Source. Everything below the
/// aggregate is deferred to the commit, so `dept_lookup`'s rows cross into
/// the deferred Combine and are parked until then. `ratio` divides by zero on
/// the HR group (its total is 60), which dead-letters HR's contributing rows
/// and forces a second retraction iteration; that iteration's Combine reads
/// `dept_lookup`'s parked rows again.
const CASCADING_PIPELINE: &str = r#"
pipeline:
  name: cross_region_cascade
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: orders
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: probe_xform
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
- type: source
  name: dept_lookup
  config:
    name: dept_lookup
    path: dept_lookup.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: budget, type: int }
- type: combine
  name: enriched
  input:
    p: probe_xform
    b: dept_lookup
  config:
    where: 'p.department == b.department'
    match: first
    on_miss: skip
    cxl: |
      emit department = p.department
      emit total = p.total
      emit budget = b.budget
    propagate_ck: driver
- type: transform
  name: ratio
  input: enriched
  config:
    cxl: |
      emit department = department
      emit total = total
      emit budget = budget
      emit ratio = 1 / (total - 60)
- type: sink
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

/// Six HR orders summing to 60 (the failing group) and three ENG orders.
const HR_ORDERS: usize = 6;

fn orders_csv(with_hr: bool) -> String {
    let mut csv = String::from("order_id,department,amount\n");
    if with_hr {
        for row in 1..=HR_ORDERS {
            csv.push_str(&format!("o{row},HR,10\n"));
        }
    }
    for (row, amount) in [(7, 100), (8, 200), (9, 300)] {
        csv.push_str(&format!("o{row},ENG,{amount}\n"));
    }
    csv
}

const LOOKUP_CSV: &str = "department,budget\nHR,100\nENG,500\n";

/// A dead letter's stage, `department` and `budget` cells, and whether it
/// is the trigger.
type DeadLetterFacts<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>, bool);

/// One finished run: its report, its Output's bytes and its dead-letter rows.
struct Run {
    report: ExecutionReport,
    output: String,
    dead_letters: Vec<dlq_sink::DlqRow>,
}

/// Run `yaml` over the given `(source, csv)` inputs, with the run's ledger
/// held to `capacity` bytes when one is given.
fn run(
    yaml: &str,
    sources: &[(&str, String)],
    capacity: Option<u64>,
) -> Result<Run, PipelineError> {
    let config: PipelineConfig = clinker_plan::yaml::from_str(yaml).expect("fixture parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture compiles");
    let readers: SourceReaders = sources
        .iter()
        .map(|(name, csv)| {
            (
                (*name).to_string(),
                clinker_exec::executor::single_file_reader(
                    "input.csv",
                    Box::new(std::io::Cursor::new(csv.clone().into_bytes())),
                ),
            )
        })
        .collect();
    let buffer = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buffer.clone()) as Box<dyn Write + Send>,
    )]);
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "cross-region-parked-input".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
        ..Default::default()
    };
    let sink = dlq_sink::CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )?;
    Ok(Run {
        report,
        output: buffer.as_string(),
        dead_letters: sink.rows(),
    })
}

/// The Output's rows below its header, sorted, so two runs compare by content.
fn sorted_rows(output: &str) -> Vec<String> {
    let mut rows: Vec<String> = output
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    rows.sort();
    rows
}

#[test]
fn cross_region_input_survives_cascading_iterations() {
    let converged = run(
        CASCADING_PIPELINE,
        &[
            ("orders", orders_csv(true)),
            ("dept_lookup", LOOKUP_CSV.to_string()),
        ],
        None,
    )
    .expect("a relaxed-key commit that iterates reads the parked build side on every iteration");
    assert!(
        converged.report.counters.retraction.iterations >= 2,
        "the fixture must force a second retraction iteration; got {}",
        converged.report.counters.retraction.iterations
    );

    // The reference never reaches HR: without HR's orders nothing fails, the
    // commit converges on its first iteration, and what it writes is what the
    // cascading run must converge to.
    let reference = run(
        CASCADING_PIPELINE,
        &[
            ("orders", orders_csv(false)),
            ("dept_lookup", LOOKUP_CSV.to_string()),
        ],
        None,
    )
    .expect("the reference run completes");
    assert_eq!(reference.report.counters.retraction.iterations, 1);
    assert!(reference.dead_letters.is_empty());
    assert_eq!(
        converged.output.lines().next(),
        reference.output.lines().next(),
        "the header is unchanged"
    );
    assert_eq!(
        sorted_rows(&converged.output),
        sorted_rows(&reference.output),
        "the converged output is exactly the ENG row, joined to its parked budget"
    );
    assert!(
        converged.output.contains(",500,"),
        "ENG carries its budget from the parked build side: {}",
        converged.output
    );

    // The failure dead-letters the HR aggregate row that `ratio` could not
    // divide, once: the iteration that re-read the parked rows neither lost
    // nor repeated it.
    let dead_letters: Vec<DeadLetterFacts<'_>> = converged
        .dead_letters
        .iter()
        .map(|row| {
            (
                row.stage(),
                row.field("department"),
                row.field("budget"),
                row.trigger(),
            )
        })
        .collect();
    assert_eq!(
        dead_letters,
        vec![(Some("transform:ratio"), Some("HR"), Some("100"), true)],
        "exactly one dead letter: HR's row, joined to its parked budget, failing in `ratio`"
    );
    assert_eq!(converged.report.counters.dlq_count, 1);
}
