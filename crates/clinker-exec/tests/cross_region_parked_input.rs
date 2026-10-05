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
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders};
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

/// One finished run: its report, its Outputs' bytes and its dead-letter rows.
struct Run {
    report: ExecutionReport,
    output: String,
    dead_letters: Vec<dlq_sink::DlqRow>,
}

/// Run `yaml` over the given `(source, csv)` inputs, compiled against the
/// default context and writing the one Output `out`.
fn run(yaml: &str, sources: &[(&str, String)]) -> Result<Run, PipelineError> {
    run_in(yaml, CompileContext::default(), sources, &["out"])
}

/// Run `yaml` compiled against `context` over the given `(source, csv)`
/// inputs, collecting every Output in `outputs` (in that order, each under
/// its name).
fn run_in(
    yaml: &str,
    context: CompileContext,
    sources: &[(&str, String)],
    outputs: &[&str],
) -> Result<Run, PipelineError> {
    let config: PipelineConfig = clinker_plan::yaml::from_str(yaml).expect("fixture parses");
    let plan = config.compile(&context).expect("fixture compiles");
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
    let buffers: Vec<SharedBuffer> = outputs.iter().map(|_| SharedBuffer::new()).collect();
    let writers: HashMap<String, Box<dyn Write + Send>> = outputs
        .iter()
        .zip(&buffers)
        .map(|(name, buffer)| {
            (
                (*name).to_string(),
                Box::new(buffer.clone()) as Box<dyn Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        execution_id: "cross-region-parked-input".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    let sink = dlq_sink::CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers_in_context(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
        context,
    )?;
    let output = if let [only] = buffers.as_slice() {
        only.as_string()
    } else {
        outputs
            .iter()
            .zip(&buffers)
            .map(|(name, buffer)| format!("== {name}\n{}", buffer.as_string()))
            .collect()
    };
    Ok(Run {
        report,
        output,
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

/// Where the deferred Combine's build side comes from: the node whose rows
/// cross into the deferred region and are parked.
#[derive(Clone, Copy)]
enum BuildSide {
    /// `dept_lookup` itself.
    Source,
    /// The `wanted` branch of Route `split` over `dept_lookup`.
    RouteBranch,
    /// The kept (main) port of Cull `trim` over `dept_lookup`.
    CullPort,
    /// The `wanted` branch of Route `split` over Transform `shout`, which
    /// computes each note in upper case from `dept_lookup`'s.
    ComputedRouteBranch,
}

impl BuildSide {
    /// The node whose rows are parked, which their charge and spill name.
    fn producer(self) -> &'static str {
        match self {
            Self::Source => "dept_lookup",
            Self::RouteBranch | Self::ComputedRouteBranch => "split",
            Self::CullPort => "trim",
        }
    }

    /// The Outputs the pipeline writes.
    fn outputs(self) -> &'static [&'static str] {
        match self {
            Self::Source => &["out"],
            Self::RouteBranch | Self::ComputedRouteBranch => &["out", "unwanted_out"],
            Self::CullPort => &["out", "removed_out"],
        }
    }
}

/// [`CASCADING_PIPELINE`] at `memory.limit` 512M, with a build side of
/// `LOOKUP_FILLER_ROWS` extra departments, each carrying a
/// `NOTE_BYTES`-character note, reached through `build`. Nothing in the
/// output reads the notes; they make the parked rows large.
fn parked_pipeline(build: BuildSide) -> String {
    let (extra_nodes, build_ref) = match build {
        BuildSide::Source => ("", "dept_lookup"),
        BuildSide::RouteBranch => (
            r#"- type: route
  name: split
  input: dept_lookup
  config:
    mode: exclusive
    conditions:
      wanted: "budget >= 0"
    default: unwanted
- type: sink
  name: unwanted_out
  input: split.unwanted
  config:
    name: unwanted_out
    path: unwanted.csv
    type: csv
    include_unmapped: true
"#,
            "split.wanted",
        ),
        BuildSide::CullPort => (
            r#"- type: cull
  name: trim
  input: dept_lookup
  config:
    partition_by: [department]
    removed_to: removed
    rules:
      - name: negative_budget
        drop_group_when: "sum(if budget < 0 then 1 else 0) > 0"
- type: sink
  name: removed_out
  input: trim.removed
  config:
    name: removed_out
    path: removed.csv
    type: csv
    include_unmapped: true
"#,
            "trim",
        ),
        BuildSide::ComputedRouteBranch => (
            r#"- type: transform
  name: shout
  input: dept_lookup
  config:
    cxl: |
      emit department = department
      emit budget = budget
      emit note = note.upper()
- type: route
  name: split
  input: shout
  config:
    mode: exclusive
    conditions:
      wanted: "budget >= 0"
    default: unwanted
- type: sink
  name: unwanted_out
  input: split.unwanted
  config:
    name: unwanted_out
    path: unwanted.csv
    type: csv
    include_unmapped: true
"#,
            "split.wanted",
        ),
    };
    format!(
        r#"
pipeline:
  name: parked_cross_region
  memory: {{ limit: "512M", backpressure: spill }}
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
      - {{ name: order_id, type: string }}
      - {{ name: department, type: string }}
      - {{ name: amount, type: int }}
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
      - {{ name: department, type: string }}
      - {{ name: budget, type: int }}
      - {{ name: note, type: string }}
{extra_nodes}- type: combine
  name: enriched
  input:
    p: probe_xform
    b: {build_ref}
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
"#
    )
}

/// Extra build-side departments, none of which any order names.
const LOOKUP_FILLER_ROWS: usize = 1_000;

/// Characters in every build-side row's note.
const NOTE_BYTES: usize = 512;

/// Characters in every note of the second, wider-note run that shows who is
/// charged for the notes.
const WIDE_NOTE_BYTES: usize = 4 * NOTE_BYTES;

/// Characters in every note of the widest run, which the composition body
/// compares against a `WIDE_NOTE_BYTES` run.
const WIDER_NOTE_BYTES: usize = 8 * NOTE_BYTES;

/// The columns a build-side row is authored with: department, budget, note.
const AUTHORED_LOOKUP_COLUMNS: u64 = 3;

/// HR and ENG, then the filler departments, each with a `note_bytes`-character
/// note; every seventh filler has a negative budget (the rows Route `split`
/// sends to `unwanted` and Cull `trim` removes).
fn lookup_csv_with(note_bytes: usize) -> String {
    let note = "n".repeat(note_bytes);
    let mut csv = format!("department,budget,note\nHR,100,{note}\nENG,500,{note}\n");
    for row in 0..LOOKUP_FILLER_ROWS {
        let budget = if row % 7 == 0 {
            -(row as i64) - 1
        } else {
            row as i64 * 10 + 1
        };
        csv.push_str(&format!("F{row:05},{budget},{note}\n"));
    }
    csv
}

/// Run the parked-rows pipeline over `build` with ample memory and
/// `note_bytes`-character notes, with or without HR's orders (with them the
/// commit iterates twice).
fn run_parked_with(build: BuildSide, with_hr: bool, note_bytes: usize) -> Run {
    run_in(
        &parked_pipeline(build),
        CompileContext::default(),
        &[
            ("orders", orders_csv(with_hr)),
            ("dept_lookup", lookup_csv_with(note_bytes)),
        ],
        build.outputs(),
    )
    .unwrap_or_else(|error| panic!("the {} build side completes: {error}", build.producer()))
}

/// Every line a run wrote, sorted, so two runs compare by content.
fn sorted_lines(output: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = output.lines().filter(|line| !line.is_empty()).collect();
    lines.sort_unstable();
    lines
}

/// Build-side rows with a non-negative budget: what Route `split` sends to
/// `wanted` and Cull `trim` keeps.
fn kept_lookup_rows() -> u64 {
    2 + (0..LOOKUP_FILLER_ROWS).filter(|row| row % 7 != 0).count() as u64
}

/// The highest charge `run` recorded under the node named `node`, or 0.
fn peak(run: &Run, node: &str) -> u64 {
    run.report
        .per_node_peak_charged_bytes
        .get(node)
        .copied()
        .unwrap_or(0)
}

/// `producer` parked `parked_rows` of `dept_lookup`'s rows in both runs,
/// whose notes differ only in length (`NOTE_BYTES` in `narrow`,
/// `WIDE_NOTE_BYTES` in `wide`). The parked rows are charged under the
/// producer, and their notes once, under the Source that read them: longer
/// notes raise `dept_lookup`'s charge and leave the producer's exactly as it
/// was, since its parked copy shares the notes and is not charged again.
fn assert_notes_charged_once(narrow: &Run, wide: &Run, producer: &str, parked_rows: u64) {
    assert_parked_and_source_charges(narrow, producer, parked_rows);
    source_rise_covers_note_growth(narrow, wide, NOTE_BYTES, WIDE_NOTE_BYTES, parked_rows);
    assert_producer_charge_unchanged(producer, narrow, &[wide]);
}

/// In `narrow`, whose notes are `NOTE_BYTES` long, `producer`'s parked rows
/// are charged under it, and `dept_lookup`, the Source that read them,
/// carries at least their notes.
fn assert_parked_and_source_charges(narrow: &Run, producer: &str, parked_rows: u64) {
    let value = std::mem::size_of::<clinker_record::Value>() as u64;
    assert!(
        peak(narrow, producer) >= parked_rows * AUTHORED_LOOKUP_COLUMNS * value,
        "`{producer}`'s parked rows are charged under it ({parked_rows} rows; peak {})",
        peak(narrow, producer)
    );
    assert!(
        peak(narrow, "dept_lookup") >= parked_rows * NOTE_BYTES as u64,
        "the notes are charged under `dept_lookup`, the Source that read them \
         ({parked_rows} rows of {NOTE_BYTES}-byte notes; peak {})",
        peak(narrow, "dept_lookup")
    );
}

/// From `shorter` to `longer`, whose notes are `shorter_bytes` and
/// `longer_bytes` characters long, `dept_lookup`'s charge rises by at least
/// the notes' growth over `parked_rows` rows. Returns the rise.
fn source_rise_covers_note_growth(
    shorter: &Run,
    longer: &Run,
    shorter_bytes: usize,
    longer_bytes: usize,
    parked_rows: u64,
) -> u64 {
    let rise = peak(longer, "dept_lookup").saturating_sub(peak(shorter, "dept_lookup"));
    assert!(
        rise >= parked_rows * (longer_bytes - shorter_bytes) as u64,
        "notes of {longer_bytes} bytes instead of {shorter_bytes} are charged to \
         `dept_lookup` (peak {} against {})",
        peak(longer, "dept_lookup"),
        peak(shorter, "dept_lookup")
    );
    rise
}

/// `producer`'s charge in every run of `others` is exactly its charge in
/// `narrow`: its parked copy shares the notes `dept_lookup` already holds
/// charged, so longer notes do not charge it again.
fn assert_producer_charge_unchanged(producer: &str, narrow: &Run, others: &[&Run]) {
    for other in others {
        assert_eq!(
            peak(other, producer),
            peak(narrow, producer),
            "`{producer}` is not charged again for notes `dept_lookup` already holds charged"
        );
    }
}

/// With ample memory, `build`'s rows are parked under its producer, charged
/// there, read again by the commit's second iteration and never written to
/// disk; the run converges to what a run without HR's orders writes. Below
/// the Source, the notes are charged once, under `dept_lookup`
/// ([`assert_notes_charged_once`]).
fn assert_parks_and_rereads(build: BuildSide, parked_rows: u64) {
    let converged = run_parked_with(build, true, NOTE_BYTES);
    let reference = run_parked_with(build, false, NOTE_BYTES);
    assert!(
        converged.report.counters.retraction.iterations >= 2,
        "the commit re-reads the parked rows on a second iteration; got {}",
        converged.report.counters.retraction.iterations
    );
    assert_eq!(reference.report.counters.retraction.iterations, 1);
    assert!(
        converged.report.per_stage_spill_bytes_written.is_empty(),
        "ample memory writes nothing to disk: {:?}",
        converged.report.per_stage_spill_bytes_written
    );
    let producer = build.producer();
    if producer == "dept_lookup" {
        let peak = peak(&converged, producer);
        assert!(
            peak >= parked_rows * NOTE_BYTES as u64,
            "`{producer}`'s parked rows are charged at their resident size, notes included \
             ({parked_rows} rows of {NOTE_BYTES}-byte notes; peak charged {peak})"
        );
    } else {
        let wide = run_parked_with(build, true, WIDE_NOTE_BYTES);
        assert_notes_charged_once(&converged, &wide, producer, parked_rows);
    }
    assert_eq!(
        sorted_lines(&converged.output),
        sorted_lines(&reference.output),
        "the converged run writes what a run without the failing group writes"
    );
    assert_eq!(converged.report.counters.dlq_count, 1, "HR's row, once");
}

#[test]
fn parked_cross_region_input_stays_resident_with_ample_memory() {
    assert_parks_and_rereads(BuildSide::Source, 2 + LOOKUP_FILLER_ROWS as u64);
}

#[test]
fn route_branch_crossing_parks_under_the_route() {
    assert_parks_and_rereads(BuildSide::RouteBranch, kept_lookup_rows());
}

#[test]
fn cull_port_crossing_parks_under_the_cull() {
    assert_parks_and_rereads(BuildSide::CullPort, kept_lookup_rows());
}

/// Notes a Transform computes are new text no Source admitted: once the
/// Route parks its rows, the parked copy may outlive the rows that carried
/// the text's only charge, so the Route's park charges the notes, and longer
/// notes raise its charge by at least their growth.
#[test]
fn computed_text_crossing_is_charged_under_the_route() {
    let build = BuildSide::ComputedRouteBranch;
    let narrow = run_parked_with(build, true, NOTE_BYTES);
    let wide = run_parked_with(build, true, WIDE_NOTE_BYTES);
    assert!(
        narrow.report.counters.retraction.iterations >= 2,
        "the commit re-reads the parked rows on a second iteration; got {}",
        narrow.report.counters.retraction.iterations
    );
    assert!(
        narrow.report.per_stage_spill_bytes_written.is_empty()
            && wide.report.per_stage_spill_bytes_written.is_empty(),
        "ample memory writes nothing to disk"
    );
    let parked_rows = kept_lookup_rows();
    assert!(
        peak(&narrow, "split") >= parked_rows * NOTE_BYTES as u64,
        "the Route's park charges the computed notes ({parked_rows} rows of \
         {NOTE_BYTES}-byte notes; peak {})",
        peak(&narrow, "split")
    );
    assert!(
        peak(&wide, "split").saturating_sub(peak(&narrow, "split"))
            >= parked_rows * (WIDE_NOTE_BYTES - NOTE_BYTES) as u64,
        "four-times-longer computed notes are charged to the Route's park \
         (peak {} against {})",
        peak(&wide, "split"),
        peak(&narrow, "split")
    );
    assert!(
        narrow.output.contains(",500,"),
        "ENG carries its budget through the computed build side: {}",
        narrow.output
    );
}

/// The relaxed-key aggregate and the Combine live in a composition body,
/// whose `lookup` input port crosses into the body's deferred region. Its
/// rows are parked under the body's edge namespace, and the commit's second
/// iteration re-enters the body and reads them again.
const BODY_COMPOSITION: &str = r#"_compose:
  name: parked_body
  inputs:
    orders:
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
    lookup:
      schema:
        - { name: department, type: string }
        - { name: budget, type: int }
        - { name: note, type: string }
  outputs:
    out: enriched
  config_schema: {}

nodes:
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
  - type: combine
    name: enriched
    input:
      p: dept_totals
      b: lookup
    config:
      where: 'p.department == b.department'
      match: first
      on_miss: skip
      cxl: |
        emit department = p.department
        emit total = p.total
        emit budget = b.budget
      propagate_ck: driver
"#;

const BODY_PIPELINE: &str = r#"
pipeline:
  name: parked_body_crossing
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
- type: source
  name: dept_lookup
  config:
    name: dept_lookup
    path: dept_lookup.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: budget, type: int }
      - { name: note, type: string }
- type: composition
  name: enrich
  input: orders
  use: ../compositions/parked_body.comp.yaml
  inputs:
    orders: orders
    lookup: dept_lookup
- type: transform
  name: ratio
  input: enrich
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

#[test]
fn composition_body_crossing_parks_under_its_body_key() {
    let workspace = tempfile::tempdir().expect("workspace");
    let compositions = workspace.path().join("compositions");
    std::fs::create_dir_all(&compositions).expect("compositions dir");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipelines dir");
    std::fs::write(compositions.join("parked_body.comp.yaml"), BODY_COMPOSITION)
        .expect("write the body");
    let run_body = |with_hr: bool, note_bytes: usize| {
        run_in(
            BODY_PIPELINE,
            CompileContext::with_pipeline_dir(
                workspace.path(),
                std::path::PathBuf::from("pipelines"),
            ),
            &[
                ("orders", orders_csv(with_hr)),
                ("dept_lookup", lookup_csv_with(note_bytes)),
            ],
            &["out"],
        )
        .expect("the body crossing completes")
    };
    let converged = run_body(true, NOTE_BYTES);
    let reference = run_body(false, NOTE_BYTES);
    assert!(
        converged.report.counters.retraction.iterations >= 2,
        "the commit re-enters the body and re-reads its parked rows; got {}",
        converged.report.counters.retraction.iterations
    );
    assert_eq!(reference.report.counters.retraction.iterations, 1);
    assert!(converged.report.per_stage_spill_bytes_written.is_empty());
    // `dept_lookup` has a second holder besides the reader that holds the
    // notes: the buffer handing its rows to the body, charged the same per
    // row whatever the notes' length. A node's figure is its largest single
    // holder, and with `NOTE_BYTES` notes that buffer is the larger, so the
    // notes' growth is measured between two widths where their holder leads.
    let parked_rows = 2 + LOOKUP_FILLER_ROWS as u64;
    assert_parked_and_source_charges(&converged, "lookup", parked_rows);
    let wide = run_body(true, WIDE_NOTE_BYTES);
    let wider = run_body(true, WIDER_NOTE_BYTES);
    let rise = source_rise_covers_note_growth(
        &wide,
        &wider,
        WIDE_NOTE_BYTES,
        WIDER_NOTE_BYTES,
        parked_rows,
    );
    let growth = parked_rows * (WIDER_NOTE_BYTES - WIDE_NOTE_BYTES) as u64;
    assert!(
        rise < 2 * growth,
        "`dept_lookup` charges the notes once, not twice (rise {rise} against growth {growth})"
    );
    assert_producer_charge_unchanged("lookup", &converged, &[&wide, &wider]);
    assert_eq!(
        sorted_lines(&converged.output),
        sorted_lines(&reference.output)
    );
    assert!(
        converged.output.contains("ENG,600,500"),
        "{}",
        converged.output
    );
    assert_eq!(converged.report.counters.dlq_count, 1);
}

/// Two relaxed-key aggregates over the same orders. `totals_x`, downstream
/// of `dept_totals`, runs only at the commit, where it parks its rows for
/// `joined`, which sits in `dept_peaks`'s region: a crossing raised during
/// the commit pass itself. `ratio` fails on HR, so the commit iterates, and
/// every iteration's `joined` must see only that iteration's `totals_x` rows:
/// under `match: all` a leftover row from the iteration before would double
/// a joined row.
const TWO_REGION_PIPELINE: &str = r#"
pipeline:
  name: two_regions
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
  name: totals_x
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
- type: aggregate
  name: dept_peaks
  input: orders
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit peak = max(amount)
- type: combine
  name: joined
  input:
    p: dept_peaks
    b: totals_x
  config:
    where: 'p.department == b.department'
    match: all
    on_miss: skip
    cxl: |
      emit department = p.department
      emit peak = p.peak
      emit total = b.total
    propagate_ck: driver
- type: transform
  name: ratio
  input: joined
  config:
    cxl: |
      emit department = department
      emit peak = peak
      emit total = total
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

#[test]
fn commit_pass_tees_do_not_leak_between_iterations() {
    let converged = run(TWO_REGION_PIPELINE, &[("orders", orders_csv(true))])
        .expect("a crossing raised during the commit pass is read by its consumer region");
    let reference = run(TWO_REGION_PIPELINE, &[("orders", orders_csv(false))])
        .expect("the reference run completes");
    assert!(
        converged.report.counters.retraction.iterations >= 2,
        "the commit iterates; got {}",
        converged.report.counters.retraction.iterations
    );
    assert_eq!(reference.report.counters.retraction.iterations, 1);
    assert_eq!(
        sorted_rows(&converged.output),
        sorted_rows(&reference.output),
        "each iteration joins exactly its own commit-pass rows"
    );
    assert_eq!(
        sorted_rows(&converged.output).len(),
        1,
        "one ENG row, not one per iteration: {}",
        converged.output
    );
    assert!(
        converged.output.contains("ENG,300,600"),
        "{}",
        converged.output
    );
}
