//! Window-level recompute value correctness under post-aggregate retract.
//!
//! Companion to `correlated_window_after_aggregate_retract.rs` and
//! `post_aggregate_recompute_determinism.rs`. Those two pin the
//! post-aggregate-window geometry where the failing predicate sits
//! INSIDE the windowed Transform's emit list — so the failing row never
//! reaches the windowed Transform's emit, and HR's exclusion flows
//! through the aggregator's recompute path alone.
//!
//! This fixture closes that gap by routing the divide-by-zero through
//! a SEPARATE downstream Transform (`gate`) that sits between the
//! windowed Transform (`running`) and the Output. Geometry:
//!
//! ```text
//! Source(CK=order_id) → Aggregate(group_by=[dept,region]) →
//!   Transform(running, window: partition=[dept], sort=[region asc]) →
//!     Transform(gate, fails on region=="north") → Output
//! ```
//!
//! `gate` fails on HR/north — the failure routes via the correlation
//! buffer (the upstream aggregate's relaxed-CK lattice keeps buffering
//! active). The detect phase decodes the synthetic
//! `$ck.aggregate.dept_region_totals` lineage on the failing cell back
//! to the source row that fed HR/north and folds that into the retract
//! scope. The deferred-region commit pass re-seeds the post-recompute
//! aggregate emits into `node_buffers[dept_region_totals]` and
//! redispatches `running` over `[HR/east, HR/west]` (the surviving
//! slice in sort_by(region asc) order); `gate` then runs over the
//! window's emits and the resulting record lands in the buffered
//! Output that the flush phase drains.
//!
//! `gate`'s emit deliberately MUTATES `running_total` (adds 1 on the
//! success path) rather than passing it through. Under deferred
//! dispatch the windowed Transform's emit and `gate`'s emit both run
//! at commit-time on post-recompute upstream data, so the writer sees
//! `gate`'s mutated value. Under the prior value-equality replay
//! architecture the same fixture would silently produce wrong output
//! because the substitution would fail to match a mutated record
//! against the recompute Delta — that failure mode is the reason this
//! fixture exists.
//!
//! The load-bearing assertion is HR/west's `running_total == 36` —
//! cumulative sum (35 = 30 + 5, after HR/north's 10 is removed) plus
//! `gate`'s +1 mutation. A 46 (45 pre-retract sum + 1) means the
//! windowed Transform did not re-evaluate against the post-recompute
//! aggregate emit. A 35 (post-recompute sum WITHOUT `gate`'s
//! mutation) means `gate` did not run at commit and the writer saw
//! the window's emit directly. A 31 (30 + 1, lone surviving row's
//! own total + mutation) means deferred dispatch keyed off an empty
//! partition, which would itself be a bug.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

fn run_pipeline(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = crate::config::parse_config(yaml).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: config
            .pipeline
            .vars
            .as_ref()
            .map(crate::config::convert_pipeline_vars)
            .unwrap_or_default(),
        shutdown_token: None,
    };

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = PipelineExecutor::run_with_readers_writers(
        &config,
        &primary,
        readers,
        writers.into(),
        &params,
    )?;
    Ok((report.counters, report.dlq_entries, buf.as_string()))
}

/// `gate`'s emit deliberately mutates `running_total` on the success
/// path — adding 1 to whatever the windowed Transform produced — so
/// that the test proves deferred-dispatch's commit-time evaluation
/// flows through to the writer:
///
/// ```text
/// emit running_total = running_total + 1 / (if region == "north" then 0 else 1)
/// ```
///
/// For `region == "north"`, the divisor is `0`, so the evaluator
/// returns `DivisionByZero` ("division by zero") and the failing record
/// is parked in the correlation buffer (correlation buffering is active
/// because the upstream aggregate's `group_by` omits the source CK).
/// For non-north rows, `1 / 1 == 1`, so `running_total + 1` is the
/// emitted value. The +1 mutation is load-bearing: under deferred
/// dispatch `gate` runs ONCE at commit-time on the post-recompute
/// window emit, so the writer must see the post-recompute window value
/// + 1. A test that used a value-identity gate (e.g. `+ 0 /`) could
/// not distinguish "gate ran at commit on corrected data" from "the
/// pre-retract gate emit happened to value-equal the post-recompute
/// emit"; the +1 mutation makes the two outcomes structurally
/// different at the writer.
const PIPELINE: &str = r#"
pipeline:
  name: window_recompute_correctness
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: region, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_region_totals
  input: src
  config:
    group_by: [department, region]
    cxl: |
      emit department = department
      emit region = region
      emit total = sum(amount)
- type: transform
  name: running
  input: dept_region_totals
  config:
    cxl: |
      emit department = department
      emit region = region
      emit total = total
      emit running_total = $window.cumulative_sum(total)
    analytic_window:
      group_by: [department]
      sort_by:
        - field: region
          order: asc
- type: transform
  name: gate
  input: running
  config:
    cxl: |
      emit department = department
      emit region = region
      emit total = total
      emit running_total = running_total + 1 / (if region == "north" then 0 else 1)
- type: output
  name: out
  input: gate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;

const CSV: &str = "\
order_id,department,region,amount
O1,HR,east,30
O2,HR,north,10
O3,HR,west,5
O4,ENG,east,300
O5,ENG,west,50
";

/// Pre-retract: the `running` Transform admits all three HR rows
/// (east at running_total=30, north at 40, west at 45) into its window's
/// `partition_outputs[HR]`. ENG's two rows (east at 300, west at 350)
/// land in `partition_outputs[ENG]`. `gate` then evaluates each and
/// fails on HR/north (`0 / 0`), parking that record in the correlation
/// buffer keyed by the synthetic `$ck.aggregate.dept_region_totals`
/// column the relaxed aggregate stamped at finalize.
///
/// Detect: the trigger cell carries one synthetic CK lookup, expanding
/// to the contributing source row id (O2, rn=2). The running window's
/// HR partition retract list drops the arena position of HR/north only;
/// ENG is untouched.
///
/// Recompute: walks `[HR/east, HR/west]` (the surviving slice in
/// sort_by(region asc) order) and emits per-row Deltas. HR/east's
/// `cumulative_sum` is unchanged at 30; HR/west's drops from 45 to 35
/// because HR/north's 10 is no longer in the prefix.
///
/// Deferred-region commit pass: dispatches `running` and then `gate`
/// over the post-recompute aggregate emits. The window's
/// `cumulative_sum` evaluates fresh on `[HR/east, HR/west]`; `gate`
/// then runs on each of `running`'s emits and adds 1, landing the
/// final value in the buffered Output that flush drains. There is no
/// substitution against pre-retract records — the writer never sees
/// the pre-retract emits because they were never produced (forward
/// pass short-circuited the deferred consumers).
///
/// Strict assertions:
///
/// 1. `dlq_count == 1` — exactly the HR/north trigger.
/// 2. The DLQ row carries `department=HR` and `region=north` and the
///    `division by zero` error message.
/// 3. `partitions_dispatched >= 1` — the running windowed Transform
///    is dispatched on the deferred-region commit pass.
/// 4. Output row count == 4 (HR/north excluded; HR/east + HR/west +
///    ENG/east + ENG/west reach the writer).
/// 5. **Load-bearing**: HR/west's `running_total == 36` — proves
///    BOTH that the window-level cumulative_sum excluded HR/north's
///    10 (post-recompute aggregate emit) AND that `gate`'s +1
///    mutation flowed through to the writer (commit-time evaluation
///    on the post-recompute window emit).
/// 6. HR/east's `running_total == 31` (= 30 + `gate`'s +1; first in
///    partition so cumulative_sum is unchanged at 30).
/// 7. ENG/east's `running_total == 301`; ENG/west's `running_total ==
///    351` — ENG partition is not in the recompute scope, but `gate`
///    still runs on every surviving record at commit, so the +1
///    mutation appears uniformly across all written rows.
#[test]
fn post_aggregate_window_recompute_corrects_running_total() {
    let (counters, dlq, output) =
        run_pipeline(PIPELINE, CSV).expect("post-aggregate-window pipeline must execute");

    assert_eq!(
        counters.dlq_count, 1,
        "exactly one row (HR/north) hits division by zero in `gate`"
    );
    assert_eq!(dlq.len(), 1);

    let trigger = &dlq[0];
    assert!(trigger.trigger, "the single DLQ entry is the trigger");
    assert_eq!(trigger.error_message, "division by zero");
    assert!(
        trigger
            .original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "HR")),
        "trigger record must carry department=HR"
    );
    assert!(
        trigger
            .original_record
            .values()
            .iter()
            .any(|v| matches!(v, clinker_record::Value::String(s) if s.as_ref() == "north")),
        "trigger record must carry region=north"
    );

    assert!(
        counters.retraction.partitions_dispatched >= 1,
        "the running windowed Transform must dispatch on the deferred-region \
         commit pass; got {}",
        counters.retraction.partitions_dispatched,
    );

    let mut by_dept_region: HashMap<(String, String), HashMap<String, String>> = HashMap::new();
    let mut lines = output.lines();
    let header_line = lines.next().expect("output csv must carry a header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers
        .iter()
        .position(|h| *h == "department")
        .expect("department column present");
    let region_idx = headers
        .iter()
        .position(|h| *h == "region")
        .expect("region column present");
    let mut data_row_count = 0;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let v: Vec<&str> = line.split(',').collect();
        let row: HashMap<String, String> = headers
            .iter()
            .zip(v.iter())
            .map(|(h, val)| ((*h).to_string(), (*val).to_string()))
            .collect();
        by_dept_region.insert((v[dept_idx].to_string(), v[region_idx].to_string()), row);
        data_row_count += 1;
    }
    assert_eq!(
        data_row_count, 4,
        "HR/north must be excluded; HR/east, HR/west, ENG/east, ENG/west reach the writer"
    );
    assert!(
        !by_dept_region.contains_key(&("HR".to_string(), "north".to_string())),
        "HR/north must NOT reach the writer (DLQ trigger)"
    );

    let hr_east = by_dept_region
        .get(&("HR".to_string(), "east".to_string()))
        .expect("HR/east row reaches the writer");
    assert_eq!(hr_east.get("total").map(String::as_str), Some("30"));
    assert_eq!(
        hr_east.get("running_total").map(String::as_str),
        Some("31"),
        "HR/east is the first row in the partition; cumulative_sum is 30 \
         and `gate` adds 1 → 31"
    );

    let hr_west = by_dept_region
        .get(&("HR".to_string(), "west".to_string()))
        .expect("HR/west row reaches the writer");
    assert_eq!(hr_west.get("total").map(String::as_str), Some("5"));
    assert_eq!(
        hr_west.get("running_total").map(String::as_str),
        Some("36"),
        "HR/west's running_total must be 36 (= 30 + 5 from cumulative_sum, \
         + 1 from `gate`) after the deferred-region commit pass. A 46 means \
         the windowed Transform did not re-evaluate against the post-recompute \
         aggregate emit. A 35 means `gate` did not run at commit and the writer \
         saw the window's emit directly. Either failure mode would prove the \
         architecture broken in a way no other test catches."
    );

    let eng_east = by_dept_region
        .get(&("ENG".to_string(), "east".to_string()))
        .expect("ENG/east row reaches the writer");
    assert_eq!(eng_east.get("total").map(String::as_str), Some("300"));
    assert_eq!(
        eng_east.get("running_total").map(String::as_str),
        Some("301"),
        "ENG/east is unaffected by HR's retract; cumulative_sum is 300 \
         and `gate` adds 1 → 301"
    );

    let eng_west = by_dept_region
        .get(&("ENG".to_string(), "west".to_string()))
        .expect("ENG/west row reaches the writer");
    assert_eq!(eng_west.get("total").map(String::as_str), Some("50"));
    assert_eq!(
        eng_west.get("running_total").map(String::as_str),
        Some("351"),
        "ENG/west = 300 + 50 from cumulative_sum + 1 from `gate` = 351; the \
         ENG partition is not in the recompute scope but `gate` still runs \
         on every surviving record at commit time"
    );
}
