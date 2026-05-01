//! Window-level recompute value correctness under post-aggregate retract.
//!
//! Companion to `correlated_window_after_aggregate_retract.rs` and
//! `post_aggregate_recompute_determinism.rs`. Those two pin the
//! post-aggregate-window geometry where the failing predicate sits
//! INSIDE the windowed Transform's emit list — so the failing row never
//! reaches `partition_outputs`, and HR's exclusion flows through the
//! aggregator's recompute path. As a result `partitions_recomputed`
//! ends up zero in those tests; the window-level recompute is exercised
//! only for run-to-run determinism, not for value correctness.
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
//! `running` admits all three HR rows (east, north, west) into its
//! window's `partition_outputs`. `gate` fails on HR/north — the failure
//! routes via the correlation buffer (the upstream aggregate's relaxed-CK
//! lattice keeps buffering active). The detect phase decodes the
//! synthetic `$ck.aggregate.dept_region_totals` lineage on the failing
//! cell back to the source row that fed HR/north, then drops that
//! source row's id into the running window's HR partition retract list.
//! `partitions_recomputed` increments to 1; the recompute pass reruns
//! `cumulative_sum` over `[HR/east, HR/west]` (the surviving slice in
//! sort_by(region asc) order) and emits Deltas pairing pre-retract
//! outputs with post-retract ones. Replay substitutes the new rows into
//! the buffered Output.
//!
//! The load-bearing assertion is HR/west's `running_total == 35` — the
//! cumulative sum after HR/north's contribution is removed. A
//! 45 (the pre-retract value) means the window-level recompute's
//! Delta substitution did not land in the buffered Output. A 30 (the
//! lone surviving row's own `total`) means the recompute keyed off an
//! empty partition, which would itself be a bug.

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
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
        Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
            as Box<dyn std::io::Read + Send>,
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)?;
    Ok((report.counters, report.dlq_entries, buf.as_string()))
}

/// `gate`'s emit produces records value-identical to `running`'s emit
/// on the success path:
///
/// ```text
/// emit running_total = running_total + 0 / (if region == "north" then 0 else 1)
/// ```
///
/// For `region == "north"`, the divisor is `0`, so the evaluator
/// returns `DivisionByZero` ("division by zero") and the failing record
/// is parked in the correlation buffer (correlation buffering is active
/// because the upstream aggregate's `group_by` omits the source CK).
/// For non-north rows, `0 / 1 == 0`, so `running_total + 0 ==
/// running_total` and the emitted record is byte-identical to the input
/// from `running`. That identity is load-bearing: replay's
/// `substitute_in_correlation_buffers` matches the recompute Delta's
/// `retract_old_row` (the running emit) against buffered records by
/// `Value`-list equality; if `gate` perturbed the record on the success
/// path, the substitution would silently no-op and the recomputed
/// `running_total = 35` would never reach the writer.
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
      emit running_total = running_total + 0 / (if region == "north" then 0 else 1)
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
/// Replay: substitutes the new rows into the buffered Output through
/// the `gate` Transform's value-identical projection (see PIPELINE
/// docstring for why `gate` is a no-op on the success path).
///
/// Strict assertions:
///
/// 1. `dlq_count == 1` — exactly the HR/north trigger.
/// 2. The DLQ row carries `department=HR` and `region=north` and the
///    `division by zero` error message.
/// 3. `partitions_recomputed == 1` — the running window's HR partition
///    is recomputed once. ENG is untouched.
/// 4. Output row count == 4 (HR/north excluded; HR/east + HR/west +
///    ENG/east + ENG/west reach the writer).
/// 5. **Load-bearing**: HR/west's `running_total == 35` — proves the
///    window-level recompute excluded HR/north's 10 from the cumulative
///    sum AND that the resulting Delta successfully substituted into the
///    buffered Output.
/// 6. HR/east's `running_total == 30` (unchanged; first in partition).
/// 7. ENG/east's `running_total == 300`; ENG/west's `running_total ==
///    350` — ENG partition is not recomputed.
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

    assert_eq!(
        counters.retraction.partitions_recomputed, 1,
        "the running window's HR partition is recomputed exactly once; \
         ENG is untouched. A value of 0 here would mean the failing \
         row never reached the window's `partition_outputs` and the \
         test geometry collapses to the existing in-emit-failure tests."
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
        Some("30"),
        "HR/east is the first row in the partition; cumulative_sum is unchanged at 30"
    );

    let hr_west = by_dept_region
        .get(&("HR".to_string(), "west".to_string()))
        .expect("HR/west row reaches the writer");
    assert_eq!(hr_west.get("total").map(String::as_str), Some("5"));
    assert_eq!(
        hr_west.get("running_total").map(String::as_str),
        Some("35"),
        "HR/west's running_total must be 35 (= 30 + 5) after the window-level \
         recompute drops HR/north's 10 from the cumulative prefix. A 45 means \
         the recompute Delta failed to substitute into the buffered Output \
         (the load-bearing failure mode this test exists to catch)."
    );

    let eng_east = by_dept_region
        .get(&("ENG".to_string(), "east".to_string()))
        .expect("ENG/east row reaches the writer");
    assert_eq!(eng_east.get("total").map(String::as_str), Some("300"));
    assert_eq!(
        eng_east.get("running_total").map(String::as_str),
        Some("300"),
        "ENG/east is unaffected by HR's retract"
    );

    let eng_west = by_dept_region
        .get(&("ENG".to_string(), "west".to_string()))
        .expect("ENG/west row reaches the writer");
    assert_eq!(eng_west.get("total").map(String::as_str), Some("50"));
    assert_eq!(
        eng_west.get("running_total").map(String::as_str),
        Some("350"),
        "ENG/west = 300 + 50 = 350; the ENG partition is not in the recompute scope"
    );
}
