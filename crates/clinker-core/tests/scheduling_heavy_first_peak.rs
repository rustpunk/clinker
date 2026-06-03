//! Memory-aware dispatch lowers peak resident consumer usage.
//!
//! Two independent `Source -> Aggregate` chains converge on a `Combine`.
//! The heavy chain accumulates a large per-group state; the light chain
//! produces a non-trivial output that must wait, materialized, for the
//! combine to consume it. Dispatch order decides what coexists in the
//! arbitrator's consumer registry at the run's high-water mark:
//!
//! - heavy-first: drain the heavy chain (release its large state) before
//!   the light chain's output has to sit alongside it, so the peak is
//!   roughly `max(heavy_state, light_state + heavy_output)`;
//! - light-first (declaration / topo order): the light chain's output is
//!   already materialized when the heavy chain builds its large state, so
//!   the peak is roughly `heavy_state + light_output` — strictly higher by
//!   the light chain's materialized output.
//!
//! Both runs process byte-identical records (fed from in-memory cursors)
//! under a fresh 100 GiB arbitrator, so `should_spill()` never fires: no
//! spill, no back-pressure, and the measured `peak_consumer_usage_bytes`
//! is decoupled from process-global RSS and deterministic under parallel
//! `cargo test`. The only difference between the two runs is the dispatch
//! order, which the scheduler derives from the per-Source volume seed:
//! when the on-disk seed files are present and sized heavy>light the
//! scheduler front-loads the heavy chain; when they are absent every seed
//! is `0` and the scheduler falls back to topological order — the prior
//! behavior. The seed is read from file metadata only; the records the run
//! actually processes come from the cursors, so the two runs are identical
//! workloads dispatched in two orders.

use clinker_core::config::CompileContext;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use std::io::Write;
use std::path::Path;

/// 100 GiB hard limit (soft floor 80 GiB) so real-process RSS can never
/// push the arbitrator into `should_spill()` — the huge-budget convention
/// that decouples consumer accounting from process RSS and keeps the
/// measured peak deterministic under a saturated parallel test run.
const HUGE_LIMIT: &str = "100G";

/// Heavy chain: a single group reduced with `sum`, so the Aggregate emits
/// ONE tiny output row but must first materialize its whole source input as
/// the `node_buffer` slot feeding it — the large reclaimable footprint the
/// scheduler front-loads, released the moment the Aggregate drains that
/// slot. Light chain: many groups reduced with `collect`, so its output is
/// a non-trivial materialized buffer that the `Combine` holds.
///
/// The two chains converge on a `Combine` so both outputs are live when the
/// join runs; the peak gap comes from the PRE-join phase, where dispatch
/// order decides whether the heavy chain's large input buffer coexists with
/// the light chain's materialized output.
///
/// The light chain is declared LAST so the topological sort (which emits the
/// most-recently-declared source first) places its Source at the lower topo
/// index — making heavy-first a genuine reorder away from topo order, not a
/// coincidence of declaration.
fn two_chain_combine_yaml(limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: heavy_first_peak
  memory:
    limit: {limit}
nodes:
  - type: source
    name: heavy_src
    config:
      name: heavy_src
      type: csv
      path: heavy.csv
      schema:
        - {{ name: k, type: string }}
        - {{ name: v, type: int }}
  - type: source
    name: light_src
    config:
      name: light_src
      type: csv
      path: light.csv
      schema:
        - {{ name: k, type: string }}
        - {{ name: v, type: string }}
  - type: aggregate
    name: heavy_agg
    input: heavy_src
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(v)
  - type: aggregate
    name: light_agg
    input: light_src
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit items = collect(v)
  - type: combine
    name: joined
    input:
      h: heavy_agg
      l: light_agg
    config:
      where: "h.k == l.k"
      match: first
      on_miss: null_fields
      cxl: |
        emit k = h.k
        emit heavy_total = h.total
        emit light_items = l.items
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// Heavy-chain CSV: one group, `rows` integer rows. The `sum` Aggregate
/// reduces this to a single tiny output row, but the whole input must first
/// materialize as the `node_buffer` feeding the Aggregate — the large,
/// reclaimable footprint that dominates the heavy chain. Every key is `g0`
/// so there is exactly one output group.
fn make_heavy_csv(rows: usize) -> String {
    let mut s = String::from("k,v\n");
    for i in 0..rows {
        s.push_str(&format!("g0,{}\n", i % 100));
    }
    s
}

/// Light-chain CSV: `groups` distinct keys, `per_group` wide string rows
/// each. The `collect` Aggregate retains every value, so the light chain's
/// OUTPUT is itself a non-trivial materialized buffer the Combine holds —
/// the term whose coexistence with the heavy chain's input buffer the
/// dispatch order decides. The first group is `g0` so it equi-joins the
/// heavy chain's single `g0` output row.
fn make_light_csv(groups: usize, per_group: usize, value_width: usize) -> String {
    let mut s = String::from("k,v\n");
    let val = "x".repeat(value_width);
    for g in 0..groups {
        for _ in 0..per_group {
            s.push_str(&format!("g{g},{val}\n"));
        }
    }
    s
}

fn write_file(dir: &Path, name: &str, bytes_len: usize) {
    let mut f = std::fs::File::create(dir.join(name)).expect("create seed file");
    // Content is irrelevant to the run — only the byte length seeds the
    // scheduler's volume estimate. Records come from the cursors below.
    f.write_all(&vec![b'x'; bytes_len])
        .expect("write seed file");
    f.flush().expect("flush seed file");
}

/// Run the two-chain-combine pipeline against an in-memory copy of each
/// chain's CSV, resolving source `path:` seeds against `anchor`. Returns
/// the run's `peak_consumer_usage_bytes` high-water mark.
fn run_peak(anchor: &Path, heavy_csv: &str, light_csv: &str, limit: &str) -> u64 {
    let yaml = two_chain_combine_yaml(limit);
    let config = clinker_core::config::parse_config(&yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(anchor, "");
    let plan = config.compile(&ctx).expect("compile");

    let readers: SourceReaders = [
        (
            "heavy_src".to_string(),
            clinker_core::executor::single_file_reader(
                "heavy.csv",
                Box::new(std::io::Cursor::new(heavy_csv.as_bytes().to_vec())),
            ),
        ),
        (
            "light_src".to_string(),
            clinker_core::executor::single_file_reader(
                "light.csv",
                Box::new(std::io::Cursor::new(light_csv.as_bytes().to_vec())),
            ),
        ),
    ]
    .into_iter()
    .collect();

    let sink = SharedBuffer::new();
    let writers: std::collections::HashMap<String, Box<dyn Write + Send>> =
        std::collections::HashMap::from([(
            "out".to_string(),
            Box::new(sink) as Box<dyn Write + Send>,
        )]);

    let params = PipelineRunParams {
        execution_id: "heavy-first-peak".to_string(),
        batch_id: "heavy-first-peak".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers_in_context(
        &plan, readers, writers, &params, ctx,
    )
    .expect("pipeline executes");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");
    assert_eq!(
        report.cumulative_spill_bytes, 0,
        "the 100 GiB budget must keep the run entirely off the spill path"
    );
    report.peak_consumer_usage_bytes
}

/// Minimal `Write` sink that discards bytes; the test only inspects the
/// arbitrator peak, never the emitted records.
#[derive(Clone, Default)]
struct SharedBuffer;

impl SharedBuffer {
    fn new() -> Self {
        Self
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn heavy_first_dispatch_lowers_peak_consumer_usage() {
    // Heavy chain: one group, many rows -> a large input `node_buffer`
    // feeding a `sum` Aggregate that emits a single tiny output row. Light
    // chain: many wide `collect` groups -> a materialized output buffer far
    // larger than the source-ingest residue, so when it is held (light-first
    // order) alongside the heavy chain's large input buffer the peak is
    // visibly higher than when the heavy chain drains first.
    let heavy_csv = make_heavy_csv(40_000);
    let light_csv = make_light_csv(12_000, 2, 64);

    // Identical records feed both runs; only the dispatch order differs.
    let tmp_heavy_first = tempfile::tempdir().unwrap();
    let tmp_prior = tempfile::tempdir().unwrap();

    // Heavy-first run: seed files present, sized heavy >> light, so the
    // scheduler front-loads the heavy chain's Source.
    write_file(tmp_heavy_first.path(), "heavy.csv", 8 * 1024 * 1024);
    write_file(tmp_heavy_first.path(), "light.csv", 64 * 1024);
    let peak_heavy_first = run_peak(tmp_heavy_first.path(), &heavy_csv, &light_csv, HUGE_LIMIT);

    // Prior-order run: seed files ABSENT (empty temp dir), so every Source
    // seeds `0` and the scheduler falls back to topological order — the
    // light chain (lower topo index) dispatches first.
    let peak_prior = run_peak(tmp_prior.path(), &heavy_csv, &light_csv, HUGE_LIMIT);

    // The structural gap is the light chain's materialized `collect` output
    // (~1.5 MB for 12k groups), which is held alongside the heavy chain's
    // large input buffer only in light-first order. Require the heavy-first
    // peak to be lower by at least 1 MiB — far above the few-hundred-KB
    // source-ingest residue that is the only term subject to ingest-thread
    // timing, so the assertion is robust under parallel `cargo test` rather
    // than riding on a razor-thin margin.
    const MIN_GAP: u64 = 1024 * 1024;
    assert!(
        peak_heavy_first + MIN_GAP <= peak_prior,
        "heavy-first dispatch must lower the peak consumer-usage high-water by at least {MIN_GAP} \
         bytes (the held light-chain output): heavy_first={peak_heavy_first} prior={peak_prior} \
         gap={}",
        peak_prior.saturating_sub(peak_heavy_first),
    );

    // Both peaks must be dominated by the heavy chain's ~8 MB input buffer —
    // a sanity floor proving the workload (not noise) sets the scale, so the
    // gap above is a real fraction of a real peak.
    assert!(
        peak_heavy_first > 4 * 1024 * 1024,
        "the heavy chain's large input buffer must dominate the peak: \
         heavy_first={peak_heavy_first}"
    );
}
