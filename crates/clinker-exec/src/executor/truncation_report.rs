//! Run-scoped account of values Sink writers cut to fit a column under
//! `truncation: warn`, reported as one W367 advisory per affected Sink.
//!
//! Every Sink writer is built by `build_format_writer`, which wraps it in
//! [`TruncationReporting`]. The wrapper settles its writer's account into the
//! shared [`TruncationLedger`] when the writer is dropped, so a Sink path that
//! builds a writer cannot forget to report it, and the W367 advisory and the
//! `SinkTruncations` metric both read the same settled count.
//!
//! Memory: one entry per Sink, each bounded by the Sink's schema (a count,
//! a longest length and at most `TRUNCATION_EXAMPLE_LIMIT` record numbers per
//! truncating column). Nothing here grows with the number of records.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use clinker_format::error::FormatError;
use clinker_format::{FormatWriter, TruncationSummary};
use clinker_record::{DocumentContext, Record};

/// Settled truncations per Sink, shared by every writer the run builds,
/// including the streaming writer threads.
#[derive(Clone, Default)]
pub(crate) struct TruncationLedger(Arc<Mutex<BTreeMap<String, SinkTruncations>>>);

#[derive(Default)]
struct SinkTruncations {
    summary: TruncationSummary,
    /// Records delivered by every settled writer of this Sink; the next
    /// writer's record numbers start after these.
    records: u64,
}

impl TruncationLedger {
    fn with<T>(&self, f: impl FnOnce(&mut BTreeMap<String, SinkTruncations>) -> T) -> T {
        // A panic while holding the lock leaves plain counters behind, which
        // remain a valid account to report from.
        let mut sinks = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        f(&mut sinks)
    }

    fn settle(&self, sink: &str, summary: Option<TruncationSummary>, records: u64) {
        if summary.is_none() && records == 0 {
            return;
        }
        self.with(|sinks| {
            let entry = sinks.entry(sink.to_string()).or_default();
            if let Some(summary) = summary {
                entry.summary.merge_after(summary, entry.records);
            }
            entry.records = entry.records.saturating_add(records);
        });
    }

    /// Values truncated so far by every settled writer of `sink`.
    pub(crate) fn truncated_cells(&self, sink: &str) -> u64 {
        self.with(|sinks| {
            sinks
                .get(sink)
                .map_or(0, |entry| entry.summary.total_cells())
        })
    }

    /// W367 advisories in Sink declaration order, one per Sink that truncated.
    pub(crate) fn advisories(
        &self,
        sink_configs: &[clinker_plan::config::SinkConfig],
    ) -> Vec<String> {
        self.with(|sinks| {
            sink_configs
                .iter()
                .filter_map(|sink| {
                    let entry = sinks.get(&sink.name)?;
                    (!entry.summary.is_empty()).then(|| render(&sink.name, &entry.summary))
                })
                .collect()
        })
    }
}

/// `W367 output 'ledger': 3 value(s) truncated under `truncation: warn`: name
/// ×2 (longest 212 bytes, width 30; records 3, 9), city ×1 (…)`.
fn render(sink: &str, summary: &TruncationSummary) -> String {
    let columns = summary
        .columns
        .iter()
        .map(|column| {
            let records = column
                .example_records
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let more = if column.more_records { ", …" } else { "" };
            format!(
                "{} ×{} (longest {} bytes, width {}; records {records}{more})",
                column.column, column.cells, column.longest_bytes, column.width
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "W367 output '{sink}': {} value(s) truncated to fit under `truncation: warn`: {columns}. \
         Widen the column, shorten the value upstream, or set `truncation: error` to reject \
         such records.",
        summary.total_cells()
    )
}

/// Wraps one Sink writer so its truncation account settles into the run's
/// ledger when the writer is dropped: after a clean finish, a failure, or
/// cancellation alike, since every record it delivered is already written.
pub(crate) struct TruncationReporting<W: FormatWriter> {
    inner: W,
    ledger: TruncationLedger,
    sink: String,
    /// Records this writer delivered; offsets the next writer's numbering.
    records: u64,
}

impl<W: FormatWriter> TruncationReporting<W> {
    pub(crate) fn new(inner: W, ledger: TruncationLedger, sink: String) -> Self {
        Self {
            inner,
            ledger,
            sink,
            records: 0,
        }
    }
}

impl<W: FormatWriter> Drop for TruncationReporting<W> {
    fn drop(&mut self) {
        self.ledger
            .settle(&self.sink, self.inner.truncation_summary(), self.records);
    }
}

impl<W: FormatWriter> FormatWriter for TruncationReporting<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.inner.write_record(record)?;
        self.records = self.records.saturating_add(1);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()
    }
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.inner.flush_bytes()
    }
    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.begin_document(doc)
    }
    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.end_document(doc)
    }
    fn bytes_written(&self) -> Option<u64> {
        self.inner.bytes_written()
    }
    fn truncation_summary(&self) -> Option<TruncationSummary> {
        self.inner.truncation_summary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::ColumnTruncation;

    fn summary(cells: u64, examples: &[u64]) -> TruncationSummary {
        TruncationSummary {
            columns: vec![ColumnTruncation {
                column: "name".into(),
                width: 4,
                cells,
                longest_bytes: 9,
                example_records: examples.to_vec(),
                more_records: false,
            }],
        }
    }

    #[test]
    fn later_writers_of_a_sink_are_numbered_after_earlier_ones() {
        let ledger = TruncationLedger::default();
        ledger.settle("out", Some(summary(2, &[1, 3])), 5);
        ledger.settle("out", None, 4);
        ledger.settle("out", Some(summary(1, &[2])), 3);
        assert_eq!(ledger.truncated_cells("out"), 3);
        assert_eq!(ledger.truncated_cells("other"), 0);
        ledger.with(|sinks| {
            assert_eq!(
                sinks["out"].summary.columns[0].example_records,
                vec![1, 3, 11]
            );
            assert_eq!(sinks["out"].records, 12);
        });
    }

    #[test]
    fn advisories_follow_sink_declaration_order_and_name_the_fix() {
        let config = clinker_plan::config::parse_config(
            r#"
pipeline:
  name: w367
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: name, type: string }
  - type: sink
    name: z_out
    input: src
    config:
      name: z_out
      type: csv
      path: z.csv
  - type: sink
    name: a_out
    input: src
    config:
      name: a_out
      type: csv
      path: a.csv
"#,
        )
        .unwrap();
        let sinks = config.sink_configs().cloned().collect::<Vec<_>>();
        let ledger = TruncationLedger::default();
        ledger.settle("a_out", Some(summary(1, &[2])), 2);
        ledger.settle("z_out", Some(summary(2, &[1, 2])), 2);
        let advisories = ledger.advisories(&sinks);
        assert_eq!(advisories.len(), 2, "{advisories:?}");
        assert!(
            advisories[0].starts_with("W367 output 'z_out': 2 value(s)"),
            "{}",
            advisories[0]
        );
        assert!(
            advisories[0].contains("name ×2 (longest 9 bytes, width 4; records 1, 2)"),
            "{}",
            advisories[0]
        );
        assert!(advisories[1].contains("'a_out'"), "{}", advisories[1]);
        assert!(
            advisories[1].contains("`truncation: error`"),
            "{}",
            advisories[1]
        );
    }
}
