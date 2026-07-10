//! K-way merge over individually-sorted spill runs.
//!
//! [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer) flushes its
//! overflow as a sequence of internally-sorted spill runs; folding them back
//! into one globally-ordered stream is a k-way merge. This module owns the
//! single production implementation, shared by every operator that
//! external-sorts through `SortBuffer` and then has to merge the resulting
//! runs — the `Sort` DAG node and the sort-merge combine's Phase A external
//! sort.
//!
//! Ordering is by [`compare_records_by_fields`] — the exact field comparator
//! `SortBuffer` formed each run with — so the merged order is byte-identical to
//! the single in-memory sort the buffer would have produced without spilling.
//! The memcomparable byte key on
//! [`crate::pipeline::loser_tree::MergeEntry`] is deliberately not used here:
//! it is not provably equal to the field comparator across Integer/Decimal or
//! mixed int/float ordering, so merging on it could silently mis-order
//! cross-type keys.
//!
//! Memory model: streaming — one resident record per open run. [`SortedRunMerger`]
//! yields records lazily so a caller can re-slice the merged stream (e.g. into
//! contiguous blocks) without materializing it; [`merge_sorted_runs`] is the
//! materializing convenience wrapper for callers that need the whole run in a
//! `Vec`.

use std::cmp::Ordering;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use clinker_record::Record;

use crate::pipeline::loser_tree::LoserTree;
use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillFile, SpillReader};
use clinker_plan::config::SortField;
use clinker_plan::error::PipelineError;

/// One entry in the k-way merge: a spilled record and the payload `P` carried
/// through the sort permutation. `Ord` delegates to
/// [`compare_records_by_fields`] against the shared `sort_by`, so the merged
/// order matches every run's internal sort exactly.
struct Run<P> {
    sort_by: Arc<[SortField]>,
    record: Record,
    payload: P,
}

impl<P> PartialEq for Run<P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<P> Eq for Run<P> {}

impl<P> PartialOrd for Run<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> Ord for Run<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_records_by_fields(&self.record, &other.record, &self.sort_by)
    }
}

/// Pull the next `(record, payload)` from one spilled run, wrapping it as a
/// [`Run`] merge entry, or `None` at end of stream.
fn next_run<P: DeserializeOwned>(
    reader: &mut SpillReader<P>,
    sort_by: &Arc<[SortField]>,
) -> Result<Option<Run<P>>, PipelineError> {
    match reader.next() {
        None => Ok(None),
        Some(Ok((record, payload))) => Ok(Some(Run {
            sort_by: Arc::clone(sort_by),
            record,
            payload,
        })),
        Some(Err(e)) => Err(PipelineError::Io(std::io::Error::other(format!(
            "spill run decode failed during k-way merge: {e}"
        )))),
    }
}

/// Streaming k-way merge over individually-sorted spill runs.
///
/// Yields `(record, payload)` pairs in global sort order, one resident record
/// per open run. Owns the `SpillFile`s for the merge's lifetime: each
/// `SpillReader` opens its own file handle, but the underlying temp file is
/// deleted when its `SpillFile` (a `tempfile::TempPath`) drops, so the files
/// must outlive iteration.
///
/// Stable: `SortBuffer` spills runs in input order (run 0 earliest) with a
/// stable per-run sort, and the loser tree breaks ties by lower stream index,
/// so equal keys emit in input order.
///
/// Module-private: the only current consumer is [`merge_sorted_runs`], which
/// materializes the stream. The lazy form exists so a later caller can
/// re-slice the merged run into bounded blocks; that caller widens the
/// visibility when it lands.
struct SortedRunMerger<P> {
    /// Held only to keep the backing temp files alive across iteration; the
    /// readers below own independent file handles, not borrows of these.
    _files: Vec<SpillFile<P>>,
    readers: Vec<SpillReader<P>>,
    tree: LoserTree<Run<P>>,
    sort_by: Arc<[SortField]>,
    /// Latches once a decode error has been surfaced so `next` stops rather
    /// than re-polling an already-failed reader.
    done: bool,
}

impl<P: DeserializeOwned> SortedRunMerger<P> {
    /// Open a reader over every run and seed the loser tree with one record
    /// per run. Returns an I/O error if any run fails to open or its first
    /// record fails to decode.
    fn new(files: Vec<SpillFile<P>>, sort_by: &[SortField]) -> Result<Self, PipelineError> {
        let sort_by: Arc<[SortField]> = Arc::from(sort_by.to_vec());
        let mut readers: Vec<SpillReader<P>> = files
            .iter()
            .map(|f| f.reader())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "spill run open failed during k-way merge: {e}"
                )))
            })?;

        let mut initial: Vec<Option<Run<P>>> = Vec::with_capacity(readers.len());
        for reader in &mut readers {
            initial.push(next_run(reader, &sort_by)?);
        }
        let tree = LoserTree::new(initial);

        Ok(Self {
            _files: files,
            readers,
            tree,
            sort_by,
            done: false,
        })
    }
}

impl<P: DeserializeOwned> Iterator for SortedRunMerger<P> {
    type Item = Result<(Record, P), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // All runs exhausted → merge complete.
        self.tree.winner()?;
        let idx = self.tree.winner_index();
        // Refill the winning run before extracting the current winner: the
        // reader position is independent of the tree cursor, so this reads the
        // winner's successor. A decode error aborts the whole merge — the
        // partial output the caller has collected is discarded when it
        // propagates the `Err`.
        let refill = match next_run(&mut self.readers[idx], &self.sort_by) {
            Ok(next) => next,
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        };
        let winner = self
            .tree
            .take_winner(refill)
            .expect("winner present under the winner() guard above");
        Some(Ok((winner.record, winner.payload)))
    }
}

/// K-way merge the individually-sorted spill runs into one globally-ordered
/// `Vec`, preserving each record's payload. Convenience wrapper over
/// [`SortedRunMerger`] for callers that need the full run materialized.
///
/// Memory model: holds one resident record per open run plus the fully
/// materialized output.
pub(crate) fn merge_sorted_runs<P: DeserializeOwned>(
    files: Vec<SpillFile<P>>,
    sort_by: &[SortField],
) -> Result<Vec<(Record, P)>, PipelineError> {
    SortedRunMerger::new(files, sort_by)?.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use clinker_plan::config::SortOrder;
    use clinker_record::{Schema, Value};
    use rust_decimal::Decimal;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k".into(), "id".into()]))
    }

    /// A record whose sort key `k` is a `Decimal` (mantissa/10) and whose
    /// `id` mirrors the carried payload for readback.
    fn rec(schema: &Arc<Schema>, k_mantissa: i64, id: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![
                Value::Decimal(Decimal::new(k_mantissa, 1)),
                Value::Integer(id),
            ],
        )
    }

    fn sort_by_k_asc() -> Vec<SortField> {
        vec![SortField {
            field: "k".into(),
            order: SortOrder::Asc,
            null_order: None,
        }]
    }

    /// Multiple individually-sorted spill runs — with a `Decimal` sort key,
    /// duplicate keys spanning different runs, and unsorted input within each
    /// run — must k-way merge into one globally-ordered run that preserves
    /// every record's payload and emits equal keys in input order (stable).
    /// This is the correctness the enforcer sort and the sort-merge Phase A
    /// external sort lost when they rejected multi-run spills instead of
    /// merging them.
    #[test]
    fn merge_sorted_runs_globally_orders_stably_and_preserves_payload() {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        // budget=1 is the spill-everything threshold; runs are flushed
        // explicitly below so each carries several unsorted records.
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by.clone(), 1, None, true, schema.clone());

        // Run A: keys 3.0, 1.0, 2.0 (payload 0,1,2).
        buf.push(rec(&schema, 30, 0), 0);
        buf.push(rec(&schema, 10, 1), 1);
        buf.push(rec(&schema, 20, 2), 2);
        buf.sort_and_spill().unwrap();
        // Run B: keys 2.0, 1.0 (payload 3,4) — duplicates 2.0 and 1.0 from A.
        buf.push(rec(&schema, 20, 3), 3);
        buf.push(rec(&schema, 10, 4), 4);
        buf.sort_and_spill().unwrap();
        // Run C: keys 2.0, 4.0 (payload 5,6) — duplicate 2.0 from A and B.
        buf.push(rec(&schema, 20, 5), 5);
        buf.push(rec(&schema, 40, 6), 6);
        buf.sort_and_spill().unwrap();

        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 3, "three explicit flushes → three runs");
                files
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled after three flushes"),
        };

        let merged = merge_sorted_runs(files, &sort_by).unwrap();

        // Every input record survives the merge exactly once.
        assert_eq!(merged.len(), 7);

        // Globally ordered on the Decimal key, ascending.
        let keys: Vec<Decimal> = merged
            .iter()
            .map(|(r, _)| match r.get("k") {
                Some(Value::Decimal(d)) => *d,
                other => panic!("expected Decimal key, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                Decimal::new(10, 1),
                Decimal::new(10, 1),
                Decimal::new(20, 1),
                Decimal::new(20, 1),
                Decimal::new(20, 1),
                Decimal::new(30, 1),
                Decimal::new(40, 1),
            ],
        );

        // Stable: for equal keys, records emit in input order (run 0 earliest,
        // then input order within a run). The carried payload proves it and
        // that no record picked up another's payload through the merge.
        let payloads: Vec<u64> = merged.iter().map(|(_, p)| *p).collect();
        assert_eq!(
            payloads,
            vec![1, 4, 2, 3, 5, 0, 6],
            "equal keys must emit in input order and carry their own payload"
        );

        // The payload identity matches the record's `id` column for every row,
        // confirming the pairing survived both the spill envelope and the merge.
        for (record, payload) in &merged {
            assert_eq!(record.get("id"), Some(&Value::Integer(*payload as i64)));
        }
    }

    /// The unit `()` payload (the sort-merge build side carries no order tag)
    /// round-trips through a multi-run merge.
    #[test]
    fn merge_sorted_runs_unit_payload_orders_globally() {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by.clone(), 1, None, true, schema.clone());
        // Interleaved keys across four explicit runs.
        for (i, k) in [50, 10, 40, 20, 30, 0].into_iter().enumerate() {
            buf.push(rec(&schema, k, i as i64), ());
            buf.sort_and_spill().unwrap();
        }
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        };
        assert!(files.len() > 1, "expected multiple runs");
        let merged = merge_sorted_runs(files, &sort_by).unwrap();
        let keys: Vec<Decimal> = merged
            .iter()
            .map(|(r, _)| match r.get("k") {
                Some(Value::Decimal(d)) => *d,
                other => panic!("expected Decimal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                Decimal::new(0, 1),
                Decimal::new(10, 1),
                Decimal::new(20, 1),
                Decimal::new(30, 1),
                Decimal::new(40, 1),
                Decimal::new(50, 1),
            ],
        );
    }

    /// A single run is already globally ordered; the merger is an identity
    /// pass over it (k=1).
    #[test]
    fn merge_sorted_runs_single_run_is_identity() {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by.clone(), 1_000_000, None, true, schema.clone());
        buf.push(rec(&schema, 30, 0), 0);
        buf.push(rec(&schema, 10, 1), 1);
        buf.sort_and_spill().unwrap(); // force a single explicit run
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        };
        assert_eq!(files.len(), 1);
        let merged = merge_sorted_runs(files, &sort_by).unwrap();
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0].0.get("k"),
            Some(&Value::Decimal(Decimal::new(10, 1)))
        );
        assert_eq!(
            merged[1].0.get("k"),
            Some(&Value::Decimal(Decimal::new(30, 1)))
        );
    }

    /// The streaming iterator yields the same global order as the
    /// materializing wrapper, one record at a time.
    #[test]
    fn sorted_run_merger_streams_in_order() {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by.clone(), 1, None, true, schema.clone());
        for (i, k) in [30, 10, 20].into_iter().enumerate() {
            buf.push(rec(&schema, k, i as i64), i as u64);
            buf.sort_and_spill().unwrap();
        }
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        };
        let merger = SortedRunMerger::new(files, &sort_by).unwrap();
        let collected: Vec<(Record, u64)> = merger.map(|r| r.unwrap()).collect();
        let keys: Vec<Decimal> = collected
            .iter()
            .map(|(r, _)| match r.get("k") {
                Some(Value::Decimal(d)) => *d,
                other => panic!("expected Decimal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                Decimal::new(10, 1),
                Decimal::new(20, 1),
                Decimal::new(30, 1)
            ],
        );
    }
}
