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
//! Ordering mirrors the [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer)
//! mode the runs were spilled under, so the merged order is byte-identical to
//! the single in-memory sort the buffer would have produced without spilling:
//!   - Field-ordered ([`SortedRunMerger::new`]): by [`compare_records_by_fields`]
//!     — the exact field comparator each run was formed with.
//!   - Payload-ordered ([`SortedRunMerger::new_payload_ordered`]): by the
//!     carried payload `P: Ord` directly, matching a payload-ordered buffer.
//!
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

/// How [`SortedRunMerger`] orders runs against each other, mirroring the
/// [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer) mode each run was
/// spilled under. Cheap to clone: the field variant shares one `Arc`.
#[derive(Clone)]
enum RunOrdering {
    /// Order by [`compare_records_by_fields`] over the shared fields — the
    /// record carries the sort key.
    Fields(Arc<[SortField]>),
    /// Order by the carried payload `P: Ord` directly — no record field.
    Payload,
}

/// One entry in the k-way merge: a spilled record and the payload `P` carried
/// through the sort permutation. `Ord` delegates to the run's [`RunOrdering`],
/// so the merged order matches every run's internal sort exactly.
struct Run<P> {
    ordering: RunOrdering,
    record: Record,
    payload: P,
}

impl<P: Ord> PartialEq for Run<P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<P: Ord> Eq for Run<P> {}

impl<P: Ord> PartialOrd for Run<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P: Ord> Ord for Run<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        match &self.ordering {
            RunOrdering::Fields(sort_by) => {
                compare_records_by_fields(&self.record, &other.record, sort_by)
            }
            RunOrdering::Payload => self.payload.cmp(&other.payload),
        }
    }
}

/// Pull the next `(record, payload)` from one spilled run, wrapping it as a
/// [`Run`] merge entry under `ordering`, or `None` at end of stream. `context`
/// names the calling operator/phase so a decode failure localizes to the right
/// node.
fn next_run<P: DeserializeOwned>(
    reader: &mut SpillReader<P>,
    ordering: &RunOrdering,
    context: &'static str,
) -> Result<Option<Run<P>>, PipelineError> {
    match reader.next() {
        None => Ok(None),
        Some(Ok((record, payload))) => Ok(Some(Run {
            ordering: ordering.clone(),
            record,
            payload,
        })),
        Some(Err(e)) => Err(PipelineError::Io(std::io::Error::other(format!(
            "{context}: spill run decode failed during k-way merge: {e}"
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
/// so records with an equal ordering key emit run-0-first, then in input order
/// within a run. In the payload-ordered mode an "equal key" is an equal
/// payload; a caller wanting a total order gives the payload a unique
/// tie-break component (the merge order is otherwise deterministic but carries
/// no stability guarantee beyond that (run-index, within-run) rule).
///
/// Callers that need the whole run at once use [`merge_sorted_runs`]; callers
/// that can consume incrementally (draining straight into their own buffer, or
/// re-slicing the merged run into bounded blocks) drive this iterator directly.
pub(crate) struct SortedRunMerger<P: Ord> {
    /// Held only to keep the backing temp files alive across iteration; the
    /// readers below own independent file handles, not borrows of these.
    _files: Vec<SpillFile<P>>,
    readers: Vec<SpillReader<P>>,
    tree: LoserTree<Run<P>>,
    ordering: RunOrdering,
    /// Names the calling operator/phase for decode-error messages.
    context: &'static str,
    /// Latches once a decode error has been surfaced so `next` stops rather
    /// than re-polling an already-failed reader.
    done: bool,
}

impl<P: DeserializeOwned + Ord> SortedRunMerger<P> {
    /// Field-ordered merge: runs are ordered by [`compare_records_by_fields`]
    /// over `sort_by`, matching a field-ordered
    /// [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer). `context` names
    /// the calling operator/phase so a spill open or decode failure localizes
    /// to the right node.
    pub(crate) fn new(
        files: Vec<SpillFile<P>>,
        sort_by: &[SortField],
        context: &'static str,
    ) -> Result<Self, PipelineError> {
        Self::open(
            files,
            RunOrdering::Fields(Arc::from(sort_by.to_vec())),
            context,
        )
    }

    /// Payload-ordered merge: runs are ordered by the carried payload `P: Ord`
    /// directly, matching a payload-ordered
    /// [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer). Folds
    /// payload-sorted runs back into one globally payload-sorted stream. See the
    /// type-level doc for tie behavior on equal payloads.
    pub(crate) fn new_payload_ordered(
        files: Vec<SpillFile<P>>,
        context: &'static str,
    ) -> Result<Self, PipelineError> {
        Self::open(files, RunOrdering::Payload, context)
    }

    /// Open a reader over every run and seed the loser tree with one record per
    /// run under `ordering`. Returns an I/O error if any run fails to open or
    /// its first record fails to decode; `context` localizes such a failure to
    /// the calling operator/phase.
    fn open(
        files: Vec<SpillFile<P>>,
        ordering: RunOrdering,
        context: &'static str,
    ) -> Result<Self, PipelineError> {
        let mut readers: Vec<SpillReader<P>> = files
            .iter()
            .map(|f| f.reader())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "{context}: spill run open failed during k-way merge: {e}"
                )))
            })?;

        let mut initial: Vec<Option<Run<P>>> = Vec::with_capacity(readers.len());
        for reader in &mut readers {
            initial.push(next_run(reader, &ordering, context)?);
        }
        let tree = LoserTree::new(initial);

        Ok(Self {
            _files: files,
            readers,
            tree,
            ordering,
            context,
            done: false,
        })
    }
}

impl<P: DeserializeOwned + Ord> Iterator for SortedRunMerger<P> {
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
        let refill = match next_run(&mut self.readers[idx], &self.ordering, self.context) {
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
/// [`SortedRunMerger`] for callers that need the full run materialized;
/// `context` names the calling operator for spill-error messages.
///
/// Memory model: holds one resident record per open run plus the fully
/// materialized output.
pub(crate) fn merge_sorted_runs<P: DeserializeOwned + Ord>(
    files: Vec<SpillFile<P>>,
    sort_by: &[SortField],
    context: &'static str,
) -> Result<Vec<(Record, P)>, PipelineError> {
    SortedRunMerger::new(files, sort_by, context)?.collect()
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

        let merged = merge_sorted_runs(files, &sort_by, "test").unwrap();

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
        let merged = merge_sorted_runs(files, &sort_by, "test").unwrap();
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
        let merged = merge_sorted_runs(files, &sort_by, "test").unwrap();
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

    /// Payload-ordered runs merged by the payload-ordered merger must yield the
    /// exact globally sorted sequence a single std sort of the same tuples
    /// produces. Tuple shape `(i64, i64, u64)` mirrors the range-join driver
    /// payload; the run includes negative primary keys, and the `u64` third
    /// component is a unique `RecordOrder` tag making the payload a total order.
    /// The `id` column carries that tag too, so the check also proves each
    /// record kept its own payload across the spill envelope and the merge.
    #[test]
    fn payload_ordered_merge_matches_std_sorted_oracle() {
        let schema = schema();
        let tuples: Vec<(i64, i64, u64)> = vec![
            (5, -2, 0),
            (-7, 3, 1),
            (5, -2, 2),
            (0, 0, 3),
            (-7, -9, 4),
            (12, 1, 5),
            (-7, 3, 6),
            (3, 8, 7),
            (5, -2, 8),
            (-100, 4, 9),
        ];
        // budget=1 with explicit flushes: three runs carrying several unsorted
        // tuples each, the last flushed by finish() as the residue.
        let mut buf: SortBuffer<(i64, i64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, schema.clone());
        let push_chunk = |buf: &mut SortBuffer<(i64, i64, u64)>, chunk: &[(i64, i64, u64)]| {
            for &t in chunk {
                // id column mirrors the payload's RecordOrder tag for readback.
                buf.push(rec(&schema, t.0, t.2 as i64), t);
            }
        };
        push_chunk(&mut buf, &tuples[0..4]);
        buf.sort_and_spill().unwrap();
        push_chunk(&mut buf, &tuples[4..7]);
        buf.sort_and_spill().unwrap();
        push_chunk(&mut buf, &tuples[7..10]);

        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled after explicit flushes"),
        };
        assert!(files.len() >= 2, "forced spill must produce multiple runs");

        let merged: Vec<(Record, (i64, i64, u64))> =
            SortedRunMerger::new_payload_ordered(files, "test")
                .unwrap()
                .map(|r| r.unwrap())
                .collect();

        let mut oracle = tuples.clone();
        oracle.sort();
        let merged_payloads: Vec<(i64, i64, u64)> = merged.iter().map(|(_, p)| *p).collect();
        assert_eq!(merged_payloads, oracle, "merged order must equal std sort");

        // Every record kept its own payload: id column == payload's third field.
        for (record, payload) in &merged {
            assert_eq!(record.get("id"), Some(&Value::Integer(payload.2 as i64)));
        }
    }

    /// Equal payloads across different runs emit run-0-first, then in input
    /// order within a run — the deterministic tie rule the loser tree provides.
    /// The `id` column tags each record's origin so the emitted order is
    /// observable.
    #[test]
    fn payload_ordered_merge_breaks_ties_by_run_then_input_order() {
        let schema = schema();
        let mut buf: SortBuffer<(i64, i64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, schema.clone());
        // Run A: two records with the same payload, ids 0 then 1 (input order).
        buf.push(rec(&schema, 0, 0), (7, 7, 7));
        buf.push(rec(&schema, 0, 1), (7, 7, 7));
        buf.sort_and_spill().unwrap();
        // Run B (spilled later): same payload, id 2.
        buf.push(rec(&schema, 0, 2), (7, 7, 7));
        buf.sort_and_spill().unwrap();

        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        };
        assert_eq!(files.len(), 2);

        let ids: Vec<i64> = SortedRunMerger::new_payload_ordered(files, "test")
            .unwrap()
            .map(|r| match r.unwrap().0.get("id") {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected Integer id, got {other:?}"),
            })
            .collect();
        // Run A (ids 0, 1 in input order) precedes run B (id 2).
        assert_eq!(ids, vec![0, 1, 2]);
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
        let merger = SortedRunMerger::new(files, &sort_by, "test").unwrap();
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
