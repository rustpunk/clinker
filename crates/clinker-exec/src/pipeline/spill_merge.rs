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
//! Memory model: streaming with a bounded fan-in. A single merge pass holds
//! one resident record per open run and one open file descriptor per open run,
//! so a naive open-every-run merge would let both scale with the total run
//! count — and a pathologically fragmented spill (thousands of tiny runs) can
//! blow past the process file-descriptor ceiling before it ever consults the
//! memory budget. [`SortedRunMerger`] instead bounds the number of
//! simultaneously-open runs to at most [`MERGE_FAN_IN`]: when a caller hands it
//! more runs than that, it first folds them down through a cascaded multi-pass
//! merge (each pass merging groups of at most `MERGE_FAN_IN` runs into one
//! intermediate spilled run) until at most `MERGE_FAN_IN` remain, then streams
//! the final merge lazily. Open descriptors and per-open-run residency stay
//! `O(MERGE_FAN_IN)` regardless of how many runs were spilled. When the run
//! count is already within the fan-in the cascade is a no-op and the merge is a
//! single streaming pass, byte-identical to before. [`SortedRunMerger`] yields
//! records lazily so a caller can re-slice the merged stream (e.g. into
//! contiguous blocks) without materializing it; [`merge_sorted_runs`] is the
//! materializing convenience wrapper for callers that need the whole run in a
//! `Vec`.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use clinker_record::Record;

use crate::pipeline::loser_tree::LoserTree;
use crate::pipeline::memory::MemoryArbitrator;
use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillFile, SpillReader, SpillWriter};
use clinker_plan::config::SortField;
use clinker_plan::error::PipelineError;

/// Maximum number of spill runs merged in one pass, i.e. the most readers (and
/// therefore open file descriptors) the merge holds open at once. Runs beyond
/// this are folded in through extra cascaded passes rather than opened
/// simultaneously.
///
/// A fixed modest constant rather than a fraction of the memory budget: the
/// binding constraint here is the OS file-descriptor ceiling (commonly ~1024
/// soft), a fixed resource unrelated to the RSS budget, and 64 leaves ample
/// headroom for the rest of the pipeline's descriptors while still collapsing a
/// huge run count in very few passes (e.g. 10k runs fold to <=64 in two
/// passes). It also caps the merge's live residency at `O(64)` — ~64 resident
/// records plus ~64 buffered readers, single-digit MiB — independent of the run
/// count. Must stay `>= 2` so the cascade strictly shrinks the run count each
/// pass and terminates.
const MERGE_FAN_IN: usize = 64;

/// Disk-spill charging and format configuration the cascaded merge needs to
/// write and account the intermediate runs it produces when the run count
/// exceeds [`MERGE_FAN_IN`]. Borrowed, and consulted only while the merge folds
/// its runs down at construction — never during iteration — so the merger
/// itself stores none of it and carries no lifetime. A caller that opens the
/// merge lazily parks the owned pieces itself and hands a fresh borrow in at
/// open time (see the merge-on-drain node buffer).
#[derive(Clone, Copy)]
pub(crate) struct MergeBudget<'a> {
    /// Disk-spill accountant. Every intermediate run is charged against this
    /// exactly like the caller's own spills, so a cascade that writes past
    /// `storage.spill.disk_cap_bytes` aborts with the same `SpillCapExceeded`
    /// (E320) surface rather than silently writing over quota.
    pub(crate) budget: &'a MemoryArbitrator,
    /// The spilling node's name, so an intermediate run's bytes attribute to
    /// the right stage in the per-stage breakdown and name the E320 diagnostic.
    pub(crate) node: &'a str,
    /// LZ4 vs. raw for intermediate runs, matching the caller's own spill
    /// compression so a cascade reuses the operator's chosen on-disk format.
    pub(crate) compress: bool,
}

/// Owned form of [`MergeBudget`] for a caller that opens the merge lazily — the
/// merge-on-drain node buffer parks its charging context here at adopt time
/// (where the arbitrator and node name are in scope) and lends a borrow via
/// [`OwnedMergeBudget::as_borrowed`] at the first drain poll, so the fold-down
/// still charges the disk quota even though the merge opens far from the
/// dispatcher.
pub(crate) struct OwnedMergeBudget {
    budget: Arc<MemoryArbitrator>,
    node: Arc<str>,
    compress: bool,
}

impl OwnedMergeBudget {
    pub(crate) fn new(budget: Arc<MemoryArbitrator>, node: Arc<str>, compress: bool) -> Self {
        Self {
            budget,
            node,
            compress,
        }
    }

    /// Borrow the parked context as a [`MergeBudget`] for one merge open.
    pub(crate) fn as_borrowed(&self) -> MergeBudget<'_> {
        MergeBudget {
            budget: &self.budget,
            node: &self.node,
            compress: self.compress,
        }
    }
}

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

/// Streaming k-way merge over individually-sorted spill runs, with the number
/// of simultaneously-open runs bounded to at most [`MERGE_FAN_IN`].
///
/// Yields `(record, payload)` pairs in global sort order, one resident record
/// per open run. Owns the `SpillFile`s for the final merge's lifetime: each
/// `SpillReader` opens its own file handle, but the underlying temp file is
/// deleted when its `SpillFile` (a `tempfile::TempPath`) drops, so the files
/// must outlive iteration. When a cascade ran, `_files` holds the final
/// intermediate runs (the originals were consumed and unlinked as they folded
/// in), so those too are cleaned up when the merger drops.
///
/// Stable, and — the load-bearing property — stable *independent of how many
/// cascade passes ran*, so the output is byte-identical whatever [`MERGE_FAN_IN`]
/// is. `SortBuffer` spills runs in input order (run 0 earliest) with a stable
/// per-run sort, and the loser tree breaks ties by lower stream index, so an
/// equal ordering key emits run-0-first, then in input order within a run. The
/// cascade preserves exactly this: it only ever merges *contiguous* groups of
/// runs and keeps the merged runs in the same left-to-right order, so at every
/// pass a lower stream index still covers strictly-earlier original runs. The
/// tie-break therefore always resolves to the original `(run-index, within-run)`
/// position — the merge is effectively a total-order sort on
/// `(ordering-key, original-position)` — no matter how the runs were grouped or
/// how many intermediate passes it took. In the payload-ordered mode an "equal
/// key" is an equal payload; a caller wanting a total order still gives the
/// payload a unique tie-break component, and either way the emitted order does
/// not depend on the fan-in.
///
/// Callers that need the whole run at once use [`merge_sorted_runs`]; callers
/// that can consume incrementally (draining straight into their own buffer, or
/// re-slicing the merged run into bounded blocks) drive this iterator directly.
pub(crate) struct SortedRunMerger<P: Ord> {
    /// Held only to keep the backing temp files alive across iteration; the
    /// readers below own independent file handles, not borrows of these. After
    /// a cascade these are the final intermediate runs, at most [`MERGE_FAN_IN`]
    /// of them.
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

impl<P: Serialize + DeserializeOwned + Ord> SortedRunMerger<P> {
    /// Field-ordered merge: runs are ordered by [`compare_records_by_fields`]
    /// over `sort_by`, matching a field-ordered
    /// [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer). `context` names
    /// the calling operator/phase so a spill open or decode failure localizes
    /// to the right node. `budget` charges any intermediate runs a cascade
    /// spills; it is untouched when the run count is already within the fan-in.
    pub(crate) fn new(
        files: Vec<SpillFile<P>>,
        sort_by: &[SortField],
        context: &'static str,
        budget: MergeBudget<'_>,
    ) -> Result<Self, PipelineError> {
        Self::open(
            files,
            RunOrdering::Fields(Arc::from(sort_by.to_vec())),
            context,
            budget,
            MERGE_FAN_IN,
        )
    }

    /// Payload-ordered merge: runs are ordered by the carried payload `P: Ord`
    /// directly, matching a payload-ordered
    /// [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer). Folds
    /// payload-sorted runs back into one globally payload-sorted stream. See the
    /// type-level doc for tie behavior on equal payloads. `budget` charges any
    /// intermediate runs a cascade spills.
    pub(crate) fn new_payload_ordered(
        files: Vec<SpillFile<P>>,
        context: &'static str,
        budget: MergeBudget<'_>,
    ) -> Result<Self, PipelineError> {
        Self::open(files, RunOrdering::Payload, context, budget, MERGE_FAN_IN)
    }

    /// Fold `files` down to at most `fan_in` runs (a no-op when it already is),
    /// then open a reader over each surviving run and seed the loser tree with
    /// one record per run under `ordering`. Returns an I/O error if a run fails
    /// to open, a record fails to decode, or an intermediate-run spill crosses
    /// the disk quota (E320); `context` localizes such a failure to the calling
    /// operator/phase.
    fn open(
        files: Vec<SpillFile<P>>,
        ordering: RunOrdering,
        context: &'static str,
        budget: MergeBudget<'_>,
        fan_in: usize,
    ) -> Result<Self, PipelineError> {
        debug_assert!(fan_in >= 2, "bounded-k fan-in must be >= 2 to terminate");
        // Collapse the run count to the fan-in *before* opening the streaming
        // loser tree, so open descriptors and per-open-run residency never
        // scale with the total run count.
        let files = reduce_to_fan_in(files, &ordering, context, &budget, fan_in)?;

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

/// Collapse `files` to at most `fan_in` sorted runs by repeatedly merging
/// contiguous groups of up to `fan_in` runs into one intermediate run, so the
/// final streaming merge — and every reduction pass — opens at most `fan_in`
/// readers (hence file descriptors) and holds at most `fan_in` resident records
/// at once, regardless of how many runs were spilled.
///
/// Order preservation: a run is always a contiguous "super-run" covering an
/// interval of the original run indices, internally ordered exactly as a single
/// pass would leave it. Groups are contiguous and merged in index order, and the
/// merged list keeps super-runs in increasing-interval order. Because every
/// group merge uses the same comparison and the loser tree's
/// lower-stream-index-first tie-break, and a lower stream index always covers
/// strictly-lower original indices, equal keys stay in `(original-run,
/// within-run)` order through every pass. The result is therefore byte-identical
/// to a single unbounded pass whatever `fan_in` is. See the type-level doc.
///
/// Termination: each pass with `len > fan_in` replaces the list with
/// `ceil(len / fan_in)` runs, which is strictly smaller for `fan_in >= 2`.
fn reduce_to_fan_in<P: Serialize + DeserializeOwned + Ord>(
    files: Vec<SpillFile<P>>,
    ordering: &RunOrdering,
    context: &'static str,
    budget: &MergeBudget<'_>,
    fan_in: usize,
) -> Result<Vec<SpillFile<P>>, PipelineError> {
    let mut runs = files;
    while runs.len() > fan_in {
        let mut next: Vec<SpillFile<P>> = Vec::with_capacity(runs.len().div_ceil(fan_in));
        let mut iter = runs.into_iter();
        loop {
            let group: Vec<SpillFile<P>> = iter.by_ref().take(fan_in).collect();
            match group.len() {
                0 => break,
                // A lone leftover run is already ordered and is the
                // highest-interval super-run of this pass; carry it forward
                // untouched rather than rewrite (and re-charge) an identical
                // copy. It only ever occurs as the final group, so the
                // increasing-interval order is preserved.
                1 => next.push(group.into_iter().next().expect("group.len() == 1")),
                _ => next.push(merge_group(group, ordering, context, budget)?),
            }
        }
        runs = next;
    }
    Ok(runs)
}

/// Merge one contiguous group of `2..=fan_in` sorted runs into a single new
/// spilled run and charge its on-disk bytes against the disk-spill quota exactly
/// as the caller's own spills are charged — surfacing `SpillCapExceeded` (E320)
/// when this write pushes the cumulative total past the cap. Streams straight
/// from the loser tree into the writer, so it holds one resident record per open
/// run — `O(group.len())`. The group's input `SpillFile`s drop at return,
/// unlinking their temp files, so a cascade never leaks its consumed runs.
fn merge_group<P: Serialize + DeserializeOwned + Ord>(
    group: Vec<SpillFile<P>>,
    ordering: &RunOrdering,
    context: &'static str,
    budget: &MergeBudget<'_>,
) -> Result<SpillFile<P>, PipelineError> {
    let schema = Arc::clone(group[0].schema());
    // Write the intermediate run beside its inputs, on the same spill volume the
    // caller's runs already live on, so a cascade never crosses onto a different
    // device or escapes the run's spill root.
    let spill_dir = group[0].path().parent().map(Path::to_path_buf);

    let mut readers: Vec<SpillReader<P>> = group
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
        initial.push(next_run(reader, ordering, context)?);
    }
    let mut tree = LoserTree::new(initial);

    // Intermediate-run write faults keep their structured `SpillError`, so an
    // `ENOSPC` mid-cascade still surfaces as `DiskFull` (E321) and a vanished
    // spill dir as `DirUnavailable`, rather than flattening into a generic `Io`.
    let mut writer = SpillWriter::<P>::new(schema, spill_dir.as_deref(), budget.compress)
        .map_err(PipelineError::from)?;
    while tree.winner().is_some() {
        let idx = tree.winner_index();
        // Refill before extracting, exactly as the streaming iterator does: the
        // reader position is independent of the tree cursor, so this reads the
        // winner's successor.
        let refill = next_run(&mut readers[idx], ordering, context)?;
        let winner = tree
            .take_winner(refill)
            .expect("winner present under the winner() guard");
        writer
            .write_pair(&winner.record, &winner.payload)
            .map_err(PipelineError::from)?;
    }
    let (file, written) = writer.finish_with_bytes().map_err(PipelineError::from)?;
    if written > 0 && budget.budget.record_spill_bytes(budget.node, written) {
        return Err(PipelineError::spill_cap_exceeded(
            budget.node,
            budget.budget.disk_quota(),
            written,
            budget.budget.cumulative_spill_bytes(),
        ));
    }
    Ok(file)
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
/// Memory model: holds one resident record per open run (bounded to
/// [`MERGE_FAN_IN`]) plus the fully materialized output.
pub(crate) fn merge_sorted_runs<P: Serialize + DeserializeOwned + Ord>(
    files: Vec<SpillFile<P>>,
    sort_by: &[SortField],
    context: &'static str,
    budget: MergeBudget<'_>,
) -> Result<Vec<(Record, P)>, PipelineError> {
    SortedRunMerger::new(files, sort_by, context, budget)?.collect()
}

/// Test-only constructors that pin the cascade fan-in explicitly, so a test can
/// force the multi-pass path (`fan_in = 2`) or a single pass (`fan_in` above the
/// run count) over the same input and prove the output does not depend on it.
#[cfg(test)]
impl<P: Serialize + DeserializeOwned + Ord> SortedRunMerger<P> {
    fn new_with_fan_in(
        files: Vec<SpillFile<P>>,
        sort_by: &[SortField],
        context: &'static str,
        budget: MergeBudget<'_>,
        fan_in: usize,
    ) -> Result<Self, PipelineError> {
        Self::open(
            files,
            RunOrdering::Fields(Arc::from(sort_by.to_vec())),
            context,
            budget,
            fan_in,
        )
    }

    fn new_payload_ordered_with_fan_in(
        files: Vec<SpillFile<P>>,
        context: &'static str,
        budget: MergeBudget<'_>,
        fan_in: usize,
    ) -> Result<Self, PipelineError> {
        Self::open(files, RunOrdering::Payload, context, budget, fan_in)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use clinker_plan::config::SortOrder;
    use clinker_record::{Schema, Value};
    use rust_decimal::Decimal;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k".into(), "id".into()]))
    }

    /// An unbounded-disk-quota arbitrator for the merge tests that only care
    /// about order. The quota tests build their own with a tight cap.
    fn unlimited_arbitrator() -> MemoryArbitrator {
        MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy))
    }

    fn test_budget(arb: &MemoryArbitrator) -> MergeBudget<'_> {
        MergeBudget {
            budget: arb,
            node: "test",
            compress: true,
        }
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

        let arb = unlimited_arbitrator();
        let merged = merge_sorted_runs(files, &sort_by, "test", test_budget(&arb)).unwrap();

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
        let arb = unlimited_arbitrator();
        let merged = merge_sorted_runs(files, &sort_by, "test", test_budget(&arb)).unwrap();
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
        let arb = unlimited_arbitrator();
        let merged = merge_sorted_runs(files, &sort_by, "test", test_budget(&arb)).unwrap();
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

        let arb = unlimited_arbitrator();
        let merged: Vec<(Record, (i64, i64, u64))> =
            SortedRunMerger::new_payload_ordered(files, "test", test_budget(&arb))
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

        let arb = unlimited_arbitrator();
        let ids: Vec<i64> = SortedRunMerger::new_payload_ordered(files, "test", test_budget(&arb))
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
        let arb = unlimited_arbitrator();
        let merger = SortedRunMerger::new(files, &sort_by, "test", test_budget(&arb)).unwrap();
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

    fn key_decimal(rec: &Record) -> Decimal {
        match rec.get("k") {
            Some(Value::Decimal(d)) => *d,
            other => panic!("expected Decimal key, got {other:?}"),
        }
    }

    fn id_of(rec: &Record) -> u64 {
        match rec.get("id") {
            Some(Value::Integer(n)) => *n as u64,
            other => panic!("expected Integer id, got {other:?}"),
        }
    }

    /// Field-ordered runs plus their sort spec and the `(key, payload)` oracle.
    type FieldRuns = (Vec<SpillFile<u64>>, Vec<SortField>, Vec<(Decimal, u64)>);
    /// Payload-ordered runs plus the `(payload.0, payload.1, payload.2, id)` oracle.
    type PayloadRuns = (Vec<SpillFile<(i64, i64, u64)>>, Vec<(i64, i64, u64, u64)>);

    /// Build `num_runs` individually-sorted field-ordered spill runs whose sort
    /// key `k` cycles through {10,20,30} (so keys collide heavily within and
    /// across runs) and whose `u64` payload is a globally-increasing input
    /// counter. The counter equals each record's `(run, within-run)` position,
    /// so a stable sort of the flattened input on `(key, payload)` is the exact
    /// order a correct merge must produce — the determinism oracle.
    fn build_dup_key_runs(num_runs: usize, per_run: usize) -> FieldRuns {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        // budget=1 spills on every push; an explicit flush per run makes exactly
        // `num_runs` individually-sorted runs.
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by.clone(), 1, None, true, schema.clone());
        let mut oracle: Vec<(Decimal, u64)> = Vec::new();
        let mut counter: u64 = 0;
        for _ in 0..num_runs {
            for _ in 0..per_run {
                let k_mant = ((counter % 3) as i64 + 1) * 10;
                buf.push(rec(&schema, k_mant, counter as i64), counter);
                oracle.push((Decimal::new(k_mant, 1), counter));
                counter += 1;
            }
            buf.sort_and_spill().unwrap();
        }
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled after explicit flushes"),
        };
        assert_eq!(files.len(), num_runs, "one run per explicit flush");
        oracle.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        (files, sort_by, oracle)
    }

    /// Build `num_runs` payload-ordered runs whose `(i64,i64,u64)` payload's
    /// leading component cycles through {0,1,2} (so whole payloads collide and
    /// ties abound) while the record `id` column carries the unique global input
    /// position. Oracle rows are `(payload.0, payload.1, payload.2, id)` stably
    /// sorted by payload with ties broken by input position — the order a
    /// correct merge must emit, ties included.
    fn build_dup_payload_runs(num_runs: usize, per_run: usize) -> PayloadRuns {
        let schema = schema();
        let mut buf: SortBuffer<(i64, i64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, schema.clone());
        let mut rows: Vec<(i64, i64, u64, u64)> = Vec::new();
        let mut counter: u64 = 0;
        for _ in 0..num_runs {
            for _ in 0..per_run {
                let payload = ((counter % 3) as i64, 7, 7);
                buf.push(rec(&schema, counter as i64, counter as i64), payload);
                rows.push((payload.0, payload.1, payload.2, counter));
                counter += 1;
            }
            buf.sort_and_spill().unwrap();
        }
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled after explicit flushes"),
        };
        assert_eq!(files.len(), num_runs);
        rows.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)).then(a.3.cmp(&b.3)));
        (files, rows)
    }

    /// A single streaming pass (fan-in above the run count) and a maximally
    /// cascaded multi-pass (fan-in 2, several intermediate passes) over the same
    /// field-ordered input with heavy duplicate keys must emit byte-identical
    /// output, and both must equal the `(key, input-position)` oracle. This is
    /// the load-bearing property: the tie-break stays stable no matter how many
    /// intermediate passes the cascade ran.
    #[test]
    fn field_ordered_merge_output_is_independent_of_fan_in() {
        let arb = unlimited_arbitrator();

        let (files_single, sort_by, oracle) = build_dup_key_runs(12, 4);
        let single: Vec<(Decimal, u64)> =
            SortedRunMerger::new_with_fan_in(files_single, &sort_by, "test", test_budget(&arb), 64)
                .unwrap()
                .map(|r| {
                    let (rec, p) = r.unwrap();
                    (key_decimal(&rec), p)
                })
                .collect();

        let (files_multi, _, _) = build_dup_key_runs(12, 4);
        let multi: Vec<(Decimal, u64)> =
            SortedRunMerger::new_with_fan_in(files_multi, &sort_by, "test", test_budget(&arb), 2)
                .unwrap()
                .map(|r| {
                    let (rec, p) = r.unwrap();
                    (key_decimal(&rec), p)
                })
                .collect();

        assert_eq!(single.len(), 48);
        assert_eq!(
            single, multi,
            "field-ordered output must not depend on the cascade fan-in"
        );
        assert_eq!(
            multi, oracle,
            "cascaded merge must equal the (key, input-order) oracle even with duplicate keys"
        );
    }

    /// The payload-ordered mode with heavy payload ties: single pass vs.
    /// fan-in-2 multi-pass must be byte-identical and equal the
    /// `(payload, input-position)` oracle, so equal-payload records keep their
    /// `(run, within-run)` order across every intermediate pass.
    #[test]
    fn payload_ordered_merge_output_is_independent_of_fan_in() {
        let arb = unlimited_arbitrator();

        let (files_single, oracle) = build_dup_payload_runs(10, 5);
        let single: Vec<(i64, i64, u64, u64)> = SortedRunMerger::new_payload_ordered_with_fan_in(
            files_single,
            "test",
            test_budget(&arb),
            64,
        )
        .unwrap()
        .map(|r| {
            let (rec, p) = r.unwrap();
            (p.0, p.1, p.2, id_of(&rec))
        })
        .collect();

        let (files_multi, _) = build_dup_payload_runs(10, 5);
        let multi: Vec<(i64, i64, u64, u64)> = SortedRunMerger::new_payload_ordered_with_fan_in(
            files_multi,
            "test",
            test_budget(&arb),
            2,
        )
        .unwrap()
        .map(|r| {
            let (rec, p) = r.unwrap();
            (p.0, p.1, p.2, id_of(&rec))
        })
        .collect();

        assert_eq!(single.len(), 50);
        assert_eq!(
            single, multi,
            "payload-ordered output must not depend on the cascade fan-in"
        );
        assert_eq!(
            multi, oracle,
            "cascaded payload merge must equal the (payload, input-order) oracle even with ties"
        );
    }

    /// Over many runs the reduction collapses to at most `fan_in` runs, so the
    /// final streaming merge — and every reduction pass — opens at most `fan_in`
    /// readers (hence file descriptors) at once, structurally bounding residency
    /// regardless of the total run count. The reduced runs still fold to the
    /// canonical order, so the bound costs no correctness.
    #[test]
    fn reduce_to_fan_in_bounds_the_open_run_count() {
        let arb = unlimited_arbitrator();
        let (files, sort_by, oracle) = build_dup_key_runs(100, 2);
        assert_eq!(files.len(), 100);

        let ordering = RunOrdering::Fields(Arc::from(sort_by.clone()));
        let reduced = reduce_to_fan_in(files, &ordering, "test", &test_budget(&arb), 4).unwrap();
        assert!(
            (1..=4).contains(&reduced.len()),
            "reduction must leave 1..=fan_in runs, got {}",
            reduced.len()
        );

        let merged: Vec<(Decimal, u64)> =
            SortedRunMerger::new_with_fan_in(reduced, &sort_by, "test", test_budget(&arb), 4)
                .unwrap()
                .map(|r| {
                    let (rec, p) = r.unwrap();
                    (key_decimal(&rec), p)
                })
                .collect();
        assert_eq!(merged, oracle);
    }

    /// A run count above [`MERGE_FAN_IN`] drives the *public* constructor down
    /// the cascaded path automatically — a single-pass open would hold this many
    /// readers (hence descriptors) at once; the bounded merge never does, and
    /// still yields the canonical order.
    #[test]
    fn many_runs_merge_through_default_fan_in_matches_oracle() {
        let arb = unlimited_arbitrator();
        let num_runs = MERGE_FAN_IN + 3;
        let (files, sort_by, oracle) = build_dup_key_runs(num_runs, 1);
        let merged: Vec<(Decimal, u64)> =
            merge_sorted_runs(files, &sort_by, "test", test_budget(&arb))
                .unwrap()
                .into_iter()
                .map(|(rec, p)| (key_decimal(&rec), p))
                .collect();
        assert_eq!(merged.len(), num_runs);
        assert_eq!(merged, oracle);
    }

    /// A cascade writes intermediate runs, and those bytes are charged against
    /// the disk-spill quota exactly like any other spill — so a tight cap aborts
    /// the merge with the `SpillCapExceeded` (E320) surface naming the node.
    #[test]
    fn cascade_charges_intermediate_runs_and_surfaces_e320() {
        let arb = unlimited_arbitrator();
        arb.set_max_spill_bytes(1); // any intermediate write trips the cap
        let budget = MergeBudget {
            budget: &arb,
            node: "quota-node",
            compress: true,
        };
        let (files, sort_by, _) = build_dup_key_runs(8, 4);
        // `SortedRunMerger` is not `Debug`, so unwrap the error by hand.
        let err = match SortedRunMerger::new_with_fan_in(files, &sort_by, "test", budget, 2) {
            Ok(_) => panic!("a tiny disk quota must abort the cascade"),
            Err(e) => e,
        };
        match err {
            PipelineError::SpillCapExceeded { node, .. } => {
                assert_eq!(node, "quota-node", "E320 must name the merging node");
            }
            other => panic!("expected SpillCapExceeded (E320), got {other:?}"),
        }
    }

    /// After a full multi-pass drain and drop, no spill file — original run or
    /// intermediate — is left behind in the spill directory: the cascade unlinks
    /// each input as it folds in and the merger unlinks its final runs on drop.
    #[test]
    fn cascade_cleans_up_all_spill_files_in_the_dir() {
        let arb = unlimited_arbitrator();
        let dir = tempfile::tempdir().unwrap();
        let schema = schema();
        let sort_by = sort_by_k_asc();
        let mut buf: SortBuffer<u64> = SortBuffer::new(
            sort_by.clone(),
            1,
            Some(dir.path().to_path_buf()),
            true,
            schema.clone(),
        );
        for i in 0..20u64 {
            let k = ((i % 3) as i64 + 1) * 10;
            buf.push(rec(&schema, k, i as i64), i);
            buf.sort_and_spill().unwrap();
        }
        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => files,
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        };
        assert_eq!(files.len(), 20);
        assert!(
            dir.path().read_dir().unwrap().count() >= 20,
            "runs live in the dir before the merge"
        );

        // fan_in = 2 forces intermediate runs into the same dir; drain fully.
        let budget = MergeBudget {
            budget: &arb,
            node: "test",
            compress: true,
        };
        let merged: Vec<u64> = SortedRunMerger::new_with_fan_in(files, &sort_by, "test", budget, 2)
            .unwrap()
            .map(|r| r.unwrap().1)
            .collect();
        assert_eq!(merged.len(), 20);
        assert_eq!(
            dir.path().read_dir().unwrap().count(),
            0,
            "every original run and intermediate run must be unlinked"
        );
    }
}
