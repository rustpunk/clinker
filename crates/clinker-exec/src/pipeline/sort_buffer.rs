//! Sort buffer: accumulates `(record, payload)` pairs, sorts in-memory or
//! spills to disk.
//!
//! Used at source-sort, output-sort, and DAG enforcer-sort intercept points.
//! The buffer tracks its own memory usage and spills sorted chunks to
//! postcard temp files — optionally LZ4-framed per the workspace
//! `[storage.spill] compress` knob — when the budget is exceeded.
//!
//! Two ordering modes, chosen at construction:
//!   - Field-ordered ([`SortBuffer::new`]): the sort key is read from the
//!     record by the authored comparator
//!     ([`compare_authored_keys`](crate::pipeline::sort_key::compare_authored_keys)),
//!     its fields resolved to column positions once against the buffer's
//!     schema, and the payload rides along inert. Every source/output/DAG/join
//!     sort uses this.
//!   - Payload-ordered ([`SortBuffer::new_payload_ordered`]): pairs order by the
//!     payload `P: Ord` directly, with no record field consulted. This serves a
//!     sort whose key is a value computed off the record — e.g. a range join
//!     keying on evaluated inequality expressions that back no single column —
//!     so the key need not be stamped onto the record as a synthetic field.
//!
//! A field-ordered buffer sorts a per-row index rather than the pairs: each
//! entry holds the first eight bytes of the row's order-preserving key and the
//! row's position, equal prefixes fall back to the full comparator and then to
//! the position, and the pairs are moved into the index's order in place. The
//! result is the stable sort by the comparator. When prefixes keep colliding
//! while the full keys differ, the buffer measures it while encoding, stops
//! abbreviating for that run and every later one, and sorts on the comparator
//! alone. Each row's index entry is charged at push whichever way its run ends
//! up sorted, and neither the index nor the abbreviations reach disk: the spill
//! format is unchanged.
//!
//! A sequential sort decides on a doubling schedule of checkpoints and can stop
//! abbreviating part-way through a run; a pooled sort decides once over the
//! whole run from its merged chunk sketches. On input whose early rows collide
//! and whose later rows diversify the two can decide differently. The output is
//! identical either way, and the two are claimed to agree only on the abort
//! fixtures the tests pin.
//!
//! Generic over per-record payload `P`. Source/output sort uses `SortBuffer<()>`;
//! the DAG enforcer-sort carries a `SourceRowId`, while the sort-merge join uses
//! its own typed ordering payload. Payload travels inside the spill envelope
//! (bundled-tuple sort), not on a parallel array, to avoid permutation-reindex
//! bugs.

use clinker_record::owned_storage::{AllocationResources, SharedStorage};
use std::cmp::Ordering;
use std::path::PathBuf;

use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut};
use serde::{Serialize, de::DeserializeOwned};

use clinker_record::{Record, Schema};

use crate::pipeline::sort_key::{ResolvedSortKeys, abbreviated_key};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use clinker_plan::SpillError;
use clinker_plan::config::SortField;

use crate::sketch::{Hll, splitmix64};

/// The heap bytes a sort payload owns beyond its inline `size_of<P>`. Folded
/// into the buffer's per-pair byte estimate so a payload carrying a
/// variable-length key — the block-band IEJoin's canonical equality bytes — is
/// charged for the RAM it actually holds. Without it the spill threshold, and
/// (through the same per-pair figure) block sizing, resident admission, and the
/// pre-output abort gate, would all read under the true residency for a wide or
/// data-expanding equality key. A payload with no heap key uses the default `0`,
/// so pure-range and every other sort stay byte-for-byte unchanged.
pub trait HeapBytes {
    /// Retained payload backing not already charged to the target allocation ledger.
    fn unaccounted_heap_bytes(&self, resources: &AllocationResources) -> usize;
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapBytes for () {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for u64 {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for crate::executor::stream_event::SourceRowId {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (u64, u64) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (u64, crate::executor::stream_event::SourceRowId) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (crate::executor::stream_event::SourceRowId, u64) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (u64, u64, u64) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (crate::executor::stream_event::SourceRowId, u64, u64) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}
impl HeapBytes for (i64, i64, u64) {
    fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
        0
    }
}

/// One entry of a field-ordered sort's per-row index: the row's abbreviated
/// sort key and the row's position among the resident pairs.
type SortIndexEntry = (u64, usize);

/// Bytes a field-ordered buffer charges for each row's sort-index entry, taken
/// from the entry's own layout so the charge cannot drift from what the index
/// holds.
pub(crate) const SORT_INDEX_ENTRY_BYTES: usize = std::mem::size_of::<SortIndexEntry>();

/// Rows at the first abbreviation checkpoint of a sequential sort; each later
/// checkpoint doubles it.
const FIRST_ABBREVIATION_CHECKPOINT: usize = 100;

/// Past this many rows each checkpoint the schedule reaches tightens the
/// share of distinct full keys the abbreviations must keep apart, so a large
/// sort pays for abbreviating only while it still separates most rows.
const ABBREVIATION_TIGHTENS_AFTER_ROWS: usize = 10_000;

/// Whether a sort over `rows` rows should keep sorting on abbreviations, from
/// the estimated distinct abbreviations and distinct full keys among them.
/// Continue while the abbreviations keep apart more than a share of the
/// distinct keys: a fifth, shrinking by a factor of 0.65 at each checkpoint
/// past [`ABBREVIATION_TIGHTENS_AFTER_ROWS`]. Both estimates count as at
/// least one, so a run whose keys are all equal keeps abbreviating (equal full
/// keys would tie under either comparator). The one rule both the sequential
/// and the pooled pass apply.
fn abbreviation_continues(abbreviated: u64, full: u64, rows: usize) -> bool {
    let mut share = 0.20;
    let mut checkpoint = FIRST_ABBREVIATION_CHECKPOINT;
    while checkpoint <= rows {
        if checkpoint > ABBREVIATION_TIGHTENS_AFTER_ROWS {
            share *= 0.65;
        }
        checkpoint *= 2;
    }
    abbreviated.max(1) as f64 > full.max(1) as f64 * share
}

/// Distinct-count sketches of one pass's abbreviations and full keys. Local to
/// one sort: every run is measured on its own rows.
struct KeySketches {
    abbreviated: Hll<256>,
    full: Hll<256>,
}

impl KeySketches {
    fn new() -> Self {
        Self {
            abbreviated: Hll::new(),
            full: Hll::new(),
        }
    }

    fn add(&mut self, abbreviation: u64, key: &[u8]) {
        // Fixed seeds, so the verdict on a given input is reproducible.
        const FULL_KEY_HASHER: ahash::RandomState = ahash::RandomState::with_seeds(
            0x243F_6A88_85A3_08D3,
            0x1319_8A2E_0370_7344,
            0xA409_3822_299F_31D0,
            0x082E_FA98_EC4E_6C89,
        );
        self.abbreviated.add(splitmix64(abbreviation));
        self.full.add(FULL_KEY_HASHER.hash_one(key));
    }

    fn merge(&mut self, other: &Self) {
        self.abbreviated.merge(&other.abbreviated);
        self.full.merge(&other.full);
    }

    fn continues(&self, rows: usize) -> bool {
        abbreviation_continues(self.abbreviated.estimate(), self.full.estimate(), rows)
    }
}

/// Rows per chunk of the pooled encode pass: `min(rows, 4 × threads)` near-equal
/// contiguous chunks, so the chunk sketches are bounded by the pool, not the
/// input.
fn encode_chunk_rows(rows: usize, threads: usize) -> usize {
    let chunks = rows.min(4 * threads.max(1)).max(1);
    rows.div_ceil(chunks).max(1)
}

/// Encode every row's abbreviated key in arrival order on the calling thread.
/// When `measure`, checks the sketches at 100 rows and each doubling, and
/// returns `Err(rows)` with the checkpoint that decided to abort.
fn encode_sequential<P>(
    pairs: &[(Record, P)],
    keys: &ResolvedSortKeys,
    measure: bool,
) -> Result<Vec<SortIndexEntry>, usize> {
    let mut index = Vec::with_capacity(pairs.len());
    let mut key = Vec::new();
    let mut sketches = KeySketches::new();
    let mut checkpoint = FIRST_ABBREVIATION_CHECKPOINT;
    for (row, (record, _)) in pairs.iter().enumerate() {
        keys.encode_into(record, &mut key);
        let abbreviation = abbreviated_key(&key);
        index.push((abbreviation, row));
        if measure {
            sketches.add(abbreviation, &key);
            if row + 1 == checkpoint {
                if !sketches.continues(checkpoint) {
                    return Err(checkpoint);
                }
                checkpoint *= 2;
            }
        }
    }
    Ok(index)
}

/// Encode every row's abbreviated key in parallel chunks on `pool`, each chunk
/// writing its own slice of the index with its own scratch key and sketches.
/// When `measure`, merges the chunk sketches and decides once over all rows,
/// returning `Err(rows)` to abort. A sort below the first checkpoint never
/// aborts, as on the sequential path.
fn encode_pooled<P: Sync>(
    pairs: &[(Record, P)],
    keys: &ResolvedSortKeys,
    pool: &rayon::ThreadPool,
    measure: bool,
) -> Result<Vec<SortIndexEntry>, usize> {
    let rows = pairs.len();
    let chunk_rows = encode_chunk_rows(rows, pool.current_num_threads());
    let mut index: Vec<SortIndexEntry> = vec![(0, 0); rows];
    let chunk_sketches: Vec<KeySketches> = pool.install(|| {
        index
            .par_chunks_mut(chunk_rows)
            .zip(pairs.par_chunks(chunk_rows))
            .enumerate()
            .map(|(chunk, (entries, chunk_pairs))| {
                let mut key = Vec::new();
                let mut sketches = KeySketches::new();
                for (offset, (entry, (record, _))) in
                    entries.iter_mut().zip(chunk_pairs).enumerate()
                {
                    keys.encode_into(record, &mut key);
                    let abbreviation = abbreviated_key(&key);
                    *entry = (abbreviation, chunk * chunk_rows + offset);
                    if measure {
                        sketches.add(abbreviation, &key);
                    }
                }
                sketches
            })
            .collect()
    });
    if measure && rows >= FIRST_ABBREVIATION_CHECKPOINT {
        let mut merged = KeySketches::new();
        for sketches in &chunk_sketches {
            merged.merge(sketches);
        }
        if !merged.continues(rows) {
            return Err(rows);
        }
    }
    Ok(index)
}

/// Reorder `pairs` so position `k` holds the pair `index[k]` names, following
/// each cycle of the permutation with swaps. Each visited entry is marked by
/// pointing it at its own position, so no scratch beyond the index is needed.
fn permute_in_place<T>(pairs: &mut [T], index: &mut [SortIndexEntry]) {
    for start in 0..pairs.len() {
        let mut slot = start;
        loop {
            let source = index[slot].1;
            index[slot].1 = slot;
            if source == start {
                break;
            }
            pairs.swap(slot, source);
            slot = source;
        }
    }
}

/// How many runs of equal abbreviations a sort examined, and how many of
/// those it had to sort because they were out of order.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TieRuns {
    examined: usize,
    sorted: usize,
}

impl TieRuns {
    fn plus(self, other: Self) -> Self {
        Self {
            examined: self.examined + other.examined,
            sorted: self.sorted + other.sorted,
        }
    }
}

/// Whether `run` is already in order under the comparator: one comparison per
/// adjacent pair, no allocation.
fn in_order<P>(run: &[(Record, P)], keys: &ResolvedSortKeys) -> bool {
    run.windows(2)
        .all(|pair| keys.compare(&pair[0].0, &pair[1].0) != Ordering::Greater)
}

/// Settle the order inside each run of equal abbreviations, on the calling
/// thread. `pairs` is already in index order and `index[k].0` is the
/// abbreviation of `pairs[k]`, so each run holds its rows in arrival order. A
/// run already in order under the comparator is left as it is (an all-equal
/// run always is); any other run is stable-sorted on its own, which keeps
/// equal keys in arrival order. The sort allocates scratch for that run alone,
/// never more than the stable sort of the whole buffer would.
fn fix_tie_runs<P>(
    pairs: &mut [(Record, P)],
    index: &[SortIndexEntry],
    keys: &ResolvedSortKeys,
) -> TieRuns {
    let mut runs = TieRuns::default();
    let mut start = 0;
    while start < pairs.len() {
        let abbreviation = index[start].0;
        let end = start
            + index[start..]
                .iter()
                .take_while(|&&(other, _)| other == abbreviation)
                .count();
        if end - start >= 2 {
            runs.examined += 1;
            let run = &mut pairs[start..end];
            if !in_order(run, keys) {
                run.sort_by(|(a, _), (b, _)| keys.compare(a, b));
                runs.sorted += 1;
            }
        }
        start = end;
    }
    runs
}

/// [`fix_tie_runs`] spread over the pool the caller has installed. A slice no
/// longer than `leaf` is fixed sequentially. A longer one is split at the run
/// boundary nearest its middle (the first at or after it, else the last before
/// it) and both halves are fixed in parallel, so no run is ever split and each
/// is examined exactly once. A longer slice with no interior boundary is a
/// single run: it is checked once and, when out of order, sorted with the
/// pool's stable sort. Every split shortens both halves, so the recursion ends;
/// its searches read only the index, and it allocates nothing per run.
fn fix_tie_runs_parallel<P: Send>(
    pairs: &mut [(Record, P)],
    index: &[SortIndexEntry],
    keys: &ResolvedSortKeys,
    leaf: usize,
) -> TieRuns {
    let len = pairs.len();
    if len <= leaf {
        return fix_tie_runs(pairs, index, keys);
    }
    let boundary = |k: usize| index[k - 1].0 != index[k].0;
    let middle = len / 2;
    let split = (middle..len)
        .find(|&k| boundary(k))
        .or_else(|| (1..middle).rev().find(|&k| boundary(k)));
    match split {
        Some(k) => {
            let (left_pairs, right_pairs) = pairs.split_at_mut(k);
            let (left_index, right_index) = index.split_at(k);
            let (left, right) = rayon::join(
                || fix_tie_runs_parallel(left_pairs, left_index, keys, leaf),
                || fix_tie_runs_parallel(right_pairs, right_index, keys, leaf),
            );
            left.plus(right)
        }
        None => {
            let sorted = !in_order(pairs, keys);
            if sorted {
                pairs.par_sort_by(|(a, _), (b, _)| keys.compare(a, b));
            }
            TieRuns {
                examined: 1,
                sorted: usize::from(sorted),
            }
        }
    }
}

/// Result of finishing a sort buffer: either all (record, payload) pairs
/// fit in memory, or some were spilled to disk.
pub enum SortedOutput<P> {
    /// All pairs fit in memory. Sorted and ready to iterate.
    InMemory(Vec<(Record, P)>),
    /// Pairs were spilled to sorted temp files. Must be merged via LoserTree.
    Spilled(Vec<SpillFile<P>>),
}

/// How a [`SortBuffer`] orders its accumulated pairs.
enum SortOrdering {
    /// Order by the authored fields, resolved once against the buffer's
    /// schema; the record carries the sort key and the payload rides along
    /// inert.
    Fields(ResolvedSortKeys),
    /// Order by the carried payload `P: Ord` directly, with no record field
    /// consulted. For a sort key computed off the record rather than stored in
    /// a column.
    Payload,
}

/// Whether a field-ordered buffer sorts its rows on abbreviated keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Abbreviation {
    /// Abbreviate, measuring each run's abbreviations against its full keys,
    /// and latch off once they stop telling the rows apart.
    Trying,
    /// Sort on the full comparator only; latched by an abort, and the state of
    /// a payload-ordered buffer, which has no keys to abbreviate.
    Off,
    /// Abbreviate without measuring, so a test can drive the abbreviated sort
    /// on keys whose abbreviations collide.
    #[cfg(test)]
    Forced,
}

/// Accumulates `(record, payload)` pairs, sorts in-memory or spills to disk
/// when the byte budget is exceeded. Orders either by a record-field comparator
/// (payload inert) or by the payload `P: Ord` directly, per the constructor
/// chosen.
pub struct SortBuffer<P> {
    pairs: Vec<(Record, P)>,
    ordering: SortOrdering,
    bytes_used: usize,
    unaccounted_bytes_used: usize,
    allocation_resources: AllocationResources,
    /// Total pairs ever pushed, across every spilled run and the resident
    /// tail. Never decremented on spill — `sort_and_spill` and `finish` move
    /// pairs out but drop none, so this equals the exact count the finished
    /// output emits. Lets a spilled consumer size its drain in O(1) without a
    /// disk scan.
    total_rows: usize,
    spill_threshold: usize,
    spill_dir: Option<PathBuf>,
    /// Whether spilled sorted runs are LZ4-compressed. Resolved by the caller
    /// from the workspace `[storage.spill] compress` knob against the sort
    /// schema's width and the run's batch size, so the on-disk format matches
    /// what `--explain` reports.
    spill_compress: bool,
    spill_files: Vec<SpillFile<P>>,
    schema: SharedStorage<Schema>,
    /// The run's kernel pool the comparator sort runs on. `None` sorts
    /// sequentially on the calling thread; the sort never reaches rayon's
    /// global pool, whose workers the run neither sizes nor owns.
    kernel_pool: Option<std::sync::Arc<rayon::ThreadPool>>,
    abbreviation: Abbreviation,
    /// The rows at which this buffer aborted abbreviation, once it has.
    #[cfg(test)]
    abbreviation_abort: Option<usize>,
    /// Tie runs examined and sorted over every run this buffer sorted.
    #[cfg(test)]
    tie_runs: TieRuns,
}

// `P: Ord` spans the whole impl, not just the payload-ordered constructor, so
// the shared `sort_and_spill` / `finish` path can compare payloads in the
// payload-ordered mode without splitting the buffer across two impl blocks.
// Every payload type in use is already `Ord`, so this constrains no caller.
// `Sync` lets the pooled sort read the resident pairs from every worker; every
// payload type in use is plain data.
impl<P: Serialize + DeserializeOwned + Send + Sync + Ord + HeapBytes> SortBuffer<P> {
    /// Field-ordered buffer: pairs sort by `sort_by` over each record's fields
    /// and the payload rides along inert. The historical mode; used by every
    /// source/output/DAG/join sort.
    pub fn new(
        sort_by: Vec<SortField>,
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        spill_compress: bool,
        schema: SharedStorage<Schema>,
        allocation_resources: AllocationResources,
    ) -> Self {
        Self {
            pairs: Vec::new(),
            ordering: SortOrdering::Fields(ResolvedSortKeys::for_sort_fields(
                &sort_by,
                Some(&schema),
            )),
            bytes_used: 0,
            unaccounted_bytes_used: 0,
            allocation_resources,
            total_rows: 0,
            spill_threshold,
            spill_dir,
            spill_compress,
            spill_files: Vec::new(),
            schema,
            kernel_pool: None,
            abbreviation: Abbreviation::Trying,
            #[cfg(test)]
            abbreviation_abort: None,
            #[cfg(test)]
            tie_runs: TieRuns::default(),
        }
    }

    /// Payload-ordered buffer: pairs sort by the payload `P: Ord` directly, with
    /// no record field consulted. For a sort key computed off the record (e.g. a
    /// range join's evaluated inequality keys) that backs no single column, so
    /// no synthetic sort column need be stamped onto the records. Spill,
    /// merge, and charging behave exactly as in the field-ordered mode.
    pub fn new_payload_ordered(
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        spill_compress: bool,
        schema: SharedStorage<Schema>,
        allocation_resources: AllocationResources,
    ) -> Self {
        Self {
            pairs: Vec::new(),
            ordering: SortOrdering::Payload,
            bytes_used: 0,
            unaccounted_bytes_used: 0,
            allocation_resources,
            total_rows: 0,
            spill_threshold,
            spill_dir,
            spill_compress,
            spill_files: Vec::new(),
            schema,
            kernel_pool: None,
            abbreviation: Abbreviation::Off,
            #[cfg(test)]
            abbreviation_abort: None,
            #[cfg(test)]
            tie_runs: TieRuns::default(),
        }
    }

    /// Sort on `pool` instead of sequentially on the calling thread. Output is
    /// identical either way; the pool only spreads the comparator sort over
    /// the run's kernel workers.
    pub fn with_kernel_pool(mut self, pool: std::sync::Arc<rayon::ThreadPool>) -> Self {
        self.kernel_pool = Some(pool);
        self
    }

    /// Abbreviate every run without measuring whether it pays.
    #[cfg(test)]
    pub(crate) fn forcing_abbreviation(mut self) -> Self {
        self.abbreviation = Abbreviation::Forced;
        self
    }

    /// Sort on the full comparator only.
    #[cfg(test)]
    pub(crate) fn without_abbreviation(mut self) -> Self {
        self.abbreviation = Abbreviation::Off;
        self
    }

    /// The rows at which the buffer aborted abbreviation: the deciding
    /// checkpoint on the sequential path, the whole run on the pooled path.
    /// `None` while it has not aborted.
    #[cfg(test)]
    pub(crate) fn abbreviation_abort(&self) -> Option<usize> {
        self.abbreviation_abort
    }

    /// Runs of two or more equal abbreviations examined, over every run sorted.
    #[cfg(test)]
    pub(crate) fn tie_runs_examined(&self) -> usize {
        self.tie_runs.examined
    }

    /// Runs of equal abbreviations that were out of order and had to be sorted.
    #[cfg(test)]
    pub(crate) fn tie_runs_sorted(&self) -> usize {
        self.tie_runs.sorted
    }

    /// Push a `(record, payload)` pair into the buffer. The payload's own heap
    /// (its `HeapBytes`, e.g. a variable-length key) is charged alongside the
    /// record so a wide-key payload is not undercounted, and a field-ordered
    /// buffer also charges the row's sort-index entry.
    pub fn push(&mut self, record: Record, payload: P) {
        // A spilled run stores each row's values by position and decodes them
        // under this buffer's schema, so a row of other columns would come back
        // with its values under the wrong names.
        debug_assert!(
            SharedStorage::ptr_eq(record.schema(), &self.schema)
                || record.schema().columns() == self.schema.columns(),
            "sort buffer received a row whose columns {:?} differ from the buffer's columns \
             {:?}; a spilled run would reattach the buffer's schema to the row's positional values",
            record.schema().columns(),
            self.schema.columns(),
        );
        // A field-ordered sort builds one index entry per resident row when it
        // sorts, and the stable sort it falls back to holds scratch of about
        // the same size, so the entry is charged from the row's arrival
        // whichever way the run ends up sorted.
        let index_bytes = match self.ordering {
            SortOrdering::Fields(_) => SORT_INDEX_ENTRY_BYTES,
            SortOrdering::Payload => 0,
        };
        let size = std::mem::size_of::<Record>()
            + record.estimated_heap_size()
            + std::mem::size_of::<P>()
            + payload.heap_bytes()
            + index_bytes;
        self.unaccounted_bytes_used += std::mem::size_of::<Record>()
            + record.unaccounted_heap_size(&self.allocation_resources)
            + std::mem::size_of::<P>()
            + payload.unaccounted_heap_bytes(&self.allocation_resources)
            + index_bytes;
        self.bytes_used += size;
        self.total_rows += 1;
        self.pairs.push((record, payload));
    }

    /// Check if the buffer has exceeded its memory threshold.
    pub fn should_spill(&self) -> bool {
        self.bytes_used > 0 && self.bytes_used >= self.spill_threshold
    }

    /// Stable-sort the in-memory pairs by the buffer's ordering mode: a
    /// parallel stable sort on the buffer's kernel pool when it has one, else
    /// the sequential stable sort on the calling thread. `par_sort_by`
    /// preserves the tie-break order of the sequential `slice::sort_by`, so a
    /// spilled run is byte-identical either way and equal keys keep input
    /// order. A field-ordered buffer first tries its abbreviated index sort,
    /// which produces that same order.
    fn sort_pairs(&mut self) {
        if self.pairs.len() >= 2
            && self.abbreviation != Abbreviation::Off
            && self.sort_abbreviated()
        {
            return;
        }
        // Split the borrow so the comparator can read `ordering` while the
        // sort holds `pairs` mutably.
        let Self {
            pairs,
            ordering,
            kernel_pool,
            ..
        } = self;
        match (ordering, kernel_pool.as_deref()) {
            (SortOrdering::Fields(keys), Some(pool)) => pool.install(|| {
                pairs.par_sort_by(|(a, _), (b, _)| keys.compare(a, b));
            }),
            (SortOrdering::Fields(keys), None) => {
                pairs.sort_by(|(a, _), (b, _)| keys.compare(a, b));
            }
            (SortOrdering::Payload, Some(pool)) => pool.install(|| {
                pairs.par_sort_by(|(_, a), (_, b)| a.cmp(b));
            }),
            (SortOrdering::Payload, None) => pairs.sort_by(|(_, a), (_, b)| a.cmp(b)),
        }
    }

    /// Sort a field-ordered buffer's pairs through a per-row index of
    /// abbreviated keys. The index is sorted on its two integers alone, the
    /// abbreviation and then the row's position, so the in-place unstable sort
    /// yields each run of equal abbreviations in arrival order without reading
    /// a row. The pairs are moved into index order, and then each run of equal
    /// abbreviations is checked once and sorted on the comparator only when it
    /// is out of order. Where two abbreviations differ they already decide the
    /// order, so the result is exactly the stable sort by the comparator.
    ///
    /// The index is the only per-row allocation and holds exactly the entries
    /// charged at push; the integer sort allocates nothing; a run's sort needs
    /// scratch for that run alone, so the scratch alive at once stays within
    /// what the stable sort of the whole buffer would hold.
    ///
    /// Returns `false`, having latched abbreviation off for every later run,
    /// when the encode pass measured that the abbreviations no longer tell the
    /// rows apart; the caller then sorts on the full comparator.
    fn sort_abbreviated(&mut self) -> bool {
        let SortOrdering::Fields(keys) = &self.ordering else {
            return false;
        };
        let measure = self.abbreviation == Abbreviation::Trying;
        let pool = self.kernel_pool.as_deref();
        let encoded = match pool {
            Some(pool) => encode_pooled(&self.pairs, keys, pool, measure),
            None => encode_sequential(&self.pairs, keys, measure),
        };
        let mut index = match encoded {
            Ok(index) => index,
            Err(abort_rows) => {
                debug_assert!(
                    (FIRST_ABBREVIATION_CHECKPOINT..=self.pairs.len()).contains(&abort_rows),
                    "an abort is decided at a checkpoint the run reached"
                );
                self.abbreviation = Abbreviation::Off;
                #[cfg(test)]
                {
                    self.abbreviation_abort = Some(abort_rows);
                }
                return false;
            }
        };
        debug_assert_eq!(
            index.capacity(),
            self.pairs.len(),
            "the sort index holds exactly the entries charged at push"
        );
        match pool {
            Some(pool) => pool.install(|| index.par_sort_unstable()),
            None => index.sort_unstable(),
        }
        permute_in_place(&mut self.pairs, &mut index);
        let runs = match pool {
            Some(pool) => {
                let leaf = encode_chunk_rows(self.pairs.len(), pool.current_num_threads()).max(2);
                pool.install(|| fix_tie_runs_parallel(&mut self.pairs, &index, keys, leaf))
            }
            None => fix_tie_runs(&mut self.pairs, &index, keys),
        };
        debug_assert!(runs.sorted <= runs.examined);
        #[cfg(test)]
        {
            self.tie_runs = self.tie_runs.plus(runs);
        }
        true
    }

    /// Sort the current in-memory pairs and write them to a spill file. Clears
    /// the buffer and resets the byte counter. Returns the spilled file's exact
    /// on-disk byte length so the caller can charge it against the pipeline
    /// disk-spill quota; an empty buffer writes nothing and returns 0.
    pub fn sort_and_spill(&mut self) -> Result<u64, SpillError> {
        if self.pairs.is_empty() {
            return Ok(0);
        }

        self.sort_pairs();

        let mut writer: SpillWriter<P> = SpillWriter::new(
            self.schema.clone(),
            self.spill_dir.as_deref(),
            self.spill_compress,
        )?;
        // Draining drops every pair even if writing fails. Reset both observations
        // before that ownership transfer so a reusable buffer never reports lost rows.
        self.bytes_used = 0;
        self.unaccounted_bytes_used = 0;
        for (record, payload) in self.pairs.drain(..) {
            writer.write_pair(&record, &payload)?;
        }
        // The writer reports its own on-disk byte total (the same figure the
        // disk-cap accounting charges for every spill op), so a written run is
        // always charged — no post-hoc `stat` that could fail and charge 0.
        let (spill_file, written) = writer.finish_with_bytes()?;
        self.spill_files.push(spill_file);
        self.bytes_used = 0;
        Ok(written)
    }

    /// Finish the sort buffer. If pairs were spilled, remaining in-memory
    /// pairs are also spilled. Returns the sorted output — in-memory pairs or
    /// the list of sorted spill files for merge — paired with the byte length
    /// of the residue run this call spilled (0 when nothing spilled here), so
    /// the caller can charge that final run against the disk-spill quota.
    pub fn finish(mut self) -> Result<(SortedOutput<P>, u64), SpillError> {
        if self.spill_files.is_empty() {
            self.sort_pairs();
            Ok((SortedOutput::InMemory(self.pairs), 0))
        } else {
            let residue = if !self.pairs.is_empty() {
                self.sort_and_spill()?
            } else {
                0
            };
            Ok((SortedOutput::Spilled(self.spill_files), residue))
        }
    }

    /// Current estimated memory usage in bytes.
    pub fn bytes_used(&self) -> usize {
        self.bytes_used
    }

    /// Retained pair bytes not already accounted by this buffer's allocation ledger.
    pub fn unaccounted_bytes_used(&self) -> usize {
        self.unaccounted_bytes_used
    }

    /// Total pairs pushed over this buffer's life, across every spilled run and
    /// the resident tail. Equals the exact row count the finished output emits,
    /// so a spilled-run consumer can size its drain without a disk scan.
    pub(crate) fn total_rows(&self) -> usize {
        self.total_rows
    }
}

/// `MemoryConsumer` wrapper for a `SortBuffer<P>`. Holds an
/// `Arc<ConsumerHandle>` shared with the buffer: the buffer mirrors
/// its `bytes_used` into `handle.bytes` on every `push` / `sort_and_spill`
/// transition. `try_spill` flips the handle's spill-request flag; the
/// buffer's owning operator reads it at the next batch boundary and
/// calls `sort_and_spill` in-thread.
///
/// `spill_priority = 20`: sort runs are cheaper to flush than hash-
/// aggregation rebuilds — every run is already sequentially ordered
/// and writes straight through `SpillWriter<P>` with no per-group
/// fixup. `can_back_pressure = false`: an in-flight sort buffer
/// cannot be paused without losing run continuity; sort consumers
/// expect inputs to arrive monotonically.
pub struct SortConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl SortConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for SortConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    /// 0: the range join (IEJoin) kernel registers this consumer, and no
    /// reclaim pass can reach that kernel, which spills on its own
    /// thresholds, so a pass would elect it and free nothing. A refused
    /// request's E310 lists it as `cannot spill` and counts its bytes as
    /// state that cannot spill.
    fn reclaimable_bytes(&self) -> u64 {
        0
    }

    fn peak_charged_bytes(&self) -> Option<u64> {
        Some(self.handle.peak_bytes())
    }

    fn spill_priority(&self) -> i32 {
        20
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    fn test_allocation_resources() -> clinker_record::owned_storage::AllocationResources {
        clinker_format::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
        )
        .resources()
        .allocation()
        .clone()
    }

    use super::*;
    use clinker_record::Value;
    use std::sync::Arc;

    fn test_schema() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into(), "value".into()])))
    }

    fn make_record(schema: &SharedStorage<Schema>, name: &str, value: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![Value::String(name.into()), Value::Integer(value)],
        )
    }

    fn sort_by_value_asc() -> Vec<SortField> {
        vec![SortField {
            field: "value".into(),
            order: clinker_plan::config::SortOrder::Asc,
            null_order: None,
        }]
    }

    #[test]
    fn test_sort_buffer_push_tracks_bytes() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        assert_eq!(buf.bytes_used(), 0);
        buf.push(make_record(&schema, "Alice", 1), ());
        assert!(buf.bytes_used() > 0);
        let first = buf.bytes_used();
        buf.push(make_record(&schema, "Bob", 2), ());
        assert!(buf.bytes_used() > first);
    }

    #[test]
    fn test_sort_buffer_should_spill_at_threshold() {
        let schema = test_schema();
        // Very small threshold — should spill after a few records
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            100,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        assert!(!buf.should_spill());
        // Push records until we exceed 100 bytes
        for i in 0..10 {
            buf.push(make_record(&schema, &format!("name_{i}"), i), ());
            if buf.should_spill() {
                return; // Test passes — spill triggered
            }
        }
        panic!("should_spill() never triggered with 100-byte threshold");
    }

    #[test]
    fn test_sort_buffer_in_memory_sort() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(make_record(&schema, "Charlie", 30), ());
        buf.push(make_record(&schema, "Alice", 10), ());
        buf.push(make_record(&schema, "Bob", 20), ());

        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 3);
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(20)));
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_produces_spill_files() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        ); // threshold=1 → spill immediately
        buf.push(make_record(&schema, "Alice", 10), ());
        assert!(buf.should_spill());
        buf.sort_and_spill().unwrap();
        assert_eq!(buf.bytes_used(), 0);
        assert_eq!(buf.pairs.len(), 0);
    }

    #[test]
    fn test_sort_buffer_finish_spilled_returns_files() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );

        // Push and spill twice
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();
        buf.push(make_record(&schema, "A", 10), ());
        buf.sort_and_spill().unwrap();
        // Push one more without spilling — finish() will spill it
        buf.push(make_record(&schema, "C", 30), ());

        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 3); // 2 manual spills + 1 from finish()
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_files_are_sorted() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(make_record(&schema, "C", 30), ());
        buf.push(make_record(&schema, "A", 10), ());
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();

        // Read back and verify sorted
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                let reader = files[0].reader().unwrap();
                let recs: Vec<Record> = reader.map(|r| r.unwrap().0).collect();
                assert_eq!(recs.len(), 3);
                assert_eq!(recs[0].get("value"), Some(&Value::Integer(10)));
                assert_eq!(recs[1].get("value"), Some(&Value::Integer(20)));
                assert_eq!(recs[2].get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_survives_in_memory_sort() {
        // Pattern B gate: payload travels with the record through sort permutation.
        let schema = test_schema();
        let mut buf: SortBuffer<u64> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(make_record(&schema, "Charlie", 30), 100);
        buf.push(make_record(&schema, "Alice", 10), 200);
        buf.push(make_record(&schema, "Bob", 20), 300);
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 3);
                // Sorted by value asc: Alice(10,200), Bob(20,300), Charlie(30,100)
                assert_eq!(pairs[0].1, 200);
                assert_eq!(pairs[1].1, 300);
                assert_eq!(pairs[2].1, 100);
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_survives_spill() {
        // Payload survives the postcard spill envelope through the round-trip.
        let schema = test_schema();
        let mut buf: SortBuffer<u64> = SortBuffer::new(
            sort_by_value_asc(),
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(make_record(&schema, "C", 30), 100);
        buf.push(make_record(&schema, "A", 10), 200);
        buf.push(make_record(&schema, "B", 20), 300);
        buf.sort_and_spill().unwrap();
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                let reader = files[0].reader().unwrap();
                let pairs: Vec<(Record, u64)> = reader.map(|r| r.unwrap()).collect();
                assert_eq!(pairs.len(), 3);
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[0].1, 200);
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(20)));
                assert_eq!(pairs[1].1, 300);
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Integer(30)));
                assert_eq!(pairs[2].1, 100);
            }
            _ => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_ordered_in_memory_sorts_by_payload() {
        // Payload-ordered mode sorts by the (i64, i64, u64) payload — the shape
        // a range join carries — and never consults a record field. Records
        // carry an unrelated `value`; ordering must ignore it. Negative primary
        // keys must order correctly.
        let schema = test_schema();
        let mut buf: SortBuffer<(i64, i64, u64)> = SortBuffer::new_payload_ordered(
            1_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(make_record(&schema, "a", 999), (5, 0, 0));
        buf.push(make_record(&schema, "b", 111), (-3, 2, 1));
        buf.push(make_record(&schema, "c", 555), (-3, 1, 2));
        buf.push(make_record(&schema, "d", 222), (5, 0, 3));
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                let payloads: Vec<(i64, i64, u64)> = pairs.iter().map(|(_, p)| *p).collect();
                assert_eq!(payloads, vec![(-3, 1, 2), (-3, 2, 1), (5, 0, 0), (5, 0, 3)]);
            }
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_ordered_forced_spill_multiple_runs() {
        // A tiny threshold plus explicit flushes forces several
        // individually-sorted runs to disk; finish() flushes the residue as one
        // more. Payload-ordered spill uses the same envelope as field-ordered.
        let schema = test_schema();
        let mut buf: SortBuffer<(i64, i64, u64)> = SortBuffer::new_payload_ordered(
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        for i in 0..3i64 {
            buf.push(make_record(&schema, "r", i), (i, 0, i as u64));
            assert!(buf.should_spill());
            buf.sort_and_spill().unwrap();
        }
        // One more pair left resident for finish() to flush.
        buf.push(make_record(&schema, "r", 9), (9, 0, 9));
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => assert_eq!(files.len(), 4, "3 flushes + 1 residue"),
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    /// A field-ordered buffer charges each row's sort-index entry at push,
    /// both in its pressure figure and in what it reports as unaccounted; a
    /// payload-ordered buffer has no index and charges nothing for one.
    #[test]
    fn field_ordered_push_charges_the_sort_index() {
        let schema = test_schema();
        let record = make_record(&schema, "Alice", 1);
        let mut fields: SortBuffer<u64> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            false,
            schema.clone(),
            test_allocation_resources(),
        );
        let mut payload: SortBuffer<u64> = SortBuffer::new_payload_ordered(
            1_000_000,
            None,
            false,
            schema.clone(),
            test_allocation_resources(),
        );
        for pushed in 1..=3usize {
            fields.push(record.clone(), pushed as u64);
            payload.push(record.clone(), pushed as u64);
            assert_eq!(
                fields.bytes_used() - payload.bytes_used(),
                pushed * SORT_INDEX_ENTRY_BYTES
            );
            assert_eq!(
                fields.unaccounted_bytes_used() - payload.unaccounted_bytes_used(),
                pushed * SORT_INDEX_ENTRY_BYTES
            );
        }
        assert!(!fields.should_spill());
        assert!(matches!(
            fields.finish().unwrap(),
            (SortedOutput::InMemory(_), 0)
        ));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "differ from the buffer's columns")]
    fn sort_buffer_rejects_a_record_whose_columns_differ_from_its_schema() {
        let schema = test_schema();
        let reordered =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["value".into(), "name".into()])));
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            true,
            schema,
            test_allocation_resources(),
        );
        buf.push(
            Record::new(
                reordered,
                vec![Value::Integer(1), Value::String("a".into())],
            ),
            (),
        );
    }

    #[test]
    fn test_sort_buffer_empty_returns_empty() {
        let schema = test_schema();
        let buf: SortBuffer<()> = SortBuffer::new(
            sort_by_value_asc(),
            1_000_000,
            None,
            true,
            schema,
            test_allocation_resources(),
        );
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => assert!(pairs.is_empty()),
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }
    #[test]
    fn sort_buffer_separates_physical_pressure_from_local_and_foreign_ownership() {
        use clinker_format::preparation::MemoryOnlyResources;
        use clinker_record::{FieldStr, owned_storage::OwnedValues};
        use std::num::NonZeroUsize;
        let local = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let foreign = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = local.resources().allocation().clone();
        let scope = resources.scope().unwrap();
        let foreign_resources = foreign.resources();
        let foreign_scope = foreign_resources.allocation().scope().unwrap();
        let local_text = FieldStr::try_new(&"local".repeat(64), &scope).unwrap();
        let foreign_text = FieldStr::try_new(&"foreign".repeat(64), &foreign_scope).unwrap();
        let alias = local_text.clone();
        let mut values = OwnedValues::try_with_capacity(16, &scope).unwrap();
        values.try_push(Value::String(local_text), &scope).unwrap();
        values
            .try_push(
                Value::Array(OwnedValues::from_vec(vec![
                    Value::String(foreign_text),
                    Value::String("legacy".repeat(64).into()),
                ])),
                &scope,
            )
            .unwrap();
        let record = Record::from_owned_values(test_schema(), values).unwrap();
        let physical = std::mem::size_of::<Record>() + record.estimated_heap_size();
        let relative = std::mem::size_of::<Record>() + record.unaccounted_heap_size(&resources);
        assert!(physical > relative);
        assert!(
            relative > std::mem::size_of::<Record>(),
            "foreign and legacy children remain attributed"
        );
        let cloned = record.clone();
        assert!(
            !cloned.values_are_accounted_by(&resources),
            "cloning creates independent value slots"
        );
        assert!(
            cloned.unaccounted_heap_size(&resources) > record.unaccounted_heap_size(&resources)
        );
        drop(cloned);
        let charged = local.used();
        let mut buf = SortBuffer::new_payload_ordered(
            physical,
            None,
            false,
            test_schema(),
            resources.clone(),
        );
        buf.push(record, ());
        assert!(
            buf.should_spill(),
            "pressure uses physical ownership even when admission is lower"
        );
        assert_eq!(buf.bytes_used(), physical);
        assert_eq!(buf.unaccounted_bytes_used(), relative);
        assert_eq!(local.used(), charged);
        assert!(buf.sort_and_spill().unwrap() > 0);
        assert_eq!(buf.bytes_used(), 0);
        assert_eq!(buf.unaccounted_bytes_used(), 0);
        assert!(
            local.used() > 0,
            "escaped text stays charged after original slots spill"
        );
        assert_eq!(foreign.used(), 0);
        let (SortedOutput::Spilled(files), _) = buf.finish().unwrap() else {
            panic!("spilled output");
        };
        let (decoded, ()) = files[0].reader().unwrap().next().unwrap().unwrap();
        assert!(!decoded.values_are_accounted_by(&resources));
        assert_eq!(
            decoded.unaccounted_heap_size(&resources),
            decoded.estimated_heap_size()
        );
        assert_eq!(decoded.get("name"), Some(&Value::String(alias.clone())));
        drop(alias);
        assert_eq!(local.used(), 0);
    }

    #[test]
    fn sort_buffer_memory_finish_moves_governed_slots_and_failed_write_clears_counters() {
        use clinker_record::owned_storage::OwnedValues;
        #[derive(Eq, PartialEq, Ord, PartialOrd, serde::Deserialize)]
        struct RefuseSerialization;
        impl serde::Serialize for RefuseSerialization {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom(
                    "deliberate spill payload failure",
                ))
            }
        }
        impl HeapBytes for RefuseSerialization {
            fn unaccounted_heap_bytes(&self, _resources: &AllocationResources) -> usize {
                0
            }
        }
        let resources = test_allocation_resources();
        let scope = resources.scope().unwrap();
        let mut values = OwnedValues::try_with_capacity(8, &scope).unwrap();
        values.try_push(Value::Null, &scope).unwrap();
        values.try_push(Value::Integer(4), &scope).unwrap();
        let record = Record::from_owned_values(test_schema(), values).unwrap();
        let mut memory = SortBuffer::new_payload_ordered(
            usize::MAX,
            None,
            false,
            test_schema(),
            resources.clone(),
        );
        assert_eq!(memory.sort_and_spill().unwrap(), 0);
        memory.push(record, ());
        assert_eq!(
            memory.unaccounted_bytes_used(),
            std::mem::size_of::<Record>()
        );
        let (SortedOutput::InMemory(mut rows), 0) = memory.finish().unwrap() else {
            panic!("resident output");
        };
        let (moved, ()) = rows.pop().unwrap();
        assert!(moved.values_are_accounted_by(&resources));
        let mut failing = SortBuffer::new_payload_ordered(1, None, false, test_schema(), resources);
        failing.push(moved, RefuseSerialization);
        assert!(failing.sort_and_spill().is_err());
        assert_eq!(failing.bytes_used(), 0);
        assert_eq!(failing.unaccounted_bytes_used(), 0);
        assert!(failing.pairs.is_empty());
        assert_eq!(failing.sort_and_spill().unwrap(), 0);
    }

    /// Field-ordered buffers abbreviate their sort keys; these tests drive the
    /// abbreviated sort and its abort and compare every output with a stable
    /// sort by the authored comparator.
    mod abbreviated {
        use super::*;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::sort_key::compare_authored_keys;
        use crate::pipeline::spill_merge::{MergeBudget, merge_sorted_runs};
        use clinker_plan::config::{NullOrder, SortOrder};

        fn schema() -> SharedStorage<Schema> {
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["k".into(), "n".into()])))
        }

        fn field(name: &str, order: SortOrder, null_order: NullOrder) -> SortField {
            SortField {
                field: name.into(),
                order,
                null_order: Some(null_order),
            }
        }

        /// `k` ascending, then `n` ascending.
        fn by_key_then_n() -> Vec<SortField> {
            vec![
                field("k", SortOrder::Asc, NullOrder::Last),
                field("n", SortOrder::Asc, NullOrder::Last),
            ]
        }

        /// One row per key, each with an integer second field and its arrival
        /// index as the payload.
        fn rows(schema: &SharedStorage<Schema>, keys: Vec<Value>) -> Vec<(Record, u64)> {
            keys.into_iter()
                .enumerate()
                .map(|(i, key)| {
                    let record =
                        Record::new(schema.clone(), vec![key, Value::Integer((i % 97) as i64)]);
                    (record, i as u64)
                })
                .collect()
        }

        /// The payload order of a stable sort by the authored comparator.
        fn oracle(input: &[(Record, u64)], sort_by: &[SortField]) -> Vec<u64> {
            let mut sorted = input.to_vec();
            sorted.sort_by(|(a, _), (b, _)| compare_authored_keys(a, b, sort_by));
            sorted.into_iter().map(|(_, payload)| payload).collect()
        }

        #[derive(Clone, Copy, Debug)]
        enum Mode {
            Auto,
            Forced,
            Off,
        }

        fn buffer(
            sort_by: &[SortField],
            schema: &SharedStorage<Schema>,
            mode: Mode,
            pooled: bool,
        ) -> SortBuffer<u64> {
            let buffer = SortBuffer::new(
                sort_by.to_vec(),
                usize::MAX,
                None,
                false,
                schema.clone(),
                test_allocation_resources(),
            );
            let buffer = match mode {
                Mode::Auto => buffer,
                Mode::Forced => buffer.forcing_abbreviation(),
                Mode::Off => buffer.without_abbreviation(),
            };
            if pooled {
                buffer.with_kernel_pool(Arc::clone(crate::test_support::test_kernel_pool()))
            } else {
                buffer
            }
        }

        /// Sort `input` resident; returns the payload order and the
        /// buffer's abort report.
        fn resident(
            input: &[(Record, u64)],
            sort_by: &[SortField],
            mode: Mode,
            pooled: bool,
        ) -> (Vec<u64>, Option<usize>) {
            let mut buf = buffer(sort_by, &schema_of(input), mode, pooled);
            for (record, payload) in input.iter().cloned() {
                buf.push(record, payload);
            }
            // Sort the resident pairs as `finish` would, keeping the buffer to
            // read its abort report.
            buf.sort_pairs();
            let sorted = std::mem::take(&mut buf.pairs);
            (
                sorted.into_iter().map(|(_, p)| p).collect(),
                buf.abbreviation_abort(),
            )
        }

        /// Sort `input` resident, sequentially or on `pool`; returns the payload
        /// order and the buffer, to read what the sort reports.
        fn resident_on(
            input: &[(Record, u64)],
            sort_by: &[SortField],
            mode: Mode,
            pool: Option<&Arc<rayon::ThreadPool>>,
        ) -> (Vec<u64>, SortBuffer<u64>) {
            let mut buf = buffer(sort_by, &schema_of(input), mode, false);
            if let Some(pool) = pool {
                buf = buf.with_kernel_pool(Arc::clone(pool));
            }
            for (record, payload) in input.iter().cloned() {
                buf.push(record, payload);
            }
            buf.sort_pairs();
            let sorted = std::mem::take(&mut buf.pairs);
            (sorted.into_iter().map(|(_, p)| p).collect(), buf)
        }

        /// Sort `input` spilling a run every `run_rows` rows, then merge the
        /// runs.
        fn spilled(
            input: &[(Record, u64)],
            sort_by: &[SortField],
            mode: Mode,
            pooled: bool,
            run_rows: usize,
        ) -> Vec<u64> {
            let mut buf = buffer(sort_by, &schema_of(input), mode, pooled);
            for (i, (record, payload)) in input.iter().cloned().enumerate() {
                buf.push(record, payload);
                if (i + 1) % run_rows == 0 {
                    buf.sort_and_spill().unwrap();
                }
            }
            let SortedOutput::Spilled(files) = buf.finish().unwrap().0 else {
                panic!("explicit spills produce runs");
            };
            merge(files, sort_by)
        }

        fn merge(files: Vec<SpillFile<u64>>, sort_by: &[SortField]) -> Vec<u64> {
            let arbitrator =
                MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
            let budget = MergeBudget {
                budget: &arbitrator,
                node: "sort",
                compress: false,
                charge_owner: None,
            };
            merge_sorted_runs(files, sort_by, "abbreviated sort test", budget)
                .unwrap()
                .into_iter()
                .map(|(_, p)| p)
                .collect()
        }

        fn schema_of(input: &[(Record, u64)]) -> SharedStorage<Schema> {
            input
                .first()
                .map_or_else(schema, |(record, _)| record.schema().clone())
        }

        fn instant(seconds: i64) -> Value {
            Value::DateTime(
                chrono::DateTime::from_timestamp(seconds, 0)
                    .unwrap()
                    .naive_utc(),
            )
        }

        /// `n` scrambled indices `0..n`, a fixed odd-multiplier permutation.
        fn scrambled(n: usize) -> impl Iterator<Item = u64> {
            (0..n as u64).map(move |i| i.wrapping_mul(0x9E37_79B1) % n as u64)
        }

        fn distinct_datetimes(n: usize) -> Vec<Value> {
            scrambled(n)
                .map(|i| instant(1_600_000_000 + i as i64 * 61))
                .collect()
        }

        fn distinct_integers(n: usize) -> Vec<Value> {
            scrambled(n).map(|i| Value::Integer(i as i64)).collect()
        }

        fn prefixed_strings(prefix: &str, n: usize) -> Vec<Value> {
            scrambled(n)
                .map(|i| Value::String(format!("{prefix}{i:06}").into()))
                .collect()
        }

        fn short_strings(n: usize) -> Vec<Value> {
            scrambled(n)
                .map(|i| Value::String(format!("s{i:05}").into()))
                .collect()
        }

        #[test]
        fn abbreviated_sort_keeps_arrival_order_among_equal_keys() {
            let schema = schema();
            let keys = (0..5_000)
                .map(|i| Value::String(["pear", "apple", "fig"][i % 3].into()))
                .collect();
            let input = rows(&schema, keys);
            let sort_by = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let expected = oracle(&input, &sort_by);
            for pooled in [false, true] {
                let (sorted, abort) = resident(&input, &sort_by, Mode::Forced, pooled);
                assert_eq!(abort, None, "a forced abbreviation never aborts");
                assert_eq!(sorted, expected, "pooled: {pooled}");
            }
        }

        #[test]
        fn abbreviation_aborts_on_a_datetime_leading_key_and_output_is_unchanged() {
            let schema = schema();
            let input = rows(&schema, distinct_datetimes(2_000));
            let sort_by = by_key_then_n();
            let expected = oracle(&input, &sort_by);
            for pooled in [false, true] {
                let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, pooled);
                assert!(abort.is_some(), "pooled: {pooled}: a datetime lead aborts");
                assert_eq!(sorted, expected, "pooled: {pooled}");
                let (unabbreviated, _) = resident(&input, &sort_by, Mode::Off, pooled);
                assert_eq!(unabbreviated, expected, "pooled: {pooled}");
                assert_eq!(
                    spilled(&input, &sort_by, Mode::Auto, pooled, 500),
                    expected,
                    "pooled: {pooled}"
                );
            }
        }

        #[test]
        fn abbreviation_continues_on_distinct_short_keys() {
            let schema = schema();
            let input = rows(&schema, distinct_integers(2_000));
            let sort_by = by_key_then_n();
            let expected = oracle(&input, &sort_by);
            for pooled in [false, true] {
                let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, pooled);
                assert_eq!(
                    abort, None,
                    "pooled: {pooled}: distinct integers abbreviate"
                );
                assert_eq!(sorted, expected, "pooled: {pooled}");
            }
        }

        #[test]
        fn abbreviated_sort_orders_long_strings_sharing_a_prefix() {
            let schema = schema();
            let keys = (0..3_000u64)
                .map(|i| {
                    let r = i.wrapping_mul(0x9E37_79B1) % 3_000;
                    if r % 11 == 0 {
                        Value::Null
                    } else {
                        // Duplicates: 1,000 distinct tails over 3,000 rows,
                        // padded so the strings run 24 to 64 bytes.
                        let tail = format!("{:04}", r % 1_000);
                        let pad = "x".repeat((r % 41) as usize);
                        Value::String(format!("a-twenty-byte-prefix{tail}{pad}").into())
                    }
                })
                .collect();
            let input = rows(&schema, keys);
            let sort_by = vec![
                field("k", SortOrder::Desc, NullOrder::First),
                field("n", SortOrder::Asc, NullOrder::Last),
            ];
            let expected = oracle(&input, &sort_by);
            for pooled in [false, true] {
                for mode in [Mode::Forced, Mode::Auto] {
                    let (sorted, _) = resident(&input, &sort_by, mode, pooled);
                    assert_eq!(sorted, expected, "{mode:?}, pooled: {pooled}");
                    assert_eq!(
                        spilled(&input, &sort_by, mode, pooled, 700),
                        expected,
                        "{mode:?} spilled, pooled: {pooled}"
                    );
                }
            }
        }

        #[test]
        fn pooled_and_sequential_abbreviation_reach_the_same_verdict() {
            let schema = schema();
            let fixtures: Vec<(&str, Vec<Value>, bool)> = vec![
                ("datetime lead", distinct_datetimes(2_000), true),
                ("short integers", distinct_integers(2_000), false),
                (
                    "strings sharing a 12-byte prefix",
                    prefixed_strings("twelve-bytes", 2_000),
                    true,
                ),
                (
                    "16 distinct integers",
                    (0..2_000).map(|i| Value::Integer(i % 16)).collect(),
                    false,
                ),
            ];
            // Sorted on the key alone, so the full keys are exactly as
            // distinct as the fixture's values.
            let sort_by = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            for (name, keys, aborts) in fixtures {
                let input = rows(&schema, keys);
                let expected = oracle(&input, &sort_by);
                for pooled in [false, true] {
                    let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, pooled);
                    assert_eq!(abort.is_some(), aborts, "{name}, pooled: {pooled}");
                    assert_eq!(sorted, expected, "{name}, pooled: {pooled}");
                }
            }

            // A single datetime field whose first half repeats one instant and
            // whose second half is 10,000 distinct instants: the full keys
            // stay alike until the 12,800-row checkpoint.
            let skewed: Vec<Value> = (0..20_000)
                .map(|i| instant(1_600_000_000 + if i < 10_000 { 0 } else { i * 61 }))
                .collect();
            let input = rows(&schema, skewed);
            let expected = oracle(&input, &sort_by);
            let threads = crate::test_support::test_kernel_pool().current_num_threads();
            let chunk_rows = encode_chunk_rows(input.len(), threads);
            assert!(
                input.len().div_ceil(chunk_rows) >= 2,
                "the pool encodes the skewed fixture in several chunks"
            );
            let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, false);
            let at = abort.expect("the sequential path aborts on the skewed fixture");
            assert!(
                6_400 < at && at <= 12_800,
                "the sequential path continued through 6,400 rows and aborted by 12,800, at {at}"
            );
            assert_eq!(sorted, expected);
            let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, true);
            assert!(
                abort.is_some(),
                "the pooled path sees the distinct half only through its merged sketches"
            );
            assert_eq!(sorted, expected);
        }

        #[test]
        fn each_spilled_run_decides_abbreviation_on_its_own_rows() {
            let schema = schema();
            let distinct = rows(&schema, short_strings(2_000));
            let colliding: Vec<(Record, u64)> =
                rows(&schema, prefixed_strings("twelve-bytes", 2_000))
                    .into_iter()
                    .map(|(record, payload)| (record, payload + 2_000))
                    .collect();
            let input: Vec<(Record, u64)> = distinct.iter().chain(&colliding).cloned().collect();
            let sort_by = by_key_then_n();
            let expected = oracle(&input, &sort_by);
            for pooled in [false, true] {
                let mut buf = buffer(&sort_by, &schema, Mode::Auto, pooled);
                for (record, payload) in distinct.iter().cloned() {
                    buf.push(record, payload);
                }
                buf.sort_and_spill().unwrap();
                assert_eq!(buf.abbreviation_abort(), None, "pooled: {pooled}");
                for (record, payload) in colliding.iter().cloned() {
                    buf.push(record, payload);
                }
                buf.sort_and_spill().unwrap();
                assert!(buf.abbreviation_abort().is_some(), "pooled: {pooled}");
                let SortedOutput::Spilled(files) = buf.finish().unwrap().0 else {
                    panic!("explicit spills produce runs");
                };
                assert_eq!(merge(files, &sort_by), expected, "pooled: {pooled}");
            }
        }

        /// A run of equal abbreviations whose rows are already in order (here
        /// every run is one repeated key) costs one check and is not sorted; a
        /// run the abbreviation cannot order is sorted once, also on a pool
        /// small enough that the run is longer than one slice of the parallel
        /// pass.
        #[test]
        fn tie_runs_already_in_order_are_not_sorted_again() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let two_threads = Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(2)
                    .build()
                    .expect("build a two-thread pool"),
            );

            let repeated = scrambled(5_000)
                .map(|i| Value::Integer((i % 16) as i64))
                .collect();
            let input = rows(&schema, repeated);
            let expected = oracle(&input, &key_alone);
            for pool in [None, Some(crate::test_support::test_kernel_pool())] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected, "pooled: {}", pool.is_some());
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    (16, 0),
                    "pooled: {}: sixteen runs of one key each, all already in order",
                    pool.is_some()
                );
            }

            let input = rows(&schema, prefixed_strings("a-twenty-byte-prefix", 3_000));
            let expected = oracle(&input, &key_alone);
            assert!(
                encode_chunk_rows(input.len(), two_threads.current_num_threads()) < input.len(),
                "on two threads the single run is longer than one slice"
            );
            for pool in [
                None,
                Some(crate::test_support::test_kernel_pool()),
                Some(&two_threads),
            ] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected);
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    (1, 1),
                    "one run the abbreviation cannot order, sorted once"
                );
            }
        }

        /// Runs end exactly where the abbreviation changes: four groups of
        /// strings whose abbreviations differ only between groups form four
        /// runs, each sorted on its own.
        #[test]
        fn tie_runs_end_where_the_abbreviation_changes() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let grouped = scrambled(4_000)
                .map(|i| {
                    let group = ["groupA", "groupB", "groupC", "groupD"][(i % 4) as usize];
                    Value::String(format!("{group}{i:05}").into())
                })
                .collect();
            let input = rows(&schema, grouped);
            let expected = oracle(&input, &key_alone);
            for pool in [None, Some(crate::test_support::test_kernel_pool())] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected, "pooled: {}", pool.is_some());
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    (4, 4),
                    "pooled: {}: one run per group",
                    pool.is_some()
                );
            }
        }
    }
}
