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

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
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

/// The fewest rows in a chunk of a pooled abbreviated sort, and the fewest rows
/// a measured sort abbreviates at all. The standard library's stable sort takes
/// a scratch of at least this many elements for any run longer than twenty
/// rows, so a chunk of at least this length never needs a tie-run scratch
/// larger than itself, and a shorter run has nothing to gain from an index.
const MIN_CHUNK_ROWS: usize = 48;

/// How many contiguous chunks a sort of `rows` rows on a pool of `threads`
/// threads splits into: one per thread, as long as each holds at least
/// [`MIN_CHUNK_ROWS`] rows. A single chunk is the sequential sort.
fn chunk_count(rows: usize, threads: usize) -> usize {
    (rows / MIN_CHUNK_ROWS).clamp(1, threads.max(1))
}

/// The first row of chunk `chunk` among `chunks` contiguous chunks over `rows`
/// rows. Chunk lengths differ by at most one row, so none is shorter than
/// `rows / chunks`, and [`chunk_count`] keeps that at least [`MIN_CHUNK_ROWS`].
fn chunk_start(rows: usize, chunks: usize, chunk: usize) -> usize {
    chunk * rows / chunks
}

/// One chunk of a pooled sort: its first row in the run, its pairs and their
/// index entries.
type Chunk<'a, P> = (usize, &'a mut [(Record, P)], &'a mut [SortIndexEntry]);

/// Split a run's pairs and its index, which hold the same rows, into the
/// `chunks` contiguous chunks [`chunk_start`] places. The list holds one entry
/// per chunk, so its length is bounded by the pool, not the input.
fn split_chunks<'a, P>(
    mut pairs: &'a mut [(Record, P)],
    mut index: &'a mut [SortIndexEntry],
    chunks: usize,
) -> Vec<Chunk<'a, P>> {
    let rows = pairs.len();
    debug_assert_eq!(rows, index.len());
    let mut split = Vec::with_capacity(chunks);
    for chunk in 0..chunks {
        let first_row = chunk_start(rows, chunks, chunk);
        let len = chunk_start(rows, chunks, chunk + 1) - first_row;
        let (chunk_pairs, rest_pairs) = std::mem::take(&mut pairs).split_at_mut(len);
        let (chunk_index, rest_index) = std::mem::take(&mut index).split_at_mut(len);
        split.push((first_row, chunk_pairs, chunk_index));
        pairs = rest_pairs;
        index = rest_index;
    }
    split
}

/// Rows a pooled sort encodes on the calling thread, through the sequential
/// checkpoints, before it encodes the rest in parallel: the run up to its fifth
/// checkpoint. A run that stops abbreviating within them never pays for the
/// parallel encode, and decides exactly as a sequential sort would.
const POOLED_PREFIX_ROWS: usize = 1_600;

/// Encode the abbreviated key of each of `pairs`, rows `0..pairs.len()` of the
/// run, in arrival order on the calling thread, appending one entry per row to
/// `index` and feeding `sketches`. When `measure`, checks the sketches at 100
/// rows and each doubling, and returns `Err(rows)` with the checkpoint that
/// decided to abort.
fn encode_sequential<P>(
    pairs: &[(Record, P)],
    keys: &ResolvedSortKeys,
    measure: bool,
    index: &mut Vec<SortIndexEntry>,
    sketches: &mut KeySketches,
) -> Result<(), usize> {
    let mut key = Vec::new();
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
    Ok(())
}

/// Encode a pooled sort's abbreviated keys into `index`, over the `chunks`
/// chunks the sort will order. The first [`POOLED_PREFIX_ROWS`] rows are
/// encoded on the calling thread through the sequential checkpoints, so a run
/// that stops abbreviating early stops there. Each chunk then encodes its own
/// rows past the prefix in parallel on `pool`, with its own scratch key and
/// sketches, writing its own slice of the index; the rows are only read.
///
/// When `measure`, the prefix's sketches are merged with every chunk's and the
/// run is decided once over all its rows, returning `Err(rows)` to abort. A
/// chunk's distinct counts are not the run's, so the chunks contribute sketches
/// rather than verdicts. A run no longer than the prefix takes no such verdict:
/// it was decided at the same checkpoints a sequential sort of it reaches.
/// Returns how many rows were encoded in parallel.
fn encode_pooled<P: Send>(
    pairs: &mut [(Record, P)],
    keys: &ResolvedSortKeys,
    pool: &rayon::ThreadPool,
    chunks: usize,
    measure: bool,
    index: &mut Vec<SortIndexEntry>,
) -> Result<usize, usize> {
    let rows = pairs.len();
    let prefix_rows = rows.min(POOLED_PREFIX_ROWS);
    let mut sketches = KeySketches::new();
    encode_sequential(&pairs[..prefix_rows], keys, measure, index, &mut sketches)?;
    if rows == prefix_rows {
        return Ok(0);
    }
    index.resize(rows, (0, 0));
    let chunk_sketches: Vec<KeySketches> = pool.install(|| {
        split_chunks(pairs, index, chunks)
            .into_par_iter()
            .map(|(first_row, chunk_pairs, entries)| {
                let mut key = Vec::new();
                let mut sketches = KeySketches::new();
                let encoded_by_prefix = prefix_rows.saturating_sub(first_row);
                for (offset, (entry, (record, _))) in entries
                    .iter_mut()
                    .zip(chunk_pairs.iter())
                    .enumerate()
                    .skip(encoded_by_prefix)
                {
                    keys.encode_into(record, &mut key);
                    let abbreviation = abbreviated_key(&key);
                    *entry = (abbreviation, first_row + offset);
                    if measure {
                        sketches.add(abbreviation, &key);
                    }
                }
                sketches
            })
            .collect()
    });
    if measure {
        for chunk in &chunk_sketches {
            sketches.merge(chunk);
        }
        if !sketches.continues(rows) {
            return Err(rows);
        }
    }
    Ok(rows - prefix_rows)
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
/// equal keys in arrival order. One run is sorted at a time, and its scratch
/// is for that run alone.
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

/// Sort one chunk's pairs into the stable order of the comparator through its
/// index entries, which name rows of the whole run: rebase them to the chunk's
/// `first_row`, sort them on their two integers (in place, no allocation), move
/// the pairs into that order, and fix the chunk's tie runs.
fn sort_chunk<P>(
    pairs: &mut [(Record, P)],
    index: &mut [SortIndexEntry],
    first_row: usize,
    keys: &ResolvedSortKeys,
) -> TieRuns {
    if first_row != 0 {
        for entry in index.iter_mut() {
            entry.1 -= first_row;
        }
    }
    index.sort_unstable();
    permute_in_place(pairs, index);
    fix_tie_runs(pairs, index, keys)
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
    /// Rows the pooled encode pass encoded in parallel, over every run.
    #[cfg(test)]
    rows_encoded_in_parallel: usize,
    /// Merges of presorted chunks a pooled sort ran, over every run.
    #[cfg(test)]
    pooled_merges: usize,
    /// Chunks found in order just before a pooled sort's merge, over every
    /// run.
    #[cfg(test)]
    chunks_in_order_before_merge: usize,
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
            #[cfg(test)]
            rows_encoded_in_parallel: 0,
            #[cfg(test)]
            pooled_merges: 0,
            #[cfg(test)]
            chunks_in_order_before_merge: 0,
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
            #[cfg(test)]
            rows_encoded_in_parallel: 0,
            #[cfg(test)]
            pooled_merges: 0,
            #[cfg(test)]
            chunks_in_order_before_merge: 0,
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
    /// checkpoint when one decided (on the sequential path, or within a pooled
    /// sort's prefix), else the whole run on the pooled path. `None` while it
    /// has not aborted.
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

    /// Rows the pooled encode pass encoded in parallel, after the prefix
    /// encoded on the calling thread.
    #[cfg(test)]
    pub(crate) fn rows_encoded_in_parallel(&self) -> usize {
        self.rows_encoded_in_parallel
    }

    /// Merges of presorted chunks a pooled sort ran.
    #[cfg(test)]
    pub(crate) fn pooled_merges(&self) -> usize {
        self.pooled_merges
    }

    /// Chunks that were in order under the comparator just before a pooled
    /// sort's merge.
    #[cfg(test)]
    pub(crate) fn chunks_in_order_before_merge(&self) -> usize {
        self.chunks_in_order_before_merge
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
        if self.abbreviates(self.pairs.len()) && self.sort_abbreviated() {
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

    /// Whether a run of `rows` rows is sorted through the abbreviated index. A
    /// measured sort leaves a run shorter than [`MIN_CHUNK_ROWS`] to the
    /// comparator sort without latching anything, so a later, longer run still
    /// abbreviates.
    fn abbreviates(&self, rows: usize) -> bool {
        match self.abbreviation {
            Abbreviation::Trying => rows >= MIN_CHUNK_ROWS,
            Abbreviation::Off => false,
            #[cfg(test)]
            Abbreviation::Forced => rows >= 2,
        }
    }

    /// Sort a field-ordered buffer's pairs through a per-row index of
    /// abbreviated keys, in [`chunk_count`] contiguous chunks: one without a
    /// pool or on a one-thread pool, else one per pool thread.
    ///
    /// Each chunk sorts its index entries on their two integers alone, the
    /// abbreviation and then the row's position, so the in-place unstable sort
    /// yields each run of equal abbreviations in arrival order without reading
    /// a row. The chunk's pairs are moved into index order, and each run of
    /// equal abbreviations is checked once and sorted on the comparator only
    /// when it is out of order. Where two abbreviations differ they already
    /// decide the order, so each chunk ends as the stable sort of its own rows.
    /// One chunk is then the whole result. Several chunks are sorted in
    /// parallel, the index is dropped, and the pool's stable `par_sort_by`
    /// with the comparator merges them: chunks are contiguous and in arrival
    /// order, so of two rows with equal keys the earlier one is still first,
    /// and the stable merge keeps it there.
    ///
    /// The index holds exactly the entries charged at push and is the only
    /// per-row allocation this adds to the comparator sort; the integer sorts
    /// allocate nothing; each chunk sorts one tie run at a time with scratch
    /// for that run alone. The merge is the comparator sort's own parallel
    /// sort, with its own scratch, and runs after the index is gone.
    ///
    /// Returns `false`, having latched abbreviation off for every later run,
    /// when the encode pass measured that the abbreviations no longer tell the
    /// rows apart; the caller then sorts on the full comparator.
    fn sort_abbreviated(&mut self) -> bool {
        let SortOrdering::Fields(keys) = &self.ordering else {
            return false;
        };
        let measure = self.abbreviation == Abbreviation::Trying;
        let rows = self.pairs.len();
        let pooled = self.kernel_pool.as_deref().and_then(|pool| {
            let chunks = chunk_count(rows, pool.current_num_threads());
            (chunks >= 2).then_some((pool, chunks))
        });
        let mut index = Vec::with_capacity(rows);
        let encoded = match pooled {
            Some((pool, chunks)) => {
                encode_pooled(&mut self.pairs, keys, pool, chunks, measure, &mut index)
            }
            None => encode_sequential(
                &self.pairs,
                keys,
                measure,
                &mut index,
                &mut KeySketches::new(),
            )
            .map(|()| 0),
        };
        let parallel_rows = match encoded {
            Ok(parallel_rows) => parallel_rows,
            Err(abort_rows) => {
                debug_assert!(
                    (FIRST_ABBREVIATION_CHECKPOINT..=rows).contains(&abort_rows),
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
        debug_assert!(parallel_rows <= rows);
        #[cfg(test)]
        {
            self.rows_encoded_in_parallel += parallel_rows;
        }
        debug_assert_eq!(
            index.capacity(),
            rows,
            "the sort index holds exactly the entries charged at push"
        );
        let runs = match pooled {
            None => sort_chunk(&mut self.pairs, &mut index, 0, keys),
            Some((pool, chunks)) => {
                let runs = pool.install(|| {
                    split_chunks(&mut self.pairs, &mut index, chunks)
                        .into_par_iter()
                        .map(|(first_row, pairs, index)| sort_chunk(pairs, index, first_row, keys))
                        .reduce(TieRuns::default, TieRuns::plus)
                });
                drop(index);
                #[cfg(test)]
                {
                    self.chunks_in_order_before_merge += (0..chunks)
                        .filter(|&chunk| {
                            let first = chunk_start(rows, chunks, chunk);
                            let end = chunk_start(rows, chunks, chunk + 1);
                            in_order(&self.pairs[first..end], keys)
                        })
                        .count();
                }
                pool.install(|| {
                    self.pairs.par_sort_by(|(a, _), (b, _)| keys.compare(a, b));
                });
                #[cfg(test)]
                {
                    self.pooled_merges += 1;
                }
                runs
            }
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

        /// A dedicated pool of exactly `threads` workers, so a test of the
        /// chunked sort does not depend on the machine's thread count.
        fn pool_of(threads: usize) -> Arc<rayon::ThreadPool> {
            Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("build a dedicated test pool"),
            )
        }

        /// The chunks a sort of `rows` rows splits into on `pool`; one
        /// without a pool.
        fn chunks_on(rows: usize, pool: Option<&Arc<rayon::ThreadPool>>) -> usize {
            pool.map_or(1, |pool| chunk_count(rows, pool.current_num_threads()))
        }

        /// The value these fixtures' abbreviations distinguish: an integer
        /// key's value, or a string key's first six characters (the eight-byte
        /// abbreviation spends two bytes on the field's null sentinel and the
        /// value's type tag).
        fn abbreviated_group(record: &Record) -> String {
            match record.get("k") {
                Some(Value::Integer(i)) => i.to_string(),
                Some(Value::String(s)) => s.as_str().chars().take(6).collect(),
                other => panic!("no abbreviation group for {other:?}"),
            }
        }

        /// The tie runs (examined, sorted) a sort of `input` in `chunks`
        /// chunks reaches: in each chunk, one run per abbreviation group of at
        /// least two rows, sorted when its rows in arrival order are out of
        /// order under the authored comparator.
        fn expected_tie_runs(
            input: &[(Record, u64)],
            sort_by: &[SortField],
            chunks: usize,
        ) -> (usize, usize) {
            let rows = input.len();
            let (mut examined, mut sorted) = (0, 0);
            for chunk in 0..chunks {
                let mut groups: std::collections::BTreeMap<String, Vec<&Record>> =
                    std::collections::BTreeMap::new();
                for (record, _) in
                    &input[chunk_start(rows, chunks, chunk)..chunk_start(rows, chunks, chunk + 1)]
                {
                    groups
                        .entry(abbreviated_group(record))
                        .or_default()
                        .push(record);
                }
                for run in groups.values().filter(|run| run.len() >= 2) {
                    examined += 1;
                    if run.windows(2).any(|pair| {
                        compare_authored_keys(pair[0], pair[1], sort_by) == Ordering::Greater
                    }) {
                        sorted += 1;
                    }
                }
            }
            (examined, sorted)
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
            let two_threads = pool_of(2);
            assert!(
                chunks_on(input.len(), Some(&two_threads)) >= 2,
                "the pool encodes the skewed fixture in several chunks"
            );
            let (sorted, abort) = resident(&input, &sort_by, Mode::Auto, false);
            let at = abort.expect("the sequential path aborts on the skewed fixture");
            assert!(
                6_400 < at && at <= 12_800,
                "the sequential path continued through 6,400 rows and aborted by 12,800, at {at}"
            );
            assert_eq!(sorted, expected);
            let (sorted, buf) = resident_on(&input, &sort_by, Mode::Auto, Some(&two_threads));
            assert!(
                buf.abbreviation_abort().is_some(),
                "the pooled path sees the distinct half only through its merged sketches"
            );
            assert_eq!(sorted, expected);

            // A run no longer than the pooled prefix is decided at the
            // sequential checkpoints alone. Its first 800 rows cycle 100 short
            // strings and its last 700 share a 12-byte prefix: every checkpoint
            // up to 800 rows sees about as many distinct abbreviations as
            // distinct keys, and no checkpoint falls after it. A verdict over
            // all 1,500 rows would see 101 abbreviations for about 800 keys.
            let prefix_decided: Vec<Value> = (0..800)
                .map(|i| Value::String(format!("k{:03}", i % 100).into()))
                .chain(prefixed_strings("twelve-bytes", 700))
                .collect();
            let input = rows(&schema, prefix_decided);
            let expected = oracle(&input, &sort_by);
            assert!(input.len() <= POOLED_PREFIX_ROWS);
            assert!(chunks_on(input.len(), Some(&two_threads)) >= 2);
            for pool in [None, Some(&two_threads)] {
                let (sorted, buf) = resident_on(&input, &sort_by, Mode::Auto, pool);
                assert_eq!(
                    buf.abbreviation_abort(),
                    None,
                    "pooled: {}: the prefix's checkpoints continue",
                    pool.is_some()
                );
                assert_eq!(sorted, expected);
            }
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
        /// run the abbreviation cannot order is sorted once. A pooled sort fixes
        /// the runs of each chunk on its own, so a run spread over two chunks
        /// is examined, and sorted, once in each.
        #[test]
        fn tie_runs_already_in_order_are_not_sorted_again() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let two_threads = pool_of(2);

            let repeated = scrambled(5_000)
                .map(|i| Value::Integer((i % 16) as i64))
                .collect();
            let input = rows(&schema, repeated);
            let expected = oracle(&input, &key_alone);
            for pool in [None, Some(&two_threads)] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected, "pooled: {}", pool.is_some());
                let chunks = chunks_on(input.len(), pool);
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    expected_tie_runs(&input, &key_alone, chunks),
                    "{chunks} chunks: one run per key per chunk, all already in order"
                );
                assert_eq!(buf.tie_runs_sorted(), 0);
            }
            assert_eq!(
                expected_tie_runs(&input, &key_alone, 1),
                (16, 0),
                "sequentially, sixteen runs of one key each"
            );

            let input = rows(&schema, prefixed_strings("a-twenty-byte-prefix", 3_000));
            let expected = oracle(&input, &key_alone);
            assert_eq!(
                chunks_on(input.len(), Some(&two_threads)),
                2,
                "on two threads the single run spans both chunks"
            );
            for (pool, runs) in [(None, (1, 1)), (Some(&two_threads), (2, 2))] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected);
                assert_eq!(
                    expected_tie_runs(&input, &key_alone, chunks_on(input.len(), pool)),
                    runs
                );
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    runs,
                    "one run the abbreviation cannot order, sorted once per chunk"
                );
            }
        }

        /// Runs end exactly where the abbreviation changes: four groups of
        /// strings whose abbreviations differ only between groups form four
        /// runs, each sorted on its own; a pooled sort forms them in each
        /// chunk.
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
            let two_threads = pool_of(2);
            assert_eq!(expected_tie_runs(&input, &key_alone, 1), (4, 4));
            for pool in [None, Some(&two_threads)] {
                let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, pool);
                assert_eq!(sorted, expected, "pooled: {}", pool.is_some());
                let chunks = chunks_on(input.len(), pool);
                assert_eq!(
                    (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                    expected_tie_runs(&input, &key_alone, chunks),
                    "{chunks} chunks: one run per group per chunk"
                );
            }
        }

        /// A pooled sort decides on its prefix on the calling thread before it
        /// encodes the rest in parallel: a key whose abbreviations collide stops
        /// at the first checkpoint with nothing encoded in parallel, and a key
        /// that keeps abbreviating encodes every row after the prefix in
        /// parallel.
        #[test]
        fn pooled_sort_decides_on_its_prefix_before_the_parallel_encode() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let two_threads = pool_of(2);
            let pool = Some(&two_threads);

            let input = rows(&schema, distinct_datetimes(20_000));
            let (sorted, buf) = resident_on(&input, &key_alone, Mode::Auto, pool);
            assert_eq!(buf.abbreviation_abort(), Some(100));
            assert_eq!(buf.rows_encoded_in_parallel(), 0);
            assert_eq!(sorted, oracle(&input, &key_alone));

            let input = rows(&schema, distinct_integers(20_000));
            let (sorted, buf) = resident_on(&input, &key_alone, Mode::Auto, pool);
            assert_eq!(buf.abbreviation_abort(), None);
            assert_eq!(buf.rows_encoded_in_parallel(), 20_000 - POOLED_PREFIX_ROWS);
            assert_eq!(sorted, oracle(&input, &key_alone));
        }

        /// 20,000 rows on one string field: the first 10,000 cycle 100
        /// distinct short strings, whose abbreviations tell them apart; the
        /// last 10,000 are distinct strings sharing a 12-byte prefix, whose
        /// abbreviations all collide. A sequential sort keeps abbreviating
        /// through 6,400 rows and stops by 12,800; only a verdict that sees the
        /// second half stops a pooled one.
        fn skewed_strings() -> Vec<Value> {
            (0..10_000)
                .map(|i| Value::String(format!("k{:03}", i % 100).into()))
                .chain(prefixed_strings("twelve-bytes", 10_000))
                .collect()
        }

        /// Dedicated pools of two, three and four workers, built once for the
        /// property below.
        fn small_pool(threads: usize) -> &'static Arc<rayon::ThreadPool> {
            static POOLS: std::sync::OnceLock<[Arc<rayon::ThreadPool>; 3]> =
                std::sync::OnceLock::new();
            &POOLS.get_or_init(|| [pool_of(2), pool_of(3), pool_of(4)])[threads - 2]
        }

        /// A string drawn from `prefix` plus up to `max_tail` characters of a
        /// three-letter alphabet that includes NUL, so strings tie often and
        /// differ past the eighth byte when the prefix is long.
        fn small_string(
            prefix: &'static str,
            max_tail: usize,
        ) -> impl proptest::strategy::Strategy<Value = Value> {
            use proptest::prelude::*;
            prop::collection::vec(prop::sample::select(vec!['a', 'b', '\0']), 0..=max_tail)
                .prop_map(move |tail| {
                    Value::String(
                        format!("{prefix}{}", tail.into_iter().collect::<String>()).into(),
                    )
                })
        }

        /// Sort values from small pools, so rows tie on a field often and a
        /// later field decides: integers, floats with NaN and signed zeros,
        /// decimals, short strings, strings sharing a 14-byte prefix (up to 40
        /// bytes, NULs included), dates and datetimes either side of 1970, and
        /// nulls.
        fn sort_value() -> impl proptest::strategy::Strategy<Value = Value> {
            use proptest::prelude::*;
            prop_oneof![
                5 => prop_oneof![
                    (-3i64..=3).prop_map(Value::Integer),
                    prop::sample::select(vec![
                        0.0,
                        -0.0,
                        1.5,
                        -2.0,
                        f64::NAN,
                        -f64::NAN,
                        f64::INFINITY
                    ])
                    .prop_map(Value::Float),
                    (-30i64..=30, 0u32..=2)
                        .prop_map(|(m, s)| Value::Decimal(rust_decimal::Decimal::new(m, s))),
                    small_string("", 6),
                    small_string("shared-prefix-", 26),
                    (0u64..4).prop_map(|d| Value::Date(
                        chrono::NaiveDate::from_ymd_opt(1969, 12, 30).unwrap()
                            + chrono::Days::new(d)
                    )),
                    (-2i64..=2, 0u32..3).prop_map(|(years, nanos)| Value::DateTime(
                        chrono::DateTime::from_timestamp(years * 31_536_000, nanos)
                            .unwrap()
                            .naive_utc()
                    )),
                ],
                1 => Just(Value::Null),
            ]
        }

        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]

            /// A sort split into per-thread chunks, each sorted on its own and
            /// then merged, writes exactly the stable sort by the authored
            /// comparator, whatever the keys, their directions and null
            /// placements, the pool's size and whether abbreviation is measured
            /// or forced. Where the whole run fits the sequential prefix, the
            /// pooled sort also stops abbreviating exactly where, and why, a
            /// sequential sort of the same rows does.
            #[test]
            fn chunked_pooled_sort_equals_the_stable_sort(
                values in proptest::collection::vec(
                    (sort_value(), sort_value(), sort_value()),
                    96..=3_000,
                ),
                fields in proptest::collection::vec(
                    (0usize..4, proptest::prelude::any::<bool>(), 0usize..3),
                    1..=3,
                ),
                threads in 2usize..=4,
                forced in proptest::prelude::any::<bool>(),
            ) {
                // Index 3 names a column the schema does not have.
                let names = ["a", "b", "c", "missing"];
                let sort_by: Vec<SortField> = fields
                    .iter()
                    .map(|(name, descending, nulls)| SortField {
                        field: names[*name].to_string(),
                        order: if *descending { SortOrder::Desc } else { SortOrder::Asc },
                        null_order: [None, Some(NullOrder::First), Some(NullOrder::Last)][*nulls],
                    })
                    .collect();
                let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
                    "a".into(),
                    "b".into(),
                    "c".into(),
                ])));
                let input: Vec<(Record, u64)> = values
                    .into_iter()
                    .enumerate()
                    .map(|(i, (a, b, c))| (Record::new(schema.clone(), vec![a, b, c]), i as u64))
                    .collect();
                let mode = if forced { Mode::Forced } else { Mode::Auto };
                let pool = small_pool(threads);
                let chunks = chunks_on(input.len(), Some(pool));
                proptest::prop_assert!((2..=threads).contains(&chunks));

                let expected = oracle(&input, &sort_by);
                let (sorted, pooled) = resident_on(&input, &sort_by, mode, Some(pool));
                proptest::prop_assert_eq!(&sorted, &expected);
                let merged = usize::from(pooled.abbreviation_abort().is_none());
                proptest::prop_assert_eq!(pooled.pooled_merges(), merged);

                if !forced && input.len() <= POOLED_PREFIX_ROWS {
                    let (_, sequential) = resident_on(&input, &sort_by, mode, None);
                    proptest::prop_assert_eq!(
                        pooled.abbreviation_abort(),
                        sequential.abbreviation_abort()
                    );
                }
            }
        }

        /// Each chunk of a pooled sort is already in order under the
        /// comparator when the merge starts, so the merge only joins sorted
        /// pieces; it runs once per run.
        #[test]
        fn pooled_chunks_are_in_order_before_the_merge() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let four_threads = pool_of(4);
            let fixtures: [Vec<Value>; 2] = [
                distinct_integers(20_000),
                scrambled(20_000)
                    .map(|i| Value::Integer((i % 16) as i64))
                    .collect(),
            ];
            for keys in fixtures {
                let input = rows(&schema, keys);
                assert_eq!(chunks_on(input.len(), Some(&four_threads)), 4);
                let (sorted, buf) =
                    resident_on(&input, &key_alone, Mode::Forced, Some(&four_threads));
                assert_eq!(sorted, oracle(&input, &key_alone));
                assert_eq!(buf.chunks_in_order_before_merge(), 4);
                assert_eq!(buf.pooled_merges(), 1);
            }
        }

        /// Rows whose keys are equal but fall on both sides of a chunk boundary
        /// leave the merge in arrival order: every abbreviation here is equal,
        /// so the one tie run spans both chunks of a two-thread sort, and each
        /// chunk sorts its share of it before the merge joins them.
        #[test]
        fn tie_runs_split_by_a_chunk_boundary_keep_arrival_order() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let two_threads = pool_of(2);
            let suffix = |i: u64| i % 50;
            let input = rows(
                &schema,
                scrambled(4_000)
                    .map(|i| Value::String(format!("tiekey{:02}", suffix(i)).into()))
                    .collect(),
            );
            assert_eq!(chunks_on(input.len(), Some(&two_threads)), 2);
            let boundary = chunk_start(input.len(), 2, 1);
            let left: std::collections::BTreeSet<String> = input[..boundary]
                .iter()
                .map(|(record, _)| format!("{:?}", record.get("k")))
                .collect();
            assert!(
                input[boundary..]
                    .iter()
                    .any(|(record, _)| left.contains(&format!("{:?}", record.get("k")))),
                "equal keys sit on both sides of the chunk boundary"
            );
            let (sorted, buf) = resident_on(&input, &key_alone, Mode::Forced, Some(&two_threads));
            assert_eq!(sorted, oracle(&input, &key_alone));
            assert_eq!(
                (buf.tie_runs_examined(), buf.tie_runs_sorted()),
                expected_tie_runs(&input, &key_alone, 2)
            );
            assert_eq!(buf.pooled_merges(), 1);
        }

        /// A pool of one thread sorts as a sort without a pool does: the same
        /// checkpoints and abort row, nothing encoded in parallel, no merge,
        /// the same tie runs and the same output.
        #[test]
        fn a_one_thread_pool_sorts_exactly_as_the_sequential_path() {
            let schema = schema();
            let key_alone = vec![field("k", SortOrder::Asc, NullOrder::Last)];
            let one_thread = pool_of(1);
            let fixtures: [(&str, Vec<Value>); 3] = [
                ("distinct instants", distinct_datetimes(2_000)),
                ("skewed strings", skewed_strings()),
                (
                    "16 distinct integers",
                    scrambled(5_000)
                        .map(|i| Value::Integer((i % 16) as i64))
                        .collect(),
                ),
            ];
            for (name, keys) in fixtures {
                let input = rows(&schema, keys);
                let expected = oracle(&input, &key_alone);
                for mode in [Mode::Forced, Mode::Auto] {
                    let (sequential_out, sequential) = resident_on(&input, &key_alone, mode, None);
                    let (pooled_out, pooled) =
                        resident_on(&input, &key_alone, mode, Some(&one_thread));
                    let report = |buf: &SortBuffer<u64>| {
                        (
                            buf.abbreviation_abort(),
                            buf.rows_encoded_in_parallel(),
                            buf.pooled_merges(),
                            buf.tie_runs_examined(),
                            buf.tie_runs_sorted(),
                        )
                    };
                    assert_eq!(report(&pooled), report(&sequential), "{name}, {mode:?}");
                    assert_eq!(pooled.rows_encoded_in_parallel(), 0, "{name}, {mode:?}");
                    assert_eq!(pooled.pooled_merges(), 0, "{name}, {mode:?}");
                    assert_eq!(sequential_out, expected, "{name}, {mode:?}");
                    assert_eq!(pooled_out, expected, "{name}, {mode:?}");
                    match (name, mode) {
                        (_, Mode::Forced) => assert_eq!(pooled.abbreviation_abort(), None),
                        ("distinct instants", _) => {
                            assert_eq!(pooled.abbreviation_abort(), Some(100));
                        }
                        ("skewed strings", _) => {
                            let at = pooled.abbreviation_abort().expect("the skewed run aborts");
                            assert!(6_400 < at && at <= 12_800, "aborted at {at}");
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
