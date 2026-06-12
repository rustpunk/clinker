//! Build-side primitives for the grace hash join: hash-to-partition
//! assignment, the per-partition distinct-key sketch consulted on an
//! E310 abort, the build-record byte estimate that drives spill-victim
//! selection, and the byte-bounded chunk iterator the BNL fallback
//! feeds its build side through.

use clinker_record::Record;

use crate::sketch::Hll;

/// Maximum partition bit width. 12 bits = 4096 partitions; beyond this,
/// per-partition overhead (file handles, postcard headers, hashbrown
/// allocations) outweighs further skew reduction. AsterixDB and DuckDB
/// converge on the same cap.
pub(super) const MAX_HASH_BITS: u8 = 12;

/// Per-partition distinct-key sketch carried by the grace hash join.
///
/// Fixed at 64 registers (≈±13% nominal error, 64 bytes per partition):
/// this is a diagnostic-quality estimate consulted only when an E310
/// abort fires, so the BNL fallback can report "partition 7, ~3.2M
/// distinct keys" instead of a bare OOM. The planner-grade statistics
/// catalog instantiates the same [`Hll`] at ≥1024 registers; the
/// hot-path partition sketch deliberately stays small.
pub(crate) type GraceHll = Hll<64>;

// ──────────────────────────────────────────────────────────────────────────
// PartitionAssigner
// ──────────────────────────────────────────────────────────────────────────

/// Assigns 64-bit hash values to one of `2^hash_bits` partitions using
/// the *upper* bits of the hash. Independence from the lower bits
/// (which hashbrown uses for bucket placement) is the property that
/// lets a partition's hash table survive an `assigner.double()` split
/// without re-bucketing.
///
/// Capped at 12 bits (4096 partitions); larger fan-outs amortize per-
/// partition overhead poorly and trade memory savings for handle and
/// header overhead.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionAssigner {
    hash_bits: u8,
    mask: u64,
    shift: u8,
}

impl PartitionAssigner {
    /// Construct an assigner for `hash_bits` partition bits, clamped to
    /// `[1, MAX_HASH_BITS]`.
    pub(crate) fn new(hash_bits: u8) -> Self {
        let hash_bits = hash_bits.clamp(1, MAX_HASH_BITS);
        let mask = if hash_bits == 64 {
            !0u64
        } else {
            (1u64 << hash_bits) - 1
        };
        let shift = 64 - hash_bits;
        Self {
            hash_bits,
            mask,
            shift,
        }
    }

    /// Map a 64-bit hash to a partition index in `[0, 2^hash_bits)`.
    #[inline]
    pub(crate) fn partition_for(&self, hash: u64) -> u16 {
        ((hash >> self.shift) & self.mask) as u16
    }

    pub(crate) fn num_partitions(&self) -> usize {
        1usize << self.hash_bits
    }

    pub(crate) fn hash_bits(&self) -> u8 {
        self.hash_bits
    }

    /// Returns the assigner with one additional bit, or `None` when at
    /// the 12-bit cap. The doubled assigner refines partition boundaries:
    /// every record that mapped to partition `p` under the parent now
    /// maps to either `2p` or `2p + 1` under the child.
    pub(crate) fn double(&self) -> Option<Self> {
        if self.hash_bits >= MAX_HASH_BITS {
            None
        } else {
            Some(Self::new(self.hash_bits + 1))
        }
    }
}

/// Estimate one record's heap footprint plus header overhead. Used as
/// the unit input to spill-victim selection. Conservative; over-counts
/// favor earlier spills over budget overshoots.
pub(super) fn estimated_record_bytes(record: &Record) -> usize {
    record.estimated_heap_size() + std::mem::size_of::<Record>()
}

/// Iterator over a build-record buffer that yields chunks bounded by
/// estimated heap bytes. Emits at least one record per non-empty
/// remainder so a chunk budget smaller than a single record's footprint
/// still terminates rather than spinning. Records are moved out of the
/// underlying Vec; the iterator drains its source.
pub(crate) struct BuildChunkIter {
    source: std::vec::IntoIter<Record>,
    pending: Option<Record>,
    byte_budget: usize,
}

impl BuildChunkIter {
    /// Construct an iterator over `records`. `byte_budget` is the
    /// maximum estimated heap footprint per emitted chunk. The
    /// constructor enforces a `byte_budget >= 1` floor so the iterator
    /// always makes forward progress.
    pub(crate) fn new(records: Vec<Record>, byte_budget: usize) -> Self {
        Self {
            source: records.into_iter(),
            pending: None,
            byte_budget: byte_budget.max(1),
        }
    }
}

impl Iterator for BuildChunkIter {
    type Item = Vec<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out: Vec<Record> = Vec::new();
        let mut accumulated: usize = 0;
        if let Some(r) = self.pending.take() {
            accumulated += estimated_record_bytes(&r);
            out.push(r);
        }
        for r in self.source.by_ref() {
            let cost = estimated_record_bytes(&r);
            if !out.is_empty() && accumulated + cost > self.byte_budget {
                // Stash this record for the next chunk so the size cap
                // bites. The single-record-per-chunk degenerate case is
                // covered by the empty-out guard.
                self.pending = Some(r);
                break;
            }
            accumulated += cost;
            out.push(r);
        }
        if out.is_empty() { None } else { Some(out) }
    }
}
