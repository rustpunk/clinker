//! Build-side primitives for the grace hash join: hash-to-partition
//! assignment, the per-partition distinct-key sketch consulted on an
//! E310 abort, the build-record byte estimate that drives spill-victim
//! selection, and the byte-bounded chunk iterator the BNL fallback
//! feeds its build side through.

use clinker_record::Record;

/// Maximum partition bit width. 12 bits = 4096 partitions; beyond this,
/// per-partition overhead (file handles, postcard headers, hashbrown
/// allocations) outweighs further skew reduction. AsterixDB and DuckDB
/// converge on the same cap.
pub(super) const MAX_HASH_BITS: u8 = 12;

// ──────────────────────────────────────────────────────────────────────────
// HyperLogLog distinct-key sketch
// ──────────────────────────────────────────────────────────────────────────

/// Number of HyperLogLog registers. 64 registers ≈ ±13% nominal error;
/// pairs with the bias-correction constant `HLL_ALPHA_64` to give a
/// usable cardinality estimate at 64 bytes per partition (one byte per
/// register, leading-zero count fits comfortably in a `u8` for any
/// 64-bit hash). Sized for diagnostic-quality reporting in E310, not
/// for query-plan cost estimation.
const HLL_REGISTERS: usize = 64;

/// Number of register-index bits derived from a 64-bit hash. `64 = 2^6`,
/// so the top 6 bits select the register and the remaining 58 bits feed
/// the leading-zero count.
const HLL_INDEX_BITS: u32 = 6;

/// Bias-correction constant for `m = 64`. Flajolet et al. (2007) give a
/// closed form for `m >= 128`; for `m = 64` we use the empirically
/// validated `0.709`, which matches Apache DataSketches and Redis
/// HyperLogLog at the same register count.
const HLL_ALPHA_64: f64 = 0.709;

/// Tiny per-partition HyperLogLog sketch.
///
/// Memory: 64 bytes (one register per slot). Approximates the count of
/// distinct hash values fed in via [`Hll::add`]; consulted by
/// `bnl_fallback` when an E310 abort fires so the operator-facing
/// diagnostic carries an actionable cardinality estimate ("partition 7,
/// ~3.2M distinct keys") instead of a bare OOM.
///
/// The estimate uses the harmonic-mean form with bias correction at
/// small range (`E < 2.5m`); large-range correction is unnecessary
/// because the input domain is bounded by `u32::MAX - 1` records per
/// partition (the build-side cap enforced in `CombineHashTable::build`).
#[derive(Debug, Clone)]
pub(crate) struct Hll {
    registers: [u8; HLL_REGISTERS],
}

impl Hll {
    /// Construct a fresh sketch with all registers zeroed.
    pub(crate) fn new() -> Self {
        Self {
            registers: [0; HLL_REGISTERS],
        }
    }

    /// Feed one 64-bit hash value into the sketch. Cost is one branch
    /// plus a max-update on a single register.
    pub(crate) fn add(&mut self, hash: u64) {
        // Top HLL_INDEX_BITS bits pick the register; leading-zero count
        // of the remaining bits + 1 is the contribution. The +1
        // convention (rho = position of leftmost 1-bit, 1-indexed)
        // matches the Flajolet 2007 paper.
        let idx = (hash >> (64 - HLL_INDEX_BITS)) as usize;
        let w = (hash << HLL_INDEX_BITS) | (1u64 << (HLL_INDEX_BITS - 1));
        let rho = (w.leading_zeros() + 1) as u8;
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    /// Return the approximate distinct-value count.
    ///
    /// Uses the harmonic-mean estimator `E = alpha_m * m^2 / sum(2^-M[j])`,
    /// with the small-range linear-counting correction when more than
    /// half the registers are still zero.
    pub(crate) fn estimate(&self) -> u64 {
        let m = HLL_REGISTERS as f64;
        let mut sum_inv = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum_inv += 2f64.powi(-(r as i32));
            if r == 0 {
                zeros += 1;
            }
        }
        let raw = HLL_ALPHA_64 * m * m / sum_inv;
        // Linear-counting branch: when register vector is sparse, the
        // harmonic-mean estimator is biased high. Switch to the
        // closed-form `m * ln(m / V)` correction (Flajolet 2007 §4),
        // which is unbiased in this regime.
        if raw <= 2.5 * m && zeros > 0 {
            (m * (m / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}

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
