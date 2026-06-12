//! Bloom membership sketch with Kirsch-Mitzenmacher double hashing.

/// Bloom filter answering approximate set membership over hashed keys.
///
/// Sized from an expected element count `n` and a target false-positive
/// rate `p`: `m = ceil(-n * ln(p) / (ln 2)^2)` bits and
/// `k = round((m/n) * ln 2)` hash probes. The `k` bit indices for one key
/// are derived from a single 64-bit hash split into two 32-bit halves
/// `(h1, h2)` via Kirsch-Mitzenmacher double hashing
/// (`g_i = h1 + i*h2 mod m`), so no second hash function is needed.
///
/// A Bloom filter never reports a false negative: a key that was inserted
/// always tests present. It may report a false positive at approximately
/// the configured rate `p`, so [`Bloom::contains`] answers "possibly
/// present" / "definitely absent".
///
/// Memory: `ceil(m / 8)` bytes. When `n` was itself an estimate (e.g. an
/// HLL distinct-count rather than an exact row count), the caller records
/// that provenance separately — the filter sizing is only as good as `n`.
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u64>,
    /// Bit count `m` (not word count); indices are taken mod `m`.
    m: u64,
    k: u32,
}

impl Bloom {
    /// Construct a filter sized for `expected_items` elements at target
    /// false-positive rate `false_positive_rate`.
    ///
    /// # Panics
    ///
    /// Panics when `false_positive_rate` is not in the open interval
    /// `(0, 1)` — a rate of zero needs infinite bits and a rate of one
    /// needs no filter.
    pub fn new(expected_items: u64, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "Bloom false-positive rate must be in (0, 1); got {false_positive_rate}"
        );
        // A zero-element filter still needs a non-empty bit array so the
        // index math is total; one word (64 bits) is the floor.
        let n = expected_items.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let m = (-n * false_positive_rate.ln() / (ln2 * ln2)).ceil() as u64;
        let m = m.max(64);
        let k = (((m as f64 / n) * ln2).round() as u32).max(1);
        let words = m.div_ceil(64) as usize;
        Self {
            bits: vec![0u64; words],
            m,
            k,
        }
    }

    /// Split a 64-bit hash into the `(h1, h2)` base pair for double
    /// hashing. `h2` is forced odd so `i*h2` cannot collapse to a single
    /// residue class when `m` shares factors with `h2`.
    #[inline]
    fn base_hashes(hash: u64) -> (u64, u64) {
        let h1 = hash & 0xFFFF_FFFF;
        let h2 = (hash >> 32) | 1;
        (h1, h2)
    }

    /// Insert a hashed key.
    pub fn insert(&mut self, hash: u64) {
        let (h1, h2) = Self::base_hashes(hash);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            self.bits[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }

    /// Test membership. Returns `false` only when the key was definitely
    /// never inserted; `true` means "possibly present" (subject to the
    /// configured false-positive rate).
    pub fn contains(&self, hash: u64) -> bool {
        let (h1, h2) = Self::base_hashes(hash);
        (0..self.k as u64).all(|i| {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            self.bits[(bit / 64) as usize] & (1u64 << (bit % 64)) != 0
        })
    }

    /// Number of bits `m` in the filter.
    pub fn bit_count(&self) -> u64 {
        self.m
    }

    /// Number of hash probes `k` per key.
    pub fn hash_count(&self) -> u32 {
        self.k
    }
}
