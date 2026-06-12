//! HyperLogLog distinct-count sketch, generalized over register count.

/// HyperLogLog distinct-count sketch with `M` registers.
///
/// `M` must be a power of two; `register_index_bits` (`log2(M)`) of the
/// hash select the register and the remaining bits feed the leading-zero
/// count. The standard error is `1.04 / sqrt(M)`, so the register count
/// trades memory (one byte per register) for accuracy: `M = 64` gives
/// ≈±13% at 64 bytes (the diagnostic instance the grace-hash join carries
/// per partition), while `M = 1024` gives ≈±3.25% at 1 KiB — the
/// planner-grade precision the statistics catalog instantiates.
///
/// Memory: `M` bytes. Approximates the count of distinct hash values fed
/// in via [`Hll::add`]; the estimate uses the harmonic-mean form with the
/// small-range linear-counting correction (Flajolet et al. 2007). Sparse
/// representations and full HLL++ bias tables are deliberately out of
/// scope — the linear-counting branch covers the only low-cardinality
/// regime where the harmonic-mean estimator is materially biased.
#[derive(Debug, Clone)]
pub struct Hll<const M: usize> {
    registers: [u8; M],
}

impl<const M: usize> Default for Hll<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const M: usize> Hll<M> {
    /// Bias-correction constant `alpha_m`.
    ///
    /// Flajolet et al. (2007) give a closed form for `m >= 128`; for
    /// `m = 64` (and the unused smaller widths) the empirically validated
    /// `0.709` matches Apache DataSketches and Redis HyperLogLog at that
    /// register count, where the closed form drifts.
    const fn alpha() -> f64 {
        match M {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            // Flajolet `alpha_m = 0.7213 / (1 + 1.079/m)` for m >= 128.
            _ => 0.7213 / (1.0 + 1.079 / M as f64),
        }
    }

    /// Number of register-index bits derived from a 64-bit hash:
    /// `log2(M)`. The top `index_bits` of the hash select the register;
    /// the remaining `64 - index_bits` feed the leading-zero count.
    const fn index_bits() -> u32 {
        M.trailing_zeros()
    }

    /// Construct a fresh sketch with all registers zeroed.
    ///
    /// # Panics
    ///
    /// Panics at construction when `M` is not a power of two — the
    /// register-index derivation requires `log2(M)` to be exact.
    pub fn new() -> Self {
        assert!(
            M.is_power_of_two(),
            "Hll register count M must be a power of two; got {M}"
        );
        Self { registers: [0; M] }
    }

    /// Feed one 64-bit hash value into the sketch. Cost is one branch
    /// plus a max-update on a single register.
    pub fn add(&mut self, hash: u64) {
        // Top `index_bits` bits pick the register; the leading-zero count
        // of the remaining bits + 1 is the contribution. The +1 convention
        // (rho = position of the leftmost 1-bit, 1-indexed) matches the
        // Flajolet 2007 paper. Setting the sentinel bit at position
        // `index_bits - 1` bounds rho so a hash whose low bits are all
        // zero cannot read past the register width.
        let index_bits = Self::index_bits();
        let idx = (hash >> (64 - index_bits)) as usize;
        let w = (hash << index_bits) | (1u64 << (index_bits - 1));
        let rho = (w.leading_zeros() + 1) as u8;
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    /// Merge another sketch of the same register width into this one by
    /// taking the per-register maximum. Two sketches built over disjoint
    /// input slices merge into the sketch the union would have produced —
    /// the property that lets per-partition or per-Rayon-worker sketches
    /// combine without re-scanning their inputs.
    pub fn merge(&mut self, other: &Self) {
        for (slot, &r) in self.registers.iter_mut().zip(other.registers.iter()) {
            if r > *slot {
                *slot = r;
            }
        }
    }

    /// Return the approximate distinct-value count.
    ///
    /// Uses the harmonic-mean estimator `E = alpha_m * m^2 / sum(2^-M[j])`,
    /// switching to the closed-form linear-counting correction
    /// `m * ln(m / V)` when the register vector is still sparse (`E <= 2.5m`
    /// with at least one empty register), where the harmonic-mean form is
    /// biased high.
    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let mut sum_inv = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum_inv += 2f64.powi(-(r as i32));
            if r == 0 {
                zeros += 1;
            }
        }
        let raw = Self::alpha() * m * m / sum_inv;
        if raw <= 2.5 * m && zeros > 0 {
            (m * (m / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}
