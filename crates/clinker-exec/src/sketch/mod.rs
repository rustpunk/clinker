//! Hand-rolled probabilistic sketches shared across the engine: a
//! HyperLogLog distinct-count estimator, a Misra-Gries heavy-hitter
//! tracker, and a Bloom membership filter.
//!
//! These are the exec-time maintainers behind the planner's statistics
//! catalog. They live here, in the execution crate, because the only
//! component that populates them while records flow — the grace-hash join,
//! which already carries a per-partition HLL — is here, and the planner
//! crate cannot depend on the execution crate. The catalog reads back the
//! *finalized* summaries these sketches produce.
//!
//! Every sketch is allocation-bounded and `merge`-able so per-partition or
//! per-Rayon-worker instances combine into the result a single-threaded
//! pass would have produced.

mod bloom;
mod hll;
mod misra_gries;

pub use bloom::Bloom;
pub use hll::Hll;
pub use misra_gries::MisraGries;

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::RandomState;

    /// Mix a sequential index into a well-distributed 64-bit hash via the
    /// SplitMix64 finalizer. The sketches assume near-uniform input bits;
    /// SplitMix64 passes BigCrush, so feeding it `0..n` gives the HLL
    /// register-index and leading-zero distributions their accuracy bounds
    /// are derived under — a weaker mixer leaves residual structure in the
    /// high bits and inflates the apparent error.
    fn mix(i: u64) -> u64 {
        let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Assert an HLL of register width `M` estimates `n` distinct hashes
    /// within three standard errors (`3 * 1.04/sqrt(M)`). Three sigma
    /// keeps the bound tight enough to catch a broken estimator while
    /// tolerating the sketch's inherent variance.
    fn assert_hll_band<const M: usize>(n: u64) {
        let mut sketch = Hll::<M>::new();
        for i in 0..n {
            sketch.add(mix(i));
        }
        let est = sketch.estimate() as f64;
        let std_err = 1.04 / (M as f64).sqrt();
        let rel_err = (est - n as f64).abs() / n as f64;
        assert!(
            rel_err <= 3.0 * std_err,
            "Hll<{M}> estimate {est} for {n} distinct: relative error {rel_err:.4} \
             exceeds 3 sigma ({:.4})",
            3.0 * std_err
        );
    }

    #[test]
    fn hll_accuracy_band_across_register_widths() {
        for &n in &[100u64, 10_000, 1_000_000] {
            assert_hll_band::<64>(n);
            assert_hll_band::<1024>(n);
            assert_hll_band::<4096>(n);
        }
    }

    #[test]
    fn hll_merge_matches_single_pass_union() {
        // Two disjoint halves merged equal a single sketch over the union.
        let mut a = Hll::<1024>::new();
        let mut b = Hll::<1024>::new();
        let mut whole = Hll::<1024>::new();
        for i in 0..50_000u64 {
            let h = mix(i);
            whole.add(h);
            if i % 2 == 0 { a.add(h) } else { b.add(h) }
        }
        a.merge(&b);
        assert_eq!(
            a.estimate(),
            whole.estimate(),
            "merged disjoint halves must match the single-pass union estimate"
        );
    }

    #[test]
    fn hll_empty_is_zero() {
        assert_eq!(Hll::<1024>::new().estimate(), 0);
    }

    #[test]
    fn misra_gries_estimate_within_error_bound() {
        // A few hot keys plus a long uniform tail. Every reported estimate
        // must under-count by at most N/k and never over-count.
        let k = 16;
        let mut mg = MisraGries::new(k);
        let mut truth: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        // Hot keys 0..4 each appear 2000 times.
        for hot in 0..4u64 {
            for _ in 0..2000 {
                mg.add(hot);
                *truth.entry(hot).or_default() += 1;
            }
        }
        // Cold tail: 5000 distinct keys once each.
        for cold in 100..5100u64 {
            mg.add(cold);
            *truth.entry(cold).or_default() += 1;
        }
        let bound = mg.error_bound();
        for (key, est) in mg.top_k(k) {
            let actual = truth[&key];
            assert!(
                est <= actual,
                "MisraGries must never over-count: {est} > {actual}"
            );
            assert!(
                actual - est <= bound,
                "MisraGries under-count {actual}-{est} exceeds N/k bound {bound}"
            );
        }
        // The four genuine hot keys must all survive — their frequency far
        // exceeds the N/k error floor.
        let survivors: std::collections::HashSet<u64> =
            mg.top_k(k).into_iter().map(|(k, _)| k).collect();
        for hot in 0..4u64 {
            assert!(
                survivors.contains(&hot),
                "hot key {hot} must survive in the sketch"
            );
        }
    }

    #[test]
    fn misra_gries_merge_preserves_bound() {
        let k = 8;
        let mut left = MisraGries::new(k);
        let mut right = MisraGries::new(k);
        let mut truth: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        for i in 0..10_000u64 {
            let key = if i % 3 == 0 { 7 } else { 100 + (i % 900) };
            if i % 2 == 0 {
                left.add(key)
            } else {
                right.add(key)
            }
            *truth.entry(key).or_default() += 1;
        }
        left.merge(&right);
        let bound = left.error_bound();
        for (key, est) in left.top_k(k) {
            let actual = truth[&key];
            assert!(est <= actual, "merged estimate over-counts");
            assert!(
                actual - est <= bound,
                "merged under-count exceeds bound {bound}"
            );
        }
    }

    #[test]
    fn bloom_never_false_negatives_and_bounded_false_positives() {
        let p = 0.01;
        let n = 10_000u64;
        let mut bloom = Bloom::new(n, p);
        let hasher = RandomState::with_seeds(1, 2, 3, 4);
        // Insert n members.
        for i in 0..n {
            bloom.insert(hasher.hash_one(i));
        }
        // No false negatives: every member tests present.
        for i in 0..n {
            assert!(
                bloom.contains(hasher.hash_one(i)),
                "Bloom reported a false negative for member {i}"
            );
        }
        // False-positive rate over a disjoint probe set stays within
        // 1.5x of the configured target (slack for finite-sample noise).
        let probes = 10_000u64;
        let mut false_positives = 0u64;
        for i in n..(n + probes) {
            if bloom.contains(hasher.hash_one(i)) {
                false_positives += 1;
            }
        }
        let observed = false_positives as f64 / probes as f64;
        assert!(
            observed <= 1.5 * p,
            "Bloom observed false-positive rate {observed:.4} exceeds 1.5x target {p}"
        );
    }
}
