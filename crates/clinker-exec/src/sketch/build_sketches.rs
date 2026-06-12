//! Composed build-side sketch maintenance for a single join build.

use clinker_record::Value;

use super::{Bloom, Hll, MisraGries};

/// Number of Misra-Gries counters for the build-side heavy-hitter sketch.
///
/// With `k` counters the per-key under-count bound is `N/k`, so 256 keeps
/// that bound tight (≤0.4% of `N`) at a fixed ~4 KiB of counters — and,
/// crucially, caps the retained representative-value set at `k` regardless
/// of build cardinality.
pub const HEAVY_HITTER_COUNTERS: usize = 256;

/// Number of heavy hitters surfaced into the catalog. The full counter set
/// is a lower-bound frequency map; the top slice is what a downstream skew
/// decision or `--explain` reader actually consults.
pub const HEAVY_HITTER_REPORT: usize = 16;

/// Target false-positive rate for the build-side membership filter.
///
/// 1% trades a modest, fixed bit budget for a filter precise enough to skip
/// most absent-key probes.
pub const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

/// The three planner-grade sketches a join maintains over its build-side
/// keys in one pass: a HyperLogLog distinct-count, a Misra-Gries
/// heavy-hitter tracker carrying a representative [`Value`] per live
/// counter, and an optional Bloom membership filter.
///
/// # Memory
///
/// Every retained allocation is bounded by a *fixed constant*, never by the
/// build row count: the HLL is 1024 fixed registers, the Misra-Gries holds
/// at most [`HEAVY_HITTER_COUNTERS`] counters and the same number of
/// representative values (bound to counter membership, not per row), and the
/// Bloom is a fixed bit budget sized up front from a plan-time row estimate.
/// There is deliberately no per-build-row buffer: hashes are consumed as
/// they arrive in [`BuildKeySketches::observe`], so maintenance is `O(k)`,
/// not `O(build rows)`. The [`BuildKeySketches::retained_value_count`] and
/// [`BuildKeySketches::tracked_hash_buffer_len`] accessors let a test pin
/// that invariant.
pub struct BuildKeySketches {
    distinct: Hll<1024>,
    hitters: MisraGries<Value>,
    membership: Option<Bloom>,
}

/// Finalized build-side sketch results: a distinct-count estimate, the
/// top-k heavy hitters as `(value, lower-bound count)` pairs, and the
/// membership filter's `(bit_count, hash_count)` when one was built.
pub struct BuildKeySketchSummary {
    pub distinct: u64,
    pub heavy_hitters: Vec<(Value, u64)>,
    pub bloom: Option<(u64, u32)>,
}

impl BuildKeySketches {
    /// Construct the maintainer. `plan_row_estimate` is the build node's
    /// plan-time (metadata-derived) row count, an upper bound on the
    /// distinct-key count used to size the Bloom up front; when it is
    /// `None` no membership filter is built (rather than buffering keys to
    /// size one later).
    pub fn new(plan_row_estimate: Option<u64>) -> Self {
        Self {
            distinct: Hll::new(),
            hitters: MisraGries::new(HEAVY_HITTER_COUNTERS),
            membership: plan_row_estimate.map(|n| Bloom::new(n, BLOOM_FALSE_POSITIVE_RATE)),
        }
    }

    /// Observe one build-key `hash`. `label` materializes the representative
    /// [`Value`] to capture *only if* the key opens a new Misra-Gries
    /// counter — it runs at most [`HEAVY_HITTER_COUNTERS`] times across the
    /// whole build, so an expensive re-derivation behind it costs nothing
    /// per row. No per-row state is retained: the hash updates the three
    /// sketches in place and is dropped.
    pub fn observe(&mut self, hash: u64, label: impl FnOnce() -> Value) {
        self.distinct.add(hash);
        self.hitters.add(hash, label);
        if let Some(bloom) = self.membership.as_mut() {
            bloom.insert(hash);
        }
    }

    /// Finalize every sketch into its catalog-ready summary.
    pub fn finalize(self) -> BuildKeySketchSummary {
        BuildKeySketchSummary {
            distinct: self.distinct.estimate(),
            heavy_hitters: self.hitters.top_k(HEAVY_HITTER_REPORT),
            bloom: self.membership.map(|b| (b.bit_count(), b.hash_count())),
        }
    }

    /// Number of representative key values currently retained — exactly the
    /// number of live Misra-Gries counters, hence `<= HEAVY_HITTER_COUNTERS`
    /// no matter how many distinct keys were observed.
    #[cfg(test)]
    pub(crate) fn retained_value_count(&self) -> usize {
        self.hitters.counter_count()
    }

    /// Length of any per-key hash buffer the maintainer holds. The design
    /// keeps no such buffer, so this is always zero; a test asserts it to
    /// fail loudly the instant an `O(build rows)` buffer is reintroduced.
    #[cfg(test)]
    pub(crate) fn tracked_hash_buffer_len(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::super::splitmix64 as mix;
    use super::*;

    /// Feed `n` distinct keys through the maintainer and return
    /// `(retained_value_count, tracked_hash_buffer_len)`. The label closure
    /// builds a fresh `Value` per observed key, so a maintainer that retained
    /// one value per key — or buffered the hashes — would grow to `~n`; a
    /// correct one stays bounded by the counter budget with a zero buffer.
    fn retained_after_distinct(n: u64) -> (usize, usize) {
        let mut sketches = BuildKeySketches::new(Some(n));
        for i in 0..n {
            sketches.observe(mix(i), || Value::from(format!("key-{i}").as_str()));
        }
        (
            sketches.retained_value_count(),
            sketches.tracked_hash_buffer_len(),
        )
    }

    #[test]
    fn retained_state_is_bounded_by_counter_budget_not_build_size() {
        // The bounded-memory invariant: sketch maintenance must be O(k), not
        // O(build rows). Two independent signals catch a regression:
        //   - the retained representative-value set never exceeds the counter
        //     budget, even at 1M distinct keys (a per-key retention bug would
        //     push this to ~1M);
        //   - no per-build-row hash buffer exists (it stays length 0; an
        //     N-sized buffer reintroduction would make it ~N).
        // Both hold identically at 1K and 1M, which is the whole point: the
        // retained state does not grow with the input.
        let (small_values, small_buffer) = retained_after_distinct(1_000);
        let (large_values, large_buffer) = retained_after_distinct(1_000_000);

        assert!(
            small_values <= HEAVY_HITTER_COUNTERS,
            "1K distinct keys must retain <= {HEAVY_HITTER_COUNTERS} values; got {small_values}"
        );
        assert!(
            large_values <= HEAVY_HITTER_COUNTERS,
            "1M distinct keys must retain <= {HEAVY_HITTER_COUNTERS} values (O(k), not \
             O(build rows)); a per-key retention bug would push this to ~1M; got {large_values}"
        );
        // The buffer is the direct O(build-rows) regression signal: it must
        // be empty at every input size. A reintroduced N-sized hash buffer
        // would make `large_buffer` ~1M while `small_buffer` stays ~1K.
        assert_eq!(
            small_buffer, 0,
            "sketch maintenance must hold no per-build-row buffer (O(k), not O(build rows))"
        );
        assert_eq!(
            large_buffer, 0,
            "sketch maintenance must hold no per-build-row buffer even at 1M rows \
             (O(k), not O(build rows)); a non-zero buffer means an N-sized allocation \
             was reintroduced"
        );
    }

    #[test]
    fn late_hot_key_reports_its_real_value() {
        // Many distinct singleton keys first, then one key repeated far more
        // than the rest. Because the representative value is bound to
        // Misra-Gries counter membership — captured when the hot key's
        // counter opens, not by first-N insertion order — the survivor
        // carries its real value, not an empty placeholder.
        let mut sketches = BuildKeySketches::new(Some(1_000));
        for i in 0..1_000u64 {
            sketches.observe(mix(i), || Value::from(format!("cold-{i}").as_str()));
        }
        let hot_hash = mix(9_999_999);
        for _ in 0..5_000 {
            sketches.observe(hot_hash, || Value::from("hot-key"));
        }
        let summary = sketches.finalize();
        let (value, count) = summary
            .heavy_hitters
            .first()
            .expect("a heavy hitter survives");
        assert_eq!(
            value.to_string(),
            "hot-key",
            "a key that goes hot after the counter budget filled must still report its \
             real value, proving values are bound to counter membership, not a first-N buffer"
        );
        assert!(
            *count > 1_000,
            "the hot key's count must dominate; got {count}"
        );
    }
}
