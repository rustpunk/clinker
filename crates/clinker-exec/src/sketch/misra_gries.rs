//! Misra-Gries heavy-hitter sketch over hashed keys.

use std::collections::HashMap;

/// Misra-Gries frequency sketch tracking up to `k` heavy hitters over a
/// stream of hashed keys.
///
/// Maintains at most `k` counters. A key already counted increments its
/// counter; a new key with spare capacity opens a counter at one;
/// otherwise every counter is decremented and any that reaches zero is
/// evicted. After `add`-ing `N` keys, a counter's value `c` for key `i`
/// satisfies `f_i - N/k <= c <= f_i` — the sketch only ever
/// **under-counts**. The heavy-hitter list is therefore a lower bound on
/// frequency and must never be used to *exclude* a key (a key absent from
/// the list may still be frequent); it is safe only to *promote* a key
/// the sketch reports.
///
/// Memory: O(k) entries. `merge` combines two sketches counter-wise then
/// re-prunes to `k`, so per-partition or per-Rayon-worker sketches combine
/// without re-scanning their inputs.
#[derive(Debug, Clone)]
pub struct MisraGries {
    k: usize,
    counters: HashMap<u64, u64>,
    total: u64,
}

impl MisraGries {
    /// Construct a sketch bounded to `k` counters.
    ///
    /// # Panics
    ///
    /// Panics when `k` is zero — a zero-capacity sketch can track no key.
    pub fn new(k: usize) -> Self {
        assert!(k > 0, "MisraGries capacity k must be non-zero");
        Self {
            k,
            counters: HashMap::with_capacity(k),
            total: 0,
        }
    }

    /// Feed one hashed key into the sketch.
    pub fn add(&mut self, key: u64) {
        self.total += 1;
        if let Some(c) = self.counters.get_mut(&key) {
            *c += 1;
        } else if self.counters.len() < self.k {
            self.counters.insert(key, 1);
        } else {
            // Decrement-all: every tracked key loses one count; the
            // incoming key is implicitly cancelled against the decrements.
            self.counters.retain(|_, c| {
                *c -= 1;
                *c > 0
            });
        }
    }

    /// Total number of keys fed in (`N`), independent of how many survive
    /// as counters. Pairs with `k` to bound the per-key error at `N/k`.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Per-key error bound `N/k`: the maximum amount by which any reported
    /// estimate can under-count the key's true frequency.
    pub fn error_bound(&self) -> u64 {
        self.total / self.k as u64
    }

    /// Merge another sketch into this one. Counters are summed key-wise,
    /// then the combined set is pruned back to `k` survivors by the
    /// Misra-Gries decrement rule (subtract the `(k+1)`-th largest count
    /// from all and drop non-positives), preserving the `f_i - N/k` bound
    /// across the union.
    ///
    /// # Panics
    ///
    /// Panics when the two sketches were built with different `k` — a
    /// merge across capacities has no well-defined error bound.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.k, other.k,
            "MisraGries merge requires matching capacity k"
        );
        self.total += other.total;
        for (&key, &c) in &other.counters {
            *self.counters.entry(key).or_insert(0) += c;
        }
        if self.counters.len() <= self.k {
            return;
        }
        // Prune to k: the `(k+1)`-th largest count is the floor every
        // counter is reduced by, mirroring a batch of decrement-all steps.
        let mut counts: Vec<u64> = self.counters.values().copied().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let floor = counts[self.k];
        self.counters.retain(|_, c| {
            *c = c.saturating_sub(floor);
            *c > 0
        });
    }

    /// Return up to `n` surviving heavy hitters as `(key_hash, estimate)`
    /// pairs in descending estimate order. Each estimate is a lower bound
    /// on the key's true frequency (`>= f_i - N/k`).
    pub fn top_k(&self, n: usize) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> =
            self.counters.iter().map(|(&key, &c)| (key, c)).collect();
        // Sort by descending count, breaking ties by key hash so the order
        // is deterministic across runs and hash-map iteration orders.
        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        entries.truncate(n);
        entries
    }
}
