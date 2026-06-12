//! Misra-Gries heavy-hitter sketch over hashed keys.

use std::collections::HashMap;

/// One live counter: a frequency plus a representative label captured when
/// the counter was opened, so a surviving heavy hitter can be reported with
/// the value an author would recognize rather than a bare hash.
#[derive(Debug, Clone)]
struct Counter<L> {
    count: u64,
    label: L,
}

/// Misra-Gries frequency sketch tracking up to `k` heavy hitters over a
/// stream of hashed keys, carrying a representative label `L` per live
/// counter.
///
/// Maintains at most `k` counters. A key already counted increments its
/// counter; a new key with spare capacity opens a counter at one, capturing
/// the label supplied with it; otherwise every counter is decremented and
/// any that reaches zero is evicted (dropping its label). After `add`-ing
/// `N` keys, a counter's value `c` for key `i` satisfies
/// `f_i - N/k <= c <= f_i` — the sketch only ever **under-counts**. The
/// heavy-hitter list is therefore a lower bound on frequency and must never
/// be used to *exclude* a key (a key absent from the list may still be
/// frequent); it is safe only to *promote* a key the sketch reports.
///
/// Memory: O(k) counters and O(k) labels — bounded by the counter budget,
/// not the input size, so a high-cardinality stream costs no more than a
/// hot one. Because the label is bound to counter membership, every
/// surviving counter carries its true label; a key that becomes hot only
/// late still captures its label the moment its counter opens.
///
/// `merge` combines two sketches counter-wise then re-prunes to `k`, so
/// per-partition or per-Rayon-worker sketches combine without re-scanning
/// their inputs. Equal hashes denote the same key, so a merge keeps the
/// existing counter's label.
#[derive(Debug, Clone)]
pub struct MisraGries<L> {
    k: usize,
    counters: HashMap<u64, Counter<L>>,
    total: u64,
}

impl<L: Clone> MisraGries<L> {
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

    /// Feed one hashed key into the sketch. `label` produces the
    /// representative value to capture *only if* this key opens a new
    /// counter — it is invoked at most `k` times across the whole stream
    /// (once per counter opened), so a caller can put an expensive
    /// materialization (re-deriving a key value) behind it without paying
    /// that cost per row.
    pub fn add(&mut self, key: u64, label: impl FnOnce() -> L) {
        self.total += 1;
        if let Some(counter) = self.counters.get_mut(&key) {
            counter.count += 1;
        } else if self.counters.len() < self.k {
            self.counters.insert(
                key,
                Counter {
                    count: 1,
                    label: label(),
                },
            );
        } else {
            // Decrement-all: every tracked key loses one count; the
            // incoming key is implicitly cancelled against the decrements.
            self.counters.retain(|_, counter| {
                counter.count -= 1;
                counter.count > 0
            });
        }
    }

    /// Total number of keys fed in (`N`), independent of how many survive
    /// as counters. Pairs with `k` to bound the per-key error at `N/k`.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Number of live counters — at most `k`, and therefore the number of
    /// representative labels retained. Bounded by the counter budget
    /// regardless of how many distinct keys were fed in. Exists for the
    /// bounded-memory invariant probe in tests.
    #[cfg(test)]
    pub(crate) fn counter_count(&self) -> usize {
        self.counters.len()
    }

    /// Per-key error bound `N/k`: the maximum amount by which any reported
    /// estimate can under-count the key's true frequency.
    pub fn error_bound(&self) -> u64 {
        self.total / self.k as u64
    }

    /// Merge another sketch into this one. Counters are summed key-wise
    /// (keeping the existing label, since equal hashes denote the same key),
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
        for (&key, counter) in &other.counters {
            self.counters
                .entry(key)
                .and_modify(|existing| existing.count += counter.count)
                .or_insert_with(|| counter.clone());
        }
        if self.counters.len() <= self.k {
            return;
        }
        // Prune to k: the `(k+1)`-th largest count is the floor every
        // counter is reduced by, mirroring a batch of decrement-all steps.
        let mut counts: Vec<u64> = self.counters.values().map(|c| c.count).collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let floor = counts[self.k];
        self.counters.retain(|_, counter| {
            counter.count = counter.count.saturating_sub(floor);
            counter.count > 0
        });
    }

    /// Return up to `n` surviving heavy hitters as `(label, estimate)` pairs
    /// in descending estimate order. Each estimate is a lower bound on the
    /// key's true frequency (`>= f_i - N/k`); each label is the
    /// representative captured when the key's counter opened.
    pub fn top_k(&self, n: usize) -> Vec<(L, u64)> {
        // Sort by descending count, breaking ties by key hash so the order
        // is deterministic across runs and hash-map iteration orders.
        let mut entries: Vec<(&u64, &Counter<L>)> = self.counters.iter().collect();
        entries.sort_unstable_by(|a, b| b.1.count.cmp(&a.1.count).then(a.0.cmp(b.0)));
        entries.truncate(n);
        entries
            .into_iter()
            .map(|(_, counter)| (counter.label.clone(), counter.count))
            .collect()
    }
}
