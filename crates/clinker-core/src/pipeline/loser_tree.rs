//! K-way merge via loser tree. O(log k) comparisons per output record.
//!
//! Flat `Vec<usize>` array with DataFusion-style indexing for any k
//! (no power-of-2 padding required). Exhausted streams use `None` as
//! an infinity sentinel. Stable merge via stream-index tiebreaker.
//!
//! Reference: DataFusion PR #4301 (50-72% faster than BinaryHeap).

use std::cmp::Ordering;

use clinker_record::Record;

/// A single entry in the k-way merge: pre-encoded sort key + full record.
pub struct MergeEntry {
    pub key: Vec<u8>,
    pub record: Record,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// K-way merge loser tree.
///
/// After initialization, `winner()` returns the smallest element.
/// Call `replace_winner(next)` to advance: supply the next element
/// from the winning stream (or `None` if exhausted), and the tree
/// replays to find the new winner.
pub struct LoserTree {
    /// Internal nodes. `tree[0]` holds the overall winner index.
    /// `tree[1..k]` hold loser indices at each internal node.
    tree: Vec<usize>,
    /// One cursor per input stream. `None` = exhausted (infinity sentinel).
    cursors: Vec<Option<MergeEntry>>,
}

impl LoserTree {
    /// Create a new loser tree from initial entries (one per stream).
    pub fn new(initial_entries: Vec<Option<MergeEntry>>) -> Self {
        let k = initial_entries.len();
        let mut lt = LoserTree {
            tree: vec![usize::MAX; k],
            cursors: initial_entries,
        };
        lt.init_loser_tree();
        lt
    }

    /// The current winner entry, or `None` if all streams are exhausted.
    pub fn winner(&self) -> Option<&MergeEntry> {
        if self.cursors.is_empty() {
            return None;
        }
        self.cursors[self.tree[0]].as_ref()
    }

    /// The index of the current winning stream.
    pub fn winner_index(&self) -> usize {
        self.tree[0]
    }

    /// Replace the winner's entry with the next element from its stream
    /// (or `None` if exhausted), then replay the tree to find the new winner.
    pub fn replace_winner(&mut self, entry: Option<MergeEntry>) {
        let winner_idx = self.tree[0];
        self.cursors[winner_idx] = entry;
        self.update_loser_tree();
    }

    fn init_loser_tree(&mut self) {
        if self.cursors.is_empty() {
            return;
        }
        for i in 0..self.cursors.len() {
            let mut winner = i;
            let mut cmp_node = self.leaf_index(i);
            while cmp_node != 0 && self.tree[cmp_node] != usize::MAX {
                let challenger = self.tree[cmp_node];
                if self.is_gt(winner, challenger) {
                    self.tree[cmp_node] = winner;
                    winner = challenger;
                }
                cmp_node = self.parent_index(cmp_node);
            }
            self.tree[cmp_node] = winner;
        }
    }

    fn update_loser_tree(&mut self) {
        let mut winner = self.tree[0];
        let mut cmp_node = self.leaf_index(winner);
        while cmp_node != 0 {
            let challenger = self.tree[cmp_node];
            if self.is_gt(winner, challenger) {
                self.tree[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = self.parent_index(cmp_node);
        }
        self.tree[0] = winner;
    }

    /// Returns true if stream `a` should lose to stream `b`.
    /// Exhausted streams (None) always lose. Ties broken by stream index
    /// (lower index wins) for stable merge.
    fn is_gt(&self, a: usize, b: usize) -> bool {
        match (&self.cursors[a], &self.cursors[b]) {
            (None, _) => true,
            (_, None) => false,
            (Some(ac), Some(bc)) => ac.cmp(bc).then_with(|| a.cmp(&b)).is_gt(),
        }
    }

    fn leaf_index(&self, cursor_index: usize) -> usize {
        (self.cursors.len() + cursor_index) / 2
    }

    fn parent_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: u8) -> Option<MergeEntry> {
        Some(MergeEntry {
            key: vec![key],
            record: {
                use clinker_record::{Record, Schema, Value};
                use std::sync::Arc;
                let schema = Arc::new(Schema::new(vec!["v".into()]));
                Record::new(schema, vec![Value::Integer(key as i64)])
            },
        })
    }

    #[test]
    fn test_loser_tree_init_2_streams() {
        let lt = LoserTree::new(vec![entry(5), entry(3)]);
        assert_eq!(lt.winner().unwrap().key, vec![3]);
        assert_eq!(lt.winner_index(), 1);
    }

    #[test]
    fn test_loser_tree_init_16_streams() {
        let entries: Vec<_> = (0..16).map(|i| entry(i)).collect();
        let lt = LoserTree::new(entries);
        assert_eq!(lt.winner().unwrap().key, vec![0]);
        assert_eq!(lt.winner_index(), 0);
    }

    #[test]
    fn test_loser_tree_non_power_of_2() {
        let entries = vec![entry(10), entry(5), entry(8), entry(3), entry(7)];
        let lt = LoserTree::new(entries);
        assert_eq!(lt.winner().unwrap().key, vec![3]);
    }

    #[test]
    fn test_loser_tree_replay_advances_winner() {
        let mut lt = LoserTree::new(vec![entry(1), entry(2), entry(3)]);
        assert_eq!(lt.winner().unwrap().key, vec![1]);

        // Replace stream 0's entry with 4
        lt.replace_winner(entry(4));
        assert_eq!(lt.winner().unwrap().key, vec![2]);
        assert_eq!(lt.winner_index(), 1);
    }

    #[test]
    fn test_loser_tree_exhausted_stream_loses() {
        let mut lt = LoserTree::new(vec![entry(1), entry(2)]);
        assert_eq!(lt.winner().unwrap().key, vec![1]);

        // Exhaust stream 0
        lt.replace_winner(None);
        assert_eq!(lt.winner().unwrap().key, vec![2]);
        assert_eq!(lt.winner_index(), 1);
    }

    #[test]
    fn test_loser_tree_stable_merge_tiebreak() {
        // Equal keys from streams 0 and 3 — stream 0 should win (lower index)
        let entries = vec![entry(5), entry(10), entry(10), entry(5)];
        let lt = LoserTree::new(entries);
        assert_eq!(lt.winner().unwrap().key, vec![5]);
        assert_eq!(lt.winner_index(), 0); // stream 0 wins over stream 3
    }

    #[test]
    fn test_loser_tree_all_exhausted() {
        let mut lt = LoserTree::new(vec![entry(1), entry(2)]);
        lt.replace_winner(None); // exhaust stream 0
        lt.replace_winner(None); // exhaust stream 1
        assert!(lt.winner().is_none());
    }

    #[test]
    fn test_loser_tree_merge_entry_ord() {
        let a = MergeEntry {
            key: vec![1, 2, 3],
            record: {
                use clinker_record::{Record, Schema, Value};
                use std::sync::Arc;
                let s = Arc::new(Schema::new(vec!["x".into()]));
                Record::new(s, vec![Value::Integer(999)]) // record content irrelevant
            },
        };
        let b = MergeEntry {
            key: vec![1, 2, 4],
            record: {
                use clinker_record::{Record, Schema, Value};
                use std::sync::Arc;
                let s = Arc::new(Schema::new(vec!["x".into()]));
                Record::new(s, vec![Value::Integer(1)]) // different record, but key matters
            },
        };
        assert!(a < b); // comparison uses key bytes, not record
    }

    #[test]
    fn test_loser_tree_single_stream() {
        let mut lt = LoserTree::new(vec![entry(42)]);
        assert_eq!(lt.winner().unwrap().key, vec![42]);
        lt.replace_winner(entry(43));
        assert_eq!(lt.winner().unwrap().key, vec![43]);
        lt.replace_winner(None);
        assert!(lt.winner().is_none());
    }

    #[test]
    fn test_loser_tree_full_merge_sequence() {
        // Merge 3 sorted streams: [1,4,7], [2,5,8], [3,6,9]
        let mut lt = LoserTree::new(vec![entry(1), entry(2), entry(3)]);
        let streams: Vec<Vec<u8>> = vec![vec![4, 7], vec![5, 8], vec![6, 9]];
        let mut iters: Vec<std::vec::IntoIter<u8>> =
            streams.into_iter().map(|s| s.into_iter()).collect();

        let mut result = Vec::new();
        while lt.winner().is_some() {
            let idx = lt.winner_index();
            result.push(lt.winner().unwrap().key[0]);
            let next = iters[idx].next().map(|k| MergeEntry {
                key: vec![k],
                record: {
                    use clinker_record::{Record, Schema, Value};
                    use std::sync::Arc;
                    let s = Arc::new(Schema::new(vec!["v".into()]));
                    Record::new(s, vec![Value::Integer(k as i64)])
                },
            });
            lt.replace_winner(next);
        }
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
