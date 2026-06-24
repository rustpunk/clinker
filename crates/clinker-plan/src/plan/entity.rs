#![forbid(unsafe_code)]
//! Dense entity-id storage: typed `u32` newtype keys plus the two map
//! shapes that key off them.
//!
//! This mirrors the `cranelift-entity` / rustc `IndexVec` model — a
//! newtype index plus a dense `SecondaryMap` that attaches per-entity
//! side data — while taking no external dependency. Everything here is
//! pure `std`.
//!
//! The whole point is to replace `HashMap<NodeName, T>`-style side tables
//! with `O(1)`-indexed vectors keyed by a stable, dense id. An
//! [`EntityRef`] is just a position; identity is conferred by *where the
//! id was minted*, not by anything stored in the value.

use std::marker::PhantomData;
use std::ops::Index;

/// A typed, dense, `u32`-backed entity index.
///
/// Implementors are the newtype keys minted by [`entity_id!`]. The
/// contract is a bijection with `usize` over the representable range:
/// `EntityRef::new(i).index() == i`. Keys are positions into a backing
/// `Vec`, so they must be `Copy`, totally comparable, and hashable to
/// serve as map keys and set members.
pub trait EntityRef: Copy + Eq + std::hash::Hash {
    /// Build the key for slot `index`.
    fn new(index: usize) -> Self;

    /// The slot this key addresses.
    fn index(self) -> usize;
}

/// Define a dense `u32` entity-id newtype implementing [`EntityRef`].
///
/// Accepts pass-through doc comments and attributes ahead of the struct,
/// so callers document the id at the definition site. The generated type
/// derives the full ordering/hashing/serde stack and renders as
/// `Name(3)` in both `Debug` and `Display`.
macro_rules! entity_id {
    ($(#[$meta:meta])* $vis:vis struct $name:ident;) => {
        $(#[$meta])*
        #[derive(
            Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize,
        )]
        $vis struct $name(u32);

        impl $crate::plan::entity::EntityRef for $name {
            #[inline]
            fn new(index: usize) -> Self {
                debug_assert!(
                    index <= u32::MAX as usize,
                    concat!(stringify!($name), " index out of u32 range"),
                );
                $name(index as u32)
            }

            #[inline]
            fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                // `Name(3)` reads the same in logs and in `{:?}` dumps,
                // and stays terse inside the larger plan structures that
                // embed these ids by the hundreds.
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

/// Dense side-table from an [`EntityRef`] key to a value, with a stored
/// default for absent keys.
///
/// This never mints keys — it attaches data to keys minted elsewhere.
/// Reads of an absent key (one past the end of the materialized backing
/// vec) return the stored default rather than panicking; writes grow the
/// vec with clones of that default. This is the Cranelift / rustc
/// "dense + default-on-absent" shape.
///
/// The default determines what "absent" reads as: a
/// `SecondaryMap<K, Option<T>>` reads `None` for never-written keys, a
/// `SecondaryMap<K, u32>` reads `0`. Choose `V` (and, via
/// [`with_default`](SecondaryMap::with_default), the default itself) so
/// the absent reading is the one you want.
#[derive(Debug, Clone)]
pub struct SecondaryMap<K: EntityRef, V: Clone> {
    values: Vec<V>,
    default: V,
    _key: PhantomData<fn(K) -> K>,
}

impl<K: EntityRef, V: Clone> SecondaryMap<K, V> {
    /// An empty map whose absent-key default is `default`. Use this when
    /// `V` has no `Default`, or when the wanted absent reading differs
    /// from `V::default()`.
    pub fn with_default(default: V) -> Self {
        Self {
            values: Vec::new(),
            default,
            _key: PhantomData,
        }
    }

    /// Borrow the value at `k`, returning the stored default for any key
    /// not yet materialized. Never panics.
    pub fn get(&self, k: K) -> &V {
        self.values.get(k.index()).unwrap_or(&self.default)
    }

    /// Set the value at `k`, growing the backing vec with clones of the
    /// default so every intermediate slot stays readable as the default.
    pub fn insert(&mut self, k: K, v: V) {
        let i = k.index();
        if i >= self.values.len() {
            self.values.resize(i + 1, self.default.clone());
        }
        self.values[i] = v;
    }

    /// Drop all materialized slots. The default is retained.
    pub fn clear(&mut self) {
        self.values.clear();
    }
}

impl<K: EntityRef, V: Clone> Index<K> for SecondaryMap<K, V> {
    type Output = V;

    /// Returns the stored default for absent keys; never panics.
    fn index(&self, k: K) -> &V {
        self.get(k)
    }
}

entity_id! {
    /// Dense, globally-unique identity for a node in a compiled plan.
    ///
    /// Minted once at graph-construction time and carried unchanged
    /// through every plan and exec pass, a `PlanNodeId` is *the* key for
    /// per-node side tables ([`SecondaryMap`] keyed by it). It is
    /// deliberately distinct from a graph `NodeIndex`: a `NodeIndex` is a
    /// storage position within one graph and can shift as the graph is
    /// rebuilt, whereas a `PlanNodeId` is stable identity that survives
    /// lowering, re-indexing, and cross-pass handoff.
    pub struct PlanNodeId;
}

#[cfg(test)]
mod tests {
    use super::*;

    // A standalone test key so the map tests do not lean on PlanNodeId's
    // semantics, only on the EntityRef contract.
    entity_id! {
        /// Test-only entity key.
        pub struct TestId;
    }

    #[test]
    fn entity_ref_round_trips() {
        for i in [0usize, 1, 2, 7, 1000, u32::MAX as usize] {
            assert_eq!(TestId::new(i).index(), i);
        }
    }

    #[test]
    fn entity_ref_equality_and_ordering_follow_u32() {
        let a = TestId::new(3);
        let b = TestId::new(3);
        let c = TestId::new(9);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a < c);
        assert!(c > b);
    }

    #[test]
    fn entity_id_renders_as_documented() {
        let id = PlanNodeId::new(3);
        assert_eq!(format!("{id}"), "PlanNodeId(3)");
        assert_eq!(format!("{id:?}"), "PlanNodeId(3)");
    }

    #[test]
    fn secondary_map_with_default_for_non_default_value() {
        // &str has no Default; with_default supplies the absent reading.
        let mut m: SecondaryMap<TestId, &str> = SecondaryMap::with_default("missing");
        assert_eq!(*m.get(TestId::new(0)), "missing");
        m.insert(TestId::new(1), "present");
        assert_eq!(*m.get(TestId::new(0)), "missing");
        assert_eq!(*m.get(TestId::new(1)), "present");
    }

    #[test]
    fn secondary_map_insert_past_end_grows_with_default() {
        let mut m: SecondaryMap<TestId, u32> = SecondaryMap::with_default(0);
        m.insert(TestId::new(3), 77);
        // Intermediate slots materialized to the default; reads go through
        // both `get` and the `Index` impl.
        assert_eq!(*m.get(TestId::new(0)), 0);
        assert_eq!(m[TestId::new(1)], 0);
        assert_eq!(m[TestId::new(2)], 0);
        assert_eq!(m[TestId::new(3)], 77);
        // Beyond the materialized range still reads the default.
        assert_eq!(*m.get(TestId::new(4)), 0);
    }

    #[test]
    fn secondary_map_clear_retains_default() {
        let mut m: SecondaryMap<TestId, u32> = SecondaryMap::with_default(9);
        m.insert(TestId::new(0), 1);
        m.clear();
        // Cleared slots read the retained default again.
        assert_eq!(*m.get(TestId::new(0)), 9);
    }
}
