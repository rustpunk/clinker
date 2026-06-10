//! A 24-byte field-value string with three storage arms behind one `str` API.
//!
//! `Value::String` is the dominant payload in an ETL record stream, so its
//! width sets the per-`Value` byte cost that drives the RSS / spill threshold.
//! `FieldStr` keeps that width at 24 bytes — the same as `smol_str::SmolStr`,
//! so `Value` stays 32 bytes — while supporting three representations:
//!
//! - **inline** — values up to [`INLINE_CAP`] bytes live in the struct with
//!   zero heap allocation (the dominant short-field shape: ids, codes, flags);
//! - **shared** — longer values that repeat or are cloned across stages
//!   (fan-out, group-by, joins) are `Arc<str>`-backed, so a clone is an O(1)
//!   refcount bump rather than an allocation + copy;
//! - **unique** — longer values flagged long-and-unique by the source schema
//!   are `Box<str>`-backed, dropping the ~16-byte `Arc` strong/weak refcount
//!   header that is pure overhead when a value never repeats (UUIDs, street
//!   addresses, free-text notes).
//!
//! The arm is an in-memory storage hint only: all access is through the `str`
//! the value denotes (via [`Deref`], [`as_str`](FieldStr::as_str), and the
//! `str`-delegating `Eq`/`Ord`/`Hash`/`Display`), so a shared and a unique
//! `FieldStr` of equal content compare, sort, hash, and group/join as equal.
//! That equivalence is what lets the unique arm be a transparent per-column
//! footprint optimization rather than a new value domain.
//!
//! # Layout & niche
//!
//! The three arms share one 24-byte footprint via a private union discriminated
//! by the trailing byte. The inline arm stores its length there (`0..=INLINE_CAP`);
//! the two heap arms use the sentinel tags [`TAG_SHARED`] and [`TAG_UNIQUE`],
//! which are chosen above `INLINE_CAP` so they can never collide with a valid
//! inline length. This is `FieldStr`'s own niche — it owns the tag byte outright
//! rather than borrowing spare bits from `smol_str`'s private `InlineSize` enum,
//! whose layout `#[repr]` does not stabilize and which exposes no public spare
//! niche. Owning the niche keeps the unsafe self-contained and auditable here.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

/// Maximum number of bytes stored inline (without heap allocation).
///
/// The 24-byte footprint spends its trailing byte on the discriminant, leaving
/// 23 bytes for inline data — matching `smol_str`'s inline capacity, so swapping
/// `SmolStr` for `FieldStr` does not change which values stay off the heap.
pub const INLINE_CAP: usize = 23;

/// Trailing-byte tag marking the `Arc<str>`-backed shared arm. Above
/// [`INLINE_CAP`] so it never aliases a valid inline length.
const TAG_SHARED: u8 = 0xFF;

/// Trailing-byte tag marking the `Box<str>`-backed unique arm. Above
/// [`INLINE_CAP`] so it never aliases a valid inline length.
const TAG_UNIQUE: u8 = 0xFE;

/// Inline arm: `len` bytes of UTF-8 in `data`, with the discriminant byte at
/// `data[INLINE_CAP]` holding `len` (always `<= INLINE_CAP`, hence `< TAG_*`).
#[derive(Clone, Copy)]
#[repr(C)]
struct Inline {
    data: [u8; INLINE_CAP],
    /// Doubles as length (inline) and discriminant: `<= INLINE_CAP` means
    /// inline; `TAG_SHARED` / `TAG_UNIQUE` select a heap arm.
    tag: u8,
}

/// A heap arm's leading fat-pointer fields. `ptr`/`len` describe the backing
/// `str`; `tag` (at the same trailing-byte offset as [`Inline::tag`]) selects
/// shared vs unique. The owning smart pointer (`Arc<str>` or `Box<str>`) is
/// reconstructed from `ptr`/`len` for deref, clone, and drop.
#[derive(Clone, Copy)]
#[repr(C)]
struct HeapHeader {
    ptr: *const u8,
    len: usize,
    /// Padding so `tag` lands at byte offset [`INLINE_CAP`], overlapping
    /// [`Inline::tag`]. `ptr` (8) + `len` (8) = 16 bytes; 7 padding bytes carry
    /// the offset to 23, then `tag` at 23.
    _pad: [u8; INLINE_CAP - 2 * std::mem::size_of::<usize>()],
    tag: u8,
}

/// 24-byte union of the inline and heap representations, discriminated by the
/// trailing `tag` byte both arms share at offset [`INLINE_CAP`].
#[repr(C)]
union Repr {
    inline: Inline,
    heap: HeapHeader,
}

/// A field-value string stored inline, `Arc`-shared, or `Box`-unique behind a
/// single `str` API. 24 bytes wide; see the module docs for the layout and the
/// arm-as-storage-hint equivalence guarantee.
pub struct FieldStr {
    repr: Repr,
}

// SAFETY: `FieldStr` owns either inline bytes, an `Arc<str>`, or a `Box<str>`.
// All three are `Send`/`Sync` (`Arc<str>: Send + Sync`, `Box<str>: Send + Sync`),
// and the raw `*const u8` is only ever reconstituted back into its owning smart
// pointer, never shared as a bare pointer, so the auto-trait reasoning matches
// the owned-pointer arms. The inline arm holds only `Copy` bytes.
unsafe impl Send for FieldStr {}
// SAFETY: see the `Send` impl — every arm's backing storage is `Sync` and
// access is read-only through `&str` once constructed.
unsafe impl Sync for FieldStr {}

const _: () = {
    // The whole design rests on the 24-byte footprint: a wider `FieldStr` would
    // push `Value` past 32 bytes and inflate the per-`Value` cost model.
    assert!(std::mem::size_of::<FieldStr>() == 24);
    // The discriminant byte must occupy the same offset in both arms, so reading
    // `tag` through either union field always reads the live discriminant. Both
    // arms are `#[repr(C)]` with `tag` last; this pins that they coincide.
    assert!(std::mem::offset_of!(Inline, tag) == INLINE_CAP);
    assert!(std::mem::offset_of!(HeapHeader, tag) == INLINE_CAP);
    // The two heap tags must sit outside the inline-length range so the
    // discriminant is unambiguous.
    assert!(TAG_SHARED as usize > INLINE_CAP);
    assert!(TAG_UNIQUE as usize > INLINE_CAP);
};

impl FieldStr {
    /// Constructs a `FieldStr` choosing inline storage when the value fits,
    /// otherwise the `Arc`-shared arm. This is the default policy: identical
    /// to `smol_str`'s inline-or-shared split, so default-constructed field
    /// values keep their pre-existing heap behavior byte-for-byte.
    #[inline]
    pub fn new(s: &str) -> Self {
        if s.len() <= INLINE_CAP {
            Self::new_inline(s)
        } else {
            Self::new_shared(s)
        }
    }

    /// Constructs the inline arm. Caller guarantees `s.len() <= INLINE_CAP`.
    #[inline]
    fn new_inline(s: &str) -> Self {
        debug_assert!(s.len() <= INLINE_CAP);
        let mut data = [0u8; INLINE_CAP];
        data[..s.len()].copy_from_slice(s.as_bytes());
        FieldStr {
            repr: Repr {
                inline: Inline {
                    data,
                    tag: s.len() as u8,
                },
            },
        }
    }

    /// Constructs the `Arc`-shared arm unconditionally (no inline fast path),
    /// allocating a fresh `Arc<str>`.
    #[inline]
    fn new_shared(s: &str) -> Self {
        Self::from_arc(Arc::from(s))
    }

    /// Wraps an existing `Arc<str>` into the shared arm without re-allocating,
    /// preserving refcount sharing with any other holder of that `Arc`.
    #[inline]
    fn from_arc(arc: Arc<str>) -> Self {
        let len = arc.len();
        // Hand the allocation's ownership to the raw pointer; `Drop` /`Clone`
        // reconstitute the `Arc` from `ptr`/`len`.
        let ptr = Arc::into_raw(arc) as *const u8;
        FieldStr {
            repr: Repr {
                heap: HeapHeader {
                    ptr,
                    len,
                    _pad: [0; INLINE_CAP - 2 * std::mem::size_of::<usize>()],
                    tag: TAG_SHARED,
                },
            },
        }
    }

    /// Constructs the header-free `Box`-unique arm.
    ///
    /// Intended for source columns a schema flags long-and-unique: the value
    /// never repeats, so the `Arc` refcount header would be pure overhead.
    /// Short values still take the unique arm here (no inline fast path) — the
    /// flag is an explicit author assertion that this column's values are long,
    /// and honoring it verbatim keeps the arm's footprint predictable.
    #[inline]
    pub fn new_unique(s: &str) -> Self {
        Self::from_box(Box::from(s))
    }

    /// Wraps an existing `Box<str>` into the unique arm without re-allocating.
    #[inline]
    fn from_box(boxed: Box<str>) -> Self {
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *const u8;
        FieldStr {
            repr: Repr {
                heap: HeapHeader {
                    ptr,
                    len,
                    _pad: [0; INLINE_CAP - 2 * std::mem::size_of::<usize>()],
                    tag: TAG_UNIQUE,
                },
            },
        }
    }

    /// Returns the discriminant byte both arms share at offset [`INLINE_CAP`].
    #[inline]
    fn tag(&self) -> u8 {
        // SAFETY: `tag` lives at the same `#[repr(C)]` offset in both union
        // arms (`Inline::tag` and `HeapHeader::tag`), so reading it through
        // either field is well-defined regardless of which arm is active.
        unsafe { self.repr.inline.tag }
    }

    /// Borrows the value as a `str`. The single read path for every arm.
    #[inline]
    pub fn as_str(&self) -> &str {
        let tag = self.tag();
        if tag <= INLINE_CAP as u8 {
            // SAFETY: inline arm — `tag` is the byte length (`<= INLINE_CAP`),
            // and `new_inline` only ever copies valid UTF-8 into `data[..len]`.
            unsafe {
                let inline = &self.repr.inline;
                std::str::from_utf8_unchecked(&inline.data[..tag as usize])
            }
        } else {
            // SAFETY: heap arm — `ptr`/`len` were produced by `Arc::into_raw` /
            // `Box::into_raw` on a `str`, so the bytes are live, owned by this
            // `FieldStr`, and valid UTF-8 for `len` bytes. The borrow is tied to
            // `&self`, so the allocation outlives the returned `&str`.
            unsafe {
                let heap = &self.repr.heap;
                let bytes = std::slice::from_raw_parts(heap.ptr, heap.len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }

    /// Heap bytes owned by this value, excluding the 24-byte `FieldStr` itself.
    ///
    /// Inline values own no heap (`0`). Both heap arms charge their UTF-8 byte
    /// length; the model deliberately ignores the `Arc` header for the shared
    /// arm (consistent with the pre-existing `SmolStr` accounting) and the unique
    /// arm has no header to charge. Drives `SortBuffer`'s self-tracking budget.
    #[inline]
    pub fn heap_size(&self) -> usize {
        if self.tag() <= INLINE_CAP as u8 {
            0
        } else {
            // SAFETY: heap arm — `len` is valid in both heap representations.
            unsafe { self.repr.heap.len }
        }
    }

    /// Returns true when the value is stored in the header-free unique arm
    /// rather than the default inline-or-`Arc`-shared one. Lets the reader and
    /// its tests confirm a schema-flagged column landed in the intended
    /// representation; the arm is otherwise invisible through the `str` API.
    #[inline]
    pub fn is_unique(&self) -> bool {
        self.tag() == TAG_UNIQUE
    }
}

impl Drop for FieldStr {
    fn drop(&mut self) {
        let tag = self.tag();
        if tag == TAG_SHARED {
            // SAFETY: the shared arm owns one `Arc<str>` strong count produced by
            // `Arc::into_raw`. Reconstitute the fat pointer from `ptr`/`len` and
            // let the `Arc` drop, releasing exactly that one count.
            unsafe {
                let heap = self.repr.heap;
                let slice = std::ptr::slice_from_raw_parts(heap.ptr, heap.len) as *const str;
                drop(Arc::from_raw(slice));
            }
        } else if tag == TAG_UNIQUE {
            // SAFETY: the unique arm owns one `Box<str>` produced by
            // `Box::into_raw`. Reconstitute and drop it, freeing the allocation.
            unsafe {
                let heap = self.repr.heap;
                let slice =
                    std::ptr::slice_from_raw_parts_mut(heap.ptr as *mut u8, heap.len) as *mut str;
                drop(Box::from_raw(slice));
            }
        }
        // Inline arm owns no heap — nothing to release.
    }
}

impl Clone for FieldStr {
    fn clone(&self) -> Self {
        let tag = self.tag();
        if tag <= INLINE_CAP as u8 {
            // SAFETY: inline arm holds only `Copy` bytes; bitwise copy is sound.
            FieldStr {
                repr: Repr {
                    inline: unsafe { self.repr.inline },
                },
            }
        } else if tag == TAG_SHARED {
            // SAFETY: shared arm — reconstitute the `Arc` to bump its strong
            // count, then re-leak both the original (via `ManuallyDrop`, so this
            // borrow does not release a count) and the clone.
            unsafe {
                let heap = self.repr.heap;
                let slice = std::ptr::slice_from_raw_parts(heap.ptr, heap.len) as *const str;
                let arc = ManuallyDrop::new(Arc::from_raw(slice));
                let cloned: Arc<str> = Arc::clone(&arc);
                Self::from_arc(cloned)
            }
        } else {
            // Unique arm has no shared ownership to bump — a clone deep-copies
            // the bytes into a fresh `Box<str>`, preserving the unique arm.
            Self::from_box(Box::from(self.as_str()))
        }
    }
}

impl Deref for FieldStr {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for FieldStr {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for FieldStr {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

// ── str-delegating comparison & hashing ───────────────────────────────────
//
// Every relational and hashing impl routes through `as_str()`, so the storage
// arm is invisible to equality, ordering, hashing, and grouping. This is the
// load-bearing guarantee that a shared `FieldStr` and a unique `FieldStr` of
// equal content behave identically everywhere (group-by, distinct, join, sort).

impl PartialEq for FieldStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for FieldStr {}

impl PartialEq<str> for FieldStr {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for FieldStr {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialOrd for FieldStr {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FieldStr {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for FieldStr {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash exactly as `str` does, so `HashMap`/`HashSet` keyed by content
        // see no difference between the arms (and match a bare `&str` lookup
        // via the `Borrow<str>` impl).
        self.as_str().hash(state);
    }
}

impl fmt::Display for FieldStr {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for FieldStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

// ── Constructors from owned/borrowed string types ─────────────────────────
//
// Every conversion defaults to the inline-or-shared policy (`FieldStr::new`),
// matching the prior `SmolStr` behavior exactly. The unique arm is reachable
// only through the explicit `new_unique` constructor, driven by the schema
// flag — never by an ambient `From` conversion — so default construction is
// byte-identical to pre-existing behavior.

impl From<&str> for FieldStr {
    #[inline]
    fn from(s: &str) -> Self {
        FieldStr::new(s)
    }
}

impl From<String> for FieldStr {
    #[inline]
    fn from(s: String) -> Self {
        // Reuse the owned allocation for the shared arm when it would not fit
        // inline, avoiding a re-copy.
        if s.len() <= INLINE_CAP {
            FieldStr::new_inline(&s)
        } else {
            FieldStr::from_arc(Arc::from(s))
        }
    }
}

impl From<smol_str::SmolStr> for FieldStr {
    #[inline]
    fn from(s: smol_str::SmolStr) -> Self {
        FieldStr::new(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(s: &impl Hash) -> u64 {
        let mut h = DefaultHasher::new();
        s.hash(&mut h);
        h.finish()
    }

    #[test]
    fn size_is_24_bytes() {
        assert_eq!(std::mem::size_of::<FieldStr>(), 24);
    }

    #[test]
    fn inline_arm_holds_short_values_without_heap() {
        for s in ["", "x", "short", &"a".repeat(INLINE_CAP)] {
            let fs = FieldStr::new(s);
            assert_eq!(fs.as_str(), s, "inline roundtrip for {s:?}");
            assert_eq!(fs.len(), s.len());
            assert_eq!(fs.heap_size(), 0, "{s:?} should be inline");
            assert!(!fs.is_unique());
            assert_eq!(fs.heap_size(), 0);
        }
    }

    #[test]
    fn shared_arm_holds_long_values_on_heap() {
        let long = "this string is definitely longer than twenty-three bytes";
        assert!(long.len() > INLINE_CAP);
        let fs = FieldStr::new(long);
        assert_eq!(fs.as_str(), long);
        assert_eq!(fs.len(), long.len());
        assert!(fs.heap_size() > 0);
        assert!(!fs.is_unique());
        assert_eq!(fs.heap_size(), long.len());
    }

    #[test]
    fn unique_arm_is_header_free_box() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let fs = FieldStr::new_unique(uuid);
        assert_eq!(fs.as_str(), uuid);
        assert_eq!(fs.len(), uuid.len());
        assert!(fs.heap_size() > 0);
        assert!(fs.is_unique());
        assert_eq!(fs.heap_size(), uuid.len());
    }

    #[test]
    fn cross_arm_equality_shared_vs_unique() {
        let s = "550e8400-e29b-41d4-a716-446655440000";
        let shared = FieldStr::new(s);
        let unique = FieldStr::new_unique(s);
        assert!(shared.heap_size() > 0 && !shared.is_unique());
        assert!(unique.is_unique());
        // Equal content compares, orders, and hashes equal across arms.
        assert_eq!(shared, unique);
        assert_eq!(shared.cmp(&unique), Ordering::Equal);
        assert_eq!(hash_of(&shared), hash_of(&unique));
        // And both equal a bare `str` hash via the `Borrow<str>` contract.
        assert_eq!(hash_of(&shared), hash_of(&s));
    }

    #[test]
    fn cross_arm_equality_inline_vs_unique() {
        // A short value via the inline default and via the explicit unique arm
        // must still compare and hash equal — content, not arm, decides.
        let s = "short-id";
        let inline = FieldStr::new(s);
        let unique = FieldStr::new_unique(s);
        assert_eq!(inline.heap_size(), 0);
        assert!(unique.is_unique());
        assert_eq!(inline, unique);
        assert_eq!(hash_of(&inline), hash_of(&unique));
    }

    #[test]
    fn ordering_is_lexicographic_regardless_of_arm() {
        let a_inline = FieldStr::new("apple");
        let b_unique = FieldStr::new_unique("banana-is-a-much-longer-value-here");
        assert!(a_inline < b_unique);
        let a_unique = FieldStr::new_unique("apple-but-now-long-enough-to-box");
        let b_shared = FieldStr::new("banana-but-now-long-enough-to-arc");
        assert!(a_unique < b_shared);
    }

    #[test]
    fn shared_clone_shares_allocation() {
        let long = "this string is definitely longer than twenty-three bytes";
        let original = FieldStr::new(long);
        let cloned = original.clone();
        // A shared clone is an O(1) refcount bump: clone aliases the original's
        // backing allocation (same `str` address).
        assert_eq!(original.as_str().as_ptr(), cloned.as_str().as_ptr());
        assert_eq!(original.as_str(), cloned.as_str());
    }

    #[test]
    fn unique_clone_deep_copies_and_stays_unique() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let original = FieldStr::new_unique(uuid);
        let cloned = original.clone();
        // No shared ownership to bump — the clone owns a distinct allocation.
        assert_ne!(original.as_str().as_ptr(), cloned.as_str().as_ptr());
        assert_eq!(original.as_str(), cloned.as_str());
        assert!(cloned.is_unique());
    }

    #[test]
    fn inline_clone_is_value_copy() {
        let fs = FieldStr::new("inline-value");
        let cloned = fs.clone();
        assert_eq!(fs, cloned);
        assert_eq!(cloned.heap_size(), 0);
    }

    #[test]
    fn drop_releases_each_arm_without_double_free() {
        // Exercise drop on every arm, including a shared clone (two strong
        // counts) and a unique clone (two boxes). Under address sanitizer /
        // miri this proves no double-free or leak; under normal test it proves
        // drop does not panic and ownership reconstitution is balanced.
        for _ in 0..1000 {
            let inline = FieldStr::new("x");
            let shared = FieldStr::new("a value that is comfortably over the inline boundary");
            let shared2 = shared.clone();
            let unique = FieldStr::new_unique("another value comfortably over the inline boundary");
            let unique2 = unique.clone();
            drop((inline, shared, shared2, unique, unique2));
        }
    }

    #[test]
    fn from_string_reuses_or_inlines() {
        let short = String::from("short");
        let fs = FieldStr::from(short);
        assert_eq!(fs.as_str(), "short");
        assert_eq!(fs.heap_size(), 0);

        let long = String::from("a string value well past the inline boundary of the field type");
        let fs = FieldStr::from(long.clone());
        assert_eq!(fs.as_str(), long);
        assert!(fs.heap_size() > 0);
        assert!(!fs.is_unique());
    }

    #[test]
    fn from_smol_str_defaults_to_inline_or_shared() {
        let fs = FieldStr::from(smol_str::SmolStr::new("via-smol"));
        assert_eq!(fs.as_str(), "via-smol");
        assert!(!fs.is_unique());
    }

    #[test]
    fn utf8_multibyte_roundtrips_in_every_arm() {
        // Inline (fits within INLINE_CAP), shared, and unique arms must all
        // preserve multibyte UTF-8 exactly.
        let short = "café"; // 5 bytes
        assert!(short.len() <= INLINE_CAP);
        assert_eq!(FieldStr::new(short).as_str(), short);

        let long = "a longer string with accents: café, naïve, résumé, jalapeño!";
        assert!(long.len() > INLINE_CAP);
        assert_eq!(FieldStr::new(long).as_str(), long);
        assert_eq!(FieldStr::new_unique(long).as_str(), long);
    }

    #[test]
    fn hashset_membership_is_arm_agnostic() {
        use std::collections::HashSet;
        let mut set: HashSet<FieldStr> = HashSet::new();
        set.insert(FieldStr::new("550e8400-e29b-41d4-a716-446655440000"));
        // A unique-arm probe of equal content hits the shared-arm entry.
        assert!(
            set.contains(FieldStr::new_unique("550e8400-e29b-41d4-a716-446655440000").as_str())
        );
        // Inserting the same content via the unique arm does not grow the set.
        set.insert(FieldStr::new_unique("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(set.len(), 1);
    }
}
