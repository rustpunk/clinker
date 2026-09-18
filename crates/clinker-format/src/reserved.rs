//! Exact-layout containers. Grants cover both blocks throughout replacement.
//!
//! Only `len` elements are initialized; allocation and deallocation always use
//! the identical layout. ZSTs use an aligned dangling pointer and no allocation.

use std::alloc::{Layout, alloc, dealloc};
use std::marker::PhantomData;
use std::ptr::NonNull;

use clinker_record::owned_storage::{
    AllocationLease, AllocationScope, ResourceError, ResourceErrorKind,
};

#[cfg(test)]
thread_local! { static FAIL_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn allocate(layout: Layout) -> *mut u8 {
    #[cfg(test)]
    if FAIL_ALLOCATION.with(|fail| fail.replace(false)) {
        return std::ptr::null_mut();
    }
    // SAFETY: all callers supply a nonzero valid Layout.
    unsafe { alloc(layout) }
}

/// Allocate an already-admitted box. Failure returns the intact owner so its
/// caller can observe the error while all grants are still live, before drop.
pub(crate) fn try_box<T>(value: T) -> Result<Box<T>, (ResourceError, T)> {
    #[cfg(test)]
    if std::mem::size_of::<T>() != 0 && FAIL_ALLOCATION.with(|fail| fail.replace(false)) {
        return Err((
            ResourceError::new(ResourceErrorKind::Allocation, std::mem::size_of::<T>(), 0),
            value,
        ));
    }
    clinker_record::owned_storage::try_box(value)
}

/// A fallibly growing vector whose allocation is admitted before it occurs.
/// Retains capacity until drop; never exposes a growable standard container.
pub struct ReservedVec<T> {
    ptr: NonNull<T>,
    len: usize,
    capacity: usize,
    grant: Option<AllocationLease>,
    scope: AllocationScope,
    marker: PhantomData<T>,
}

// SAFETY: exclusively owns initialized T values and its allocation. Moving the
// owner between threads is safe exactly when moving T is safe.
unsafe impl<T: Send> Send for ReservedVec<T> {}
// SAFETY: shared access exposes only shared T slices, never mutation.
unsafe impl<T: Sync> Sync for ReservedVec<T> {}

impl<T> ReservedVec<T> {
    /// Construct empty storage without allocating.
    pub fn new(scope: AllocationScope) -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            capacity: 0,
            grant: None,
            scope,
            marker: PhantomData,
        }
    }

    /// Number of initialized elements.
    pub fn len(&self) -> usize {
        self.len
    }
    /// Whether no elements are initialized.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Number of admitted element slots.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    /// Borrow initialized elements only.
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: the pointer is aligned even for empty/ZST storage; precisely
        // len elements have been initialized and belong to this owner.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
    /// Mutably borrow initialized elements only.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: same initialization invariant as as_slice, with exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Admit and allocate exactly the requested capacity, preserving the old
    /// allocation and contents on refusal or allocation failure.
    pub fn reserve_exact(&mut self, capacity: usize) -> Result<(), ResourceError> {
        if capacity <= self.capacity {
            return Ok(());
        }
        self.scope.check_cancelled()?;
        let layout = Layout::array::<T>(capacity)
            .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, capacity, 0))?;
        if layout.size() == 0 {
            self.capacity = capacity;
            return Ok(());
        }
        let grant = self.scope.reserve(layout)?;
        // SAFETY: layout has positive size and valid alignment. Null is handled
        // as a recoverable failure, dropping the unused grant.
        let new = NonNull::new(allocate(layout).cast::<T>())
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Allocation, layout.size(), 0))?;
        // SAFETY: new and old allocations do not overlap; len initialized T
        // values fit in both. A bitwise move calls no user code and cannot panic.
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), new.as_ptr(), self.len);
        }
        if self.capacity != 0 {
            // SAFETY: old pointer came from this exact layout. Its elements
            // were moved, so deallocate without running their destructors.
            unsafe {
                dealloc(
                    self.ptr.as_ptr().cast(),
                    Layout::array::<T>(self.capacity).unwrap_unchecked(),
                );
            }
        }
        self.ptr = new;
        self.capacity = capacity;
        self.grant = Some(grant);
        Ok(())
    }

    /// Append one element, reserving its replacement allocation first.
    pub fn push(&mut self, value: T) -> Result<(), ResourceError> {
        let next = self
            .len
            .checked_add(1)
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Layout, usize::MAX, 0))?;
        self.reserve_amortized(next)?;
        // SAFETY: next is admitted, the slot is uninitialized and uniquely owned.
        unsafe {
            self.ptr.as_ptr().add(self.len).write(value);
        }
        self.len = next;
        Ok(())
    }

    fn reserve_amortized(&mut self, needed: usize) -> Result<(), ResourceError> {
        if needed <= self.capacity {
            return Ok(());
        }
        let preferred = self.capacity.checked_mul(2).unwrap_or(needed).max(needed);
        match self.reserve_exact(preferred) {
            Err(error) if preferred != needed && error.kind == ResourceErrorKind::Budget => {
                self.reserve_exact(needed)
            }
            result => result,
        }
    }
}

impl<T> Drop for ReservedVec<T> {
    fn drop(&mut self) {
        // Use a separate guard so a panicking element destructor still frees
        // the block and releases the grant exactly once during unwinding.
        struct Block<T> {
            ptr: NonNull<T>,
            capacity: usize,
            _grant: Option<AllocationLease>,
        }
        impl<T> Drop for Block<T> {
            fn drop(&mut self) {
                if self.capacity != 0 && std::mem::size_of::<T>() != 0 {
                    // SAFETY: this is the identical layout that allocated ptr.
                    unsafe {
                        dealloc(
                            self.ptr.as_ptr().cast(),
                            Layout::array::<T>(self.capacity).unwrap_unchecked(),
                        );
                    }
                }
            }
        }
        let _block = Block {
            ptr: self.ptr,
            capacity: self.capacity,
            _grant: self.grant.take(),
        };
        // SAFETY: the slice includes exactly initialized elements, including
        // ZSTs with destructors. Slice drop handles partially unwound drops.
        unsafe {
            std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                self.ptr.as_ptr(),
                self.len,
            ));
        }
    }
}

/// Admitted byte storage, with no unchecked growth escape hatch.
pub type ReservedBuffer = ReservedVec<u8>;

impl ReservedBuffer {
    /// Copy bytes after reserving their complete replacement peak.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), ResourceError> {
        let end = self
            .len
            .checked_add(bytes.len())
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Layout, bytes.len(), 0))?;
        self.reserve_amortized(end)?;
        // SAFETY: end fits the allocation and the input cannot alias exclusive self.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.ptr.as_ptr().add(self.len),
                bytes.len(),
            );
        }
        self.len = end;
        Ok(())
    }
}

/// UTF-8 text backed by admitted bytes. Appending always preserves UTF-8.
pub struct ReservedText(ReservedBuffer);
impl ReservedText {
    /// Construct empty text without allocating.
    pub fn new(scope: AllocationScope) -> Self {
        Self(ReservedBuffer::new(scope))
    }
    /// Append valid UTF-8 after admission.
    pub fn push_str(&mut self, value: &str) -> Result<(), ResourceError> {
        self.0.extend_from_slice(value.as_bytes())
    }
    /// Borrow the initialized UTF-8 text.
    pub fn as_str(&self) -> &str {
        // SAFETY: only valid str bytes enter this private buffer.
        unsafe { std::str::from_utf8_unchecked(self.0.as_slice()) }
    }
    /// Move the same allocation and grant without copying bytes.
    pub fn into_bytes(self) -> ReservedBuffer {
        self.0
    }

    /// Transfer UTF-8 backing without copying or reallocating. The receiving
    /// owner must never grow the String and must destroy it before its lease.
    /// Kept crate-private so callers cannot detach uncharged growable storage.
    pub(crate) fn into_string_parts(self) -> (String, Option<AllocationLease>) {
        let mut bytes = self.0;
        let (pointer, len, capacity) = (bytes.ptr.as_ptr(), bytes.len, bytes.capacity);
        let lease = bytes.grant.take();
        bytes.ptr = NonNull::dangling();
        bytes.len = 0;
        bytes.capacity = 0;
        // SAFETY: ReservedBuffer allocates Layout::array::<u8>(capacity), the
        // identical allocator/layout Vec<u8> and String use. Exactly len bytes
        // are initialized and all entered through str appends. Empty storage
        // has an aligned dangling pointer. The old owner was disarmed above;
        // the returned lease still covers the entire unchanged allocation.
        let text =
            unsafe { String::from_utf8_unchecked(Vec::from_raw_parts(pointer, len, capacity)) };
        (text, lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preparation::MemoryOnlyResources;
    use std::num::NonZeroUsize;

    #[test]
    fn fixed_width_warning_text_transfer_preserves_backing_and_charge() {
        for value in ["", "complete ASCII warning", "complete é好 warning"] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
            let mut text = ReservedText::new(provider.resources().allocation().scope().unwrap());
            text.push_str(value).unwrap();
            let pointer = text.as_str().as_ptr();
            let charge = provider.used();
            let (text, lease) = text.into_string_parts();
            assert_eq!(text.as_ptr(), pointer);
            assert_eq!(text, value);
            assert_eq!(text.capacity(), charge);
            assert_eq!(provider.used(), charge);
            drop(text);
            assert_eq!(
                provider.used(),
                charge,
                "lease survives the transferred backing"
            );
            drop(lease);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn memory_allocator_failure_releases_new_grant_preserves_old() {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
        let mut bytes = ReservedBuffer::new(provider.resources().allocation().scope().unwrap());
        bytes.extend_from_slice(b"old").unwrap();
        FAIL_ALLOCATION.with(|fail| fail.set(true));
        assert_eq!(
            bytes.extend_from_slice(b"new").unwrap_err().kind,
            ResourceErrorKind::Allocation
        );
        assert_eq!(bytes.as_slice(), b"old");
        assert_eq!(provider.used(), 3);
        drop(bytes);
        assert_eq!(provider.used(), 0);
    }
}
