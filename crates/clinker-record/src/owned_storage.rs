//! Allocation-owned storage primitives.

use std::alloc::Layout;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Allocate an already-admitted box. Failure returns the intact owner so its
/// caller can observe the error while all grants are still live, before drop.
/// The caller must retain admission through destruction of the allocation.
#[doc(hidden)]
pub fn try_box<T>(value: T) -> Result<Box<T>, (ResourceError, T)> {
    let layout = Layout::new::<T>();
    if layout.size() == 0 {
        return Ok(Box::new(value));
    }
    // SAFETY: layout is nonzero and valid; null is handled before initialization.
    let pointer = unsafe { std::alloc::alloc(layout) }.cast::<T>();
    let Some(pointer) = std::ptr::NonNull::new(pointer) else {
        return Err((
            ResourceError::new(ResourceErrorKind::Allocation, layout.size(), 0),
            value,
        ));
    };
    // SAFETY: initialize the complete allocation before creating its sole owner.
    // Box uses the identical global allocator and Layout::new::<T>() at drop.
    unsafe {
        pointer.as_ptr().write(value);
        Ok(Box::from_raw(pointer.as_ptr()))
    }
}

/// Validated UTF-8 and its complete text-plus-holder admission. Field order
/// destroys the text allocation before releasing the lease. The containing
/// shared/unique owner must deallocate its own backing before dropping this.
#[repr(C)]
pub(crate) struct OwnedText {
    text: Box<str>,
    lease: AllocationLease,
}
impl OwnedText {
    pub(crate) fn try_new(
        text: &str,
        scope: &AllocationScope,
        backing: Layout,
    ) -> Result<Self, ResourceError> {
        let text_layout = Layout::array::<u8>(text.len())
            .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, text.len(), 0))?;
        let mut lease = scope.reserve(text_layout)?;
        lease.merge(scope.reserve(backing)?)?;
        // A zero-length Box<str> has no allocation. Normal FieldStr constructors
        // choose inline storage for this case; retaining it here keeps the
        // allocation primitive's empty-input contract explicit.
        if text.is_empty() {
            return Ok(Self {
                text: Box::default(),
                lease,
            });
        }
        // SAFETY: the complete text and backing layouts were admitted above.
        // text_layout is nonzero and exactly matches the resulting Box<str>.
        let pointer = unsafe { std::alloc::alloc(text_layout) };
        let Some(pointer) = std::ptr::NonNull::new(pointer) else {
            return Err(ResourceError::new(
                ResourceErrorKind::Allocation,
                text_layout.size(),
                0,
            ));
        };
        // SAFETY: source is validated UTF-8, destination is uniquely allocated
        // for exactly its byte length, and the ranges cannot overlap. No raw
        // mutable backing escapes this constructor.
        let text = unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), pointer.as_ptr(), text.len());
            let bytes = std::ptr::slice_from_raw_parts_mut(pointer.as_ptr(), text.len());
            Box::from_raw(bytes as *mut str)
        };
        Ok(Self { text, lease })
    }
    pub(crate) fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        if self.lease.is_accounted_by(resources) {
            0
        } else {
            self.heap_size()
        }
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.text
    }
    pub(crate) fn heap_size(&self) -> usize {
        self.lease.requested_bytes()
    }
}

/// Payload backing is destroyed before the inline reservation. Children own
/// their independent allocations and are excluded from this container's lease.
#[repr(C)]
struct Governed<T> {
    payload: T,
    lease: AllocationLease,
}

/// Immutable shared payload with either existing or admitted shared backing.
///
/// Clones retain the same backing without allocating. Admitted backing is freed
/// before the payload and its inline outer lease. Children retain their own
/// independent ownership; this handle does not retroactively admit their heaps.
pub struct SharedStorage<T> {
    storage: SharedStorageKind<T>,
}

enum SharedStorageKind<T> {
    Legacy(Arc<T>),
    Governed(SharedAllocation<Governed<T>>),
}

/// Sealed backing identity without access to or ownership of the payload.
/// Governed identities retain only a non-reusable allocation ID. Legacy
/// identities retain the weak backing with an additional conservative lease;
/// they never retain the payload's children after the final strong owner drops.
pub struct SharedStorageIdentity<T> {
    identity: SharedStorageIdentityKind<T>,
}

enum SharedStorageIdentityKind<T> {
    Governed(u64),
    Legacy {
        // Declaration order frees a final weak backing before releasing charge.
        weak: std::sync::Weak<T>,
        _lease: AllocationLease,
    },
}

impl<T> SharedStorageIdentity<T> {
    /// Compare the original backing with a live candidate without allocating.
    pub fn matches(&self, candidate: &SharedStorage<T>) -> bool {
        match (&self.identity, &candidate.storage) {
            (SharedStorageIdentityKind::Governed(id), SharedStorageKind::Governed(value)) => {
                *id == value.lease.allocation_id()
            }
            (SharedStorageIdentityKind::Legacy { weak, .. }, SharedStorageKind::Legacy(value)) => {
                std::sync::Weak::ptr_eq(weak, &Arc::downgrade(value))
            }
            _ => false,
        }
    }
}

impl<T> SharedStorage<T> {
    /// Obtain a payload-free identity under the caller's finite cache scope.
    /// Legacy backing retention is admitted before downgrade, in addition to
    /// any upstream accounting. Governed IDs require no retained allocation.
    pub fn try_identity(
        &self,
        scope: &AllocationScope,
    ) -> Result<SharedStorageIdentity<T>, ResourceError> {
        scope.check_cancelled()?;
        let identity = match &self.storage {
            SharedStorageKind::Governed(value) => {
                SharedStorageIdentityKind::Governed(value.lease.allocation_id())
            }
            SharedStorageKind::Legacy(value) => {
                let layout = Layout::new::<[AtomicUsize; 2]>()
                    .extend(Layout::new::<T>())
                    .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, usize::MAX, 0))?
                    .0
                    .pad_to_align();
                let lease = scope.reserve(layout)?;
                SharedStorageIdentityKind::Legacy {
                    weak: Arc::downgrade(value),
                    _lease: lease,
                }
            }
        };
        Ok(SharedStorageIdentity { identity })
    }
    /// Own outer backing not already charged to this live aggregate ledger.
    pub fn unaccounted_outer_heap_size(&self, resources: &AllocationResources) -> usize {
        match &self.storage {
            SharedStorageKind::Governed(value) if value.lease.is_accounted_by(resources) => 0,
            _ => self.estimated_outer_heap_size(),
        }
    }

    /// Consume an existing shared owner without allocating or copying its payload.
    pub fn from_arc(value: Arc<T>) -> Self {
        Self {
            storage: SharedStorageKind::Legacy(value),
        }
    }

    /// Admit and allocate the complete outer shared backing before publishing it.
    ///
    /// Consumes `value` on success and failure. Its existing children keep their
    /// own admitted or legacy ownership; only the new outer allocation is charged
    /// here. Reservation failures propagate unchanged, while allocator refusal
    /// returns bounded evidence containing the complete outer layout size.
    pub fn try_new(value: T, scope: &AllocationScope) -> Result<Self, ResourceError> {
        let layout = SharedAllocation::<Governed<T>>::layout().map_err(|_| {
            ResourceError::new(
                ResourceErrorKind::Layout,
                std::mem::size_of::<Governed<T>>(),
                0,
            )
        })?;
        let lease = scope.reserve(layout)?;
        let shared = SharedAllocation::try_new(Governed {
            payload: value,
            lease,
        })
        .map_err(|error| {
            ResourceError::new(
                match error {
                    SharedAllocationError::Layout => ResourceErrorKind::Layout,
                    SharedAllocationError::Allocation => ResourceErrorKind::Allocation,
                },
                layout.size(),
                0,
            )
        })?;
        Ok(Self {
            storage: SharedStorageKind::Governed(shared),
        })
    }

    pub fn estimated_outer_heap_size(&self) -> usize {
        match &self.storage {
            // The two reference counts precede the payload; both the payload
            // offset and the complete allocation include alignment padding.
            // Overflow cannot describe an already allocated Arc, but retain a
            // conservative bound if its layout cannot be represented.
            SharedStorageKind::Legacy(_) => Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
                .extend(Layout::new::<T>())
                .map_or(usize::MAX, |(layout, _)| layout.pad_to_align().size()),
            SharedStorageKind::Governed(value) => value.lease.requested_bytes(),
        }
    }
    pub fn legacy_estimated_outer_heap_size(&self) -> usize {
        match &self.storage {
            SharedStorageKind::Legacy(_) => self.estimated_outer_heap_size(),
            SharedStorageKind::Governed(_) => 0,
        }
    }

    /// Compare backing identity, independently of payload equality. Handles from
    /// different storage branches cannot share a backing and always compare false.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        match (&this.storage, &other.storage) {
            (SharedStorageKind::Legacy(a), SharedStorageKind::Legacy(b)) => Arc::ptr_eq(a, b),
            (SharedStorageKind::Governed(a), SharedStorageKind::Governed(b)) => {
                std::ptr::eq(&**a, &**b)
            }
            _ => false,
        }
    }
}

impl<T> Clone for SharedStorage<T> {
    fn clone(&self) -> Self {
        Self {
            storage: match &self.storage {
                SharedStorageKind::Legacy(value) => SharedStorageKind::Legacy(value.clone()),
                SharedStorageKind::Governed(value) => SharedStorageKind::Governed(value.clone()),
            },
        }
    }
}

impl<T> Deref for SharedStorage<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match &self.storage {
            SharedStorageKind::Legacy(value) => value,
            SharedStorageKind::Governed(value) => &value.payload,
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SharedStorage<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

/// Sealed holder that frees its own Box before destroying the moved payload.
pub(crate) struct GovernedBox<T> {
    inner: ManuallyDrop<Box<Governed<T>>>,
}
impl<T: std::fmt::Debug> std::fmt::Debug for GovernedBox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.payload().fmt(f)
    }
}
impl<T> GovernedBox<T> {
    pub(crate) fn is_accounted_by(&self, resources: &AllocationResources) -> bool {
        self.inner.lease.is_accounted_by(resources)
    }
    pub(crate) fn unaccounted_backing_size(&self, resources: &AllocationResources) -> usize {
        if self.is_accounted_by(resources) {
            0
        } else {
            self.bytes()
        }
    }

    pub(crate) fn try_new(payload: T, lease: AllocationLease) -> Result<Self, ResourceError> {
        match try_box(Governed { payload, lease }) {
            Ok(inner) => Ok(Self {
                inner: ManuallyDrop::new(inner),
            }),
            Err((error, owner)) => {
                drop(owner);
                Err(error)
            }
        }
    }
    pub(crate) fn payload(&self) -> &T {
        &self.inner.payload
    }
    pub(crate) fn payload_mut(&mut self) -> &mut T {
        &mut self.inner.payload
    }
    pub(crate) fn bytes(&self) -> usize {
        self.inner.lease.requested_bytes()
    }
    pub(crate) fn into_parts(self) -> (T, AllocationLease) {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: consume the only Box once; suppress the wrapper destructor.
        // Moving out frees the holder before returning payload and reservation.
        let Governed { payload, lease } = unsafe { *ManuallyDrop::take(&mut this.inner) };
        (payload, lease)
    }
}
impl<T> Drop for GovernedBox<T> {
    fn drop(&mut self) {
        // SAFETY: this is the sole extraction from an initialized holder.
        let owner = unsafe { *ManuallyDrop::take(&mut self.inner) };
        drop(owner);
    }
}

fn layout_error(requested: usize) -> ResourceError {
    ResourceError::new(ResourceErrorKind::Layout, requested, 0)
}

/// Allocate a fresh, exactly sized Vec backing after its layout is admitted.
fn try_vec<T>(capacity: usize) -> Result<Vec<T>, ResourceError> {
    let layout = Layout::array::<T>(capacity).map_err(|_| layout_error(capacity))?;
    if layout.size() == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: layout is positive and valid; null is handled before construction.
    let pointer = unsafe { std::alloc::alloc(layout) }.cast::<T>();
    let Some(pointer) = std::ptr::NonNull::new(pointer) else {
        return Err(ResourceError::new(
            ResourceErrorKind::Allocation,
            layout.size(),
            0,
        ));
    };
    // SAFETY: fresh allocation, no initialized elements, original global layout.
    Ok(unsafe { Vec::from_raw_parts(pointer.as_ptr(), 0, capacity) })
}

pub(crate) fn reserve_holder<T>(
    scope: &AllocationScope,
    backing: Layout,
) -> Result<AllocationLease, ResourceError> {
    let mut lease = scope.reserve(backing)?;
    lease.merge(scope.reserve(Layout::new::<Governed<T>>())?)?;
    Ok(lease)
}

fn growth_capacity(current: usize, needed: usize) -> Result<usize, ResourceError> {
    current
        .checked_mul(2)
        .map(|doubled| doubled.max(needed))
        .ok_or_else(|| layout_error(needed))
}

enum VecStorage<T> {
    Legacy(Vec<T>),
    Governed(GovernedBox<Vec<T>>),
}

/// Positional values with sealed storage. Legacy wrapping moves the original
/// Vec unchanged; governed growth admits old and new backing simultaneously.
pub(crate) struct OwnedVec<T> {
    storage: VecStorage<T>,
}
impl<T> OwnedVec<T> {
    pub(crate) fn is_accounted_by(&self, resources: &AllocationResources) -> bool {
        matches!(&self.storage, VecStorage::Governed(value) if value.is_accounted_by(resources))
    }
    pub(crate) fn unaccounted_backing_size(&self, resources: &AllocationResources) -> usize {
        if self.is_accounted_by(resources) {
            0
        } else {
            self.backing_size()
        }
    }

    pub fn from_vec(values: Vec<T>) -> Self {
        Self {
            storage: VecStorage::Legacy(values),
        }
    }
    pub fn try_with_capacity(
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<Self, ResourceError> {
        let layout = Layout::array::<T>(capacity).map_err(|_| layout_error(capacity))?;
        let lease = reserve_holder::<Vec<T>>(scope, layout)?;
        let values = try_vec(capacity)?;
        Ok(Self {
            storage: VecStorage::Governed(GovernedBox::try_new(values, lease)?),
        })
    }
    fn vec(&self) -> &Vec<T> {
        match &self.storage {
            VecStorage::Legacy(v) => v,
            VecStorage::Governed(v) => v.payload(),
        }
    }
    fn vec_mut(&mut self) -> &mut Vec<T> {
        match &mut self.storage {
            VecStorage::Legacy(v) => v,
            VecStorage::Governed(v) => v.payload_mut(),
        }
    }
    pub fn as_slice(&self) -> &[T] {
        self.vec()
    }
    /// Element replacement cannot grow or extract the container backing.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.vec_mut()
    }
    pub fn len(&self) -> usize {
        self.vec().len()
    }
    pub fn is_empty(&self) -> bool {
        self.vec().is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.vec().capacity()
    }
    /// Fallible growth leaves the original container intact on refusal.
    pub fn try_reserve(
        &mut self,
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<(), ResourceError> {
        scope.check_cancelled()?;
        if capacity <= self.capacity() {
            return Ok(());
        }
        let new = Self::try_with_capacity(growth_capacity(self.capacity(), capacity)?, scope)?;
        let old = std::mem::replace(self, new);
        for value in old {
            self.vec_mut().push(value);
        }
        Ok(())
    }
    /// Return an unconsumed element with the bounded error when growth fails.
    pub fn try_push(
        &mut self,
        value: T,
        scope: &AllocationScope,
    ) -> Result<(), (ResourceError, T)> {
        let needed = match self.len().checked_add(1) {
            Some(n) => n,
            None => return Err((layout_error(self.len()), value)),
        };
        if let Err(error) = self.try_reserve(needed, scope) {
            return Err((error, value));
        }
        self.vec_mut().push(value);
        Ok(())
    }
    pub(crate) fn is_governed(&self) -> bool {
        matches!(self.storage, VecStorage::Governed(_))
    }
    pub(crate) fn backing_size(&self) -> usize {
        match &self.storage {
            VecStorage::Legacy(v) => v.capacity() * std::mem::size_of::<T>(),
            VecStorage::Governed(v) => v.bytes(),
        }
    }
    pub(crate) fn legacy_backing_size(&self) -> usize {
        if self.is_governed() {
            0
        } else {
            self.backing_size()
        }
    }
    pub(crate) fn push_reserved(&mut self, value: T) {
        self.vec_mut().push(value);
    }
}
impl<T: Clone> Clone for OwnedVec<T> {
    fn clone(&self) -> Self {
        Self::from_vec(self.as_slice().to_vec())
    }
}
impl<T: std::fmt::Debug> std::fmt::Debug for OwnedVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl<T> Deref for OwnedVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

/// Positional values retaining their backing owner during replacement and iteration.
#[derive(Clone, Debug)]
pub struct OwnedValues {
    values: OwnedVec<crate::Value>,
}
/// Restricted growth of a known legacy array, without a backing escape.
pub struct LegacyValuesMut<'a> {
    values: &'a mut Vec<crate::Value>,
}
impl LegacyValuesMut<'_> {
    pub fn push(&mut self, value: crate::Value) {
        self.values.push(value);
    }
}
impl OwnedValues {
    pub fn legacy_mut(&mut self) -> Option<LegacyValuesMut<'_>> {
        match &mut self.values.storage {
            VecStorage::Legacy(values) => Some(LegacyValuesMut { values }),
            VecStorage::Governed(_) => None,
        }
    }
    pub fn is_accounted_by(&self, resources: &AllocationResources) -> bool {
        self.values.is_accounted_by(resources)
    }
    pub fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        self.values.unaccounted_backing_size(resources)
            + self
                .iter()
                .map(|value| value.unaccounted_heap_size(resources))
                .sum::<usize>()
    }

    pub fn from_vec(values: Vec<crate::Value>) -> Self {
        Self {
            values: OwnedVec::from_vec(values),
        }
    }
    pub fn try_with_capacity(
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<Self, ResourceError> {
        Ok(Self {
            values: OwnedVec::try_with_capacity(capacity, scope)?,
        })
    }
    pub fn as_slice(&self) -> &[crate::Value] {
        self.values.as_slice()
    }
    pub fn as_mut_slice(&mut self) -> &mut [crate::Value] {
        self.values.as_mut_slice()
    }
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.values.capacity()
    }
    pub fn is_governed(&self) -> bool {
        self.values.is_governed()
    }
    pub fn try_reserve(
        &mut self,
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<(), ResourceError> {
        self.values.try_reserve(capacity, scope)
    }
    pub fn try_push(
        &mut self,
        value: crate::Value,
        scope: &AllocationScope,
    ) -> Result<(), (ResourceError, crate::Value)> {
        self.values.try_push(value, scope)
    }
    /// Move an element out without shrinking or releasing the backing charge.
    pub fn remove(&mut self, index: usize) -> Option<crate::Value> {
        if index < self.len() {
            Some(self.values.vec_mut().remove(index))
        } else {
            None
        }
    }
    pub fn heap_size(&self) -> usize {
        self.values.backing_size() + self.iter().map(crate::Value::heap_size).sum::<usize>()
    }
    pub fn legacy_heap_size(&self) -> usize {
        self.values.legacy_backing_size()
            + self
                .iter()
                .map(crate::Value::legacy_heap_size)
                .sum::<usize>()
    }
}
impl Deref for OwnedValues {
    type Target = [crate::Value];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl std::ops::DerefMut for OwnedValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}
impl PartialEq for OwnedValues {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl<'a> IntoIterator for &'a OwnedValues {
    type Item = &'a crate::Value;
    type IntoIter = std::slice::Iter<'a, crate::Value>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl IntoIterator for OwnedValues {
    type Item = crate::Value;
    type IntoIter = OwnedIntoIter<std::vec::IntoIter<crate::Value>>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// Consuming iterator whose backing is destroyed before its retained lease.
/// Yielded elements own their independent allocations; early drop destroys the
/// remainder without releasing the container's charge ahead of its allocation.
pub struct OwnedIntoIter<I> {
    inner: ManuallyDrop<I>,
    _lease: Option<AllocationLease>,
}
impl<I: Iterator> Iterator for OwnedIntoIter<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl<I: DoubleEndedIterator> DoubleEndedIterator for OwnedIntoIter<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}
impl<I: ExactSizeIterator> ExactSizeIterator for OwnedIntoIter<I> {}
impl<I: std::iter::FusedIterator> std::iter::FusedIterator for OwnedIntoIter<I> {}
impl<I> Drop for OwnedIntoIter<I> {
    fn drop(&mut self) {
        // SAFETY: initialized once, never extracted; manually destroy backing
        // before the reservation field's automatic drop, including unwind.
        unsafe {
            ManuallyDrop::drop(&mut self.inner);
        }
    }
}
impl<T> IntoIterator for OwnedVec<T> {
    type Item = T;
    type IntoIter = OwnedIntoIter<std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter {
        let (values, lease) = match self.storage {
            VecStorage::Legacy(v) => (v, None),
            VecStorage::Governed(v) => {
                let (v, lease) = v.into_parts();
                (v, Some(lease))
            }
        };
        OwnedIntoIter {
            inner: ManuallyDrop::new(values.into_iter()),
            _lease: lease,
        }
    }
}

enum KeyStorage {
    Legacy(Box<str>),
    Governed(GovernedBox<Box<str>>),
}
/// String key whose independent admitted ownership can outlive its map.
pub struct OwnedKey {
    storage: KeyStorage,
}
impl OwnedKey {
    pub fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        match &self.storage {
            KeyStorage::Legacy(key) => key.len(),
            KeyStorage::Governed(key) => key.unaccounted_backing_size(resources),
        }
    }

    pub fn from_box(key: Box<str>) -> Self {
        Self {
            storage: KeyStorage::Legacy(key),
        }
    }
    pub fn try_new(text: &str, scope: &AllocationScope) -> Result<Self, ResourceError> {
        let OwnedText { text, lease } =
            OwnedText::try_new(text, scope, Layout::new::<Governed<Box<str>>>())?;
        Ok(Self {
            storage: KeyStorage::Governed(GovernedBox::try_new(text, lease)?),
        })
    }
    pub fn as_str(&self) -> &str {
        match &self.storage {
            KeyStorage::Legacy(k) => k,
            KeyStorage::Governed(k) => k.payload(),
        }
    }
    pub fn heap_size(&self) -> usize {
        match &self.storage {
            KeyStorage::Legacy(k) => k.len(),
            KeyStorage::Governed(k) => k.bytes(),
        }
    }
    pub fn legacy_heap_size(&self) -> usize {
        match &self.storage {
            KeyStorage::Legacy(k) => k.len(),
            KeyStorage::Governed(_) => 0,
        }
    }
}
impl From<Box<str>> for OwnedKey {
    fn from(value: Box<str>) -> Self {
        Self::from_box(value)
    }
}
impl From<String> for OwnedKey {
    fn from(value: String) -> Self {
        Self::from_box(value.into_boxed_str())
    }
}
impl Clone for OwnedKey {
    fn clone(&self) -> Self {
        Self::from_box(Box::from(self.as_str()))
    }
}
impl From<&str> for OwnedKey {
    fn from(value: &str) -> Self {
        Self::from_box(Box::from(value))
    }
}
impl Deref for OwnedKey {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl AsRef<str> for OwnedKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl std::borrow::Borrow<str> for OwnedKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl PartialEq for OwnedKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for OwnedKey {}
impl PartialOrd for OwnedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OwnedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl std::hash::Hash for OwnedKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
impl std::fmt::Display for OwnedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}
impl std::fmt::Debug for OwnedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

type ValueMap = indexmap::IndexMap<OwnedKey, crate::Value>;
struct MapPayload {
    map: ValueMap,
    capacity: usize,
}
enum MapStorage {
    Legacy(Box<ValueMap>),
    Governed(GovernedBox<MapPayload>),
}
/// Ordered map with independently owned keys and values. Governing a new map
/// reserves complete pinned table/entry bounds and its holder before allocation.
pub struct OwnedMap {
    storage: MapStorage,
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment - 1)
        .map(|n| n & !(alignment - 1))
}
/// Bound for locked indexmap 2.13.0's three-field Bucket and hashbrown 0.16.1's
/// usize index table. Deliberate slack stays admitted through backing release.
pub(crate) fn map_backing_layout<K, V>(capacity: usize) -> Result<Layout, ResourceError> {
    let checked = || -> Option<Layout> {
        let word = std::mem::size_of::<usize>();
        let alignment = std::mem::align_of::<usize>()
            .max(std::mem::align_of::<K>())
            .max(std::mem::align_of::<V>());
        let stride = align_up(
            word.checked_add(std::mem::size_of::<K>())?
                .checked_add(std::mem::size_of::<V>())?
                .checked_add(3usize.checked_mul(alignment - 1)?)?,
            alignment,
        )?;
        let buckets = match capacity {
            0 => 0,
            1..=3 => 4,
            4..=7 => 8,
            8..=14 => 16,
            n => (n.checked_mul(8)? / 7).checked_next_power_of_two()?,
        };
        let table = if buckets == 0 {
            0
        } else {
            align_up(
                buckets.checked_mul(word)?,
                std::mem::align_of::<usize>().max(16),
            )?
            .checked_add(buckets)?
            .checked_add(16)?
        };
        Layout::from_size_align(
            capacity.checked_mul(stride)?.checked_add(table)?,
            alignment.max(16),
        )
        .ok()
    };
    checked().ok_or_else(|| layout_error(capacity))
}
impl OwnedMap {
    pub fn unaccounted_heap_size(&self, resources: &AllocationResources) -> usize {
        let backing = match &self.storage {
            MapStorage::Legacy(map) => {
                std::mem::size_of::<ValueMap>()
                    + crate::value::indexmap_backing_size::<OwnedKey>(map.capacity())
            }
            MapStorage::Governed(map) => map.unaccounted_backing_size(resources),
        };
        backing
            + self
                .iter()
                .map(|(key, value)| {
                    key.unaccounted_heap_size(resources) + value.unaccounted_heap_size(resources)
                })
                .sum::<usize>()
    }

    /// Build legacy keys directly as OwnedKey; this moves the map into its one
    /// existing-shape Box without rebuilding an old-key map into another table.
    pub fn from_map(map: ValueMap) -> Self {
        Self {
            storage: MapStorage::Legacy(Box::new(map)),
        }
    }
    pub fn try_with_capacity(
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<Self, ResourceError> {
        let backing = map_backing_layout::<OwnedKey, crate::Value>(capacity)?;
        let lease = reserve_holder::<MapPayload>(scope, backing)?;
        let mut map = ValueMap::new();
        if map.try_reserve_exact(capacity).is_err() {
            // The index table may already exist when entries allocation fails.
            // Drop it before releasing the complete reservation on this return.
            drop(map);
            return Err(ResourceError::new(
                ResourceErrorKind::Allocation,
                backing.size(),
                0,
            ));
        }
        Ok(Self {
            storage: MapStorage::Governed(GovernedBox::try_new(
                MapPayload { map, capacity },
                lease,
            )?),
        })
    }
    pub fn as_map(&self) -> &ValueMap {
        match &self.storage {
            MapStorage::Legacy(m) => m,
            MapStorage::Governed(m) => &m.payload().map,
        }
    }
    fn map_mut(&mut self) -> &mut ValueMap {
        match &mut self.storage {
            MapStorage::Legacy(m) => m,
            MapStorage::Governed(m) => &mut m.payload_mut().map,
        }
    }
    pub fn len(&self) -> usize {
        self.as_map().len()
    }
    pub fn is_empty(&self) -> bool {
        self.as_map().is_empty()
    }
    /// Admitted logical capacity, never library table slack, for governed maps.
    pub fn capacity(&self) -> usize {
        match &self.storage {
            MapStorage::Legacy(m) => m.capacity(),
            MapStorage::Governed(m) => m.payload().capacity,
        }
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut crate::Value> {
        self.map_mut().get_mut(key)
    }
    pub fn try_reserve(
        &mut self,
        capacity: usize,
        scope: &AllocationScope,
    ) -> Result<(), ResourceError> {
        scope.check_cancelled()?;
        if capacity <= self.capacity() {
            return Ok(());
        }
        let new = Self::try_with_capacity(growth_capacity(self.capacity(), capacity)?, scope)?;
        let old = std::mem::replace(self, new);
        for (key, value) in old {
            self.map_mut().insert(key, value);
        }
        Ok(())
    }
    /// Replace an existing value without growth, or admit a fresh larger map
    /// before moving entries. Failure returns the unconsumed key and value.
    pub fn try_insert(
        &mut self,
        key: OwnedKey,
        value: crate::Value,
        scope: &AllocationScope,
    ) -> Result<Option<crate::Value>, (ResourceError, OwnedKey, crate::Value)> {
        if let Err(error) = scope.check_cancelled() {
            return Err((error, key, value));
        }
        if let Some(existing) = self.get_mut(key.as_str()) {
            return Ok(Some(std::mem::replace(existing, value)));
        }
        let needed = match self.len().checked_add(1) {
            Some(n) => n,
            None => return Err((layout_error(self.len()), key, value)),
        };
        if let Err(error) = self.try_reserve(needed, scope) {
            return Err((error, key, value));
        }
        Ok(self.map_mut().insert(key, value))
    }
    pub fn heap_size(&self) -> usize {
        let backing = match &self.storage {
            MapStorage::Legacy(m) => {
                std::mem::size_of::<ValueMap>()
                    + crate::value::indexmap_backing_size::<OwnedKey>(m.capacity())
            }
            MapStorage::Governed(m) => m.bytes(),
        };
        backing
            + self
                .as_map()
                .iter()
                .map(|(key, value)| key.heap_size() + value.heap_size())
                .sum::<usize>()
    }
    pub fn legacy_heap_size(&self) -> usize {
        let backing = match &self.storage {
            MapStorage::Legacy(m) => {
                std::mem::size_of::<ValueMap>()
                    + crate::value::indexmap_backing_size::<OwnedKey>(m.capacity())
            }
            MapStorage::Governed(_) => 0,
        };
        backing
            + self
                .as_map()
                .iter()
                .map(|(key, value)| key.legacy_heap_size() + value.legacy_heap_size())
                .sum::<usize>()
    }
}
/// Restricted mutation of a known legacy map. The borrowed backing cannot escape.
pub struct LegacyMapMut<'a> {
    map: &'a mut ValueMap,
}
impl LegacyMapMut<'_> {
    pub fn insert(&mut self, key: OwnedKey, value: crate::Value) -> Option<crate::Value> {
        self.map.insert(key, value)
    }
    pub fn get_or_insert_with(
        &mut self,
        key: OwnedKey,
        make: impl FnOnce() -> crate::Value,
    ) -> &mut crate::Value {
        self.map.entry(key).or_insert_with(make)
    }
}
impl OwnedMap {
    pub fn legacy_mut(&mut self) -> Option<LegacyMapMut<'_>> {
        match &mut self.storage {
            MapStorage::Legacy(map) => Some(LegacyMapMut { map }),
            MapStorage::Governed(_) => None,
        }
    }
    pub fn shift_remove(&mut self, key: &str) -> Option<crate::Value> {
        self.map_mut().shift_remove(key)
    }
}
impl Deref for OwnedMap {
    type Target = ValueMap;
    fn deref(&self) -> &ValueMap {
        self.as_map()
    }
}
impl PartialEq for OwnedMap {
    fn eq(&self, other: &Self) -> bool {
        self.as_map() == other.as_map()
    }
}
impl<'a> IntoIterator for &'a OwnedMap {
    type Item = (&'a OwnedKey, &'a crate::Value);
    type IntoIter = indexmap::map::Iter<'a, OwnedKey, crate::Value>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl Clone for OwnedMap {
    fn clone(&self) -> Self {
        Self::from_map(self.as_map().clone())
    }
}
impl std::fmt::Debug for OwnedMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_map().fmt(f)
    }
}
impl IntoIterator for OwnedMap {
    type Item = (OwnedKey, crate::Value);
    type IntoIter = OwnedIntoIter<indexmap::map::IntoIter<OwnedKey, crate::Value>>;
    fn into_iter(self) -> Self::IntoIter {
        let (map, lease) = match self.storage {
            MapStorage::Legacy(m) => (*m, None),
            MapStorage::Governed(m) => {
                let (payload, lease) = m.into_parts();
                (payload.map, Some(lease))
            }
        };
        OwnedIntoIter {
            inner: ManuallyDrop::new(map.into_iter()),
            _lease: lease,
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<OwnedValues>() == std::mem::size_of::<Vec<crate::Value>>());
    assert!(std::mem::align_of::<OwnedValues>() == std::mem::align_of::<Vec<crate::Value>>());
    assert!(std::mem::size_of::<OwnedKey>() == std::mem::size_of::<Box<str>>());
    assert!(std::mem::size_of::<OwnedMap>() <= std::mem::size_of::<Vec<crate::Value>>());
};

/// Fixed-cardinality resource failure, without record values or copied strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceErrorKind {
    Budget,
    Allocation,
    Layout,
    DiskQuota,
    DescriptorQuota,
    Cancelled,
    Storage,
    Readback,
    DeliveryPoisoned,
    Finalized,
    Authority,
}

/// Bounded evidence for admission and delivery errors. Field is a schema index;
/// offset identifies a byte without copying any author-provided value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceError {
    pub kind: ResourceErrorKind,
    pub requested: usize,
    pub available: usize,
    pub field: Option<usize>,
    pub offset: Option<u64>,
}
impl ResourceError {
    /// Construct allocation-free diagnostic evidence.
    pub const fn new(kind: ResourceErrorKind, requested: usize, available: usize) -> Self {
        Self {
            kind,
            requested,
            available,
            field: None,
            offset: None,
        }
    }
}
impl std::fmt::Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resource {:?}: requested {}, available {}",
            self.kind, self.requested, self.available
        )
    }
}
impl std::error::Error for ResourceError {}
/// Stable, allocation-free identity within a resource authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerId(pub u64);

fn next_identity(counter: &AtomicU64) -> Result<u64, ResourceError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .map_err(|_| ResourceError::new(ResourceErrorKind::Authority, 1, 0))
}

/// Provider boundary. Reserve/release must be synchronized and account the
/// entire requested layout before allocation. No callback sees a destination.
pub trait AllocationAuthority: Send + Sync {
    /// Identity of the aggregate ledger. Adapters sharing a ledger return the
    /// same identity; owner identifiers travel with tokens, not a second tally.
    fn identity(&self) -> usize {
        std::ptr::from_ref(self).cast::<()>() as usize
    }
    /// Check cancellation and admit the whole layout atomically. On success,
    /// return its unique lease; on failure retain no new charge.
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationLease, ResourceError>;
    fn release(&self, owner: OwnerId, bytes: usize);
    fn check_cancelled(&self) -> Result<(), ResourceError>;
}

/// Unique reservation. Moving a value moves this token; copying needs another.
pub struct AllocationLease {
    authority: Arc<dyn AllocationAuthority>,
    owner: OwnerId,
    bytes: usize,
    allocation_id: u64,
}
impl AllocationLease {
    /// Compare live issuing and executing ledgers without mutating either grant.
    pub fn is_accounted_by(&self, resources: &AllocationResources) -> bool {
        self.authority.identity() == resources.authority.identity()
    }

    /// Provider-only accounting boundary: caller must have admitted these bytes
    /// before constructing the token. No allocation occurs here. Identity
    /// exhaustion returns a bounded error after releasing those admitted bytes.
    pub fn admitted(
        authority: Arc<dyn AllocationAuthority>,
        owner: OwnerId,
        bytes: usize,
    ) -> Result<Self, ResourceError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self::admitted_with_counter(authority, owner, bytes, &NEXT)
    }
    fn admitted_with_counter(
        authority: Arc<dyn AllocationAuthority>,
        owner: OwnerId,
        bytes: usize,
        counter: &AtomicU64,
    ) -> Result<Self, ResourceError> {
        match next_identity(counter) {
            Ok(allocation_id) => Ok(Self {
                authority,
                owner,
                bytes,
                allocation_id,
            }),
            Err(error) => {
                authority.release(owner, bytes);
                Err(error)
            }
        }
    }
    /// Reservation identity, retained by moves and transfer. A split obtains a
    /// new identity; merge retires the consumed identity into this reservation.
    pub fn allocation_id(&self) -> u64 {
        self.allocation_id
    }
    pub fn requested_bytes(&self) -> usize {
        self.bytes
    }
    pub fn owner(&self) -> OwnerId {
        self.owner
    }
    /// Move the charge to another scope of this same authority, without a
    /// release/reacquire window or a second allocation.
    pub fn transfer(&mut self, scope: &AllocationScope) -> Result<(), ResourceError> {
        if self.authority.identity() != scope.resources.authority.identity() {
            return Err(ResourceError::new(
                ResourceErrorKind::Authority,
                self.bytes,
                0,
            ));
        }
        self.owner = scope.owner;
        Ok(())
    }
    /// Partition ownership without changing total usage.
    pub fn split(&mut self, bytes: usize) -> Result<Self, ResourceError> {
        if bytes > self.bytes {
            return Err(ResourceError::new(
                ResourceErrorKind::Authority,
                bytes,
                self.bytes,
            ));
        }
        // Reserve a distinct identity before changing either ownership amount.
        // The same identity source as admitted reservations is used below.
        let mut other = Self::admitted(self.authority.clone(), self.owner, 0)?;
        self.bytes -= bytes;
        other.bytes = bytes;
        Ok(other)
    }
    /// Combine ownership only when authority and owner are identical.
    pub fn merge(&mut self, mut other: Self) -> Result<(), ResourceError> {
        if self.authority.identity() != other.authority.identity() || self.owner != other.owner {
            return Err(ResourceError::new(
                ResourceErrorKind::Authority,
                other.bytes,
                0,
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Layout, other.bytes, 0))?;
        other.bytes = 0;
        Ok(())
    }
}
impl Drop for AllocationLease {
    fn drop(&mut self) {
        self.authority.release(self.owner, self.bytes);
    }
}

/// Cloneable handle to an explicitly finite authority; never defaults to unlimited.
#[derive(Clone)]
pub struct AllocationResources {
    authority: Arc<dyn AllocationAuthority>,
}
impl AllocationResources {
    pub fn new(authority: Arc<dyn AllocationAuthority>) -> Self {
        Self { authority }
    }
    /// Aggregate ledger identity, including through provider adapters.
    pub fn identity(&self) -> usize {
        self.authority.identity()
    }
    /// Delegate admission while preserving an existing caller's owner identity.
    /// The provider checks cancellation and returns the same ledger's lease.
    pub fn reserve(
        &self,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationLease, ResourceError> {
        self.authority.clone().try_reserve(owner, layout)
    }
    /// Scope is inline, including identity and authority handle, with no heap
    /// allocation per writer. Its containing owner must account retained storage.
    pub fn scope(&self) -> Result<AllocationScope, ResourceError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        self.authority.check_cancelled()?;
        let owner = next_identity(&NEXT)?;
        Ok(AllocationScope {
            resources: self.clone(),
            owner: OwnerId(owner),
        })
    }
}

/// Inline allocation identity. Cloning grants no bytes and allocates nothing.
#[derive(Clone)]
pub struct AllocationScope {
    resources: AllocationResources,
    owner: OwnerId,
}
impl AllocationScope {
    pub fn owner(&self) -> OwnerId {
        self.owner
    }
    pub fn reserve(&self, layout: Layout) -> Result<AllocationLease, ResourceError> {
        self.resources.reserve(self.owner, layout)
    }
    pub fn check_cancelled(&self) -> Result<(), ResourceError> {
        self.resources.authority.check_cancelled()
    }
}

/// Allocation-free construction evidence for the sealed shared backing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SharedAllocationError {
    Layout,
    Allocation,
}

fn shared_layout(payload: Layout) -> Result<Layout, SharedAllocationError> {
    Layout::new::<AtomicUsize>()
        .extend(payload)
        .map(|(layout, _)| layout.pad_to_align())
        .map_err(|_| SharedAllocationError::Layout)
}

/// Sized shared backing whose allocation is freed before its payload is dropped.
///
/// Every alias consumes its library handle through the same final-owner protocol.
/// The payload can therefore own the charge for this backing: that charge stays
/// live through backing deallocation. No raw handle or shared mutation escapes.
pub(crate) struct SharedAllocation<T> {
    inner: ManuallyDrop<triomphe::Arc<T>>,
}

impl<T> SharedAllocation<T> {
    /// Complete backing layout for the pinned library's atomic count and payload.
    /// Separately allocated payload storage is additional to this checked layout.
    pub(crate) fn layout() -> Result<Layout, SharedAllocationError> {
        shared_layout(Layout::new::<T>())
    }

    /// Allocate one sized backing, returning bounded evidence on refusal.
    ///
    /// The caller must already have admitted `Self::layout()` and any separately
    /// owned payload storage. The payload must retain those charges and destroy
    /// its owned storage before releasing them. Failure disposes of the payload;
    /// successful aliases retain it until the last alias is dropped.
    pub(crate) fn try_new(payload: T) -> Result<Self, SharedAllocationError> {
        Self::layout()?;
        triomphe::Arc::try_new(payload)
            .map(|inner| Self {
                inner: ManuallyDrop::new(inner),
            })
            .map_err(|_| SharedAllocationError::Allocation)
    }
}

impl<T> Clone for SharedAllocation<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ManuallyDrop::new(triomphe::Arc::clone(&self.inner)),
        }
    }
}

impl<T> Deref for SharedAllocation<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> Drop for SharedAllocation<T> {
    fn drop(&mut self) {
        // SAFETY: inner is initialized at construction, never taken elsewhere,
        // and ManuallyDrop suppresses a second drop after this sole extraction.
        let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
        if let Some(unique) = triomphe::Arc::into_unique(inner) {
            // into_inner deallocates backing before returning the moved payload.
            // If payload destruction unwinds, its fields still own their charges.
            drop(triomphe::UniqueArc::into_inner(unique));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AllocationAuthority, AllocationLease, AllocationResources, OwnerId, ResourceError,
        ResourceErrorKind,
    };
    use super::{SharedAllocation, SharedAllocationError, shared_layout};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    #[test]
    fn shared_storage_identity_matches_only_original_backing_without_allocating() {
        use super::SharedStorage;
        let authority = std::sync::Arc::new(FiniteAuthority {
            used: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            limit: 4096,
        });
        let scope = AllocationResources::new(authority.clone()).scope().unwrap();
        let governed = SharedStorage::try_new(17_u64, &scope).unwrap();
        let legacy = SharedStorage::from_arc(std::sync::Arc::new(17_u64));
        let observation = Observation::default();
        let guard = Observe::start(&observation);
        observation.deny_allocations.store(true, SeqCst);
        let governed_id = governed.try_identity(&scope).unwrap();
        let legacy_id = legacy.try_identity(&scope).unwrap();
        assert!(governed_id.matches(&governed.clone()));
        assert!(legacy_id.matches(&legacy.clone()));
        assert!(!governed_id.matches(&legacy));
        assert!(!legacy_id.matches(&governed));
        observation.deny_allocations.store(false, SeqCst);
        drop(guard);
        assert_eq!(observation.allocations.load(SeqCst), 0);
        let other = SharedStorage::try_new(17_u64, &scope).unwrap();
        assert!(!governed_id.matches(&other));
        drop(governed);
        drop(legacy);
        let fresh = SharedStorage::from_arc(std::sync::Arc::new(17_u64));
        assert!(!legacy_id.matches(&fresh));
        let fresh_governed = SharedStorage::try_new(17_u64, &scope).unwrap();
        assert!(!governed_id.matches(&fresh_governed));
        drop((other, fresh_governed, governed_id, legacy_id));
        assert_eq!(authority.used.load(SeqCst), 0);
    }

    #[test]
    fn shared_storage_identity_legacy_denial_precedes_retention() {
        let original = std::sync::Arc::new(crate::Schema::new(vec![]));
        let storage = super::SharedStorage::from_arc(original.clone());
        let required = storage.estimated_outer_heap_size();
        assert!(required > 0);
        let authority = std::sync::Arc::new(FiniteAuthority {
            used: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            limit: required - 1,
        });
        let scope = AllocationResources::new(authority.clone()).scope().unwrap();
        let observation = Observation::default();
        let guard = Observe::start(&observation);
        let error = storage.try_identity(&scope).err().unwrap();
        drop(guard);
        assert_eq!(error.kind, ResourceErrorKind::Budget);
        assert_eq!(error.requested, required);
        assert_eq!(error.available, required - 1);
        assert_eq!(std::sync::Arc::weak_count(&original), 0);
        assert_eq!(observation.allocations.load(SeqCst), 0);
        assert_eq!(authority.used.load(SeqCst), 0);
    }

    #[test]
    fn shared_storage_identity_children_drop_and_final_weak_deallocates_before_release() {
        struct WatchedAuthority(std::sync::Arc<Observation>);
        impl AllocationAuthority for WatchedAuthority {
            fn try_reserve(
                self: std::sync::Arc<Self>,
                owner: OwnerId,
                layout: Layout,
            ) -> Result<AllocationLease, ResourceError> {
                self.0.used.fetch_add(layout.size(), SeqCst);
                AllocationLease::admitted(self, owner, layout.size())
            }
            fn release(&self, _: OwnerId, bytes: usize) {
                self.0.used.fetch_sub(bytes, SeqCst);
                self.0.released.store(self.0.tick(), SeqCst);
            }
            fn check_cancelled(&self) -> Result<(), ResourceError> {
                Ok(())
            }
        }
        for (governed, metadata) in [(false, false), (false, true), (true, false), (true, true)] {
            let observation = std::sync::Arc::new(Observation::new());
            let authority = std::sync::Arc::new(WatchedAuthority(observation.clone()));
            let scope = AllocationResources::new(authority).scope().unwrap();
            let schema = crate::Schema::with_metadata(
                vec!["long-column-name-retained-by-schema-only".into()],
                vec![Some(crate::FieldMetadata::SourceCorrelation {
                    source_field: "long-metadata-name-retained-by-schema-only".into(),
                })],
            );
            let guard = Observe::start(&observation);
            let storage = if governed {
                super::SharedStorage::try_new(schema, &scope).unwrap()
            } else {
                super::SharedStorage::from_arc(std::sync::Arc::new(schema))
            };
            // Watch the column vector separately from the outer backing.
            let child = if metadata {
                let Some(crate::FieldMetadata::SourceCorrelation { source_field }) =
                    storage.field_metadata(0)
                else {
                    panic!("metadata")
                };
                source_field.as_ptr() as usize
            } else {
                storage.columns().as_ptr() as usize
            };
            observation.payload_storage.store(child, SeqCst);
            let identity = storage.try_identity(&scope).unwrap();
            let charge = observation.used.load(SeqCst);
            assert_eq!(charge, observation.backing_size.load(SeqCst));
            drop(storage);
            assert!(observation.payload_deallocated.load(SeqCst) > 0);
            if governed {
                assert!(observation.deallocated.load(SeqCst) > 0);
                assert_eq!(observation.used.load(SeqCst), 0);
            } else {
                assert_eq!(observation.deallocated.load(SeqCst), 0);
                assert_eq!(observation.used.load(SeqCst), charge);
            }
            drop(identity);
            assert_eq!(observation.used_on_backing_dealloc.load(SeqCst), charge);
            assert!(observation.deallocated.load(SeqCst) < observation.released.load(SeqCst));
            assert_eq!(observation.used.load(SeqCst), 0);
            drop(guard);
        }
    }

    #[test]
    fn resource_diagnostic_is_neutral_and_keeps_bounded_evidence() {
        for (kind, requested, available, expected) in [
            (
                ResourceErrorKind::Budget,
                4096,
                1024,
                "resource Budget: requested 4096, available 1024",
            ),
            (
                ResourceErrorKind::DescriptorQuota,
                1,
                0,
                "resource DescriptorQuota: requested 1, available 0",
            ),
        ] {
            let mut error = ResourceError::new(kind, requested, available);
            error.field = Some(7);
            error.offset = Some(42);
            assert_eq!(error.to_string(), expected);
            assert_eq!(error.kind, kind);
            assert_eq!(error.requested, requested);
            assert_eq!(error.available, available);
            assert_eq!(error.field, Some(7));
            assert_eq!(error.offset, Some(42));
        }
    }

    #[derive(Default)]
    struct Observation {
        allocations: AtomicUsize,
        deny_allocations: AtomicBool,
        capture_backing: AtomicBool,
        backing: AtomicUsize,
        backing_size: AtomicUsize,
        backing_align: AtomicUsize,
        deallocated: AtomicUsize,
        backing_deallocations: AtomicUsize,
        used_on_backing_dealloc: AtomicUsize,
        payload_storage: AtomicUsize,
        payload_deallocated: AtomicUsize,
        used_on_payload_dealloc: AtomicUsize,
        payload_dropped: AtomicUsize,
        payload_drops: AtomicUsize,
        released: AtomicUsize,
        releases: AtomicUsize,
        used: AtomicUsize,
        clock: AtomicUsize,
    }
    impl Observation {
        fn new() -> Self {
            Self {
                capture_backing: AtomicBool::new(true),
                ..Self::default()
            }
        }
        fn tick(&self) -> usize {
            self.clock.fetch_add(1, SeqCst) + 1
        }
    }
    thread_local! {
        static OBSERVATION: Cell<*const Observation> = const { Cell::new(std::ptr::null()) };
    }
    struct Observe<'a>(&'a Observation);
    impl<'a> Observe<'a> {
        fn start(observation: &'a Observation) -> Self {
            OBSERVATION.with(|slot| assert!(slot.replace(observation).is_null()));
            Self(observation)
        }
    }
    impl Drop for Observe<'_> {
        fn drop(&mut self) {
            OBSERVATION.with(|slot| assert!(std::ptr::eq(slot.replace(std::ptr::null()), self.0)));
        }
    }
    fn observe(f: impl FnOnce(&Observation)) {
        let _ = OBSERVATION.try_with(|slot| {
            let pointer = slot.get();
            if !pointer.is_null() {
                // SAFETY: Observe borrows this observation until the thread's
                // pointer is cleared. Its callback uses only atomic scalars.
                f(unsafe { &*pointer });
            }
        });
    }
    struct Observer;
    // SAFETY: all allocation/deallocation uses System and the original layouts.
    // The scoped observer performs no allocation and never unwinds.
    unsafe impl GlobalAlloc for Observer {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let mut refuse = false;
            observe(|state| {
                state.allocations.fetch_add(1, SeqCst);
                refuse = state.deny_allocations.load(SeqCst);
            });
            if refuse {
                return std::ptr::null_mut();
            }
            // SAFETY: caller supplies the GlobalAlloc layout contract.
            let pointer = unsafe { System.alloc(layout) };
            observe(|state| {
                if !pointer.is_null() && state.capture_backing.swap(false, SeqCst) {
                    state.backing.store(pointer as usize, SeqCst);
                    state.backing_size.store(layout.size(), SeqCst);
                    state.backing_align.store(layout.align(), SeqCst);
                }
            });
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: pointer and layout came from System through this allocator.
            unsafe { System.dealloc(pointer, layout) };
            observe(|state| {
                if state.backing.load(SeqCst) == pointer as usize
                    && state.deallocated.load(SeqCst) == 0
                {
                    state.backing_deallocations.fetch_add(1, SeqCst);
                    state
                        .used_on_backing_dealloc
                        .store(state.used.load(SeqCst), SeqCst);
                    state.deallocated.store(state.tick(), SeqCst);
                }
                if state.payload_storage.load(SeqCst) == pointer as usize
                    && state.payload_deallocated.load(SeqCst) == 0
                {
                    state
                        .used_on_payload_dealloc
                        .store(state.used.load(SeqCst), SeqCst);
                    state.payload_deallocated.store(state.tick(), SeqCst);
                }
            });
        }
    }
    #[global_allocator]
    static ALLOCATOR: Observer = Observer;

    struct FiniteAuthority {
        used: AtomicUsize,
        cancelled: AtomicBool,
        limit: usize,
    }
    impl AllocationAuthority for FiniteAuthority {
        fn try_reserve(
            self: std::sync::Arc<Self>,
            owner: OwnerId,
            layout: Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.check_cancelled()?;
            self.used
                .fetch_update(SeqCst, SeqCst, |used| {
                    used.checked_add(layout.size())
                        .filter(|next| *next <= self.limit)
                })
                .map_err(|used| {
                    ResourceError::new(
                        ResourceErrorKind::Budget,
                        layout.size(),
                        self.limit.saturating_sub(used),
                    )
                })?;
            AllocationLease::admitted(self, owner, layout.size())
        }
        fn release(&self, _: OwnerId, bytes: usize) {
            self.used.fetch_sub(bytes, SeqCst);
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            if self.cancelled.load(SeqCst) {
                Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn allocation_scope_algebra_conserves_bytes_without_allocating() {
        let authority = std::sync::Arc::new(FiniteAuthority {
            used: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            limit: 64,
        });
        let resources = AllocationResources::new(authority.clone());
        let a = resources.scope().unwrap();
        let b = resources.scope().unwrap();
        let foreign = AllocationResources::new(std::sync::Arc::new(FiniteAuthority {
            used: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            limit: 64,
        }))
        .scope()
        .unwrap();
        let observation = Observation::default();
        observation.deny_allocations.store(true, SeqCst);
        let guard = Observe::start(&observation);
        let mut lease = a.reserve(Layout::new::<[u8; 64]>()).unwrap();
        let original_id = lease.allocation_id();
        let mut split = lease.split(16).unwrap();
        assert_ne!(split.allocation_id(), original_id);
        assert_eq!(lease.requested_bytes(), 48);
        assert_eq!(authority.used.load(SeqCst), 64);
        assert_eq!(
            lease.split(49).err().unwrap().kind,
            ResourceErrorKind::Authority
        );
        assert_eq!(
            split.transfer(&foreign).unwrap_err().kind,
            ResourceErrorKind::Authority
        );
        assert_eq!(split.owner(), a.owner());
        split.transfer(&b).unwrap();
        split.transfer(&a).unwrap();
        lease.merge(split).unwrap();
        assert_eq!(lease.requested_bytes(), 64);
        assert_eq!(lease.allocation_id(), original_id);
        assert_eq!(
            a.reserve(Layout::new::<u8>()).err().unwrap().kind,
            ResourceErrorKind::Budget
        );
        drop(lease);
        assert_eq!(authority.used.load(SeqCst), 0);
        authority.cancelled.store(true, SeqCst);
        assert_eq!(
            resources.scope().err().unwrap().kind,
            ResourceErrorKind::Cancelled
        );
        assert_eq!(
            a.reserve(Layout::new::<u8>()).err().unwrap().kind,
            ResourceErrorKind::Cancelled
        );
        assert_eq!(
            super::next_identity(&std::sync::atomic::AtomicU64::new(u64::MAX))
                .unwrap_err()
                .kind,
            ResourceErrorKind::Authority
        );
        drop(guard);
        assert_eq!(observation.allocations.load(SeqCst), 0);
    }

    #[test]
    fn shared_storage_identity_exhaustion_releases_admitted_bytes() {
        let authority = std::sync::Arc::new(FiniteAuthority {
            used: AtomicUsize::new(8),
            cancelled: AtomicBool::new(false),
            limit: 64,
        });
        let counter = std::sync::atomic::AtomicU64::new(u64::MAX);
        assert_eq!(
            AllocationLease::admitted_with_counter(authority.clone(), OwnerId(1), 8, &counter)
                .err()
                .unwrap()
                .kind,
            ResourceErrorKind::Authority
        );
        assert_eq!(authority.used.load(SeqCst), 0);
    }

    struct Reservation<'a> {
        state: &'a Observation,
        bytes: usize,
    }
    fn reserve(state: &Observation, bytes: usize, limit: usize) -> Option<Reservation<'_>> {
        if bytes > limit {
            return None;
        }
        assert_eq!(state.used.swap(bytes, SeqCst), 0);
        Some(Reservation { state, bytes })
    }
    impl Drop for Reservation<'_> {
        fn drop(&mut self) {
            self.state.used.fetch_sub(self.bytes, SeqCst);
            self.state.releases.fetch_add(1, SeqCst);
            self.state.released.store(self.state.tick(), SeqCst);
        }
    }
    struct Payload<'a> {
        value: usize,
        bytes: Box<[u8; 64]>,
        reservation: Reservation<'a>,
        panic_on_drop: bool,
    }
    impl Drop for Payload<'_> {
        fn drop(&mut self) {
            let state = self.reservation.state;
            state.payload_drops.fetch_add(1, SeqCst);
            state.payload_dropped.store(state.tick(), SeqCst);
            assert!(!self.panic_on_drop, "injected payload destructor panic");
        }
    }

    fn payload(state: &Observation, panic_on_drop: bool) -> Payload<'_> {
        let charge = SharedAllocation::<Payload<'_>>::layout().unwrap().size() + 64;
        let reservation = reserve(state, charge, charge).unwrap();
        let bytes = Box::new([17; 64]);
        state.payload_storage.store(bytes.as_ptr() as usize, SeqCst);
        Payload {
            value: 42,
            bytes,
            reservation,
            panic_on_drop,
        }
    }

    fn assert_disposed(state: &Observation, charge: usize) {
        assert_eq!(state.used.load(SeqCst), 0);
        assert_eq!(state.backing_deallocations.load(SeqCst), 1);
        assert_eq!(state.payload_drops.load(SeqCst), 1);
        assert_eq!(state.releases.load(SeqCst), 1);
        assert_eq!(state.used_on_backing_dealloc.load(SeqCst), charge);
        assert_eq!(state.used_on_payload_dealloc.load(SeqCst), charge);
        let backing = state.deallocated.load(SeqCst);
        let payload = state.payload_dropped.load(SeqCst);
        let bytes = state.payload_deallocated.load(SeqCst);
        let released = state.released.load(SeqCst);
        assert!(backing > 0 && backing < payload && payload < bytes && bytes < released);
    }

    #[test]
    fn shared_allocation_tracer() {
        let state = Observation::new();
        let payload = payload(&state, false);
        let charge = state.used.load(SeqCst);
        let _observe = Observe::start(&state);
        let original = SharedAllocation::try_new(payload).unwrap();
        let alias = original.clone();
        assert_eq!(alias.value, 42);
        assert_eq!(alias.bytes.as_slice(), &[17; 64]);
        assert_eq!(state.allocations.load(SeqCst), 1);
        drop(original);
        assert_eq!(state.used.load(SeqCst), charge);
        assert_eq!(state.payload_dropped.load(SeqCst), 0);
        drop(alias);
        assert_disposed(&state, charge);
    }

    #[test]
    fn reservation_refusal_precedes_shared_allocation() {
        let state = Observation::new();
        let required = SharedAllocation::<Payload<'_>>::layout().unwrap().size() + 64;
        let _observe = Observe::start(&state);
        let reservation = reserve(&state, required, required - 1);
        assert!(reservation.is_none());
        assert_eq!(state.allocations.load(SeqCst), 0);
        assert_eq!(state.used.load(SeqCst), 0);
        assert_eq!(state.releases.load(SeqCst), 0);
    }

    #[test]
    fn actual_allocator_refusal_releases_payload_without_allocating_error() {
        let state = Observation::new();
        let payload = payload(&state, false);
        let charge = state.used.load(SeqCst);
        let _observe = Observe::start(&state);
        state.deny_allocations.store(true, SeqCst);
        let result = SharedAllocation::try_new(payload);
        state.deny_allocations.store(false, SeqCst);
        assert!(matches!(result, Err(SharedAllocationError::Allocation)));
        assert_eq!(state.allocations.load(SeqCst), 1);
        assert_eq!(state.backing.load(SeqCst), 0);
        assert_eq!(state.deallocated.load(SeqCst), 0);
        assert_eq!(state.payload_drops.load(SeqCst), 1);
        assert!(state.payload_deallocated.load(SeqCst) > 0);
        assert!(state.payload_deallocated.load(SeqCst) < state.released.load(SeqCst));
        assert_eq!(state.used_on_payload_dealloc.load(SeqCst), charge);
        assert_eq!(state.releases.load(SeqCst), 1);
        assert_eq!(state.used.load(SeqCst), 0);
    }

    #[test]
    fn shared_clone_succeeds_with_allocation_disabled() {
        let state = Observation::new();
        let payload = payload(&state, false);
        let charge = state.used.load(SeqCst);
        let _observe = Observe::start(&state);
        let original = SharedAllocation::try_new(payload).unwrap();
        state.allocations.store(0, SeqCst);
        state.deny_allocations.store(true, SeqCst);
        let first = original.clone();
        let second = first.clone();
        drop(original);
        drop(first);
        state.deny_allocations.store(false, SeqCst);
        assert_eq!(state.allocations.load(SeqCst), 0);
        assert_eq!(second.value, 42);
        assert_eq!(state.used.load(SeqCst), charge);
        assert_eq!(state.deallocated.load(SeqCst), 0);
        drop(second);
        assert_disposed(&state, charge);
    }

    fn assert_layout<T>(value: T) {
        let state = Observation::new();
        let expected = SharedAllocation::<T>::layout().unwrap();
        let _observe = Observe::start(&state);
        let shared = SharedAllocation::try_new(value).unwrap();
        assert_eq!(state.allocations.load(SeqCst), 1);
        assert_eq!(state.backing_size.load(SeqCst), expected.size());
        assert_eq!(state.backing_align.load(SeqCst), expected.align());
        let (extended, offset) = Layout::new::<AtomicUsize>()
            .extend(Layout::new::<T>())
            .unwrap();
        assert_eq!(expected, extended.pad_to_align());
        let address = std::ptr::from_ref::<T>(&shared) as usize;
        assert_eq!(address % std::mem::align_of::<T>(), 0);
        assert_eq!(address - state.backing.load(SeqCst), offset);
        drop(shared);
        assert_eq!(state.backing_deallocations.load(SeqCst), 1);
    }

    #[test]
    fn backing_layout_matches_actual_aligned_and_empty_allocations() {
        #[repr(align(256))]
        struct Aligned(u8);
        let aligned = Aligned(7);
        assert_eq!(aligned.0, 7);
        assert_layout(aligned);
        assert_layout(());
        assert_layout([0u8; 3]);
        assert_layout(7u64);
    }

    #[test]
    fn checked_backing_layout_overflow_changes_no_accounting() {
        let state = Observation::new();
        let huge = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        let _observe = Observe::start(&state);
        assert_eq!(shared_layout(huge), Err(SharedAllocationError::Layout));
        assert_eq!(state.allocations.load(SeqCst), 0);
        assert_eq!(state.used.load(SeqCst), 0);
    }

    #[test]
    fn concurrent_final_aliases_destroy_and_release_once() {
        for _ in 0..64 {
            let state = Observation::new();
            let payload = payload(&state, false);
            let charge = state.used.load(SeqCst);
            let original = {
                let _observe = Observe::start(&state);
                SharedAllocation::try_new(payload).unwrap()
            };
            let alias = original.clone();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|threads| {
                for handle in [original, alias] {
                    let state = &state;
                    let barrier = &barrier;
                    threads.spawn(move || {
                        let _observe = Observe::start(state);
                        barrier.wait();
                        drop(handle);
                    });
                }
            });
            assert_disposed(&state, charge);
        }
    }

    #[test]
    fn payload_destructor_unwind_preserves_backing_bytes_charge_order() {
        let state = Observation::new();
        let payload = payload(&state, true);
        let charge = state.used.load(SeqCst);
        let result = std::panic::catch_unwind(|| {
            let _observe = Observe::start(&state);
            drop(SharedAllocation::try_new(payload).unwrap());
        });
        assert!(result.is_err());
        assert_disposed(&state, charge);
        OBSERVATION.with(|slot| assert!(slot.get().is_null()));
    }

    #[test]
    fn outer_unwind_disposes_shared_payload_once() {
        let state = Observation::new();
        let payload = payload(&state, false);
        let charge = state.used.load(SeqCst);
        let result = std::panic::catch_unwind(|| {
            let _observe = Observe::start(&state);
            let shared = SharedAllocation::try_new(payload).unwrap();
            let _alias = shared.clone();
            panic!("injected caller panic");
        });
        assert!(result.is_err());
        assert_disposed(&state, charge);
        OBSERVATION.with(|slot| assert!(slot.get().is_null()));
    }
}
