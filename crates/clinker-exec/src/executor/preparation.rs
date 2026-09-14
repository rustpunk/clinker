//! Executor-backed writer admission and raw-byte spill stages.
//!
//! The run owns one consumer. Grants outlive handles as needed, so teardown
//! cannot unregister live allocations. Residual disk debt is never reclaimed
//! merely because a temporary-file destructor ran.

use super::storage_validate::ResolvedStorage;

#[cfg(test)]
thread_local! {
    static WRITE_FAULT: std::cell::Cell<Option<io::ErrorKind>> = const { std::cell::Cell::new(None) };
    static CLOSE_FAULT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    static CLOSE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SHORT_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Consuming File transfers the sole handle; no File destructor may close it
// again. In particular, an interrupted close must never retry a possibly reused
// descriptor. Linux releases the descriptor before reporting late close errors;
// other platforms retain conservative ownership debt when release is uncertain.
struct CloseFailure {
    uncertain: bool,
}
fn close_temporary(file: File) -> Result<(), CloseFailure> {
    #[cfg(unix)]
    let result = {
        use std::os::fd::IntoRawFd;
        let fd = file.into_raw_fd();
        // SAFETY: fd is the uniquely owned live descriptor consumed above.
        let closed = unsafe { libc::close(fd) } == 0;
        if closed {
            Ok(())
        } else {
            Err(CloseFailure {
                uncertain: !cfg!(target_os = "linux"),
            })
        }
    };
    #[cfg(windows)]
    let result = {
        use std::os::windows::io::IntoRawHandle;
        let handle = file.into_raw_handle();
        // SAFETY: handle is uniquely owned by the consumed File.
        let closed = unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) } != 0;
        if closed {
            Ok(())
        } else {
            Err(CloseFailure { uncertain: true })
        }
    };
    #[cfg(test)]
    {
        CLOSE_CALLS.with(|calls| calls.set(calls.get() + 1));
        if let Some(uncertain) = CLOSE_FAULT.with(|fault| fault.take()) {
            // Exercise error ownership after the real native close. The
            // uncertain case intentionally retains a conservative quota debt.
            result?;
            return Err(CloseFailure { uncertain });
        }
    }
    result
}

fn write_temporary(file: &mut File, bytes: &[u8]) -> io::Result<usize> {
    #[cfg(test)]
    {
        if let Some(kind) = WRITE_FAULT.with(|fault| fault.take()) {
            return Err(kind.into());
        }
        if SHORT_WRITE.with(|short| short.get()) {
            return file.write(&bytes[..bytes.len().min(37)]);
        }
    }
    file.write(bytes)
}
use crate::pipeline::{
    memory::{ConsumerHandle, ConsumerId, ConsumerSpillError, MemoryArbitrator, MemoryConsumer},
    shutdown::ShutdownToken,
};
use clinker_format::{
    FormatError,
    preparation::{
        AllocationGrant, MemoryStorage, OperationStage, OwnerId, ResourceAuthority, ResourceError,
        ResourceErrorKind, StageStorage, StorageStage, WriterResources, WriterScope, io_resource,
    },
    reserved::ReservedVec,
};
use std::{
    alloc::Layout,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

/// Explicit finite executor resource capability. Configured storage is optional;
/// an absent spill root always means memory-only, never the OS temporary folder.
pub struct ExecutorResources {
    authority: Arc<ExecutorAuthority>,
}
struct ExecutorAuthority {
    arbitrator: Arc<MemoryArbitrator>,
    admission: Arc<AdmissionAuthority>,
    storage: Option<Arc<StorageCapability>>,
}
struct AdmissionAuthority {
    arbitrator: AdmissionLink,
    handle: Arc<ConsumerHandle>,
    id: ConsumerId,
    shutdown: ShutdownToken,
}

// Debt owners may be retained by the arbitrator itself. Their grant release
// links are weak to avoid a run -> debt -> grant -> run reference cycle.
struct AdmissionLink(std::sync::Weak<MemoryArbitrator>);
impl AdmissionLink {
    fn live(&self) -> Result<Arc<MemoryArbitrator>, ResourceError> {
        self.0
            .upgrade()
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Authority, 0, 0))
    }
    fn admit_writer_memory(&self, bytes: usize) -> Result<(), ResourceError> {
        self.live()?.admit_writer_memory(bytes)
    }
    fn admit_writer_disk(&self, bytes: u64) -> Result<(), ResourceError> {
        self.live()?.admit_writer_disk(bytes)
    }
    fn admit_writer_descriptor(&self, limit: usize) -> Result<(), ResourceError> {
        self.live()?.admit_writer_descriptor(limit)
    }
    fn release_writer_memory(&self, bytes: usize) {
        if let Some(arb) = self.0.upgrade() {
            arb.release_writer_memory(bytes);
        }
    }
    fn release_writer_disk(&self, bytes: u64) {
        if let Some(arb) = self.0.upgrade() {
            arb.release_writer_disk(bytes);
        }
    }
    fn release_writer_descriptor(&self) {
        if let Some(arb) = self.0.upgrade() {
            arb.release_writer_descriptor();
        }
    }
    fn unregister_consumer(&self, id: ConsumerId) {
        if let Some(arb) = self.0.upgrade() {
            arb.unregister_consumer(id);
        }
    }
    fn detach_writer_handle(&self) {
        if let Some(arb) = self.0.upgrade() {
            arb.detach_writer_handle();
        }
    }
}
impl Drop for ExecutorAuthority {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.take() {
            storage
                .active_owner
                .store(false, std::sync::atomic::Ordering::Release);
            drop(storage);
        }
        self.arbitrator.retry_writer_cleanup();
    }
}
struct WriterResourceConsumer {
    handle: Arc<ConsumerHandle>,
}

impl MemoryConsumer for WriterResourceConsumer {
    fn is_admission_managed(&self) -> bool {
        true
    }
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }
    fn spill_priority(&self) -> i32 {
        0
    }
    fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
        self.handle.request_spill();
        Ok(0)
    }
    fn can_back_pressure(&self) -> bool {
        false
    }
}
impl Drop for AdmissionAuthority {
    fn drop(&mut self) {
        self.arbitrator.unregister_consumer(self.id);
        self.arbitrator.detach_writer_handle();
    }
}
impl ExecutorResources {
    /// Establish one run consumer. The control blocks are fixed run-startup
    /// allowances; storage inventory/path allocations are admitted separately.
    pub fn new(
        arbitrator: Arc<MemoryArbitrator>,
        shutdown: ShutdownToken,
        storage: Option<&ResolvedStorage>,
        descriptors: NonZeroUsize,
    ) -> Result<Self, ResourceError> {
        let handle = ConsumerHandle::new();
        arbitrator.attach_writer_handle(handle.clone())?;
        let id = arbitrator.register_consumer(Arc::new(WriterResourceConsumer {
            handle: handle.clone(),
        }));
        let admission = Arc::new(AdmissionAuthority {
            arbitrator: AdmissionLink(Arc::downgrade(&arbitrator)),
            handle,
            id,
            shutdown,
        });
        let storage = match storage.and_then(|storage| storage.spill_root_dir.as_ref()) {
            Some(root) => Some(StorageCapability::new(
                admission.clone(),
                root,
                descriptors,
            )?),
            None => None,
        };
        if let Some(storage) = &storage {
            arbitrator.retain_writer_cleanup(storage.clone());
        }
        Ok(Self {
            authority: Arc::new(ExecutorAuthority {
                arbitrator,
                admission,
                storage,
            }),
        })
    }
    pub fn resources(&self) -> WriterResources {
        WriterResources::new(self.authority.clone())
    }
    /// Retry each debt slot once; filesystem calls run outside admission locks.
    pub fn cleanup(&self) {
        if let Some(storage) = &self.authority.storage {
            storage.cleanup();
        }
    }
    pub fn cleanup_debt_count(&self) -> usize {
        self.authority.storage.as_ref().map_or(0, |storage| {
            storage
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_slice()
                .iter()
                .filter(|slot| matches!(slot, Slot::Debt(_)))
                .count()
        })
    }
}
impl ResourceAuthority for AdmissionAuthority {
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationGrant, ResourceError> {
        self.check_cancelled()?;
        self.arbitrator.admit_writer_memory(layout.size())?;
        Ok(AllocationGrant::admitted(self, owner, layout.size()))
    }
    fn release(&self, _: OwnerId, bytes: usize) {
        self.arbitrator.release_writer_memory(bytes);
    }
    fn check_cancelled(&self) -> Result<(), ResourceError> {
        if self.shutdown.is_requested() {
            Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        } else {
            Ok(())
        }
    }
    fn create_stage(
        self: Arc<Self>,
        scope: WriterScope,
    ) -> Result<Box<dyn OperationStage>, FormatError> {
        StorageStage::create(scope.clone(), MemoryStorage::new(scope))
    }
}
impl ResourceAuthority for ExecutorAuthority {
    fn identity(&self) -> usize {
        self.admission.identity()
    }
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationGrant, ResourceError> {
        self.admission.clone().try_reserve(owner, layout)
    }
    fn release(&self, owner: OwnerId, bytes: usize) {
        self.admission.release(owner, bytes);
    }
    fn check_cancelled(&self) -> Result<(), ResourceError> {
        self.admission.check_cancelled()
    }
    fn create_stage(
        self: Arc<Self>,
        scope: WriterScope,
    ) -> Result<Box<dyn OperationStage>, FormatError> {
        match &self.storage {
            None => StorageStage::create(scope.clone(), MemoryStorage::new(scope)),
            Some(storage) => {
                // Reserve the future spill descriptor/metadata at operation
                // start so pressure cannot strand a full memory stage.
                let slot = storage.claim()?;
                let metadata = match scope.reserve(storage.file_layout) {
                    Ok(g) => g,
                    Err(e) => {
                        storage.release_slot(slot);
                        return Err(e.into());
                    }
                };
                let state = SpillStorage {
                    memory: Some(MemoryStorage::new(scope.clone())),
                    file: None,
                    storage: storage.clone(),
                    slot: Some(slot),
                    metadata: Some(metadata),
                    bytes: 0,
                    failed: None,
                    admission: self.admission.clone(),
                };
                StorageStage::create(scope, state)
            }
        }
    }
}

enum Slot {
    Free,
    Active,
    Debt(Debt),
}
struct Debt {
    close_uncertain: bool,
    path: tempfile::TempPath,
    bytes: u64,
    _metadata: AllocationGrant,
}
struct StorageCapability {
    active_owner: std::sync::atomic::AtomicBool,
    root: PathBuf,
    slots: std::sync::Mutex<ReservedVec<Slot>>,
    admission: Arc<AdmissionAuthority>,
    descriptor_limit: usize,
    file_layout: Layout,
    _root: AllocationGrant,
}

const TEMP_PREFIX: &str = "writer-";
const TEMP_RANDOM_BYTES: usize = 6;

fn checked_sum(parts: &[usize]) -> Result<usize, ResourceError> {
    parts.iter().try_fold(0usize, |total, part| {
        total
            .checked_add(*part)
            .ok_or_else(|| ResourceError::new(ResourceErrorKind::Layout, usize::MAX, 0))
    })
}

/// Requested-layout envelope audited against tempfile 3.27.0 and Rust 1.91.
/// Sum distinct allocations (including replacements), rather than multiplying
/// the path length by an unexplained safety factor. Re-audit on toolchain bumps.
fn path_join_envelope(
    base: &std::path::Path,
    child_bytes: usize,
    child_components: usize,
) -> Result<usize, ResourceError> {
    let root = base.as_os_str().len();
    let joined = checked_sum(&[root, 1, child_bytes])?;
    // PathBuf::_join clones root; _push may grow first for a separator and then
    // for the child. RawVec::grow_amortized requests max(2*old, needed, 8).
    let separator_capacity = checked_sum(&[root, root])?
        .max(checked_sum(&[root, 1])?)
        .max(8);
    let joined_capacity = checked_sum(&[separator_capacity, separator_capacity])?
        .max(joined)
        .max(8);
    let ordinary = checked_sum(&[root, separator_capacity, joined_capacity])?;
    #[cfg(windows)]
    let verbatim = {
        // Verbatim _push collects Components then rebuilds OsString. Upper
        // capacities follow RawVec doubling; include old+new at both peaks.
        let count = checked_sum(&[base.components().count(), child_components])?;
        let component_capacity = checked_sum(&[count, count])?.max(4);
        let component_peak = checked_sum(&[component_capacity, count.max(4)])?;
        let components = Layout::array::<std::path::Component<'_>>(component_peak)
            .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, component_peak, 0))?
            .size();
        let string_capacity = checked_sum(&[joined, joined])?.max(8);
        checked_sum(&[root, components, string_capacity, joined.max(8)])?
    };
    #[cfg(not(windows))]
    let verbatim = {
        let _ = child_components;
        0
    };
    Ok(ordinary.max(verbatim))
}

fn temporary_file_layout(root: &std::path::Path) -> Result<Layout, ResourceError> {
    let name = checked_sum(&[TEMP_PREFIX.len(), TEMP_RANDOM_BYTES])?;
    let path = checked_sum(&[root.as_os_str().len(), 1, name])?;
    let join = path_join_envelope(root, name, 1)?;
    // tmpname's OsString, joined path (including growth), boxed-path shrink,
    // and create_named's failure-only path clone. No failed attempt is retained
    // across tempfile's bounded collision retry loop.
    let path_allocations = checked_sum(&[name, join, path, path])?;
    // tempfile PathError contains PathBuf + io::Error; io::Error::new boxes
    // that value and its kind/trait-object carrier. Layout padding is included.
    let error_allocations = checked_sum(&[
        Layout::new::<(PathBuf, io::Error)>().size(),
        Layout::new::<(io::ErrorKind, Box<dyn std::error::Error + Send + Sync>)>().size(),
    ])?;
    #[cfg(not(windows))]
    let native = checked_sum(&[path, 1])?; // CString::new(&[u8]): capacity len+1.
    #[cfg(windows)]
    let native = {
        // to_u16s initially reserves encoded bytes+1 u16 slots (UTF-16 never
        // needs more code units than the path's encoded byte count). For long
        // absolute paths GetFullPathNameW removes relative segments and does
        // not expand short names: its required output fits that same bound.
        // fill_utf16_buf may double an insufficient buffer; include both old
        // and new allocations. get_long_path then reserves the longest added
        // prefix (\\?\UNC\, eight units) plus absolute bytes and terminator.
        let original = checked_sum(&[path, 1])?;
        let native_old = original;
        let native_new = checked_sum(&[original, original])?;
        let prefixed = checked_sum(&[path, 8, 1])?;
        let slots = checked_sum(&[original, native_old, native_new, prefixed])?;
        Layout::array::<u16>(slots)
            .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, slots, 0))?
            .size()
    };
    let bytes = checked_sum(&[path_allocations, error_allocations, native])?;
    Layout::array::<u8>(bytes).map_err(|_| ResourceError::new(ResourceErrorKind::Layout, bytes, 0))
}

impl StorageCapability {
    fn new(
        admission: Arc<AdmissionAuthority>,
        root: &std::path::Path,
        descriptors: NonZeroUsize,
    ) -> Result<Arc<Self>, ResourceError> {
        // Exactly one environment/path lookup, only for relative configured
        // roots. Its transient native workspace is a run-startup allowance,
        // independent of records or writer multiplicity. On Windows absolute()
        // also preserves drive-relative semantics. The retained clone below is
        // admitted before allocation; this temporary lookup result is then freed.
        let scope = WriterResources::new(admission.clone()).scope()?;
        let mut startup_grant = None;
        let startup_root = if root.is_absolute() {
            None
        } else {
            #[cfg(not(windows))]
            let resolved = {
                let cwd = std::env::current_dir()
                    .map_err(|error| io_resource(&error, ResourceErrorKind::Storage))?;
                let bytes =
                    path_join_envelope(&cwd, root.as_os_str().len(), root.components().count())?;
                startup_grant = Some(
                    scope
                        .reserve(Layout::array::<u8>(bytes).map_err(|_| {
                            ResourceError::new(ResourceErrorKind::Layout, bytes, 0)
                        })?)?,
                );
                cwd.join(root)
            };
            #[cfg(windows)]
            let resolved =
                {
                    // The native call also resolves drive-relative roots. Reserve
                    // the known authored-path contribution before invoking it:
                    // input UTF-16, old/new native result buffers, then WTF-8 output
                    // including its replacement peak. Only the environment-derived
                    // current-directory contribution remains a startup allowance.
                    let n = checked_sum(&[root.as_os_str().len(), 1])?;
                    let input_utf16 = Layout::array::<u16>(n)
                        .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, n, 0))?
                        .size();
                    let native_peak = checked_sum(&[input_utf16, input_utf16, input_utf16])?;
                    let output_peak = checked_sum(&[n, n, n])?;
                    let bytes = checked_sum(&[input_utf16, native_peak, output_peak])?;
                    startup_grant =
                        Some(scope.reserve(Layout::array::<u8>(bytes).map_err(|_| {
                            ResourceError::new(ResourceErrorKind::Layout, bytes, 0)
                        })?)?);
                    std::path::absolute(root)
                        .map_err(|error| io_resource(&error, ResourceErrorKind::Storage))?
                };
            Some(resolved)
        };
        let root = startup_root.as_deref().unwrap_or(root);
        let path_bytes = root.as_os_str().len();
        let root_grant = scope.reserve(
            Layout::array::<u8>(path_bytes)
                .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, path_bytes, 0))?,
        )?;
        let mut slots = ReservedVec::new(scope);
        slots.reserve_exact(descriptors.get())?;
        for _ in 0..descriptors.get() {
            slots.push(Slot::Free)?;
        }
        let file_layout = temporary_file_layout(root)?;
        let storage = Arc::new(Self {
            active_owner: std::sync::atomic::AtomicBool::new(true),
            root: root.to_path_buf(),
            slots: std::sync::Mutex::new(slots),
            admission,
            descriptor_limit: descriptors.get(),
            file_layout,
            _root: root_grant,
        });
        drop(startup_root);
        drop(startup_grant);
        Ok(storage)
    }
    fn claim(&self) -> Result<usize, ResourceError> {
        self.admission
            .arbitrator
            .admit_writer_descriptor(self.descriptor_limit)?;
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((index, slot)) = slots
            .as_mut_slice()
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| matches!(slot, Slot::Free))
        {
            *slot = Slot::Active;
            Ok(index)
        } else {
            self.admission.arbitrator.release_writer_descriptor();
            Err(ResourceError::new(ResourceErrorKind::DescriptorQuota, 1, 0))
        }
    }
    fn release_slot(&self, index: usize) {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut_slice()[index] = Slot::Free;
        self.admission.arbitrator.release_writer_descriptor();
    }
    fn cleanup(&self) {
        for index in 0..self.descriptor_limit {
            let mut debt = {
                let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
                if !matches!(slots.as_slice()[index], Slot::Debt(_)) {
                    continue;
                }
                match std::mem::replace(&mut slots.as_mut_slice()[index], Slot::Active) {
                    Slot::Debt(debt) => debt,
                    _ => unreachable!(),
                }
            };
            if debt.close_uncertain {
                // Unlink cannot prove disk release while the handle may still
                // be open. Never retry a raw handle whose identity is lost.
                self.slots
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_mut_slice()[index] = Slot::Debt(debt);
                continue;
            }
            match std::fs::remove_file(&debt.path) {
                Ok(()) => {
                    debt.path.disable_cleanup(true);
                    self.admission.arbitrator.release_writer_disk(debt.bytes);
                    self.release_slot(index);
                }
                Err(_) => {
                    self.slots
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_mut_slice()[index] = Slot::Debt(debt);
                }
            }
        }
    }
}
impl Drop for StorageCapability {
    fn drop(&mut self) {
        self.cleanup();
        // End-of-run unlink failures retain disk charges in the arbitrator.
        // Disarm tempfile's implicit retry: a hidden successful retry would
        // make the observed debt disagree with the filesystem.
        let slots = self.slots.get_mut().unwrap_or_else(|e| e.into_inner());
        for slot in slots.as_mut_slice() {
            if let Slot::Debt(mut debt) = std::mem::replace(slot, Slot::Free) {
                debt.path.disable_cleanup(true);
                if !debt.close_uncertain {
                    self.admission.arbitrator.release_writer_descriptor();
                }
            }
        }
    }
}

impl crate::pipeline::memory::reservation::WriterCleanup for StorageCapability {
    fn cleanup(&self) {
        StorageCapability::cleanup(self);
    }
    fn idle(&self) -> bool {
        !self.active_owner.load(std::sync::atomic::Ordering::Acquire)
            && self
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_slice()
                .iter()
                .all(|slot| matches!(slot, Slot::Free))
    }
    fn debts(&self) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_slice()
            .iter()
            .filter(|slot| matches!(slot, Slot::Debt(_)))
            .count()
    }
}

struct SpillStorage {
    memory: Option<MemoryStorage>,
    file: Option<(File, tempfile::TempPath)>,
    storage: Arc<StorageCapability>,
    slot: Option<usize>,
    metadata: Option<AllocationGrant>,
    bytes: u64,
    failed: Option<ResourceError>,
    admission: Arc<AdmissionAuthority>,
}
impl SpillStorage {
    fn spill(&mut self) -> Result<(), ResourceError> {
        let temp = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .rand_bytes(TEMP_RANDOM_BYTES)
            .tempfile_in(&self.storage.root)
            .map_err(|error| io_resource(&error, ResourceErrorKind::Storage))?;
        self.file = Some(temp.into_parts());
        if let Some(memory) = self.memory.take() {
            for chunk in memory.chunks() {
                let mut remaining = chunk;
                while !remaining.is_empty() {
                    let n = self.write_bytes(remaining)?;
                    if n == 0 {
                        return Err(ResourceError::new(
                            ResourceErrorKind::Storage,
                            remaining.len(),
                            0,
                        ));
                    }
                    remaining = &remaining[n..];
                }
            }
        }
        Ok(())
    }
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, ResourceError> {
        self.admission.check_cancelled()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let spill_requested = self.admission.handle.take_spill_request();
        // This consumer is never paused. Consulting the handle cannot park the
        // synchronous walk thread waiting for its own progress.
        self.admission.handle.wait_while_paused();
        if let Some(memory) = &mut self.memory {
            if !spill_requested && memory.len() < 64 * 1024 {
                match memory.write(bytes) {
                    Ok(n) => return Ok(n),
                    Err(error)
                        if memory
                            .resource_error(&error, ResourceErrorKind::Storage)
                            .kind
                            == ResourceErrorKind::Budget => {}
                    Err(error) => {
                        return Err(memory.resource_error(&error, ResourceErrorKind::Storage));
                    }
                }
            }
            self.spill()?;
        }
        let amount = bytes.len().min(8 * 1024);
        self.admission.arbitrator.admit_writer_disk(amount as u64)?;
        let result = write_temporary(
            &mut self
                .file
                .as_mut()
                .ok_or_else(|| ResourceError::new(ResourceErrorKind::Storage, 0, 0))?
                .0,
            &bytes[..amount],
        );
        let written = result.as_ref().copied().unwrap_or(0);
        self.admission
            .arbitrator
            .release_writer_disk((amount - written) as u64);
        self.bytes += written as u64;
        result.map_err(|error| io_resource(&error, ResourceErrorKind::Storage))
    }
}
impl Write for SpillStorage {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        self.write_bytes(bytes).map_err(|error| {
            self.failed = Some(error);
            io::ErrorKind::Other.into()
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Some((file, _)) = &mut self.file {
            file.flush()?;
        }
        Ok(())
    }
}
impl Read for SpillStorage {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.admission.check_cancelled() {
            self.failed = Some(error);
            return Err(io::ErrorKind::Other.into());
        }
        if let Some(memory) = &mut self.memory {
            memory.read(bytes)
        } else {
            self.file
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
                .0
                .read(bytes)
        }
    }
}
impl StageStorage for SpillStorage {
    fn failure(&self) -> Option<ResourceError> {
        self.failed
            .or_else(|| self.memory.as_ref().and_then(StageStorage::failure))
    }
    fn complete(&mut self) -> Result<(), ResourceError> {
        self.close_and_remove()
    }
    fn seal(&mut self) -> Result<u64, ResourceError> {
        if let Some(error) = self.failure() {
            return Err(error);
        }
        self.admission.check_cancelled()?;
        if let Some(memory) = &mut self.memory {
            memory.seal()
        } else {
            self.flush()
                .map_err(|e| io_resource(&e, ResourceErrorKind::Storage))?;
            self.file
                .as_mut()
                .ok_or_else(|| ResourceError::new(ResourceErrorKind::Readback, 0, 0))?
                .0
                .seek(SeekFrom::Start(0))
                .map_err(|e| io_resource(&e, ResourceErrorKind::Readback))?;
            Ok(self.bytes)
        }
    }
}
impl SpillStorage {
    fn close_and_remove(&mut self) -> Result<(), ResourceError> {
        let Some(slot) = self.slot.take() else {
            return Ok(());
        };
        let Some((file, mut path)) = self.file.take() else {
            self.storage.release_slot(slot);
            return Ok(());
        };
        let close = close_temporary(file);
        let uncertain = close.as_ref().is_err_and(|error| error.uncertain);
        if !uncertain && std::fs::remove_file(&path).is_ok() {
            path.disable_cleanup(true);
            self.admission.arbitrator.release_writer_disk(self.bytes);
            self.storage.release_slot(slot);
            return close.map_err(|_| ResourceError::new(ResourceErrorKind::Storage, 0, 0));
        }
        // The slot and metadata were admitted before creation; transferring
        // debt allocates nothing and preserves disk/descriptor ownership.
        if let Some(metadata) = self.metadata.take() {
            self.storage
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_mut_slice()[slot] = Slot::Debt(Debt {
                close_uncertain: uncertain,
                path,
                bytes: self.bytes,
                _metadata: metadata,
            });
        }
        Err(ResourceError::new(ResourceErrorKind::Storage, 0, 0))
    }
}
impl Drop for SpillStorage {
    fn drop(&mut self) {
        let _ = self.close_and_remove();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;

    #[test]
    fn stage_cancellation_bypasses_an_already_paused_handle() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = ResolvedStorage {
            spill_root_dir: Some(dir.path().to_owned()),
            free_space_warning: None,
            cap_headroom_warning: None,
        };
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            Some(&resolved),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let baseline = arb.writer_resource_usage().memory;
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        // Production arbitration never pauses this consumer. Even an already
        // paused handle must not block cancellation before the next chunk.
        provider.authority.admission.handle.pause();
        token.request();
        assert!(stage.write_all(b"cancelled").is_err());
        assert_eq!(stage.failure().unwrap().kind, ResourceErrorKind::Cancelled);
        assert!(stage.finish().is_err());
        assert_eq!(arb.writer_resource_usage().memory, baseline);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
    }

    #[test]
    fn stage_close_failure_prevents_commit_and_never_retries_handles() {
        use clinker_format::preparation::{FormatEncoder, OutputOperation, PreparedWriter};
        struct Encoder {
            commits: usize,
        }
        impl FormatEncoder for Encoder {
            type Pending = ();
            fn prepare(
                &self,
                _: OutputOperation<'_>,
                stage: &mut dyn Write,
                _: &WriterScope,
            ) -> Result<(), FormatError> {
                for _ in 0..100 {
                    stage.write_all(&[7; 1024])?;
                }
                Ok(())
            }
            fn commit(&mut self, _: ()) {
                self.commits += 1;
            }
        }
        for uncertain in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let resolved = ResolvedStorage {
                spill_root_dir: Some(dir.path().to_owned()),
                free_space_warning: None,
                cap_headroom_warning: None,
            };
            let arb = Arc::new(MemoryArbitrator::with_policy(
                256 * 1024,
                0.8,
                0.7,
                Box::new(NoOpPolicy),
            ));
            let provider = ExecutorResources::new(
                arb.clone(),
                ShutdownToken::detached(),
                Some(&resolved),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), Encoder { commits: 0 }, provider.resources())
                    .unwrap();
            CLOSE_CALLS.with(|calls| calls.set(0));
            CLOSE_FAULT.with(|fault| fault.set(Some(uncertain)));
            assert!(writer.write_operation(OutputOperation::Finalize).is_err());
            assert_eq!(writer.destination().len(), 100 * 1024);
            assert_eq!(writer.encoder().commits, 0);
            assert!(writer.write_operation(OutputOperation::Finalize).is_err());
            assert_eq!(writer.destination().len(), 100 * 1024);
            drop(writer);
            provider.cleanup();
            assert_eq!(CLOSE_CALLS.with(|calls| calls.get()), 1);
            assert_eq!(provider.cleanup_debt_count(), usize::from(uncertain));
            assert_eq!(
                arb.writer_resource_usage().descriptors,
                usize::from(uncertain)
            );
            assert_eq!(
                arb.writer_resource_usage().disk,
                if uncertain { 100 * 1024 } else { 0 }
            );
            if uncertain {
                assert!(provider.resources().scope().unwrap().stage().is_err());
                drop(provider);
                assert_eq!(arb.retry_writer_cleanup(), 1);
                assert_eq!(CLOSE_CALLS.with(|calls| calls.get()), 1);
                assert_eq!(arb.writer_resource_usage().descriptors, 1);
            }
        }
    }

    #[test]
    fn stage_short_writes_charge_actual_lengths_and_deliver_every_byte() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = ResolvedStorage {
            spill_root_dir: Some(dir.path().to_owned()),
            free_space_warning: None,
            cap_headroom_warning: None,
        };
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            Some(&resolved),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        SHORT_WRITE.with(|short| short.set(true));
        let input = vec![23; 100 * 1024];
        let result = stage.write_all(&input);
        SHORT_WRITE.with(|short| short.set(false));
        result.unwrap();
        assert_eq!(arb.writer_resource_usage().disk, input.len() as u64);
        let mut output = Vec::new();
        stage.finish().unwrap().deliver(&mut output).unwrap();
        assert_eq!(output, input);
        assert_eq!(arb.writer_resource_usage().disk, 0);
    }

    #[test]
    fn stage_disk_full_and_permission_errors_release_unused_quota() {
        for kind in [io::ErrorKind::StorageFull, io::ErrorKind::PermissionDenied] {
            let dir = tempfile::tempdir().unwrap();
            let resolved = ResolvedStorage {
                spill_root_dir: Some(dir.path().to_owned()),
                free_space_warning: None,
                cap_headroom_warning: None,
            };
            let arb = Arc::new(MemoryArbitrator::with_policy(
                128 * 1024,
                0.8,
                0.7,
                Box::new(NoOpPolicy),
            ));
            let provider = ExecutorResources::new(
                arb.clone(),
                ShutdownToken::detached(),
                Some(&resolved),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
            let baseline = arb.writer_resource_usage().memory;
            let mut stage = provider.resources().scope().unwrap().stage().unwrap();
            WRITE_FAULT.with(|fault| fault.set(Some(kind)));
            assert!(stage.write_all(&vec![1; 100 * 1024]).is_err());
            assert!(stage.finish().is_err());
            assert_eq!(arb.writer_resource_usage().disk, 0);
            assert_eq!(arb.writer_resource_usage().memory, baseline);
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }
}
