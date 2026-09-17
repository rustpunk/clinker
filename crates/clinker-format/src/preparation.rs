//! Finite allocation ownership and operation-level encoding before delivery.
//!
//! Governs requested allocation layouts, not allocator overhead or process RSS.
//! Providers never receive destination handles. Existing codecs opt in separately.

use std::alloc::Layout;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use crate::{FormatError, reserved::ReservedBuffer};
use clinker_record::{DocumentContext, Record};

/// Workspace retained from preparation through delivery, including spill.
pub const PROGRESS_BYTES: usize = 16 * 1024;
/// Independently admitted stage chunk size.
pub const STAGE_CHUNK_BYTES: usize = 16 * 1024;

pub use clinker_record::owned_storage::{
    AllocationAuthority, AllocationLease, AllocationResources, AllocationScope, OwnerId,
    ResourceError, ResourceErrorKind,
};

impl From<ResourceError> for FormatError {
    fn from(value: ResourceError) -> Self {
        Self::Resource(value)
    }
}

/// Format-only stage capability. Allocation admission belongs to the core
/// authority; a stage receives the matching writer's explicit finite scope.
pub trait ResourceAuthority: Send + Sync {
    fn create_stage(self: Arc<Self>, scope: WriterScope) -> Result<OperationStage, FormatError>;
}

/// Explicit allocation and staging capabilities from one provider.
#[derive(Clone)]
pub struct WriterResources {
    allocation: AllocationResources,
    stage: Arc<dyn ResourceAuthority>,
}
impl WriterResources {
    /// Both capabilities originate from the same provider; allocation-only
    /// callers can borrow the finite core resources without stage access.
    pub fn new<T: AllocationAuthority + ResourceAuthority + 'static>(authority: Arc<T>) -> Self {
        Self {
            allocation: AllocationResources::new(authority.clone()),
            stage: authority,
        }
    }
    pub fn allocation(&self) -> &AllocationResources {
        &self.allocation
    }
    pub fn scope(&self) -> Result<WriterScope, ResourceError> {
        Ok(WriterScope {
            allocation: self.allocation.scope()?,
            stage: self.stage.clone(),
        })
    }
}

/// Inline scope: ownership stays in the core allocation capability, while stage
/// creation remains confined to the format layer. Cloning allocates no bytes.
#[derive(Clone)]
pub struct WriterScope {
    allocation: AllocationScope,
    stage: Arc<dyn ResourceAuthority>,
}
impl WriterScope {
    pub fn allocation(&self) -> &AllocationScope {
        &self.allocation
    }
    pub fn owner(&self) -> OwnerId {
        self.allocation.owner()
    }
    pub fn reserve(&self, layout: Layout) -> Result<AllocationLease, ResourceError> {
        self.allocation.reserve(layout)
    }
    pub fn check_cancelled(&self) -> Result<(), ResourceError> {
        self.allocation.check_cancelled()
    }
    pub fn stage(&self) -> Result<OperationStage, FormatError> {
        self.check_cancelled()?;
        self.stage.clone().create_stage(self.clone())
    }
}

/// Finite standalone provider. Its single fixed Arc/mutex control block is a
/// construction allowance independent of input; all scopes are inline.
pub struct MemoryOnlyResources {
    authority: Arc<MemoryAuthority>,
}
struct MemoryAuthority {
    limit: usize,
    used: Mutex<usize>,
}
impl MemoryOnlyResources {
    pub fn new(limit: NonZeroUsize) -> Self {
        let authority = Arc::new(MemoryAuthority {
            limit: limit.get(),
            used: Mutex::new(0),
        });
        // Some platforms allocate the native mutex on first lock. Establish
        // that fixed control-block storage at startup, before any admission.
        drop(authority.used.lock().unwrap_or_else(|e| e.into_inner()));
        Self { authority }
    }
    pub fn resources(&self) -> WriterResources {
        WriterResources::new(self.authority.clone())
    }
    pub fn used(&self) -> usize {
        *self
            .authority
            .used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}
impl AllocationAuthority for MemoryAuthority {
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationLease, ResourceError> {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        let available = self.limit.saturating_sub(*used);
        if layout.size() > available {
            return Err(ResourceError::new(
                ResourceErrorKind::Budget,
                layout.size(),
                available,
            ));
        }
        *used += layout.size();
        drop(used);
        AllocationLease::admitted(self, owner, layout.size())
    }
    fn release(&self, _: OwnerId, bytes: usize) {
        *self.used.lock().unwrap_or_else(|e| e.into_inner()) -= bytes;
    }
    fn check_cancelled(&self) -> Result<(), ResourceError> {
        Ok(())
    }
}
impl ResourceAuthority for MemoryAuthority {
    fn create_stage(self: Arc<Self>, scope: WriterScope) -> Result<OperationStage, FormatError> {
        StorageStage::create(scope.clone(), MemoryStorage::new(scope))
    }
}

/// Private encoding destination with admitted backing retained through deallocation.
///
/// The inline owner drops its boxed backend before releasing the metadata grant.
/// Finishing consumes the writable capability and moves this same owner into
/// sealed bytes; no raw backend or independently detachable grant is exposed.
pub struct OperationStage {
    backend: Box<dyn StageBackend>,
    _metadata: AllocationLease,
}

impl OperationStage {
    /// Bounded failure evidence, retained without copying provider diagnostics.
    pub fn failure(&self) -> Option<ResourceError> {
        self.backend.failure()
    }

    /// Seal without reallocating the backend, retaining its grant through readback.
    pub fn finish(mut self) -> Result<PreparedBytes, FormatError> {
        let len = self.backend.seal()?;
        Ok(PreparedBytes { len, stage: self })
    }
}

impl Write for OperationStage {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.backend.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.backend.flush()
    }
}

trait StageBackend: Write + Send {
    fn failure(&self) -> Option<ResourceError>;
    fn seal(&mut self) -> Result<u64, FormatError>;
    fn deliver(&mut self, destination: &mut dyn Write, len: u64) -> Result<(), FormatError>;
}

/// Storage implementations own their memory/files and support bounded readback.
/// `seal` must rewind and establish immutable, complete bytes, without codecs.
pub trait StageStorage: Read + Write + Send {
    /// Observe a terminal resource failure established by the stage adapter,
    /// including admission before the first storage call. Must not allocate or
    /// change the returned error; delivery behavior never depends on observation.
    fn resource_failed(&mut self, _error: ResourceError) {}
    /// First terminal resource failure, retained inline for this storage's
    /// lifetime. Resource-denied I/O returns a nonallocating ErrorKind sentinel;
    /// callers must recover this evidence before interpreting that sentinel.
    /// A failed storage cannot accept more bytes or seal a partial operation.
    fn failure(&self) -> Option<ResourceError>;
    /// Recover resource evidence at each I/O boundary, including readback.
    fn resource_error(&self, error: &io::Error, fallback: ResourceErrorKind) -> ResourceError {
        self.failure()
            .unwrap_or_else(|| io_resource(error, fallback))
    }
    fn seal(&mut self) -> Result<u64, ResourceError>;
    /// Release readback storage fallibly before the encoder commits state.
    fn complete(&mut self) -> Result<(), ResourceError>;
}

/// Adapts provider storage with a retained progress buffer and charged metadata.
/// This is the sole constructor path to sealed prepared bytes.
pub struct StorageStage<S: StageStorage> {
    storage: S,
    progress: ReservedBuffer,
    scope: WriterScope,
    failed: Option<ResourceError>,
}
impl<S: StageStorage + 'static> StorageStage<S> {
    /// Reserve metadata and progress before any storage writes or destination effects.
    pub fn create(scope: WriterScope, mut storage: S) -> Result<OperationStage, FormatError> {
        let metadata = scope.reserve(Layout::new::<Self>()).inspect_err(|&error| {
            storage.resource_failed(error);
        })?;
        let mut progress = ReservedBuffer::new(scope.allocation().clone());
        progress
            .extend_from_slice(&[0; PROGRESS_BYTES])
            .inspect_err(|&error| {
                storage.resource_failed(error);
            })?;
        let backend = crate::reserved::try_box(Self {
            storage,
            progress,
            scope,
            failed: None,
        })
        .map_err(|(error, mut stage)| {
            stage.record_failure(error);
            error
        })?;
        Ok(OperationStage {
            backend,
            _metadata: metadata,
        })
    }
}
impl<S: StageStorage> StorageStage<S> {
    fn record_failure(&mut self, error: ResourceError) {
        self.failed.get_or_insert(error);
        self.storage.resource_failed(error);
    }
}
impl<S: StageStorage> Write for StorageStage<S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.scope.check_cancelled() {
            self.record_failure(error);
            return Err(io::ErrorKind::Other.into());
        }
        match self.storage.write(bytes) {
            Ok(0) if !bytes.is_empty() => {
                let error = ResourceError::new(ResourceErrorKind::Storage, bytes.len(), 0);
                self.record_failure(error);
                Err(io::ErrorKind::Other.into())
            }
            Ok(n) => Ok(n),
            Err(error) => {
                self.record_failure(
                    self.storage
                        .resource_error(&error, ResourceErrorKind::Storage),
                );
                // Provider errors can own path strings. Drop them while the
                // metadata grant is live; only bounded evidence crosses out.
                Err(io::ErrorKind::Other.into())
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        self.storage.flush().map_err(|error| {
            self.record_failure(
                self.storage
                    .resource_error(&error, ResourceErrorKind::Storage),
            );
            io::Error::from(io::ErrorKind::Other)
        })
    }
}
impl<S: StageStorage> StageBackend for StorageStage<S> {
    fn failure(&self) -> Option<ResourceError> {
        self.failed
    }
    fn seal(&mut self) -> Result<u64, FormatError> {
        if let Some(error) = self.failed {
            return Err(error.into());
        }
        self.scope.check_cancelled().inspect_err(|&error| {
            self.record_failure(error);
        })?;
        let len = self.storage.seal().inspect_err(|&error| {
            self.record_failure(error);
        })?;
        Ok(len)
    }

    fn deliver(
        &mut self,
        destination: &mut dyn Write,
        mut remaining: u64,
    ) -> Result<(), FormatError> {
        self.scope.check_cancelled().inspect_err(|&error| {
            self.record_failure(error);
        })?;
        while remaining != 0 {
            self.scope.check_cancelled().inspect_err(|&error| {
                self.record_failure(error);
            })?;
            let amount = remaining.min((PROGRESS_BYTES / 2) as u64) as usize;
            let bytes = &mut self.progress.as_mut_slice()[..amount];
            let n = match self.storage.read(bytes) {
                Ok(n) => n,
                Err(error) => {
                    let error = self
                        .storage
                        .resource_error(&error, ResourceErrorKind::Readback);
                    self.record_failure(error);
                    return Err(error.into());
                }
            };
            if n == 0 {
                let error = ResourceError::new(ResourceErrorKind::Readback, amount, 0);
                self.record_failure(error);
                return Err(error.into());
            }
            destination.write_all(&bytes[..n])?;
            remaining -= n as u64;
        }
        self.scope.check_cancelled().inspect_err(|&error| {
            self.record_failure(error);
        })?;
        self.storage.complete().inspect_err(|&error| {
            self.record_failure(error);
        })?;
        Ok(())
    }
}

/// Sealed bytes with unique ownership. Delivery consumes the readback and all
/// grants exactly once. Generic I/O may partially accept bytes before failing.
pub struct PreparedBytes {
    len: u64,
    stage: OperationStage,
}
impl PreparedBytes {
    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn deliver(mut self, destination: &mut dyn Write) -> Result<(), FormatError> {
        self.stage.backend.deliver(destination, self.len)
    }
}

/// Memory stage retains independently admitted chunks and an admitted inventory.
/// Its caller may choose a spill threshold; standalone use has only its budget.
pub struct MemoryStorage {
    scope: WriterScope,
    chunks: crate::reserved::ReservedVec<ReservedBuffer>,
    len: usize,
    read: usize,
    failed: Option<ResourceError>,
}
impl MemoryStorage {
    pub fn new(scope: WriterScope) -> Self {
        Self {
            chunks: crate::reserved::ReservedVec::new(scope.allocation().clone()),
            scope,
            len: 0,
            read: 0,
            failed: None,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Borrow already admitted chunks for spill, without acquiring workspace.
    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks.as_slice().iter().map(ReservedBuffer::as_slice)
    }
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, ResourceError> {
        self.scope.check_cancelled()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let index = self.len / STAGE_CHUNK_BYTES;
        if index == self.chunks.len() {
            let mut chunk = ReservedBuffer::new(self.scope.allocation().clone());
            chunk.reserve_exact(STAGE_CHUNK_BYTES)?;
            self.chunks.push(chunk)?;
        }
        let chunk = &mut self.chunks.as_mut_slice()[index];
        let n = bytes.len().min(STAGE_CHUNK_BYTES - chunk.len());
        chunk.extend_from_slice(&bytes[..n])?;
        self.len += n;
        Ok(n)
    }
}
impl Write for MemoryStorage {
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
        Ok(())
    }
}
impl Read for MemoryStorage {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.scope.check_cancelled() {
            self.failed = Some(error);
            return Err(io::ErrorKind::Other.into());
        }
        if self.read == self.len || out.is_empty() {
            return Ok(0);
        }
        let chunk = &self.chunks.as_slice()[self.read / STAGE_CHUNK_BYTES];
        let offset = self.read % STAGE_CHUNK_BYTES;
        let n = out.len().min(chunk.len() - offset);
        out[..n].copy_from_slice(&chunk.as_slice()[offset..offset + n]);
        self.read += n;
        Ok(n)
    }
}
impl StageStorage for MemoryStorage {
    fn failure(&self) -> Option<ResourceError> {
        self.failed
    }
    fn complete(&mut self) -> Result<(), ResourceError> {
        Ok(())
    }
    fn seal(&mut self) -> Result<u64, ResourceError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        self.scope.check_cancelled()?;
        self.read = 0;
        Ok(self.len as u64)
    }
}

/// Output operation includes implicit framing; draining bytes is independent.
pub enum OutputOperation<'a> {
    Record(&'a Record),
    BeginDocument(&'a DocumentContext),
    EndDocument(&'a DocumentContext),
    Finalize,
}
/// Encoders borrow committed state while preparing. Only commit may change it;
/// commit must perform infallible moves/scalar assignments, without allocation.
pub trait FormatEncoder {
    type Pending;
    fn prepare(
        &self,
        operation: OutputOperation<'_>,
        stage: &mut dyn Write,
        workspace: &WriterScope,
    ) -> Result<Self::Pending, FormatError>;
    fn commit(&mut self, pending: Self::Pending);
}
#[derive(Clone, Copy)]
enum DeliveryState {
    Open,
    Poisoned(ResourceError),
    Finalized,
}
/// Owns encoder, destination and finite resources. A failed delivery poisons
/// all continuation; drop neither finalizes nor retries a destination write.
pub struct PreparedWriter<W, E> {
    destination: W,
    encoder: E,
    scope: WriterScope,
    state: DeliveryState,
}
impl<W: Write, E: FormatEncoder> PreparedWriter<W, E> {
    pub fn new(
        destination: W,
        encoder: E,
        resources: WriterResources,
    ) -> Result<Self, ResourceError> {
        Ok(Self {
            destination,
            encoder,
            scope: resources.scope()?,
            state: DeliveryState::Open,
        })
    }
    pub fn encoder(&self) -> &E {
        &self.encoder
    }
    pub fn destination(&self) -> &W {
        &self.destination
    }
    pub fn write_operation(&mut self, operation: OutputOperation<'_>) -> Result<(), FormatError> {
        match self.state {
            DeliveryState::Poisoned(error) => return Err(error.into()),
            DeliveryState::Finalized => {
                return Err(ResourceError::new(ResourceErrorKind::Finalized, 0, 0).into());
            }
            DeliveryState::Open => {}
        }
        let finalize = matches!(operation, OutputOperation::Finalize);
        let mut stage = self.scope.stage()?;
        let pending = match self.encoder.prepare(operation, &mut stage, &self.scope) {
            Ok(pending) => pending,
            Err(error) => return Err(stage.failure().map(FormatError::Resource).unwrap_or(error)),
        };
        let prepared = stage.finish()?;
        if let Err(error) = prepared.deliver(&mut self.destination) {
            self.state = DeliveryState::Poisoned(ResourceError::new(
                ResourceErrorKind::DeliveryPoisoned,
                0,
                0,
            ));
            return Err(error);
        }
        self.encoder.commit(pending);
        if finalize {
            self.state = DeliveryState::Finalized;
        }
        Ok(())
    }
    /// Drain only; never emits format closing bytes.
    pub fn flush_bytes(&mut self) -> Result<(), FormatError> {
        if let DeliveryState::Poisoned(error) = self.state {
            return Err(error.into());
        }
        if let Err(error) = self.destination.flush() {
            self.state = DeliveryState::Poisoned(ResourceError::new(
                ResourceErrorKind::DeliveryPoisoned,
                0,
                0,
            ));
            return Err(error.into());
        }
        Ok(())
    }
    /// Finalize once, then drain; repeat calls never re-encode closing bytes.
    pub fn flush(&mut self) -> Result<(), FormatError> {
        if matches!(self.state, DeliveryState::Open) {
            self.write_operation(OutputOperation::Finalize)?;
        }
        self.flush_bytes()
    }
}

/// Extract legacy boxed evidence or classify native I/O. Storage adapters must
/// use [`StageStorage::resource_error`] to recover their inline failure first.
pub fn io_resource(error: &io::Error, fallback: ResourceErrorKind) -> ResourceError {
    error
        .get_ref()
        .and_then(|e| e.downcast_ref::<ResourceError>())
        .copied()
        .unwrap_or_else(|| ResourceError::new(fallback, 0, 0))
}
