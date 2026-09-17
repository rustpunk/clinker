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

/// Internal storage choice for final decoded text; never a format option.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextStorage {
    Shared,
    Unique,
}

/// Allocation-only decoding capability. Construct before reading headers or
/// body rows. Final storage owns its grants independently of this workspace;
/// the unchanged CSV and serde_json parser intermediates remain legacy.
#[derive(Clone)]
pub struct DecodeWorkspace {
    scope: AllocationScope,
}

impl DecodeWorkspace {
    /// Create a finite scope without acquiring any output-stage capability.
    pub fn new(resources: AllocationResources) -> Result<Self, ResourceError> {
        Ok(Self {
            scope: resources.scope()?,
        })
    }

    pub fn scope(&self) -> &AllocationScope {
        &self.scope
    }

    /// Validate UTF-8 by borrowing. Latin-1 counts its checked expansion and
    /// admits scratch before copying, retaining it through final construction.
    pub(crate) fn with_decoded<T>(
        &self,
        bytes: &[u8],
        charset: crate::charset::Charset,
        use_text: impl FnOnce(&str) -> Result<T, FormatError>,
    ) -> Result<T, FormatError> {
        self.scope.check_cancelled()?;
        match charset {
            crate::charset::Charset::Utf8 => {
                let text = std::str::from_utf8(bytes).map_err(|error| {
                    FormatError::Charset(format!(
                        "input is not valid UTF-8: {error}. Declare the source's character \
                         set (e.g. `encoding: iso-8859-1`) if the input uses a \
                         non-UTF-8 repertoire"
                    ))
                })?;
                use_text(text)
            }
            crate::charset::Charset::Latin1 => {
                let capacity = bytes.iter().try_fold(0usize, |len, byte| {
                    len.checked_add(if byte.is_ascii() { 1 } else { 2 })
                        .ok_or_else(|| {
                            ResourceError::new(ResourceErrorKind::Layout, bytes.len(), 0)
                        })
                })?;
                let mut scratch = ReservedBuffer::new(self.scope.clone());
                scratch.reserve_exact(capacity)?;
                for &byte in bytes {
                    let mut encoded = [0; 4];
                    scratch
                        .extend_from_slice(char::from(byte).encode_utf8(&mut encoded).as_bytes())?;
                }
                // All appended fragments came from char::encode_utf8.
                let text = std::str::from_utf8(scratch.as_slice())
                    .map_err(|_| ResourceError::new(ResourceErrorKind::Layout, capacity, 0))?;
                use_text(text)
            }
        }
    }

    /// Copy final text only after admission. Shared clones retain the original
    /// backing; unique clones follow the existing independent-copy policy.
    pub fn decode_text(
        &self,
        bytes: &[u8],
        charset: crate::charset::Charset,
        storage: TextStorage,
    ) -> Result<clinker_record::FieldStr, FormatError> {
        self.with_decoded(bytes, charset, |text| self.store_text(text, storage))
    }

    fn store_text(
        &self,
        text: &str,
        storage: TextStorage,
    ) -> Result<clinker_record::FieldStr, FormatError> {
        Ok(match storage {
            TextStorage::Shared => clinker_record::FieldStr::try_new(text, &self.scope)?,
            TextStorage::Unique => clinker_record::FieldStr::try_new_unique(text, &self.scope)?,
        })
    }

    /// Borrow the existing parser tree while admitting every final container,
    /// key and text leaf. The intermediate tree is never relabeled as admitted.
    pub fn decode_json_value(
        &self,
        parsed: &serde_json::Value,
        storage: TextStorage,
    ) -> Result<clinker_record::Value, FormatError> {
        use clinker_record::Value;
        use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
        self.scope.check_cancelled()?;
        Ok(match parsed {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(value) => Value::Bool(*value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Value::Integer(value)
                } else if value.is_u64() {
                    return Err(FormatError::Json("JSON integer exceeds the supported range and would lose precision as a float".into()));
                } else if let Some(value) = value.as_f64() {
                    Value::Float(value)
                } else {
                    // The existing arbitrary-precision parser retains numbers
                    // outside the numeric domain as text. Borrow its spelling
                    // directly, without a new unadmitted to_string buffer.
                    Value::String(self.store_text(value.as_str(), storage)?)
                }
            }
            serde_json::Value::String(value) => Value::String(self.store_text(value, storage)?),
            serde_json::Value::Array(items) => {
                let mut values = OwnedValues::try_with_capacity(items.len(), &self.scope)?;
                for item in items {
                    let value = self.decode_json_value(item, storage)?;
                    values
                        .try_push(value, &self.scope)
                        .map_err(|(error, _)| error)?;
                }
                Value::Array(values)
            }
            serde_json::Value::Object(items) => {
                let mut values = OwnedMap::try_with_capacity(items.len(), &self.scope)?;
                for (key, item) in items {
                    let key = OwnedKey::try_new(key, &self.scope)?;
                    let value = self.decode_json_value(item, storage)?;
                    values
                        .try_insert(key, value, &self.scope)
                        .map_err(|(error, _, _)| error)?;
                }
                Value::Map(values)
            }
        })
    }

    /// Decode a repeated cell using the shared split grammar or the unchanged
    /// JSON parser. Empty cells are empty arrays in every mode.
    pub fn decode_split_cell(
        &self,
        text: &str,
        spec: &crate::multi_value::SplitValues,
        storage: TextStorage,
    ) -> Result<clinker_record::Value, FormatError> {
        use crate::multi_value::{SplitFragment, visit_split_text};
        use clinker_record::Value;
        use clinker_record::owned_storage::OwnedValues;
        self.scope.check_cancelled()?;
        if text.is_empty() {
            return Ok(Value::Array(OwnedValues::try_with_capacity(
                0,
                &self.scope,
            )?));
        }
        if spec.json {
            let field = crate::error::OutputFieldName::new(&spec.field);
            let parsed: serde_json::Value = serde_json::from_str(text).map_err(|error| {
                FormatError::Json(format!(
                    "split_values `json: true` on field '{field}': cell is not valid JSON: {error}"
                ))
            })?;
            if !parsed.is_array() {
                return Err(FormatError::Json(format!(
                    "split_values `json: true` on field '{field}': cell is JSON but not an array (a `multiple:` column holds an array)"
                )));
            }
            if let Some(n) = crate::csv::reader::first_lossy_integer(&parsed) {
                return Err(FormatError::Json(format!(
                    "split_values `json: true` on field '{field}': integer {n} exceeds the supported range and would lose precision as a float"
                )));
            }
            return self.decode_json_value(&parsed, storage);
        }
        let mut count = 0usize;
        visit_split_text(text, &spec.delimiter, &spec.escape, |fragment| {
            if matches!(fragment, SplitFragment::Part(_) | SplitFragment::End) {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| ResourceError::new(ResourceErrorKind::Layout, count, 0))?;
            }
            Ok::<_, ResourceError>(())
        })?;
        let mut values = OwnedValues::try_with_capacity(count, &self.scope)?;
        let mut scratch = crate::reserved::ReservedText::new(self.scope.clone());
        visit_split_text(text, &spec.delimiter, &spec.escape, |fragment| {
            let value = match fragment {
                SplitFragment::Part(part) => self.store_text(part, storage)?,
                SplitFragment::Text(part) => {
                    scratch.push_str(part)?;
                    return Ok(());
                }
                SplitFragment::End => {
                    let value = self.store_text(scratch.as_str(), storage)?;
                    scratch = crate::reserved::ReservedText::new(self.scope.clone());
                    value
                }
            };
            values
                .try_push(Value::String(value), &self.scope)
                .map_err(|(error, _)| FormatError::from(error))
        })?;
        Ok(Value::Array(values))
    }

    /// Move the exact admitted vector into a record without a second copy.
    pub fn finish_record(
        &self,
        schema: clinker_record::owned_storage::SharedStorage<clinker_record::Schema>,
        values: clinker_record::owned_storage::OwnedValues,
    ) -> Result<Record, FormatError> {
        self.scope.check_cancelled()?;
        Record::from_owned_values(schema, values)
            .map_err(|error| FormatError::SchemaInference(error.to_string()))
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
