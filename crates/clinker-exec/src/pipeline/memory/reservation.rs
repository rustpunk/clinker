//! Writer admission on the one memory ledger: governed allocation grants,
//! writer disk quota and descriptors, and writer cleanup.
//!
//! Memory, disk and descriptors share the ledger's one lock
//! ([`super::protocol`]); memory is admitted through
//! [`MemoryArbitrator::reserve`], against the same total every registered
//! consumer's handle charges.

use super::ledger::Requester;
use super::protocol::{AdmissionGate, LedgerCore, LedgerState};
use super::*;
use clinker_format::preparation::{ResourceError, ResourceErrorKind};
use clinker_plan::runtime_error::ConsumerLabel;

/// Bounded run cleanup capability. Implementations retain paths and quota
/// ownership; callbacks run without the admission or registry lock held.
pub(crate) trait WriterCleanup: Send + Sync {
    fn cleanup(&self);
    fn idle(&self) -> bool;
    fn debts(&self) -> usize;
}

/// Live admitted resources and their memory high-water mark; excludes RSS.
/// `memory` is what governed allocation grants hold on the ledger, not the
/// consumer handle charges beside them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterResourceUsage {
    pub memory: u64,
    pub peak_memory: u64,
    pub disk: u64,
    pub descriptors: usize,
}

/// State kept under the ledger lock beside the charges: the managed writer
/// consumer's handle and id. Holding them under the lock that closes the run
/// lets exactly one writer consumer attach, and lets closing the run take its
/// id to unregister. The handle charges nothing: the writer's staged bytes
/// are governed allocations the ledger already holds.
///
/// Test builds also keep an armed forced shortfall here: it is the ledger's
/// [`AdmissionGate`], so every charge counts and fires it in the same step
/// as its own check.
#[derive(Default)]
pub(super) struct WriterBinding {
    pub handle: Option<Arc<ConsumerHandle>>,
    pub consumer_id: Option<ConsumerId>,
    #[cfg(any(test, feature = "test-utils"))]
    pub forced_shortfall: Option<super::ledger::ArmedShortfall>,
}

/// Outside test builds nothing can be armed, so the gate never refuses and
/// admission skips it.
impl AdmissionGate<ConsumerLabel> for WriterBinding {
    #[cfg(any(test, feature = "test-utils"))]
    const CAN_REFUSE: bool = true;

    #[cfg(any(test, feature = "test-utils"))]
    fn force_refusal(&mut self, label: &ConsumerLabel, resident: u64) -> bool {
        let Some(armed) = self.forced_shortfall.as_mut() else {
            return false;
        };
        let fired = armed.fires(label, resident);
        if armed.spent() {
            self.forced_shortfall = None;
        }
        fired
    }
}

/// The ledger as this crate instantiates it.
pub(super) type LedgerCell = LedgerCore<ConsumerLabel, WriterBinding>;
/// The locked ledger as this crate instantiates it.
pub(super) type LockedLedger = LedgerState<ConsumerLabel, WriterBinding>;

/// Fixed release state. It holds counters and a managed handle, never a run,
/// telemetry arena, storage callback or retained input value.
pub(crate) struct ReservationState {
    pub(super) ledger: LedgerCell,
}
impl ReservationState {
    pub(super) fn new(limit: u64) -> Self {
        let state = Self {
            ledger: LedgerCore::new(limit, WriterBinding::default()),
        };
        // Establish platform-native mutex storage at run startup.
        drop(state.ledger.lock());
        state
    }
    pub(crate) fn check_open(&self) -> Result<(), ResourceError> {
        Self::require_open(&self.ledger.lock())
    }
    fn require_open(ledger: &LockedLedger) -> Result<(), ResourceError> {
        if ledger.closed {
            Err(ResourceError::new(ResourceErrorKind::Finalized, 0, 0))
        } else {
            Ok(())
        }
    }
    fn usage(&self) -> WriterResourceUsage {
        let ledger = self.ledger.lock();
        WriterResourceUsage {
            memory: ledger.granted(),
            peak_memory: ledger.peak_granted(),
            disk: ledger.disk,
            descriptors: ledger.descriptors,
        }
    }
}

/// Bounded read-only observation of real reservation state. Retaining this
/// observer does not retain the arbitrator, data, cleanup or admission authority.
#[derive(Clone)]
pub struct WriterResourceObserver(Arc<ReservationState>);
impl WriterResourceObserver {
    pub fn usage(&self) -> WriterResourceUsage {
        self.0.usage()
    }
    pub fn is_closed(&self) -> bool {
        self.0.ledger.lock().closed
    }
    pub fn has_managed_handle(&self) -> bool {
        self.0.ledger.lock().attachment.handle.is_some()
    }
}

impl MemoryArbitrator {
    pub(crate) fn retain_writer_cleanup(&self, owner: Arc<dyn WriterCleanup>) {
        *self
            .writer_cleanup
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(owner);
    }

    /// Retry retained file debts once, including after writer handles drop.
    /// Returns remaining debts; failure never reports released disk quota.
    pub fn retry_writer_cleanup(&self) -> usize {
        let owner = self
            .writer_cleanup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(owner) = owner else {
            return 0;
        };
        owner.cleanup();
        let debts = owner.debts();
        if owner.idle() {
            // Drop outside this mutex: releasing grant owners may unregister
            // their consumer and must not run under a registry lock.
            let removed = self
                .writer_cleanup
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            drop(removed);
        }
        debts
    }

    pub fn writer_resource_usage(&self) -> WriterResourceUsage {
        self.admission.usage()
    }

    /// Admit one governed allocation of `bytes` through [`Self::reserve`] for
    /// `requester`. The caller's `AllocationLease` owns the charge from here
    /// and returns it through [`ReservationState::release_writer_memory`]
    /// with the same attribution.
    pub(crate) fn admit_writer_memory(
        &self,
        bytes: usize,
        requester: Requester,
    ) -> Result<(), ResourceError> {
        match self.reserve(bytes as u64, requester) {
            Ok(grant) => {
                grant.detach();
                Ok(())
            }
            Err(shortfall) if shortfall.is_closed() => {
                Err(ResourceError::new(ResourceErrorKind::Finalized, 0, 0))
            }
            Err(shortfall) => Err(ResourceError::new(
                ResourceErrorKind::Budget,
                bytes,
                shortfall.available.min(usize::MAX as u64) as usize,
            )),
        }
    }

    pub(crate) fn admit_writer_disk(&self, bytes: u64) -> Result<(), ResourceError> {
        let mut ledger = self.admission.ledger.lock();
        ReservationState::require_open(&ledger)?;
        let available = self
            .max_spill_bytes()
            .saturating_sub(self.cumulative_spill_bytes())
            .saturating_sub(ledger.disk);
        if bytes > available {
            return Err(ResourceError::new(
                ResourceErrorKind::DiskQuota,
                bytes.min(usize::MAX as u64) as usize,
                available.min(usize::MAX as u64) as usize,
            ));
        }
        ledger.disk += bytes;
        Ok(())
    }

    pub(crate) fn admit_writer_descriptor(&self, limit: usize) -> Result<(), ResourceError> {
        let mut ledger = self.admission.ledger.lock();
        ReservationState::require_open(&ledger)?;
        if ledger.descriptors >= limit {
            return Err(ResourceError::new(ResourceErrorKind::DescriptorQuota, 1, 0));
        }
        ledger.descriptors += 1;
        Ok(())
    }

    pub(crate) fn attach_writer_handle(
        &self,
        handle: Arc<ConsumerHandle>,
    ) -> Result<(), ResourceError> {
        let mut ledger = self.admission.ledger.lock();
        ReservationState::require_open(&ledger)?;
        if ledger.attachment.handle.is_some() {
            return Err(ResourceError::new(ResourceErrorKind::Authority, 1, 0));
        }
        ledger.attachment.handle = Some(handle);
        Ok(())
    }
    pub(crate) fn bind_writer_consumer(&self, id: ConsumerId) -> Result<(), ResourceError> {
        let mut ledger = self.admission.ledger.lock();
        if let Err(error) = ReservationState::require_open(&ledger) {
            drop(ledger);
            self.unregister_consumer(id);
            return Err(error);
        }
        ledger.attachment.consumer_id = Some(id);
        Ok(())
    }
    pub(crate) fn detach_writer_handle(&self, id: ConsumerId) {
        let detached = {
            let mut ledger = self.admission.ledger.lock();
            if ledger.attachment.consumer_id == Some(id) {
                ledger.attachment.consumer_id = None;
                ledger.attachment.handle = None;
                true
            } else {
                false
            }
        };
        if detached {
            self.unregister_consumer(id);
        }
    }
    /// Stop new writer admission and detach the managed run consumer. Existing
    /// charges remain observable and releasable; cleanup runs outside the lock.
    pub fn close_writer_resources(&self) {
        let id = {
            let mut ledger = self.admission.ledger.lock();
            ledger.closed = true;
            ledger.attachment.handle = None;
            ledger.attachment.consumer_id.take()
        };
        if let Some(id) = id {
            self.unregister_consumer(id);
        }
        self.retry_writer_cleanup();
    }
    pub fn writer_resource_observer(&self) -> WriterResourceObserver {
        WriterResourceObserver(self.admission.clone())
    }
    pub(crate) fn writer_reservation_state(&self) -> Arc<ReservationState> {
        self.admission.clone()
    }
}

impl ReservationState {
    /// Release `bytes` of governed allocation charged with `attribution`,
    /// the consumer the charge was made for (`None` for a run-scoped charge).
    /// Advances the release epoch. Releasing in the name of a consumer whose
    /// ledger entry is already gone lowers only the charged total.
    pub(crate) fn release_writer_memory(&self, bytes: usize, attribution: Option<ConsumerId>) {
        self.release_memory(bytes as u64, attribution);
    }
    pub(super) fn release_memory(&self, bytes: u64, attribution: Option<ConsumerId>) {
        self.ledger
            .lock()
            .release(bytes, attribution.map(|id| id.0));
    }
    pub(crate) fn release_writer_disk(&self, bytes: u64) {
        self.ledger.lock().disk -= bytes;
    }
    pub(crate) fn release_writer_descriptor(&self) {
        self.ledger.lock().descriptors -= 1;
    }
}
