//! Single synchronized ledger for writer allocation, disk and descriptor grants.
//! Legacy consumer observations are sampled outside this lock and remain estimates.

use super::*;
use clinker_format::preparation::{ResourceError, ResourceErrorKind};

/// Bounded run cleanup capability. Implementations retain paths and quota
/// ownership; callbacks run without the admission or registry lock held.
pub(crate) trait WriterCleanup: Send + Sync {
    fn cleanup(&self);
    fn idle(&self) -> bool;
    fn debts(&self) -> usize;
}

/// Live admitted resources and their memory high-water mark; excludes RSS.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterResourceUsage {
    pub memory: u64,
    pub peak_memory: u64,
    pub disk: u64,
    pub descriptors: usize,
}

#[derive(Default)]
pub(super) struct ReservationLedger {
    pub usage: WriterResourceUsage,
    pub handle: Option<Arc<ConsumerHandle>>,
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
        self.admission
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage
    }

    pub(crate) fn admit_writer_memory(&self, bytes: usize) -> Result<(), ResourceError> {
        // Consumer callbacks run before the lock. Managed consumers are omitted
        // from this legacy sample: their grants are already in the ledger.
        let legacy = self
            .consumers
            .load()
            .iter()
            .filter(|(_, c)| !c.is_admission_managed())
            .fold(0u64, |sum, (_, c)| sum.saturating_add(c.current_usage()));
        let mut ledger = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        let available = self
            .limit()
            .saturating_sub(legacy)
            .saturating_sub(ledger.usage.memory);
        if bytes as u64 > available {
            return Err(ResourceError::new(
                ResourceErrorKind::Budget,
                bytes,
                available.min(usize::MAX as u64) as usize,
            ));
        }
        ledger.usage.memory += bytes as u64;
        ledger.usage.peak_memory = ledger.usage.peak_memory.max(ledger.usage.memory);
        if let Some(handle) = &ledger.handle {
            handle.set_bytes(ledger.usage.memory);
        }
        self.peak_consumer_usage.fetch_max(
            legacy.saturating_add(ledger.usage.memory),
            Ordering::Relaxed,
        );
        Ok(())
    }

    pub(crate) fn release_writer_memory(&self, bytes: usize) {
        let mut ledger = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        ledger.usage.memory -= bytes as u64;
        if let Some(handle) = &ledger.handle {
            handle.set_bytes(ledger.usage.memory);
        }
    }

    pub(crate) fn admit_writer_disk(&self, bytes: u64) -> Result<(), ResourceError> {
        let mut ledger = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        let available = self
            .max_spill_bytes()
            .saturating_sub(self.cumulative_spill_bytes())
            .saturating_sub(ledger.usage.disk);
        if bytes > available {
            return Err(ResourceError::new(
                ResourceErrorKind::DiskQuota,
                bytes.min(usize::MAX as u64) as usize,
                available.min(usize::MAX as u64) as usize,
            ));
        }
        ledger.usage.disk += bytes;
        Ok(())
    }
    pub(crate) fn release_writer_disk(&self, bytes: u64) {
        self.admission
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage
            .disk -= bytes;
    }
    pub(crate) fn admit_writer_descriptor(&self, limit: usize) -> Result<(), ResourceError> {
        let mut ledger = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        if ledger.usage.descriptors >= limit {
            return Err(ResourceError::new(ResourceErrorKind::DescriptorQuota, 1, 0));
        }
        ledger.usage.descriptors += 1;
        Ok(())
    }
    pub(crate) fn release_writer_descriptor(&self) {
        self.admission
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage
            .descriptors -= 1;
    }
    pub(crate) fn attach_writer_handle(
        &self,
        handle: Arc<ConsumerHandle>,
    ) -> Result<(), ResourceError> {
        let mut ledger = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        if ledger.handle.is_some() {
            return Err(ResourceError::new(ResourceErrorKind::Authority, 1, 0));
        }
        ledger.handle = Some(handle);
        Ok(())
    }
    pub(crate) fn detach_writer_handle(&self) {
        self.admission
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .handle = None;
    }
}
