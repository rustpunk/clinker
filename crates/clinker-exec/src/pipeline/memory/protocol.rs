//! The memory ledger's synchronized core: the charged total and its peak, the
//! bytes charged in each consumer's name with their high-water mark, the
//! release epoch, and the disk and descriptor counts that share its lock.
//!
//! It holds byte counts, an epoch and holder labels, never records, RSS
//! readings or cleanup callbacks. Nothing here performs I/O, logs, emits
//! telemetry or calls into a consumer, so every critical section is a few
//! arithmetic steps (a snapshot also copies the holders' labels).
//!
//! The file locks only through `super::sync` and names no other crate type:
//! consumer ids are raw `u32`s and the holder label is a type parameter. That
//! keeps it compilable against a model checker's `sync` module unchanged.

use super::sync::{Mutex, MutexGuard};
use std::collections::BTreeMap;

/// The ledger state behind its one mutex.
///
/// `L` is the label a holder is reported under; `A` is caller-owned state
/// that must change under the same lock as the charges it reflects.
pub(crate) struct LedgerCore<L, A = ()> {
    state: Mutex<LedgerState<L, A>>,
}

impl<L, A> LedgerCore<L, A> {
    /// An open ledger admitting charges up to `limit` bytes.
    pub(crate) fn new(limit: u64, attachment: A) -> Self {
        Self {
            state: Mutex::new(LedgerState {
                limit,
                charged: 0,
                peak_charged: 0,
                release_epoch: 0,
                consumers: BTreeMap::new(),
                closed: false,
                disk: 0,
                descriptors: 0,
                attachment,
            }),
        }
    }

    /// Lock the ledger. A panic while it was held leaves only counts behind,
    /// so a poisoned lock is recovered rather than propagated.
    pub(crate) fn lock(&self) -> MutexGuard<'_, LedgerState<L, A>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Why a charge was refused. Nothing was charged in either case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    /// The ledger no longer admits charges.
    Closed,
    /// The request does not fit. `available` is what could have been
    /// granted; `oversized` means no release could ever make it fit.
    Short { available: u64, oversized: bool },
}

/// Bytes charged in one consumer's name.
///
/// `handle` is what the consumer's own handle has charged and `attributed`
/// what grants made for it hold now; `mark` is the largest their sum has
/// reached. An entry is created by the first charge to its id and removed
/// only when its consumer unregisters.
struct ConsumerEntry<L> {
    handle: u64,
    attributed: u64,
    mark: u64,
    label: Option<L>,
}

impl<L> ConsumerEntry<L> {
    fn empty() -> Self {
        Self {
            handle: 0,
            attributed: 0,
            mark: 0,
            label: None,
        }
    }

    fn current(&self) -> u64 {
        self.handle.saturating_add(self.attributed)
    }

    fn raise_mark(&mut self) {
        self.mark = self.mark.max(self.current());
    }
}

/// The locked ledger. Reached only through [`LedgerCore::lock`].
pub(crate) struct LedgerState<L, A> {
    limit: u64,
    charged: u64,
    peak_charged: u64,
    /// Advanced by every release of a nonzero byte count and by nothing
    /// else, so an unchanged epoch across a span proves no byte was released
    /// in it.
    release_epoch: u64,
    consumers: BTreeMap<u32, ConsumerEntry<L>>,
    pub(crate) closed: bool,
    /// Writer disk quota currently granted.
    pub(crate) disk: u64,
    /// Writer file descriptors currently granted.
    pub(crate) descriptors: usize,
    pub(crate) attachment: A,
}

impl<L, A> LedgerState<L, A> {
    /// The limit charges are admitted against.
    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    /// Replace the limit, refusing one below the bytes already charged.
    /// On refusal the charged total is returned and the limit is unchanged.
    pub(crate) fn set_limit(&mut self, limit: u64) -> Result<(), u64> {
        if limit < self.charged {
            return Err(self.charged);
        }
        self.limit = limit;
        Ok(())
    }

    /// Bytes charged now.
    pub(crate) fn charged(&self) -> u64 {
        self.charged
    }

    /// Highest charged total so far; a release never lowers it.
    pub(crate) fn peak_charged(&self) -> u64 {
        self.peak_charged
    }

    /// Bytes a request could be granted beside `outside` bytes the ledger
    /// does not hold but that count against the same limit.
    pub(crate) fn available(&self, outside: u64) -> u64 {
        self.limit
            .saturating_sub(self.charged.saturating_add(outside))
    }

    /// Check and charge `bytes` in one step, attributed to consumer
    /// `attribution` when given.
    ///
    /// A closed ledger refuses everything; otherwise a zero-byte request is
    /// granted without charging. A request larger than the limit, or one the
    /// charged total could not represent, is refused as oversized; one larger
    /// than [`Self::available`] is refused as short. A grant raises the peak
    /// and, when attributed, the consumer's attributed bytes and mark.
    pub(crate) fn try_charge(
        &mut self,
        bytes: u64,
        outside: u64,
        attribution: Option<u32>,
    ) -> Result<(), Refusal> {
        if self.closed {
            return Err(Refusal::Closed);
        }
        if bytes == 0 {
            return Ok(());
        }
        let available = self.available(outside);
        if bytes > self.limit {
            return Err(Refusal::Short {
                available,
                oversized: true,
            });
        }
        if bytes > available {
            return Err(Refusal::Short {
                available,
                oversized: false,
            });
        }
        let Some(charged) = self.charged.checked_add(bytes) else {
            return Err(Refusal::Short {
                available,
                oversized: true,
            });
        };
        self.charged = charged;
        self.peak_charged = self.peak_charged.max(charged);
        if let Some(id) = attribution {
            let entry = self
                .consumers
                .entry(id)
                .or_insert_with(ConsumerEntry::empty);
            entry.attributed = entry.attributed.saturating_add(bytes);
            entry.raise_mark();
        }
        Ok(())
    }

    /// Release `bytes` charged with attribution `attribution`.
    ///
    /// Lowers the charged total and, when the attributed consumer still has
    /// an entry, its attributed bytes. When the entry was already removed
    /// (its consumer unregistered while a grant in its name was still live)
    /// the per-consumer figures are left alone: those bytes were
    /// unattributed from the removal on. Advances the release epoch.
    pub(crate) fn release(&mut self, bytes: u64, attribution: Option<u32>) {
        if bytes == 0 {
            return;
        }
        self.discharge(bytes);
        if let Some(id) = attribution
            && let Some(entry) = self.consumers.get_mut(&id)
        {
            entry.attributed = entry.attributed.saturating_sub(bytes);
        }
    }

    fn discharge(&mut self, bytes: u64) {
        debug_assert!(
            bytes <= self.charged,
            "released {bytes} bytes with only {} charged",
            self.charged
        );
        self.charged = self.charged.saturating_sub(bytes);
        self.release_epoch = self.release_epoch.wrapping_add(1);
    }

    /// `id`'s high-water mark of handle plus attributed bytes, or `None`
    /// when the ledger holds no entry for it.
    pub(crate) fn consumer_mark(&self, id: u32) -> Option<u64> {
        self.consumers.get(&id).map(|entry| entry.mark)
    }

    /// Every labelled consumer holding bytes now, with its current handle
    /// plus attributed bytes, largest first (ties by id); and the charged
    /// bytes none of them holds.
    ///
    /// The remainder is what grants made in no consumer's name hold, plus
    /// what consumers without a label hold, so the holders' figures and the
    /// remainder add up to the charged total.
    pub(crate) fn holders(&self) -> (Vec<(u32, &L, u64)>, u64) {
        let mut holders: Vec<(u32, &L, u64)> = self
            .consumers
            .iter()
            .filter_map(|(&id, entry)| {
                let held = entry.current();
                match &entry.label {
                    Some(label) if held > 0 => Some((id, label, held)),
                    _ => None,
                }
            })
            .collect();
        holders.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
        let held = holders
            .iter()
            .fold(0u64, |sum, holder| sum.saturating_add(holder.2));
        (holders, self.charged.saturating_sub(held))
    }
}

/// Handle charges and entry removal as a registered consumer's binding will
/// make them, and the release epoch a reclaim pass will read.
#[cfg(test)]
impl<L, A> LedgerState<L, A> {
    pub(crate) fn release_epoch(&self) -> u64 {
        self.release_epoch
    }

    /// Charge `bytes` to `id`'s handle, unchecked, and record its label.
    pub(crate) fn charge_handle(&mut self, id: u32, label: L, bytes: u64) {
        self.charged = self.charged.saturating_add(bytes);
        self.peak_charged = self.peak_charged.max(self.charged);
        let entry = self
            .consumers
            .entry(id)
            .or_insert_with(ConsumerEntry::empty);
        entry.label = Some(label);
        entry.handle = entry.handle.saturating_add(bytes);
        entry.raise_mark();
    }

    /// Release `bytes` of `id`'s handle charge.
    pub(crate) fn release_handle(&mut self, id: u32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.discharge(bytes);
        if let Some(entry) = self.consumers.get_mut(&id) {
            entry.handle = entry.handle.saturating_sub(bytes);
        }
    }

    /// Remove `id`'s entry, returning its mark.
    pub(crate) fn remove_consumer(&mut self, id: u32) -> Option<u64> {
        self.consumers.remove(&id).map(|entry| entry.mark)
    }
}
