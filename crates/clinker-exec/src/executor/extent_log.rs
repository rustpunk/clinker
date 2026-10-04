//! A keyed log of byte frames held in memory per key and moved, when the
//! memory budget needs it, to one spill file as chained per-key extents.
//!
//! Each key owns a chain: the extents already on disk, linked forward from
//! the first by a `next` offset in each extent's header, and a resident tail
//! of frames not yet written. Frames of one key are read back in the order
//! they were appended, across every extent and then the tail, and nothing
//! else is ordered: keys interleave freely in the file.
//!
//! ## Memory
//!
//! The resident tails and the index (one entry per key) are charged through
//! the consumer handle the owner passes in, which the owner shares with its
//! other charges. They leave memory only on the
//! [`crate::pipeline::memory::MemoryArbitrator`]'s signals, never on a size
//! of the log's own: when the owner's consumer is elected
//! ([`ExtentLog::relieve`], which reads the handle's spill request), when
//! the soft threshold has tripped at a batch boundary or a decision (also
//! [`ExtentLog::relieve`]), or when the next append would pass the hard
//! limit ([`ExtentLog::admit_charge`]). Between the owner's polls the tails
//! grow by at most the appends of one batch past the soft threshold; the
//! hard limit is checked on every append. The only fixed size is the file's
//! write buffer, [`EXTENT_LOG_WRITE_BUFFER_BYTES`], which is I/O buffering,
//! not a spill decision, and is not charged, like the dead-letter writer's.
//!
//! ## Disk
//!
//! One file for the whole log, however many keys, created in the run's spill
//! directory at the first flush. Every extent is charged to the run's spill
//! quota (E320) under the node that asked for the flush, and the charge is
//! not released when the file is removed. The log holds at most two
//! descriptors: its writer and the one [`ChainReader`] a caller holds at a
//! time. The file is a temporary: dropping the log removes it, and the
//! spill directory's lock and crash purge cover a process that dies first.
//! The owner drops the log before the run's spill-directory guard, so the
//! file is closed and removed before the directory is.
//! Nothing in the log is ever promoted; a caller copies frames out of a
//! [`ChainReader`] to wherever they belong.

use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clinker_plan::SpillError;
use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;
use tempfile::TempPath;

use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};

/// Capacity of the log file's write buffer, the dead-letter writer's
/// convention. Flushed before every seek and before any read.
pub(crate) const EXTENT_LOG_WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// Each frame is stored behind its length as a little-endian `u32`.
const FRAME_LENGTH_BYTES: usize = 4;

/// An extent's fixed header, little-endian: the `next` extent's offset
/// (`NO_NEXT` for none), the payload's on-disk length, the frame count, and
/// whether the payload is LZ4-block compressed with its size prepended.
const EXTENT_HEADER_BYTES: usize = 8 + 8 + 4 + 1;

/// The `next` offset of the last extent of a chain.
const NO_NEXT: u64 = u64::MAX;

/// One key's chain: its extents on disk and its resident tail.
struct ExtentChain {
    /// Offset of the chain's first extent, `None` until its first flush.
    first: Option<u64>,
    /// Offset of the chain's last extent, whose `next` the following flush
    /// patches.
    last: Option<u64>,
    /// Frames appended since the chain's last flush, each behind its length.
    tail: Vec<u8>,
    tail_frames: u32,
}

impl ExtentChain {
    fn new() -> Self {
        Self {
            first: None,
            last: None,
            tail: Vec::new(),
            tail_frames: 0,
        }
    }
}

/// The log's file, created at the first flush.
struct ExtentFile {
    writer: BufWriter<File>,
    /// Removes the file when the log drops.
    path: TempPath,
    /// Offset one past the last byte written.
    end: u64,
}

/// A keyed log of frames: per-key resident tails, flushed on the
/// arbitrator's signals to one spill file as chained per-key extents. See
/// the module documentation for what it holds and when.
pub(crate) struct ExtentLog<K> {
    chains: HashMap<K, ExtentChain>,
    /// Sum of the tails' capacities: the resident bytes a flush frees.
    resident: u64,
    /// `resident`, shared with the owner's consumer so it can answer a spill
    /// request with the bytes a flush would free.
    resident_gauge: Arc<AtomicU64>,
    spill_root: Arc<Path>,
    compress: CompressMode,
    handle: Arc<ConsumerHandle>,
    file: Option<ExtentFile>,
}

impl<K: Eq + Hash + Clone> ExtentLog<K> {
    /// Charge for one key's index entry: the map slot, over-allocated as a
    /// hash map's growth and load factor leave it.
    pub(crate) const CHAIN_ENTRY_BYTES: u64 = chain_entry_bytes::<K>();

    /// An empty log that will create its file in `spill_root`, compress
    /// extents as `compress` resolves for each, and charge through `handle`.
    /// Creates no file.
    pub(crate) fn new(
        spill_root: Arc<Path>,
        compress: CompressMode,
        handle: Arc<ConsumerHandle>,
    ) -> Self {
        Self {
            chains: HashMap::new(),
            resident: 0,
            resident_gauge: Arc::new(AtomicU64::new(0)),
            spill_root,
            compress,
            handle,
            file: None,
        }
    }

    /// Whether the log has created its file.
    #[cfg(feature = "test-utils")]
    pub(crate) fn has_file(&self) -> bool {
        self.file.is_some()
    }

    /// The directory the log creates its file in.
    #[cfg(feature = "test-utils")]
    pub(crate) fn spill_root(&self) -> &Path {
        &self.spill_root
    }

    /// The resident tail bytes, kept current as the log changes, for a
    /// consumer that reports what a flush would free.
    pub(crate) fn resident_gauge(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.resident_gauge)
    }

    /// Bytes held in resident tails, as charged.
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident
    }

    /// Bytes charged for the index: one entry per key.
    pub(crate) fn index_bytes(&self) -> u64 {
        self.chains.len() as u64 * Self::CHAIN_ENTRY_BYTES
    }

    /// Whether `key` has a chain that was not taken.
    pub(crate) fn contains(&self, key: &K) -> bool {
        self.chains.contains_key(key)
    }

    /// The bytes appending a frame of `frame_len` bytes to `key` would add
    /// to the charge: the tail's growth, and a new index entry for a new key.
    fn append_growth(&self, key: &K, frame_len: usize) -> u64 {
        let needed = FRAME_LENGTH_BYTES + frame_len;
        match self.chains.get(key) {
            Some(chain) => tail_growth(chain.tail.len(), chain.tail.capacity(), needed),
            None => Self::CHAIN_ENTRY_BYTES + tail_growth(0, 0, needed),
        }
    }

    /// Preflight appending a frame of `frame_len` bytes to `key`, plus
    /// `extra` bytes the owner charges with it, against `budget`'s hard
    /// limit, as a node-buffer reservation does. When the charge would not
    /// fit, every tail is flushed first and the charge re-checked. Returns
    /// the resident bytes a flush freed.
    ///
    /// # Errors
    ///
    /// [`PipelineError::MemoryBudgetExceeded`] with
    /// [`clinker_plan::BudgetCategory::Arena`], naming `node` and `what`,
    /// when the charge does not fit even with every tail flushed; nothing is
    /// appended. A flush's errors, as [`Self::flush_all`].
    pub(crate) fn admit_charge(
        &mut self,
        budget: &MemoryArbitrator,
        key: &K,
        frame_len: usize,
        extra: u64,
        node: &str,
        what: &str,
    ) -> Result<u64, PipelineError> {
        let hard_limit = budget.hard_limit();
        if hard_limit == 0 {
            return Ok(0);
        }
        let growth = self.append_growth(key, frame_len).saturating_add(extra);
        if budget.sum_consumer_usage().saturating_add(growth) <= hard_limit {
            return Ok(0);
        }
        let freed = self.flush_all(budget, node)?;
        let growth = self.append_growth(key, frame_len).saturating_add(extra);
        let charged = budget.sum_consumer_usage();
        let projected = charged.saturating_add(growth);
        if projected > hard_limit {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: node.to_string(),
                used: projected,
                limit: hard_limit,
                source: clinker_plan::BudgetCategory::Arena,
                detail: Some(format!(
                    "{what} projected {projected} bytes from charged pressure {charged} plus \
                     {growth} bytes for one more held row, with every held row already on disk"
                )),
            });
        }
        Ok(freed)
    }

    /// Append `frame` to `key`'s resident tail, creating its chain on its
    /// first frame, and charge the growth. Returns the bytes charged. Never
    /// flushes; the owner preflights with [`Self::admit_charge`] and polls
    /// [`Self::relieve`].
    ///
    /// # Errors
    ///
    /// [`PipelineError::Internal`] for a frame longer than `u32::MAX` bytes
    /// or a tail holding `u32::MAX` frames.
    pub(crate) fn append(&mut self, key: &K, frame: &[u8]) -> Result<u64, PipelineError> {
        let length = u32::try_from(frame.len()).map_err(|_| PipelineError::Internal {
            op: "extent log",
            node: String::new(),
            detail: format!("a held frame of {} bytes exceeds u32::MAX", frame.len()),
        })?;
        let mut charged = 0;
        if !self.chains.contains_key(key) {
            self.chains.insert(key.clone(), ExtentChain::new());
            charged += Self::CHAIN_ENTRY_BYTES;
        }
        let chain = self.chains.get_mut(key).expect("chain inserted above");
        chain.tail_frames =
            chain
                .tail_frames
                .checked_add(1)
                .ok_or_else(|| PipelineError::Internal {
                    op: "extent log",
                    node: String::new(),
                    detail: "a held tail reached u32::MAX frames".to_string(),
                })?;
        let before = chain.tail.capacity();
        let target = next_capacity(
            chain.tail.len(),
            chain.tail.capacity(),
            FRAME_LENGTH_BYTES + frame.len(),
        );
        if target > before {
            chain.tail.reserve_exact(target - chain.tail.len());
        }
        chain.tail.extend_from_slice(&length.to_le_bytes());
        chain.tail.extend_from_slice(frame);
        let grown = (chain.tail.capacity() - before) as u64;
        self.resident += grown;
        self.resident_gauge.store(self.resident, Ordering::Relaxed);
        charged += grown;
        self.handle.add_bytes(charged);
        Ok(charged)
    }

    /// The flush policy: flush every tail when the arbitrator has elected
    /// the owner's consumer (its handle's spill request, always read and
    /// cleared here) or, at a batch boundary or a decision (`at_boundary`),
    /// when `budget`'s soft threshold has tripped. Holds no threshold of its
    /// own. Returns the resident bytes freed. `node` is charged for any
    /// extent written.
    ///
    /// # Errors
    ///
    /// As [`Self::flush_all`].
    pub(crate) fn relieve(
        &mut self,
        budget: &MemoryArbitrator,
        node: &str,
        at_boundary: bool,
    ) -> Result<u64, PipelineError> {
        // The soft check runs an arbitration round that may elect the owner's
        // consumer; reading the request after it lets this flush answer it.
        let soft = at_boundary && budget.should_spill();
        let requested = self.handle.take_spill_request();
        if soft || requested {
            self.flush_all(budget, node)
        } else {
            Ok(0)
        }
    }

    /// Write every non-empty tail as one extent at the end of the file,
    /// creating the file on the first flush, link each to its chain, charge
    /// the bytes written to `node` against the spill quota, and release each
    /// tail's allocation. Returns the resident bytes freed.
    ///
    /// # Errors
    ///
    /// [`PipelineError::Spill`] when the file cannot be created or written;
    /// [`PipelineError::spill_cap_exceeded`] (E320) when the bytes written
    /// pass the spill cap.
    pub(crate) fn flush_all(
        &mut self,
        budget: &MemoryArbitrator,
        node: &str,
    ) -> Result<u64, PipelineError> {
        if !self.chains.values().any(|chain| chain.tail_frames > 0) {
            return Ok(0);
        }
        let spill_root = Arc::clone(&self.spill_root);
        let io_error =
            |e: std::io::Error| PipelineError::from(SpillError::from_spill_dir_io(&spill_root, e));
        if self.file.is_none() {
            let (file, path) = tempfile::NamedTempFile::new_in(&*self.spill_root)
                .map_err(io_error)?
                .into_parts();
            self.file = Some(ExtentFile {
                writer: BufWriter::with_capacity(EXTENT_LOG_WRITE_BUFFER_BYTES, file),
                path,
                end: 0,
            });
        }
        let file = self.file.as_mut().expect("file created above");
        let mut patches: Vec<(u64, u64)> = Vec::new();
        let mut written: u64 = 0;
        let mut freed: u64 = 0;
        for chain in self.chains.values_mut() {
            if chain.tail_frames == 0 {
                continue;
            }
            let compressed = self
                .compress
                .resolve(chain.tail.len() as u64, u64::from(chain.tail_frames));
            let packed;
            let payload: &[u8] = if compressed {
                packed = lz4_flex::block::compress_prepend_size(&chain.tail);
                &packed
            } else {
                &chain.tail
            };
            let offset = file.end;
            let mut header = [0u8; EXTENT_HEADER_BYTES];
            header[0..8].copy_from_slice(&NO_NEXT.to_le_bytes());
            header[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
            header[16..20].copy_from_slice(&chain.tail_frames.to_le_bytes());
            header[20] = u8::from(compressed);
            file.writer.write_all(&header).map_err(io_error)?;
            file.writer.write_all(payload).map_err(io_error)?;
            let extent_bytes = (EXTENT_HEADER_BYTES + payload.len()) as u64;
            file.end += extent_bytes;
            written += extent_bytes;
            if let Some(last) = chain.last {
                patches.push((last, offset));
            }
            chain.first.get_or_insert(offset);
            chain.last = Some(offset);
            freed += chain.tail.capacity() as u64;
            chain.tail = Vec::new();
            chain.tail_frames = 0;
        }
        // Link each chain's previous extent to the one just written. The
        // buffered writer flushes on every seek.
        for (at, next) in patches {
            file.writer.seek(SeekFrom::Start(at)).map_err(io_error)?;
            file.writer
                .write_all(&next.to_le_bytes())
                .map_err(io_error)?;
        }
        file.writer
            .seek(SeekFrom::Start(file.end))
            .map_err(io_error)?;
        self.resident -= freed;
        self.resident_gauge.store(self.resident, Ordering::Relaxed);
        self.handle.sub_bytes(freed);
        if budget.record_spill_bytes(node, written) {
            return Err(PipelineError::spill_cap_exceeded(
                node,
                budget.max_spill_bytes(),
                written,
                budget.cumulative_spill_bytes(),
            ));
        }
        Ok(freed)
    }

    /// Remove `key`'s chain and return a reader over its frames in append
    /// order, or `None` when the key has no chain. The index entry's charge
    /// is released here; the taken tail's charge moves to the reader, which
    /// releases it, with its own read buffer's, when it drops. The reader
    /// opens its own descriptor on the file only when the chain has extents.
    ///
    /// # Errors
    ///
    /// [`PipelineError::Spill`] when the pending writes cannot be flushed or
    /// the file cannot be opened for reading.
    pub(crate) fn take(&mut self, key: &K) -> Result<Option<ChainReader>, PipelineError> {
        let Some(chain) = self.chains.remove(key) else {
            return Ok(None);
        };
        self.handle.sub_bytes(Self::CHAIN_ENTRY_BYTES);
        let tail_charge = chain.tail.capacity() as u64;
        self.resident -= tail_charge;
        self.resident_gauge.store(self.resident, Ordering::Relaxed);
        let mut reader = ChainReader {
            file: None,
            file_end: 0,
            next: chain.first,
            raw: Vec::new(),
            extent: Vec::new(),
            extent_pos: 0,
            extent_frames: 0,
            tail: chain.tail,
            tail_pos: 0,
            tail_frames: chain.tail_frames,
            in_tail: false,
            handle: Arc::clone(&self.handle),
            charged: tail_charge,
        };
        if chain.first.is_some() {
            let spill_root = Arc::clone(&self.spill_root);
            let io_error = |e: std::io::Error| {
                PipelineError::from(SpillError::from_spill_dir_io(&spill_root, e))
            };
            let file = self.file.as_mut().ok_or_else(|| PipelineError::Internal {
                op: "extent log",
                node: String::new(),
                detail: "a chain has extents but the log has no file".to_string(),
            })?;
            file.writer.flush().map_err(io_error)?;
            let path: PathBuf = file.path.to_path_buf();
            reader.file = Some(File::open(&path).map_err(io_error)?);
            reader.file_end = file.end;
        }
        Ok(Some(reader))
    }
}

impl<K> Drop for ExtentLog<K> {
    fn drop(&mut self) {
        // Release this log's charges from the owner's handle; the file goes
        // with its `TempPath`.
        let index = self.chains.len() as u64 * chain_entry_bytes::<K>();
        self.handle.sub_bytes(self.resident.saturating_add(index));
    }
}

/// The capacity a tail of `len` bytes in `capacity` needs to take `needed`
/// more bytes: its capacity when they fit, otherwise the larger of what they
/// need and twice the capacity, so repeated appends stay amortized.
fn next_capacity(len: usize, capacity: usize, needed: usize) -> usize {
    let required = len.saturating_add(needed);
    if required <= capacity {
        capacity
    } else {
        required.max(capacity.saturating_mul(2))
    }
}

/// The bytes a tail of `len` bytes in `capacity` grows by to take `needed`
/// more bytes.
fn tail_growth(len: usize, capacity: usize, needed: usize) -> u64 {
    (next_capacity(len, capacity, needed) - capacity) as u64
}

/// Charge for one index entry of a log keyed by `K`: the map slot,
/// over-allocated as a hash map's growth and load factor leave it.
const fn chain_entry_bytes<K>() -> u64 {
    3 * (std::mem::size_of::<(K, ExtentChain)>() as u64 + 1)
}

/// Reads one chain's frames in append order: every extent from the first,
/// one decoded extent held at a time, then the tail taken with it.
///
/// Holds at most one descriptor, opened only when the chain has extents.
/// The bytes it holds (the taken tail and the current extent) are charged
/// to the log's handle until it drops.
pub(crate) struct ChainReader {
    file: Option<File>,
    /// The file's length when the chain was taken; no extent lies past it.
    file_end: u64,
    /// The next extent to read.
    next: Option<u64>,
    /// The current extent's on-disk payload, when compressed.
    raw: Vec<u8>,
    /// The current extent's frames.
    extent: Vec<u8>,
    extent_pos: usize,
    /// Frames of the current extent not yet read.
    extent_frames: u32,
    tail: Vec<u8>,
    tail_pos: usize,
    /// Frames of the tail not yet read.
    tail_frames: u32,
    in_tail: bool,
    handle: Arc<ConsumerHandle>,
    /// Bytes this reader has charged to `handle`.
    charged: u64,
}

impl ChainReader {
    /// The next frame, or `None` after the last.
    ///
    /// # Errors
    ///
    /// [`PipelineError::Internal`] for a short read or a malformed extent or
    /// frame; [`PipelineError::Io`] of the same kind for any other read
    /// error.
    pub(crate) fn next_frame(&mut self) -> Result<Option<&[u8]>, PipelineError> {
        while !self.in_tail && self.extent_frames == 0 {
            if self.extent_pos != self.extent.len() {
                return Err(malformed("an extent holds bytes past its last frame"));
            }
            match self.next {
                Some(offset) => self.load_extent(offset)?,
                None => {
                    self.raw = Vec::new();
                    self.extent = Vec::new();
                    self.extent_pos = 0;
                    self.recharge();
                    self.in_tail = true;
                }
            }
        }
        let (buffer, pos, frames) = if self.in_tail {
            (&self.tail, &mut self.tail_pos, &mut self.tail_frames)
        } else {
            (&self.extent, &mut self.extent_pos, &mut self.extent_frames)
        };
        if *frames == 0 {
            if *pos != buffer.len() {
                return Err(malformed("a tail holds bytes past its last frame"));
            }
            return Ok(None);
        }
        let frame = split_frame(buffer, pos)?;
        *frames -= 1;
        Ok(Some(frame))
    }

    /// Read and decode the extent at `offset`, making it current.
    fn load_extent(&mut self, offset: u64) -> Result<(), PipelineError> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| malformed("a chain with extents has no open file"))?;
        let header_end = offset
            .checked_add(EXTENT_HEADER_BYTES as u64)
            .filter(|end| *end <= self.file_end)
            .ok_or_else(|| malformed("an extent header lies past the end of the file"))?;
        file.seek(SeekFrom::Start(offset)).map_err(read_error)?;
        let mut header = [0u8; EXTENT_HEADER_BYTES];
        file.read_exact(&mut header).map_err(read_error)?;
        let next = u64::from_le_bytes(header[0..8].try_into().expect("8 bytes"));
        let payload_len = u64::from_le_bytes(header[8..16].try_into().expect("8 bytes"));
        let frames = u32::from_le_bytes(header[16..20].try_into().expect("4 bytes"));
        let compressed = match header[20] {
            0 => false,
            1 => true,
            _ => return Err(malformed("an extent's compression flag is neither 0 nor 1")),
        };
        if header_end
            .checked_add(payload_len)
            .is_none_or(|end| end > self.file_end)
        {
            return Err(malformed(
                "an extent's payload lies past the end of the file",
            ));
        }
        let payload_len = usize::try_from(payload_len)
            .map_err(|_| malformed("an extent's payload does not fit in memory"))?;
        let target = if compressed {
            &mut self.raw
        } else {
            &mut self.extent
        };
        target.clear();
        target.resize(payload_len, 0);
        file.read_exact(target).map_err(read_error)?;
        if compressed {
            self.extent = lz4_flex::block::decompress_size_prepended(&self.raw)
                .map_err(|e| malformed_owned(format!("an extent does not decompress: {e}")))?;
        }
        self.extent_pos = 0;
        self.extent_frames = frames;
        self.next = (next != NO_NEXT).then_some(next);
        self.recharge();
        Ok(())
    }

    /// Move this reader's charge to the bytes it now holds.
    fn recharge(&mut self) {
        let held = (self.tail.capacity() + self.extent.capacity() + self.raw.capacity()) as u64;
        if held >= self.charged {
            self.handle.add_bytes(held - self.charged);
        } else {
            self.handle.sub_bytes(self.charged - held);
        }
        self.charged = held;
    }
}

impl Drop for ChainReader {
    fn drop(&mut self) {
        self.handle.sub_bytes(self.charged);
    }
}

/// The frame at `*pos` of `buffer`, advancing `*pos` past it.
fn split_frame<'b>(buffer: &'b [u8], pos: &mut usize) -> Result<&'b [u8], PipelineError> {
    let start = pos
        .checked_add(FRAME_LENGTH_BYTES)
        .filter(|start| *start <= buffer.len())
        .ok_or_else(|| malformed("a frame length is cut short"))?;
    let length = u32::from_le_bytes(buffer[*pos..start].try_into().expect("4 bytes")) as usize;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= buffer.len())
        .ok_or_else(|| malformed("a frame is cut short"))?;
    *pos = end;
    Ok(&buffer[start..end])
}

fn malformed(detail: &str) -> PipelineError {
    malformed_owned(detail.to_string())
}

fn malformed_owned(detail: String) -> PipelineError {
    PipelineError::Internal {
        op: "extent log read",
        node: String::new(),
        detail,
    }
}

/// A read error: a short read means the file is not what the log wrote.
fn read_error(error: std::io::Error) -> PipelineError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        malformed("an extent is cut short")
    } else {
        PipelineError::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;

    fn arbitrator() -> MemoryArbitrator {
        MemoryArbitrator::with_policy(1 << 30, 0.8, 0.6, Box::new(NoOpPolicy))
    }

    fn log_in(root: &Path, compress: CompressMode) -> ExtentLog<u32> {
        ExtentLog::new(Arc::from(root), compress, ConsumerHandle::new())
    }

    /// Frame `n` of document `doc`: distinct per frame and long enough for
    /// LZ4 to find repeats.
    fn frame(doc: u32, n: u32) -> Vec<u8> {
        format!("doc {doc} frame {n} ")
            .repeat(1 + (n as usize % 5))
            .into_bytes()
    }

    fn read_all(reader: &mut ChainReader) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        while let Some(frame) = reader.next_frame().expect("frame") {
            frames.push(frame.to_vec());
        }
        frames
    }

    fn files_in(root: &Path) -> usize {
        std::fs::read_dir(root).expect("spill root").count()
    }

    #[test]
    fn frames_replay_in_append_order_across_flushes() {
        for compress in [CompressMode::Off, CompressMode::On] {
            let root = tempfile::tempdir().expect("spill root");
            let budget = arbitrator();
            let mut log = log_in(root.path(), compress);
            let mut expected: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
            for n in 0..60u32 {
                for doc in 0..3u32 {
                    // Documents fail at different rates, so their extents
                    // interleave unevenly in the file.
                    if n % (doc + 1) != 0 {
                        continue;
                    }
                    let bytes = frame(doc, n);
                    log.append(&doc, &bytes).expect("append");
                    expected.entry(doc).or_default().push(bytes);
                }
                if n % 7 == 6 {
                    log.flush_all(&budget, "validate").expect("flush");
                }
            }
            assert!(
                budget.cumulative_spill_bytes() > 0,
                "some extents were written"
            );
            for doc in 0..3u32 {
                let mut reader = log.take(&doc).expect("take").expect("a chain");
                assert_eq!(
                    read_all(&mut reader),
                    expected[&doc],
                    "document {doc} replays in append order ({compress:?})"
                );
                assert!(!log.contains(&doc), "a taken chain leaves the index");
            }
        }
    }

    #[test]
    fn one_file_holds_every_document() {
        let root = tempfile::tempdir().expect("spill root");
        let budget = arbitrator();
        let mut log = log_in(root.path(), CompressMode::Auto);
        for doc in 0..1_000u32 {
            log.append(&doc, &frame(doc, 0)).expect("append");
            log.append(&doc, &frame(doc, 1)).expect("append");
            if doc % 10 == 9 {
                log.flush_all(&budget, "validate").expect("flush");
            }
        }
        assert_eq!(files_in(root.path()), 1, "one file however many documents");
        let mut reader = log.take(&500).expect("take").expect("a chain");
        assert_eq!(read_all(&mut reader), [frame(500, 0), frame(500, 1)]);
        drop(reader);
        drop(log);
        assert_eq!(files_in(root.path()), 0, "the file goes with the log");
    }

    #[test]
    fn no_file_until_a_flush() {
        let root = tempfile::tempdir().expect("spill root");
        let handle = ConsumerHandle::new();
        let mut log: ExtentLog<u32> = ExtentLog::new(
            Arc::from(root.path()),
            CompressMode::Auto,
            Arc::clone(&handle),
        );
        for doc in 0..50u32 {
            log.append(&doc, &frame(doc, 0)).expect("append");
        }
        assert_eq!(files_in(root.path()), 0, "appends alone create nothing");
        assert_eq!(
            handle.bytes(),
            log.resident_bytes() + log.index_bytes(),
            "the tails and the index are charged"
        );
        let mut reader = log.take(&7).expect("take").expect("a chain");
        assert_eq!(read_all(&mut reader), [frame(7, 0)]);
        drop(reader);
        assert_eq!(
            files_in(root.path()),
            0,
            "a chain without extents opens no file"
        );
        drop(log);
        assert_eq!(handle.bytes(), 0, "every charge is released");
    }
}
