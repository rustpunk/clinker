//! Binary aggregation spill infrastructure (postcard + LZ4).
//!
//! External-merge machinery the [`HashAggregator`](super::HashAggregator)
//! drives when in-memory group state exceeds the configured budget:
//! per-group payloads ([`SpillState`]) are written as length-prefixed
//! postcard records into LZ4 frame-compressed temp files and read back
//! as [`AggMergeEntry`] for the `LoserTree` k-way merge on finalize.

use serde::{Deserialize, Serialize};

use clinker_record::GroupByKey;

use super::error::HashAggError;
use super::{AggregatorGroupState, BufferedGroupState};

/// Merge entry for the binary aggregation spill path. Ordered by
/// pre-encoded sort key bytes for the `LoserTree` k-way merge.
pub(super) struct AggMergeEntry {
    pub(super) sort_key: Vec<u8>,
    pub(super) group_key: Vec<GroupByKey>,
    pub(super) state: SpillState,
}

impl PartialEq for AggMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}
impl Eq for AggMergeEntry {}
impl PartialOrd for AggMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AggMergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key.cmp(&other.sort_key)
    }
}

/// Per-group payload an `AggSpillEntry` carries. The spill format
/// stays a single entry shape — fold-mode aggregators write only
/// `Folded`, buffer-mode aggregators write only `Buffered`, the merge
/// path branches on the variant. One on-disk format covers both
/// retraction-strategy paths so the spill writer/reader/merger pair is
/// not duplicated per mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpillState {
    /// Fold-mode payload: per-group accumulator row + sidecars.
    Folded(AggregatorGroupState),
    /// Buffer-mode payload: per-row raw contributions + sidecars.
    Buffered(BufferedGroupState),
}

/// Binary spill entry serialized to disk via postcard + LZ4.
#[derive(Serialize, Deserialize)]
pub(super) struct AggSpillEntry {
    pub(super) sort_key: Vec<u8>,
    pub(super) group_key: Vec<GroupByKey>,
    pub(super) state: SpillState,
}

/// Binary aggregation spill writer. Writes `AggSpillEntry` records as
/// length-prefixed postcard payloads into an LZ4 frame-compressed temp
/// file. Postcard has no record-level framing of its own, so each entry
/// is preceded by a 4-byte little-endian u32 holding the encoded
/// payload length. No schema header — the caller knows the group-by
/// layout at construction time.
pub(super) struct AggSpillWriter {
    encoder: lz4_flex::frame::FrameEncoder<std::io::BufWriter<tempfile::NamedTempFile>>,
}

impl AggSpillWriter {
    pub(super) fn new(spill_dir: Option<&std::path::Path>) -> Result<Self, HashAggError> {
        let temp_file = if let Some(dir) = spill_dir {
            tempfile::NamedTempFile::new_in(dir)
        } else {
            tempfile::NamedTempFile::new()
        }
        .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let buf_writer = std::io::BufWriter::new(temp_file);
        let encoder = lz4_flex::frame::FrameEncoder::new(buf_writer);
        Ok(Self { encoder })
    }

    pub(super) fn write_entry(&mut self, entry: &AggSpillEntry) -> Result<(), HashAggError> {
        use std::io::Write;
        let bytes = postcard::to_stdvec(entry).map_err(|e| HashAggError::Spill(e.to_string()))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| HashAggError::Spill("spill entry exceeds 4 GiB".to_string()))?;
        self.encoder
            .write_all(&len.to_le_bytes())
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        self.encoder
            .write_all(&bytes)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<AggSpillFile, HashAggError> {
        let buf_writer = self
            .encoder
            .finish()
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let temp_file = buf_writer
            .into_inner()
            .map_err(|e| HashAggError::Spill(e.into_error().to_string()))?;
        let path = temp_file.into_temp_path();
        Ok(AggSpillFile { path })
    }
}

/// Completed binary spill file handle. Auto-deletes on drop via `TempPath`.
pub struct AggSpillFile {
    path: tempfile::TempPath,
}

impl AggSpillFile {
    pub(super) fn reader(&self) -> Result<AggSpillReader, HashAggError> {
        let file =
            std::fs::File::open(&self.path).map_err(|e| HashAggError::Spill(e.to_string()))?;
        let decoder = lz4_flex::frame::FrameDecoder::new(file);
        let buf_reader = std::io::BufReader::new(decoder);
        Ok(AggSpillReader {
            reader: buf_reader,
            len_buf: [0u8; 4],
        })
    }
}

/// Binary aggregation spill reader. Yields `AggMergeEntry` by reading a
/// 4-byte little-endian length prefix followed by a postcard-encoded
/// payload from an LZ4 frame-compressed file. A clean-boundary EOF on
/// the length prefix terminates the stream cleanly; any other read or
/// decode failure surfaces as `HashAggError::Spill`.
pub(super) struct AggSpillReader {
    reader: std::io::BufReader<lz4_flex::frame::FrameDecoder<std::fs::File>>,
    len_buf: [u8; 4],
}

impl AggSpillReader {
    pub(super) fn next_entry(&mut self) -> Result<Option<AggMergeEntry>, HashAggError> {
        use std::io::Read;
        match self.reader.read_exact(&mut self.len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(HashAggError::Spill(e.to_string())),
        }
        let len = u32::from_le_bytes(self.len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.reader
            .read_exact(&mut buf)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let entry: AggSpillEntry =
            postcard::from_bytes(&buf).map_err(|e| HashAggError::Spill(e.to_string()))?;
        Ok(Some(AggMergeEntry {
            sort_key: entry.sort_key,
            group_key: entry.group_key,
            state: entry.state,
        }))
    }
}
