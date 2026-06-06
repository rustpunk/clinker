//! Binary aggregation spill infrastructure (postcard, optionally LZ4).
//!
//! External-merge machinery the [`HashAggregator`](super::HashAggregator)
//! drives when in-memory group state exceeds the configured budget:
//! per-group payloads ([`SpillState`]) are written as length-prefixed
//! postcard records into temp files — raw or inside an LZ4 frame per the
//! workspace `[storage.spill] compress` knob — and read back as
//! [`AggMergeEntry`] for the `LoserTree` k-way merge on finalize.
//!
//! Each spill file opens with a 1-byte format tag (`0x00` uncompressed,
//! `0x01` LZ4 frame) ahead of the record stream, mirroring the inter-stage
//! [`SpillWriter`](crate::pipeline::spill::SpillWriter): the reader
//! dispatches on the on-disk format without the writer's compression choice
//! travelling out of band. `off` skips the LZ4 frame so small aggregate
//! spills avoid LZ4's per-frame fixed cost (see
//! [`clinker_plan::config::CompressMode`]).

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use clinker_plan::SpillError;
use clinker_record::GroupByKey;

use super::error::HashAggError;
use super::{AggregatorGroupState, BufferedGroupState};

/// On-disk format tag for an uncompressed aggregate spill file: the
/// postcard record stream is written raw, with no LZ4 frame.
const FORMAT_TAG_UNCOMPRESSED: u8 = 0x00;
/// On-disk format tag for an LZ4-frame-compressed aggregate spill file.
const FORMAT_TAG_LZ4: u8 = 0x01;

/// Record-stream sink for an aggregate spill file: either a raw buffered
/// writer (uncompressed) or an LZ4 frame encoder wrapping one. Mirrors the
/// inter-stage spill path's sink; the variant is chosen at construction from
/// the resolved compression decision and recorded in the leading format tag.
enum AggSpillSink {
    /// Uncompressed: postcard records written straight to the buffered file.
    Uncompressed(std::io::BufWriter<tempfile::NamedTempFile>),
    /// LZ4-frame-compressed: the historical default for large spills.
    Lz4(lz4_flex::frame::FrameEncoder<std::io::BufWriter<tempfile::NamedTempFile>>),
}

impl Write for AggSpillSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            AggSpillSink::Uncompressed(w) => w.write(buf),
            AggSpillSink::Lz4(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            AggSpillSink::Uncompressed(w) => w.flush(),
            AggSpillSink::Lz4(e) => e.flush(),
        }
    }
}

impl AggSpillSink {
    /// Finalize the stream and recover the temp file. For LZ4 this flushes
    /// the frame's buffered tail; for the uncompressed sink it flushes the
    /// `BufWriter`.
    fn into_temp_file(self) -> Result<tempfile::NamedTempFile, HashAggError> {
        let buf_writer = match self {
            AggSpillSink::Uncompressed(w) => w,
            AggSpillSink::Lz4(e) => e.finish().map_err(|e| HashAggError::Spill(e.to_string()))?,
        };
        buf_writer
            .into_inner()
            .map_err(|e| HashAggError::Spill(e.into_error().to_string()))
    }
}

/// Record-stream source for an aggregate spill file, mirroring
/// [`AggSpillSink`]: either a raw buffered file (uncompressed) or an LZ4
/// decoder over one. The variant is selected from the file's leading format
/// tag at [`AggSpillFile::reader`].
enum AggSpillSource {
    /// Uncompressed: postcard records read straight from the buffered file.
    Uncompressed(std::io::BufReader<std::fs::File>),
    /// LZ4-frame-compressed.
    Lz4(std::io::BufReader<lz4_flex::frame::FrameDecoder<std::fs::File>>),
}

impl Read for AggSpillSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            AggSpillSource::Uncompressed(r) => r.read(buf),
            AggSpillSource::Lz4(r) => r.read(buf),
        }
    }
}

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
/// length-prefixed postcard payloads into a temp file — raw or inside an
/// LZ4 frame per the resolved `compress` decision, recorded in the leading
/// format tag. Postcard has no record-level framing of its own, so each
/// entry is preceded by a 4-byte little-endian u32 holding the encoded
/// payload length. No schema header — the caller knows the group-by layout
/// at construction time.
pub(super) struct AggSpillWriter {
    sink: AggSpillSink,
}

impl AggSpillWriter {
    /// Create a new spill writer in `spill_dir` (or the OS temp dir when
    /// `None`). `compress` selects the body encoding: `true` wraps the
    /// postcard record stream in an LZ4 frame, `false` writes it raw. The
    /// caller resolves it from the workspace `[storage.spill] compress` knob
    /// (see [`clinker_plan::config::CompressMode`]); the choice is recorded
    /// in the file's leading format tag so the reader dispatches without
    /// out-of-band state.
    pub(super) fn new(
        spill_dir: Option<&std::path::Path>,
        compress: bool,
    ) -> Result<Self, HashAggError> {
        // A create failure in a spill dir the run validated at startup means
        // the directory went bad mid-run (unmounted, removed, remounted
        // read-only). Classify it through the shared `from_spill_dir_io`
        // helper so the aggregate path surfaces the same `DirUnavailable`
        // diagnostic the node-buffer and sort spill paths do, rather than
        // folding a directory fault into a generic spill-failed string. With
        // no configured dir (OS temp), a create failure is environment-level
        // and stays a generic spill error.
        let temp_file = match spill_dir {
            Some(dir) => {
                // Test-only seam: when a test has armed the mid-run spill-root
                // fault for this root, this removes the live spill directory
                // right before the open below, so the open fails with `NotFound`
                // and classifies as `DirUnavailable`. Compiled out of release
                // builds; an unarmed seam is a no-op.
                #[cfg(test)]
                crate::executor::spill_purge::maybe_invalidate_spill_root_for_test(dir);
                tempfile::NamedTempFile::new_in(dir)
                    .map_err(|e| HashAggError::SpillDir(SpillError::from_spill_dir_io(dir, e)))?
            }
            None => {
                tempfile::NamedTempFile::new().map_err(|e| HashAggError::Spill(e.to_string()))?
            }
        };
        let mut buf_writer = std::io::BufWriter::new(temp_file);
        // The format tag is the very first on-disk byte and sits ahead of the
        // (optionally framed) record stream so the reader dispatches before
        // opening a decoder.
        let tag = if compress {
            FORMAT_TAG_LZ4
        } else {
            FORMAT_TAG_UNCOMPRESSED
        };
        buf_writer
            .write_all(&[tag])
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let sink = if compress {
            AggSpillSink::Lz4(lz4_flex::frame::FrameEncoder::new(buf_writer))
        } else {
            AggSpillSink::Uncompressed(buf_writer)
        };
        Ok(Self { sink })
    }

    pub(super) fn write_entry(&mut self, entry: &AggSpillEntry) -> Result<(), HashAggError> {
        let bytes = postcard::to_stdvec(entry).map_err(|e| HashAggError::Spill(e.to_string()))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| HashAggError::Spill("spill entry exceeds 4 GiB".to_string()))?;
        self.sink
            .write_all(&len.to_le_bytes())
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        self.sink
            .write_all(&bytes)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<AggSpillFile, HashAggError> {
        let temp_file = self.sink.into_temp_file()?;
        let path = temp_file.into_temp_path();
        Ok(AggSpillFile { path })
    }
}

/// Completed binary spill file handle. Auto-deletes on drop via `TempPath`.
pub struct AggSpillFile {
    path: tempfile::TempPath,
}

impl AggSpillFile {
    /// Open a reader over this spill file.
    ///
    /// Reads the leading format tag and selects the matching record-stream
    /// source: an LZ4 decoder for `0x01`, the raw file for `0x00`. An unknown
    /// tag is a corrupt-file error.
    pub(super) fn reader(&self) -> Result<AggSpillReader, HashAggError> {
        let mut file =
            std::fs::File::open(&self.path).map_err(|e| HashAggError::Spill(e.to_string()))?;
        // The format tag is a single byte ahead of the (optionally framed)
        // record stream, written by `AggSpillWriter::new`.
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag)
            .map_err(|e| HashAggError::Spill(e.to_string()))?;
        let source = match tag[0] {
            FORMAT_TAG_LZ4 => AggSpillSource::Lz4(std::io::BufReader::new(
                lz4_flex::frame::FrameDecoder::new(file),
            )),
            FORMAT_TAG_UNCOMPRESSED => AggSpillSource::Uncompressed(std::io::BufReader::new(file)),
            other => {
                return Err(HashAggError::Spill(format!(
                    "unknown aggregate spill format tag {other:#04x}; file is corrupt"
                )));
            }
        };
        Ok(AggSpillReader {
            source,
            len_buf: [0u8; 4],
        })
    }
}

/// Binary aggregation spill reader. Yields `AggMergeEntry` by reading a
/// 4-byte little-endian length prefix followed by a postcard-encoded
/// payload, dispatching on the file's format tag for the compressed or
/// uncompressed record stream. A clean-boundary EOF on the length prefix
/// terminates the stream cleanly; any other read or decode failure surfaces
/// as `HashAggError::Spill`.
pub(super) struct AggSpillReader {
    source: AggSpillSource,
    len_buf: [u8; 4],
}

impl AggSpillReader {
    pub(super) fn next_entry(&mut self) -> Result<Option<AggMergeEntry>, HashAggError> {
        match self.source.read_exact(&mut self.len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(HashAggError::Spill(e.to_string())),
        }
        let len = u32::from_le_bytes(self.len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.source
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::error::PipelineError;

    /// LZ4 frame magic number, little-endian `0x184D2204` — the four bytes a
    /// compressed aggregate spill opens with right after the 1-byte tag.
    const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

    /// A trivially-constructed spill entry: empty group key, empty
    /// fold-mode accumulator row. Enough to exercise the framing and the
    /// read path without standing up an `AccumulatorFactory`.
    fn sample_entry(sort_key: Vec<u8>) -> AggSpillEntry {
        AggSpillEntry {
            sort_key,
            group_key: Vec::new(),
            state: SpillState::Folded(AggregatorGroupState::new(Vec::new())),
        }
    }

    // The leading format tag must record the writer's compression choice and
    // the body must honor it: an `off` aggregate spill is byte-raw (no LZ4
    // frame magic after the tag) and an `on` one is LZ4-framed. Without this
    // the `--explain` per-operator compress line is a falsehood for the
    // hash-aggregation operator, which hardcoded LZ4 before the knob reached
    // this writer.
    #[test]
    fn format_tag_and_body_honor_compress_knob() {
        for (compress, expected_tag) in [(true, FORMAT_TAG_LZ4), (false, FORMAT_TAG_UNCOMPRESSED)] {
            let mut writer = AggSpillWriter::new(None, compress).unwrap();
            writer.write_entry(&sample_entry(vec![1, 2, 3])).unwrap();
            let file = writer.finish().unwrap();
            let bytes = std::fs::read(&file.path).unwrap();
            assert_eq!(
                bytes[0], expected_tag,
                "compress={compress} must write tag {expected_tag:#04x}"
            );
            // The four bytes after the tag are the LZ4 frame magic only when
            // compressed; an `off` body opens straight into the first
            // length-prefixed postcard entry, never the frame magic.
            let body_head = &bytes[1..5];
            if compress {
                assert_eq!(
                    body_head, &LZ4_FRAME_MAGIC,
                    "on-mode body must be LZ4-framed"
                );
            } else {
                assert_ne!(
                    body_head, &LZ4_FRAME_MAGIC,
                    "off-mode body must be byte-raw, with no LZ4 frame magic"
                );
            }
        }
    }

    // Entries round-trip through both the compressed and the uncompressed
    // format identically: the knob changes only on-disk bytes, never the
    // decoded entries, so the k-way merge sees the same group order either way.
    #[test]
    fn entries_roundtrip_in_both_modes() {
        for compress in [true, false] {
            let mut writer = AggSpillWriter::new(None, compress).unwrap();
            for i in 0u8..5 {
                writer.write_entry(&sample_entry(vec![i])).unwrap();
            }
            let file = writer.finish().unwrap();
            let mut reader = file.reader().unwrap();
            let mut read_keys = Vec::new();
            while let Some(entry) = reader.next_entry().unwrap() {
                read_keys.push(entry.sort_key);
            }
            assert_eq!(
                read_keys,
                (0u8..5).map(|i| vec![i]).collect::<Vec<_>>(),
                "compress={compress}: entries must round-trip in input order"
            );
        }
    }

    #[test]
    fn spill_dir_removed_mid_run_yields_distinct_diagnostic() {
        // Mirror of the node-buffer/sort spill regression: a configured
        // aggregate spill dir that vanishes after startup validation must
        // surface the distinct `DirUnavailable` diagnostic, not a generic
        // `HashAggError::Spill(String)` that folds into an opaque internal
        // error. This is the aggregate half of the documented "Aggregate,
        // sort, and grace-hash Combine all surface the directory-level
        // cause" guarantee.
        let scratch = tempfile::tempdir().unwrap();
        let spill_dir = scratch.path().join("agg-spill-root");
        std::fs::create_dir(&spill_dir).unwrap();
        std::fs::remove_dir(&spill_dir).unwrap();

        let err = match AggSpillWriter::new(Some(&spill_dir), true) {
            Ok(_) => panic!("AggSpillWriter::new should fail when the dir is gone"),
            Err(e) => e,
        };
        assert!(
            matches!(
                &err,
                HashAggError::SpillDir(SpillError::DirUnavailable { .. })
            ),
            "expected SpillDir(DirUnavailable), got {err:?}"
        );

        // The mapping into the top-level error must land on the spill
        // arm so the rendered diagnostic is the directory-level message,
        // not a generic `Internal`.
        let pipeline_err: PipelineError = err.into();
        assert!(
            matches!(
                pipeline_err,
                PipelineError::Spill(SpillError::DirUnavailable { .. })
            ),
            "expected PipelineError::Spill(DirUnavailable), got {pipeline_err:?}"
        );
        let rendered = pipeline_err.to_string();
        assert!(
            rendered.contains("became unavailable mid-run"),
            "{rendered}"
        );
    }
}
