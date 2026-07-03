//! Multi-file streaming reader.
//!
//! [`MultiFileFormatReader`] sequences a list of [`FileSlot`]s (each a
//! provenance path plus a re-openable byte source) through a per-file format
//! reader factory, presenting them as a single [`FormatReader`] stream to the
//! executor. Each inner reader is built fresh per file via the supplied factory
//! closure.
//!
//! Per-format concat-safety:
//! - **CSV** with `has_header: true` — every inner CsvReader independently
//!   consumes its own header row at schema inference, so no special
//!   skip-header logic is needed at the wrapper level. Subsequent files
//!   must declare the same column set; mismatch surfaces as E217.
//! - **CSV** with `has_header: false` — pure concat, no header semantics.
//! - **NDJSON / JSON array / XML** — record-at-a-time with no per-file
//!   framing visible to the wrapper; pure concatenation is safe.
//! - **Fixed-width** — the unified `schema:` is resolved once at compile
//!   time, files are concatenated.
//!
//! Per-record source-file tagging: [`Self::current_file`] returns the
//! `Arc<str>` for the file that produced the most-recently-emitted
//! record, so the executor's ingestion phase can stamp `$source.file`
//! per row. The `Arc` swap happens automatically as the wrapper advances
//! across file boundaries.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_format::{FormatError, FormatReader, ReopenableSource};
use clinker_record::{Record, Schema};

/// Closure that builds a [`FormatReader`] from a single file's re-openable
/// byte source.
///
/// The factory receives a [`ReopenableSource`] rather than a one-shot
/// `Box<dyn Read>` so a reader needing multiple passes (JSON's envelope
/// pre-scan plus its body stream) can open the source twice without buffering
/// the whole file. One-pass formats (CSV, fixed-width, XML, EDI) open it once
/// and ignore the re-open capability.
///
/// `file_index` lets the factory differentiate the first file from
/// subsequent files when format-specific bookkeeping requires it
/// (currently unused — every format builds the same way per file).
pub type FactoryFn =
    dyn FnMut(ReopenableSource, usize) -> Result<Box<dyn FormatReader>, FormatError> + Send;

/// One file slot in the multi-file stream.
///
/// `path` is the record-stamping provenance identity (`$source.file`), which
/// for a staged source is the *original* matched path. `source` is the
/// re-openable handle on the actual bytes the reader streams — for a staged
/// source, the content-addressed staged copy, distinct from `path`.
pub struct FileSlot {
    pub path: PathBuf,
    pub source: ReopenableSource,
}

impl FileSlot {
    /// Wrap a one-shot `Read` handle plus its path into a slot.
    ///
    /// For in-memory inputs (test/bench cursors, the `<empty>` slot) that have
    /// no on-disk path to re-open: the reader is held lazily as a
    /// `ReopenableSource::OneShot` and streamed directly by a one-pass format,
    /// so a paced/slow reader keeps its per-row timing and nothing is read at
    /// slot construction. A multi-pass reader (JSON) buffers it on demand.
    /// File-backed production sources use [`from_path`](Self::from_path)
    /// instead, which re-opens by path and never buffers the file whole.
    pub fn new(path: impl Into<PathBuf>, reader: Box<dyn Read + Send>) -> Self {
        Self {
            path: path.into(),
            source: ReopenableSource::one_shot(reader),
        }
    }

    /// Build a slot for a file-backed source: `provenance` is the originating
    /// path stamped on records (`$source.file`), `read_path` is the stable
    /// (staged) path the reader re-opens. The reader opens `read_path` fresh
    /// per pass — no whole-file buffer is held.
    pub fn from_path(provenance: impl Into<PathBuf>, read_path: impl Into<PathBuf>) -> Self {
        Self {
            path: provenance.into(),
            source: ReopenableSource::path(read_path),
        }
    }
}

/// Wrap a list of files behind a single [`FormatReader`] facade.
///
/// The wrapper holds files in *forward* order and advances through them
/// as the active inner reader EOFs. Schema is taken from file 0; every
/// subsequent file's schema must match (column names + count) or
/// [`FormatError::Schema`] is returned (mapped to E217 by the executor).
pub struct MultiFileFormatReader {
    files: Vec<FileSlot>,
    /// Index of the next file to open. `cursor == files.len()` means
    /// every file has been drained.
    cursor: usize,
    /// Active inner reader. `None` until `schema()` or `next_record()`
    /// pulls from the first file.
    active: Option<Box<dyn FormatReader>>,
    /// `Arc<str>` for the file that produced the most-recently-emitted
    /// record. Updated when advancing to a new file.
    current_file: Arc<str>,
    /// Cached schema from file 0. Compared against subsequent files'
    /// schemas; mismatch fails fast.
    schema: Option<Arc<Schema>>,
    /// Per-file factory.
    factory: Box<FactoryFn>,
}

impl MultiFileFormatReader {
    /// Build a new multi-file wrapper.
    ///
    /// `files` must be non-empty; an empty file set is the discovery
    /// layer's responsibility (it routes through `on_no_match`).
    /// `factory` is called once per file to construct the inner reader.
    pub fn new(files: Vec<FileSlot>, factory: Box<FactoryFn>) -> Self {
        debug_assert!(
            !files.is_empty(),
            "MultiFileFormatReader requires at least one file; \
             empty file sets must be handled by the on_no_match policy"
        );
        let initial_file: Arc<str> = files
            .first()
            .map(|f| Arc::from(f.path.to_string_lossy().into_owned()))
            .unwrap_or_else(|| Arc::from(""));
        Self {
            files,
            cursor: 0,
            active: None,
            current_file: initial_file,
            schema: None,
            factory,
        }
    }

    /// Shared `Arc<str>` for the file that produced the most-recently-
    /// emitted record. The executor's ingestion phase clones this Arc
    /// per-record into a parallel provenance vector so dispatch sites
    /// can stamp `$source.file` correctly across file boundaries.
    pub fn current_file(&self) -> &Arc<str> {
        &self.current_file
    }

    /// Open the next file's inner reader and update `current_file`.
    /// Returns `Ok(true)` if a new reader was opened, `Ok(false)` if no
    /// files remain.
    fn advance(&mut self) -> Result<bool, FormatError> {
        if self.cursor >= self.files.len() {
            return Ok(false);
        }
        let idx = self.cursor;
        let slot = std::mem::replace(
            &mut self.files[idx],
            FileSlot {
                path: PathBuf::new(),
                source: ReopenableSource::one_shot(Box::new(std::io::empty())),
            },
        );
        self.current_file = Arc::from(slot.path.to_string_lossy().into_owned());
        self.cursor += 1;
        let reader = (self.factory)(slot.source, idx)?;
        self.active = Some(reader);
        Ok(true)
    }

    /// Verify that `candidate` matches the cached schema. Schema equality
    /// is column-name-and-count based — value coercion happens later via
    /// `CoercingReader`.
    fn assert_schema_match(&self, candidate: &Schema) -> Result<(), FormatError> {
        let Some(ref expected) = self.schema else {
            return Ok(());
        };
        let exp_names: Vec<&str> = expected.columns().iter().map(|c| c.as_ref()).collect();
        let cand_names: Vec<&str> = candidate.columns().iter().map(|c| c.as_ref()).collect();
        if exp_names != cand_names {
            return Err(FormatError::SchemaInference(format!(
                "multi-file source: schema mismatch at file {:?} — \
                 expected columns {:?}, got {:?}",
                self.current_file, exp_names, cand_names,
            )));
        }
        Ok(())
    }
}

impl FormatReader for MultiFileFormatReader {
    fn current_source_file(&self) -> Option<&Arc<str>> {
        Some(&self.current_file)
    }

    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }
        if self.active.is_none() && !self.advance()? {
            return Err(FormatError::SchemaInference(
                "multi-file source has no files to read".into(),
            ));
        }
        let schema = self
            .active
            .as_mut()
            .expect("advance() set active or returned false")
            .schema()?;
        self.schema = Some(Arc::clone(&schema));
        Ok(schema)
    }

    fn prepare_document(
        &mut self,
        config: &clinker_format::EnvelopeConfig,
    ) -> Result<indexmap::IndexMap<Box<str>, clinker_record::Value>, FormatError> {
        // Each file is its own document; forward the pre-scan to the
        // active per-file reader. The executor's ingest loop calls this
        // once per file (at each `current_source_file` transition), so
        // `active` is the file currently being streamed.
        if self.active.is_none() && !self.advance()? {
            return Ok(indexmap::IndexMap::new());
        }
        self.active
            .as_mut()
            .expect("advance() set active or returned false")
            .prepare_document(config)
    }

    fn take_envelope_events(&mut self) -> Vec<clinker_format::EnvelopeEvent> {
        // Forward the active per-file reader's nested-envelope boundaries.
        // Each file is a self-contained document, so its inner levels open
        // and close entirely within that file's stream; the wrapper never
        // bridges a level across a file boundary. Before any file is
        // materialized there are no events to drain.
        match self.active.as_mut() {
            Some(active) => active.take_envelope_events(),
            None => Vec::new(),
        }
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        // Materialize file 0 if we haven't yet.
        if self.active.is_none() {
            if !self.advance()? {
                return Ok(None);
            }
            // Cache file 0's schema if `schema()` hasn't been called yet.
            if self.schema.is_none() {
                let s = self.active.as_mut().unwrap().schema()?;
                self.schema = Some(s);
            }
        }
        loop {
            let active = self
                .active
                .as_mut()
                .expect("active reader present after advance");
            match active.next_record()? {
                Some(record) => return Ok(Some(record)),
                None => {
                    // Inner reader EOF'd; advance to next file.
                    if !self.advance()? {
                        return Ok(None);
                    }
                    // Verify schema against the cached one before
                    // emitting any records from this file.
                    let new_schema = self.active.as_mut().unwrap().schema()?;
                    self.assert_schema_match(&new_schema)?;
                }
            }
        }
    }

    fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
        // Drop the active (dead-lettered, trailer-reached) inner reader and
        // open the next slot. A structural-count failure fires at the file's
        // closing trailer, so the abandoned file is fully consumed — nothing
        // unread is lost. Verify the new file's schema before its records
        // flow, exactly as the EOF-driven advance in `next_record` does.
        self.active = None;
        if !self.advance()? {
            return Ok(false);
        }
        let new_schema = self.active.as_mut().unwrap().schema()?;
        self.assert_schema_match(&new_schema)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};

    fn slot(path: &str, s: &str) -> FileSlot {
        FileSlot::new(
            PathBuf::from(path),
            Box::new(std::io::Cursor::new(s.to_string().into_bytes())),
        )
    }

    fn csv_factory() -> Box<FactoryFn> {
        Box::new(
            |source: ReopenableSource, _idx: usize| -> Result<Box<dyn FormatReader>, FormatError> {
                Ok(Box::new(CsvReader::from_reader(
                    source.open()?,
                    CsvReaderConfig::default(),
                )))
            },
        )
    }

    #[test]
    fn streams_records_across_two_csv_files() {
        let files = vec![
            slot("a.csv", "id,name\n1,alice\n2,bob\n"),
            slot("b.csv", "id,name\n3,carol\n"),
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let _ = r.schema().unwrap();
        let mut count = 0;
        while r.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn current_file_updates_across_boundary() {
        let files = vec![
            slot("a.csv", "id,name\n1,alice\n"),
            slot("b.csv", "id,name\n2,bob\n"),
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        r.next_record().unwrap();
        let after_first = r.current_file().to_string();
        assert!(after_first.ends_with("a.csv"));
        r.next_record().unwrap();
        let after_second = r.current_file().to_string();
        assert!(after_second.ends_with("b.csv"));
    }

    #[test]
    fn advance_to_next_file_abandons_current_and_opens_next() {
        // `advance_to_next_file` drops the active file (skipping any of its
        // unread records) and opens the next, updating `current_file`. This is
        // the seam the ingest driver uses to keep streaming past a file it
        // dead-lettered for a structural-count failure. Returns Ok(false) once
        // no files remain.
        let files = vec![
            slot("a.csv", "id,name\n1,alice\n2,bob\n"),
            slot("b.csv", "id,name\n3,carol\n"),
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        // Read one record of file a, then abandon a's remaining record (id=2).
        assert!(r.next_record().unwrap().is_some());
        assert!(r.current_file().to_string().ends_with("a.csv"));
        assert!(
            r.advance_to_next_file().unwrap(),
            "a next file (b) opens, so advance reports true"
        );
        assert!(
            r.current_file().to_string().ends_with("b.csv"),
            "current_file moves to the next file after advancing"
        );
        // Only file b's record (carol) remains — a's unread tail (bob) is gone.
        let mut remaining: Vec<String> = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            if let clinker_record::Value::String(s) = &rec.values()[1] {
                remaining.push(s.as_str().to_string());
            }
        }
        assert_eq!(
            remaining,
            vec!["carol".to_string()],
            "file a's unread record was abandoned by the advance; only file b streams"
        );
        // No files remain, so a further advance reports false.
        assert!(
            !r.advance_to_next_file().unwrap(),
            "advancing past the last file reports false"
        );
    }

    #[test]
    fn schema_mismatch_across_files_is_rejected() {
        let files = vec![
            slot("a.csv", "id,name\n1,alice\n"),
            slot("b.csv", "id,price\n2,9.99\n"),
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let _ = r.schema().unwrap();
        // First file's record is fine.
        assert!(r.next_record().unwrap().is_some());
        // Boundary into file 2: schema mismatch should fail fast.
        match r.next_record() {
            Err(FormatError::SchemaInference(msg)) => {
                assert!(msg.contains("schema mismatch"), "got: {msg}");
            }
            other => panic!("expected schema-mismatch error, got {other:?}"),
        }
    }

    #[test]
    fn single_file_streams_unchanged() {
        let files = vec![slot("only.csv", "id\n1\n2\n3\n")];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let _ = r.schema().unwrap();
        let mut count = 0;
        while r.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn schema_is_cached_from_first_file() {
        let files = vec![
            slot("a.csv", "id,name\n1,alice\n"),
            slot("b.csv", "id,name\n2,bob\n"),
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let s1 = r.schema().unwrap();
        let s2 = r.schema().unwrap();
        assert!(Arc::ptr_eq(&s1, &s2), "schema() should be idempotent");
    }
}
