//! Multi-file streaming reader.
//!
//! [`MultiFileFormatReader`] sequences a list of `(PathBuf, Box<dyn Read>)`
//! files through a per-file format reader factory, presenting them as a
//! single [`FormatReader`] stream to the executor. Each inner reader is
//! built fresh per file via the supplied factory closure.
//!
//! Per-format concat-safety:
//! - **CSV** with `has_header: true` — every inner CsvReader independently
//!   consumes its own header row at schema inference, so no special
//!   skip-header logic is needed at the wrapper level. Subsequent files
//!   must declare the same column set; mismatch surfaces as E217.
//! - **CSV** with `has_header: false` — pure concat, no header semantics.
//! - **NDJSON / JSON array / XML** — record-at-a-time with no per-file
//!   framing visible to the wrapper; pure concatenation is safe.
//! - **Fixed-width** — schema is loaded once from `format_schema`, files
//!   are concatenated.
//!
//! Per-record source-file tagging: [`Self::current_file`] returns the
//! `Arc<str>` for the file that produced the most-recently-emitted
//! record, so the executor's ingestion phase can stamp `$source.file`
//! per row. The `Arc` swap happens automatically as the wrapper advances
//! across file boundaries.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_format::{FormatError, FormatReader};
use clinker_record::{Record, Schema};

/// Closure that builds a [`FormatReader`] from a single file's bytes.
///
/// `file_index` lets the factory differentiate the first file from
/// subsequent files when format-specific bookkeeping requires it
/// (currently unused — every format builds the same way per file).
pub type FactoryFn =
    dyn FnMut(Box<dyn Read + Send>, usize) -> Result<Box<dyn FormatReader>, FormatError> + Send;

/// One file slot in the multi-file stream.
pub struct FileSlot {
    pub path: PathBuf,
    pub reader: Box<dyn Read + Send>,
}

impl FileSlot {
    /// Convenience: wrap an open `Read` handle plus its path into a slot.
    pub fn new(path: impl Into<PathBuf>, reader: Box<dyn Read + Send>) -> Self {
        Self {
            path: path.into(),
            reader,
        }
    }
}

/// Wrap a single `Read` handle as a one-element file list. The path is
/// `"<inline>"` — used by tests and benchmarks that hand the executor
/// in-memory cursors with no real on-disk path. Production callers
/// should construct `FileSlot` directly with the real path so
/// `$source.file` carries it through to records.
impl From<Box<dyn Read + Send>> for FileSlot {
    fn from(reader: Box<dyn Read + Send>) -> Self {
        Self {
            path: PathBuf::from("<inline>"),
            reader,
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
                reader: Box::new(std::io::empty()),
            },
        );
        self.current_file = Arc::from(slot.path.to_string_lossy().into_owned());
        self.cursor += 1;
        let reader = (self.factory)(slot.reader, idx)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};

    fn cursor(s: &str) -> Box<dyn Read + Send> {
        Box::new(std::io::Cursor::new(s.to_string().into_bytes()))
    }

    fn csv_factory() -> Box<FactoryFn> {
        Box::new(
            |reader: Box<dyn Read + Send>,
             _idx: usize|
             -> Result<Box<dyn FormatReader>, FormatError> {
                Ok(Box::new(CsvReader::from_reader(
                    reader,
                    CsvReaderConfig::default(),
                )))
            },
        )
    }

    #[test]
    fn streams_records_across_two_csv_files() {
        let files = vec![
            FileSlot {
                path: PathBuf::from("a.csv"),
                reader: cursor("id,name\n1,alice\n2,bob\n"),
            },
            FileSlot {
                path: PathBuf::from("b.csv"),
                reader: cursor("id,name\n3,carol\n"),
            },
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let _ = r.schema().unwrap();
        let mut count = 0;
        while let Some(_) = r.next_record().unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn current_file_updates_across_boundary() {
        let files = vec![
            FileSlot {
                path: PathBuf::from("a.csv"),
                reader: cursor("id,name\n1,alice\n"),
            },
            FileSlot {
                path: PathBuf::from("b.csv"),
                reader: cursor("id,name\n2,bob\n"),
            },
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
    fn schema_mismatch_across_files_is_rejected() {
        let files = vec![
            FileSlot {
                path: PathBuf::from("a.csv"),
                reader: cursor("id,name\n1,alice\n"),
            },
            FileSlot {
                path: PathBuf::from("b.csv"),
                reader: cursor("id,price\n2,9.99\n"),
            },
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
        let files = vec![FileSlot {
            path: PathBuf::from("only.csv"),
            reader: cursor("id\n1\n2\n3\n"),
        }];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let _ = r.schema().unwrap();
        let mut count = 0;
        while let Some(_) = r.next_record().unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn schema_is_cached_from_first_file() {
        let files = vec![
            FileSlot {
                path: PathBuf::from("a.csv"),
                reader: cursor("id,name\n1,alice\n"),
            },
            FileSlot {
                path: PathBuf::from("b.csv"),
                reader: cursor("id,name\n2,bob\n"),
            },
        ];
        let mut r = MultiFileFormatReader::new(files, csv_factory());
        let s1 = r.schema().unwrap();
        let s2 = r.schema().unwrap();
        assert!(Arc::ptr_eq(&s1, &s2), "schema() should be idempotent");
    }
}
