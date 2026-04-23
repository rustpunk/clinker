//! Source-span infrastructure for diagnostics.
//!
//! - [`FileId`]: 1-based index into a [`SourceDb`], niche-optimized via `NonZeroU32`.
//! - [`Span`]: 12-byte `Copy` value (file + byte offset + length) pointing into
//!   the owning `SourceDb`'s file contents.
//! - [`SourceDb`]: interns loaded YAML (or in-memory buffers) and resolves
//!   spans to 1-based (line, column) pairs for human-readable diagnostics.
//!
//! `SourceDb` also implements [`miette::SourceCode`] so diagnostics can render
//! with `miette`'s fancy reporter.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 1-based file identifier. `NonZeroU32` gives `Option<FileId>` the same size
/// as `FileId`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileId(NonZeroU32);

impl FileId {
    /// Construct from a 1-based index. Primarily used by [`SourceDb`]; tests
    /// may use this directly.
    pub const fn new(raw: NonZeroU32) -> Self {
        Self(raw)
    }

    /// Raw 1-based index.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// A source-code span: 12 bytes, `Copy`, interpreted relative to its
/// [`FileId`]'s contents in a [`SourceDb`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Span {
    pub file: FileId,
    pub start: u32,
    pub len: u32,
}

impl Span {
    /// Synthetic placeholder span for diagnostics produced *before* a file
    /// has been loaded into a [`SourceDb`] (e.g. path-validation errors
    /// reported against a raw `PathBuf`). Uses [`u32::MAX`] as the file id
    /// so it cannot collide with any real [`FileId`] assigned by
    /// `SourceDb::insert`.
    ///
    /// Callers that later learn the owning file and byte range should
    /// replace the diagnostic's primary span with a real one.
    pub const SYNTHETIC: Self = Self {
        file: FileId(match NonZeroU32::new(u32::MAX) {
            Some(v) => v,
            None => unreachable!(),
        }),
        start: 0,
        len: 0,
    };

    /// A zero-length span at `offset`.
    pub const fn point(file: FileId, offset: u32) -> Self {
        Self {
            file,
            start: offset,
            len: 0,
        }
    }

    /// Synthetic span carrying a known 1-based source line but no
    /// interned `SourceDb`/byte-offset. Used by
    /// `PipelineConfig::compile()` stage 5 so `--explain` can render
    /// `(line:N)` annotations against each lowered node without
    /// requiring a `SourceDb` to be threaded through the compile path.
    ///
    /// Encoded as: `file = SYNTHETIC_FILE_ID` (u32::MAX), `start = line`,
    /// `len = 0`. Callers that want the embedded line number use
    /// [`Span::synthetic_line_number`].
    pub const fn line_only(line: u32) -> Self {
        Self {
            file: FileId(match NonZeroU32::new(u32::MAX) {
                Some(v) => v,
                None => unreachable!(),
            }),
            start: line,
            len: 0,
        }
    }

    /// If this span is a [`Span::line_only`]-flavored synthetic span,
    /// return the embedded 1-based line number. Returns `None` for
    /// real (file-backed) spans, for `Span::SYNTHETIC`, and for any
    /// other non-line-only synthetic.
    pub fn synthetic_line_number(self) -> Option<u32> {
        if self.file.get() == u32::MAX && self.len == 0 && self.start > 0 {
            Some(self.start)
        } else {
            None
        }
    }

    /// Exclusive end byte offset.
    pub const fn end(&self) -> u32 {
        self.start + self.len
    }

    /// Convert a [`serde_saphyr::Span`] to a [`Span`] under a given [`FileId`].
    ///
    /// If the saphyr span has no byte-info attached (`byte_info == (0, 0)`,
    /// e.g. it came from a non-string source or a tagged-enum / flatten
    /// context), returns a zero-length span at offset 0 — equivalent to
    /// "we know the file but not the byte position." Real byte ranges
    /// flow through unchanged.
    pub fn from_saphyr(file: FileId, span: serde_saphyr::Span) -> Self {
        let start = span.byte_offset().unwrap_or(0) as u32;
        let len = span.byte_len().unwrap_or(0) as u32;
        Self { file, start, len }
    }
}

/// Owns loaded YAML files (and in-memory buffers) keyed by [`FileId`].
///
/// Files are appended; `FileId` indices are 1-based so `Option<FileId>` stays
/// the same size as `FileId`. Span-to-(line, column) resolution walks the
/// contents on demand — no up-front line index is built.
#[derive(Default)]
pub struct SourceDb {
    files: Vec<LoadedFile>,
}

struct LoadedFile {
    path: Arc<PathBuf>,
    contents: String,
}

impl SourceDb {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load a file from disk and intern its contents. Returns the assigned
    /// [`FileId`].
    ///
    /// Takes a [`crate::security::ValidatedPath`] — the type-level proof that
    /// the path has been through [`crate::security::validate_path`]. This
    /// makes it impossible to `load` an unvalidated, potentially adversarial
    /// path:
    ///
    /// ```compile_fail
    /// use clinker_core::span::SourceDb;
    /// use std::path::PathBuf;
    /// let mut db = SourceDb::new();
    /// // Rejected: `load` will not accept a raw `PathBuf`.
    /// let _ = db.load(PathBuf::from("/etc/passwd"));
    /// ```
    pub fn load(&mut self, path: crate::security::ValidatedPath) -> std::io::Result<FileId> {
        let path = path.into_inner();
        let contents = std::fs::read_to_string(&path)?;
        Ok(self.insert(path, contents))
    }

    /// Intern an in-memory buffer as if it had been loaded from `path`.
    ///
    /// Used by Kiln's unsaved-buffer workflow and by tests that want to avoid
    /// touching the filesystem.
    pub fn insert_in_memory(&mut self, path: PathBuf, contents: String) -> FileId {
        self.insert(path, contents)
    }

    fn insert(&mut self, path: PathBuf, contents: String) -> FileId {
        let raw = u32::try_from(self.files.len() + 1).expect("SourceDb overflowed u32 file count");
        self.files.push(LoadedFile {
            path: Arc::new(path),
            contents,
        });
        FileId(NonZeroU32::new(raw).expect("len+1 is non-zero"))
    }

    /// File contents for `file`.
    pub fn contents(&self, file: FileId) -> &str {
        &self.file(file).contents
    }

    /// Path the file was loaded from (or its synthetic path for in-memory
    /// buffers).
    pub fn path(&self, file: FileId) -> &Path {
        self.file(file).path.as_ref()
    }

    /// Resolve a span to its 1-based `(line, column)` at the span's start.
    ///
    /// Lines are delimited by `'\n'`; the column is a byte column counted from
    /// the start of the line. A span pointing past end-of-file clamps to the
    /// final position.
    pub fn resolve_line_col(&self, span: Span) -> (u32, u32) {
        let contents = self.contents(span.file);
        let offset = (span.start as usize).min(contents.len());
        let before = &contents[..offset];
        let line = 1 + before.bytes().filter(|&b| b == b'\n').count() as u32;
        let line_start = before.rfind('\n').map_or(0, |i| i + 1);
        let col = 1 + (offset - line_start) as u32;
        (line, col)
    }

    fn file(&self, file: FileId) -> &LoadedFile {
        let idx = file.0.get() as usize - 1;
        self.files
            .get(idx)
            .expect("SourceDb::file: FileId out of range")
    }
}

impl miette::SourceCode for SourceDb {
    fn read_span<'a>(
        &'a self,
        span: &miette::SourceSpan,
        context_lines_before: usize,
        context_lines_after: usize,
    ) -> Result<Box<dyn miette::SpanContents<'a> + 'a>, miette::MietteError> {
        // The miette span's offset is interpreted as `file_index * large_stride + byte`
        // would be nice, but miette only gives us a single offset. For now we
        // fall back on the first file; diagnostics that want cross-file
        // rendering should carry their own `NamedSource`.
        let file = self.files.first().ok_or(miette::MietteError::OutOfBounds)?;
        file.contents
            .read_span(span, context_lines_before, context_lines_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_size_is_12_bytes() {
        assert_eq!(std::mem::size_of::<Span>(), 12);
    }

    #[test]
    fn test_source_db_loads_and_resolves_line() {
        let mut db = SourceDb::new();
        let contents = "alpha\nbravo\ncharlie\n".to_string();
        let file = db.insert_in_memory(PathBuf::from("<test>"), contents);

        // "alpha" starts at offset 0 → (1, 1)
        assert_eq!(
            db.resolve_line_col(Span {
                file,
                start: 0,
                len: 5
            }),
            (1, 1)
        );
        // "bravo" starts at offset 6 → (2, 1)
        assert_eq!(
            db.resolve_line_col(Span {
                file,
                start: 6,
                len: 5
            }),
            (2, 1)
        );
        // "arlie" within line 3: 'c' at 12, 'a' at 13 → (3, 2)
        assert_eq!(
            db.resolve_line_col(Span {
                file,
                start: 13,
                len: 5
            }),
            (3, 2)
        );
    }

    #[test]
    fn test_file_id_is_one_based_and_roundtrips() {
        let mut db = SourceDb::new();
        let a = db.insert_in_memory(PathBuf::from("a"), "x".into());
        let b = db.insert_in_memory(PathBuf::from("b"), "y".into());
        assert_eq!(a.get(), 1);
        assert_eq!(b.get(), 2);
        assert_eq!(db.contents(a), "x");
        assert_eq!(db.contents(b), "y");
    }
}
