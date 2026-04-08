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
    /// A zero-length span at `offset`.
    pub const fn point(file: FileId, offset: u32) -> Self {
        Self {
            file,
            start: offset,
            len: 0,
        }
    }

    /// Exclusive end byte offset.
    pub const fn end(&self) -> u32 {
        self.start + self.len
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
    /// This takes a raw [`PathBuf`] for now; Task 16b.1.5 will replace this
    /// with a `ValidatedPath` newtype.
    pub fn load(&mut self, path: PathBuf) -> std::io::Result<FileId> {
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
