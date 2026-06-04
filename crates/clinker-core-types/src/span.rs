//! Source-span value types for diagnostics.
//!
//! - [`FileId`]: 1-based index into a source database, niche-optimized via
//!   `NonZeroU32`.
//! - [`Span`]: 12-byte `Copy` value (file + byte offset + length) pointing into
//!   the contents of the file identified by its [`FileId`].
//!
//! These are the value types passed around between pipeline crates. The
//! stateful source interner that resolves spans to `(line, column)` pairs lives
//! one layer up, in the orchestration crate, because it is coupled to that
//! crate's path-security chokepoint.

use std::num::NonZeroU32;

/// 1-based file identifier. `NonZeroU32` gives `Option<FileId>` the same size
/// as `FileId`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileId(NonZeroU32);

impl FileId {
    /// Construct from a 1-based index. Primarily used by the source interner;
    /// tests may use this directly.
    pub const fn new(raw: NonZeroU32) -> Self {
        Self(raw)
    }

    /// Raw 1-based index.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// A source-code span: 12 bytes, `Copy`, interpreted relative to its
/// [`FileId`]'s contents in a source database.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Span {
    pub file: FileId,
    pub start: u32,
    pub len: u32,
}

impl Span {
    /// Synthetic placeholder span for diagnostics produced *before* a file
    /// has been interned (e.g. path-validation errors reported against a raw
    /// `PathBuf`). Uses [`u32::MAX`] as the file id so it cannot collide with
    /// any real [`FileId`] assigned by the source interner.
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
    /// interned source database / byte-offset. Used by the compile path's
    /// node-lowering stage so `--explain` can render `(line:N)` annotations
    /// against each lowered node without requiring a source database to be
    /// threaded through the compile path.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_size_is_12_bytes() {
        assert_eq!(std::mem::size_of::<Span>(), 12);
    }

    #[test]
    fn test_file_id_is_one_based_and_roundtrips() {
        let a = FileId::new(NonZeroU32::new(1).unwrap());
        let b = FileId::new(NonZeroU32::new(2).unwrap());
        assert_eq!(a.get(), 1);
        assert_eq!(b.get(), 2);
    }

    #[test]
    fn test_synthetic_line_number_roundtrips() {
        assert_eq!(Span::line_only(42).synthetic_line_number(), Some(42));
        assert_eq!(Span::SYNTHETIC.synthetic_line_number(), None);
        assert_eq!(
            Span::point(FileId::new(NonZeroU32::new(1).unwrap()), 7).synthetic_line_number(),
            None
        );
    }
}
