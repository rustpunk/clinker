//! Re-openable byte source backing a format reader.
//!
//! A [`ReopenableSource`] decouples a reader from a single `Box<dyn Read>`
//! handle so a reader that needs multiple passes (JSON's envelope pre-scan plus
//! its body stream) can open the bytes more than once without buffering the
//! whole input — while a one-pass reader (CSV, XML, fixed-width, EDI) consumes
//! the bytes lazily, one streamed `Read` and no buffer.
//!
//! Three shapes, each honest about its memory:
//! - [`ReopenableSource::Path`] re-opens a stable on-disk path. With staging
//!   enabled the input is a content-addressed copy held under an advisory read
//!   lock for the run, so two opens read byte-identical content; with staging
//!   off the original path is re-opened directly. Either way the batch model
//!   requires inputs to stay stable for the run's duration, and a multi-pass
//!   reader guards that with [`SourceIdentity`] — a `(len, mtime)` snapshot
//!   taken at each open that fails loud if the file changed between passes,
//!   rather than splicing a stale envelope onto a freshly-read body. Memory is
//!   O(1) per open — no whole-file buffer is ever held.
//! - [`ReopenableSource::OneShot`] wraps a single pathless `Box<dyn Read>`
//!   (a test/bench cursor, the `<empty>` slot, a network body). It is consumed
//!   *lazily*: the first [`open`](Self::open) hands out the reader as-is, so a
//!   one-pass format streams it without buffering and a paced/slow reader keeps
//!   its streaming timing. A second pass is only possible after
//!   [`into_reopenable`](Self::into_reopenable) buffers it.
//! - [`ReopenableSource::Buffered`] holds bytes in a shared `Arc<[u8]>`, handing
//!   out cursors. A `OneShot` becomes `Buffered` on demand when a multi-pass
//!   reader (JSON) needs a second open; bounded because pathless inputs are
//!   small by construction.

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// A cheap `(len, mtime)` snapshot of a re-opened source's content, taken off
/// the *open* handle (not the path) so it reflects the bytes that open will
/// read.
///
/// This is a courtesy guard against an accidental mid-run input rewrite under
/// Clinker's finite-batch input-stability contract — not a security boundary
/// and not a content fingerprint.
///
/// What it catches: a file replaced or truncated to a different length, or
/// touched to a newer mtime, in the window between a reader's two opens (the
/// body open at construction and the envelope pre-scan open in
/// `prepare_document`). Cheap — no bytes are read.
///
/// What it does NOT catch (accepted residuals; inputs must stay stable for the
/// duration of a finite batch run):
/// - A same-length, same-mtime-*tick* in-place rewrite. On coarse-granularity
///   filesystems (FAT/exFAT at 2 s) or for a truncate-and-rewrite completing
///   inside one mtime tick, the snapshot is unchanged. Closing this would
///   require hashing the whole file, which reintroduces the buffer this design
///   removes, so it is deliberately left open.
/// - A rewrite landing *after* the pre-scan, while the body still streams
///   lazily through `next_record`. The guard runs once, at the pre-scan open;
///   later body reads are not re-checked.
///
/// `Buffered` snapshots its byte length (immutable, always self-consistent);
/// `OneShot` is not re-openable, so its identity never takes part in a
/// cross-pass check.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SourceIdentity {
    len: u64,
    mtime: Option<SystemTime>,
}

/// A byte source a format reader can open, re-openably for file/buffered shapes
/// and once-lazily for a pathless one-shot reader.
///
/// A reader needing two passes (JSON's pre-scan plus body) first calls
/// [`into_reopenable`](Self::into_reopenable) so every [`open`](Self::open)
/// thereafter yields an independent `Read`; a one-pass reader just calls
/// [`open`](Self::open) once and streams it.
pub enum ReopenableSource {
    /// Re-open a stable filesystem path. Each open is a fresh `std::fs::File`;
    /// for a staged, advisory-locked source two opens read identical bytes.
    Path(PathBuf),
    /// Stream from shared in-memory bytes. Each open is a fresh cursor over the
    /// same `Arc<[u8]>`. Re-openable; bounded because such inputs are small.
    Buffered(Arc<[u8]>),
    /// A single pathless `Box<dyn Read>`, consumed lazily on the first open.
    /// The `Mutex<Option<..>>` lets `open(&self)` take the reader without
    /// buffering, preserving a slow/paced reader's streaming timing for a
    /// one-pass format. `None` after the reader has been taken or buffered.
    OneShot(Mutex<Option<Box<dyn Read + Send>>>),
}

impl ReopenableSource {
    /// Wrap a one-shot pathless reader, consumed lazily on first open.
    ///
    /// No bytes are read here — a one-pass format streams the reader directly,
    /// so a slow/paced reader keeps its per-row timing. JSON, which needs two
    /// passes, calls [`into_reopenable`](Self::into_reopenable) to buffer it.
    pub fn one_shot(reader: Box<dyn Read + Send>) -> Self {
        ReopenableSource::OneShot(Mutex::new(Some(reader)))
    }

    /// Build a buffered source by draining a one-shot reader into shared bytes.
    ///
    /// Used when a multi-pass reader needs re-openability over pathless bytes:
    /// the reader is captured once into an `Arc<[u8]>` replayed on every
    /// [`open`](Self::open). Blocking until the reader EOFs; memory is the
    /// drained size, bounded by these inputs being small.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if draining the reader fails.
    pub fn buffer<R: Read>(mut reader: R) -> std::io::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(ReopenableSource::Buffered(Arc::from(bytes)))
    }

    /// Build a path-backed source that re-opens `path` fresh on every open.
    pub fn path(path: impl Into<PathBuf>) -> Self {
        ReopenableSource::Path(path.into())
    }

    /// Convert into a shape that supports repeated [`open`](Self::open) calls.
    ///
    /// `Path` and `Buffered` are already re-openable and pass through untouched
    /// (no read). A `OneShot` is drained once into a `Buffered` so a multi-pass
    /// reader (JSON's pre-scan + body) can open it twice. The drain runs at the
    /// caller's site (the executor's per-file factory for JSON), not at slot
    /// construction, so a one-pass format never triggers it.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if draining a `OneShot` fails.
    /// Returns [`std::io::ErrorKind::InvalidInput`] if a `OneShot`'s reader was
    /// already taken by a prior [`open`](Self::open) — a multi-pass reader must
    /// convert before opening. A poisoned reader slot (a panic mid-open on
    /// another thread) surfaces as an opaque [`std::io::Error`].
    pub fn into_reopenable(self) -> std::io::Result<Self> {
        match self {
            ReopenableSource::OneShot(slot) => {
                let reader = slot
                    .into_inner()
                    .map_err(|_| std::io::Error::other("one-shot reader slot poisoned"))?
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "one-shot reader already taken; convert via into_reopenable \
                             before the first open",
                        )
                    })?;
                Self::buffer(reader)
            }
            already => Ok(already),
        }
    }

    /// Open a `Read` over the source's bytes, positioned at the start.
    ///
    /// `Path` opens a fresh file (O(1) memory); `Buffered` cursors the shared
    /// `Arc<[u8]>`; `OneShot` hands out its lazy reader on the first call.
    /// Multi-pass callers convert via [`into_reopenable`](Self::into_reopenable)
    /// first so every open yields an independent `Read`.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if a `Path` source fails to
    /// open, or [`std::io::ErrorKind::InvalidInput`] if a `OneShot` is opened
    /// more than once without first converting via
    /// [`into_reopenable`](Self::into_reopenable). `Buffered` never fails.
    pub fn open(&self) -> std::io::Result<Box<dyn Read + Send>> {
        Ok(self.open_with_identity()?.0)
    }

    /// Open a `Read` and snapshot the content identity of the bytes it will
    /// read, for a multi-pass reader that must detect the input changing
    /// between passes.
    ///
    /// The [`SourceIdentity`] is stat-ed off the open handle for a `Path`
    /// source, the byte length for a `Buffered` source, and an empty (always
    /// self-consistent) snapshot for a `OneShot`. A reader captures it on its
    /// first pass and compares it on later passes via
    /// [`SourceIdentity::ensure_matches`].
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if a `Path` source fails to
    /// open or its metadata cannot be read, or
    /// [`std::io::ErrorKind::InvalidInput`] if a `OneShot` is opened more than
    /// once without first converting via
    /// [`into_reopenable`](Self::into_reopenable).
    pub fn open_with_identity(&self) -> std::io::Result<(Box<dyn Read + Send>, SourceIdentity)> {
        match self {
            ReopenableSource::Path(path) => {
                let file = open_shared(path)?;
                // Stat the open handle, not the path, so the identity reflects
                // exactly the bytes this handle reads even if the path is
                // concurrently replaced.
                let meta = file.metadata()?;
                let identity = SourceIdentity {
                    len: meta.len(),
                    mtime: meta.modified().ok(),
                };
                Ok((Box::new(file), identity))
            }
            ReopenableSource::Buffered(bytes) => {
                let identity = SourceIdentity {
                    len: bytes.len() as u64,
                    mtime: None,
                };
                Ok((Box::new(Cursor::new(Arc::clone(bytes))), identity))
            }
            ReopenableSource::OneShot(slot) => {
                let reader = slot
                    .lock()
                    .map_err(|_| std::io::Error::other("one-shot reader slot poisoned"))?
                    .take()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "one-shot source opened twice; convert via into_reopenable first",
                        )
                    })?;
                Ok((
                    reader,
                    SourceIdentity {
                        len: 0,
                        mtime: None,
                    },
                ))
            }
        }
    }
}

impl SourceIdentity {
    /// Confirm this identity matches `prior`, the snapshot from the reader's
    /// first pass. A mismatch means a path-backed input was rewritten between
    /// passes — the envelope and body would otherwise be spliced from different
    /// content — so it is surfaced loud rather than silently accepted.
    ///
    /// `Buffered`/`OneShot` sources are immutable across passes (or not
    /// re-opened), so a check over them never trips.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] of kind [`std::io::ErrorKind::Other`] naming
    /// the change — both the length and the modified time, so an mtime-only
    /// change (same length, touched timestamp) reads as a timestamp change
    /// rather than a self-contradictory "500 bytes → 500 bytes" — when
    /// `self != prior`.
    pub fn ensure_matches(&self, prior: &SourceIdentity) -> std::io::Result<()> {
        if self == prior {
            return Ok(());
        }
        Err(std::io::Error::other(format!(
            "source file changed between the envelope pre-scan and the body read \
             (was {} bytes, mtime {:?}; now {} bytes, mtime {:?}); inputs must stay \
             stable for the duration of a run",
            prior.len, prior.mtime, self.len, self.mtime
        )))
    }
}

/// Open a file for reading with cross-process-friendly share semantics.
///
/// On Windows, opening with `FILE_SHARE_DELETE` (alongside read/write) lets a
/// concurrent atomic-rename publish or delete of a staged source proceed while
/// this read handle stays valid — matching the POSIX semantics where an
/// open file survives a concurrent `rename`/`unlink`. Each re-open of a staged
/// path goes through here so every pass shares those semantics. On Unix this is
/// a plain `File::open`; an open fd already survives a concurrent rename/unlink.
fn open_shared(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // winnt.h: FILE_SHARE_READ = 0x1, FILE_SHARE_WRITE = 0x2,
        // FILE_SHARE_DELETE = 0x4. DELETE is the load-bearing one.
        const FILE_SHARE_READ: u32 = 0x1;
        const FILE_SHARE_WRITE: u32 = 0x2;
        const FILE_SHARE_DELETE: u32 = 0x4;
        opts.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
    }
    opts.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_yields_two_independent_reads_over_identical_bytes() {
        let src = ReopenableSource::buffer(Cursor::new(b"hello world".to_vec())).unwrap();
        let mut a = String::new();
        let mut b = String::new();
        src.open().unwrap().read_to_string(&mut a).unwrap();
        src.open().unwrap().read_to_string(&mut b).unwrap();
        assert_eq!(a, "hello world");
        assert_eq!(a, b, "two opens read identical bytes");
    }

    #[test]
    fn path_yields_two_independent_reads_over_identical_bytes() {
        use std::io::Write;
        // A unique temp path under the OS temp dir, cleaned up at test end. No
        // external temp-file crate needed for a single fixed-content file.
        let path = std::env::temp_dir().join(format!(
            "clinker-reopenable-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"path bytes")
            .unwrap();
        let src = ReopenableSource::path(&path);
        let mut a = Vec::new();
        let mut b = Vec::new();
        src.open().unwrap().read_to_end(&mut a).unwrap();
        src.open().unwrap().read_to_end(&mut b).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(a, b"path bytes");
        assert_eq!(a, b, "two opens of the same path read identical bytes");
    }

    #[test]
    fn one_shot_opens_lazily_exactly_once() {
        let src = ReopenableSource::one_shot(Box::new(Cursor::new(b"once".to_vec())));
        let mut a = String::new();
        src.open().unwrap().read_to_string(&mut a).unwrap();
        assert_eq!(a, "once");
        // The reader was taken; a second open without converting is misuse and
        // must surface as a typed error, not a panic.
        let err = match src.open() {
            Ok(_) => panic!("a second one-shot open must return an error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn into_reopenable_after_open_returns_invalid_input() {
        let src = ReopenableSource::one_shot(Box::new(Cursor::new(b"gone".to_vec())));
        let mut a = String::new();
        src.open().unwrap().read_to_string(&mut a).unwrap();
        assert_eq!(a, "gone");
        // The reader is gone; converting now cannot buffer anything and must
        // surface as a typed error, not a panic.
        let err = match src.into_reopenable() {
            Ok(_) => panic!("converting an already-opened one-shot must return an error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn into_reopenable_makes_a_one_shot_re_openable() {
        let src = ReopenableSource::one_shot(Box::new(Cursor::new(b"twice".to_vec())))
            .into_reopenable()
            .unwrap();
        let mut a = String::new();
        let mut b = String::new();
        src.open().unwrap().read_to_string(&mut a).unwrap();
        src.open().unwrap().read_to_string(&mut b).unwrap();
        assert_eq!(a, "twice");
        assert_eq!(a, b, "after conversion two opens read identical bytes");
    }

    #[test]
    fn path_identity_changes_when_the_file_is_rewritten() {
        use std::io::Write;
        let path = std::env::temp_dir().join(format!(
            "clinker-identity-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"original content")
            .unwrap();
        let src = ReopenableSource::path(&path);
        let (_r1, id1) = src.open_with_identity().unwrap();

        // Rewrite the file to a different length, as an external producer would.
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"a wholly different and longer content")
            .unwrap();
        let (_r2, id2) = src.open_with_identity().unwrap();
        let _ = std::fs::remove_file(&path);

        assert_ne!(id1, id2, "a rewritten file yields a different identity");
        assert!(
            id2.ensure_matches(&id1).is_err(),
            "ensure_matches must fail loud on a changed file"
        );
    }

    #[test]
    fn buffered_identity_is_stable_across_opens() {
        let src = ReopenableSource::buffer(Cursor::new(b"stable".to_vec())).unwrap();
        let (_a, id_a) = src.open_with_identity().unwrap();
        let (_b, id_b) = src.open_with_identity().unwrap();
        assert_eq!(id_a, id_b);
        assert!(id_b.ensure_matches(&id_a).is_ok());
    }
}
