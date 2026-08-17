//! Re-openable byte source backing a format reader.
//!
//! A [`ReopenableSource`] decouples a reader from a single `Box<dyn Read>`
//! handle so a reader that needs multiple passes (JSON's envelope pre-scan plus
//! its body stream) can open the bytes more than once without buffering the
//! whole input — while a one-pass reader (CSV, XML, fixed-width, EDI) consumes
//! the bytes lazily, one streamed `Read` and no buffer.
//!
//! Four shapes, each honest about its memory. They are a private detail of
//! the type — a source is built through [`ReopenableSource::path`],
//! [`ReopenableSource::open_file`], [`ReopenableSource::one_shot`] or
//! [`ReopenableSource::buffer`], so a caller cannot assemble one that bypasses
//! the byte counting every open performs:
//! - A **path** source re-opens a stable on-disk path. With staging
//!   enabled the input is a content-addressed copy held under an advisory read
//!   lock for the run, so two opens read byte-identical content; with staging
//!   off the original path is re-opened directly. Either way the batch model
//!   requires inputs to stay stable for the run's duration, and a multi-pass
//!   reader guards that with [`SourceIdentity`] — a `(len, mtime)` snapshot
//!   taken at each open that fails loud if the file changed between passes,
//!   rather than splicing a stale envelope onto a freshly-read body. Memory is
//!   O(1) per open — no whole-file buffer is ever held.
//! - An **open-file** source retains the file handle established by capability
//!   activation. Each pass uses a cloned handle plus a private logical cursor,
//!   so path removal or atomic replacement after activation cannot change the
//!   admitted bytes. A second guard handle remains with the provider session.
//!   Memory is O(1) per pass and the file is never buffered whole.
//! - A **one-shot** source wraps a single pathless `Box<dyn Read>`
//!   (a test/bench cursor, the `<empty>` slot, a network body). It is consumed
//!   *lazily*: the first [`open`](ReopenableSource::open) hands out the reader as-is, so a
//!   one-pass format streams it without buffering and a paced/slow reader keeps
//!   its streaming timing. A second pass is only possible after
//!   [`into_reopenable`](ReopenableSource::into_reopenable) buffers it.
//! - A **buffered** source holds bytes in a shared `Arc<[u8]>`, handing
//!   out cursors. A one-shot becomes buffered on demand when a multi-pass
//!   reader (JSON) needs a second open; bounded because pathless inputs are
//!   small by construction.

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// Shared count of the bytes a source has handed to its readers.
///
/// Attached with [`ReopenableSource::with_tally`] and incremented inside
/// [`ReopenableSource::open_with_identity`], which every reader's bytes pass
/// through — so a reader neither implements nor delegates anything to be
/// counted, and a format added later is counted without knowing this exists.
///
/// Clones share one counter. The reading thread increments; an observer on
/// another thread may read a value already superseded, which is the contract
/// for a progress display and unfit for a decision.
///
/// A source read more than once (a multi-pass format's envelope pre-scan plus
/// its body) crosses this counter twice, so the count then measures IO
/// performed rather than input consumed. Whether that is so is decided from the
/// declared format before any reader exists, by the caller — not discovered
/// here — so a published denominator cannot turn back into an absence part-way
/// through a run.
#[derive(Clone, Debug, Default)]
pub struct ByteTally {
    read: Arc<AtomicU64>,
}

impl ByteTally {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes handed to readers so far. Monotonic.
    pub fn read(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    fn add(&self, n: u64) {
        if n > 0 {
            self.read.fetch_add(n, Ordering::Relaxed);
        }
    }
}

/// Counts bytes on their way out of a source to a reader.
///
/// Wraps whatever [`ReopenableSource::open_with_identity`] produced, so the
/// count follows the bytes rather than the reader that asked for them. One
/// relaxed add per `read` call — per buffer fill for a buffered reader, not
/// per record.
struct CountingReader<R> {
    inner: R,
    tally: ByteTally,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.tally.add(n as u64);
        Ok(n)
    }
}

/// Delivers exactly the source length admitted by a bounded caller.
///
/// The open-time metadata check catches a file that changed before the reader
/// was built. This wrapper closes the remaining race while the reader is live:
/// it rejects an early EOF and probes one byte past the admitted boundary so a
/// concurrent append cannot silently expand the input. At most the admitted
/// bytes are handed to the format reader; the one-byte probe is never exposed
/// or credited to a [`ByteTally`].
struct ExactLengthReader<R> {
    inner: R,
    remaining: u64,
    finished: bool,
}

impl<R: Read> ExactLengthReader<R> {
    fn new(inner: R, expected_len: u64) -> Self {
        Self {
            inner,
            remaining: expected_len,
            finished: false,
        }
    }
}

impl<R: Read> Read for ExactLengthReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.finished {
            return Ok(0);
        }
        if self.remaining > 0 {
            let admitted = usize::try_from(self.remaining)
                .unwrap_or(usize::MAX)
                .min(buffer.len());
            let read = self.inner.read(&mut buffer[..admitted])?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "source ended before its admitted length",
                ));
            }
            self.remaining -= read as u64;
            return Ok(read);
        }

        let mut overflow_probe = [0_u8; 1];
        if self.inner.read(&mut overflow_probe)? != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source grew beyond its admitted length",
            ));
        }
        self.finished = true;
        Ok(0)
    }
}

/// A cheap `(len, mtime)` snapshot of a re-opened source's content, taken off
/// the *open* handle (not the path) so it reflects the bytes that open will
/// read.
///
/// This is a courtesy guard against an accidental mid-run input rewrite under
/// Clinker's finite-batch input-stability contract — not a security boundary
/// and not a content fingerprint.
///
/// What it catches: for a `Path` source, a file replaced between opens; for
/// both `Path` and `OpenFile`, the admitted file object truncated to a
/// different length or touched to a newer mtime between a reader's two opens
/// (the body open at construction and the envelope pre-scan open in
/// `prepare_document`). An `OpenFile` source deliberately ignores later path
/// replacement because every pass reads the already-admitted file object.
/// Cheap — no bytes are read.
///
/// What it does NOT catch (accepted residuals; inputs must stay stable for the
/// duration of a finite batch run):
/// - A same-length, same-mtime-*tick* in-place rewrite. On coarse-granularity
///   filesystems (FAT/exFAT at 2 s) or for a truncate-and-rewrite completing
///   inside one mtime tick, the snapshot is unchanged. Closing this would
///   require a content-identity comparison and stability protocol with another
///   full I/O pass, so the bounded `(len, mtime)` residual is deliberately
///   accepted here.
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

/// A byte source a format reader can open, re-openably for path, retained-file,
/// and buffered shapes, and once-lazily for a pathless one-shot reader.
///
/// A reader needing two passes (JSON's pre-scan plus body) first calls
/// [`into_reopenable`](Self::into_reopenable) so every [`open`](Self::open)
/// thereafter yields an independent `Read`; a one-pass reader just calls
/// [`open`](Self::open) once and streams it.
pub struct ReopenableSource {
    kind: SourceKind,
    /// Byte counter to credit on every open, when a caller asked for one.
    /// Carried by the source rather than passed at each open so no call site
    /// can obtain bytes without counting them.
    tally: Option<ByteTally>,
    /// Exact byte length each open may deliver, when a bounded caller froze
    /// the source size before constructing its reader.
    exact_len: Option<u64>,
}

/// The four byte-source shapes. Private: callers construct through the typed
/// constructors, so the tally and optional exact-length guard cannot be
/// dropped by building a source directly.
enum SourceKind {
    /// Re-open a stable filesystem path. Each open is a fresh `std::fs::File`;
    /// for a staged, advisory-locked source two opens read identical bytes.
    Path(PathBuf),
    /// Read only from the file handle opened during capability activation.
    /// Cloned handles share an OS cursor, so every reader keeps a logical
    /// offset and serializes seek+read through the shared gate.
    OpenFile {
        file: std::fs::File,
        gate: Arc<Mutex<()>>,
    },
    /// Stream from shared in-memory bytes. Each open is a fresh cursor over the
    /// same `Arc<[u8]>`. Re-openable; bounded because such inputs are small.
    Buffered(Arc<[u8]>),
    /// A single pathless `Box<dyn Read>`, consumed lazily on the first open.
    /// The `Mutex<Option<..>>` lets `open(&self)` take the reader without
    /// buffering, preserving a slow/paced reader's streaming timing for a
    /// one-pass format. `None` after the reader has been taken or buffered.
    OneShot(Mutex<Option<Box<dyn Read + Send>>>),
}

/// Provider/session-side guard for one already-open file resource.
///
/// The opaque guard exposes no file operations. Keeping it in the active
/// capability session pins the admitted file object until the whole activation
/// group is released, independently of the reader handle transferred to a
/// worker.
pub struct RetainedFileGuard {
    _file: std::fs::File,
}

/// One logical cursor over clones of an already-open file handle.
///
/// `File::try_clone` shares the underlying cursor on supported platforms.
/// Seeking and reading under the shared gate on every call makes each pass's
/// logical offset independent even if body and pre-scan readers interleave.
struct OpenFileReader {
    file: std::fs::File,
    gate: Arc<Mutex<()>>,
    offset: u64,
}

impl Read for OpenFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| std::io::Error::other("open-file reader gate poisoned"))?;
        self.file.seek(SeekFrom::Start(self.offset))?;
        let read = self.file.read(buffer)?;
        self.offset = self
            .offset
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("open-file reader offset overflow"))?;
        Ok(read)
    }
}

impl ReopenableSource {
    /// Wrap a one-shot pathless reader, consumed lazily on first open.
    ///
    /// No bytes are read here — a one-pass format streams the reader directly,
    /// so a slow/paced reader keeps its per-row timing. JSON, which needs two
    /// passes, calls [`into_reopenable`](Self::into_reopenable) to buffer it.
    pub fn one_shot(reader: Box<dyn Read + Send>) -> Self {
        Self::of(SourceKind::OneShot(Mutex::new(Some(reader))))
    }

    fn of(kind: SourceKind) -> Self {
        Self {
            kind,
            tally: None,
            exact_len: None,
        }
    }

    /// The number of bytes an open would deliver, when that is knowable.
    ///
    /// Answered by the source itself so the size describes the bytes a reader
    /// will actually receive — for a staged input, the staged copy rather than
    /// the original path it was matched from, which staging exists precisely
    /// because the run cannot rely on.
    ///
    /// `None` for a one-shot reader, whose length is unknowable without
    /// consuming it. `Buffered`, `Path`, and `OpenFile` answer exactly.
    pub fn known_len(&self) -> Option<u64> {
        match &self.kind {
            SourceKind::Path(path) => std::fs::metadata(path).ok().map(|meta| meta.len()),
            SourceKind::OpenFile { file, .. } => file.metadata().ok().map(|meta| meta.len()),
            SourceKind::Buffered(bytes) => Some(bytes.len() as u64),
            SourceKind::OneShot(_) => None,
        }
    }

    /// Credit every byte this source hands out to `tally`.
    ///
    /// Attaching here rather than passing a counter to each open is what makes
    /// the count unforgettable: every reader obtains bytes through
    /// [`open_with_identity`](Self::open_with_identity), so a source carrying a
    /// tally is counted whatever reads it, including a format written later.
    #[must_use]
    pub fn with_tally(mut self, tally: ByteTally) -> Self {
        self.tally = Some(tally);
        self
    }

    /// Require every open to deliver exactly `expected_len` bytes.
    ///
    /// Known-length sources are rejected at open time if their handle metadata
    /// already differs. Every returned reader also rejects truncation or growth
    /// while it is being consumed, and never hands more than the admitted
    /// length to its format reader. The guard is preserved across
    /// [`into_reopenable`](Self::into_reopenable).
    #[must_use]
    pub fn with_exact_len(mut self, expected_len: u64) -> Self {
        self.exact_len = Some(expected_len);
        self
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
        Ok(Self::of(SourceKind::Buffered(Arc::from(bytes))))
    }

    /// Build a path-backed source that re-opens `path` fresh on every open.
    pub fn path(path: impl Into<PathBuf>) -> Self {
        Self::of(SourceKind::Path(path.into()))
    }

    /// Open and retain one file object for every later reader pass.
    ///
    /// The source and returned opaque guard each retain a handle to the same
    /// admitted file object. Later path removal or replacement therefore does
    /// not change which bytes the run reads. Every pass clones the retained
    /// handle, rewinds it, and streams with O(1) memory.
    ///
    /// Windows opens include delete sharing so atomic rename/removal has the
    /// same admitted-handle behavior as POSIX unlink/rename.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O failure if the path cannot be opened, the
    /// guard handle cannot be cloned, or metadata cannot be read.
    pub fn open_file(path: impl AsRef<Path>) -> std::io::Result<(Self, RetainedFileGuard)> {
        let file = open_shared(path.as_ref())?;
        let guard = file.try_clone()?;
        file.metadata()?;
        Ok((
            Self::of(SourceKind::OpenFile {
                file,
                gate: Arc::new(Mutex::new(())),
            }),
            RetainedFileGuard { _file: guard },
        ))
    }

    /// Convert into a shape that supports repeated [`open`](Self::open) calls.
    ///
    /// `Path`, `OpenFile`, and `Buffered` are already re-openable and pass
    /// through untouched (no read). A `OneShot` is drained once into a
    /// `Buffered` so a multi-pass reader (JSON's pre-scan + body) can open it
    /// twice. The drain runs at the caller's site (the executor's per-file
    /// factory for JSON), not at slot construction, so a one-pass format never
    /// triggers it.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if draining a `OneShot` fails.
    /// Returns [`std::io::ErrorKind::InvalidInput`] if a `OneShot`'s reader was
    /// already taken by a prior [`open`](Self::open) — a multi-pass reader must
    /// convert before opening. A poisoned reader slot (a panic mid-open on
    /// another thread) surfaces as an opaque [`std::io::Error`].
    pub fn into_reopenable(self) -> std::io::Result<Self> {
        let tally = self.tally;
        let exact_len = self.exact_len;
        let converted = match self.kind {
            SourceKind::OneShot(slot) => {
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
                match exact_len {
                    Some(expected_len) => {
                        Self::buffer(ExactLengthReader::new(reader, expected_len))?
                    }
                    None => Self::buffer(reader)?,
                }
            }
            already => Self::of(already),
        };
        Ok(Self {
            kind: converted.kind,
            tally,
            exact_len,
        })
    }

    /// Open a `Read` over the source's bytes, positioned at the start.
    ///
    /// `Path` opens a fresh file; `OpenFile` clones the retained file object;
    /// `Buffered` cursors the shared `Arc<[u8]>`; `OneShot` hands out its lazy
    /// reader on the first call. All file shapes use O(1) memory. Multi-pass
    /// callers convert via [`into_reopenable`](Self::into_reopenable) first so
    /// every open yields an independent `Read`.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if a `Path` source fails to
    /// open or an `OpenFile` handle cannot be cloned/rewound, or
    /// [`std::io::ErrorKind::InvalidInput`] if a `OneShot` is opened more than
    /// once without first converting via
    /// [`into_reopenable`](Self::into_reopenable). `Buffered` never fails.
    pub fn open(&self) -> std::io::Result<Box<dyn Read + Send>> {
        Ok(self.open_with_identity()?.0)
    }

    /// Open a `Read` and snapshot the content identity of the bytes it will
    /// read, for a multi-pass reader that must detect the admitted file object
    /// changing between passes.
    ///
    /// The [`SourceIdentity`] is stat-ed off the per-pass open handle for a
    /// `Path` or retained `OpenFile` source, the byte length for a `Buffered`
    /// source, and an empty (always self-consistent) snapshot for a `OneShot`.
    /// A reader captures it on its first pass and compares it on later passes via
    /// [`SourceIdentity::ensure_matches`].
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if a `Path` source fails to
    /// open, an `OpenFile` handle cannot be cloned/rewound, or file metadata
    /// cannot be read, or
    /// [`std::io::ErrorKind::InvalidInput`] if a `OneShot` is opened more than
    /// once without first converting via
    /// [`into_reopenable`](Self::into_reopenable).
    pub fn open_with_identity(&self) -> std::io::Result<(Box<dyn Read + Send>, SourceIdentity)> {
        let (mut reader, identity) = self.open_uncounted()?;
        if let Some(expected_len) = self.exact_len {
            if !matches!(&self.kind, SourceKind::OneShot(_)) && identity.len != expected_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "source length changed after admission (expected {expected_len} bytes, opened {} bytes)",
                        identity.len
                    ),
                ));
            }
            reader = Box::new(ExactLengthReader::new(reader, expected_len));
        }
        let Some(tally) = self.tally.clone() else {
            return Ok((reader, identity));
        };
        Ok((
            Box::new(CountingReader {
                inner: reader,
                tally,
            }),
            identity,
        ))
    }

    /// Open the raw bytes, before any counting wrapper. Private so the counted
    /// path is the only way out of this type.
    fn open_uncounted(&self) -> std::io::Result<(Box<dyn Read + Send>, SourceIdentity)> {
        match &self.kind {
            SourceKind::Path(path) => {
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
            SourceKind::OpenFile { file, gate } => {
                let _guard = gate
                    .lock()
                    .map_err(|_| std::io::Error::other("open-file reader gate poisoned"))?;
                let mut cloned = file.try_clone()?;
                cloned.rewind()?;
                let metadata = cloned.metadata()?;
                let identity = SourceIdentity {
                    len: metadata.len(),
                    mtime: metadata.modified().ok(),
                };
                Ok((
                    Box::new(OpenFileReader {
                        file: cloned,
                        gate: Arc::clone(gate),
                        offset: 0,
                    }),
                    identity,
                ))
            }
            SourceKind::Buffered(bytes) => {
                let identity = SourceIdentity {
                    len: bytes.len() as u64,
                    mtime: None,
                };
                Ok((Box::new(Cursor::new(Arc::clone(bytes))), identity))
            }
            SourceKind::OneShot(slot) => {
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
    /// first pass. A mismatch means a file-backed input was rewritten between
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
    fn a_tallied_source_counts_every_byte_it_hands_out() {
        let tally = ByteTally::new();
        let src = ReopenableSource::buffer(Cursor::new(b"twelve bytes".to_vec()))
            .unwrap()
            .with_tally(tally.clone());
        assert_eq!(tally.read(), 0, "nothing is counted before a read");

        let mut sink = Vec::new();
        src.open().unwrap().read_to_end(&mut sink).unwrap();
        assert_eq!(tally.read(), 12);
    }

    #[test]
    fn exact_length_rejects_a_file_changed_before_open() {
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "clinker-exact-open-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, b"four").unwrap();
        let src = ReopenableSource::path(&path).with_exact_len(4);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"!")
            .unwrap();

        let err = match src.open() {
            Ok(_) => panic!("a changed source length must fail before reading"),
            Err(err) => err,
        };
        let _ = std::fs::remove_file(&path);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn exact_length_rejects_growth_without_handing_over_the_extra_byte() {
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "clinker-exact-grow-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, b"four").unwrap();
        let tally = ByteTally::new();
        let src = ReopenableSource::path(&path)
            .with_exact_len(4)
            .with_tally(tally.clone());
        let mut reader = src.open().unwrap();
        let mut admitted = [0_u8; 4];
        reader.read_exact(&mut admitted).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"!")
            .unwrap();

        let err = reader.read_to_end(&mut Vec::new()).unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(admitted, *b"four");
        assert_eq!(tally.read(), 4, "the overflow probe is never handed over");
    }

    #[test]
    fn exact_length_rejects_truncation_during_a_read() {
        let path = std::env::temp_dir().join(format!(
            "clinker-exact-shrink-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, b"four").unwrap();
        let src = ReopenableSource::path(&path).with_exact_len(4);
        let mut reader = src.open().unwrap();
        let mut head = [0_u8; 2];
        reader.read_exact(&mut head).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(2)
            .unwrap();

        let err = reader.read_to_end(&mut Vec::new()).unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn exact_length_is_enforced_while_buffering_a_one_shot() {
        let err = match ReopenableSource::one_shot(Box::new(Cursor::new(b"five!".to_vec())))
            .with_exact_len(4)
            .into_reopenable()
        {
            Ok(_) => panic!("buffering must not bypass the exact-length guard"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_source_reports_the_length_an_open_would_deliver() {
        use std::io::Write;
        let path = std::env::temp_dir().join(format!(
            "clinker-len-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"nine byte")
            .unwrap();
        assert_eq!(ReopenableSource::path(&path).known_len(), Some(9));
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            ReopenableSource::buffer(Cursor::new(b"four".to_vec()))
                .unwrap()
                .known_len(),
            Some(4)
        );
        assert_eq!(
            ReopenableSource::buffer(std::io::empty())
                .unwrap()
                .known_len(),
            Some(0),
            "a source with no bytes knows it, rather than withdrawing a total"
        );
        assert_eq!(
            ReopenableSource::one_shot(Box::new(Cursor::new(b"x".to_vec()))).known_len(),
            None,
            "a one-shot length is unknowable without consuming it"
        );
    }

    /// Counting happens as bytes are handed over, not when a source is
    /// exhausted. This is the property that makes the byte axis useful on a
    /// single large input, where every other count sits still until the end —
    /// and it is checked here, deterministically, rather than by racing a
    /// once-per-second progress record against a read still in flight.
    #[test]
    fn a_partial_read_counts_only_what_it_took() {
        let tally = ByteTally::new();
        let src = ReopenableSource::buffer(Cursor::new(vec![b'x'; 4096]))
            .unwrap()
            .with_tally(tally.clone());

        let mut reader = src.open().unwrap();
        let mut head = [0_u8; 100];
        reader.read_exact(&mut head).unwrap();
        let after_first = tally.read();
        assert!(
            (100..4096).contains(&after_first),
            "a partial read reports partial bytes, got {after_first}"
        );

        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(tally.read(), 4096, "the whole source is accounted for");
        assert!(
            tally.read() > after_first,
            "the count advanced within the one open"
        );
    }

    #[test]
    fn an_untallied_source_is_unchanged() {
        let src = ReopenableSource::buffer(Cursor::new(b"plain".to_vec())).unwrap();
        let mut sink = String::new();
        src.open().unwrap().read_to_string(&mut sink).unwrap();
        assert_eq!(sink, "plain");
    }

    /// The count follows the bytes, not the reader, so a second pass over the
    /// same source adds to it. This is why a run whose format re-reads its
    /// input publishes no byte denominator: the count would overrun it.
    #[test]
    fn a_second_pass_doubles_the_count() {
        let tally = ByteTally::new();
        let src = ReopenableSource::buffer(Cursor::new(b"sixteen bytes!!!".to_vec()))
            .unwrap()
            .with_tally(tally.clone())
            .into_reopenable()
            .unwrap();

        let mut a = Vec::new();
        let mut b = Vec::new();
        src.open().unwrap().read_to_end(&mut a).unwrap();
        src.open().unwrap().read_to_end(&mut b).unwrap();
        assert_eq!(a.len(), 16);
        assert_eq!(tally.read(), 32, "both passes cross the counter");
    }

    #[test]
    fn a_tally_survives_conversion_of_a_one_shot() {
        let tally = ByteTally::new();
        let src = ReopenableSource::one_shot(Box::new(Cursor::new(b"kept".to_vec())))
            .with_tally(tally.clone())
            .into_reopenable()
            .unwrap();
        let mut sink = Vec::new();
        src.open().unwrap().read_to_end(&mut sink).unwrap();
        assert_eq!(tally.read(), 4, "the tally is not dropped by buffering");
    }

    #[test]
    fn a_path_source_counts_its_file_bytes() {
        use std::io::Write;
        let path = std::env::temp_dir().join(format!(
            "clinker-tally-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"on disk")
            .unwrap();
        let tally = ByteTally::new();
        let src = ReopenableSource::path(&path).with_tally(tally.clone());
        let mut sink = Vec::new();
        src.open().unwrap().read_to_end(&mut sink).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(tally.read(), 7);
    }

    #[test]
    fn buffered_identity_is_stable_across_opens() {
        let src = ReopenableSource::buffer(Cursor::new(b"stable".to_vec())).unwrap();
        let (_a, id_a) = src.open_with_identity().unwrap();
        let (_b, id_b) = src.open_with_identity().unwrap();
        assert_eq!(id_a, id_b);
        assert!(id_b.ensure_matches(&id_a).is_ok());
    }

    #[test]
    fn an_open_file_handle_survives_path_replacement_across_interleaved_passes() {
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "clinker-open-handle-{}-{:?}.json",
            std::process::id(),
            std::thread::current().id()
        ));
        let displaced = path.with_extension("original");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"original bytes")
            .unwrap();

        let (source, _session_guard) =
            ReopenableSource::open_file(&path).expect("open retained file handle");
        let mut body = source.open().expect("open body pass");
        let mut body_prefix = [0_u8; 8];
        body.read_exact(&mut body_prefix).expect("read body prefix");

        std::fs::rename(&path, &displaced).expect("replace the admitted path");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"replacement")
            .unwrap();

        let mut prescan_bytes = Vec::new();
        source
            .open()
            .expect("open pre-scan pass")
            .read_to_end(&mut prescan_bytes)
            .expect("read pre-scan pass");
        let mut body_suffix = Vec::new();
        body.read_to_end(&mut body_suffix)
            .expect("finish body pass");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&displaced);
        assert_eq!(prescan_bytes, b"original bytes");
        assert_eq!(
            [body_prefix.as_slice(), body_suffix.as_slice()].concat(),
            b"original bytes"
        );
    }

    #[test]
    fn an_open_file_handle_detects_same_inode_mutation_between_passes() {
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "clinker-open-handle-mutation-{}-{:?}.json",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"original")
            .unwrap();

        let (source, _session_guard) =
            ReopenableSource::open_file(&path).expect("open retained file handle");
        let (_first, before) = source.open_with_identity().expect("first pass identity");
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("open admitted inode for mutation")
            .write_all(b"mutated to a different length")
            .expect("mutate admitted inode");
        let (_second, after) = source.open_with_identity().expect("later pass identity");

        let _ = std::fs::remove_file(&path);
        assert_ne!(before, after);
        assert!(
            after.ensure_matches(&before).is_err(),
            "same-inode mutation must not splice bytes across format passes"
        );
    }
}
