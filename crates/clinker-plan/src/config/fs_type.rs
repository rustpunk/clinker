//! Pure filesystem-type and same-device probing behind a portable facade.
//!
//! This module answers two physical questions about a path, with **no policy
//! attached**: what *kind* of filesystem backs it ([`FsKind`]), and whether
//! two paths sit on the *same device* ([`same_device`]). Whether a given
//! answer is acceptable — spilling onto tmpfs is pointless, staging onto a
//! network share is dangerous, staging onto the source's own volume moves no
//! I/O — is the validation layer's call, not this module's. Keeping detection
//! and policy separate lets both the config-time staging check and the
//! executor-startup spill check share one probing implementation rather than
//! each carrying its own.
//!
//! First-class on Linux, macOS, and Windows, each behind a `#[cfg]` arm:
//!
//! - **Linux** — `statfs(2)` `f_type` magic numbers. tmpfs and ramfs are
//!   in-memory; NFS / SMB(2) / CIFS / FUSE are network/userspace transports.
//!   nix exports some magics ([`TMPFS_MAGIC`](nix::sys::statfs::TMPFS_MAGIC)
//!   etc.) but not ramfs or the modern SMB2/CIFS magics, so those are defined
//!   locally from the kernel `magic.h` values.
//! - **macOS** — `statfs(2)` `f_fstypename`, matched as a **string**. The
//!   numeric `f_type` on Darwin is undocumented and unstable across releases,
//!   so the string name is the only reliable signal. macOS has no native
//!   tmpfs, so the in-memory check is a documented no-op there — a RAM disk on
//!   macOS surfaces as `hfs`/`apfs` over a synthetic device and is classified
//!   [`FsKind::Local`].
//! - **Windows** — `GetVolumePathNameW` to find the volume mount root, then
//!   `GetDriveTypeW` (`DRIVE_REMOTE` ⇒ network, `DRIVE_RAMDISK` ⇒ in-memory).
//!   Same-device identity uses the NTFS volume serial number from
//!   `GetVolumeInformationByHandleW` (libstd's `volume_serial_number()` is
//!   nightly-only, so it is read directly here).

use std::io;
use std::path::Path;

/// The class of filesystem backing a path, as far as spill/staging policy
/// cares. Detection only — the reject/allow decision lives in the validation
/// layer, which maps these variants to its own rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsKind {
    /// A local, durable, block-backed filesystem (ext4 / xfs / btrfs / apfs /
    /// NTFS / …). The fast, safe default for spill and staging targets.
    Local,
    /// An in-memory filesystem (Linux tmpfs / ramfs). Spilling here defeats
    /// the purpose of spilling — it trades RSS for page-cache pressure without
    /// moving bytes off RAM. macOS reports no path as in-memory (see module
    /// docs), so this variant only ever arises on Linux and Windows.
    InMemory,
    /// A network or userspace-bridged filesystem (NFS / SMB / CIFS / FUSE).
    /// Prone to the soft-mount silent-truncation and mmap-data-loss failure
    /// modes that motivate staging *away from* such mounts; a spill or staging
    /// *target* on one reintroduces exactly the fragility staging exists to
    /// escape.
    Network,
}

/// Filesystem family used to verify a configured publication destination.
///
/// Publication profiles distinguish NFS from SMB because each profile is
/// qualified independently. Other network and userspace filesystems remain
/// explicit so they cannot be mistaken for either qualified family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilesystemFamily {
    /// A local, durable filesystem.
    Local,
    /// An in-memory filesystem.
    InMemory,
    /// An NFS mount. The configured profile carries the qualified protocol
    /// version; the portable filesystem probe identifies the transport family.
    Nfs,
    /// An SMB or CIFS mount. The configured profile carries the qualified
    /// protocol version; the portable filesystem probe identifies the family.
    Smb,
    /// A network or userspace mount that is neither identifiable NFS nor SMB.
    OtherNetwork,
}

impl FilesystemFamily {
    /// Stable diagnostic spelling for the detected family.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::InMemory => "in_memory",
            Self::Nfs => "nfs",
            Self::Smb => "smb",
            Self::OtherNetwork => "other_network",
        }
    }

    pub(crate) fn publication_correction(self) -> &'static str {
        match self {
            Self::Local => "use `[storage.publication]\ndestination_profile = \"local\"`",
            Self::Nfs => {
                "use `[storage.publication]\ndestination_profile = \"nfs_v4_1\"` only after qualifying this NFS destination"
            }
            Self::Smb => {
                "use `[storage.publication]\ndestination_profile = \"smb_3_1_1\"` only after qualifying this SMB destination"
            }
            Self::InMemory => "choose a durable local or qualified network destination",
            Self::OtherNetwork => {
                "choose a local, qualified NFSv4.1, or qualified SMB3.1.1 destination"
            }
        }
    }

    fn kind(self) -> FsKind {
        match self {
            Self::Local => FsKind::Local,
            Self::InMemory => FsKind::InMemory,
            Self::Nfs | Self::Smb | Self::OtherNetwork => FsKind::Network,
        }
    }
}

/// Classify the filesystem backing `path`.
///
/// `path` must exist (it is the thing being probed); a non-existent path or a
/// failed syscall surfaces as an [`io::Error`] rather than a guessed
/// classification, so the caller can fail fast rather than silently treat an
/// unprobeable path as [`FsKind::Local`].
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the platform probe fails — the
/// path does not exist, cannot be stat'd / queried, or the OS call returns an
/// error.
pub fn classify(path: &Path) -> io::Result<FsKind> {
    classify_family(path).map(FilesystemFamily::kind)
}

/// Classify the filesystem family backing a publication destination.
///
/// Unlike [`classify`], this preserves the distinction between NFS, SMB, and
/// other network transports so a configured qualified profile cannot accept a
/// different network filesystem merely because both are remote.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when the path is missing or the
/// platform probe fails.
pub fn classify_family(path: &Path) -> io::Result<FilesystemFamily> {
    // Enforce the documented "path must exist" contract uniformly. Unix
    // statfs(2) already errors on a missing path, but the Windows volume query
    // resolves the path's drive root and would succeed for a non-existent
    // file — so check existence here rather than leave the behavior per-OS.
    if !path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "cannot classify filesystem: path does not exist: {}",
                path.display()
            ),
        ));
    }
    classify_family_impl(path)
}

/// Whether two existing paths reside on the same physical device / volume.
///
/// On Unix this compares `st_dev`; on Windows it compares the NTFS volume
/// serial number. Both paths must exist and be probeable.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when either path cannot be probed.
pub fn same_device(a: &Path, b: &Path) -> io::Result<bool> {
    same_device_impl(a, b)
}

/// Whether the filesystem that *would back a file at `path`* preserves case in
/// filenames (`true`) or folds it (`false`).
///
/// This drives output-path collision detection: two paths differing only in
/// case (`errors.csv` vs `Errors.csv`) are two distinct files on a
/// case-sensitive filesystem but **one** physical file on a case-insensitive
/// one (the common macOS APFS and Windows NTFS default). Case must therefore
/// be folded *conditionally* — only when the target filesystem actually folds
/// it — otherwise a correct case-sensitive Linux pipeline would be wrongly
/// flagged for a collision that does not exist on its disk.
///
/// The answer is obtained by **active probe**, not by mapping a filesystem-type
/// table to a case-sensitivity guess: a real file is created and the same path
/// re-cased is re-statted. This is the mechanism Git uses to auto-detect
/// `core.ignorecase`, and it is correct regardless of OS, mount options, or
/// per-directory case-sensitivity attributes (Windows per-directory case
/// sensitivity, APFS case-sensitive volumes) that a static table cannot see.
///
/// `path` itself need not exist. The probe runs in the **nearest existing
/// ancestor directory** of `path`, because that is the filesystem the file will
/// be created on once its parent directories are materialized (the writer
/// `create_dir_all`s the missing parents onto that same filesystem). The
/// probe file is created and removed inside that directory; it never collides
/// with the real output path.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when no existing ancestor directory can
/// be found or the probe file cannot be created in it. Callers treat a probe
/// failure as "assume case-sensitive" (do not flag a collision) so a transient
/// or permission failure never converts a correct pipeline into a hard
/// validation error; the silent-data-loss risk a probe failure leaves unguarded
/// is confined to filesystems that are simultaneously case-insensitive *and*
/// unprobeable, which a writable output directory never is in practice.
pub fn case_sensitive_dir(path: &Path) -> io::Result<bool> {
    let dir = nearest_existing_dir(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no existing ancestor directory to probe for case-sensitivity",
        )
    })?;
    CASE_ANSWERS.answer(&dir, probe_case_sensitive)
}

/// Whether the filesystem that *would back a file at `path`* looks a name up
/// under any of its canonically-equivalent spellings (`true`) or only under
/// the exact scalars given (`false`).
///
/// This is the **second, independent** axis of output-path collision
/// detection, and it is orthogonal to [`case_sensitive_dir`] — neither implies
/// the other, and each closes a silent double-write the other leaves open:
///
/// - **APFS** is normalization-*insensitive* in **both** its case-sensitive
///   and case-insensitive variants: lookup hashes the NFD-normalized name (and
///   case-folds it too, on the ci variant). So a *case-sensitive* APFS volume
///   — the default on iOS, and an option on macOS — still resolves `caf\u{e9}`
///   and `cafe\u{301}` to one file while keeping `Caf\u{e9}` distinct. Folding
///   alone would miss that entirely.
/// - **HFS+** stores a variant of NFD outright, so the same holds there.
/// - **NTFS** does the opposite: it upper-cases on lookup but never
///   normalizes, so the two NFC/NFD spellings are two genuinely different
///   files. Normalizing alone would falsely merge them.
///
/// As with case-sensitivity, no platform exposes a capability bit for this, so
/// the answer is obtained by **active probe** in the nearest existing ancestor
/// directory: a file is created under its composed (NFC) name and the
/// decomposed (NFD) spelling of that same name is re-statted. The probe file
/// shares the `clinker-case-probe-` prefix with the case probe and is removed
/// through the same [`ProbeFile`] guard, so it leaves no residue on any exit
/// path.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] when no existing ancestor directory
/// can be found or the probe file cannot be created in it. Callers treat a
/// probe failure as "assume the volume normalizes nothing" (do not merge), the
/// same safe default [`case_sensitive_dir`] failures take.
pub fn normalization_insensitive_dir(path: &Path) -> io::Result<bool> {
    let dir = nearest_existing_dir(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no existing ancestor directory to probe for normalization-insensitivity",
        )
    })?;
    NORMALIZATION_ANSWERS.answer(&dir, probe_normalization_insensitive)
}

/// Remembered case-sensitivity verdicts, one per directory.
static CASE_ANSWERS: VolumeAnswers = VolumeAnswers::new();

/// Remembered normalization-insensitivity verdicts, one per directory.
static NORMALIZATION_ANSWERS: VolumeAnswers = VolumeAnswers::new();

/// One probe verdict per directory, asked of the filesystem once.
///
/// A probe creates and removes a file, and what it measures is a property of
/// the volume rather than of the moment. A search over numbered candidate
/// names in one directory asks for a key per candidate, which without this
/// writes and unlinks a file per candidate in the directory it is about to
/// write into.
struct VolumeAnswers {
    seen:
        std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeMap<std::path::PathBuf, bool>>>,
}

impl VolumeAnswers {
    const fn new() -> Self {
        Self {
            seen: std::sync::OnceLock::new(),
        }
    }

    fn answer(
        &self,
        dir: &Path,
        probe: impl FnOnce(&Path) -> io::Result<bool>,
    ) -> io::Result<bool> {
        let seen = self
            .seen
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()));
        if let Ok(remembered) = seen.lock()
            && let Some(answer) = remembered.get(dir)
        {
            return Ok(*answer);
        }
        let answer = probe(dir)?;
        if let Ok(mut remembered) = seen.lock() {
            remembered.insert(dir.to_path_buf(), answer);
        }
        Ok(answer)
    }

    /// Record a verdict for `dir` without asking the filesystem.
    ///
    /// The per-component rule can only be exercised against a path that
    /// crosses volumes behaving differently, and no test can mount two. This
    /// is where an injected verdict enters: the memo is already the single
    /// place a verdict is read from, so seeding it is the same seam a real
    /// probe fills, not a second one beside it.
    #[cfg(test)]
    fn remember(&self, dir: &Path, answer: bool) {
        let seen = self
            .seen
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()));
        if let Ok(mut remembered) = seen.lock() {
            remembered.insert(dir.to_path_buf(), answer);
        }
    }
}

/// Walk up from `path`'s parent to the first directory that exists. Returns
/// `None` only when neither the parent chain nor the current directory exists,
/// which cannot happen for a relative path (`.` always exists) but can for an
/// absolute path whose entire prefix is absent.
fn nearest_existing_dir(path: &Path) -> Option<std::path::PathBuf> {
    let mut candidate = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // A bare filename (`errors.csv`) or empty parent resolves against the
        // current working directory.
        _ => std::path::PathBuf::from("."),
    };
    loop {
        if candidate.is_dir() {
            return Some(candidate);
        }
        match candidate.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => candidate = parent.to_path_buf(),
            _ => return None,
        }
    }
}

/// Removes a probe file when it goes out of scope, so a volume-behavior probe
/// leaves no residue on **any** return path — the normal one, an early `?`
/// bubble, or an unwinding panic between creating the file and reading the
/// result.
///
/// A probe cannot be relocated to an OS temp dir to sidestep the residue
/// concern: it must be created in the very directory being classified, because
/// case-sensitivity and normalization-insensitivity are per-*filesystem* (and,
/// on Windows, per-*directory*) properties that only a file created on that
/// exact target reveals. Since the target is frequently a tracked directory
/// (the collision-detection path probes the current working directory for a
/// bare output filename), guaranteed cleanup is the mechanism that keeps a
/// probe from ever littering the working tree.
struct ProbeFile {
    path: std::path::PathBuf,
}

impl Drop for ProbeFile {
    fn drop(&mut self) {
        // Best-effort: a genuine OS unlink failure is outside our control, and a
        // removal error must never mask the probe result the caller is after.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Create a uniquely-named lowercase probe file in `dir` and report whether its
/// uppercased name resolves to the same file. OS-agnostic: it measures the
/// filesystem's actual behavior rather than inferring it from a type table, so
/// no `#[cfg]` arm is needed. The probe file is registered with a [`ProbeFile`]
/// guard the instant it exists, so it is removed on every exit path rather than
/// only on the success path.
fn probe_case_sensitive(dir: &Path) -> io::Result<bool> {
    // A lowercase, process+time-unique stem so two concurrent probes in one
    // directory never clash. The extension stays lowercase; only the stem case
    // is toggled for the re-stat.
    let stem = unique_probe_stem();
    let lower = dir.join(format!("{stem}.tmp"));
    // Create exclusively so a stale same-named file can never make the probe
    // read a foreign filesystem state as our own.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lower)?;
    // Adopt the probe into an RAII guard the moment it exists on disk, so any
    // subsequent early return or panic still removes it. The binding is named
    // (`_probe`, not `_`) so it lives to the end of the scope rather than being
    // dropped immediately.
    let _probe = ProbeFile { path: lower };
    drop(file);

    let upper = dir.join(format!("{}.TMP", stem.to_ascii_uppercase()));
    // If the uppercased name resolves to a file, the filesystem folded the case
    // back onto the lowercase probe we just created — it is case-insensitive.
    let insensitive = upper.exists();

    Ok(!insensitive)
}

/// Create a probe file under the **composed** (NFC) spelling of a name and
/// report whether its **decomposed** (NFD) spelling resolves to the same file.
/// Mirrors [`probe_case_sensitive`] exactly — same stem, same exclusive
/// creation, same RAII cleanup — because the question has the same shape: what
/// the volume does with a name is only answerable by asking the volume.
///
/// The distinguishing character is `é`: `U+00E9` composed, `e` + `U+0301`
/// decomposed. Canonically equivalent, so a normalization-insensitive volume
/// (APFS in either variant, HFS+) finds the probe file under the decomposed
/// spelling; a normalizing-nothing volume (ext4, NTFS) does not.
fn probe_normalization_insensitive(dir: &Path) -> io::Result<bool> {
    let stem = unique_probe_stem();
    let composed = dir.join(format!("{stem}-\u{00e9}.tmp"));
    // Exclusive creation, as in the case probe: a stale same-named file must
    // never let the probe read a foreign filesystem state as its own.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&composed)?;
    let _probe = ProbeFile { path: composed };
    drop(file);

    let decomposed = dir.join(format!("{stem}-e\u{0301}.tmp"));
    Ok(decomposed.exists())
}

/// A probe stem unique to this process and instant, so two concurrent probes
/// in one directory never clash. Shared by both probes, and by the
/// `clinker-case-probe-` prefix the residue tests scan for.
fn unique_probe_stem() -> String {
    format!(
        "clinker-case-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// The identity of a destination path: one physical location, one key.
///
/// Two producers naming one file must be recognised as naming one file, and a
/// root registered before its directory exists must match the same root
/// checked afterwards. Both need the path reduced the same way, so both go
/// through here rather than each keying on the text it happens to hold.
///
/// `.` components are dropped first, because they never change which file a
/// path names. The longest existing prefix is then resolved, which is what
/// makes a symlinked parent and its target one destination; what does not
/// exist yet cannot be resolved and is kept as written.
///
/// A `..` inside the existing prefix is left to that resolution rather than
/// applied to the text. The two are not the same: when the component before it
/// is a symlink, the kernel follows the link and then goes up from where it
/// lands, so cancelling the pair lexically names a different file than the one
/// that will be opened. Past the existing prefix the text *is* what the kernel
/// will see, and the pair is cancelled — see [`resolved_prefix`].
/// A relative path is resolved against the working directory first, so the
/// same file named relatively in one place and absolutely in another has one
/// identity. Leaving that to callers is what let the plan-time check and the
/// runtime ledger key one destination two ways and report a pipeline clean
/// that the run then refused.
///
/// The reduced path is then keyed through [`collision_key`]'s rules over its
/// **native** representation — the OS's own bytes on Unix, its own UTF-16 code
/// units on Windows — never over a lossy string. A path that is not valid
/// Unicode has no lossy rendering that is also injective: every invalid
/// sequence renders as `U+FFFD`, so `\xff` and `\xfe` in one directory come
/// out as the same text and two genuinely *different* destinations collapse
/// into one identity. That is the opposite failure from a missed collision and
/// a worse one: a missed collision writes one file twice, whereas a merged
/// identity refuses, or silently reroutes, a destination that was never in
/// conflict.
#[must_use]
pub fn destination_identity(path: &Path) -> String {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    };
    let reduced = resolved_prefix(&without_cur_dir(&absolute));
    identity_of(&reduced)
}

/// `path` with its `.` components dropped.
fn without_cur_dir(path: &Path) -> std::path::PathBuf {
    path.components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .map(std::path::Component::as_os_str)
        .collect()
}

/// One component of the tail that sits past the longest existing prefix,
/// together with what the walk was told about it.
struct TailComponent {
    name: std::ffi::OsString,
    /// The filesystem was asked for this exact path and answered that nothing
    /// is there. A canonicalize failure alone does not establish that -- it is
    /// also how a permission denial, a symlink loop, and an over-long name
    /// come back -- and only genuine absence licenses cancelling a following
    /// `..` against this name.
    absent: bool,
}

/// `path` with its longest existing prefix canonicalized and the `..`
/// components past that prefix cancelled.
///
/// The walk steps over a `..` rather than stopping at it -- `file_name` is
/// `None` for such a component, and treating that as the end of the road left
/// the existing prefix in front of it unresolved too.
///
/// A `..` is then cancelled only against a component the filesystem said does
/// not exist, which is what makes the cancellation kernel-equivalent rather
/// than merely lexical: the danger in reducing `a/../b` textually is that `a`
/// may be a symlink, in which case the kernel goes up from where the link
/// lands and not from `a`'s own parent -- and a name that does not exist is
/// not a symlink. Nothing can be created under it either, so the parent
/// directories a run makes on its way to the destination are made along this
/// same reduced route, as real directories. Absence is checked with
/// `symlink_metadata` rather than inferred from the canonicalize failure,
/// because that failure covers cases (`EACCES`, `ELOOP`) where a symlink is
/// exactly what may be sitting there; those keep the `..` as written.
///
/// A `..` that cancels the whole tail lands on the resolved prefix, and the
/// prefix is popped: it came out of `canonicalize`, so it holds no symlinks
/// and going up from it textually is going up from it truthfully. At the root
/// the pop is a no-op, which is what the kernel does with `/..` as well.
fn resolved_prefix(path: &Path) -> std::path::PathBuf {
    let mut unresolved: Vec<TailComponent> = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(resolved) = cursor.canonicalize() {
            return rebuild_past_prefix(resolved, &unresolved);
        }
        let Some(parent) = cursor.parent() else {
            return path.to_path_buf();
        };
        let Some(last) = cursor.components().next_back() else {
            return path.to_path_buf();
        };
        unresolved.push(TailComponent {
            name: last.as_os_str().to_owned(),
            absent: matches!(
                cursor.symlink_metadata(),
                Err(error) if error.kind() == io::ErrorKind::NotFound
            ),
        });
        cursor = parent;
    }
}

/// Join the tail -- innermost component last in `tail` -- onto the resolved
/// prefix, cancelling the `..` components that are sound to cancel.
fn rebuild_past_prefix(resolved: std::path::PathBuf, tail: &[TailComponent]) -> std::path::PathBuf {
    let mut prefix = resolved;
    let mut kept: Vec<&TailComponent> = Vec::new();
    for component in tail.iter().rev() {
        if component.name != ".." {
            kept.push(component);
            continue;
        }
        match kept.last() {
            Some(previous) if previous.absent && previous.name != ".." => {
                kept.pop();
            }
            // Every tail component the `..` could have cancelled is gone, so
            // it goes up from the resolved prefix itself.
            None => {
                prefix.pop();
            }
            // The component in front of it may exist, and may therefore be a
            // symlink. What that pair names is the kernel's to say.
            Some(_) => kept.push(component),
        }
    }
    for component in kept {
        prefix.push(&component.name);
    }
    prefix
}

/// Canonical collision key for an output path: the key under which two paths
/// are considered to name the *same physical file*.
///
/// Two **orthogonal** volume behaviours can make different spellings name one
/// file, and each is probed separately because neither implies the other:
///
/// | [`case_sensitive_dir`] | [`normalization_insensitive_dir`] | key | volume |
/// |---|---|---|---|
/// | insensitive | insensitive | `nfd(fold(nfd(s)))` | APFS (ci), HFS+ |
/// | insensitive | normalizing | `fold(s)` | NTFS |
/// | sensitive | insensitive | `nfd(s)` | APFS (cs), iOS |
/// | sensitive | normalizing | `s` | ext4, xfs, btrfs |
///
/// The both-fire composition is Unicode D145 *canonical caseless match*,
/// outer NFD included. That outer pass is conformance and forward-insurance
/// rather than a live correction today, and the distinction is worth being
/// exact about: a rule advertised as tighter than it is would be worse than a
/// stated gap. Canonical ordering only reorders adjacent *non-starters*, so a
/// fold can only break NFD by turning a starter into one — and on Unicode
/// 16.0 no simple fold does. The single fold that moves a combining class at
/// all, `U+0345 -> U+03B9`, moves it 240 -> 0, the harmless direction. (Under
/// *full* folding the outer pass does real work, which is where its reputation
/// comes from.) `no_simple_fold_raises_a_canonical_combining_class` pins that
/// premise, so a later Unicode table cannot quietly make the outer NFD
/// load-bearing without the tests saying so.
///
/// The fold is Unicode **simple** (C+S) folding, not default/full folding. The
/// distinction is not a nicety: full folding maps `ß` to `ss`, so it declares
/// `straße.csv` and `strasse.csv` one file — which no filesystem does. This is
/// also why the `caseless` crate is not used here despite exposing D145
/// directly: its fold is full.
///
/// A probe failure falls back to leaving that axis alone, the safe default
/// that never merges two paths the filesystem might keep distinct.
///
/// # Residual gap
///
/// This rule is close, not exact, and it is wrong in both directions — stated
/// separately because the two directions have very different consequences.
///
/// **Over-detection (safe).** The key may merge two names a real volume keeps
/// apart: simple folding covers non-BMP scalars that NTFS's `$UpCase` table
/// and HFS+'s BMP-only case table cannot fold; `$UpCase` is written at
/// format time and frozen, so an older volume folds less than the table this
/// crate ships; and HFS+ leaves certain ranges undecomposed that NFD
/// decomposes. The cost is refusing a run that would have worked, with a
/// diagnostic naming both paths — visible, and correctable by renaming.
///
/// **Under-detection (the real gap, and irreducible).** NTFS does not fold the
/// way Unicode does. It *upper-cases*, 1:1 only, through a per-volume table
/// that has carried Turkish linguistic mappings since NT 4.0 and Azeri since
/// NT 5.01 — so it folds locale-sensitively where Unicode's simple fold is
/// deliberately locale-independent. `İ.csv` and `i.csv` may therefore be one
/// file on such a volume while this key still reports two, and the run writes
/// that file twice. No table fixes this, which is precisely why the empirical
/// probe is primary and the Unicode tables are only the fallback behaviour
/// applied once a probe has said which axes are live.
///
/// Both the config-time DLQ-collision check and the runtime DLQ partitioner key
/// on this single function so their notions of "same file" cannot drift.
#[must_use]
pub fn collision_key(path: &str) -> String {
    identity_of(Path::new(path))
}

/// The two volume behaviours a key is folded under, as probed for one
/// directory.
#[derive(Clone, Copy)]
struct FoldingVerdict {
    folds_case: bool,
    folds_normalization: bool,
}

/// Key `path` under the behaviour probed for each of its components.
///
/// Kept apart from the two public entry points so [`collision_key`] and
/// [`destination_identity`] land in one key space rather than two that merely
/// look alike: the runtime ledger and the plan-time check compare their keys
/// against each other.
///
/// **Each component is folded under a probe of its own containing directory**,
/// not under one probe applied to the whole path. A path crosses mount points,
/// and each volume it crosses answers for the names directly inside it and for
/// nothing else. One probe of the deepest directory said that `/srv/Reports/ci`
/// and `/srv/reports/ci` fold to one key whenever the innermost volume happens
/// to be case-insensitive — even where `/srv` is case-sensitive and the two are
/// two genuinely different mounted volumes holding two genuinely different
/// files, which the collision check then refused to run.
///
/// Where a component's own directory cannot be probed — an unwritable `/`, a
/// component past the point the path exists — the whole-path verdict stands in.
/// That is the answer this function gave everywhere before, so an unprobeable
/// ancestor is never *worse* than it was; it is simply not improved. Probing
/// per component and defaulting each unprobeable one to "folds nothing" would
/// be worse, because it would stop folding `/Users/Foo` on the very macOS
/// volume that folds it.
///
/// The key is built component-wise but written with the platform's own
/// separators, so a path whose components all answer alike keys to exactly the
/// string the whole-path fold produced.
fn identity_of(path: &Path) -> String {
    use std::path::Component;

    // One probe of the nearest existing ancestor: the answer that stands in
    // wherever a component's own directory has none to give.
    let fallback = FoldingVerdict {
        folds_case: !case_sensitive_dir(path).unwrap_or(true),
        folds_normalization: normalization_insensitive_dir(path).unwrap_or(false),
    };

    let mut key = String::new();
    let mut container = std::path::PathBuf::new();
    let separator = std::path::MAIN_SEPARATOR;
    let separate = |key: &mut String| {
        if !key.is_empty() && !key.ends_with(separator) {
            key.push(separator);
        }
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                // A Windows drive or share root: no containing directory
                // answers for it, so it keys the way the whole path does.
                push_identity(
                    prefix.as_os_str(),
                    fallback.folds_case,
                    fallback.folds_normalization,
                    &mut key,
                );
                container.push(prefix.as_os_str());
            }
            Component::RootDir => {
                key.push(separator);
                container.push(std::path::MAIN_SEPARATOR_STR);
            }
            // Not names of anything, so nothing folds them. `Components`
            // yields these only where they change which file is named.
            Component::CurDir => {
                separate(&mut key);
                key.push('.');
                container.push(".");
            }
            Component::ParentDir => {
                separate(&mut key);
                key.push_str("..");
                container.push("..");
            }
            Component::Normal(name) => {
                let verdict = verdict_for(&container, name, fallback);
                separate(&mut key);
                push_identity(
                    name,
                    verdict.folds_case,
                    verdict.folds_normalization,
                    &mut key,
                );
                container.push(name);
            }
        }
    }
    key
}

/// The folding behaviour of the volume that backs `name` inside `container`.
///
/// Both probes resolve to the nearest existing directory at or above
/// `container`, so a component past the end of what exists is answered by the
/// deepest directory that does exist — the same directory the whole-path probe
/// used. Each distinct directory is probed once per process and remembered, so
/// the per-component rule costs one probe pair per directory a plan's outputs
/// touch, not one per path.
fn verdict_for(
    container: &Path,
    name: &std::ffi::OsStr,
    fallback: FoldingVerdict,
) -> FoldingVerdict {
    let probe_at = if container.as_os_str().is_empty() {
        std::path::PathBuf::from(name)
    } else {
        container.join(name)
    };
    FoldingVerdict {
        folds_case: case_sensitive_dir(&probe_at)
            .map_or(fallback.folds_case, |sensitive| !sensitive),
        folds_normalization: normalization_insensitive_dir(&probe_at)
            .unwrap_or(fallback.folds_normalization),
    }
}

/// The Unicode reduction applied to one run of a path that *is* valid Unicode.
/// Split out from the segmenting so the composition rule can be tested against
/// injected probe verdicts on every platform, not only on a host whose disk
/// happens to behave the right way.
fn fold_run(run: &str, folds_case: bool, folds_normalization: bool) -> String {
    use unicode_normalization::UnicodeNormalization;
    match (folds_case, folds_normalization) {
        (false, false) => run.to_string(),
        (true, false) => simple_case_fold(run),
        (false, true) => run.nfd().collect(),
        (true, true) => simple_case_fold(&run.nfd().collect::<String>())
            .nfd()
            .collect(),
    }
}

/// Unicode **simple** case folding (the `C` and `S` rows of `CaseFolding.txt`):
/// one scalar in, one scalar out. `case_folded` returns `None` for a scalar
/// that folds to itself, `ß` among them — the property that keeps `straße` and
/// `strasse` two files.
fn simple_case_fold(run: &str) -> String {
    run.chars()
        .map(|scalar| {
            unicode_case_mapping::case_folded(scalar)
                .and_then(|folded| char::from_u32(folded.get()))
                .unwrap_or(scalar)
        })
        .collect()
}

/// Append `name`'s key to `out`, in the OS's own representation.
///
/// Folding is defined over Unicode scalar values; a native path may hold
/// sequences that are not scalar values at all (any byte string on Unix, any
/// UTF-16 unit sequence including unpaired surrogates on Windows). The
/// composition rule is therefore: **split the name at the boundaries of what
/// can be interpreted as Unicode, fold each maximal valid run, and escape each
/// uninterpretable unit verbatim.** A fully-valid path — every realistic one —
/// is a single run and keys exactly as if the name were a `&str`; an invalid
/// unit stops the fold at its edges instead of stopping it for the whole path.
///
/// A name that is valid Unicode throughout — which is every realistic path —
/// is one run, and its key is that run folded, written verbatim. Only a name
/// carrying at least one uninterpretable unit needs the segmented form, and it
/// is tagged with a leading NUL to keep the two forms in disjoint key spaces.
/// NUL is available for that because it is the one unit no path can carry: it
/// terminates a path string on every supported platform, so a name holding one
/// can never be opened and is never a destination.
///
/// The segmented form is injective because each segment is self-delimiting: a
/// folded run is written length-prefixed as `u<byte-len>:<run>` and an
/// uninterpretable unit as `x<4 hex digits>`, so segment boundaries are
/// recoverable and no two distinct names produce one key. That injectivity is
/// the whole point — it is what keeps two paths differing *only* in their
/// invalid units from collapsing into one destination.
///
/// The key is opaque: callers use it as a map key and carry the raw path
/// separately for diagnostics.
fn push_identity(
    name: &std::ffi::OsStr,
    folds_case: bool,
    folds_normalization: bool,
    out: &mut String,
) {
    if let Some(whole) = name.to_str() {
        out.push_str(&fold_run(whole, folds_case, folds_normalization));
        return;
    }
    out.push(UNINTERPRETABLE_TAG);
    push_uninterpretable_identity(name, folds_case, folds_normalization, out);
}

/// Marks the segmented key space. See [`push_identity`] for why NUL is the
/// unit that can carry this without ever colliding with a real path.
const UNINTERPRETABLE_TAG: char = '\0';

#[cfg(unix)]
fn push_uninterpretable_identity(
    name: &std::ffi::OsStr,
    folds_case: bool,
    folds_normalization: bool,
    out: &mut String,
) {
    use std::os::unix::ffi::OsStrExt;
    push_identity_bytes(name.as_bytes(), folds_case, folds_normalization, out);
}

#[cfg(windows)]
fn push_uninterpretable_identity(
    name: &std::ffi::OsStr,
    folds_case: bool,
    folds_normalization: bool,
    out: &mut String,
) {
    use std::os::windows::ffi::OsStrExt;
    // Windows paths are UTF-16 code units, which `decode_utf16` splits into
    // exactly the two cases the rule distinguishes: a scalar value, or an
    // unpaired surrogate that is not one.
    let mut run = String::new();
    for unit in char::decode_utf16(name.encode_wide()) {
        match unit {
            Ok(scalar) => run.push(scalar),
            Err(unpaired) => {
                if !run.is_empty() {
                    push_run(out, &fold_run(&run, folds_case, folds_normalization));
                    run.clear();
                }
                push_escape(out, u32::from(unpaired.unpaired_surrogate()));
            }
        }
    }
    if !run.is_empty() {
        push_run(out, &fold_run(&run, folds_case, folds_normalization));
    }
}

/// Neither Unix nor Windows: `as_encoded_bytes` is the only lossless view of an
/// `OsStr` the standard library offers portably. Its exact encoding is
/// unspecified, which is acceptable here because these keys never leave the
/// process — they are compared against other keys built the same way in the
/// same run, never persisted.
#[cfg(not(any(unix, windows)))]
fn push_uninterpretable_identity(
    name: &std::ffi::OsStr,
    folds_case: bool,
    folds_normalization: bool,
    out: &mut String,
) {
    push_identity_bytes(
        name.as_encoded_bytes(),
        folds_case,
        folds_normalization,
        out,
    );
}

/// Segment a byte-oriented native name into maximal valid-UTF-8 runs and the
/// individual bytes that cannot begin or continue one.
#[cfg(not(windows))]
fn push_identity_bytes(name: &[u8], folds_case: bool, folds_normalization: bool, out: &mut String) {
    let mut rest = name;
    while !rest.is_empty() {
        let error = match std::str::from_utf8(rest) {
            Ok(run) => {
                push_run(out, &fold_run(run, folds_case, folds_normalization));
                return;
            }
            Err(error) => error,
        };
        let valid = error.valid_up_to();
        match std::str::from_utf8(&rest[..valid]) {
            Ok(run) if !run.is_empty() => {
                push_run(out, &fold_run(run, folds_case, folds_normalization));
            }
            // Unreachable by `valid_up_to`'s contract. Escaping the bytes
            // rather than dropping them keeps the key lossless even if that
            // contract were ever to change under us.
            Ok(_) => {}
            Err(_) => {
                for byte in &rest[..valid] {
                    push_escape(out, u32::from(*byte));
                }
            }
        }
        // `error_len() == None` means the name ends mid-sequence: everything
        // left is an incomplete tail and none of it is interpretable.
        let bad = error.error_len().unwrap_or(rest.len() - valid);
        for byte in &rest[valid..valid + bad] {
            push_escape(out, u32::from(*byte));
        }
        rest = &rest[valid + bad..];
    }
}

/// A folded run, length-prefixed so the run's own bytes can never be mistaken
/// for the start of the next segment.
fn push_run(out: &mut String, folded: &str) {
    out.push('u');
    out.push_str(&folded.len().to_string());
    out.push(':');
    out.push_str(folded);
}

/// One uninterpretable native unit, as a fixed-width hex escape. Fixed width is
/// what makes it self-delimiting without a length prefix; four digits covers a
/// Unix byte and a Windows unpaired surrogate alike, so the encoding reads the
/// same on every platform.
fn push_escape(out: &mut String, unit: u32) {
    const HEX: [u8; 16] = *b"0123456789abcdef";
    out.push('x');
    for shift in [12u32, 8, 4, 0] {
        out.push(HEX[((unit >> shift) & 0xf) as usize] as char);
    }
}

// ---------------------------------------------------------------------------
// Linux
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn classify_family_impl(path: &Path) -> io::Result<FilesystemFamily> {
    use nix::sys::statfs::{
        FUSE_SUPER_MAGIC, NFS_SUPER_MAGIC, SMB_SUPER_MAGIC, TMPFS_MAGIC, statfs,
    };

    // Magics absent from nix's exported set, taken from the kernel's
    // `include/uapi/linux/magic.h`:
    //   RAMFS_MAGIC        0x858458f6  (in-memory, like tmpfs but no swap)
    //   SMB2_MAGIC_NUMBER  0xfe534d42  (cifs.ko mounting SMB2/3 — the modern
    //                                   Windows-share transport; SMB_SUPER_MAGIC
    //                                   0x517b is the retired smbfs)
    //   CIFS_MAGIC_NUMBER  0xff534d42  (cifs.ko's other reported magic)
    // Compared as u64 against the raw `f_type` so the per-arch width of nix's
    // `FsType` inner integer is irrelevant.
    const RAMFS_MAGIC: u64 = 0x8584_58f6;
    const SMB2_MAGIC_NUMBER: u64 = 0xfe53_4d42;
    const CIFS_MAGIC_NUMBER: u64 = 0xff53_4d42;

    let st = statfs(path).map_err(io::Error::from)?;
    let raw = st.filesystem_type().0 as u64;

    let tmpfs = TMPFS_MAGIC.0 as u64;
    let nfs = NFS_SUPER_MAGIC.0 as u64;
    let smb = SMB_SUPER_MAGIC.0 as u64;
    let fuse = FUSE_SUPER_MAGIC.0 as u64;

    Ok(if raw == tmpfs || raw == RAMFS_MAGIC {
        FilesystemFamily::InMemory
    } else if raw == nfs {
        FilesystemFamily::Nfs
    } else if raw == smb || raw == SMB2_MAGIC_NUMBER || raw == CIFS_MAGIC_NUMBER {
        FilesystemFamily::Smb
    } else if raw == fuse {
        FilesystemFamily::OtherNetwork
    } else {
        FilesystemFamily::Local
    })
}

// ---------------------------------------------------------------------------
// macOS (and other Apple targets)
// ---------------------------------------------------------------------------

#[cfg(target_vendor = "apple")]
fn classify_family_impl(path: &Path) -> io::Result<FilesystemFamily> {
    use nix::sys::statfs::statfs;

    // Darwin's `f_type` is undocumented and shifts between releases, so the
    // string `f_fstypename` is the only stable signal. Match the network
    // transports by name; macOS has no native tmpfs, so nothing here maps to
    // `InMemory` — a macOS RAM disk presents as hfs/apfs over a synthetic
    // device and reads as `Local`, which the module docs call out.
    let st = statfs(path).map_err(io::Error::from)?;
    let name = st.filesystem_type_name().to_ascii_lowercase();

    Ok(match name.as_str() {
        "nfs" => FilesystemFamily::Nfs,
        "smbfs" | "cifs" => FilesystemFamily::Smb,
        "webdav" | "ftp" | "afpfs" => FilesystemFamily::OtherNetwork,
        // FUSE mounts on macOS report the backing implementation's name
        // (e.g. "macfuse", "osxfuse", or a "fuse"-prefixed string).
        n if n.contains("fuse") => FilesystemFamily::OtherNetwork,
        _ => FilesystemFamily::Local,
    })
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn classify_family_impl(path: &Path) -> io::Result<FilesystemFamily> {
    use windows_sys::Win32::Storage::FileSystem::GetDriveTypeW;
    use windows_sys::Win32::System::WindowsProgramming::{DRIVE_RAMDISK, DRIVE_REMOTE};

    // GetDriveTypeW wants the volume mount root, not an arbitrary path, so
    // resolve the mount root first and classify the drive behind it.
    let root = win::volume_root(path)?;
    let root_wide = win::to_wide(&root);
    // SAFETY: `root_wide` is a NUL-terminated UTF-16 string; GetDriveTypeW
    // reads it as a PCWSTR and returns a plain enum value (never an error
    // code that requires GetLastError).
    let drive_type = unsafe { GetDriveTypeW(root_wide.as_ptr()) };
    Ok(match drive_type {
        // GetDriveTypeW does not expose the remote transport. Keep it
        // unqualified so Windows cannot silently accept an NFS or SMB profile
        // without protocol-family evidence.
        DRIVE_REMOTE => FilesystemFamily::OtherNetwork,
        DRIVE_RAMDISK => FilesystemFamily::InMemory,
        _ => FilesystemFamily::Local,
    })
}

// ---------------------------------------------------------------------------
// Fallback for any target that is neither Linux, Apple, nor Windows (e.g. the
// BSDs). statfs name-matching could classify these too, but no such target is
// a first-class clinker platform, so they conservatively read as `Local`
// rather than carrying an unverified per-OS table.
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
fn classify_family_impl(path: &Path) -> io::Result<FilesystemFamily> {
    // Still require the path to exist so the contract ("must be probeable")
    // holds uniformly across targets.
    std::fs::metadata(path)?;
    Ok(FilesystemFamily::Local)
}

#[cfg(unix)]
fn same_device_impl(a: &Path, b: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let dev = |p: &Path| -> io::Result<u64> { std::fs::metadata(p).map(|m| m.dev()) };
    Ok(dev(a)? == dev(b)?)
}

#[cfg(windows)]
fn same_device_impl(a: &Path, b: &Path) -> io::Result<bool> {
    Ok(win::volume_serial(a)? == win::volume_serial(b)?)
}

/// Fallback same-device check for targets that are neither Unix nor Windows:
/// compares the path's root prefix component. Coarser than the `st_dev` /
/// volume-serial checks above, but every clinker target hits one of those, so
/// this arm exists only to keep the function total.
#[cfg(not(any(unix, windows)))]
fn same_device_impl(a: &Path, b: &Path) -> io::Result<bool> {
    std::fs::metadata(a)?;
    std::fs::metadata(b)?;
    Ok(a.components().next() == b.components().next())
}

#[cfg(windows)]
mod win {
    use std::ffi::OsString;
    use std::io;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::path::Path;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_READ, FILE_SHARE_WRITE,
        GetVolumeInformationByHandleW, GetVolumePathNameW, OPEN_EXISTING,
    };

    /// Encode an OS string as a NUL-terminated UTF-16 buffer for a `PCWSTR`.
    pub(super) fn to_wide(s: &std::ffi::OsStr) -> Vec<u16> {
        s.encode_wide().chain(std::iter::once(0)).collect()
    }

    /// Map `path` to the mount root of the volume that contains it via
    /// `GetVolumePathNameW`. Two paths share a volume exactly when their mount
    /// roots match, which distinguishes distinct mount points sharing a drive
    /// letter where a prefix compare cannot.
    pub(super) fn volume_root(path: &Path) -> io::Result<OsString> {
        let wide = to_wide(path.as_os_str());
        // MAX_PATH is the documented ceiling for a volume mount-point string.
        let mut buf = vec![0u16; 260];
        // SAFETY: `wide` is a NUL-terminated UTF-16 path; `buf` is a writable
        // u16 buffer whose capacity is passed as the length, exactly as the
        // GetVolumePathNameW contract requires.
        let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), buf.as_mut_ptr(), buf.len() as u32) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        Ok(OsString::from_wide(&buf[..len]))
    }

    /// Read the NTFS volume serial number for the volume containing `path`.
    ///
    /// libstd's `MetadataExt::volume_serial_number()` is nightly-only, so the
    /// handle is opened and queried directly. The handle is opened with zero
    /// desired access plus `FILE_FLAG_BACKUP_SEMANTICS` — the documented way
    /// to obtain a metadata-only handle that works for both files and
    /// directories without requiring read permission.
    pub(super) fn volume_serial(path: &Path) -> io::Result<u32> {
        let wide = to_wide(path.as_os_str());
        // SAFETY: `wide` is a NUL-terminated UTF-16 path. Zero desired access
        // with FILE_FLAG_BACKUP_SEMANTICS yields a metadata-only handle valid
        // for files and directories; the remaining args follow the CreateFileW
        // contract (no security attributes, no template handle).
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let mut serial: u32 = 0;
        // SAFETY: `handle` is a valid open handle (checked above). Every output
        // pointer is either a live `&mut` or null where the field is unwanted,
        // exactly as GetVolumeInformationByHandleW permits.
        let ok = unsafe {
            GetVolumeInformationByHandleW(
                handle,
                std::ptr::null_mut(),
                0,
                &mut serial,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        // SAFETY: `handle` came from CreateFileW and has not been closed.
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(serial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Ärger.csv` and `ärger.csv` name one file on a case-insensitive volume.
    /// ASCII-only folding reports two, so that volume gets written twice under
    /// two identities the engine believes are distinct destinations.
    #[test]
    fn case_folding_reaches_beyond_ascii() {
        assert_eq!(
            fold_run("Ärger.csv", true, false),
            fold_run("ärger.csv", true, false),
            "a case-folding volume names one file here"
        );
        assert_eq!(
            fold_run("ΣΊΣΥΦΟΣ.csv", true, false),
            fold_run("σίσυφος.csv", true, false),
            "and it is the Unicode fold, not a Latin-1 approximation"
        );
    }

    /// The residual **under-detection** gap, asserted rather than described,
    /// so it stays a known quantity instead of drifting into a surprise.
    ///
    /// NTFS has folded `İ` (`U+0130`) linguistically since NT 4.0 through its
    /// per-volume `$UpCase` table. Unicode's simple fold is deliberately
    /// locale-independent and gives `U+0130` no `C`/`S` mapping at all, so
    /// `İ.csv` and `i.csv` key as two destinations here while such a volume
    /// may hold one file. No table closes this — it is why the empirical probe
    /// is primary. Over-detection is the safe direction and this is the unsafe
    /// one, so it is written down.
    #[test]
    fn locale_sensitive_folds_are_a_known_under_detection_gap() {
        assert_ne!(
            fold_run("İ.csv", true, true),
            fold_run("i.csv", true, true),
            "if this now merges, the fold stopped being locale-independent \
             and the documented residual gap needs rewriting"
        );
    }

    /// The fold is Unicode **simple** (C+S), not default/full. Full folding
    /// maps `ß` to `ss` and would merge two paths that every real filesystem
    /// keeps apart — the reason `caseless`, whose D145 helper is otherwise
    /// exactly this rule, cannot be used.
    #[test]
    fn case_folding_is_simple_not_full() {
        for axes in [(true, false), (true, true)] {
            assert_ne!(
                fold_run("straße.csv", axes.0, axes.1),
                fold_run("strasse.csv", axes.0, axes.1),
                "no filesystem folds ß to ss; {axes:?} must not either"
            );
        }
    }

    /// Composed and decomposed spellings of one name are one file on a
    /// normalization-insensitive volume — including a **case-sensitive** APFS
    /// volume, where folding alone detects nothing.
    #[test]
    fn normalization_reconciles_composed_and_decomposed() {
        let composed = "caf\u{e9}.csv";
        let decomposed = "cafe\u{301}.csv";
        assert_eq!(
            fold_run(composed, false, true),
            fold_run(decomposed, false, true),
            "a normalization-insensitive volume names one file here"
        );
        assert_eq!(
            fold_run(composed, true, true),
            fold_run(decomposed, true, true),
            "and so does one that folds case as well"
        );
    }

    /// The two axes are orthogonal: each probe closes a gap the other leaves
    /// wide open, so a key composed from only one of them is wrong on real
    /// hardware. Asserting all four cells is what pins that.
    #[test]
    fn the_two_axes_are_independent() {
        let (upper_composed, lower_composed) = ("Caf\u{e9}.csv", "caf\u{e9}.csv");
        let lower_decomposed = "cafe\u{301}.csv";

        // ext4 / xfs / NTFS-with-per-directory-case-sensitivity: nothing merges.
        assert_ne!(
            fold_run(upper_composed, false, false),
            fold_run(lower_composed, false, false)
        );
        assert_ne!(
            fold_run(lower_composed, false, false),
            fold_run(lower_decomposed, false, false)
        );

        // NTFS: folds, never normalizes.
        assert_eq!(
            fold_run(upper_composed, true, false),
            fold_run(lower_composed, true, false)
        );
        assert_ne!(
            fold_run(lower_composed, true, false),
            fold_run(lower_decomposed, true, false),
            "NTFS keeps the two normal forms apart"
        );

        // Case-sensitive APFS / iOS: normalizes, never folds.
        assert_ne!(
            fold_run(upper_composed, false, true),
            fold_run(lower_composed, false, true),
            "a case-sensitive volume keeps the two cases apart"
        );
        assert_eq!(
            fold_run(lower_composed, false, true),
            fold_run(lower_decomposed, false, true)
        );

        // Case-insensitive APFS / HFS+: both, which is Unicode D145.
        assert_eq!(
            fold_run(upper_composed, true, true),
            fold_run(lower_decomposed, true, true)
        );
    }

    /// The premise that lets the outer NFD in the both-axes composition be
    /// correct-but-currently-inert, pinned so a Unicode data bump cannot
    /// silently invalidate it.
    ///
    /// Canonical ordering only reorders *adjacent non-starters*. A simple fold
    /// can therefore only put a string out of canonical order by turning a
    /// starter (ccc 0) into a non-starter. On Unicode 16.0 no scalar does
    /// that — the only fold that moves a combining class at all is
    /// `U+0345 -> U+03B9`, which goes the harmless direction (240 -> 0). If a
    /// later table adds a raising fold, this fires, and the outer NFD stops
    /// being insurance and starts being load-bearing.
    #[test]
    fn no_simple_fold_raises_a_canonical_combining_class() {
        use unicode_normalization::char::canonical_combining_class as combining_class;
        let raising: Vec<u32> = (0u32..=0x10_FFFF)
            .filter_map(char::from_u32)
            .filter(|scalar| {
                unicode_case_mapping::case_folded(*scalar)
                    .and_then(|folded| char::from_u32(folded.get()))
                    .is_some_and(|folded| {
                        combining_class(*scalar) == 0 && combining_class(folded) != 0
                    })
            })
            .map(u32::from)
            .collect();
        assert!(
            raising.is_empty(),
            "these folds raise a combining class, so the outer NFD in \
             `fold_run` is now load-bearing rather than insurance: {raising:04X?}"
        );
    }

    /// Defect: routing a path through `to_string_lossy` turns *every* invalid
    /// sequence into `U+FFFD`, so two genuinely different destinations that
    /// differ only in their invalid bytes come out as one identity. That is
    /// the opposite of a missed collision and worse: two distinct files are
    /// treated as one.
    #[test]
    fn distinct_uninterpretable_paths_keep_distinct_identities() {
        let dir = tempfile::tempdir().unwrap();
        let first = uninterpretable_child(dir.path(), 0);
        let second = uninterpretable_child(dir.path(), 1);
        assert_ne!(
            first, second,
            "the two test paths must differ to begin with"
        );
        assert_ne!(
            destination_identity(&first),
            destination_identity(&second),
            "two destinations differing only in uninterpretable units are two files"
        );
    }

    /// And an uninterpretable unit must not key as the replacement character
    /// either — otherwise a path a user really did spell with `U+FFFD` would
    /// merge with an unrelated one carrying invalid bytes.
    #[test]
    fn an_uninterpretable_unit_is_not_the_replacement_character() {
        let dir = tempfile::tempdir().unwrap();
        let invalid = uninterpretable_child(dir.path(), 0);
        let spelled = dir.path().join("\u{FFFD}.csv");
        assert_ne!(
            destination_identity(&invalid),
            destination_identity(&spelled),
            "an invalid unit is not the same destination as a literal U+FFFD"
        );
    }

    /// An uninterpretable unit stops the fold at its own edges, not for the
    /// whole path: the valid runs around it still fold. Folding is defined
    /// over scalar values, so the composition rule is to segment first and
    /// fold each maximal valid run.
    #[test]
    fn an_uninterpretable_unit_does_not_stop_the_fold_around_it() {
        let upper = uninterpretable_between("ÄRGER", "MÜNCHEN.csv");
        let lower = uninterpretable_between("ärger", "münchen.csv");
        let key = |name: &std::ffi::OsString| {
            let mut out = String::new();
            push_identity(name.as_os_str(), true, false, &mut out);
            out
        };
        assert_ne!(upper, lower);
        assert_eq!(
            key(&upper),
            key(&lower),
            "the runs either side of an uninterpretable unit still fold"
        );
    }

    /// A path holding an uninterpretable unit at index `nth` of a small set of
    /// distinct ones, built in the platform's own representation.
    #[cfg(unix)]
    fn uninterpretable_child(dir: &Path, nth: usize) -> std::path::PathBuf {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let mut bytes = dir.as_os_str().as_bytes().to_vec();
        bytes.extend_from_slice(b"/");
        bytes.push([0xff, 0xfe][nth]);
        bytes.extend_from_slice(b".csv");
        std::path::PathBuf::from(std::ffi::OsString::from_vec(bytes))
    }

    #[cfg(windows)]
    fn uninterpretable_child(dir: &Path, nth: usize) -> std::path::PathBuf {
        use std::os::windows::ffi::{OsStrExt, OsStringExt};
        let mut units: Vec<u16> = dir.as_os_str().encode_wide().collect();
        units.push(u16::from(b'\\'));
        // Lone high surrogates: valid UTF-16 code units, not scalar values.
        units.push([0xd800, 0xd801][nth]);
        units.extend(".csv".encode_utf16());
        std::path::PathBuf::from(std::ffi::OsString::from_wide(&units))
    }

    /// `before` and `after` joined by one uninterpretable unit.
    #[cfg(unix)]
    fn uninterpretable_between(before: &str, after: &str) -> std::ffi::OsString {
        use std::os::unix::ffi::OsStringExt;
        let mut bytes = before.as_bytes().to_vec();
        bytes.push(0xff);
        bytes.extend_from_slice(after.as_bytes());
        std::ffi::OsString::from_vec(bytes)
    }

    #[cfg(windows)]
    fn uninterpretable_between(before: &str, after: &str) -> std::ffi::OsString {
        use std::os::windows::ffi::OsStringExt;
        let mut units: Vec<u16> = before.encode_utf16().collect();
        units.push(0xd800);
        units.extend(after.encode_utf16());
        std::ffi::OsString::from_wide(&units)
    }

    /// Two real destinations differing only in non-ASCII case are one file
    /// exactly when the volume under them folds case. Both branches assert, so
    /// this is a live check on every CI platform rather than a test that
    /// quietly does nothing off macOS/Windows.
    #[test]
    fn case_variant_destinations_collide_exactly_when_the_volume_folds() {
        let dir = tempfile::tempdir().unwrap();
        let upper = dir.path().join("Ärger.csv");
        let lower = dir.path().join("ärger.csv");
        let folds = !case_sensitive_dir(&upper).unwrap();
        eprintln!("volume under {:?} folds case: {folds}", dir.path());
        assert_eq!(
            destination_identity(&upper) == destination_identity(&lower),
            folds,
            "non-ASCII case variants must collide iff the volume folds case"
        );
    }

    /// The same, for the normalization axis.
    #[test]
    fn normalization_variant_destinations_collide_exactly_when_the_volume_normalizes() {
        let dir = tempfile::tempdir().unwrap();
        let composed = dir.path().join("caf\u{e9}.csv");
        let decomposed = dir.path().join("cafe\u{301}.csv");
        let normalizes = normalization_insensitive_dir(&composed).unwrap();
        eprintln!(
            "volume under {:?} is normalization-insensitive: {normalizes}",
            dir.path()
        );
        assert_eq!(
            destination_identity(&composed) == destination_identity(&decomposed),
            normalizes,
            "NFC/NFD variants must collide iff the volume normalizes lookups"
        );
    }

    /// Unconditional on every volume, because no filesystem expands `ß`: this
    /// one holds whatever the probes say, which is exactly why it guards
    /// against a future switch to full folding.
    #[test]
    fn sharp_s_destinations_never_collide() {
        let dir = tempfile::tempdir().unwrap();
        assert_ne!(
            destination_identity(&dir.path().join("straße.csv")),
            destination_identity(&dir.path().join("strasse.csv")),
            "no filesystem folds ß to ss, so these are always two destinations"
        );
    }

    /// The normalization probe is cross-checked against the ground truth on
    /// whatever filesystem the host provides, exactly as the case probe is:
    /// create a composed name, ask whether its decomposed spelling resolves to
    /// it, and require the probe to have said the same thing.
    #[test]
    fn normalization_probe_agrees_with_a_direct_re_spelling_stat() {
        let dir = tempfile::tempdir().unwrap();
        let composed = dir.path().join("caf\u{e9}.csv");
        std::fs::write(&composed, b"x").unwrap();
        let decomposed_twin = dir.path().join("cafe\u{301}.csv");
        let resolved = decomposed_twin.exists();

        let verdict = normalization_insensitive_dir(&dir.path().join("out.csv")).unwrap();
        assert_eq!(
            verdict, resolved,
            "the probe must report what the filesystem actually does"
        );
    }

    /// The normalization probe leaves no residue either — it shares the
    /// `clinker-case-probe-` prefix and the same RAII guard.
    #[test]
    fn normalization_probe_leaves_no_probe_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("errors.csv");
        let _ = normalization_insensitive_dir(&target).unwrap();
        assert!(!target.exists(), "the probe must not create the real path");
        let residue: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name.to_string_lossy().starts_with("clinker-case-probe-"))
            .collect();
        assert!(residue.is_empty(), "probe residue left behind: {residue:?}");
    }

    #[test]
    fn tempdir_is_probeable() {
        // A real, existing directory classifies without error on every host.
        let dir = tempfile::tempdir().unwrap();
        let kind = classify(dir.path()).unwrap();
        // The host's tempdir is whatever the OS provides; on CI it is a local
        // disk, but a developer running tests inside a tmpfs /tmp would see
        // InMemory. Both are valid — the contract is only that probing
        // succeeds and returns one of the known variants.
        assert!(matches!(
            kind,
            FsKind::Local | FsKind::InMemory | FsKind::Network
        ));
    }

    #[test]
    fn classify_errors_on_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(classify(&missing).is_err());
    }

    #[test]
    fn same_device_true_for_two_paths_in_one_dir() {
        // Two files created under the same tempdir share a device/volume.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        std::fs::write(&a, b"a").unwrap();
        std::fs::write(&b, b"b").unwrap();
        assert!(same_device(&a, &b).unwrap());
    }

    #[test]
    fn same_device_for_dir_and_its_own_child() {
        let dir = tempfile::tempdir().unwrap();
        let child = dir.path().join("c.txt");
        std::fs::write(&child, b"c").unwrap();
        assert!(same_device(dir.path(), &child).unwrap());
    }

    #[test]
    fn same_device_errors_on_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.txt");
        std::fs::write(&real, b"x").unwrap();
        let missing = dir.path().join("nope.txt");
        assert!(same_device(&real, &missing).is_err());
    }

    #[test]
    fn case_sensitive_dir_probes_an_existing_dir_without_error() {
        // The host tempdir is whatever the OS provides; the contract here is
        // only that the active probe runs to completion and returns a verdict
        // (whichever the host filesystem actually is) rather than erroring.
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("errors.csv");
        let verdict = case_sensitive_dir(&target).unwrap();
        // The probe leaves no file behind: it cleans up its temp probe and the
        // real output path was never created.
        assert!(!target.exists());
        // `verdict` is a real measurement of this filesystem; both values are
        // legitimate depending on the host (case-sensitive ext4/tmpfs vs a
        // case-insensitive mount), so we only assert it is one of the two.
        let _ = verdict;
    }

    #[test]
    fn case_sensitive_dir_walks_up_to_an_existing_ancestor() {
        // A path whose parent directories do not exist yet still probes: the
        // walk-up lands on the nearest existing ancestor (the filesystem the
        // writer will `create_dir_all` the missing parents onto).
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a/b/c/errors.csv");
        assert!(!deep.parent().unwrap().exists());
        // Should not error — it probes `dir` (the nearest existing ancestor).
        let _ = case_sensitive_dir(&deep).unwrap();
    }

    #[test]
    fn case_sensitive_dir_handles_bare_filename_against_cwd() {
        // A bare filename has an empty parent and must resolve against the
        // current working directory, which always exists, so the probe never
        // fails for lack of an ancestor.
        let verdict = case_sensitive_dir(Path::new("errors.csv"));
        assert!(verdict.is_ok());
    }

    #[test]
    fn case_sensitive_dir_agrees_with_a_direct_re_case_stat() {
        // Cross-check the probe against the ground truth on whatever filesystem
        // the host provides: create a lowercase file, ask whether its uppercase
        // twin resolves to it, and assert `case_sensitive_dir` returns the
        // opposite of that fold. Deterministic on every host (it measures the
        // real filesystem) and never a silent no-op.
        let dir = tempfile::tempdir().unwrap();
        let lower = dir.path().join("errors.csv");
        std::fs::write(&lower, b"x").unwrap();
        let upper_twin = dir.path().join("ERRORS.CSV");
        let folded = upper_twin.exists();

        let target = dir.path().join("out.csv");
        let sensitive = case_sensitive_dir(&target).unwrap();
        // Case-insensitive filesystem ⇔ the uppercase twin folded onto the
        // lowercase file.
        assert_eq!(sensitive, !folded);
    }

    #[test]
    fn probe_file_guard_removes_its_file_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("residue.tmp");
        std::fs::write(&p, b"x").unwrap();
        assert!(p.exists());
        {
            let _guard = ProbeFile { path: p.clone() };
        }
        assert!(!p.exists(), "ProbeFile must remove its file on drop");
    }

    #[test]
    fn probe_file_guard_removes_its_file_even_when_unwinding() {
        // A probe file created against a tracked directory must not survive a
        // panic between creation and the point where it would otherwise be
        // cleaned up. A plain best-effort `remove_file` at the end of the
        // function would leak here; the RAII guard removes the file while the
        // stack unwinds.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("residue-panic.tmp");
        std::fs::write(&p, b"x").unwrap();
        let path_for_closure = p.clone();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ProbeFile {
                path: path_for_closure,
            };
            panic!("boom");
        }));
        assert!(caught.is_err(), "the closure was expected to panic");
        assert!(
            !p.exists(),
            "ProbeFile must remove its file even while the stack unwinds"
        );
    }

    /// One physical location, one identity. `.` never changes which file a
    /// path names, so it is dropped; a symlinked parent and its target are the
    /// same place, so the existing prefix is resolved; and `..` is left to
    /// that resolution, because cancelling it against a symlink names a
    /// different file than the kernel will open.
    #[test]
    fn one_location_has_one_identity() {
        let root = tempfile::tempdir().expect("temporary root");
        let real = root.path().join("real");
        std::fs::create_dir(&real).expect("create the real directory");

        let direct = real.join("out.csv");
        assert_eq!(
            destination_identity(&real.join(".").join("out.csv")),
            destination_identity(&direct),
            "a `.` component names the same file"
        );

        #[cfg(unix)]
        {
            let link = root.path().join("link");
            std::os::unix::fs::symlink(&real, &link).expect("link to the real directory");
            assert_eq!(
                destination_identity(&link.join("out.csv")),
                destination_identity(&direct),
                "and so does a symlinked parent, which is what the resolution is for"
            );

            // A `..` further along must not cost the prefix in front of it its
            // resolution. Asserted through the link, because comparing two
            // spellings that reduce to the same text proves nothing: what is
            // being checked is that the prefix was resolved at all.
            let through_link = link.join("later").join("..").join("out.csv");
            assert!(
                destination_identity(&through_link).starts_with(
                    &destination_identity(&real)
                        .trim_end_matches("out.csv")
                        .to_owned()
                ),
                "the prefix in front of a `..` is resolved through the link"
            );
            assert_eq!(
                destination_identity(&through_link),
                destination_identity(&real.join("later").join("..").join("out.csv")),
                "so the link and its target agree either side of one"
            );

            // `link/../out.csv` is `<root>/out.csv` only if `..` is cancelled
            // against the link's own name; the kernel resolves the link first
            // and goes up from `real`, which is the same place.
            assert_eq!(
                destination_identity(&link.join("..").join("out.csv")),
                destination_identity(&root.path().join("out.csv")),
                "`..` after a symlink goes up from where the link lands"
            );
        }
    }

    /// A `..` past the end of what exists names the same file as the spelling
    /// without it. Two producers writing `out/data.csv` and
    /// `out/pending/../data.csv` write one file, so they must key as one --
    /// keeping the `..` as text gave them two keys, and the collision check
    /// admitted both.
    #[test]
    fn a_dot_dot_past_the_existing_prefix_is_cancelled() {
        let root = tempfile::tempdir().expect("temporary root");
        let out = root.path().join("out");
        std::fs::create_dir(&out).expect("create the existing prefix");

        assert_eq!(
            destination_identity(&out.join("pending").join("..").join("data.csv")),
            destination_identity(&out.join("data.csv")),
            "`pending` does not exist, so it cannot be a symlink, and the pair cancels"
        );

        assert_eq!(
            destination_identity(
                &out.join("a")
                    .join("b")
                    .join("..")
                    .join("..")
                    .join("data.csv")
            ),
            destination_identity(&out.join("data.csv")),
            "and so does a run of them"
        );

        // Cancelling the whole tail lands on the resolved prefix, which holds
        // no symlinks -- so going up from it textually is going up from it
        // truthfully.
        assert_eq!(
            destination_identity(&out.join("pending").join("..").join("..").join("data.csv")),
            destination_identity(&root.path().join("data.csv")),
            "a `..` with nothing left to cancel goes up from the resolved prefix"
        );

        // A `..` that runs out of prefix stays where it is, as it does for the
        // kernel. Asserted on the reduction itself: keying it would probe the
        // filesystem root, which a test has no business writing to.
        //
        // A root is not one component everywhere: on Windows an absolute path
        // opens with a prefix (`\\?\C:`, `C:`, `\\server\share`) *and* a root
        // separator, and taking only the first yields `\\?\C:`, which names
        // the current directory of drive C: rather than the drive's root. Take
        // the whole run of leading prefix/root components, and hold the result
        // to `has_root` so a path that is merely drive-relative cannot pass
        // for a rooted one.
        let root_of = |path: &Path| {
            let mut root = std::path::PathBuf::new();
            for component in path.components() {
                match component {
                    std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                        root.push(component.as_os_str());
                    }
                    _ => break,
                }
            }
            assert!(root.has_root(), "an absolute path has a root: {root:?}");
            root
        };
        // `resolved_prefix` reaches the root through `canonicalize`, so the
        // expectation has to start from the same canonical spelling of it --
        // on Windows that is the verbatim `\\?\C:\`, not the `C:\` the
        // temporary directory was named with.
        let canonical_out = out.canonicalize().expect("the existing prefix resolves");
        let deep = out.join("nope").join("..").join("..");
        let mut escaping = deep.clone();
        for _ in 0..out.components().count() {
            escaping.push("..");
        }
        assert_eq!(
            resolved_prefix(&escaping.join("data.csv")),
            root_of(&canonical_out).join("data.csv"),
            "`..` past the root stays at the root"
        );

        #[cfg(unix)]
        {
            // The reduction stops where certainty does: `later` exists and is
            // a symlink, so what `later/..` names is the kernel's to say and
            // the resolution -- not the text -- decides it.
            let elsewhere = root.path().join("elsewhere");
            std::fs::create_dir(&elsewhere).expect("create the link target");
            let later = out.join("later");
            std::os::unix::fs::symlink(&elsewhere, &later).expect("link into it");
            assert_eq!(
                destination_identity(&later.join("..").join("data.csv")),
                destination_identity(&root.path().join("data.csv")),
                "`..` after a symlink goes up from where the link lands, not from `out`"
            );
        }
    }

    /// One probe is one volume's answer, not the whole path's.
    ///
    /// `identity_of` probed the deepest existing directory and applied that
    /// verdict to every component above it. A path crosses mount points: with
    /// `/srv` case-sensitive and `/srv/Reports` and `/srv/reports` two
    /// separate case-insensitive volumes, both outputs folded to one key and
    /// the collision check reported E317 for two genuinely different files,
    /// refusing a valid pipeline. Each component now answers to the directory
    /// that actually holds it.
    ///
    /// Read off the key one path produces rather than off a pair of
    /// case-variant sibling directories. Two directories differing only in
    /// case can be *created* only where the container keeps case, and the
    /// temporary volume does not on macOS — the second `create_dir_all` is a
    /// no-op there, `canonicalize` hands back the one on-disk spelling, and
    /// the pair the fixture meant to build is one directory named twice. The
    /// single chain here exists under one spelling on every platform, and
    /// every verdict the rule reads is seeded, so the expected key is the same
    /// everywhere: `Reports` is answered by `root`, which keeps case, and `CI`
    /// by `Reports`, which folds it. One verdict for the whole path cannot
    /// produce that key whichever verdict it picks.
    #[test]
    fn a_component_folds_under_its_own_directory_not_the_deepest_one() {
        let root = tempfile::tempdir().expect("tempdir");
        let root = root.path().canonicalize().expect("canonical tempdir");
        let inner = root.join("Reports").join("CI");
        std::fs::create_dir_all(&inner).expect("mkdir");

        // `root` keeps case; `Reports` stands for a case-insensitive volume
        // mounted inside it, and `CI` for a directory on that volume. A probe
        // answers for the names *inside* the directory it runs in, so it is
        // the seeded verdict of `root` that decides how `Reports` keys.
        CASE_ANSWERS.remember(&root, true);
        CASE_ANSWERS.remember(&root.join("Reports"), false);
        CASE_ANSWERS.remember(&inner, false);

        let key = destination_identity(&inner.join("Out.csv"));
        let separator = std::path::MAIN_SEPARATOR;
        assert!(
            key.ends_with(&format!("Reports{separator}ci{separator}out.csv")),
            "`Reports` sits in a directory that keeps case and `CI` in one that \
             folds it, so the two adjacent components key differently: {key}"
        );

        // The folding volume still folds the names it actually holds, which is
        // the consequence the per-component rule must not cost: two spellings
        // inside it are one destination.
        assert_eq!(
            destination_identity(&inner.join("Out.csv")),
            destination_identity(&inner.join("out.csv")),
            "a name inside a case-insensitive volume is still one destination"
        );
    }
}
