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
    classify_impl(path)
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

// ---------------------------------------------------------------------------
// Linux
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn classify_impl(path: &Path) -> io::Result<FsKind> {
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
        FsKind::InMemory
    } else if raw == nfs
        || raw == smb
        || raw == SMB2_MAGIC_NUMBER
        || raw == CIFS_MAGIC_NUMBER
        || raw == fuse
    {
        FsKind::Network
    } else {
        FsKind::Local
    })
}

// ---------------------------------------------------------------------------
// macOS (and other Apple targets)
// ---------------------------------------------------------------------------

#[cfg(target_vendor = "apple")]
fn classify_impl(path: &Path) -> io::Result<FsKind> {
    use nix::sys::statfs::statfs;

    // Darwin's `f_type` is undocumented and shifts between releases, so the
    // string `f_fstypename` is the only stable signal. Match the network
    // transports by name; macOS has no native tmpfs, so nothing here maps to
    // `InMemory` — a macOS RAM disk presents as hfs/apfs over a synthetic
    // device and reads as `Local`, which the module docs call out.
    let st = statfs(path).map_err(io::Error::from)?;
    let name = st.filesystem_type_name().to_ascii_lowercase();

    Ok(match name.as_str() {
        "nfs" | "smbfs" | "cifs" | "webdav" | "ftp" | "afpfs" => FsKind::Network,
        // FUSE mounts on macOS report the backing implementation's name
        // (e.g. "macfuse", "osxfuse", or a "fuse"-prefixed string).
        n if n.contains("fuse") => FsKind::Network,
        _ => FsKind::Local,
    })
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

#[cfg(windows)]
fn classify_impl(path: &Path) -> io::Result<FsKind> {
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
        DRIVE_REMOTE => FsKind::Network,
        DRIVE_RAMDISK => FsKind::InMemory,
        _ => FsKind::Local,
    })
}

// ---------------------------------------------------------------------------
// Fallback for any target that is neither Linux, Apple, nor Windows (e.g. the
// BSDs). statfs name-matching could classify these too, but no such target is
// a first-class clinker platform, so they conservatively read as `Local`
// rather than carrying an unverified per-OS table.
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
fn classify_impl(path: &Path) -> io::Result<FsKind> {
    // Still require the path to exist so the contract ("must be probeable")
    // holds uniformly across targets.
    std::fs::metadata(path)?;
    Ok(FsKind::Local)
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
}
