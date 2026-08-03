//! Handle-relative output containment and same-filesystem promotion.
//!
//! [`ValidatedPath`] proves that a logical path passed plan-time validation.
//! This module adds the use-time proof: every directory component is opened
//! without following links, the destination directory handle is retained, and
//! the final open or rename is relative to that handle.

use std::ffi::OsString;
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use clinker_plan::security::ValidatedPath;
use thiserror::Error;

/// The only storage profiles eligible for output publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesystemProfile {
    Detected,
    Local,
    LinuxNfsV41LoopbackCi,
    LinuxSmb311LoopbackCi,
}

impl FilesystemProfile {
    fn parse(name: &str) -> Result<Self, ContainmentError> {
        match name {
            "detected-filesystem" => Ok(Self::Detected),
            "local-filesystem" => Ok(Self::Local),
            "linux-nfsv4.1-loopback-ci" => Ok(Self::LinuxNfsV41LoopbackCi),
            "linux-smb3.1.1-loopback-ci" => Ok(Self::LinuxSmb311LoopbackCi),
            _ => Err(ContainmentError::PolicyRequired {
                profile: name.to_owned(),
                detail: "storage profile is not in the qualified output-publication matrix",
            }),
        }
    }
}

/// Creation behavior for a contained output leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenDisposition {
    /// Create a new file and reject an existing leaf.
    CreateNew,
    /// Create the leaf or truncate an existing regular file.
    Truncate,
}

/// Collision behavior for same-filesystem atomic promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionDisposition {
    /// Replace an existing destination atomically.
    Replace,
    /// Refuse to replace an existing destination.
    NoReplace,
}

/// Matchable containment failures for callers and diagnostics.
#[derive(Debug, Error)]
pub enum ContainmentError {
    /// The requested operation violates a proven security invariant.
    #[error("security_policy[{code}]: {detail} ({path})", path = path.display())]
    SecurityPolicy {
        code: &'static str,
        path: PathBuf,
        detail: &'static str,
    },
    /// The filesystem combination has no positive qualification record.
    #[error("policy_required[{profile}]: {detail}")]
    PolicyRequired {
        profile: String,
        detail: &'static str,
    },
    /// An operating-system operation failed without changing policy category.
    #[error(
        "output containment {operation} failed for {path}: {source}",
        path = path.display()
    )]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The atomic rename completed, but durability of the visible destination
    /// entry could not be established.
    #[error(
        "output promotion made the destination visible, but synchronizing its parent failed for {path}: {source}",
        path = path.display()
    )]
    VisibleButUnsynced {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl ContainmentError {
    fn security(code: &'static str, path: &Path, detail: &'static str) -> Self {
        Self::SecurityPolicy {
            code,
            path: path.to_path_buf(),
            detail,
        }
    }

    fn io(operation: &'static str, path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }
}

/// A destination leaf anchored beneath a retained, link-free parent handle.
#[derive(Debug)]
pub struct OutputContainment {
    destination: ValidatedPath,
    leaf: OsString,
    parent: platform::DirectoryAnchor,
}

/// A destination-local hidden output that is ready for atomic publication.
///
/// The destination boundary and its retained parent handle stay alive from
/// admission through [`Self::commit`]. Dropping this value before commit leaves
/// the hidden partial in place for operator inspection and never touches the
/// final destination.
#[derive(Debug)]
pub struct StagedOutput {
    destination: OutputContainment,
    quarantine_leaf: OsString,
    quarantine_path: PathBuf,
    disposition: PromotionDisposition,
    reservation_leaf: Option<OsString>,
}

static QUARANTINE_COUNTER: AtomicU64 = AtomicU64::new(0);

impl OutputContainment {
    /// Resolve a qualified profile before touching the destination, then open
    /// and retain its parent directory without following any link component.
    ///
    /// # Errors
    ///
    /// Returns [`ContainmentError::PolicyRequired`] for every unlisted or
    /// mismatched storage profile and [`ContainmentError::SecurityPolicy`] for
    /// link traversal, non-normal leaves, or unsupported path forms.
    pub fn for_profile(
        destination: ValidatedPath,
        profile_name: &str,
    ) -> Result<Self, ContainmentError> {
        // Parse first: an unsupported profile cannot cause even a directory
        // handle open, much less an output side effect.
        let profile = FilesystemProfile::parse(profile_name)?;
        let path = absolute_path(destination.as_path())?;
        let leaf = normal_leaf(&path)?;
        let parent_path = path.parent().ok_or_else(|| {
            ContainmentError::security(
                "destination_parent_missing",
                &path,
                "destination must have an existing parent directory",
            )
        })?;
        let parent = platform::DirectoryAnchor::open(parent_path)?;
        parent.verify_profile(profile, parent_path)?;
        Ok(Self {
            destination,
            leaf,
            parent,
        })
    }

    /// Borrow the validated logical destination.
    #[must_use]
    pub fn destination(&self) -> &ValidatedPath {
        &self.destination
    }

    /// Open the final leaf relative to the retained directory handle.
    ///
    /// # Errors
    ///
    /// Returns a security-policy error when the leaf is a link/reparse point,
    /// and an I/O error when creation fails for another reason.
    pub fn open(&self, disposition: OpenDisposition) -> Result<File, ContainmentError> {
        self.parent
            .open_leaf(&self.leaf, disposition, self.destination.as_path())
    }

    /// Check the final leaf relative to the retained destination handle.
    ///
    /// # Errors
    ///
    /// Returns a security-policy error for a linked/reparse destination leaf,
    /// or an I/O error when the leaf cannot be inspected.
    pub fn destination_exists(&self) -> Result<bool, ContainmentError> {
        self.parent
            .leaf_exists(&self.leaf, self.destination.as_path())
    }

    /// Create a destination-local hidden file without opening or truncating the
    /// final leaf. The returned [`StagedOutput`] retains the destination anchor
    /// required for a later handle-relative promotion.
    ///
    /// # Errors
    ///
    /// Returns a security-policy or I/O error if a unique hidden leaf cannot be
    /// created relative to the retained destination directory.
    pub fn stage(
        self,
        disposition: PromotionDisposition,
    ) -> Result<(StagedOutput, File), ContainmentError> {
        let reservation_leaf = if disposition == PromotionDisposition::NoReplace {
            if self.destination_exists()? {
                return Err(ContainmentError::io(
                    "reserve-destination-leaf",
                    self.destination.as_path(),
                    std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "destination already exists",
                    ),
                ));
            }
            let mut reservation_leaf = OsString::from(".clinker-");
            reservation_leaf.push(&self.leaf);
            reservation_leaf.push(".reservation");
            let reservation_path =
                parent_path_of(self.destination.as_path()).join(&reservation_leaf);
            let reservation = self.parent.open_leaf(
                &reservation_leaf,
                OpenDisposition::CreateNew,
                &reservation_path,
            )?;
            drop(reservation);
            Some(reservation_leaf)
        } else {
            None
        };
        let parent_path = self
            .destination
            .as_path()
            .parent()
            .expect("construction requires a destination parent");
        for _ in 0..1024 {
            let counter = QUARANTINE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut quarantine_leaf = OsString::from(".clinker-");
            quarantine_leaf.push(&self.leaf);
            quarantine_leaf.push(format!("-{}-{counter}.partial", std::process::id()));
            let quarantine_path = parent_path.join(&quarantine_leaf);
            match self.parent.open_leaf(
                &quarantine_leaf,
                OpenDisposition::CreateNew,
                &quarantine_path,
            ) {
                Ok(file) => {
                    return Ok((
                        StagedOutput {
                            destination: self,
                            quarantine_leaf,
                            quarantine_path,
                            disposition,
                            reservation_leaf,
                        },
                        file,
                    ));
                }
                Err(ContainmentError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    if let Some(reservation_leaf) = reservation_leaf.as_ref() {
                        let _ = self.parent.remove_leaf(reservation_leaf);
                    }
                    return Err(error);
                }
            }
        }
        if let Some(reservation_leaf) = reservation_leaf.as_ref() {
            let _ = self.parent.remove_leaf(reservation_leaf);
        }
        Err(ContainmentError::io(
            "create-quarantine-leaf",
            self.destination.as_path(),
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "could not allocate a unique destination-local quarantine leaf",
            ),
        ))
    }

    /// Atomically promote a validated source relative to its independently
    /// retained parent handle. Cross-filesystem promotion is rejected before
    /// rename; there is deliberately no copy fallback.
    ///
    /// # Errors
    ///
    /// Returns a security-policy error for source link traversal or differing
    /// filesystems, and an I/O error for a failed same-filesystem rename/sync.
    pub fn promote_from(
        &self,
        source: ValidatedPath,
        disposition: PromotionDisposition,
    ) -> Result<(), ContainmentError> {
        self.promote_from_with_sync_fault(source, disposition, false)
    }

    /// Promotion entry point used by deterministic boundary tests that must
    /// fail only after the final rename has made the destination visible.
    pub(crate) fn promote_from_with_sync_fault(
        &self,
        source: ValidatedPath,
        disposition: PromotionDisposition,
        fail_after_rename: bool,
    ) -> Result<(), ContainmentError> {
        let source_path = absolute_path(source.as_path())?;
        let source_leaf = normal_leaf(&source_path)?;
        let source_parent_path = source_path.parent().ok_or_else(|| {
            ContainmentError::security(
                "source_parent_missing",
                &source_path,
                "promotion source must have an existing parent directory",
            )
        })?;
        let source_parent = platform::DirectoryAnchor::open(source_parent_path)?;
        self.parent.promote(
            &source_parent,
            &source_leaf,
            &self.leaf,
            disposition,
            &source_path,
            self.destination.as_path(),
            fail_after_rename,
        )
    }
}

impl StagedOutput {
    /// The final destination selected for this staged output.
    #[must_use]
    pub fn destination(&self) -> &ValidatedPath {
        self.destination.destination()
    }

    /// The hidden destination-local path holding partial or complete bytes.
    #[must_use]
    pub fn partial_path(&self) -> &Path {
        &self.quarantine_path
    }

    /// Synchronize and atomically publish the hidden output through the parent
    /// handle retained since admission.
    ///
    /// # Errors
    ///
    /// Returns a collision, confinement, synchronization, or publication error.
    pub fn commit(mut self) -> Result<(), ContainmentError> {
        let result = self.destination.parent.promote(
            &self.destination.parent,
            &self.quarantine_leaf,
            &self.destination.leaf,
            self.disposition,
            &self.quarantine_path,
            self.destination.destination.as_path(),
            false,
        );
        if result.is_ok()
            && let Some(reservation_leaf) = self.reservation_leaf.take()
        {
            self.destination.parent.remove_leaf(&reservation_leaf)?;
        }
        result
    }
}

impl Drop for StagedOutput {
    fn drop(&mut self) {
        if let Some(reservation_leaf) = self.reservation_leaf.take() {
            let _ = self.destination.parent.remove_leaf(&reservation_leaf);
        }
    }
}

fn parent_path_of(path: &Path) -> &Path {
    path.parent()
        .expect("construction requires a destination parent")
}

fn absolute_path(path: &Path) -> Result<PathBuf, ContainmentError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|source| ContainmentError::io("resolve-current-directory", path, source))
}

fn normal_leaf(path: &Path) -> Result<OsString, ContainmentError> {
    match path.components().next_back() {
        Some(Component::Normal(leaf)) if !leaf.is_empty() => Ok(leaf.to_os_string()),
        _ => Err(ContainmentError::security(
            "invalid_destination_leaf",
            path,
            "output leaf must be one normal path component",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clinker_plan::security::validate_path;

    use super::{ContainmentError, OutputContainment, PromotionDisposition};

    #[test]
    fn post_rename_sync_failure_reports_visible_destination() {
        let root = tempfile::tempdir().expect("temporary output root");
        let source_path = root.path().join("partial.bin");
        let destination_path = root.path().join("result.bin");
        std::fs::write(&source_path, b"complete artifact").expect("source artifact");
        let source = validate_path(Path::new("partial.bin"), root.path(), false)
            .expect("source fixture should validate");
        let destination = validate_path(Path::new("result.bin"), root.path(), false)
            .expect("destination fixture should validate");
        let boundary = OutputContainment::for_profile(destination, "local-filesystem")
            .expect("local destination should be supported");

        let error = boundary
            .promote_from_with_sync_fault(source, PromotionDisposition::Replace, true)
            .expect_err("injected post-rename synchronization failure must surface");

        assert!(matches!(error, ContainmentError::VisibleButUnsynced { .. }));
        assert!(!source_path.exists(), "rename consumed the partial");
        assert_eq!(
            std::fs::read(destination_path).expect("visible final remains inspectable"),
            b"complete artifact"
        );
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::os::fd::OwnedFd;
    use std::path::{Component, Path};

    use nix::errno::Errno;
    use nix::fcntl::{OFlag, RenameFlags, openat, renameat, renameat2};
    use nix::sys::stat::{Mode, fstat};
    use nix::sys::statfs::{NFS_SUPER_MAGIC, fstatfs};
    use nix::unistd::{UnlinkatFlags, unlinkat};

    use super::{ContainmentError, FilesystemProfile, OpenDisposition, PromotionDisposition};

    const CIFS_SUPER_MAGIC: u32 = 0xff53_4d42;
    const SMB2_SUPER_MAGIC: u32 = 0xfe53_4d42;
    const SMB_SUPER_MAGIC: u32 = 0x517b;

    #[derive(Debug)]
    pub(super) struct DirectoryAnchor {
        file: File,
        device: u64,
    }

    impl DirectoryAnchor {
        pub(super) fn open(path: &Path) -> Result<Self, ContainmentError> {
            let mut current = File::open("/")
                .map_err(|source| ContainmentError::io("open-root", path, source))?;
            for component in path.components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(name) => {
                        let next = openat(
                            &current,
                            name,
                            OFlag::O_RDONLY
                                | OFlag::O_DIRECTORY
                                | OFlag::O_NOFOLLOW
                                | OFlag::O_CLOEXEC,
                            Mode::empty(),
                        )
                        .map_err(|error| component_error(path, error))?;
                        current = File::from(next);
                    }
                    _ => {
                        return Err(ContainmentError::security(
                            "non_normal_ancestor",
                            path,
                            "output ancestors must be absolute normal path components",
                        ));
                    }
                }
            }
            let stat = fstat(&current)
                .map_err(|error| nix_io("inspect-destination-parent", path, error))?;
            Ok(Self {
                file: current,
                device: stat.st_dev as u64,
            })
        }

        pub(super) fn verify_profile(
            &self,
            profile: FilesystemProfile,
            path: &Path,
        ) -> Result<(), ContainmentError> {
            let stat =
                fstatfs(&self.file).map_err(|error| nix_io("probe-filesystem", path, error))?;
            // Linux filesystem magic values are a 32-bit UAPI even where the
            // statfs field is machine-word-sized. Normalize before comparing
            // so high-bit CIFS/SMB2 identities work on every architecture.
            let magic = stat.filesystem_type().0 as u32;
            let observed = observed_filesystem(magic);
            let supported = matches!(
                (profile, observed),
                (FilesystemProfile::Detected, _)
                    | (FilesystemProfile::Local, ObservedFilesystem::Local)
                    | (
                        FilesystemProfile::LinuxNfsV41LoopbackCi,
                        ObservedFilesystem::Nfs
                    )
                    | (
                        FilesystemProfile::LinuxSmb311LoopbackCi,
                        ObservedFilesystem::Smb
                    )
            );
            if supported {
                Ok(())
            } else {
                Err(ContainmentError::PolicyRequired {
                    profile: profile_name(profile).to_owned(),
                    detail: "requested profile does not match the destination filesystem capability probe",
                })
            }
        }

        pub(super) fn open_leaf(
            &self,
            leaf: &OsStr,
            disposition: OpenDisposition,
            display_path: &Path,
        ) -> Result<File, ContainmentError> {
            let creation = match disposition {
                OpenDisposition::CreateNew => OFlag::O_CREAT | OFlag::O_EXCL,
                OpenDisposition::Truncate => OFlag::O_CREAT | OFlag::O_TRUNC,
            };
            let fd = openat(
                &self.file,
                leaf,
                OFlag::O_WRONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | creation,
                Mode::from_bits_truncate(0o600),
            )
            .map_err(|error| leaf_error(display_path, error))?;
            Ok(File::from(fd))
        }

        pub(super) fn leaf_exists(
            &self,
            leaf: &OsStr,
            display_path: &Path,
        ) -> Result<bool, ContainmentError> {
            match openat(
                &self.file,
                leaf,
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => {
                    drop(fd);
                    Ok(true)
                }
                Err(Errno::ENOENT) => Ok(false),
                Err(error) => Err(leaf_error(display_path, error)),
            }
        }

        pub(super) fn remove_leaf(&self, leaf: &OsStr) -> Result<(), ContainmentError> {
            unlinkat(&self.file, leaf, UnlinkatFlags::NoRemoveDir)
                .map_err(|error| nix_io("remove-contained-leaf", Path::new(leaf), error))
        }

        #[allow(clippy::too_many_arguments)]
        pub(super) fn promote(
            &self,
            source_parent: &Self,
            source_leaf: &OsStr,
            destination_leaf: &OsStr,
            disposition: PromotionDisposition,
            source_path: &Path,
            destination_path: &Path,
            fail_after_rename: bool,
        ) -> Result<(), ContainmentError> {
            let source_fd: OwnedFd = openat(
                &source_parent.file,
                source_leaf,
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|error| leaf_error(source_path, error))?;
            let source_stat = fstat(&source_fd)
                .map_err(|error| nix_io("inspect-promotion-source", source_path, error))?;
            if source_stat.st_dev as u64 != self.device || source_parent.device != self.device {
                return Err(ContainmentError::security(
                    "cross_filesystem_promotion",
                    destination_path,
                    "atomic publication requires source quarantine and destination on one filesystem",
                ));
            }
            File::from(source_fd).sync_all().map_err(|source| {
                ContainmentError::io("sync-promotion-source", source_path, source)
            })?;

            match disposition {
                PromotionDisposition::Replace => renameat(
                    &source_parent.file,
                    source_leaf,
                    &self.file,
                    destination_leaf,
                ),
                PromotionDisposition::NoReplace => renameat2(
                    &source_parent.file,
                    source_leaf,
                    &self.file,
                    destination_leaf,
                    RenameFlags::RENAME_NOREPLACE,
                ),
            }
            .map_err(|error| nix_io("atomic-promotion", destination_path, error))?;

            if fail_after_rename {
                return Err(ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source: std::io::Error::other("injected post-rename directory sync failure"),
                });
            }
            self.file
                .sync_all()
                .map_err(|source| ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source,
                })
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ObservedFilesystem {
        Local,
        Nfs,
        Smb,
    }

    fn observed_filesystem(magic: u32) -> ObservedFilesystem {
        if magic == NFS_SUPER_MAGIC.0 as u32 {
            ObservedFilesystem::Nfs
        } else if matches!(magic, CIFS_SUPER_MAGIC | SMB2_SUPER_MAGIC | SMB_SUPER_MAGIC) {
            ObservedFilesystem::Smb
        } else {
            ObservedFilesystem::Local
        }
    }

    fn profile_name(profile: FilesystemProfile) -> &'static str {
        match profile {
            FilesystemProfile::Detected => "detected-filesystem",
            FilesystemProfile::Local => "local-filesystem",
            FilesystemProfile::LinuxNfsV41LoopbackCi => "linux-nfsv4.1-loopback-ci",
            FilesystemProfile::LinuxSmb311LoopbackCi => "linux-smb3.1.1-loopback-ci",
        }
    }

    fn component_error(path: &Path, error: Errno) -> ContainmentError {
        if matches!(error, Errno::ELOOP | Errno::ENOTDIR) {
            ContainmentError::security(
                "linked_or_replaced_ancestor",
                path,
                "an output ancestor is a link, is not a directory, or was replaced",
            )
        } else {
            nix_io("open-destination-ancestor", path, error)
        }
    }

    fn leaf_error(path: &Path, error: Errno) -> ContainmentError {
        if matches!(error, Errno::ELOOP) {
            ContainmentError::security("linked_output_leaf", path, "output leaf is a symbolic link")
        } else {
            nix_io("open-contained-leaf", path, error)
        }
    }

    fn nix_io(operation: &'static str, path: &Path, error: Errno) -> ContainmentError {
        ContainmentError::io(
            operation,
            path,
            std::io::Error::from_raw_os_error(error as i32),
        )
    }

    #[cfg(test)]
    mod tests {
        use super::{
            CIFS_SUPER_MAGIC, NFS_SUPER_MAGIC, ObservedFilesystem, SMB_SUPER_MAGIC,
            SMB2_SUPER_MAGIC, observed_filesystem,
        };

        #[test]
        fn linux_remote_filesystem_magic_classification_covers_supported_protocol_families() {
            assert_eq!(
                observed_filesystem(NFS_SUPER_MAGIC.0 as u32),
                ObservedFilesystem::Nfs
            );
            for magic in [CIFS_SUPER_MAGIC, SMB2_SUPER_MAGIC, SMB_SUPER_MAGIC] {
                assert_eq!(observed_filesystem(magic), ObservedFilesystem::Smb);
            }
            assert_eq!(observed_filesystem(0xef53), ObservedFilesystem::Local);
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::ffi::{CString, OsStr};
    use std::fs::File;
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Component, Path};

    use super::{ContainmentError, FilesystemProfile, OpenDisposition, PromotionDisposition};

    #[derive(Debug)]
    pub(super) struct DirectoryAnchor {
        file: File,
        device: u64,
    }

    impl DirectoryAnchor {
        pub(super) fn open(path: &Path) -> Result<Self, ContainmentError> {
            // macOS exposes the system-owned `/var` directory through the
            // stable `/var -> private/var` alias. Accept that one operating-
            // system alias without weakening the no-follow rule for any
            // caller-controlled ancestor beneath it.
            let resolved_path = resolve_system_var_alias(path);
            let mut current = File::open("/")
                .map_err(|source| ContainmentError::io("open-root", path, source))?;
            for component in resolved_path.components() {
                match component {
                    Component::RootDir => {}
                    Component::Normal(name) => {
                        let name = c_string(name, path)?;
                        // SAFETY: `current` is an open directory, `name` is a
                        // NUL-terminated single component, and a successful fd
                        // is immediately transferred into `File` ownership.
                        let fd = unsafe {
                            libc::openat(
                                current.as_raw_fd(),
                                name.as_ptr(),
                                libc::O_RDONLY
                                    | libc::O_DIRECTORY
                                    | libc::O_NOFOLLOW
                                    | libc::O_CLOEXEC,
                            )
                        };
                        if fd < 0 {
                            return Err(component_error(path, std::io::Error::last_os_error()));
                        }
                        // SAFETY: `openat` returned a fresh owned descriptor.
                        current = unsafe { File::from_raw_fd(fd) };
                    }
                    _ => {
                        return Err(ContainmentError::security(
                            "non_normal_ancestor",
                            path,
                            "output ancestors must be absolute normal path components",
                        ));
                    }
                }
            }
            let stat = file_stat(&current, path)?;
            Ok(Self {
                file: current,
                device: stat.st_dev as u64,
            })
        }

        pub(super) fn verify_profile(
            &self,
            profile: FilesystemProfile,
            path: &Path,
        ) -> Result<(), ContainmentError> {
            let mut stat = MaybeUninit::<libc::statfs>::uninit();
            // SAFETY: `stat` points to writable storage and `self.file` owns a
            // valid descriptor for the lifetime of this call.
            let result = unsafe { libc::fstatfs(self.file.as_raw_fd(), stat.as_mut_ptr()) };
            if result != 0 {
                return Err(ContainmentError::io(
                    "probe-filesystem",
                    path,
                    std::io::Error::last_os_error(),
                ));
            }
            // SAFETY: successful `fstatfs` initialized the structure.
            let stat = unsafe { stat.assume_init() };
            let length = stat
                .f_fstypename
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(stat.f_fstypename.len());
            let filesystem = String::from_utf8_lossy(
                &stat.f_fstypename[..length]
                    .iter()
                    .map(|byte| *byte as u8)
                    .collect::<Vec<_>>(),
            )
            .into_owned();
            if profile == FilesystemProfile::Detected
                || (profile == FilesystemProfile::Local
                    && filesystem != "nfs"
                    && filesystem != "smbfs")
            {
                Ok(())
            } else {
                Err(ContainmentError::PolicyRequired {
                    profile: profile_name(profile).to_owned(),
                    detail: "only local macOS filesystems are eligible; Linux loopback profiles are platform-specific",
                })
            }
        }

        pub(super) fn open_leaf(
            &self,
            leaf: &OsStr,
            disposition: OpenDisposition,
            display_path: &Path,
        ) -> Result<File, ContainmentError> {
            let leaf = c_string(leaf, display_path)?;
            let creation = match disposition {
                OpenDisposition::CreateNew => libc::O_CREAT | libc::O_EXCL,
                OpenDisposition::Truncate => libc::O_CREAT | libc::O_TRUNC,
            };
            // SAFETY: the retained parent fd and NUL-terminated one-component
            // leaf meet `openat`'s contract; success transfers ownership below.
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    leaf.as_ptr(),
                    libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | creation,
                    0o600,
                )
            };
            if fd < 0 {
                return Err(leaf_error(display_path, std::io::Error::last_os_error()));
            }
            // SAFETY: `openat` returned a fresh owned descriptor.
            Ok(unsafe { File::from_raw_fd(fd) })
        }

        pub(super) fn leaf_exists(
            &self,
            leaf: &OsStr,
            display_path: &Path,
        ) -> Result<bool, ContainmentError> {
            let leaf = c_string(leaf, display_path)?;
            // SAFETY: the retained parent and one-component leaf meet openat's
            // contract. A successful descriptor is closed immediately.
            let fd = unsafe {
                libc::openat(
                    self.file.as_raw_fd(),
                    leaf.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if fd >= 0 {
                // SAFETY: openat returned a fresh descriptor.
                drop(unsafe { File::from_raw_fd(fd) });
                return Ok(true);
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(leaf_error(display_path, error))
            }
        }

        pub(super) fn remove_leaf(&self, leaf: &OsStr) -> Result<(), ContainmentError> {
            let leaf = c_string(leaf, Path::new(leaf))?;
            // SAFETY: the retained directory and one-component leaf satisfy
            // unlinkat's contract.
            let result = unsafe { libc::unlinkat(self.file.as_raw_fd(), leaf.as_ptr(), 0) };
            if result == 0 {
                Ok(())
            } else {
                Err(ContainmentError::io(
                    "remove-contained-leaf",
                    Path::new(leaf.to_str().unwrap_or("<non-utf8>")),
                    std::io::Error::last_os_error(),
                ))
            }
        }

        #[allow(clippy::too_many_arguments)]
        pub(super) fn promote(
            &self,
            source_parent: &Self,
            source_leaf: &OsStr,
            destination_leaf: &OsStr,
            disposition: PromotionDisposition,
            source_path: &Path,
            destination_path: &Path,
            fail_after_rename: bool,
        ) -> Result<(), ContainmentError> {
            if disposition == PromotionDisposition::NoReplace {
                return Err(ContainmentError::PolicyRequired {
                    profile: "macos-atomic-no-replace".to_owned(),
                    detail: "the approved macOS renameat primitive does not provide atomic no-replace promotion",
                });
            }
            let source_leaf = c_string(source_leaf, source_path)?;
            let destination_leaf = c_string(destination_leaf, destination_path)?;
            // SAFETY: the source leaf is opened relative to a retained parent;
            // success transfers ownership into `File` immediately.
            let fd = unsafe {
                libc::openat(
                    source_parent.file.as_raw_fd(),
                    source_leaf.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(leaf_error(source_path, std::io::Error::last_os_error()));
            }
            // SAFETY: `openat` returned a fresh owned descriptor.
            let source_file = unsafe { File::from_raw_fd(fd) };
            let source_stat = file_stat(&source_file, source_path)?;
            if source_stat.st_dev as u64 != self.device || source_parent.device != self.device {
                return Err(ContainmentError::security(
                    "cross_filesystem_promotion",
                    destination_path,
                    "atomic publication requires source quarantine and destination on one filesystem",
                ));
            }
            source_file.sync_all().map_err(|source| {
                ContainmentError::io("sync-promotion-source", source_path, source)
            })?;
            // SAFETY: both names are single NUL-terminated components and both
            // descriptors are retained directory handles on the same device.
            let result = unsafe {
                libc::renameat(
                    source_parent.file.as_raw_fd(),
                    source_leaf.as_ptr(),
                    self.file.as_raw_fd(),
                    destination_leaf.as_ptr(),
                )
            };
            if result != 0 {
                return Err(ContainmentError::io(
                    "atomic-promotion",
                    destination_path,
                    std::io::Error::last_os_error(),
                ));
            }
            if fail_after_rename {
                return Err(ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source: std::io::Error::other("injected post-rename directory sync failure"),
                });
            }
            self.file
                .sync_all()
                .map_err(|source| ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source,
                })
        }
    }

    fn resolve_system_var_alias(path: &Path) -> std::path::PathBuf {
        let var = Path::new("/var");
        let Ok(relative) = path.strip_prefix(var) else {
            return path.to_path_buf();
        };
        if std::fs::read_link(var).is_ok_and(|target| target == Path::new("private/var")) {
            Path::new("/private/var").join(relative)
        } else {
            path.to_path_buf()
        }
    }

    fn file_stat(file: &File, path: &Path) -> Result<libc::stat, ContainmentError> {
        let mut stat = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `stat` points to writable storage and `file` is valid.
        let result = unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) };
        if result != 0 {
            return Err(ContainmentError::io(
                "inspect-directory-handle",
                path,
                std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: successful `fstat` initialized the structure.
        Ok(unsafe { stat.assume_init() })
    }

    fn c_string(name: &OsStr, path: &Path) -> Result<CString, ContainmentError> {
        CString::new(name.as_bytes()).map_err(|_| {
            ContainmentError::security(
                "nul_path_component",
                path,
                "output path component contains a NUL byte",
            )
        })
    }

    fn component_error(path: &Path, error: std::io::Error) -> ContainmentError {
        if matches!(error.raw_os_error(), Some(code) if code == libc::ELOOP || code == libc::ENOTDIR)
        {
            ContainmentError::security(
                "linked_or_replaced_ancestor",
                path,
                "an output ancestor is a link, is not a directory, or was replaced",
            )
        } else {
            ContainmentError::io("open-destination-ancestor", path, error)
        }
    }

    fn leaf_error(path: &Path, error: std::io::Error) -> ContainmentError {
        if error.raw_os_error() == Some(libc::ELOOP) {
            ContainmentError::security("linked_output_leaf", path, "output leaf is a symbolic link")
        } else {
            ContainmentError::io("open-contained-leaf", path, error)
        }
    }

    fn profile_name(profile: FilesystemProfile) -> &'static str {
        match profile {
            FilesystemProfile::Detected => "detected-filesystem",
            FilesystemProfile::Local => "local-filesystem",
            FilesystemProfile::LinuxNfsV41LoopbackCi => "linux-nfsv4.1-loopback-ci",
            FilesystemProfile::LinuxSmb311LoopbackCi => "linux-smb3.1.1-loopback-ci",
        }
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::path::{Component, Path, PathBuf};

    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FILE_CREATE, FILE_DIRECTORY_FILE, FILE_DISPOSITION_INFORMATION, FILE_NON_DIRECTORY_FILE,
        FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_OVERWRITE_IF, FILE_RENAME_INFORMATION,
        FILE_SYNCHRONOUS_IO_NONALERT, FileDispositionInformation, FileRenameInformation,
        NtCreateFile, NtSetInformationFile,
    };
    use windows_sys::Win32::Foundation::{
        ERROR_INVALID_NAME, HANDLE, INVALID_HANDLE_VALUE, OBJ_CASE_INSENSITIVE,
        RtlNtStatusToDosError, STATUS_SUCCESS, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileAttributeTagInfo, FlushFileBuffers,
        GetFileInformationByHandleEx, GetFinalPathNameByHandleW, GetVolumeInformationByHandleW,
        OPEN_EXISTING,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    use super::{ContainmentError, FilesystemProfile, OpenDisposition, PromotionDisposition};

    #[derive(Debug)]
    pub(super) struct DirectoryAnchor {
        handle: OwnedHandle,
        canonical: PathBuf,
        volume_serial: u32,
    }

    impl DirectoryAnchor {
        pub(super) fn open(path: &Path) -> Result<Self, ContainmentError> {
            let mut current_path = PathBuf::new();
            let mut current = None;
            let component_count = path.components().count();
            for (index, component) in path.components().enumerate() {
                let access = if index + 1 == component_count {
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE
                } else {
                    FILE_GENERIC_READ
                };
                match component {
                    Component::Prefix(prefix) => current_path.push(prefix.as_os_str()),
                    Component::RootDir => {
                        current_path.push(Path::new(r"\"));
                        current = Some(open_directory(&current_path, access)?);
                    }
                    Component::Normal(name) => {
                        current_path.push(name);
                        let parent = current.as_ref().ok_or_else(|| {
                            ContainmentError::security(
                                "destination_parent_missing",
                                path,
                                "destination must have an absolute directory root",
                            )
                        })?;
                        current = Some(open_directory_at(parent, name, &current_path, access)?);
                    }
                    _ => {
                        return Err(ContainmentError::security(
                            "non_normal_ancestor",
                            path,
                            "output ancestors must be absolute normal path components",
                        ));
                    }
                }
            }
            let handle = current.ok_or_else(|| {
                ContainmentError::security(
                    "destination_parent_missing",
                    path,
                    "destination must have an existing parent directory",
                )
            })?;
            let canonical = final_path(&handle, path)?;
            let volume_serial = volume_serial(&handle, path)?;
            Ok(Self {
                handle,
                canonical,
                volume_serial,
            })
        }

        pub(super) fn verify_profile(
            &self,
            profile: FilesystemProfile,
            _path: &Path,
        ) -> Result<(), ContainmentError> {
            let remote = self
                .canonical
                .to_string_lossy()
                .to_ascii_lowercase()
                .starts_with(r"\\?\unc\");
            if profile == FilesystemProfile::Detected
                || (profile == FilesystemProfile::Local && !remote)
            {
                Ok(())
            } else {
                Err(ContainmentError::PolicyRequired {
                    profile: profile_name(profile).to_owned(),
                    detail: "Windows remote shares and Linux loopback profiles are not qualified by this profile",
                })
            }
        }

        pub(super) fn open_leaf(
            &self,
            leaf: &OsStr,
            disposition: OpenDisposition,
            display_path: &Path,
        ) -> Result<File, ContainmentError> {
            let creation = match disposition {
                OpenDisposition::CreateNew => FILE_CREATE,
                OpenDisposition::Truncate => FILE_OVERWRITE_IF,
            };
            let handle = open_file_at(
                &self.handle,
                leaf,
                FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
                creation,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            )
            .map_err(|source| ContainmentError::io("open-contained-leaf", display_path, source))?;
            reject_reparse(&handle, display_path)?;
            Ok(File::from(handle))
        }

        pub(super) fn leaf_exists(
            &self,
            leaf: &OsStr,
            display_path: &Path,
        ) -> Result<bool, ContainmentError> {
            match open_file_at(
                &self.handle,
                leaf,
                FILE_GENERIC_READ,
                FILE_OPEN,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            ) {
                Ok(handle) => {
                    reject_reparse(&handle, display_path)?;
                    Ok(true)
                }
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(source) => Err(ContainmentError::io(
                    "inspect-contained-leaf",
                    display_path,
                    source,
                )),
            }
        }

        pub(super) fn remove_leaf(&self, leaf: &OsStr) -> Result<(), ContainmentError> {
            let handle = open_file_at(
                &self.handle,
                leaf,
                DELETE,
                FILE_OPEN,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            )
            .map_err(|source| {
                ContainmentError::io("open-contained-leaf-for-removal", Path::new(leaf), source)
            })?;
            let disposition = FILE_DISPOSITION_INFORMATION { DeleteFile: true };
            let mut status_block = unsafe { std::mem::zeroed::<IO_STATUS_BLOCK>() };
            // SAFETY: the handle has DELETE access and all pointers refer to
            // initialized storage for this synchronous native call.
            let status = unsafe {
                NtSetInformationFile(
                    handle.as_raw_handle() as HANDLE,
                    &mut status_block,
                    (&disposition as *const FILE_DISPOSITION_INFORMATION).cast(),
                    size_of::<FILE_DISPOSITION_INFORMATION>() as u32,
                    FileDispositionInformation,
                )
            };
            if status == STATUS_SUCCESS {
                Ok(())
            } else {
                Err(ContainmentError::io(
                    "remove-contained-leaf",
                    Path::new(leaf),
                    std::io::Error::from_raw_os_error(unsafe {
                        RtlNtStatusToDosError(status) as i32
                    }),
                ))
            }
        }

        #[allow(clippy::too_many_arguments)]
        pub(super) fn promote(
            &self,
            source_parent: &Self,
            source_leaf: &OsStr,
            destination_leaf: &OsStr,
            disposition: PromotionDisposition,
            source_path: &Path,
            destination_path: &Path,
            fail_after_rename: bool,
        ) -> Result<(), ContainmentError> {
            let source_handle = open_file_at(
                &source_parent.handle,
                source_leaf,
                FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
                FILE_OPEN,
                FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            )
            .map_err(|source| ContainmentError::io("open-promotion-source", source_path, source))?;
            reject_reparse(&source_handle, source_path)?;
            let source_volume = volume_serial(&source_handle, source_path)?;
            if source_volume != self.volume_serial
                || source_parent.volume_serial != self.volume_serial
            {
                return Err(ContainmentError::security(
                    "cross_filesystem_promotion",
                    destination_path,
                    "atomic publication requires source quarantine and destination on one filesystem",
                ));
            }
            File::from(source_handle.try_clone().map_err(|source| {
                ContainmentError::io("clone-promotion-source", source_path, source)
            })?)
            .sync_all()
            .map_err(|source| ContainmentError::io("sync-promotion-source", source_path, source))?;

            let destination_wide: Vec<u16> = destination_leaf.encode_wide().collect();
            // The native contract requires the full fixed structure size plus
            // the complete FileName byte length. This is intentionally not an
            // offset calculation: the structure has trailing alignment on
            // x64, and omitting it yields STATUS_INVALID_PARAMETER.
            let bytes =
                size_of::<FILE_RENAME_INFORMATION>() + destination_wide.len() * size_of::<u16>();
            let mut storage = vec![0_u64; bytes.div_ceil(size_of::<u64>())];
            let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
            // SAFETY: `storage` is aligned for `FILE_RENAME_INFORMATION`, sized for
            // its full fixed structure plus the complete UTF-16 leaf, and
            // remains alive for the duration of the OS call.
            unsafe {
                (*info).Anonymous.ReplaceIfExists = disposition == PromotionDisposition::Replace;
                (*info).RootDirectory = self.handle.as_raw_handle() as HANDLE;
                (*info).FileNameLength = (destination_wide.len() * size_of::<u16>()) as u32;
                std::ptr::copy_nonoverlapping(
                    destination_wide.as_ptr(),
                    (*info).FileName.as_mut_ptr(),
                    destination_wide.len(),
                );
            }
            let mut status_block = unsafe { std::mem::zeroed::<IO_STATUS_BLOCK>() };
            // SAFETY: the source handle carries DELETE access, `info`
            // describes a destination leaf relative to the retained parent,
            // and every pointer remains valid for the synchronous call.
            let status = unsafe {
                NtSetInformationFile(
                    source_handle.as_raw_handle() as HANDLE,
                    &mut status_block,
                    info.cast_const().cast(),
                    bytes as u32,
                    FileRenameInformation,
                )
            };
            if status != STATUS_SUCCESS {
                return Err(ContainmentError::io(
                    "atomic-promotion",
                    destination_path,
                    std::io::Error::from_raw_os_error(unsafe {
                        RtlNtStatusToDosError(status) as i32
                    }),
                ));
            }
            if fail_after_rename {
                return Err(ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source: std::io::Error::other("injected post-rename directory sync failure"),
                });
            }
            // SAFETY: the retained parent handle is valid. A failure is a
            // durability failure and must block a positive result.
            let flushed = unsafe { FlushFileBuffers(self.handle.as_raw_handle() as HANDLE) };
            if flushed == 0 {
                return Err(ContainmentError::VisibleButUnsynced {
                    path: destination_path.to_path_buf(),
                    source: std::io::Error::last_os_error(),
                });
            }
            Ok(())
        }
    }

    fn open_directory(path: &Path, access: u32) -> Result<OwnedHandle, ContainmentError> {
        let handle = open_file(
            path,
            access,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        )
        .map_err(|source| ContainmentError::io("open-destination-ancestor", path, source))?;
        let attributes = attributes(&handle, path)?;
        if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(ContainmentError::security(
                "linked_or_replaced_ancestor",
                path,
                "an output ancestor is a reparse point or was replaced",
            ));
        }
        if attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
            return Err(ContainmentError::security(
                "non_directory_ancestor",
                path,
                "output ancestor is not a directory",
            ));
        }
        Ok(handle)
    }

    fn open_directory_at(
        parent: &OwnedHandle,
        leaf: &OsStr,
        path: &Path,
        access: u32,
    ) -> Result<OwnedHandle, ContainmentError> {
        let handle = open_file_at(
            parent,
            leaf,
            access,
            FILE_OPEN,
            FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        )
        .map_err(|source| ContainmentError::io("open-destination-ancestor", path, source))?;
        let attributes = attributes(&handle, path)?;
        if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(ContainmentError::security(
                "linked_or_replaced_ancestor",
                path,
                "an output ancestor is a reparse point or was replaced",
            ));
        }
        if attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
            return Err(ContainmentError::security(
                "non_directory_ancestor",
                path,
                "output ancestor is not a directory",
            ));
        }
        Ok(handle)
    }

    fn open_file_at(
        parent: &OwnedHandle,
        leaf: &OsStr,
        access: u32,
        creation: u32,
        options: u32,
    ) -> std::io::Result<OwnedHandle> {
        let wide: Vec<u16> = leaf.encode_wide().collect();
        let byte_length: u16 = (wide.len() * size_of::<u16>())
            .try_into()
            .map_err(|_| std::io::Error::from_raw_os_error(ERROR_INVALID_NAME as i32))?;
        let mut name = UNICODE_STRING {
            Length: byte_length,
            MaximumLength: byte_length,
            Buffer: wide.as_ptr().cast_mut(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_raw_handle() as HANDLE,
            ObjectName: &mut name,
            Attributes: OBJ_CASE_INSENSITIVE,
            SecurityDescriptor: std::ptr::null_mut(),
            SecurityQualityOfService: std::ptr::null_mut(),
        };
        let mut status_block = unsafe { std::mem::zeroed::<IO_STATUS_BLOCK>() };
        let mut handle = INVALID_HANDLE_VALUE;
        // SAFETY: every pointer references initialized storage for the call,
        // `parent` remains open, and `wide` is a single relative component.
        let status = unsafe {
            NtCreateFile(
                &mut handle,
                access,
                &attributes,
                &mut status_block,
                std::ptr::null(),
                FILE_ATTRIBUTE_NORMAL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                creation,
                options,
                std::ptr::null(),
                0,
            )
        };
        if status != STATUS_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(unsafe {
                RtlNtStatusToDosError(status) as i32
            }));
        }
        // SAFETY: successful `NtCreateFile` returned a fresh owned handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) })
    }

    fn open_file(
        path: &Path,
        access: u32,
        creation: u32,
        flags: u32,
    ) -> std::io::Result<OwnedHandle> {
        let path = wide_path(path);
        // SAFETY: `path` is NUL-terminated, optional security/template
        // pointers are null, and a successful raw handle is owned below.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                creation,
                flags,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `CreateFileW` returned a fresh owned handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) })
    }

    fn reject_reparse(handle: &OwnedHandle, path: &Path) -> Result<(), ContainmentError> {
        if attributes(handle, path)?.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            Err(ContainmentError::security(
                "linked_output_leaf",
                path,
                "output leaf is a reparse point",
            ))
        } else {
            Ok(())
        }
    }

    fn attributes(
        handle: &OwnedHandle,
        path: &Path,
    ) -> Result<FILE_ATTRIBUTE_TAG_INFO, ContainmentError> {
        let mut info = MaybeUninit::<FILE_ATTRIBUTE_TAG_INFO>::uninit();
        // SAFETY: `info` is writable and sized for the requested information
        // class; the handle remains valid for the call.
        let result = unsafe {
            GetFileInformationByHandleEx(
                handle.as_raw_handle() as HANDLE,
                FileAttributeTagInfo,
                info.as_mut_ptr().cast(),
                size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
            )
        };
        if result == 0 {
            return Err(ContainmentError::io(
                "inspect-file-handle",
                path,
                std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: successful `GetFileInformationByHandleEx` initialized info.
        Ok(unsafe { info.assume_init() })
    }

    fn final_path(handle: &OwnedHandle, path: &Path) -> Result<PathBuf, ContainmentError> {
        let mut buffer = vec![0_u16; 32_768];
        // SAFETY: `buffer` is writable for its declared length and the handle
        // remains valid for the call.
        let length = unsafe {
            GetFinalPathNameByHandleW(
                handle.as_raw_handle() as HANDLE,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                0,
            )
        } as usize;
        if length == 0 || length >= buffer.len() {
            return Err(ContainmentError::io(
                "resolve-directory-handle",
                path,
                std::io::Error::last_os_error(),
            ));
        }
        buffer.truncate(length);
        Ok(PathBuf::from(String::from_utf16_lossy(&buffer)))
    }

    fn volume_serial(handle: &OwnedHandle, path: &Path) -> Result<u32, ContainmentError> {
        let mut serial = 0_u32;
        // SAFETY: all optional output buffers are null and `serial` points to
        // valid writable storage for the retained handle's volume identifier.
        let result = unsafe {
            GetVolumeInformationByHandleW(
                handle.as_raw_handle() as HANDLE,
                std::ptr::null_mut(),
                0,
                &mut serial,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if result == 0 {
            return Err(ContainmentError::io(
                "inspect-filesystem-volume",
                path,
                std::io::Error::last_os_error(),
            ));
        }
        Ok(serial)
    }

    fn wide_path(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    fn profile_name(profile: FilesystemProfile) -> &'static str {
        match profile {
            FilesystemProfile::Detected => "detected-filesystem",
            FilesystemProfile::Local => "local-filesystem",
            FilesystemProfile::LinuxNfsV41LoopbackCi => "linux-nfsv4.1-loopback-ci",
            FilesystemProfile::LinuxSmb311LoopbackCi => "linux-smb3.1.1-loopback-ci",
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::path::{Path, PathBuf};

    use super::{ContainmentError, FilesystemProfile, OpenDisposition, PromotionDisposition};

    #[derive(Debug)]
    pub(super) struct DirectoryAnchor {
        _path: PathBuf,
    }

    impl DirectoryAnchor {
        pub(super) fn open(path: &Path) -> Result<Self, ContainmentError> {
            Ok(Self {
                _path: path.to_path_buf(),
            })
        }

        pub(super) fn verify_profile(
            &self,
            profile: FilesystemProfile,
            _path: &Path,
        ) -> Result<(), ContainmentError> {
            Err(ContainmentError::PolicyRequired {
                profile: profile_name(profile).to_owned(),
                detail: "this platform has no compiled handle-relative containment implementation",
            })
        }

        pub(super) fn open_leaf(
            &self,
            _leaf: &OsStr,
            _disposition: OpenDisposition,
            _display_path: &Path,
        ) -> Result<File, ContainmentError> {
            Err(ContainmentError::PolicyRequired {
                profile: "local-filesystem".to_owned(),
                detail: "output creation is unavailable without a handle-relative containment implementation",
            })
        }

        pub(super) fn leaf_exists(
            &self,
            _leaf: &OsStr,
            _display_path: &Path,
        ) -> Result<bool, ContainmentError> {
            Err(ContainmentError::PolicyRequired {
                profile: "local-filesystem".to_owned(),
                detail: "output inspection is unavailable without a handle-relative containment implementation",
            })
        }

        pub(super) fn remove_leaf(&self, _leaf: &OsStr) -> Result<(), ContainmentError> {
            Err(ContainmentError::PolicyRequired {
                profile: "local-filesystem".to_owned(),
                detail: "output cleanup is unavailable without a handle-relative containment implementation",
            })
        }

        #[allow(clippy::too_many_arguments)]
        pub(super) fn promote(
            &self,
            _source_parent: &Self,
            _source_leaf: &OsStr,
            _destination_leaf: &OsStr,
            _disposition: PromotionDisposition,
            _source_path: &Path,
            _destination_path: &Path,
            _fail_after_rename: bool,
        ) -> Result<(), ContainmentError> {
            Err(ContainmentError::PolicyRequired {
                profile: "local-filesystem".to_owned(),
                detail: "output promotion is unavailable without a handle-relative containment implementation",
            })
        }
    }

    fn profile_name(profile: FilesystemProfile) -> &'static str {
        match profile {
            FilesystemProfile::Detected => "detected-filesystem",
            FilesystemProfile::Local => "local-filesystem",
            FilesystemProfile::LinuxNfsV41LoopbackCi => "linux-nfsv4.1-loopback-ci",
            FilesystemProfile::LinuxSmb311LoopbackCi => "linux-smb3.1.1-loopback-ci",
        }
    }
}
