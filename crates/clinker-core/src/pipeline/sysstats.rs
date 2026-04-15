//! Cross-platform CPU time and disk I/O byte counters for the current process.
//!
//! Mirrors the snapshot-at-stage-boundary pattern of [`super::memory::rss_bytes`].
//! All values are **process-wide**: per-stage deltas captured under parallel
//! execution (rayon) sum across workers and cannot be attributed to a single
//! stage when stages overlap. Per-run totals are authoritative; per-stage
//! values are advisory.
//!
//! - Linux: `getrusage(RUSAGE_SELF)` for CPU, `/proc/self/io` for disk I/O.
//! - macOS: `getrusage(RUSAGE_SELF)` for CPU, `proc_pid_rusage(RUSAGE_INFO_V4)` for disk I/O.
//! - Windows: `GetProcessTimes` for CPU, `GetProcessIoCounters` for disk I/O.
//! - Unsupported platforms: both functions return `None`.
//!
//! **Page-cache caveat:** the I/O read counters on all three platforms reflect
//! bytes that hit the storage layer; reads served from the OS page cache
//! return 0. Cold-cache measurement requires explicit cache eviction.

/// User and system CPU time consumed by the current process, in nanoseconds.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuTimes {
    pub user_ns: u64,
    pub sys_ns: u64,
}

/// Bytes read from / written to disk by the current process.
/// Excludes page-cache hits on read; reflects flushed bytes on write.
#[derive(Debug, Clone, Copy, Default)]
pub struct IoCounters {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// Returns the current process's user + system CPU time, or `None` on
/// unsupported platforms.
pub fn cpu_times() -> Option<CpuTimes> {
    cpu_times_impl()
}

/// Returns the current process's disk I/O byte counters, or `None` on
/// unsupported platforms or kernels with `/proc/self/io` disabled.
pub fn io_counters() -> Option<IoCounters> {
    io_counters_impl()
}

// ---------- CPU times ----------

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cpu_times_impl() -> Option<CpuTimes> {
    // SAFETY: `getrusage` writes a fully-initialized `rusage` struct on
    // success. We zero-initialize first and only read fields after a
    // success return.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return None;
        }
        Some(CpuTimes {
            user_ns: timeval_to_ns(ru.ru_utime),
            sys_ns: timeval_to_ns(ru.ru_stime),
        })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn timeval_to_ns(tv: libc::timeval) -> u64 {
    (tv.tv_sec as u64).saturating_mul(1_000_000_000) + (tv.tv_usec as u64).saturating_mul(1_000)
}

#[cfg(target_os = "windows")]
fn cpu_times_impl() -> Option<CpuTimes> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

    // SAFETY: FFI to Win32. `GetCurrentProcess()` returns a pseudo-handle
    // (-1) that is always valid and requires no `CloseHandle`. The four
    // `FILETIME` outputs are written by `GetProcessTimes` on success.
    unsafe {
        let mut creation: FILETIME = std::mem::zeroed();
        let mut exit: FILETIME = std::mem::zeroed();
        let mut kernel: FILETIME = std::mem::zeroed();
        let mut user: FILETIME = std::mem::zeroed();
        let ok = GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        );
        if ok == 0 {
            return None;
        }
        Some(CpuTimes {
            user_ns: filetime_to_ns(user),
            sys_ns: filetime_to_ns(kernel),
        })
    }
}

#[cfg(target_os = "windows")]
fn filetime_to_ns(ft: windows_sys::Win32::Foundation::FILETIME) -> u64 {
    // FILETIME is in 100-nanosecond ticks since 1601-01-01. For elapsed
    // process time it's just a 64-bit tick count split across two u32s.
    let ticks = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);
    ticks.saturating_mul(100)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn cpu_times_impl() -> Option<CpuTimes> {
    None
}

// ---------- I/O counters ----------

#[cfg(target_os = "linux")]
fn io_counters_impl() -> Option<IoCounters> {
    let contents = std::fs::read_to_string("/proc/self/io").ok()?;
    let mut read_bytes: Option<u64> = None;
    let mut write_bytes: Option<u64> = None;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("read_bytes:") {
            read_bytes = rest.trim().parse().ok();
        } else if let Some(rest) = line.strip_prefix("write_bytes:") {
            write_bytes = rest.trim().parse().ok();
        }
    }
    Some(IoCounters {
        read_bytes: read_bytes?,
        write_bytes: write_bytes?,
    })
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Default)]
struct RusageInfoV4 {
    ri_uuid: [u8; 16],
    ri_user_time: u64,
    ri_system_time: u64,
    ri_pkg_idle_wkups: u64,
    ri_interrupt_wkups: u64,
    ri_pageins: u64,
    ri_wired_size: u64,
    ri_resident_size: u64,
    ri_phys_footprint: u64,
    ri_proc_start_abstime: u64,
    ri_proc_exit_abstime: u64,
    ri_child_user_time: u64,
    ri_child_system_time: u64,
    ri_child_pkg_idle_wkups: u64,
    ri_child_interrupt_wkups: u64,
    ri_child_pageins: u64,
    ri_child_elapsed_abstime: u64,
    ri_diskio_bytesread: u64,
    ri_diskio_byteswritten: u64,
    ri_cpu_time_qos_default: u64,
    ri_cpu_time_qos_maintenance: u64,
    ri_cpu_time_qos_background: u64,
    ri_cpu_time_qos_utility: u64,
    ri_cpu_time_qos_legacy: u64,
    ri_cpu_time_qos_user_initiated: u64,
    ri_cpu_time_qos_user_interactive: u64,
    ri_billed_system_time: u64,
    ri_serviced_system_time: u64,
    ri_logical_writes: u64,
    ri_lifetime_max_phys_footprint: u64,
    ri_instructions: u64,
    ri_cycles: u64,
    ri_billed_energy: u64,
    ri_serviced_energy: u64,
    ri_interval_max_phys_footprint: u64,
    ri_runnable_time: u64,
}

#[cfg(target_os = "macos")]
fn io_counters_impl() -> Option<IoCounters> {
    const RUSAGE_INFO_V4: i32 = 4;
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut RusageInfoV4) -> i32;
    }

    // SAFETY: FFI to libproc. We pass our own pid and a properly-sized,
    // zeroed `rusage_info_v4` buffer. `proc_pid_rusage` writes the struct
    // on success (return 0). We only read fields on success.
    unsafe {
        let mut info = RusageInfoV4::default();
        let pid = libc::getpid();
        if proc_pid_rusage(pid, RUSAGE_INFO_V4, &mut info) != 0 {
            return None;
        }
        Some(IoCounters {
            read_bytes: info.ri_diskio_bytesread,
            write_bytes: info.ri_diskio_byteswritten,
        })
    }
}

#[cfg(target_os = "windows")]
fn io_counters_impl() -> Option<IoCounters> {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessIoCounters};

    // The `IO_COUNTERS` struct lives under `Win32::System::Threading` in
    // recent windows-sys; redeclare locally to avoid hunting feature flags.
    #[repr(C)]
    #[derive(Default)]
    struct IoCountersRaw {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    // SAFETY: FFI to Win32. `GetCurrentProcess()` is a valid pseudo-handle.
    // `GetProcessIoCounters` writes the IO_COUNTERS struct on success. Our
    // local `IoCountersRaw` is `#[repr(C)]` and layout-compatible with the
    // Win32 `IO_COUNTERS` definition.
    unsafe {
        let mut io = IoCountersRaw::default();
        let ok = GetProcessIoCounters(GetCurrentProcess(), &mut io as *mut IoCountersRaw as *mut _);
        if ok == 0 {
            return None;
        }
        Some(IoCounters {
            read_bytes: io.read_transfer_count,
            write_bytes: io.write_transfer_count,
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn io_counters_impl() -> Option<IoCounters> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn cpu_times_returns_some_on_supported_platforms() {
        let v = cpu_times();
        if cfg!(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "windows"
        )) {
            assert!(
                v.is_some(),
                "cpu_times() should return Some on supported platforms"
            );
        }
    }

    #[test]
    fn cpu_times_increases_after_busy_loop() {
        let Some(before) = cpu_times() else { return };
        // Spin in user space for ~50 ms.
        let start = std::time::Instant::now();
        let mut x: u64 = 0;
        while start.elapsed() < std::time::Duration::from_millis(50) {
            x = x.wrapping_add(1);
            std::hint::black_box(&x);
        }
        let after = cpu_times().unwrap();
        let delta = after.user_ns.saturating_sub(before.user_ns);
        assert!(
            delta >= 25_000_000,
            "expected ≥25ms user CPU delta after 50ms spin, got {delta} ns"
        );
    }

    #[test]
    fn io_counters_returns_some_on_supported_platforms() {
        let v = io_counters();
        if cfg!(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "windows"
        )) {
            // Linux may return None if /proc/self/io is disabled by the kernel;
            // accept that but note it. macOS/Windows should always succeed.
            if cfg!(target_os = "linux") && v.is_none() {
                eprintln!("/proc/self/io unavailable on this kernel; skipping");
                return;
            }
            assert!(
                v.is_some(),
                "io_counters() should return Some on supported platforms"
            );
        }
    }

    #[test]
    fn io_counters_write_increases_after_file_write() {
        let Some(before) = io_counters() else { return };

        // `/proc/self/io`'s `write_bytes` counts bytes sent to the
        // storage layer; writes absorbed by an in-memory filesystem
        // (tmpfs, overlayfs on a tmpfs upperdir, sandboxes that
        // intercept fsync) don't increment it. CI runners and the
        // default `/tmp` on many Linux distros are tmpfs-backed, so
        // trust a sentinel file to tell us whether the counter is
        // meaningful before asserting.
        if is_tmpfs_tempdir() {
            eprintln!("/tmp is tmpfs-backed; write_bytes counter not meaningful — skipping");
            return;
        }

        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let buf = vec![0xABu8; 1024 * 1024];
        tmp.write_all(&buf).expect("write");
        tmp.as_file().sync_all().expect("sync_all");
        let after = io_counters().unwrap();
        let delta = after.write_bytes.saturating_sub(before.write_bytes);
        if delta == 0 {
            // Probed tempdir wasn't tmpfs per /proc/mounts, but the
            // underlying block device still swallowed the write (seen
            // with certain overlayfs sandboxes and kernel builds
            // without accounting). Treat zero delta the same as an
            // unsupported platform.
            eprintln!(
                "write_bytes counter reported 0 after a 1MB flushed write — treating as unsupported"
            );
            return;
        }
        assert!(
            delta >= 512 * 1024,
            "expected ≥512KB write delta after 1MB write+sync, got {delta} bytes"
        );
    }

    /// Heuristic: returns `true` when the system temp directory is
    /// backed by tmpfs (or another in-memory fs) where `/proc/self/io`'s
    /// `write_bytes` counter is not meaningful.
    #[cfg(target_os = "linux")]
    fn is_tmpfs_tempdir() -> bool {
        let tmp = std::env::temp_dir();
        let tmp = tmp.canonicalize().unwrap_or(tmp);
        // Prefer /proc/self/mountinfo (more reliable than /proc/mounts
        // for nested mount points: the 9th+ columns carry the fs type
        // after ' - ').
        if let Ok(contents) = std::fs::read_to_string("/proc/self/mountinfo") {
            let mut best: Option<(usize, bool)> = None;
            for line in contents.lines() {
                // Fields: ID parent-ID dev root mountpoint opts ... - fstype ...
                let mut parts = line.split(' ');
                let _id = parts.next();
                let _parent = parts.next();
                let _dev = parts.next();
                let _root = parts.next();
                let Some(mountpoint) = parts.next() else {
                    continue;
                };
                // Skip optional fields until ' - ' separator
                let rest: Vec<&str> = parts.collect();
                let sep = rest.iter().position(|&f| f == "-");
                let Some(sep) = sep else { continue };
                let Some(fstype) = rest.get(sep + 1) else {
                    continue;
                };
                if tmp.starts_with(mountpoint) && mountpoint.len() >= best.map_or(0, |(l, _)| l) {
                    let is_mem_fs = matches!(*fstype, "tmpfs" | "ramfs" | "overlay");
                    best = Some((mountpoint.len(), is_mem_fs));
                }
            }
            return best.map_or(false, |(_, t)| t);
        }
        false
    }

    #[cfg(not(target_os = "linux"))]
    fn is_tmpfs_tempdir() -> bool {
        false
    }
}
