//! Cross-platform RSS tracking and memory budget for spill decisions.
//!
//! `rss_bytes()` returns the current process RSS via platform-native APIs:
//! - Linux: `/proc/self/statm` (resident pages × page size)
//! - macOS: `mach_task_basic_info` via `mach2` (pure Rust FFI)
//! - Windows: `K32GetProcessMemoryInfo` via `windows-sys` (pure Rust FFI)
//! - Unsupported: returns `None`
//!
//! `MemoryBudget` governs spill decisions: `should_spill()` returns true when
//! RSS exceeds `limit * spill_threshold_pct`. Polled once per chunk at the
//! start of Phase 2 processing.

/// Cross-platform RSS measurement. Returns `None` on unsupported platforms.
pub fn rss_bytes() -> Option<u64> {
    rss_bytes_impl()
}

#[cfg(target_os = "linux")]
fn rss_bytes_impl() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    Some(resident_pages * page_size)
}

#[cfg(target_os = "macos")]
fn rss_bytes_impl() -> Option<u64> {
    use mach2::kern_return::KERN_SUCCESS;
    use mach2::message::mach_msg_type_number_t;
    use mach2::task::task_info;
    use mach2::task_info::{
        MACH_TASK_BASIC_INFO, MACH_TASK_BASIC_INFO_COUNT, mach_task_basic_info,
    };
    use mach2::traps::mach_task_self;

    // SAFETY: FFI call to mach kernel. `mach_task_self()` returns the current
    // task port (always valid). `task_info` reads process memory stats into
    // the zeroed struct. No aliasing or lifetime concerns — all data is copied.
    unsafe {
        let mut info = mach_task_basic_info::default();
        let mut count: mach_msg_type_number_t = MACH_TASK_BASIC_INFO_COUNT;
        let ret = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info as *mut mach_task_basic_info as *mut _,
            &mut count,
        );
        if ret == KERN_SUCCESS {
            Some(info.resident_size)
        } else {
            None
        }
    }
}

#[cfg(target_os = "windows")]
fn rss_bytes_impl() -> Option<u64> {
    use std::mem;
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // SAFETY: FFI call to Win32 API. `GetCurrentProcess()` returns a
    // pseudo-handle constant (-1) that is always valid and requires no
    // `CloseHandle`. `K32GetProcessMemoryInfo` reads process memory stats
    // into the zeroed struct. `cb` must be set to the struct size before the
    // call for version compatibility.
    unsafe {
        let handle = GetCurrentProcess();
        let mut pmc: PROCESS_MEMORY_COUNTERS = mem::zeroed();
        pmc.cb = mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        let ok = K32GetProcessMemoryInfo(
            handle,
            &mut pmc,
            mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        );
        if ok != 0 {
            Some(pmc.WorkingSetSize as u64)
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn rss_bytes_impl() -> Option<u64> {
    None
}

/// Memory budget governing spill decisions.
///
/// Polled once per chunk at the start of Phase 2 processing.
/// Streaming: RSS is the only signal. Blocking stage spill triggers
/// (sort, distinct) are implemented in Phase 8.
///
/// `max_spill_bytes` adds an optional disk-spill quota distinct from
/// the RSS hard limit: even when memory pressure is fine, an unbounded
/// stream of spill files can still exhaust local disk. Operators that
/// already poll `should_spill()` poll `record_spill_bytes()` after each
/// spill commit; on overflow the operator surfaces E310 with a partition
/// + spill-bytes diagnostic rather than continuing to fill the disk.
pub struct MemoryBudget {
    /// Total memory limit in bytes (the hard limit). Default: 512MB.
    pub limit: u64,
    /// Fraction of limit at which proactive spill triggers (the soft limit).
    /// Default: 0.80. Dual-threshold model: 80% soft / 100% hard / 20% spike
    /// allowance — OTel Memory Limiter consensus.
    pub spill_threshold_pct: f64,
    /// Peak RSS observed across all `observe()` / `should_spill()` calls.
    /// `None` on platforms where RSS measurement is unavailable.
    pub peak_rss: Option<u64>,
    /// Optional disk-spill quota in bytes. `u64::MAX` = unlimited.
    /// Polled by spill operators alongside `should_spill()`. When
    /// cumulative spill bytes exceed this value, the operator emits
    /// E310 with a partition + spill-bytes diagnostic.
    pub max_spill_bytes: u64,
    /// Cumulative bytes written across all spill operators in this
    /// pipeline run. Updated by callers via `record_spill_bytes()`.
    /// Stays at zero until the first spill writer commits a file.
    pub cumulative_spill_bytes: u64,
}

impl MemoryBudget {
    pub fn new(limit: u64, spill_threshold_pct: f64) -> Self {
        Self {
            limit,
            spill_threshold_pct,
            peak_rss: None,
            max_spill_bytes: u64::MAX,
            cumulative_spill_bytes: 0,
        }
    }

    /// Build from config memory_limit string ("512M", "2G", raw bytes).
    ///
    /// Uses the 0.80 default for `spill_threshold_pct` (dual-threshold model:
    /// 80% soft / 100% hard / 20% spike allowance). `max_spill_bytes`
    /// defaults to `u64::MAX` (unlimited disk quota); the surface is
    /// not yet wired to a config field — callers that want a quota
    /// set it directly on the constructed budget.
    pub fn from_config(memory_limit: Option<&str>) -> Self {
        let limit = parse_memory_limit_bytes(memory_limit);
        Self::new(limit, 0.80)
    }

    /// Poll current RSS and update `peak_rss` if it exceeds the recorded peak.
    /// Called at chunk boundaries during Phase 2; also called by `should_spill()`.
    pub fn observe(&mut self) {
        if let Some(rss) = rss_bytes() {
            self.peak_rss = Some(self.peak_rss.map_or(rss, |prev| prev.max(rss)));
        }
    }

    /// Returns true when current RSS exceeds the spill threshold.
    /// Also updates `peak_rss` as a side-effect.
    /// Returns false if RSS cannot be measured (unsupported platform).
    pub fn should_spill(&mut self) -> bool {
        self.observe();
        self.peak_rss
            .is_some_and(|rss| rss > (self.limit as f64 * self.spill_threshold_pct) as u64)
    }

    /// Spill threshold in absolute bytes.
    pub fn spill_threshold_bytes(&self) -> u64 {
        (self.limit as f64 * self.spill_threshold_pct) as u64
    }

    /// Soft limit: point at which proactive spill begins.
    /// Equals limit * spill_threshold_pct (default 0.80).
    pub fn soft_limit(&self) -> u64 {
        (self.limit as f64 * self.spill_threshold_pct) as u64
    }

    /// Hard limit: the absolute budget. RSS above this = abort.
    pub fn hard_limit(&self) -> u64 {
        self.limit
    }

    /// Spike allowance between soft and hard limits (default 20%).
    pub fn spike_allowance(&self) -> u64 {
        self.hard_limit() - self.soft_limit()
    }

    /// True when RSS exceeds hard limit. Check periodically in probe loops
    /// (every 10K output records) to prevent unbounded fan-out.
    pub fn should_abort(&mut self) -> bool {
        self.observe();
        self.peak_rss.is_some_and(|rss| rss > self.limit)
    }

    /// Disk-spill quota in bytes. `u64::MAX` indicates unlimited.
    pub fn disk_quota(&self) -> u64 {
        self.max_spill_bytes
    }

    /// Cumulative spill bytes recorded so far across every spill
    /// operator polling this budget.
    pub fn cumulative_spill_bytes(&self) -> u64 {
        self.cumulative_spill_bytes
    }

    /// Add `n` bytes to the cumulative spill counter. Returns `true`
    /// when the running total has exceeded `max_spill_bytes` — the
    /// operator's signal to surface E310 instead of continuing to
    /// write. Saturating-add keeps an overflowing pipeline from
    /// wrapping back below the quota silently.
    pub fn record_spill_bytes(&mut self, n: u64) -> bool {
        self.cumulative_spill_bytes = self.cumulative_spill_bytes.saturating_add(n);
        self.cumulative_spill_bytes > self.max_spill_bytes
    }
}

/// Parse memory limit string to bytes. Supports "512M", "2G", "512m", "2g",
/// raw integer string. Returns 512MB default if None or unparseable.
pub fn parse_memory_limit_bytes(s: Option<&str>) -> u64 {
    s.and_then(|s| {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('G').or_else(|| s.strip_suffix('g')) {
            num.parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('M').or_else(|| s.strip_suffix('m')) {
            num.parse::<u64>().ok().map(|n| n * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('K').or_else(|| s.strip_suffix('k')) {
            num.parse::<u64>().ok().map(|n| n * 1024)
        } else {
            s.parse::<u64>().ok()
        }
    })
    .unwrap_or(512 * 1024 * 1024) // 512MB default
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rss_bytes_returns_some() {
        let rss = rss_bytes();
        // On Linux (our CI), this should return Some.
        // On unsupported platforms, this test is a no-op.
        if cfg!(target_os = "linux") {
            assert!(rss.is_some(), "rss_bytes() should return Some on Linux");
            assert!(rss.unwrap() > 0, "RSS should be positive");
        }
    }

    #[test]
    fn test_rss_bytes_increases_after_alloc() {
        if rss_bytes().is_none() {
            return; // Skip on unsupported platforms
        }
        let before = rss_bytes().unwrap();
        // Allocate 10MB and touch it to ensure pages are resident
        let big_vec: Vec<u8> = vec![1u8; 10 * 1024 * 1024];
        std::hint::black_box(&big_vec);
        let after = rss_bytes().unwrap();
        // RSS should increase by at least 5MB (accounting for page granularity)
        assert!(
            after >= before + 5 * 1024 * 1024,
            "RSS should increase by at least 5MB after 10MB alloc, got before={before} after={after}"
        );
    }

    #[test]
    fn test_memory_budget_below_threshold() {
        let mut budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        // Test process RSS should be well under 410MB (80% of 512MB)
        if rss_bytes().is_some() {
            assert!(!budget.should_spill());
        }
    }

    #[test]
    fn test_memory_budget_above_threshold() {
        // Budget of 1MB — any running process exceeds this
        let mut budget = MemoryBudget::new(1024 * 1024, 0.80);
        if rss_bytes().is_some() {
            assert!(budget.should_spill());
        }
    }

    #[test]
    fn test_memory_budget_peak_rss_tracked() {
        let mut budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        if rss_bytes().is_none() {
            return; // Skip on unsupported platforms
        }
        budget.observe();
        assert!(
            budget.peak_rss.is_some(),
            "peak_rss should be set after observe()"
        );
        let first_peak = budget.peak_rss.unwrap();
        budget.observe();
        // Peak must be non-decreasing
        assert!(budget.peak_rss.unwrap() >= first_peak);
    }

    #[test]
    fn test_memory_budget_default_values() {
        let budget = MemoryBudget::from_config(None);
        assert_eq!(budget.limit, 512 * 1024 * 1024);
        assert!((budget.spill_threshold_pct - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_quota_default_unlimited() {
        let budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        assert_eq!(budget.disk_quota(), u64::MAX);
        assert_eq!(budget.cumulative_spill_bytes(), 0);
    }

    #[test]
    fn test_record_spill_bytes_under_quota() {
        let mut budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        budget.max_spill_bytes = 1024;
        assert!(!budget.record_spill_bytes(256));
        assert_eq!(budget.cumulative_spill_bytes(), 256);
        assert!(!budget.record_spill_bytes(512));
        assert_eq!(budget.cumulative_spill_bytes(), 768);
    }

    #[test]
    fn test_record_spill_bytes_overflows_quota() {
        let mut budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        budget.max_spill_bytes = 1024;
        assert!(!budget.record_spill_bytes(1024));
        assert!(budget.record_spill_bytes(1));
        assert_eq!(budget.cumulative_spill_bytes(), 1025);
    }

    #[test]
    fn test_record_spill_bytes_saturates_on_overflow() {
        // Saturating-add on a u64 counter: even an absurdly large
        // accumulation cannot wrap below the configured quota and
        // silently disable the gate.
        let mut budget = MemoryBudget::new(512 * 1024 * 1024, 0.80);
        budget.max_spill_bytes = 1024;
        budget.cumulative_spill_bytes = u64::MAX - 10;
        assert!(budget.record_spill_bytes(100));
        assert_eq!(budget.cumulative_spill_bytes(), u64::MAX);
    }

    #[test]
    fn test_memory_limit_cli_parse_suffixes() {
        assert_eq!(parse_memory_limit_bytes(Some("512M")), 536_870_912);
        assert_eq!(parse_memory_limit_bytes(Some("512m")), 536_870_912);
        assert_eq!(parse_memory_limit_bytes(Some("2G")), 2_147_483_648);
        assert_eq!(parse_memory_limit_bytes(Some("2g")), 2_147_483_648);
        assert_eq!(parse_memory_limit_bytes(Some("512K")), 524_288);
        assert_eq!(parse_memory_limit_bytes(Some("1024")), 1024);
        assert_eq!(parse_memory_limit_bytes(None), 512 * 1024 * 1024);
        assert_eq!(parse_memory_limit_bytes(Some("garbage")), 512 * 1024 * 1024);
    }
}
