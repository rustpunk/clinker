//! Cross-platform RSS tracking and memory budget for spill decisions.
//!
//! `rss_bytes()` returns the current process RSS via platform-native APIs:
//! - Linux: `/proc/self/statm` (resident pages × page size)
//! - macOS: `mach_task_basic_info` via `mach2` (future — todo for non-Linux)
//! - Windows: `K32GetProcessMemoryInfo` via `windows-sys` (future — todo for non-Linux)
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
    // mach2::mach_task_basic_info — deferred until macOS CI is available.
    // For now, return None (falls back to allocation-counting heuristic).
    None
}

#[cfg(target_os = "windows")]
fn rss_bytes_impl() -> Option<u64> {
    // windows_sys::Win32::System::ProcessStatus::K32GetProcessMemoryInfo
    // Deferred until Windows CI is available.
    None
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
pub struct MemoryBudget {
    /// Total memory limit in bytes. Default: 512MB.
    pub limit: u64,
    /// Fraction of limit at which spill triggers. Default: 0.60.
    pub spill_threshold_pct: f64,
}

impl MemoryBudget {
    pub fn new(limit: u64, spill_threshold_pct: f64) -> Self {
        Self {
            limit,
            spill_threshold_pct,
        }
    }

    /// Build from config memory_limit string ("512M", "2G", raw bytes).
    pub fn from_config(memory_limit: Option<&str>) -> Self {
        let limit = parse_memory_limit_bytes(memory_limit);
        Self::new(limit, 0.60)
    }

    /// Returns true when current RSS exceeds the spill threshold.
    /// Returns false if RSS cannot be measured (unsupported platform).
    pub fn should_spill(&self) -> bool {
        rss_bytes().map_or(false, |rss| {
            rss > (self.limit as f64 * self.spill_threshold_pct) as u64
        })
    }

    /// Spill threshold in absolute bytes.
    pub fn spill_threshold_bytes(&self) -> u64 {
        (self.limit as f64 * self.spill_threshold_pct) as u64
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
        let budget = MemoryBudget::new(512 * 1024 * 1024, 0.60);
        // Test process RSS should be well under 307MB (60% of 512MB)
        if rss_bytes().is_some() {
            assert!(!budget.should_spill());
        }
    }

    #[test]
    fn test_memory_budget_above_threshold() {
        // Budget of 1MB — any running process exceeds this
        let budget = MemoryBudget::new(1024 * 1024, 0.60);
        if rss_bytes().is_some() {
            assert!(budget.should_spill());
        }
    }

    #[test]
    fn test_memory_budget_default_values() {
        let budget = MemoryBudget::from_config(None);
        assert_eq!(budget.limit, 512 * 1024 * 1024);
        assert!((budget.spill_threshold_pct - 0.60).abs() < f64::EPSILON);
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
