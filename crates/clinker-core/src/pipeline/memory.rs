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

/// Discriminant tag carried on every `MemoryBudget` charge so a budget
/// overflow can name which surface tripped the hard limit. Sources are
/// summed against the single `limit` counter; the tag is for
/// diagnostics and downstream routing only.
///
/// Append-only. Removing a variant is a breaking change for any
/// `MemoryBudgetExceeded` consumer that destructures `source`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BudgetCategory {
    /// Source-rooted Phase-0 arena, node-rooted arenas, deferred-region
    /// admission buffers, grace-hash build/probe accounting, and the
    /// disk-spill quota counter. Every budget-tracked allocation that
    /// is not `ctx.node_buffers` falls under this tag.
    Arena,
    /// `ctx.node_buffers` — the inter-stage handoff layer between
    /// non-fused operators. Not wired to the budget today; reserved
    /// for the upcoming sub-issues that charge node-buffer mutations.
    NodeBuffer,
}

impl std::fmt::Display for BudgetCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arena => f.write_str("arena"),
            Self::NodeBuffer => f.write_str("node_buffer"),
        }
    }
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
    cumulative_spill_bytes: u64,
    /// Cumulative bytes charged across every Arena build site sharing this
    /// budget. Source arena + N node-rooted arenas share one cumulative
    /// counter so a pipeline declaring a 512MB limit cannot multiply that
    /// limit across operators.
    arena_bytes_charged: u64,
    /// Cumulative bytes alive in the inter-stage handoff layer between
    /// non-fused DAG operators. Counted toward the same `limit` envelope
    /// as `arena_bytes_charged` via `total_charged()` — the declared
    /// memory limit is a pipeline-wide ceiling, not per-surface.
    node_buffer_bytes_charged: u64,
}

impl MemoryBudget {
    pub fn new(limit: u64, spill_threshold_pct: f64) -> Self {
        Self {
            limit,
            spill_threshold_pct,
            peak_rss: None,
            max_spill_bytes: u64::MAX,
            cumulative_spill_bytes: 0,
            arena_bytes_charged: 0,
            node_buffer_bytes_charged: 0,
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

    /// Add `n` bytes to the cumulative arena-byte counter. Returns
    /// `true` when the running total has exceeded the hard memory
    /// limit — the build site's signal to surface E310 instead of
    /// admitting the next record. Saturating-add ensures an
    /// overflowing accumulation cannot wrap below the limit and
    /// silently disable the gate.
    ///
    /// One shared counter across the source-rooted Phase-0 arena and
    /// every node-rooted arena built at an upstream operator's
    /// dispatch-arm exit: the pipeline's declared memory limit is a
    /// pipeline-wide envelope, not a per-arena one.
    pub fn charge_arena_bytes(&mut self, n: u64) -> bool {
        self.arena_bytes_charged = self.arena_bytes_charged.saturating_add(n);
        self.arena_bytes_charged > self.limit
    }

    /// Cumulative arena bytes charged so far across every Arena build
    /// site polling this budget.
    pub fn arena_bytes_charged(&self) -> u64 {
        self.arena_bytes_charged
    }

    /// Add `n` bytes to the node-buffer counter when a producer hands
    /// a buffer to a `ctx.node_buffers` slot. Returns `true` when the
    /// running total of (arena + node_buffer) has exceeded `limit` —
    /// the executor's signal to surface
    /// `MemoryBudgetExceeded { source: BudgetCategory::NodeBuffer, .. }`
    /// instead of admitting the next stage's output. Saturating-add
    /// keeps an overflowing accumulation from wrapping below the limit
    /// and silently disabling the gate.
    pub fn charge_node_buffer_bytes(&mut self, n: u64) -> bool {
        self.node_buffer_bytes_charged = self.node_buffer_bytes_charged.saturating_add(n);
        self.total_charged() > self.limit
    }

    /// Subtract `n` bytes from the node-buffer counter when a consumer
    /// drains the corresponding `ctx.node_buffers` slot. Saturating-sub
    /// guards against over-discharge from estimate drift between the
    /// producer's charge and the consumer's discharge.
    pub fn discharge_node_buffer_bytes(&mut self, n: u64) {
        self.node_buffer_bytes_charged = self.node_buffer_bytes_charged.saturating_sub(n);
    }

    /// Cumulative node-buffer bytes currently alive across every
    /// `ctx.node_buffers` slot polling this budget.
    pub fn node_buffer_bytes_charged(&self) -> u64 {
        self.node_buffer_bytes_charged
    }

    /// Sum of every accounted live-byte counter. Compared against
    /// `limit` for the hard-fail counter-based gate, complementary to
    /// the RSS-based `should_abort()`. `cumulative_spill_bytes` is
    /// *not* included — disk spill is governed by its own
    /// `max_spill_bytes` quota, not the in-memory `limit` envelope.
    /// Saturating-add prevents two near-`u64::MAX` counters from
    /// wrapping the sum and silently passing the gate.
    pub fn total_charged(&self) -> u64 {
        self.arena_bytes_charged
            .saturating_add(self.node_buffer_bytes_charged)
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
    fn test_budget_category_display_shape() {
        assert_eq!(BudgetCategory::Arena.to_string(), "arena");
        assert_eq!(BudgetCategory::NodeBuffer.to_string(), "node_buffer");
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

    #[test]
    fn test_charge_node_buffer_bytes_increases_counter() {
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        assert!(!budget.charge_node_buffer_bytes(256));
        assert_eq!(budget.node_buffer_bytes_charged(), 256);
        assert!(!budget.charge_node_buffer_bytes(512));
        assert_eq!(budget.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_node_buffer_bytes_decreases_counter() {
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        budget.charge_node_buffer_bytes(1024);
        budget.discharge_node_buffer_bytes(256);
        assert_eq!(budget.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_saturates_on_over_discharge() {
        // Estimate drift between producer's charge and consumer's
        // discharge must not wrap the counter to a huge positive
        // value and silently disable the gate downstream.
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        budget.charge_node_buffer_bytes(100);
        budget.discharge_node_buffer_bytes(200);
        assert_eq!(budget.node_buffer_bytes_charged(), 0);
    }

    #[test]
    fn test_charge_node_buffer_bytes_under_limit_returns_false() {
        let mut budget = MemoryBudget::new(1024, 0.80);
        assert!(!budget.charge_node_buffer_bytes(256));
    }

    #[test]
    fn test_charge_node_buffer_bytes_at_limit_returns_false() {
        // Strict `>` semantics: exactly at the limit is NOT an
        // overflow, matching `record_spill_bytes` and
        // `charge_arena_bytes`. Only the byte that crosses trips.
        let mut budget = MemoryBudget::new(1024, 0.80);
        assert!(!budget.charge_node_buffer_bytes(1024));
        assert_eq!(budget.node_buffer_bytes_charged(), 1024);
    }

    #[test]
    fn test_charge_node_buffer_bytes_over_limit_returns_true() {
        let mut budget = MemoryBudget::new(1024, 0.80);
        assert!(!budget.charge_node_buffer_bytes(1024));
        assert!(budget.charge_node_buffer_bytes(1));
        assert_eq!(budget.node_buffer_bytes_charged(), 1025);
    }

    #[test]
    fn test_charge_node_buffer_bytes_trips_via_arena_contribution() {
        // Load-bearing test: the new counter shares the `limit`
        // envelope with the arena counter. Neither one alone exceeds
        // the limit, but their sum does — and the node-buffer charge
        // must surface that.
        let mut budget = MemoryBudget::new(1024, 0.80);
        assert!(!budget.charge_arena_bytes(800));
        assert!(!budget.charge_node_buffer_bytes(100));
        assert!(budget.charge_node_buffer_bytes(200));
        assert_eq!(budget.total_charged(), 1100);
    }

    #[test]
    fn test_charge_node_buffer_bytes_saturates_on_overflow() {
        // Saturating-add on a u64 counter: even an absurdly large
        // accumulation cannot wrap below the configured limit and
        // silently disable the gate.
        let mut budget = MemoryBudget::new(1024, 0.80);
        budget.node_buffer_bytes_charged = u64::MAX - 10;
        assert!(budget.charge_node_buffer_bytes(100));
        assert_eq!(budget.node_buffer_bytes_charged(), u64::MAX);
    }

    #[test]
    fn test_total_charged_sums_arena_and_node_buffer() {
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        budget.charge_arena_bytes(400);
        budget.charge_node_buffer_bytes(600);
        assert_eq!(budget.total_charged(), 1000);
    }

    #[test]
    fn test_total_charged_excludes_cumulative_spill_bytes() {
        // Disk spill is governed by `max_spill_bytes`, not the
        // in-memory `limit` envelope. Including spill bytes in
        // `total_charged()` would double-count the spilled work
        // against the RAM budget and force premature aborts.
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        budget.cumulative_spill_bytes = 9999;
        budget.charge_arena_bytes(100);
        assert_eq!(budget.total_charged(), 100);
    }

    #[test]
    fn test_total_charged_saturates_on_overflow() {
        // Two near-`u64::MAX` counters must not wrap the sum and
        // silently pass the `total_charged() > limit` gate.
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        budget.arena_bytes_charged = u64::MAX - 10;
        budget.node_buffer_bytes_charged = 1000;
        assert_eq!(budget.total_charged(), u64::MAX);
    }
}
