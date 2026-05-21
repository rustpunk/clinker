//! Cross-platform RSS tracking and the central memory arbitrator.
//!
//! `rss_bytes()` returns the current process RSS via platform-native APIs:
//! - Linux: `/proc/self/statm` (resident pages × page size)
//! - macOS: `mach_task_basic_info` via `mach2` (pure Rust FFI)
//! - Windows: `K32GetProcessMemoryInfo` via `windows-sys` (pure Rust FFI)
//! - Unsupported: returns `None`
//!
//! `MemoryArbitrator` is the single seat that governs every spill / abort
//! decision in the executor. Each spill-capable operator (Aggregate, sort,
//! grace-hash, sort-merge join, IEJoin, inter-stage `node_buffers`) polls
//! the same arbitrator instance and trusts its `should_spill` /
//! `should_abort` answer. The trait surface (`MemoryConsumer`,
//! `ArbitrationPolicy`) lets later work plug in policies that pick a
//! victim across operators instead of reacting independently. Until a
//! real policy is installed, the arbitrator ships with `NoOpPolicy`,
//! which preserves the pre-existing react-only behavior byte-for-byte.

use std::collections::HashMap;

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

/// Discriminant tag carried on every arbitrator charge so a budget
/// overflow can name which surface tripped the hard limit. Sources are
/// summed against the single `limit` counter; the tag is for
/// diagnostics and downstream routing only.
///
/// Append-only. Removing a variant is a breaking change for any
/// `MemoryBudgetExceeded` consumer that destructures `source`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BudgetCategory {
    /// Source-rooted arenas, node-rooted arenas, deferred-region
    /// admission buffers, grace-hash build/probe accounting, and the
    /// disk-spill quota counter. Every budget-tracked allocation that
    /// is not `ctx.node_buffers` falls under this tag.
    Arena,
    /// `ctx.node_buffers` — the inter-stage handoff layer between
    /// non-fused operators. Charged at producer admission, discharged
    /// at consumer drain; shares the same `limit` envelope as
    /// `BudgetCategory::Arena` through `total_charged()`.
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

/// Stable identifier for a memory consumer registered with the
/// arbitrator. Assigned by `MemoryArbitrator` at registration time and
/// referenced by every subsequent arbitration decision.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerId(u32);

/// Reason a `MemoryConsumer::try_spill` call could not free the
/// requested number of bytes.
///
/// Distinct from `crate::pipeline::spill::SpillError`, which models
/// disk-write and serialization failures. `ConsumerSpillError` is the
/// arbitrator-facing signal that a victim either failed to write or
/// could not free enough state to satisfy the target.
#[derive(Debug)]
pub enum ConsumerSpillError {
    /// Underlying spill medium (disk, OS) returned an I/O error.
    Io(std::io::Error),
    /// Consumer ran spill to completion but freed fewer bytes than the
    /// arbitrator requested — the caller may need to select another
    /// victim or escalate to a hard abort.
    BelowTarget { target: u64, freed: u64 },
}

impl std::fmt::Display for ConsumerSpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "consumer spill I/O failure: {e}"),
            Self::BelowTarget { target, freed } => write!(
                f,
                "consumer freed {freed} bytes; arbitrator requested {target}"
            ),
        }
    }
}

impl std::error::Error for ConsumerSpillError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::BelowTarget { .. } => None,
        }
    }
}

/// Memory-consuming operator that the arbitrator can interrogate and,
/// when a policy elects it, ask to give up memory.
///
/// Implementations live with their operator (Aggregate, sort,
/// grace-hash, sort-merge join, IEJoin, inter-stage buffers). The
/// arbitrator stores them as `Box<dyn MemoryConsumer>` and reads
/// `current_usage` / `spill_priority` / `can_back_pressure` on every
/// arbitration round; `pause` / `resume` / `try_spill` fire only when
/// a policy selects the consumer as a victim.
///
/// `Send + Sync` so the arbitrator can be shared across worker threads
/// without compromising the existing pipeline-context concurrency
/// posture.
pub trait MemoryConsumer: Send + Sync {
    /// Live bytes the consumer currently holds against the arbitrator's
    /// `limit` envelope. Read every arbitration round; must be cheap.
    fn current_usage(&self) -> u64;

    /// Relative spill cost. Lower = spill first. The arbitrator's
    /// policy uses this when comparing victims of similar size.
    /// Convention: `0` for cheap-to-spill consumers
    /// (`ctx.node_buffers`), `10` for grace-hash, `20` for sort,
    /// `30` for Aggregate.
    fn spill_priority(&self) -> i32;

    /// Best-effort attempt to release `target_bytes` of live state to
    /// disk. Returns the actual number of bytes freed, which may be
    /// less than `target_bytes` (the policy then re-selects).
    fn try_spill(&mut self, target_bytes: u64) -> Result<u64, ConsumerSpillError>;

    /// Whether the consumer can be paused at its inbound channel
    /// instead of forced to spill. True for Sources and inter-stage
    /// buffers fronted by a bounded channel; false for blocking
    /// operators that have no upstream to gate.
    fn can_back_pressure(&self) -> bool;

    /// Pause the consumer's inbound channel. No-op for consumers that
    /// return `false` from `can_back_pressure`.
    fn pause(&mut self);

    /// Resume a previously paused consumer.
    fn resume(&mut self);
}

/// Policy that selects which `MemoryConsumer` gives up memory when
/// the arbitrator reports pressure.
///
/// Returning `None` is a vote to take no action (e.g. `NoOpPolicy`,
/// or a real policy that decides current pressure is transient).
pub trait ArbitrationPolicy: Send + Sync {
    /// Pick a victim from the candidates the arbitrator currently
    /// has registered. `pressure_bytes` is the gap between observed
    /// RSS and the soft limit at the moment the arbitrator polled.
    fn select_victim(
        &self,
        consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        pressure_bytes: u64,
    ) -> Option<ConsumerId>;
}

/// Policy that selects no victim under any pressure.
///
/// Default policy on every freshly constructed arbitrator. Preserves
/// pre-arbitrator react-only behavior: every operator continues to
/// poll `should_spill` / `should_abort` and decides for itself.
/// Replaced at runtime once a real `ArbitrationPolicy` is installed.
pub struct NoOpPolicy;

impl ArbitrationPolicy for NoOpPolicy {
    fn select_victim(
        &self,
        _consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        _pressure_bytes: u64,
    ) -> Option<ConsumerId> {
        None
    }
}

/// Central memory arbitrator. Owns the RSS hard / soft limits, the
/// per-category byte counters (arena, node-buffer, disk spill), and
/// the policy + consumer registry used to elect spill victims.
///
/// `should_spill` is the single polling entry used by every spill-
/// capable operator. It updates `peak_rss`, consults the registered
/// `ArbitrationPolicy`, and reports whether the soft threshold has
/// been crossed. `should_abort` mirrors the same check against the
/// hard `limit`.
///
/// `max_spill_bytes` is a disk-spill quota distinct from the RSS
/// envelope: even when RSS is fine, an unbounded stream of spill
/// files can still exhaust local disk. Operators poll
/// `record_spill_bytes` after each commit; on overflow the operator
/// surfaces E310 with a partition + spill-bytes diagnostic instead
/// of continuing to fill the disk.
pub struct MemoryArbitrator {
    /// Total memory limit in bytes (the hard limit). Default: 512MB.
    pub limit: u64,
    /// Fraction of limit at which proactive spill triggers (the soft
    /// limit). Default: 0.80. Dual-threshold model: 80% soft / 100%
    /// hard / 20% spike allowance — OTel Memory Limiter consensus.
    pub spill_threshold_pct: f64,
    /// Peak RSS observed across all `observe()` / `should_spill()`
    /// calls. `None` on platforms where RSS measurement is
    /// unavailable.
    pub peak_rss: Option<u64>,
    /// Optional disk-spill quota in bytes. `u64::MAX` = unlimited.
    /// Polled by spill operators alongside `should_spill()`. When
    /// cumulative spill bytes exceed this value the operator emits
    /// E310 with a partition + spill-bytes diagnostic.
    pub max_spill_bytes: u64,
    cumulative_spill_bytes: u64,
    /// Cumulative bytes charged across every Arena build site sharing
    /// this arbitrator. The source-rooted arena and every node-rooted
    /// arena share one counter so a pipeline declaring a 512MB limit
    /// cannot multiply that limit across operators.
    arena_bytes_charged: u64,
    /// Cumulative bytes alive in the inter-stage handoff layer between
    /// non-fused DAG operators. Summed with `arena_bytes_charged` via
    /// `total_charged()` — the declared memory limit is a
    /// pipeline-wide ceiling, not per-surface.
    node_buffer_bytes_charged: u64,
    consumers: HashMap<ConsumerId, Box<dyn MemoryConsumer>>,
    policy: Box<dyn ArbitrationPolicy>,
}

impl MemoryArbitrator {
    /// Build an arbitrator with `limit` bytes hard ceiling and
    /// `spill_threshold_pct` soft-limit fraction. Ships with
    /// `NoOpPolicy` and no registered consumers.
    pub fn new(limit: u64, spill_threshold_pct: f64) -> Self {
        Self {
            limit,
            spill_threshold_pct,
            peak_rss: None,
            max_spill_bytes: u64::MAX,
            cumulative_spill_bytes: 0,
            arena_bytes_charged: 0,
            node_buffer_bytes_charged: 0,
            consumers: HashMap::new(),
            policy: Box::new(NoOpPolicy),
        }
    }

    /// Build from config `memory_limit` string ("512M", "2G", raw
    /// bytes). Uses the 0.80 default for `spill_threshold_pct`
    /// (80% soft / 100% hard / 20% spike allowance).
    /// `max_spill_bytes` defaults to `u64::MAX` (unlimited disk
    /// quota); callers that want a quota set it directly on the
    /// constructed arbitrator.
    pub fn from_config(memory_limit: Option<&str>) -> Self {
        let limit = parse_memory_limit_bytes(memory_limit);
        Self::new(limit, 0.80)
    }

    /// Poll current RSS and update `peak_rss` if it exceeds the
    /// recorded peak. Called at chunk boundaries by every spill-
    /// capable operator; also called from `should_spill()` and
    /// `should_abort()`.
    pub fn observe(&mut self) {
        if let Some(rss) = rss_bytes() {
            self.peak_rss = Some(self.peak_rss.map_or(rss, |prev| prev.max(rss)));
        }
    }

    /// True when current RSS exceeds the soft spill threshold. Also
    /// updates `peak_rss` as a side-effect and runs one arbitration
    /// round (the round is a no-op under `NoOpPolicy`). False when
    /// RSS cannot be measured.
    pub fn should_spill(&mut self) -> bool {
        self.observe();
        let tripped = self.peak_rss.is_some_and(|rss| rss > self.soft_limit());
        if tripped {
            // The result is intentionally discarded: real
            // victim selection lands once non-trivial policies and
            // consumer registrations exist. `NoOpPolicy` always
            // returns `None`, so behavior is byte-for-byte identical
            // to the pre-arbitrator code path.
            let _ = self.poll_arbitration();
        }
        tripped
    }

    /// Soft spill threshold in absolute bytes.
    pub fn spill_threshold_bytes(&self) -> u64 {
        self.soft_limit()
    }

    /// Soft limit: point at which proactive spill begins. Equals
    /// `limit * spill_threshold_pct` (default 0.80).
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

    /// True when RSS exceeds the hard limit. Check periodically in
    /// probe loops (every 10K output records) to prevent unbounded
    /// fan-out from blowing the ceiling between spill polls.
    pub fn should_abort(&mut self) -> bool {
        self.observe();
        self.peak_rss.is_some_and(|rss| rss > self.limit)
    }

    /// Disk-spill quota in bytes. `u64::MAX` = unlimited.
    pub fn disk_quota(&self) -> u64 {
        self.max_spill_bytes
    }

    /// Cumulative spill bytes recorded so far across every spill
    /// operator polling this arbitrator.
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
    /// One shared counter across the source-rooted arena and every
    /// node-rooted arena built at an upstream operator's dispatch-arm
    /// exit: the pipeline's declared memory limit is a pipeline-wide
    /// envelope, not a per-arena one.
    pub fn charge_arena_bytes(&mut self, n: u64) -> bool {
        self.arena_bytes_charged = self.arena_bytes_charged.saturating_add(n);
        self.arena_bytes_charged > self.limit
    }

    /// Cumulative arena bytes charged so far across every Arena build
    /// site polling this arbitrator.
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
    /// `ctx.node_buffers` slot polling this arbitrator.
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

    /// Run one arbitration round and return the elected victim, if
    /// any. Used internally by `should_spill`; real callers will
    /// land once policies and consumer registrations exist. With
    /// `NoOpPolicy` and an empty consumer registry the call is a
    /// cheap loop over zero entries and always returns `None`.
    fn poll_arbitration(&mut self) -> Option<ConsumerId> {
        let pressure = self.peak_rss.unwrap_or(0).saturating_sub(self.soft_limit());
        let snapshot: Vec<(ConsumerId, &dyn MemoryConsumer)> = self
            .consumers
            .iter()
            .map(|(id, consumer)| (*id, consumer.as_ref()))
            .collect();
        self.policy.select_victim(&snapshot, pressure)
    }
}

/// Parse memory limit string to bytes. Supports "512M", "2G",
/// "512m", "2g", raw integer string. Returns 512MB default if
/// `None` or unparseable.
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
    fn test_memory_arbitrator_below_threshold() {
        let mut arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        // Test process RSS should be well under 410MB (80% of 512MB)
        if rss_bytes().is_some() {
            assert!(!arbitrator.should_spill());
        }
    }

    #[test]
    fn test_memory_arbitrator_above_threshold() {
        // Limit of 1MB — any running process exceeds this
        let mut arbitrator = MemoryArbitrator::new(1024 * 1024, 0.80);
        if rss_bytes().is_some() {
            assert!(arbitrator.should_spill());
        }
    }

    #[test]
    fn test_memory_arbitrator_peak_rss_tracked() {
        let mut arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        if rss_bytes().is_none() {
            return; // Skip on unsupported platforms
        }
        arbitrator.observe();
        assert!(
            arbitrator.peak_rss.is_some(),
            "peak_rss should be set after observe()"
        );
        let first_peak = arbitrator.peak_rss.unwrap();
        arbitrator.observe();
        // Peak must be non-decreasing
        assert!(arbitrator.peak_rss.unwrap() >= first_peak);
    }

    #[test]
    fn test_memory_arbitrator_default_values() {
        let arbitrator = MemoryArbitrator::from_config(None);
        assert_eq!(arbitrator.limit, 512 * 1024 * 1024);
        assert!((arbitrator.spill_threshold_pct - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_quota_default_unlimited() {
        let arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        assert_eq!(arbitrator.disk_quota(), u64::MAX);
        assert_eq!(arbitrator.cumulative_spill_bytes(), 0);
    }

    #[test]
    fn test_record_spill_bytes_under_quota() {
        let mut arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        arbitrator.max_spill_bytes = 1024;
        assert!(!arbitrator.record_spill_bytes(256));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 256);
        assert!(!arbitrator.record_spill_bytes(512));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 768);
    }

    #[test]
    fn test_record_spill_bytes_overflows_quota() {
        let mut arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        arbitrator.max_spill_bytes = 1024;
        assert!(!arbitrator.record_spill_bytes(1024));
        assert!(arbitrator.record_spill_bytes(1));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 1025);
    }

    #[test]
    fn test_record_spill_bytes_saturates_on_overflow() {
        // Saturating-add on a u64 counter: even an absurdly large
        // accumulation cannot wrap below the configured quota and
        // silently disable the gate.
        let mut arbitrator = MemoryArbitrator::new(512 * 1024 * 1024, 0.80);
        arbitrator.max_spill_bytes = 1024;
        arbitrator.cumulative_spill_bytes = u64::MAX - 10;
        assert!(arbitrator.record_spill_bytes(100));
        assert_eq!(arbitrator.cumulative_spill_bytes(), u64::MAX);
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
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        assert!(!arbitrator.charge_node_buffer_bytes(256));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 256);
        assert!(!arbitrator.charge_node_buffer_bytes(512));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_node_buffer_bytes_decreases_counter() {
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        arbitrator.charge_node_buffer_bytes(1024);
        arbitrator.discharge_node_buffer_bytes(256);
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_saturates_on_over_discharge() {
        // Estimate drift between producer's charge and consumer's
        // discharge must not wrap the counter to a huge positive
        // value and silently disable the gate downstream.
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        arbitrator.charge_node_buffer_bytes(100);
        arbitrator.discharge_node_buffer_bytes(200);
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 0);
    }

    #[test]
    fn test_charge_node_buffer_bytes_under_limit_returns_false() {
        let mut arbitrator = MemoryArbitrator::new(1024, 0.80);
        assert!(!arbitrator.charge_node_buffer_bytes(256));
    }

    #[test]
    fn test_charge_node_buffer_bytes_at_limit_returns_false() {
        // Strict `>` semantics: exactly at the limit is NOT an
        // overflow, matching `record_spill_bytes` and
        // `charge_arena_bytes`. Only the byte that crosses trips.
        let mut arbitrator = MemoryArbitrator::new(1024, 0.80);
        assert!(!arbitrator.charge_node_buffer_bytes(1024));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 1024);
    }

    #[test]
    fn test_charge_node_buffer_bytes_over_limit_returns_true() {
        let mut arbitrator = MemoryArbitrator::new(1024, 0.80);
        assert!(!arbitrator.charge_node_buffer_bytes(1024));
        assert!(arbitrator.charge_node_buffer_bytes(1));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 1025);
    }

    #[test]
    fn test_charge_node_buffer_bytes_trips_via_arena_contribution() {
        // Load-bearing test: the new counter shares the `limit`
        // envelope with the arena counter. Neither one alone exceeds
        // the limit, but their sum does — and the node-buffer charge
        // must surface that.
        let mut arbitrator = MemoryArbitrator::new(1024, 0.80);
        assert!(!arbitrator.charge_arena_bytes(800));
        assert!(!arbitrator.charge_node_buffer_bytes(100));
        assert!(arbitrator.charge_node_buffer_bytes(200));
        assert_eq!(arbitrator.total_charged(), 1100);
    }

    #[test]
    fn test_charge_node_buffer_bytes_saturates_on_overflow() {
        // Saturating-add on a u64 counter: even an absurdly large
        // accumulation cannot wrap below the configured limit and
        // silently disable the gate.
        let mut arbitrator = MemoryArbitrator::new(1024, 0.80);
        arbitrator.node_buffer_bytes_charged = u64::MAX - 10;
        assert!(arbitrator.charge_node_buffer_bytes(100));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), u64::MAX);
    }

    #[test]
    fn test_total_charged_sums_arena_and_node_buffer() {
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        arbitrator.charge_arena_bytes(400);
        arbitrator.charge_node_buffer_bytes(600);
        assert_eq!(arbitrator.total_charged(), 1000);
    }

    #[test]
    fn test_total_charged_excludes_cumulative_spill_bytes() {
        // Disk spill is governed by `max_spill_bytes`, not the
        // in-memory `limit` envelope. Including spill bytes in
        // `total_charged()` would double-count the spilled work
        // against the RAM budget and force premature aborts.
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        arbitrator.cumulative_spill_bytes = 9999;
        arbitrator.charge_arena_bytes(100);
        assert_eq!(arbitrator.total_charged(), 100);
    }

    #[test]
    fn test_total_charged_saturates_on_overflow() {
        // Two near-`u64::MAX` counters must not wrap the sum and
        // silently pass the `total_charged() > limit` gate.
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        arbitrator.arena_bytes_charged = u64::MAX - 10;
        arbitrator.node_buffer_bytes_charged = 1000;
        assert_eq!(arbitrator.total_charged(), u64::MAX);
    }

    /// Minimal `MemoryConsumer` used to exercise the trait surface
    /// from inside the crate — there are no production implementers
    /// yet; real ones land alongside operator registration.
    struct MockConsumer {
        usage: u64,
        priority: i32,
        paused: bool,
        spill_response: u64,
    }

    impl MemoryConsumer for MockConsumer {
        fn current_usage(&self) -> u64 {
            self.usage
        }
        fn spill_priority(&self) -> i32 {
            self.priority
        }
        fn try_spill(&mut self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
            let freed = self.spill_response.min(target_bytes);
            self.usage = self.usage.saturating_sub(freed);
            if freed == target_bytes {
                Ok(freed)
            } else {
                Err(ConsumerSpillError::BelowTarget {
                    target: target_bytes,
                    freed,
                })
            }
        }
        fn can_back_pressure(&self) -> bool {
            true
        }
        fn pause(&mut self) {
            self.paused = true;
        }
        fn resume(&mut self) {
            self.paused = false;
        }
    }

    #[test]
    fn test_noop_policy_select_victim_returns_none_empty() {
        let policy = NoOpPolicy;
        assert!(policy.select_victim(&[], 0).is_none());
        assert!(policy.select_victim(&[], 4096).is_none());
    }

    #[test]
    fn test_noop_policy_select_victim_returns_none_with_candidates() {
        let policy = NoOpPolicy;
        let mut consumer = MockConsumer {
            usage: 4096,
            priority: 0,
            paused: false,
            spill_response: 4096,
        };
        let id = ConsumerId(0);
        let consumer_ref: &dyn MemoryConsumer = &consumer;
        assert!(policy.select_victim(&[(id, consumer_ref)], 1024).is_none());
        // Touch every other trait method so the mock is fully exercised
        // — this is the in-crate non-test infrastructure that 117b/c
        // will replace.
        assert_eq!(consumer.current_usage(), 4096);
        assert_eq!(consumer.spill_priority(), 0);
        assert!(consumer.can_back_pressure());
        consumer.pause();
        assert!(consumer.paused);
        consumer.resume();
        assert!(!consumer.paused);
        assert!(consumer.try_spill(4096).is_ok());
        let mut short = MockConsumer {
            usage: 1024,
            priority: 10,
            paused: false,
            spill_response: 100,
        };
        assert!(matches!(
            short.try_spill(500),
            Err(ConsumerSpillError::BelowTarget {
                target: 500,
                freed: 100
            })
        ));
    }

    #[test]
    fn test_memory_arbitrator_new_installs_noop_policy() {
        let mut arbitrator = MemoryArbitrator::new(u64::MAX, 0.80);
        // poll_arbitration is private — exercise it through the
        // public `should_spill` path with a deliberately tiny
        // limit so the soft-threshold gate trips.
        arbitrator.limit = 1;
        arbitrator.peak_rss = Some(100);
        // NoOpPolicy yields no victim, so should_spill still returns
        // true (RSS-based) and no consumer state changes.
        assert!(arbitrator.should_spill());
        assert!(arbitrator.consumers.is_empty());
    }

    #[test]
    fn test_consumer_spill_error_displays_io_and_below_target() {
        let io = ConsumerSpillError::Io(std::io::Error::other("disk full"));
        assert!(format!("{io}").contains("disk full"));
        let short = ConsumerSpillError::BelowTarget {
            target: 1024,
            freed: 256,
        };
        let msg = format!("{short}");
        assert!(msg.contains("256"));
        assert!(msg.contains("1024"));
    }
}
