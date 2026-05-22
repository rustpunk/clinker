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
//! `ArbitrationPolicy`) lets policies pick a victim across operators
//! instead of reacting independently. Production paths install a policy
//! chosen by the pipeline-level `memory.backpressure` knob:
//! `pause` (default) installs `BackPressurePreferred -> Priority`,
//! `spill` installs bare `Priority`, `both` installs
//! `BackPressurePreferred -> LargestFirst`. Tests that want the older
//! react-only behavior pass `Box::new(NoOpPolicy)` explicitly.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

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

/// Shared state between an operator and the `MemoryConsumer`
/// wrapper that the arbitrator registers on its behalf.
///
/// Pull-mode attribution surface: the operator owns one end of the
/// `Arc<ConsumerHandle>` and updates `bytes` on every admit / spill
/// transition; the consumer wrapper owns the other end and reads
/// `bytes` from inside `MemoryConsumer::current_usage`. Decoupled so
/// neither side needs a lock on the other's state — both update
/// lock-free atomics.
///
/// `spill_requested` is the arbitrator's nudge to the operator. The
/// consumer wrapper's `try_spill` flips this flag; the operator's hot
/// loop reads it at batch boundaries via `take_spill_request()` and
/// performs the actual spill in-thread (the existing per-operator
/// spill path). This avoids the deadlock that would arise from a
/// reentrant `Mutex` around operator state when the arbitrator polls
/// from the same thread that holds the operator's live state.
///
/// `paused` mirrors the `MemoryConsumer::pause` / `resume` signal for
/// back-pressureable consumers (Sources, `node_buffers` slots that
/// chain to a pauseable Source). The producer hot loop checks
/// `is_paused()` at batch boundaries; a real blocking wait
/// (`Condvar::wait`) lands alongside the producer-side wiring.
pub struct ConsumerHandle {
    bytes: AtomicU64,
    spill_requested: AtomicBool,
    paused: AtomicBool,
}

impl ConsumerHandle {
    /// Construct an empty handle. Operators and consumer wrappers
    /// share `Arc<ConsumerHandle>` clones of the same instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicU64::new(0),
            spill_requested: AtomicBool::new(false),
            paused: AtomicBool::new(false),
        })
    }

    /// Current live-byte count the operator has reported.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Absolute set: replaces the counter with `n`. Use when the
    /// operator already maintains a `usize` byte tally and just
    /// mirrors it into the atomic at batch boundaries.
    pub fn set_bytes(&self, n: u64) {
        self.bytes.store(n, Ordering::Relaxed);
    }

    /// Saturating add. Use on per-record admissions when the operator
    /// tracks deltas instead of an absolute total.
    pub fn add_bytes(&self, n: u64) {
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_add(n))
            });
    }

    /// Saturating subtract. Use on consumer drains to keep the
    /// counter aligned with what is still live in the operator.
    pub fn sub_bytes(&self, n: u64) {
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(n))
            });
    }

    /// Flip the spill-request flag to `true`. Called by the consumer
    /// wrapper's `try_spill`; the operator's hot loop reads via
    /// `take_spill_request` and reacts on its next batch boundary.
    pub fn request_spill(&self) {
        self.spill_requested.store(true, Ordering::Release);
    }

    /// Read-and-clear the spill-request flag. Returns `true` exactly
    /// once per `request_spill` call (until the next request); the
    /// operator's hot loop polls this at batch boundaries.
    pub fn take_spill_request(&self) -> bool {
        self.spill_requested.swap(false, Ordering::Acquire)
    }

    /// Whether the consumer is currently paused. Producers (Source
    /// ingest tasks, back-pressureable `node_buffer` writers) check
    /// this at batch boundaries; once a blocking wait is wired into
    /// the producer loop, `is_paused() == true` triggers the wait.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Flip the pause flag to `true`. Called by the consumer
    /// wrapper's `pause` for back-pressureable consumers.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Flip the pause flag to `false`. Called by the consumer
    /// wrapper's `resume`.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
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

    /// Pause the consumer's inbound channel. Default body is a no-op:
    /// blocking operators that return `false` from `can_back_pressure`
    /// inherit the default and do not need to override. Back-pressureable
    /// consumers (Sources, `node_buffers` slots whose producer chains to
    /// a pauseable Source) override with a real `PauseSignal::pause()`
    /// call.
    fn pause(&mut self) {}

    /// Resume a previously paused consumer. Default body is a no-op,
    /// mirroring `pause`.
    fn resume(&mut self) {}
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

    /// Short identifier for diagnostics surfaces (`--explain`).
    /// Composing wrappers (`BackPressurePreferred`) recurse to spell
    /// out their inner policy, e.g. `BackPressurePreferred -> Priority`.
    fn policy_name(&self) -> Cow<'static, str>;
}

/// Policy that selects no victim under any pressure.
///
/// Used by tests that want to exercise the RSS-driven react-only
/// path without arbitration interference. Production paths always
/// install a real policy via `MemoryArbitrator::default_policy()`
/// or the `memory.backpressure` knob; this type exists so that
/// pre-policy behavior is reachable as an explicit choice rather
/// than a hidden default.
pub struct NoOpPolicy;

impl ArbitrationPolicy for NoOpPolicy {
    fn select_victim(
        &self,
        _consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        _pressure_bytes: u64,
    ) -> Option<ConsumerId> {
        None
    }

    fn policy_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("NoOp")
    }
}

/// Policy that elects whichever consumer is currently holding the
/// most bytes. On a tie, the last equally-maximum entry in the
/// snapshot wins (per `std`'s `max_by_key` contract); the snapshot
/// itself comes from `HashMap` iteration, so insertion order is
/// not load-bearing — what is load-bearing is that selection is
/// deterministic within a single arbitrator instance for a given
/// `consumers` membership.
///
/// Mirrors Spark's `TaskMemoryManager` largest-acquired-first
/// strategy: freeing the biggest holder yields the most headroom
/// per spill call.
pub struct LargestFirst;

impl ArbitrationPolicy for LargestFirst {
    fn select_victim(
        &self,
        consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        _pressure_bytes: u64,
    ) -> Option<ConsumerId> {
        consumers
            .iter()
            .max_by_key(|(_, c)| c.current_usage())
            .map(|(id, _)| *id)
    }

    fn policy_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("LargestFirst")
    }
}

/// Policy that elects the consumer with the lowest
/// `spill_priority()` value (lower = spill first). Ties broken by
/// `current_usage()` (largest first) so that two equally-priority
/// consumers still produce deterministic, headroom-maximizing
/// selection.
///
/// Suits the cheapest-to-spill-first heuristic: `node_buffers` (0)
/// before `grace-hash` (10) before sort (20) before Aggregate (30).
/// Per-consumer priorities are set in each operator's
/// `MemoryConsumer` impl (lands in 117c).
pub struct Priority;

impl ArbitrationPolicy for Priority {
    fn select_victim(
        &self,
        consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        _pressure_bytes: u64,
    ) -> Option<ConsumerId> {
        consumers
            .iter()
            .min_by(|(_, a), (_, b)| {
                a.spill_priority()
                    .cmp(&b.spill_priority())
                    .then_with(|| b.current_usage().cmp(&a.current_usage()))
            })
            .map(|(id, _)| *id)
    }

    fn policy_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Priority")
    }
}

/// Policy wrapper that prefers pausing a back-pressureable consumer
/// over forcing anyone to spill. Falls back to the wrapped policy
/// when no consumer reports `can_back_pressure() == true`.
///
/// Spill incurs disk I/O for no benefit when the downstream will
/// drain in finite time — pausing the producer is strictly cheaper.
/// This is the runtime default: `BackPressurePreferred::wrapping(
/// Priority)` (see `MemoryArbitrator::default_policy`).
pub struct BackPressurePreferred {
    fallback: Box<dyn ArbitrationPolicy>,
}

impl BackPressurePreferred {
    /// Wrap `p` so any back-pressureable consumer is preferred over
    /// what `p` would have picked.
    pub fn wrapping<P: ArbitrationPolicy + 'static>(p: P) -> Self {
        Self {
            fallback: Box::new(p),
        }
    }
}

impl ArbitrationPolicy for BackPressurePreferred {
    fn select_victim(
        &self,
        consumers: &[(ConsumerId, &dyn MemoryConsumer)],
        pressure_bytes: u64,
    ) -> Option<ConsumerId> {
        consumers
            .iter()
            .find(|(_, c)| c.can_back_pressure())
            .map(|(id, _)| *id)
            .or_else(|| self.fallback.select_victim(consumers, pressure_bytes))
    }

    fn policy_name(&self) -> Cow<'static, str> {
        Cow::Owned(format!(
            "BackPressurePreferred -> {}",
            self.fallback.policy_name()
        ))
    }
}

/// Central memory arbitrator. Owns the RSS hard / soft limits, the
/// per-category byte counters (arena, node-buffer, disk spill), and
/// the policy + consumer registry used to elect spill victims.
///
/// Interior-mutable so a single arbitrator can be shared as
/// `Arc<MemoryArbitrator>` across every dispatch arm and operator
/// worker thread without per-arm reconstruction. Counters are
/// `AtomicU64` (lock-free fetch_update / fetch_max); the consumer
/// registry is `Mutex<HashMap>` (locked only on register / unregister
/// / `sum_consumer_usage` / `poll_arbitration`). `policy` is
/// constructor-set and immutable thereafter.
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
    /// `AtomicU64` for `&self` access; production sets this once at
    /// construction. Tests reconfigure via `set_limit`.
    limit: AtomicU64,
    /// Fraction of limit at which proactive spill triggers (the soft
    /// limit). Default: 0.80. Dual-threshold model: 80% soft / 100%
    /// hard / 20% spike allowance — OTel Memory Limiter consensus.
    /// Plain `f64`: constructor-set, never reconfigured at runtime;
    /// `std` has no atomic for it and a `Mutex` would be overkill.
    spill_threshold_pct: f64,
    /// Peak RSS observed across all `observe()` / `should_spill()`
    /// calls. Sentinel `0` = never observed; the `peak_rss()` getter
    /// maps `0` back to `None` to preserve the pre-117c "platform
    /// without RSS support" branch. RSS is never legitimately 0 on
    /// Linux / macOS / Windows for a live process so the sentinel is
    /// unambiguous.
    peak_rss: AtomicU64,
    /// Optional disk-spill quota in bytes. `u64::MAX` = unlimited.
    /// Polled by spill operators alongside `should_spill()`. When
    /// cumulative spill bytes exceed this value the operator emits
    /// E310 with a partition + spill-bytes diagnostic.
    max_spill_bytes: AtomicU64,
    cumulative_spill_bytes: AtomicU64,
    /// Cumulative bytes charged across every Arena build site sharing
    /// this arbitrator. The source-rooted arena and every node-rooted
    /// arena share one counter so a pipeline declaring a 512MB limit
    /// cannot multiply that limit across operators.
    arena_bytes_charged: AtomicU64,
    /// Cumulative bytes alive in the inter-stage handoff layer between
    /// non-fused DAG operators. Summed with `arena_bytes_charged` via
    /// `total_charged()` — the declared memory limit is a
    /// pipeline-wide ceiling, not per-surface.
    node_buffer_bytes_charged: AtomicU64,
    /// Registry of operator wrappers the arbitrator polls for
    /// per-operator `current_usage()` and routes `pause` / `resume` /
    /// `try_spill` callbacks to. Locked only on register / unregister
    /// and inside `poll_arbitration` (rare — at most once per soft-
    /// trip per polling operator).
    consumers: Mutex<HashMap<ConsumerId, Box<dyn MemoryConsumer>>>,
    /// Source of fresh `ConsumerId` values handed out by
    /// `register_consumer`. Monotonic; never reused even across
    /// `unregister_consumer`, so a stale `ConsumerId` cannot collide
    /// with a later registration.
    next_consumer_id: AtomicU32,
    /// Constructor-set; immutable thereafter.
    policy: Box<dyn ArbitrationPolicy>,
}

impl MemoryArbitrator {
    /// Build an arbitrator with `limit` bytes hard ceiling,
    /// `spill_threshold_pct` soft-limit fraction, and an explicit
    /// `ArbitrationPolicy`. Ships with no registered consumers.
    ///
    /// Production paths construct via this constructor with the
    /// policy chosen by `memory.backpressure` (see
    /// `BackpressureKnob::build_policy` in `crate::config`).
    /// Tests that want pre-policy react-only behavior pass
    /// `Box::new(NoOpPolicy)`; tests that want the production
    /// default pass `Self::default_policy()`.
    pub fn with_policy(
        limit: u64,
        spill_threshold_pct: f64,
        policy: Box<dyn ArbitrationPolicy>,
    ) -> Self {
        Self {
            limit: AtomicU64::new(limit),
            spill_threshold_pct,
            peak_rss: AtomicU64::new(0),
            max_spill_bytes: AtomicU64::new(u64::MAX),
            cumulative_spill_bytes: AtomicU64::new(0),
            arena_bytes_charged: AtomicU64::new(0),
            node_buffer_bytes_charged: AtomicU64::new(0),
            consumers: Mutex::new(HashMap::new()),
            next_consumer_id: AtomicU32::new(0),
            policy,
        }
    }

    /// Runtime default policy: prefer pausing a back-pressureable
    /// consumer over forcing anyone to spill, falling back to
    /// `Priority` (cheapest-to-spill first) when no consumer can
    /// be paused. Returned as a fresh `Box` so each arbitrator gets
    /// its own policy instance.
    pub fn default_policy() -> Box<dyn ArbitrationPolicy> {
        Box::new(BackPressurePreferred::wrapping(Priority))
    }

    /// Read access to the active policy. Used by `--explain` to
    /// render the `arbitration:` annotation in the plan header.
    pub fn policy(&self) -> &dyn ArbitrationPolicy {
        self.policy.as_ref()
    }

    /// Poll current RSS and update `peak_rss` if it exceeds the
    /// recorded peak. Called at chunk boundaries by every spill-
    /// capable operator; also called from `should_spill()` and
    /// `should_abort()`. Lock-free `fetch_max`.
    pub fn observe(&self) {
        if let Some(rss) = rss_bytes() {
            self.peak_rss.fetch_max(rss, Ordering::Relaxed);
        }
    }

    /// True when current RSS exceeds the soft spill threshold. Also
    /// updates `peak_rss` as a side-effect and runs one arbitration
    /// round; the round is a no-op when no consumers are registered.
    /// False when RSS cannot be measured (sentinel `0`).
    pub fn should_spill(&self) -> bool {
        self.observe();
        let tripped = self.peak_rss.load(Ordering::Relaxed) > self.soft_limit();
        if tripped {
            // Discarded victim id is the right shape until the
            // arbitrator gains a callback path back into the elected
            // consumer's `try_spill` / `pause`; the round-trip lands
            // alongside operator-side `MemoryConsumer` impls.
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
        (self.limit.load(Ordering::Relaxed) as f64 * self.spill_threshold_pct) as u64
    }

    /// Hard limit: the absolute budget. RSS above this = abort.
    pub fn hard_limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Configured memory limit in bytes (the hard limit).
    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Reconfigure the hard limit. Production paths set this only at
    /// construction; integration tests use this to drive deterministic
    /// overflow scenarios without spawning processes of the requested
    /// RSS size.
    pub fn set_limit(&self, n: u64) {
        self.limit.store(n, Ordering::Relaxed);
    }

    /// Soft-limit fraction (constructor-set; default 0.80).
    pub fn spill_threshold_pct(&self) -> f64 {
        self.spill_threshold_pct
    }

    /// Spike allowance between soft and hard limits (default 20%).
    pub fn spike_allowance(&self) -> u64 {
        self.hard_limit() - self.soft_limit()
    }

    /// True when RSS exceeds the hard limit. Check periodically in
    /// probe loops (every 10K output records) to prevent unbounded
    /// fan-out from blowing the ceiling between spill polls.
    pub fn should_abort(&self) -> bool {
        self.observe();
        self.peak_rss.load(Ordering::Relaxed) > self.limit.load(Ordering::Relaxed)
    }

    /// Peak RSS observed so far, or `None` on platforms where
    /// `rss_bytes()` cannot measure (no `observe()` call has ever
    /// produced a non-zero sample).
    pub fn peak_rss(&self) -> Option<u64> {
        match self.peak_rss.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    /// Seed the peak RSS counter to a deterministic value. Production
    /// paths never call this; integration tests use it to drive the
    /// arbitration loop without faking the OS RSS reading.
    pub fn set_peak_rss_for_test(&self, n: u64) {
        self.peak_rss.store(n, Ordering::Relaxed);
    }

    /// Disk-spill quota in bytes. `u64::MAX` = unlimited.
    pub fn disk_quota(&self) -> u64 {
        self.max_spill_bytes.load(Ordering::Relaxed)
    }

    /// Configured disk-spill quota in bytes.
    pub fn max_spill_bytes(&self) -> u64 {
        self.max_spill_bytes.load(Ordering::Relaxed)
    }

    /// Reconfigure the disk-spill quota. Production sets this at
    /// construction; integration tests use it to drive E310
    /// overshoot scenarios.
    pub fn set_max_spill_bytes(&self, n: u64) {
        self.max_spill_bytes.store(n, Ordering::Relaxed);
    }

    /// Cumulative spill bytes recorded so far across every spill
    /// operator polling this arbitrator.
    pub fn cumulative_spill_bytes(&self) -> u64 {
        self.cumulative_spill_bytes.load(Ordering::Relaxed)
    }

    /// Add `n` bytes to the cumulative spill counter. Returns `true`
    /// when the running total has exceeded `max_spill_bytes` — the
    /// operator's signal to surface E310 instead of continuing to
    /// write. Saturating-add via `fetch_update` keeps an overflowing
    /// pipeline from wrapping back below the quota silently.
    pub fn record_spill_bytes(&self, n: u64) -> bool {
        let _ =
            self.cumulative_spill_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_add(n))
                });
        self.cumulative_spill_bytes.load(Ordering::Relaxed)
            > self.max_spill_bytes.load(Ordering::Relaxed)
    }

    /// Add `n` bytes to the cumulative arena-byte counter. Returns
    /// `true` when the running total has exceeded the hard memory
    /// limit — the build site's signal to surface E310 instead of
    /// admitting the next record. Saturating-add via `fetch_update`
    /// ensures an overflowing accumulation cannot wrap below the
    /// limit and silently disable the gate.
    ///
    /// One shared counter across the source-rooted arena and every
    /// node-rooted arena built at an upstream operator's dispatch-arm
    /// exit: the pipeline's declared memory limit is a pipeline-wide
    /// envelope, not a per-arena one.
    pub fn charge_arena_bytes(&self, n: u64) -> bool {
        let _ =
            self.arena_bytes_charged
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_add(n))
                });
        self.arena_bytes_charged.load(Ordering::Relaxed) > self.limit.load(Ordering::Relaxed)
    }

    /// Cumulative arena bytes charged so far across every Arena build
    /// site polling this arbitrator.
    pub fn arena_bytes_charged(&self) -> u64 {
        self.arena_bytes_charged.load(Ordering::Relaxed)
    }

    /// Add `n` bytes to the node-buffer counter when a producer hands
    /// a buffer to a `ctx.node_buffers` slot. Returns `true` when the
    /// running total of (arena + node_buffer) has exceeded `limit` —
    /// the executor's signal to surface
    /// `MemoryBudgetExceeded { source: BudgetCategory::NodeBuffer, .. }`
    /// instead of admitting the next stage's output. Saturating-add
    /// keeps an overflowing accumulation from wrapping below the limit
    /// and silently disabling the gate.
    pub fn charge_node_buffer_bytes(&self, n: u64) -> bool {
        let _ = self.node_buffer_bytes_charged.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_add(n)),
        );
        self.total_charged() > self.limit.load(Ordering::Relaxed)
    }

    /// Subtract `n` bytes from the node-buffer counter when a consumer
    /// drains the corresponding `ctx.node_buffers` slot. Saturating-sub
    /// via `fetch_update` guards against over-discharge from estimate
    /// drift between the producer's charge and the consumer's
    /// discharge.
    pub fn discharge_node_buffer_bytes(&self, n: u64) {
        let _ = self.node_buffer_bytes_charged.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(n)),
        );
    }

    /// Cumulative node-buffer bytes currently alive across every
    /// `ctx.node_buffers` slot polling this arbitrator.
    pub fn node_buffer_bytes_charged(&self) -> u64 {
        self.node_buffer_bytes_charged.load(Ordering::Relaxed)
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
            .load(Ordering::Relaxed)
            .saturating_add(self.node_buffer_bytes_charged.load(Ordering::Relaxed))
    }

    /// Register a consumer with the arbitrator. Returns a fresh
    /// `ConsumerId` the operator records for later
    /// `unregister_consumer` calls. The only path that adds a
    /// contributor to the arbitrator's policy registry.
    pub fn register_consumer(&self, consumer: Box<dyn MemoryConsumer>) -> ConsumerId {
        let id = ConsumerId(self.next_consumer_id.fetch_add(1, Ordering::Relaxed));
        self.consumers.lock().unwrap().insert(id, consumer);
        id
    }

    /// Remove a previously-registered consumer from the policy
    /// registry. Returns the boxed consumer if `id` was registered,
    /// `None` otherwise. Operators call this at teardown when their
    /// state has been fully drained and the wrapper is no longer a
    /// meaningful spill victim.
    pub fn unregister_consumer(&self, id: ConsumerId) -> Option<Box<dyn MemoryConsumer>> {
        self.consumers.lock().unwrap().remove(&id)
    }

    /// Number of consumers currently registered. Diagnostics surface
    /// for `--explain` and integration tests; per-consumer attribution
    /// reads `current_usage()` via `sum_consumer_usage`.
    pub fn consumer_count(&self) -> usize {
        self.consumers.lock().unwrap().len()
    }

    /// Sum of `current_usage()` across every registered consumer.
    /// Pull-mode attribution: only the operator knows which of its
    /// bytes are reclaimable right now (a grace-hash with on-disk
    /// partitions has held bytes ≠ reclaimable bytes). The Spark /
    /// Velox / DataFusion memory pools converged on this shape for
    /// victim selection because no central counter can represent the
    /// distinction.
    pub fn sum_consumer_usage(&self) -> u64 {
        self.consumers
            .lock()
            .unwrap()
            .values()
            .map(|c| c.current_usage())
            .sum()
    }

    /// Run one arbitration round and return the elected victim, if
    /// any. Used internally by `should_spill`. Snapshots the consumer
    /// registry under the registry lock, computes the policy decision,
    /// and emits a `tracing::warn` when peak RSS and the sum of
    /// registered consumer usages disagree by more than 10% of the
    /// configured limit — useful signal that allocator overhead or
    /// fragmentation is meaningful, never a failure.
    ///
    /// Warning is suppressed when the consumer registry is empty: no
    /// pull-mode attribution means no signal to compare against and
    /// the disagreement is structurally guaranteed.
    fn poll_arbitration(&self) -> Option<ConsumerId> {
        let peak_rss = self.peak_rss.load(Ordering::Relaxed);
        let pressure = peak_rss.saturating_sub(self.soft_limit());
        let limit = self.limit.load(Ordering::Relaxed);
        let consumers = self.consumers.lock().unwrap();
        if consumers.is_empty() {
            return self.policy.select_victim(&[], pressure);
        }
        let snapshot: Vec<(ConsumerId, &dyn MemoryConsumer)> = consumers
            .iter()
            .map(|(id, consumer)| (*id, consumer.as_ref()))
            .collect();
        let charged_sum: u64 = snapshot.iter().map(|(_, c)| c.current_usage()).sum();
        let tenth = limit / 10;
        if tenth > 0 && peak_rss.abs_diff(charged_sum) > tenth {
            tracing::warn!(
                peak_rss = peak_rss,
                charged_sum = charged_sum,
                limit = limit,
                "memory arbitrator: RSS and pull-mode charged bytes disagree by more than 10% of limit"
            );
        }
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
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        // Test process RSS should be well under 410MB (80% of 512MB)
        if rss_bytes().is_some() {
            assert!(!arbitrator.should_spill());
        }
    }

    #[test]
    fn test_memory_arbitrator_above_threshold() {
        // Limit of 1MB — any running process exceeds this
        let arbitrator = MemoryArbitrator::with_policy(1024 * 1024, 0.80, Box::new(NoOpPolicy));
        if rss_bytes().is_some() {
            assert!(arbitrator.should_spill());
        }
    }

    #[test]
    fn test_memory_arbitrator_peak_rss_tracked() {
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        if rss_bytes().is_none() {
            return; // Skip on unsupported platforms
        }
        arbitrator.observe();
        assert!(
            arbitrator.peak_rss().is_some(),
            "peak_rss should be set after observe()"
        );
        let first_peak = arbitrator.peak_rss().unwrap();
        arbitrator.observe();
        // Peak must be non-decreasing
        assert!(arbitrator.peak_rss().unwrap() >= first_peak);
    }

    #[test]
    fn test_parse_memory_limit_default_512mb() {
        let limit = parse_memory_limit_bytes(None);
        let arbitrator = MemoryArbitrator::with_policy(limit, 0.80, Box::new(NoOpPolicy));
        assert_eq!(arbitrator.limit(), 512 * 1024 * 1024);
        assert!((arbitrator.spill_threshold_pct() - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn test_disk_quota_default_unlimited() {
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        assert_eq!(arbitrator.disk_quota(), u64::MAX);
        assert_eq!(arbitrator.cumulative_spill_bytes(), 0);
    }

    #[test]
    fn test_record_spill_bytes_under_quota() {
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        arbitrator.set_max_spill_bytes(1024);
        assert!(!arbitrator.record_spill_bytes(256));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 256);
        assert!(!arbitrator.record_spill_bytes(512));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 768);
    }

    #[test]
    fn test_record_spill_bytes_overflows_quota() {
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        arbitrator.set_max_spill_bytes(1024);
        assert!(!arbitrator.record_spill_bytes(1024));
        assert!(arbitrator.record_spill_bytes(1));
        assert_eq!(arbitrator.cumulative_spill_bytes(), 1025);
    }

    #[test]
    fn test_record_spill_bytes_saturates_on_overflow() {
        // Saturating-add on a u64 counter: even an absurdly large
        // accumulation cannot wrap below the configured quota and
        // silently disable the gate.
        let arbitrator =
            MemoryArbitrator::with_policy(512 * 1024 * 1024, 0.80, Box::new(NoOpPolicy));
        arbitrator.set_max_spill_bytes(1024);
        arbitrator
            .cumulative_spill_bytes
            .store(u64::MAX - 10, Ordering::Relaxed);
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
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        assert!(!arbitrator.charge_node_buffer_bytes(256));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 256);
        assert!(!arbitrator.charge_node_buffer_bytes(512));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_node_buffer_bytes_decreases_counter() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        arbitrator.charge_node_buffer_bytes(1024);
        arbitrator.discharge_node_buffer_bytes(256);
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 768);
    }

    #[test]
    fn test_discharge_saturates_on_over_discharge() {
        // Estimate drift between producer's charge and consumer's
        // discharge must not wrap the counter to a huge positive
        // value and silently disable the gate downstream.
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        arbitrator.charge_node_buffer_bytes(100);
        arbitrator.discharge_node_buffer_bytes(200);
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 0);
    }

    #[test]
    fn test_charge_node_buffer_bytes_under_limit_returns_false() {
        let arbitrator = MemoryArbitrator::with_policy(1024, 0.80, Box::new(NoOpPolicy));
        assert!(!arbitrator.charge_node_buffer_bytes(256));
    }

    #[test]
    fn test_charge_node_buffer_bytes_at_limit_returns_false() {
        // Strict `>` semantics: exactly at the limit is NOT an
        // overflow, matching `record_spill_bytes` and
        // `charge_arena_bytes`. Only the byte that crosses trips.
        let arbitrator = MemoryArbitrator::with_policy(1024, 0.80, Box::new(NoOpPolicy));
        assert!(!arbitrator.charge_node_buffer_bytes(1024));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), 1024);
    }

    #[test]
    fn test_charge_node_buffer_bytes_over_limit_returns_true() {
        let arbitrator = MemoryArbitrator::with_policy(1024, 0.80, Box::new(NoOpPolicy));
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
        let arbitrator = MemoryArbitrator::with_policy(1024, 0.80, Box::new(NoOpPolicy));
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
        let arbitrator = MemoryArbitrator::with_policy(1024, 0.80, Box::new(NoOpPolicy));
        arbitrator
            .node_buffer_bytes_charged
            .store(u64::MAX - 10, Ordering::Relaxed);
        assert!(arbitrator.charge_node_buffer_bytes(100));
        assert_eq!(arbitrator.node_buffer_bytes_charged(), u64::MAX);
    }

    #[test]
    fn test_total_charged_sums_arena_and_node_buffer() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
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
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        arbitrator
            .cumulative_spill_bytes
            .store(9999, Ordering::Relaxed);
        arbitrator.charge_arena_bytes(100);
        assert_eq!(arbitrator.total_charged(), 100);
    }

    #[test]
    fn test_total_charged_saturates_on_overflow() {
        // Two near-`u64::MAX` counters must not wrap the sum and
        // silently pass the `total_charged() > limit` gate.
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        arbitrator
            .arena_bytes_charged
            .store(u64::MAX - 10, Ordering::Relaxed);
        arbitrator
            .node_buffer_bytes_charged
            .store(1000, Ordering::Relaxed);
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
    fn test_with_policy_installs_chosen_policy() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        // poll_arbitration is private — exercise it through the
        // public `should_spill` path with a deliberately tiny
        // limit so the soft-threshold gate trips.
        arbitrator.set_limit(1);
        arbitrator.set_peak_rss_for_test(100);
        // NoOpPolicy yields no victim, so should_spill still returns
        // true (RSS-based) and no consumer state changes.
        assert!(arbitrator.should_spill());
        assert_eq!(arbitrator.consumer_count(), 0);
        assert_eq!(arbitrator.policy().policy_name(), "NoOp");
    }

    #[test]
    fn test_register_consumer_assigns_monotonic_ids() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id_a = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 10,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        let id_b = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 20,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        let id_c = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 30,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        assert_ne!(id_a, id_b);
        assert_ne!(id_b, id_c);
        assert_ne!(id_a, id_c);
        assert_eq!(arbitrator.consumer_count(), 3);
    }

    #[test]
    fn test_unregister_consumer_returns_removed() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 100,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        assert_eq!(arbitrator.consumer_count(), 1);
        let removed = arbitrator.unregister_consumer(id);
        assert!(removed.is_some());
        assert_eq!(arbitrator.consumer_count(), 0);
        // Second unregister with the same id returns None — stale ids
        // are not silently re-bound to a later registration.
        assert!(arbitrator.unregister_consumer(id).is_none());
    }

    #[test]
    fn test_sum_consumer_usage_aggregates_registered() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        assert_eq!(arbitrator.sum_consumer_usage(), 0);
        arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 1024,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 4096,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 100,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        assert_eq!(arbitrator.sum_consumer_usage(), 1024 + 4096 + 100);
    }

    #[test]
    fn test_consumer_ids_are_never_reused_after_unregister() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id_a = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 1,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        arbitrator.unregister_consumer(id_a);
        let id_b = arbitrator.register_consumer(Box::new(MockConsumer {
            usage: 1,
            priority: 0,
            paused: false,
            spill_response: 0,
        }));
        // Monotonic allocator: even after unregister, the next id is
        // strictly above the previous one — stale ConsumerId references
        // cannot collide with a later registration.
        assert_ne!(id_a, id_b);
    }

    /// Operator that intentionally does NOT override the default
    /// `pause` / `resume` trait bodies — exercises the default no-op
    /// implementations added alongside register_consumer.
    struct DefaultPauseConsumer {
        usage: u64,
    }
    impl MemoryConsumer for DefaultPauseConsumer {
        fn current_usage(&self) -> u64 {
            self.usage
        }
        fn spill_priority(&self) -> i32 {
            0
        }
        fn try_spill(&mut self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
        // No pause / resume override — defaults must compile and be
        // callable without panic.
    }

    #[test]
    fn test_consumer_handle_set_add_sub_bytes_round_trip() {
        let h = ConsumerHandle::new();
        assert_eq!(h.bytes(), 0);
        h.set_bytes(1024);
        assert_eq!(h.bytes(), 1024);
        h.add_bytes(512);
        assert_eq!(h.bytes(), 1536);
        h.sub_bytes(256);
        assert_eq!(h.bytes(), 1280);
        // Saturating-sub: discharge past zero clamps to zero, never
        // wraps to a huge positive value that would silently disable
        // downstream pressure gates.
        h.sub_bytes(9999);
        assert_eq!(h.bytes(), 0);
    }

    #[test]
    fn test_consumer_handle_saturating_add_clamps_at_u64_max() {
        let h = ConsumerHandle::new();
        h.set_bytes(u64::MAX - 10);
        h.add_bytes(100);
        // Saturates at u64::MAX rather than wrapping below the limit.
        assert_eq!(h.bytes(), u64::MAX);
    }

    #[test]
    fn test_consumer_handle_request_and_take_spill_round_trip() {
        let h = ConsumerHandle::new();
        assert!(!h.take_spill_request());
        h.request_spill();
        // First `take` after a `request` sees `true`.
        assert!(h.take_spill_request());
        // Subsequent `take` calls without a fresh `request` see `false`.
        assert!(!h.take_spill_request());
    }

    #[test]
    fn test_consumer_handle_pause_resume_round_trip() {
        let h = ConsumerHandle::new();
        assert!(!h.is_paused());
        h.pause();
        assert!(h.is_paused());
        h.resume();
        assert!(!h.is_paused());
    }

    #[test]
    fn test_default_pause_resume_are_callable_noops() {
        let mut consumer = DefaultPauseConsumer { usage: 42 };
        // Both default bodies are no-ops; they must not panic and
        // must leave the consumer's observable state unchanged.
        consumer.pause();
        assert_eq!(consumer.current_usage(), 42);
        consumer.resume();
        assert_eq!(consumer.current_usage(), 42);
    }

    #[test]
    fn test_largest_first_picks_largest() {
        let small = MockConsumer {
            usage: 10,
            priority: 0,
            paused: false,
            spill_response: 0,
        };
        let big = MockConsumer {
            usage: 50,
            priority: 0,
            paused: false,
            spill_response: 0,
        };
        let mid = MockConsumer {
            usage: 30,
            priority: 0,
            paused: false,
            spill_response: 0,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> = vec![
            (ConsumerId(0), &small),
            (ConsumerId(1), &big),
            (ConsumerId(2), &mid),
        ];
        let policy = LargestFirst;
        assert_eq!(policy.select_victim(&consumers, 100), Some(ConsumerId(1)));
        assert_eq!(policy.policy_name(), "LargestFirst");
    }

    #[test]
    fn test_largest_first_ties_broken_by_slice_order() {
        let a = MockConsumer {
            usage: 100,
            priority: 0,
            paused: false,
            spill_response: 0,
        };
        let b = MockConsumer {
            usage: 100,
            priority: 0,
            paused: false,
            spill_response: 0,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> =
            vec![(ConsumerId(0), &a), (ConsumerId(1), &b)];
        // `max_by_key` returns the last equally-maximum element; the
        // policy inherits that contract. The arbitrator's snapshot is
        // `HashMap`-ordered so insertion order isn't load-bearing —
        // what matters is the selection is deterministic given a
        // fixed input slice.
        assert_eq!(
            LargestFirst.select_victim(&consumers, 0),
            Some(ConsumerId(1))
        );
    }

    #[test]
    fn test_largest_first_empty_returns_none() {
        assert!(LargestFirst.select_victim(&[], 1024).is_none());
    }

    #[test]
    fn test_priority_picks_lowest_priority_value() {
        let high = MockConsumer {
            usage: 100,
            priority: 10,
            paused: false,
            spill_response: 0,
        };
        let low = MockConsumer {
            usage: 100,
            priority: 1,
            paused: false,
            spill_response: 0,
        };
        let mid = MockConsumer {
            usage: 100,
            priority: 5,
            paused: false,
            spill_response: 0,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> = vec![
            (ConsumerId(0), &high),
            (ConsumerId(1), &low),
            (ConsumerId(2), &mid),
        ];
        assert_eq!(Priority.select_victim(&consumers, 0), Some(ConsumerId(1)));
        assert_eq!(Priority.policy_name(), "Priority");
    }

    #[test]
    fn test_priority_ties_broken_by_usage_largest_first() {
        let small = MockConsumer {
            usage: 10,
            priority: 5,
            paused: false,
            spill_response: 0,
        };
        let big = MockConsumer {
            usage: 90,
            priority: 5,
            paused: false,
            spill_response: 0,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> =
            vec![(ConsumerId(0), &small), (ConsumerId(1), &big)];
        assert_eq!(Priority.select_victim(&consumers, 0), Some(ConsumerId(1)));
    }

    #[test]
    fn test_priority_empty_returns_none() {
        assert!(Priority.select_victim(&[], 1024).is_none());
    }

    /// Consumer whose `can_back_pressure()` returns false. Used to
    /// confirm `BackPressurePreferred` falls back to the wrapped
    /// policy when no consumer can be paused.
    struct UnpausableConsumer {
        usage: u64,
        priority: i32,
    }
    impl MemoryConsumer for UnpausableConsumer {
        fn current_usage(&self) -> u64 {
            self.usage
        }
        fn spill_priority(&self) -> i32 {
            self.priority
        }
        fn try_spill(&mut self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
        fn pause(&mut self) {}
        fn resume(&mut self) {}
    }

    #[test]
    fn test_back_pressure_preferred_picks_back_pressureable() {
        let unpausable = UnpausableConsumer {
            usage: 1000,
            priority: 0,
        };
        // MockConsumer.can_back_pressure() returns true.
        let pausable = MockConsumer {
            usage: 10,
            priority: 99,
            paused: false,
            spill_response: 0,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> =
            vec![(ConsumerId(0), &unpausable), (ConsumerId(1), &pausable)];
        let policy = BackPressurePreferred::wrapping(Priority);
        // Even though `unpausable` would win on Priority (priority 0 < 99),
        // the back-pressureable consumer is preferred.
        assert_eq!(policy.select_victim(&consumers, 0), Some(ConsumerId(1)));
    }

    #[test]
    fn test_back_pressure_preferred_falls_back_when_none() {
        let a = UnpausableConsumer {
            usage: 100,
            priority: 10,
        };
        let b = UnpausableConsumer {
            usage: 100,
            priority: 1,
        };
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> =
            vec![(ConsumerId(0), &a), (ConsumerId(1), &b)];
        let policy = BackPressurePreferred::wrapping(Priority);
        // No back-pressureable consumer → delegate to Priority, which
        // picks the lower priority value (b, id 1).
        assert_eq!(policy.select_victim(&consumers, 0), Some(ConsumerId(1)));
    }

    #[test]
    fn test_back_pressure_preferred_empty_returns_none() {
        let policy = BackPressurePreferred::wrapping(Priority);
        assert!(policy.select_victim(&[], 0).is_none());
    }

    #[test]
    fn test_default_policy_name_is_composed() {
        let policy = MemoryArbitrator::default_policy();
        assert_eq!(policy.policy_name(), "BackPressurePreferred -> Priority");
    }

    #[test]
    fn test_back_pressure_preferred_wrapping_largest_first_name() {
        let policy = BackPressurePreferred::wrapping(LargestFirst);
        assert_eq!(
            policy.policy_name(),
            "BackPressurePreferred -> LargestFirst"
        );
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
