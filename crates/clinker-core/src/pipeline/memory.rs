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

use arc_swap::ArcSwap;
use petgraph::graph::NodeIndex;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::Condvar;
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
    /// non-fused operators. Each slot registers a `NodeBufferConsumer`
    /// wrapper; the arbitrator's pull-mode `current_usage` reads the
    /// slot's live footprint at every policy poll.
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

/// Producer / consumer pause coordination primitive.
///
/// Pairs an `AtomicBool` pause flag with a `Mutex<()>` + `Condvar` so
/// producer hot loops can BLOCK until resumed rather than busy-spin.
/// The atomic flag is the fast path — `is_paused()` is lock-free, so
/// the common unblocked case stays uncontended.
///
/// Concurrency-substrate agnostic: a dedicated source thread parks on
/// `Condvar::wait`, and a Rayon worker parks the same way. No new crate
/// dependencies — `Condvar` and `Mutex` are `std`.
pub struct PauseSignal {
    paused: AtomicBool,
    mu: Mutex<()>,
    cv: Condvar,
}

impl PauseSignal {
    /// Construct an unblocked signal.
    pub fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            mu: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    /// Whether the signal is currently in the paused state. Lock-free.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Flip to paused. Subsequent `wait_while_paused()` calls block
    /// until `resume()` lands.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Flip to resumed and wake every parked waiter. Producers
    /// re-check the flag inside the `wait` loop so a spurious wake
    /// stays a no-op.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        self.cv.notify_all();
    }

    /// Block the calling thread until the signal is not paused.
    /// Returns immediately when the fast path (`is_paused() == false`)
    /// holds, so the unpaused case never acquires the mutex.
    pub fn wait_while_paused(&self) {
        if !self.is_paused() {
            return;
        }
        let mut guard = self.mu.lock().unwrap();
        while self.is_paused() {
            guard = self.cv.wait(guard).unwrap();
        }
    }
}

impl Default for PauseSignal {
    fn default() -> Self {
        Self::new()
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
/// `pause_signal` mirrors the `MemoryConsumer::pause` / `resume` signal
/// for back-pressureable consumers (Sources, `node_buffers` slots that
/// chain to a pauseable Source). Producer hot loops call
/// `wait_while_paused()` at batch boundaries; the call blocks on a
/// `Condvar` until the arbitrator's `resume` notifies. The fast path
/// (not paused) is lock-free.
pub struct ConsumerHandle {
    bytes: AtomicU64,
    spill_requested: AtomicBool,
    pause_signal: PauseSignal,
}

impl ConsumerHandle {
    /// Construct an empty handle. Operators and consumer wrappers
    /// share `Arc<ConsumerHandle>` clones of the same instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicU64::new(0),
            spill_requested: AtomicBool::new(false),
            pause_signal: PauseSignal::new(),
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

    /// Whether the consumer is currently paused. Lock-free; producers
    /// that want to actually block until resumed use
    /// `wait_while_paused()` instead.
    pub fn is_paused(&self) -> bool {
        self.pause_signal.is_paused()
    }

    /// Flip the pause flag to `true`. Called by the consumer
    /// wrapper's `pause` for back-pressureable consumers.
    pub fn pause(&self) {
        self.pause_signal.pause();
    }

    /// Flip the pause flag to `false` and wake every parked
    /// `wait_while_paused()` caller. Called by the consumer wrapper's
    /// `resume`.
    pub fn resume(&self) {
        self.pause_signal.resume();
    }

    /// Block the calling producer thread until the signal is not
    /// paused. Returns immediately when the fast path holds, so the
    /// unpaused case stays uncontended.
    pub fn wait_while_paused(&self) {
        self.pause_signal.wait_while_paused();
    }
}

/// Memory-consuming operator that the arbitrator can interrogate and,
/// when a policy elects it, ask to give up memory.
///
/// Implementations live with their operator (Aggregate, sort,
/// grace-hash, sort-merge join, IEJoin, inter-stage buffers). The
/// arbitrator holds them as `Arc<dyn MemoryConsumer>` in a copy-on-write
/// snapshot and reads `current_usage` / `spill_priority` /
/// `can_back_pressure` on every arbitration round; `pause` / `resume` /
/// `try_spill` fire only when a policy selects the consumer as a victim.
///
/// All methods take `&self`: the arbitrator drives them from a shared
/// snapshot read, and every implementation routes its mutable state
/// through a shared `Arc<ConsumerHandle>` of atomics, so no exclusive
/// access is required.
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
    ///
    /// Takes `&self`: production consumers route the spill request
    /// through a shared `Arc<ConsumerHandle>` whose atomics need no
    /// exclusive access, which lets the arbitrator drive the callback
    /// from a lock-free snapshot read rather than a mutable registry.
    fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError>;

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
    /// call. Takes `&self` because the pause flag lives behind a shared
    /// handle's atomic.
    fn pause(&self) {}

    /// Resume a previously paused consumer. Default body is a no-op,
    /// mirroring `pause`.
    fn resume(&self) {}
}

/// Per-node memory predictions plus a stable ordering key, supplied to
/// [`MemoryArbitrator::next_runnable`] so it can choose which of several
/// currently-runnable nodes to dispatch by predicted memory impact.
///
/// The plan is the natural implementer: the byte predictions are the
/// plan-time volume estimates carried on each node's `NodeProperties`,
/// and the stable index is the node's position in the plan's canonical
/// topological order. `next_runnable` treats this as a read-only view —
/// it never mutates the plan and calls each method at most once per
/// candidate per invocation.
///
/// All three methods key off [`NodeIndex`]. The caller guarantees the
/// hint knows every index in the `runnable` slice it passes; an unknown
/// index is the caller's contract violation, and an implementer should
/// answer `0` / a stable fallback rather than panic.
pub trait SchedulingHint: Send + Sync {
    /// Predicted peak live bytes for `id` — the volume the node holds at
    /// once at its peak, in the same units as [`MemoryArbitrator::soft_limit`].
    /// `0` means "unknown" (no file-size seed reached the node); an
    /// unknown node participates normally in the tiebreaks but never
    /// trips the headroom filter, which is what keeps behavior unchanged
    /// when no statistics are available.
    fn predicted_peak_bytes(&self, id: NodeIndex) -> u64;

    /// Predicted bytes `id` releases back to the budget when it finishes
    /// draining (a blocking operator drops its accumulated state on its
    /// last emit; a streaming node frees nothing). Used as the higher-
    /// priority freed tiebreak: a node that frees memory the instant it
    /// completes (a ready blocking operator) reclaims headroom *now*, so
    /// it is preferred over one that merely unlocks an eventual reclaim.
    fn predicted_freed_bytes_on_complete(&self, id: NodeIndex) -> u64;

    /// Predicted largest reclaimable footprint anywhere in the subtree
    /// rooted at `id` — the headroom that running this node's chain to
    /// completion eventually unlocks (for a fresh Source, its downstream
    /// blocking operator's accumulated state). The lower-priority freed
    /// tiebreak: it distinguishes candidates that free nothing *now* but
    /// whose chains differ in eventual reclaim, so the scheduler launches
    /// the independent chain whose completion frees the most first. `0`
    /// when nothing downstream accumulates, so it never disturbs the
    /// "no-estimates == today's order" floor.
    fn predicted_subtree_reclaim_bytes(&self, id: NodeIndex) -> u64;

    /// Position of `id` in the plan's canonical topological order — the
    /// deterministic final tiebreak. This must be the SAME order the
    /// executor walks today (front-to-back over `topo_order`), so that
    /// when every candidate's `predicted_peak_bytes` is `0` the chosen
    /// node is exactly the one a plain topo walk would dispatch first.
    /// That equivalence is the load-bearing "no-estimates == today's
    /// order" invariant the downstream dispatch integration relies on.
    fn stable_index(&self, id: NodeIndex) -> usize;
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
/// preserves registration order, so the tie-break is deterministic
/// within a single arbitrator instance for a given `consumers`
/// membership.
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

/// Build the boxed [`ArbitrationPolicy`] a `pipeline.memory.backpressure`
/// knob selects.
///
/// Keeps the concrete policy types ([`Priority`], [`LargestFirst`],
/// [`BackPressurePreferred`]) and their wiring inside the memory
/// subsystem: the config layer carries only the plain
/// [`BackpressureKnob`](crate::config::BackpressureKnob) selector and
/// never names a policy type. Called by every production path that turns
/// parsed config into a live arbitrator.
///
/// - `spill` → bare [`Priority`]: react-only, cheapest-to-spill-first.
/// - `pause` → [`MemoryArbitrator::default_policy`]
///   (`BackPressurePreferred -> Priority`): prefer pausing a producer,
///   otherwise spill cheapest first. This is the runtime default.
/// - `both` → `BackPressurePreferred -> LargestFirst`: prefer pausing,
///   otherwise force the largest holder regardless of priority.
pub fn build_policy(knob: crate::config::BackpressureKnob) -> Box<dyn ArbitrationPolicy> {
    use crate::config::BackpressureKnob;
    match knob {
        BackpressureKnob::Spill => Box::new(Priority),
        BackpressureKnob::Pause => MemoryArbitrator::default_policy(),
        BackpressureKnob::Both => Box::new(BackPressurePreferred::wrapping(LargestFirst)),
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
/// registry is a copy-on-write `ArcSwap<Vec<..>>` snapshot — readers
/// (`sum_consumer_usage`, `poll_arbitration`, `should_spill`) load the
/// current immutable Vec with no lock, and the rare register / unregister
/// clones-and-swaps a fresh Vec. `policy` is constructor-set and
/// immutable thereafter.
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
    /// High-water mark of `sum_consumer_usage()` sampled whenever a
    /// streaming charge handle admits a batch. Lets a test prove the
    /// charged-bytes peak of a streaming stage stays bounded to one
    /// in-flight batch (plus the channel's bound) rather than the whole
    /// stage output — the per-batch admit/discharge invariant. Updated
    /// lock-free via `fetch_max`. Surfaced on the `ExecutionReport` as
    /// `peak_consumer_usage_bytes` for callers that assert the bound.
    peak_consumer_usage: AtomicU64,
    /// Registry of operator wrappers the arbitrator polls for
    /// per-operator `current_usage()` and routes `pause` / `resume` /
    /// `try_spill` callbacks to. A copy-on-write snapshot: hot read
    /// paths load the current immutable Vec lock-free and iterate it;
    /// register / unregister (rare, once per operator lifetime) clone
    /// the Vec, mutate the clone, and atomically store it. Elements are
    /// `Arc` so an in-flight reader holding the old snapshot keeps its
    /// wrappers alive even as a concurrent unregister swaps them out.
    consumers: ArcSwap<Vec<(ConsumerId, Arc<dyn MemoryConsumer>)>>,
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
    /// policy chosen by `memory.backpressure` (see [`build_policy`]).
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
            peak_consumer_usage: AtomicU64::new(0),
            consumers: ArcSwap::from_pointee(Vec::new()),
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
            // Drive the round-trip: the elected victim is paused
            // (back-pressureable) or asked to spill (everything else).
            // `try_spill` flips the consumer's `spill_requested` flag,
            // which the operator's hot loop reads via
            // `take_spill_request` and reacts to in-thread. `pause`
            // flips the consumer's `PauseSignal`, which the
            // producer's `wait_while_paused` blocks on.
            self.poll_arbitration();
        }
        tripped
    }

    /// True when current RSS exceeds the soft spill threshold, without
    /// running an arbitration round. Updates `peak_rss` as a side effect
    /// but never elects a victim, so it never calls `pause()` on a
    /// registered consumer.
    ///
    /// A streaming stage spills *its own* in-flight batch when this trips
    /// (it owns the batch and discharges it through disk in-thread). It
    /// must not drive the pausing round `should_spill` runs: that round
    /// can elect the live Source consumer and `pause()` its ingest thread,
    /// which has no production resume hook — the Source channel then stops
    /// refilling, the dispatch loop blocks on `recv`, and the pipeline
    /// deadlocks. The streaming path's back-pressure is the bounded
    /// inter-stage channel, not a pause signal.
    pub fn should_spill_self(&self) -> bool {
        self.observe();
        self.peak_rss.load(Ordering::Relaxed) > self.soft_limit()
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

    /// Register a consumer with the arbitrator. Returns a fresh
    /// `ConsumerId` the operator records for later
    /// `unregister_consumer` calls. The only path that adds a
    /// contributor to the arbitrator's policy registry.
    ///
    /// Clones the current snapshot Vec, appends the new entry, and
    /// atomically swaps it in. `O(N)` in the registry size, but
    /// registration is a once-per-operator-lifetime event, so the cost
    /// is off the per-batch hot path readers traverse.
    pub fn register_consumer(&self, consumer: Arc<dyn MemoryConsumer>) -> ConsumerId {
        let id = ConsumerId(self.next_consumer_id.fetch_add(1, Ordering::Relaxed));
        self.consumers.rcu(|current| {
            let mut next = Vec::with_capacity(current.len() + 1);
            next.extend(current.iter().cloned());
            next.push((id, Arc::clone(&consumer)));
            next
        });
        id
    }

    /// Remove a previously-registered consumer from the policy
    /// registry. Returns the consumer if `id` was registered, `None`
    /// otherwise. Operators call this at teardown when their state has
    /// been fully drained and the wrapper is no longer a meaningful
    /// spill victim.
    ///
    /// Clones the snapshot minus the removed entry and swaps it in.
    /// The `id` lookup is a linear scan, acceptable because the
    /// registry stays small (one entry per spill-capable operator) and
    /// unregister is a rare teardown event.
    pub fn unregister_consumer(&self, id: ConsumerId) -> Option<Arc<dyn MemoryConsumer>> {
        let mut removed = None;
        self.consumers.rcu(|current| {
            removed = current
                .iter()
                .find(|(cid, _)| *cid == id)
                .map(|(_, c)| Arc::clone(c));
            current
                .iter()
                .filter(|(cid, _)| *cid != id)
                .cloned()
                .collect::<Vec<_>>()
        });
        removed
    }

    /// Number of consumers currently registered. Diagnostics surface
    /// for `--explain` and integration tests; per-consumer attribution
    /// reads `current_usage()` via `sum_consumer_usage`.
    pub fn consumer_count(&self) -> usize {
        self.consumers.load().len()
    }

    /// Sum of `current_usage()` across every registered consumer.
    /// Pull-mode attribution: only the operator knows which of its
    /// bytes are reclaimable right now (a grace-hash with on-disk
    /// partitions has held bytes ≠ reclaimable bytes). The Spark /
    /// Velox / DataFusion memory pools converged on this shape for
    /// victim selection because no central counter can represent the
    /// distinction.
    ///
    /// Reads the current snapshot lock-free.
    pub fn sum_consumer_usage(&self) -> u64 {
        self.consumers
            .load()
            .iter()
            .map(|(_, c)| c.current_usage())
            .sum()
    }

    /// Sample `sum_consumer_usage()` and raise the running peak to it.
    /// Called at every streaming per-batch charge so the peak reflects
    /// the largest charged footprint a streaming stage ever held in
    /// flight. Lock-free `fetch_max` over the freshly summed snapshot.
    pub fn sample_peak_consumer_usage(&self) {
        let now = self.sum_consumer_usage();
        self.peak_consumer_usage.fetch_max(now, Ordering::Relaxed);
    }

    /// High-water mark of `sum_consumer_usage()` observed across the run,
    /// or `0` if no streaming charge ever sampled it. A streaming stage's
    /// peak stays bounded to one in-flight batch (plus the channel's
    /// bound), proving the per-batch admit/discharge model never charges
    /// the whole stage at once.
    pub fn peak_consumer_usage(&self) -> u64 {
        self.peak_consumer_usage.load(Ordering::Relaxed)
    }

    /// Choose which of several currently-runnable nodes to dispatch next,
    /// preferring the node whose predicted peak fits the remaining
    /// headroom under the soft limit so a run stays inside its budget
    /// instead of forcing a spill.
    ///
    /// `runnable` is the set of nodes with no unsatisfied dependencies;
    /// `hint` supplies each node's predicted bytes plus its stable
    /// topological position. The slice is borrowed read-only — the
    /// method never reorders or mutates it.
    ///
    /// Selection order (each tier breaks the previous tier's ties):
    ///
    /// 1. **Headroom fit.** Compute `remaining = soft_limit() -
    ///    sum_consumer_usage()`, clamped at `0`. A candidate "fits" when
    ///    its `predicted_peak_bytes <= remaining`. Fitting candidates are
    ///    preferred over non-fitting ones. If NONE fit, every candidate
    ///    stays eligible — the method always returns a node, never
    ///    nothing, because the executor must make progress even when the
    ///    cheapest runnable node already overflows the budget (it will
    ///    spill, which is correct behavior, not a reason to stall).
    /// 2. **Largest freed-on-complete.** Among the preferred set, pick the
    ///    node predicted to release the most memory when it drains, since
    ///    finishing it soonest reclaims the most headroom downstream.
    /// 3. **Stable index.** Remaining ties break to the lowest
    ///    `hint.stable_index(id)` — the node's position in the plan's
    ///    topological order. This is the deterministic floor: when every
    ///    candidate's `predicted_peak_bytes` is `0` (no volume estimates),
    ///    tiers 1 and 2 are all-equal, so the lowest-stable-index node
    ///    wins, which is exactly the node a plain front-to-back topo walk
    ///    would dispatch first. That is the "no-estimates == today's
    ///    order" invariant: absent statistics, scheduling order is
    ///    byte-for-byte today's order.
    ///
    /// Headroom is recomputed fresh on every call (never cached) because
    /// `sum_consumer_usage()` is a lock-free snapshot that a concurrent
    /// register / unregister can move between calls — the same benign
    /// race `poll_arbitration` already tolerates.
    ///
    /// # Panics
    ///
    /// Panics if `runnable` is empty. The caller dispatches only when at
    /// least one node is runnable, so an empty set is a caller bug, not a
    /// reachable runtime state.
    pub fn next_runnable(&self, runnable: &[NodeIndex], hint: &dyn SchedulingHint) -> NodeIndex {
        assert!(
            !runnable.is_empty(),
            "next_runnable called with an empty runnable set"
        );

        let remaining_headroom = self.soft_limit().saturating_sub(self.sum_consumer_usage());

        // Rank key per candidate, smaller is better:
        //   (0 if fits else 1, -immediate_freed, -subtree_reclaim, stable_index)
        // `Reverse` on each freed term flips max-freed to a min on the sort
        // key so the whole comparison is a single ascending min over a tuple.
        // `predicted_peak_bytes == 0` (unknown) is always <= headroom, so
        // an unknown node fits — it never trips the filter and falls
        // straight through to the stable-index floor, preserving today's
        // order when no estimates exist.
        //
        // The two freed terms are ordered immediate-before-subtree on
        // purpose: a node that reclaims headroom the instant it completes
        // (a ready blocking operator) is always taken over one that only
        // unlocks an eventual reclaim (a fresh Source whose downstream
        // Aggregate will drain later). Draining the ready operator first is
        // the peak-minimizing greedy choice; the subtree term then breaks
        // ties only among candidates with equal immediate reclaim — the
        // fresh Sources of independent chains — front-loading the chain
        // whose completion frees the most. With no estimates both terms are
        // `0` for every candidate, so selection still collapses to the
        // stable-index floor.
        runnable
            .iter()
            .copied()
            .min_by_key(|&id| {
                let fits = hint.predicted_peak_bytes(id) <= remaining_headroom;
                (
                    u8::from(!fits),
                    std::cmp::Reverse(hint.predicted_freed_bytes_on_complete(id)),
                    std::cmp::Reverse(hint.predicted_subtree_reclaim_bytes(id)),
                    hint.stable_index(id),
                )
            })
            // `runnable` is non-empty (asserted above), so `min_by_key`
            // always yields a node.
            .expect("non-empty runnable set yields a selection")
    }

    /// Run one arbitration round and return the elected victim, if
    /// any. Used internally by `should_spill`. Loads the current
    /// consumer snapshot lock-free, computes the policy decision, and
    /// emits a `tracing::warn` when peak RSS and the sum of registered
    /// consumer usages disagree by more than 10% of the configured
    /// limit — useful signal that allocator overhead or fragmentation
    /// is meaningful, never a failure.
    ///
    /// Warning is suppressed when the consumer registry is empty: no
    /// pull-mode attribution means no signal to compare against and
    /// the disagreement is structurally guaranteed.
    fn poll_arbitration(&self) -> Option<ConsumerId> {
        let peak_rss = self.peak_rss.load(Ordering::Relaxed);
        let pressure = peak_rss.saturating_sub(self.soft_limit());
        let limit = self.limit.load(Ordering::Relaxed);
        // The loaded guard pins the snapshot Vec for the whole round,
        // so the elected victim's `Arc` stays alive even if a
        // concurrent unregister swaps in a new snapshot mid-round.
        let consumers = self.consumers.load();
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
        let victim = self.policy.select_victim(&snapshot, pressure);
        // Round-trip: invoke the corresponding action on the elected
        // victim through the shared `&` the snapshot provides. No
        // exclusive access is needed — every consumer routes its
        // mutation through atomics behind a shared handle.
        if let Some(id) = victim
            && let Some((_, consumer)) = consumers.iter().find(|(cid, _)| *cid == id)
        {
            if consumer.can_back_pressure() {
                consumer.pause();
            } else {
                let _ = consumer.try_spill(pressure);
            }
        }
        victim
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
    fn pause_signal_fast_path_when_not_paused() {
        let signal = PauseSignal::new();
        assert!(!signal.is_paused());
        // Should return immediately without acquiring the mutex.
        signal.wait_while_paused();
    }

    #[test]
    fn pause_signal_blocks_until_resume() {
        let signal = Arc::new(PauseSignal::new());
        signal.pause();
        assert!(signal.is_paused());

        let producer = signal.clone();
        let handle = std::thread::spawn(move || {
            // Block until the main thread calls resume.
            producer.wait_while_paused();
        });

        // Wait briefly to ensure the producer has parked on the Condvar.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!handle.is_finished(), "producer should still be parked");

        signal.resume();
        // Producer should wake within reasonable time and finish.
        handle.join().expect("producer thread should join cleanly");
        assert!(!signal.is_paused());
    }

    #[test]
    fn consumer_handle_pause_routes_through_pause_signal() {
        let handle = ConsumerHandle::new();
        assert!(!handle.is_paused());
        handle.pause();
        assert!(handle.is_paused());
        handle.resume();
        assert!(!handle.is_paused());
    }

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

    /// Minimal `MemoryConsumer` used to exercise the trait surface
    /// from inside the crate. Mirrors the production wrappers' interior
    /// mutability — `usage` and `paused` live behind atomics so the
    /// `&self` trait methods can mutate them without exclusive access.
    struct MockConsumer {
        usage: AtomicU64,
        priority: i32,
        paused: AtomicBool,
        spill_response: u64,
    }

    impl MockConsumer {
        fn new(usage: u64, priority: i32, spill_response: u64) -> Self {
            Self {
                usage: AtomicU64::new(usage),
                priority,
                paused: AtomicBool::new(false),
                spill_response,
            }
        }

        fn is_paused(&self) -> bool {
            self.paused.load(Ordering::Acquire)
        }
    }

    impl MemoryConsumer for MockConsumer {
        fn current_usage(&self) -> u64 {
            self.usage.load(Ordering::Acquire)
        }
        fn spill_priority(&self) -> i32 {
            self.priority
        }
        fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
            let freed = self.spill_response.min(target_bytes);
            self.usage.fetch_sub(
                freed.min(self.usage.load(Ordering::Acquire)),
                Ordering::AcqRel,
            );
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
        fn pause(&self) {
            self.paused.store(true, Ordering::Release);
        }
        fn resume(&self) {
            self.paused.store(false, Ordering::Release);
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
        let consumer = MockConsumer::new(4096, 0, 4096);
        let id = ConsumerId(0);
        let consumer_ref: &dyn MemoryConsumer = &consumer;
        assert!(policy.select_victim(&[(id, consumer_ref)], 1024).is_none());
        // Touch every other trait method so the mock is fully exercised.
        assert_eq!(consumer.current_usage(), 4096);
        assert_eq!(consumer.spill_priority(), 0);
        assert!(consumer.can_back_pressure());
        consumer.pause();
        assert!(consumer.is_paused());
        consumer.resume();
        assert!(!consumer.is_paused());
        assert!(consumer.try_spill(4096).is_ok());
        let short = MockConsumer::new(1024, 10, 100);
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
        let id_a = arbitrator.register_consumer(Arc::new(MockConsumer::new(10, 0, 0)));
        let id_b = arbitrator.register_consumer(Arc::new(MockConsumer::new(20, 0, 0)));
        let id_c = arbitrator.register_consumer(Arc::new(MockConsumer::new(30, 0, 0)));
        assert_ne!(id_a, id_b);
        assert_ne!(id_b, id_c);
        assert_ne!(id_a, id_c);
        assert_eq!(arbitrator.consumer_count(), 3);
    }

    #[test]
    fn test_unregister_consumer_returns_removed() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id = arbitrator.register_consumer(Arc::new(MockConsumer::new(100, 0, 0)));
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
        arbitrator.register_consumer(Arc::new(MockConsumer::new(1024, 0, 0)));
        arbitrator.register_consumer(Arc::new(MockConsumer::new(4096, 0, 0)));
        arbitrator.register_consumer(Arc::new(MockConsumer::new(100, 0, 0)));
        assert_eq!(arbitrator.sum_consumer_usage(), 1024 + 4096 + 100);
    }

    #[test]
    fn arena_consumer_contributes_to_sum_and_silences_disagreement_warning() {
        use crate::pipeline::arena::ArenaConsumer;

        // A window arena holding ~8 MiB. The arbitrator's RSS reflects
        // exactly that allocation (idealized — no harness slack).
        const ARENA_BYTES: u64 = 8 * 1024 * 1024;
        // 100 GiB hard limit so real-process RSS cannot push peak_rss
        // above the test-seeded value via `observe()`'s `fetch_max`.
        let arbitrator =
            MemoryArbitrator::with_policy(100 * 1024 * 1024 * 1024, 0.50, Box::new(NoOpPolicy));
        arbitrator.set_peak_rss_for_test(ARENA_BYTES);

        // Before registration the arena contributes nothing, so the
        // pull-mode picture disagrees with RSS by the full arena size —
        // exactly the false-positive the registration closes.
        assert_eq!(arbitrator.sum_consumer_usage(), 0);
        let tenth = arbitrator.limit() / 10;
        assert!(
            ARENA_BYTES.abs_diff(arbitrator.sum_consumer_usage()) <= tenth,
            "sanity: 8 MiB is within 10% of a 100 GiB limit even unregistered"
        );

        // Register the arena exactly as `finalize_node_rooted_windows`
        // does: a fresh handle seeded to the arena's measured bytes.
        let handle = ConsumerHandle::new();
        handle.set_bytes(ARENA_BYTES);
        let id = arbitrator.register_consumer(Arc::new(ArenaConsumer::new(handle.clone())));

        // Attribution: the arena's bytes now flow through pull-mode.
        assert_eq!(arbitrator.sum_consumer_usage(), ARENA_BYTES);
        assert_eq!(arbitrator.consumer_count(), 1);

        // Disagreement-warning silence: the charged sum reaches parity
        // with peak RSS, so `peak_rss.abs_diff(charged_sum)` is zero —
        // far under the 10%-of-limit threshold `poll_arbitration` warns
        // on. The arena is the only sizable in-memory state.
        let charged_sum = arbitrator.sum_consumer_usage();
        let peak_rss = arbitrator.peak_rss().unwrap();
        assert_eq!(
            peak_rss.abs_diff(charged_sum),
            0,
            "registered arena makes charged bytes match RSS — no disagreement"
        );

        // Teardown leaves the registry empty (the executor's top-scope
        // and body-scope drains unregister exactly this way).
        arbitrator.unregister_consumer(id);
        assert_eq!(arbitrator.consumer_count(), 0);
        assert_eq!(arbitrator.sum_consumer_usage(), 0);
    }

    #[test]
    fn arena_consumer_ranks_last_among_spill_victims() {
        use crate::pipeline::arena::ArenaConsumer;

        // A non-actionable arena (can't pause, can't free) must never be
        // preferred over a spillable consumer. Under `Priority`, lower
        // spill_priority wins; the arena's `i32::MAX - 1` sits behind a
        // grace-hash-shaped consumer at priority 10, so the policy elects
        // the spillable. `should_spill` drives the round and acts on the
        // victim — a back-pressureable MockConsumer gets paused, which is
        // the observable proof it (not the arena) was elected.
        // 100 GiB hard limit so real-process RSS cannot push peak_rss
        // above the test-seeded value via `observe()`'s `fetch_max`.
        let arbitrator =
            MemoryArbitrator::with_policy(100 * 1024 * 1024 * 1024, 0.50, Box::new(Priority));
        let arena_handle = ConsumerHandle::new();
        arena_handle.set_bytes(512 * 1024 * 1024);
        // The arena is far larger than the spillable; only the priority
        // ordering keeps it from being elected, which is the point.
        arbitrator.register_consumer(Arc::new(ArenaConsumer::new(arena_handle)));
        let spillable = Arc::new(MockConsumer::new(1024, 10, 0));
        arbitrator.register_consumer(spillable.clone());

        arbitrator.set_peak_rss_for_test(75 * 1024 * 1024 * 1024);
        assert!(arbitrator.should_spill());
        assert!(
            spillable.is_paused(),
            "Priority must elect the spillable consumer, never the non-actionable arena"
        );
    }

    #[test]
    fn test_consumer_ids_are_never_reused_after_unregister() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id_a = arbitrator.register_consumer(Arc::new(MockConsumer::new(1, 0, 0)));
        arbitrator.unregister_consumer(id_a);
        let id_b = arbitrator.register_consumer(Arc::new(MockConsumer::new(1, 0, 0)));
        // Monotonic allocator: even after unregister, the next id is
        // strictly above the previous one — stale ConsumerId references
        // cannot collide with a later registration.
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn test_register_then_unregister_leaves_empty_snapshot() {
        let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
        let id = arbitrator.register_consumer(Arc::new(MockConsumer::new(512, 0, 0)));
        assert_eq!(arbitrator.consumer_count(), 1);
        assert_eq!(arbitrator.sum_consumer_usage(), 512);
        let removed = arbitrator.unregister_consumer(id);
        assert!(removed.is_some());
        // The copy-on-write swap leaves an empty Vec behind, not a
        // tombstoned slot: both the count and the pull-mode sum read
        // zero from the fresh snapshot.
        assert_eq!(arbitrator.consumer_count(), 0);
        assert_eq!(arbitrator.sum_consumer_usage(), 0);
    }

    #[test]
    fn test_concurrent_register_during_read_never_tears_snapshot() {
        use std::sync::Barrier;
        use std::thread;

        // A reader thread hammers `sum_consumer_usage` while a writer
        // thread registers fresh consumers. The copy-on-write snapshot
        // guarantees each read observes one complete immutable Vec —
        // never a Vec mid-mutation — so every observed sum is a multiple
        // of the per-consumer usage and no read panics on a torn entry.
        const PER_CONSUMER: u64 = 1024;
        const WRITES: usize = 64;

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            u64::MAX,
            0.80,
            Box::new(NoOpPolicy),
        ));
        let start = Arc::new(Barrier::new(2));

        let reader = {
            let arbitrator = Arc::clone(&arbitrator);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                for _ in 0..10_000 {
                    let sum = arbitrator.sum_consumer_usage();
                    // A torn read would surface as a sum that is not an
                    // exact multiple of the per-consumer charge.
                    assert_eq!(sum % PER_CONSUMER, 0);
                }
            })
        };

        let writer = {
            let arbitrator = Arc::clone(&arbitrator);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                for _ in 0..WRITES {
                    arbitrator.register_consumer(Arc::new(MockConsumer::new(PER_CONSUMER, 0, 0)));
                }
            })
        };

        reader.join().unwrap();
        writer.join().unwrap();
        assert_eq!(arbitrator.consumer_count(), WRITES);
        assert_eq!(
            arbitrator.sum_consumer_usage(),
            PER_CONSUMER * WRITES as u64
        );
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
        fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
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
        let consumer = DefaultPauseConsumer { usage: 42 };
        // Both default bodies are no-ops; they must not panic and
        // must leave the consumer's observable state unchanged.
        consumer.pause();
        assert_eq!(consumer.current_usage(), 42);
        consumer.resume();
        assert_eq!(consumer.current_usage(), 42);
    }

    #[test]
    fn test_largest_first_picks_largest() {
        let small = MockConsumer::new(10, 0, 0);
        let big = MockConsumer::new(50, 0, 0);
        let mid = MockConsumer::new(30, 0, 0);
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
        let a = MockConsumer::new(100, 0, 0);
        let b = MockConsumer::new(100, 0, 0);
        let consumers: Vec<(ConsumerId, &dyn MemoryConsumer)> =
            vec![(ConsumerId(0), &a), (ConsumerId(1), &b)];
        // `max_by_key` returns the last equally-maximum element; the
        // policy inherits that contract. The arbitrator's snapshot
        // preserves registration order, so insertion order isn't
        // load-bearing here — what matters is the selection is
        // deterministic given a fixed input slice.
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
        let high = MockConsumer::new(100, 10, 0);
        let low = MockConsumer::new(100, 1, 0);
        let mid = MockConsumer::new(100, 5, 0);
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
        let small = MockConsumer::new(10, 5, 0);
        let big = MockConsumer::new(90, 5, 0);
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
        fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
        fn pause(&self) {}
        fn resume(&self) {}
    }

    #[test]
    fn test_back_pressure_preferred_picks_back_pressureable() {
        let unpausable = UnpausableConsumer {
            usage: 1000,
            priority: 0,
        };
        // MockConsumer.can_back_pressure() returns true.
        let pausable = MockConsumer::new(10, 99, 0);
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

    /// In-memory [`SchedulingHint`] for the `next_runnable` gate tests.
    /// Each `NodeIndex` maps to `(predicted_peak, predicted_freed,
    /// predicted_subtree_reclaim, stable_index)`. An index absent from the
    /// map answers `(0, 0, 0, usize::MAX)`, mirroring how the production
    /// `ExecutionPlanDag` impl treats a node missing from `node_properties`
    /// / `topo_order`.
    struct MockHint {
        entries: std::collections::HashMap<NodeIndex, (u64, u64, u64, usize)>,
    }

    impl MockHint {
        fn new(entries: &[(NodeIndex, u64, u64, u64, usize)]) -> Self {
            Self {
                entries: entries
                    .iter()
                    .map(|&(id, peak, freed, subtree, idx)| (id, (peak, freed, subtree, idx)))
                    .collect(),
            }
        }
    }

    impl SchedulingHint for MockHint {
        fn predicted_peak_bytes(&self, id: NodeIndex) -> u64 {
            self.entries.get(&id).map(|&(p, _, _, _)| p).unwrap_or(0)
        }
        fn predicted_freed_bytes_on_complete(&self, id: NodeIndex) -> u64 {
            self.entries.get(&id).map(|&(_, f, _, _)| f).unwrap_or(0)
        }
        fn predicted_subtree_reclaim_bytes(&self, id: NodeIndex) -> u64 {
            self.entries.get(&id).map(|&(_, _, s, _)| s).unwrap_or(0)
        }
        fn stable_index(&self, id: NodeIndex) -> usize {
            self.entries
                .get(&id)
                .map(|&(_, _, _, s)| s)
                .unwrap_or(usize::MAX)
        }
    }

    /// Arbitrator whose soft limit is exactly `soft` bytes (hard limit
    /// `soft / 0.80`) and whose current consumer usage is exactly `used`
    /// bytes, so `next_runnable`'s remaining headroom is `soft - used`.
    /// `NoOpPolicy` because `next_runnable` never consults the
    /// arbitration policy.
    fn arbitrator_with_headroom(soft: u64, used: u64) -> MemoryArbitrator {
        // soft_limit() = limit * 0.80, so set limit = soft / 0.80.
        let limit = (soft as f64 / 0.80) as u64;
        let arb = MemoryArbitrator::with_policy(limit, 0.80, Box::new(NoOpPolicy));
        assert_eq!(arb.soft_limit(), soft, "soft limit setup");
        if used > 0 {
            arb.register_consumer(Arc::new(MockConsumer::new(used, 0, 0)));
        }
        assert_eq!(arb.sum_consumer_usage(), used, "usage setup");
        arb
    }

    #[test]
    fn next_runnable_prefers_headroom_fit_over_larger_freed() {
        // Soft limit 800, 300 already used => remaining headroom 500.
        let arb = arbitrator_with_headroom(800, 300);
        let fits = NodeIndex::new(0);
        let overflows = NodeIndex::new(1);
        // `overflows` would free far more, but its peak (600) exceeds the
        // 500 headroom; `fits` (peak 400) fits. Tier 1 must win over the
        // tier-2 freed-bytes pull.
        let hint = MockHint::new(&[(fits, 400, 0, 0, 0), (overflows, 600, 10_000, 0, 1)]);
        let chosen = arb.next_runnable(&[fits, overflows], &hint);
        assert_eq!(chosen, fits, "fitting node must beat a non-fitting one");
        // Order-independent: same result when the slice lists them reversed.
        let chosen_rev = arb.next_runnable(&[overflows, fits], &hint);
        assert_eq!(chosen_rev, fits);
    }

    #[test]
    fn next_runnable_breaks_fit_tie_by_largest_freed() {
        // Remaining headroom 500; both candidates fit (peaks 100 / 200).
        let arb = arbitrator_with_headroom(800, 300);
        let small_freed = NodeIndex::new(0);
        let big_freed = NodeIndex::new(1);
        let hint = MockHint::new(&[(small_freed, 100, 50, 0, 0), (big_freed, 200, 400, 0, 1)]);
        let chosen = arb.next_runnable(&[small_freed, big_freed], &hint);
        assert_eq!(
            chosen, big_freed,
            "among fitting candidates, the larger freed-on-complete wins"
        );
        let chosen_rev = arb.next_runnable(&[big_freed, small_freed], &hint);
        assert_eq!(chosen_rev, big_freed);
    }

    #[test]
    fn next_runnable_breaks_freed_tie_by_larger_subtree_reclaim() {
        // Remaining headroom 500; both candidates fit and free nothing the
        // instant they complete (immediate freed 0 — the shape of two fresh
        // Sources feeding independent blocking chains). Only the downstream
        // subtree reclaim distinguishes them: the candidate whose chain
        // unlocks the larger eventual reclaim must win, so the scheduler
        // front-loads the heavier chain.
        let arb = arbitrator_with_headroom(800, 300);
        let light_chain_src = NodeIndex::new(0);
        let heavy_chain_src = NodeIndex::new(1);
        // light has the LOWER stable index, so a subtree-blind selection
        // would take it; the heavier subtree reclaim must override that.
        let hint = MockHint::new(&[
            (light_chain_src, 100, 0, 1_000, 0),
            (heavy_chain_src, 200, 0, 9_000, 1),
        ]);
        let chosen = arb.next_runnable(&[light_chain_src, heavy_chain_src], &hint);
        assert_eq!(
            chosen, heavy_chain_src,
            "with equal immediate freed, the larger downstream subtree reclaim wins, \
             front-loading the heavier independent chain even though it sorts later"
        );
        let chosen_rev = arb.next_runnable(&[heavy_chain_src, light_chain_src], &hint);
        assert_eq!(chosen_rev, heavy_chain_src);
    }

    #[test]
    fn next_runnable_prefers_immediate_freed_over_larger_subtree_reclaim() {
        // A ready blocking operator (immediate freed > 0) competes against a
        // fresh Source whose chain unlocks a far larger eventual reclaim
        // (subtree reclaim 50_000, immediate 0). Draining the ready operator
        // reclaims headroom NOW, which is the peak-minimizing choice; the
        // larger-but-eventual subtree reclaim must not pull selection toward
        // the fresh Source. This is the tier ordering that keeps the
        // "drain a ready aggregate before charging the next source" behavior
        // intact once subtree reclaim is propagated up to Sources.
        let arb = arbitrator_with_headroom(800, 300);
        let ready_blocking = NodeIndex::new(0);
        let fresh_source = NodeIndex::new(1);
        let hint = MockHint::new(&[
            (ready_blocking, 100, 400, 400, 0),
            (fresh_source, 200, 0, 50_000, 1),
        ]);
        let chosen = arb.next_runnable(&[ready_blocking, fresh_source], &hint);
        assert_eq!(
            chosen, ready_blocking,
            "immediate freed (drain-now reclaim) must outrank a larger eventual subtree reclaim"
        );
        let chosen_rev = arb.next_runnable(&[fresh_source, ready_blocking], &hint);
        assert_eq!(chosen_rev, ready_blocking);
    }

    #[test]
    fn next_runnable_breaks_remaining_tie_by_stable_index() {
        // Remaining headroom 500; all three fit and free the same amount,
        // so only the stable index distinguishes them.
        let arb = arbitrator_with_headroom(800, 300);
        let low = NodeIndex::new(7);
        let mid = NodeIndex::new(3);
        let high = NodeIndex::new(5);
        // stable_index ordering (low=2, mid=4, high=9) is deliberately
        // unrelated to the NodeIndex values, proving the tiebreak keys off
        // the hint's stable index (topo position) and not the raw index.
        let hint = MockHint::new(&[
            (low, 100, 200, 0, 2),
            (mid, 100, 200, 0, 4),
            (high, 100, 200, 0, 9),
        ]);
        let chosen = arb.next_runnable(&[high, mid, low], &hint);
        assert_eq!(
            chosen, low,
            "equal fit + equal freed must break to the lowest stable index"
        );
        // Slice order does not change the answer.
        let chosen_alt = arb.next_runnable(&[low, mid, high], &hint);
        assert_eq!(chosen_alt, low);
    }

    #[test]
    fn next_runnable_all_zero_estimates_returns_lowest_stable_index() {
        // No volume estimates: every candidate predicts peak 0 / freed 0.
        // The method must reproduce today's lowest-index / topo order
        // exactly — the load-bearing "no statistics == unchanged behavior"
        // invariant. Headroom value is irrelevant because peak 0 always
        // fits; use a tight headroom to prove it.
        let arb = arbitrator_with_headroom(800, 790);
        let a = NodeIndex::new(11);
        let b = NodeIndex::new(2);
        let c = NodeIndex::new(8);
        // stable_index = topo position; a is first (0), c second (1),
        // b last (2).
        let hint = MockHint::new(&[(a, 0, 0, 0, 0), (c, 0, 0, 0, 1), (b, 0, 0, 0, 2)]);
        // Pass the slice in an order that does NOT match topo position to
        // prove the result is the lowest stable index, not the first slot.
        let chosen = arb.next_runnable(&[b, c, a], &hint);
        assert_eq!(
            chosen, a,
            "with all-zero estimates the lowest stable index (topo position 0) wins"
        );
        let chosen_again = arb.next_runnable(&[c, a, b], &hint);
        assert_eq!(chosen_again, a, "selection is independent of slice order");
    }
}
