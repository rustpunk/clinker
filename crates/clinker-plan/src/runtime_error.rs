//! Runtime-failure vocabulary the top-level [`PipelineError`](crate::error::PipelineError)
//! aggregates.
//!
//! [`SpillError`] is a leaf enum produced by the execution engine's
//! disk-spill subsystem, but it is defined here, alongside the error type
//! that names it, so the planning layer can own the unified `PipelineError`
//! without depending upward on the executor. [`MemorySurface`] and
//! [`ConsumerLabel`] name the holders of charged memory in author vocabulary
//! for the same reason, and [`MemoryShortfallReport`] is the E310 report built
//! from them.

/// Disk-spill I/O or decode failure.
///
/// Surfaced by the spill reader/writer in the execution layer. The decode
/// variants preserve the underlying postcard / JSON-header context that a
/// bare [`std::io::Error`] would lose.
#[derive(Debug)]
pub enum SpillError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Postcard(postcard::Error),
    InvalidSchema(String),
    /// The spill root directory became unusable mid-run: it was removed,
    /// unmounted, remounted read-only, or had its permissions revoked after
    /// the run validated it at startup. Distinct from [`SpillError::Io`] so
    /// the rendered diagnostic points at the directory and its likely cause
    /// (an NFS remount, a volume unmount, an over-eager temp-file cleaner)
    /// rather than reading as a generic byte-stream I/O failure. Carries the
    /// offending directory path and the underlying OS message.
    DirUnavailable {
        dir: String,
        source: String,
    },
    /// E321 — a spill write failed because the spill volume ran out of
    /// space (`std::io::ErrorKind::StorageFull`, i.e. `ENOSPC`). Distinct
    /// from both [`SpillError::Io`] (so the rendered diagnostic names the
    /// volume and the disk-full cause rather than reading as a generic
    /// byte-stream fault) and from the cap-exceeded surface
    /// (`PipelineError::SpillCapExceeded`): the disk physically filled,
    /// the run did not merely cross its configured spill quota. Keeping
    /// the two apart is the point of duckdb/duckdb#14142, where a cap hit
    /// rendered as an out-of-memory message and operators inspected `df`
    /// only to find free space. Carries the offending directory path and
    /// the underlying OS message.
    DiskFull {
        dir: String,
        source: String,
    },
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Io(e) => write!(f, "spill I/O error: {e}"),
            SpillError::Json(e) => write!(f, "spill JSON header error: {e}"),
            SpillError::Postcard(e) => write!(f, "spill postcard error: {e}"),
            SpillError::InvalidSchema(msg) => write!(f, "spill schema error: {msg}"),
            SpillError::DirUnavailable { dir, source } => write!(
                f,
                "spill directory {dir} became unavailable mid-run: {source} \
                 (the directory may have been unmounted, remounted read-only, \
                 deleted by an external cleaner, or had its permissions revoked)"
            ),
            SpillError::DiskFull { dir, source } => write!(
                f,
                "E321 spill volume at {dir} is out of space: {source} \
                 (the disk physically filled — this is not the configured \
                 spill cap and not an out-of-memory condition; free space on \
                 the volume or point storage.spill.dir at a larger one)"
            ),
        }
    }
}

impl SpillError {
    /// Classify an [`std::io::Error`] raised while creating a spill file in
    /// the spill root directory.
    ///
    /// A failure to create or write a spill file in a directory the run
    /// validated as writable at startup falls into three buckets, each with
    /// its own diagnostic so the operator's remediation is unambiguous:
    ///
    /// - The directory itself went bad mid-run (`NotFound` →
    ///   removed/unmounted, `PermissionDenied`/`ReadOnlyFilesystem` →
    ///   permissions revoked or read-only remount) → [`SpillError::DirUnavailable`].
    /// - The volume ran out of space (`StorageFull`, i.e. `ENOSPC`) →
    ///   [`SpillError::DiskFull`], kept distinct from the configured spill
    ///   cap so a full disk never renders as a cap-exceeded or OOM message.
    /// - Any other kind (a genuine byte-stream fault, a short write) stays
    ///   [`SpillError::Io`].
    pub fn from_spill_dir_io(dir: &std::path::Path, e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
                SpillError::DirUnavailable {
                    dir: dir.display().to_string(),
                    source: e.to_string(),
                }
            }
            ErrorKind::StorageFull => SpillError::DiskFull {
                dir: dir.display().to_string(),
                source: e.to_string(),
            },
            _ => SpillError::Io(e),
        }
    }
}

impl std::error::Error for SpillError {}

impl From<std::io::Error> for SpillError {
    fn from(e: std::io::Error) -> Self {
        SpillError::Io(e)
    }
}

impl From<serde_json::Error> for SpillError {
    fn from(e: serde_json::Error) -> Self {
        SpillError::Json(e)
    }
}

impl From<postcard::Error> for SpillError {
    fn from(e: postcard::Error) -> Self {
        SpillError::Postcard(e)
    }
}

impl From<lz4_flex::frame::Error> for SpillError {
    fn from(e: lz4_flex::frame::Error) -> Self {
        SpillError::Io(std::io::Error::other(e.to_string()))
    }
}

/// What a piece of charged memory holds, named in the words a pipeline author
/// uses for their own pipeline.
///
/// Memory diagnostics name the holder of every charged byte by its node and
/// one of these surfaces, so the rendered text never exposes the engine's own
/// machinery. Closed: a new kind of retained state adds a variant here together
/// with its author-facing wording.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MemorySurface {
    /// Records a Source has read and not yet handed on.
    RowsRead,
    /// Rows waiting between two nodes, named by their author-given names.
    BufferedRows { from: String, to: String },
    /// Per-group accumulators of an Aggregate.
    GroupState,
    /// Rows collected for sorting.
    SortBuffer,
    /// The build side a join holds while it matches the probe side.
    JoinBuildSide,
    /// Other state a join holds between records.
    JoinState,
    /// The failing rows of a failed document, held until the document is
    /// dead-lettered.
    HeldFailingRows,
    /// The rows of a document an Output holds until the run knows whether
    /// the document failed: under document-granularity dead-lettering a
    /// document's rows are written only once it is known not to have failed.
    OpenDocumentRows,
    /// The run's record of rows already dead-lettered, kept so a row several
    /// Sinks hold is dead-lettered once.
    DeadLetteredRowSet,
    /// State a routing or filtering decision keeps between records, such as
    /// Cull's per-group drop decisions.
    DecisionState,
    /// Rows held while Reshape groups complete.
    ReshapeGroups,
    /// Rows held while Cull groups complete.
    CullGroups,
    /// The index a window reads its neighbouring rows through.
    WindowIndex,
    /// Rows collected so a node can scan all of them.
    ScanMaterialization,
    /// Output bytes staged before they are written.
    OutputStaging,
    /// Credentials resolved for the run.
    CredentialRegistry,
    /// Rows held until their correlation group commits.
    CorrelationGroups,
    /// Rows parked between two regions of the pipeline until they commit.
    ParkedCrossRegionRows { from: String, to: String },
}

impl std::fmt::Display for MemorySurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use clinker_core_types::QuoteName;
        match self {
            Self::RowsRead => f.write_str("rows read from the source"),
            Self::BufferedRows { from, to } => {
                write!(
                    f,
                    "rows buffered between {} and {}",
                    from.as_str().quoted_name(),
                    to.as_str().quoted_name()
                )
            }
            Self::GroupState => f.write_str("group state"),
            Self::SortBuffer => f.write_str("sort buffer"),
            Self::JoinBuildSide => f.write_str("join build side"),
            Self::JoinState => f.write_str("join state"),
            Self::HeldFailingRows => f.write_str("held failing rows of a failed document"),
            Self::OpenDocumentRows => f.write_str("rows held until their document is decided"),
            Self::DeadLetteredRowSet => f.write_str("set of rows already dead-lettered"),
            Self::DecisionState => f.write_str("decision state"),
            Self::ReshapeGroups => f.write_str("rows held for Reshape groups"),
            Self::CullGroups => f.write_str("rows held for Cull groups"),
            Self::WindowIndex => f.write_str("window index"),
            Self::ScanMaterialization => f.write_str("rows collected for a full scan"),
            Self::OutputStaging => f.write_str("output staging"),
            Self::CredentialRegistry => f.write_str("credential registry"),
            Self::CorrelationGroups => f.write_str("rows held for correlation groups"),
            Self::ParkedCrossRegionRows { from, to } => {
                write!(
                    f,
                    "rows held between {} and {} for commit",
                    from.as_str().quoted_name(),
                    to.as_str().quoted_name()
                )
            }
        }
    }
}

/// Who holds a piece of charged memory: the author-given name of the node that
/// owns it and what it is.
///
/// Run-scoped state that no single node owns carries the run-level surface it
/// serves (output staging, the credential registry).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerLabel {
    pub node: String,
    pub surface: MemorySurface,
}

/// Why a request for memory was refused, as the E310 diagnostic reports it.
///
/// Every byte figure comes from one reading of the run's memory ledger taken
/// at the refusal, so `holders`, `other_holders_bytes` and
/// `unattributed_bytes` add up to `charged_bytes` exactly and the suggested
/// limit is derived from the same charged total. The holder states and the
/// private-memory figure are read just after that reading (see each field).
/// The report names nodes, surfaces and byte counts only; it never carries a
/// record value.
///
/// Its [`Display`](std::fmt::Display) is the E310 text: a greppable headline,
/// then the charged total, the largest holders, what the reclaim round did, the
/// smallest limit with room for the request (in both the YAML and the CLI
/// spelling when `memory.limit` was the limit enforced), and a remedy keyed
/// to the largest holder that cannot spill.
/// When [`LimitReading::ProcessMemory`] was the reading over the limit, the
/// headline and the limit line state the process reading instead of a
/// request, and nothing claims the charged state fills the limit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryShortfallReport {
    /// The node that asked for memory and what the memory was for; `None`
    /// when the request was made for the run as a whole and no node is known.
    pub requester: Option<ConsumerLabel>,
    /// Where the group of input rows the request was for begins, when one
    /// group is at fault (a Cull or Reshape group too large to hold whole);
    /// `None` otherwise.
    pub group_first_row: Option<RowPosition>,
    /// An estimate of how many distinct join keys the partition of a join's
    /// build side held, when the join stopped while matching one partition
    /// it could not split further; `None` otherwise. A count, never a key.
    pub join_partition_distinct_keys: Option<u64>,
    /// The reading of the run's memory that was over the limit: the charged
    /// total, or the process's own memory. The headline and the suggested
    /// limit state only what this reading established.
    pub reading: LimitReading,
    /// Bytes the refused request asked for.
    pub requested_bytes: u64,
    /// The limit charges were granted against, named as the run enforced
    /// it: `memory.limit`, or the smaller test capacity a test run was held
    /// to. The headline and the `fix:` line name it.
    pub limit: EnforcedLimit,
    /// Bytes charged to the run when the request was refused.
    pub charged_bytes: u64,
    /// The process's private memory (memory no other process shares),
    /// sampled once while the report was built; `None` where the platform
    /// gives no reading.
    pub private_bytes: Option<u64>,
    /// The largest holders of charged memory, largest first, at most
    /// [`MemoryShortfallReport::LISTED_HOLDERS`].
    pub holders: Vec<HolderReport>,
    /// How many holders did not fit in `holders`.
    pub other_holders_count: u32,
    /// Bytes the holders beyond `holders` hold together.
    pub other_holders_bytes: u64,
    /// Charged bytes no single node holds: memory the run holds as a whole,
    /// such as output staging.
    pub unattributed_bytes: u64,
    /// Charged bytes no spill could free: what the holders that cannot spill
    /// hold (listed or not), plus `unattributed_bytes`.
    pub unspillable_bytes: u64,
    /// What the reclaim round did before the refusal; `None` when the request
    /// was refused without one: a request made where the run's state cannot
    /// be spilled, or a check that refuses without a round. The one record
    /// of whether a round ran: the headline says nothing more could be
    /// spilled only when this holds a round.
    pub reclaim: Option<ReclaimReport>,
    /// The smallest limit with room for this request and what was charged:
    /// `charged_bytes + requested_bytes` rounded up to a whole MiB (see
    /// [`suggested_limit_floor`]). A floor, not a recommendation: later
    /// stages may need more.
    pub suggested_limit_bytes: u64,
    /// No spill could make the request fit: it is larger than the limit on
    /// its own, or larger than what is left of it beside
    /// `unspillable_bytes`.
    pub oversized: bool,
}

impl MemoryShortfallReport {
    /// Holders the report lists by name; the rest are summed on one line.
    pub const LISTED_HOLDERS: usize = 5;

    /// Name `node` and `surface` as the requester of a report that names
    /// none. A report that already names its requester keeps it: the name
    /// the ledger recorded at the refusal outranks one supplied afterwards
    /// by the code the refusal was propagated through.
    pub fn attribute_if_unnamed(&mut self, node: &str, surface: MemorySurface) {
        if self.requester.is_none() {
            self.requester = Some(ConsumerLabel {
                node: node.to_string(),
                surface,
            });
        }
    }
}

/// The limit a run's charges were granted against, and which setting it is.
///
/// Recorded where the limit is installed, never inferred afterwards by
/// comparing figures, so a report cannot name a limit the run did not
/// enforce.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnforcedLimit {
    /// `memory.limit`: the configured limit, or the default when none is
    /// configured.
    MemoryLimit(u64),
    /// A ledger capacity below `memory.limit` that a test, or a debug build
    /// run with a test capacity, held the run to. No pipeline author's run
    /// enforces one.
    TestCapacity(u64),
}

impl EnforcedLimit {
    /// The limit in bytes.
    pub fn bytes(self) -> u64 {
        match self {
            Self::MemoryLimit(bytes) | Self::TestCapacity(bytes) => bytes,
        }
    }
}

/// The limit named with its figure, as the E310 headline names it:
/// `memory.limit 8.0 MiB` or `the test ledger capacity 8.0 MiB`.
impl std::fmt::Display for EnforcedLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryLimit(bytes) => write!(f, "memory.limit {}", Bytes(*bytes)),
            Self::TestCapacity(bytes) => write!(f, "the test ledger capacity {}", Bytes(*bytes)),
        }
    }
}

/// Which reading of the run's memory a [`MemoryShortfallReport`] found over
/// the limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitReading {
    /// The charged total: the request did not fit beside what the run had
    /// charged. `requested_bytes` is the request.
    Charged,
    /// The process's memory as the operating system reports it: its highest
    /// resident reading stood over the limit, whatever the charged total was.
    /// `requested_bytes` is how far over the limit that reading stood, and
    /// the suggested limit is the reading itself rounded up.
    ProcessMemory {
        /// The highest resident memory the engine read for the process.
        peak_resident_bytes: u64,
    },
}

/// Where a group of input rows begins, named the way the dead-letter output
/// numbers rows: the Source node that read the group's first row and that
/// row's number among the rows the Source read (counting from 1). It
/// identifies a group without printing its key, which is a record value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPosition {
    /// The author-given name of the Source node that read the row.
    pub source: String,
    /// The row's number among the rows that Source read, counting from 1.
    pub row: u64,
}

/// One holder of charged memory in a [`MemoryShortfallReport`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolderReport {
    /// The author-given name of the node that owns the memory.
    pub node: String,
    /// What the memory is.
    pub surface: MemorySurface,
    /// The bytes it holds charged: its own charge plus the memory granted in
    /// its name, at the refusal. The current figure, not its high-water mark.
    pub bytes: u64,
    /// Why it still holds them.
    pub state: HolderState,
}

/// Why a holder in a [`MemoryShortfallReport`] still held its memory when the
/// request was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum HolderState {
    /// Its memory cannot be written to disk.
    CannotSpill,
    /// It spilled what it could and holds the least it can work with.
    AtFloor,
    /// A Source paused so it reads no further until memory is freed; the
    /// rows it has already read stay in memory.
    PausedSource,
    /// A Source still reading; the rows it has read stay counted here until
    /// the steps holding them pass them on, write them out, or spill them.
    ActiveSource,
    /// The node that made the refused request.
    Requester,
    /// Its memory can spill, but it was in use while the request was made, so
    /// the request could not spill it.
    InUse,
}

impl std::fmt::Display for HolderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CannotSpill => "cannot spill",
            Self::AtFloor => "at its floor",
            Self::PausedSource => "paused source",
            Self::ActiveSource => "active source",
            Self::Requester => "requester",
            Self::InUse => "in use",
        })
    }
}

/// What the reclaim round that preceded a refusal did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReclaimReport {
    /// The nodes whose memory the round asked to spill, in the order it asked.
    pub holders_asked: Vec<String>,
    /// Bytes the asked holders released while the round spilled them.
    pub bytes_freed: u64,
    /// The Sources paused when the request was refused.
    pub sources_paused: Vec<String>,
}

/// The smallest limit, rounded up to a whole MiB, that grants a request of
/// `requested` bytes beside `charged` bytes already held. Never below
/// `charged + requested`; saturates at the largest whole MiB a `u64` holds.
pub fn suggested_limit_floor(charged: u64, requested: u64) -> u64 {
    round_up_to_mebibyte(charged.saturating_add(requested))
}

/// `bytes` rounded up to a whole MiB and written the way `memory.limit` and
/// `--memory-limit` accept it: `<n>G` when the result is a whole number of
/// GiB, else `<n>M`. The value it names is never below `bytes`.
pub fn suggested_limit_text(bytes: u64) -> String {
    let mebibytes = round_up_to_mebibyte(bytes) / MIB;
    if mebibytes > 0 && mebibytes.is_multiple_of(1024) {
        format!("{}G", mebibytes / 1024)
    } else {
        format!("{mebibytes}M")
    }
}

const MIB: u64 = 1024 * 1024;

fn round_up_to_mebibyte(bytes: u64) -> u64 {
    match bytes.div_ceil(MIB).checked_mul(MIB) {
        Some(rounded) => rounded,
        None => u64::MAX - u64::MAX % MIB,
    }
}

/// A byte count in binary units with one decimal (`512 B`, `1.5 KiB`,
/// `3.0 MiB`), for the memory diagnostics.
struct Bytes(u64);

impl std::fmt::Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
        if self.0 < 1024 {
            return write!(f, "{} B", self.0);
        }
        let mut unit = 1024u128;
        for (index, name) in UNITS.iter().enumerate() {
            // Tenths of this unit, rounded to nearest; move up a unit rather
            // than print "1024.0".
            let tenths = (u128::from(self.0) * 10 + unit / 2) / unit;
            if tenths < 10_240 || index == UNITS.len() - 1 {
                return write!(f, "{}.{} {name}", tenths / 10, tenths % 10);
            }
            unit *= 1024;
        }
        unreachable!("the last unit always returns")
    }
}

/// A list of node names, each quoted the one way every diagnostic quotes a
/// name, joined for one line of the report.
fn quoted_names(names: &[String]) -> String {
    use clinker_core_types::QuoteName;
    names
        .iter()
        .map(|name| name.as_str().quoted_name().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The section of `clinker explain --code E310` that covers state of this
/// kind, when there is one.
fn fix_section(surface: &MemorySurface) -> Option<&'static str> {
    match surface {
        MemorySurface::JoinBuildSide | MemorySurface::JoinState => Some("Join build side"),
        MemorySurface::GroupState => Some("Group state"),
        MemorySurface::ReshapeGroups => Some("Rows held for Reshape groups"),
        MemorySurface::CullGroups => Some("Rows held for Cull groups"),
        MemorySurface::DecisionState => Some("Decision state"),
        MemorySurface::WindowIndex => Some("Window index"),
        MemorySurface::BufferedRows { .. }
        | MemorySurface::ScanMaterialization
        | MemorySurface::ParkedCrossRegionRows { .. }
        | MemorySurface::CorrelationGroups
        | MemorySurface::SortBuffer => Some("Rows buffered between two steps"),
        MemorySurface::HeldFailingRows | MemorySurface::DeadLetteredRowSet => {
            Some("Held failing rows")
        }
        MemorySurface::OpenDocumentRows => Some("Rows held until their document is decided"),
        MemorySurface::RowsRead
        | MemorySurface::OutputStaging
        | MemorySurface::CredentialRegistry => None,
    }
}

impl std::fmt::Display for MemoryShortfallReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use clinker_core_types::QuoteName;
        self.write_headline(f)?;
        if let Some(first) = &self.group_first_row {
            write!(
                f,
                "\n  group: the one whose first row is row {} of source {}",
                first.row,
                first.source.quoted_name()
            )?;
        }
        match self.join_partition_distinct_keys {
            None => {}
            // One key's rows share every hash bit a split could use, so no
            // repartitioning separates them; say so, because the remedy
            // differs from a partition that holds many keys.
            Some(1) => f.write_str(
                "\n  join partition: about 1 distinct key; one key's rows cannot be split \
                 across partitions, so repartitioning cannot make them fit",
            )?,
            Some(keys) => write!(f, "\n  join partition: about {keys} distinct keys")?,
        }

        write!(
            f,
            "\n  charged {} of {}",
            Bytes(self.charged_bytes),
            Bytes(self.limit.bytes())
        )?;
        if self.limit.bytes() > 0 {
            let percent = u128::from(self.charged_bytes) * 100 / u128::from(self.limit.bytes());
            write!(f, " ({percent}%)")?;
        }
        if let Some(private) = self.private_bytes {
            write!(f, " · private memory {}", Bytes(private))?;
        }

        if !self.holders.is_empty() || self.other_holders_count > 0 || self.unattributed_bytes > 0 {
            f.write_str("\n  largest holders:")?;
            for holder in &self.holders {
                write!(
                    f,
                    "\n    {}  {}  {}  {}",
                    holder.node.quoted_name(),
                    holder.surface,
                    Bytes(holder.bytes),
                    holder.state
                )?;
            }
            if self.other_holders_count > 0 {
                write!(
                    f,
                    "\n    +{} more {}  {}",
                    self.other_holders_count,
                    if self.other_holders_count == 1 {
                        "holder"
                    } else {
                        "holders"
                    },
                    Bytes(self.other_holders_bytes)
                )?;
            }
            if self.unattributed_bytes > 0 {
                write!(
                    f,
                    "\n    not held by any one node  {}",
                    Bytes(self.unattributed_bytes)
                )?;
            }
        }

        match &self.reclaim {
            None => f.write_str("\n  reclaim: none attempted")?,
            Some(round) => {
                write!(
                    f,
                    "\n  reclaim: asked {} {} to spill",
                    round.holders_asked.len(),
                    if round.holders_asked.len() == 1 {
                        "holder"
                    } else {
                        "holders"
                    }
                )?;
                if !round.holders_asked.is_empty() {
                    write!(f, " ({})", quoted_names(&round.holders_asked))?;
                }
                write!(
                    f,
                    ", freed {}; paused {} {}",
                    Bytes(round.bytes_freed),
                    round.sources_paused.len(),
                    if round.sources_paused.len() == 1 {
                        "source"
                    } else {
                        "sources"
                    }
                )?;
                if !round.sources_paused.is_empty() {
                    write!(f, " ({})", quoted_names(&round.sources_paused))?;
                }
            }
        }

        // The fix line says what its floor is for, not how it was summed:
        // the request is on the headline and the charge on its own line.
        // Under a test capacity it raises that capacity, and the
        // `memory.limit` paste lines are left out because they would not
        // lift it.
        let suggested = suggested_limit_text(self.suggested_limit_bytes);
        let (setting, room_for) = match self.limit {
            EnforcedLimit::MemoryLimit(_) => ("the limit", "the smallest limit"),
            EnforcedLimit::TestCapacity(_) => ("the test ledger capacity", "the smallest capacity"),
        };
        write!(f, "\n  fix: raise {setting} to at least {suggested} — ")?;
        match self.reading {
            LimitReading::Charged => write!(
                f,
                "{room_for} with room for this request and what the run already holds"
            )?,
            LimitReading::ProcessMemory {
                peak_resident_bytes,
            } => write!(f, "process memory reached {}", Bytes(peak_resident_bytes))?,
        }
        if let EnforcedLimit::MemoryLimit(_) = self.limit {
            write!(
                f,
                "; later stages may need more\
                 \n    pipeline:\
                 \n      memory: {{ limit: \"{suggested}\" }}\
                 \n    or: --memory-limit {suggested}"
            )?;
        }

        // An oversized request's remedy is the one for what the requester was
        // holding. When it is larger than the whole limit it fits beside
        // nothing, so no holder is to blame and that remedy is the only one;
        // otherwise the largest holder that cannot spill is named first.
        let requester_remedy =
            self.requester
                .as_ref()
                .filter(|_| self.oversized)
                .and_then(|requester| {
                    fix_section(&requester.surface).map(|section| (requester, section))
                });
        let alone_too_large = self.requested_bytes > self.limit.bytes();
        let holder_remedy = if alone_too_large && requester_remedy.is_some() {
            None
        } else {
            self.holders.iter().find(|holder| {
                matches!(
                    holder.state,
                    HolderState::CannotSpill | HolderState::AtFloor
                )
            })
        };
        if let Some(holder) = holder_remedy {
            write!(
                f,
                "\n  remedy: {}'s {} holds {} and {}",
                holder.node.quoted_name(),
                holder.surface,
                Bytes(holder.bytes),
                if holder.state == HolderState::CannotSpill {
                    "cannot be spilled"
                } else {
                    "could not be spilled further"
                }
            )?;
            if let Some(section) = fix_section(&holder.surface) {
                write!(f, "; see \"{section}\" in clinker explain --code E310")?;
            }
        } else if let Some((requester, section)) = requester_remedy {
            write!(
                f,
                "\n  remedy: {}'s {} cannot fit the limit in one piece; see \"{section}\" in \
                 clinker explain --code E310",
                requester.node.quoted_name(),
                requester.surface
            )?;
        }
        // The charged state fills the limit only in the ledger form; under a
        // process-memory reading the charged total sits below it.
        if self.reading == LimitReading::Charged
            && self.charged_bytes > 0
            && self.unspillable_bytes >= self.charged_bytes
        {
            f.write_str(
                "\n  spilling cannot help: the state that fills the limit cannot be written to disk",
            )?;
        }
        f.write_str("\n  See: clinker explain --code E310")
    }
}

impl MemoryShortfallReport {
    /// The first line of the E310 text, in one of three forms: the process
    /// reading over the limit; one request no spill can make room for; or
    /// the ordinary form, which states the request and how much of the limit
    /// was left (`limit − charged`, from the same reading). The ordinary
    /// form adds that nothing more could be spilled only when
    /// [`Self::reclaim`] holds the round that tried; a report with no round
    /// claims no spill was tried. Every form names the limit the run
    /// enforced.
    fn write_headline(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use clinker_core_types::QuoteName;
        f.write_str("E310")?;
        if let Some(requester) = &self.requester {
            write!(f, " {}", requester.node.quoted_name())?;
        }
        f.write_str(": ")?;
        if let LimitReading::ProcessMemory {
            peak_resident_bytes,
        } = self.reading
        {
            write!(
                f,
                "process memory peaked at {} resident, over {}",
                Bytes(peak_resident_bytes),
                self.limit
            )?;
            if let Some(requester) = &self.requester {
                write!(
                    f,
                    ", while {} held {}",
                    requester.node.quoted_name(),
                    requester.surface
                )?;
            }
            return write!(f, "; the run had charged {}", Bytes(self.charged_bytes));
        }
        if self.oversized {
            f.write_str("one request")?;
            if let Some(requester) = &self.requester {
                write!(f, " for {}", requester.surface)?;
            }
            write!(
                f,
                " needs {}, more than {} can hold",
                Bytes(self.requested_bytes),
                self.limit
            )?;
            if self.requested_bytes <= self.limit.bytes() {
                write!(
                    f,
                    " beside {} of state that cannot spill",
                    Bytes(self.unspillable_bytes)
                )?;
            }
            return f.write_str(" — spilling cannot help");
        }
        write!(f, "needed {} more", Bytes(self.requested_bytes))?;
        if let Some(requester) = &self.requester {
            write!(f, " for {}", requester.surface)?;
        }
        match self.limit.bytes().checked_sub(self.charged_bytes) {
            Some(left) if left > 0 => {
                write!(f, ", but only {} of {} was left", Bytes(left), self.limit)?
            }
            _ => write!(f, ", but none of {} was left", self.limit)?,
        }
        if self.reclaim.is_some() {
            f.write_str(" and nothing more could be spilled")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};
    use std::path::Path;

    #[test]
    fn storage_full_classifies_as_disk_full() {
        let dir = Path::new("/mnt/spill");
        let e = Error::new(ErrorKind::StorageFull, "No space left on device");
        match SpillError::from_spill_dir_io(dir, e) {
            SpillError::DiskFull { dir, .. } => assert_eq!(dir, "/mnt/spill"),
            other => panic!("ENOSPC must classify as DiskFull; got {other:?}"),
        }
    }

    #[test]
    fn disk_full_render_distinguishes_from_oom_and_cap() {
        let e = SpillError::DiskFull {
            dir: "/mnt/spill".to_string(),
            source: "No space left on device (os error 28)".to_string(),
        };
        let rendered = e.to_string();
        assert!(rendered.contains("E321"), "{rendered}");
        assert!(rendered.contains("out of space"), "{rendered}");
        // Must not read as an OOM or a cap stop.
        assert!(rendered.contains("not an out-of-memory"), "{rendered}");
        assert!(
            rendered.contains("not the configured spill cap"),
            "{rendered}"
        );
    }

    #[test]
    fn directory_faults_still_classify_as_dir_unavailable() {
        // The DiskFull addition must not steal the directory-level faults.
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::ReadOnlyFilesystem,
        ] {
            let e = Error::new(kind, "boom");
            assert!(
                matches!(
                    SpillError::from_spill_dir_io(Path::new("/d"), e),
                    SpillError::DirUnavailable { .. }
                ),
                "{kind:?} must stay DirUnavailable"
            );
        }
    }

    #[test]
    fn byte_figures_use_binary_units_with_one_decimal() {
        assert_eq!(Bytes(0).to_string(), "0 B");
        assert_eq!(Bytes(1023).to_string(), "1023 B");
        assert_eq!(Bytes(1024).to_string(), "1.0 KiB");
        assert_eq!(Bytes(1536).to_string(), "1.5 KiB");
        assert_eq!(Bytes(3 * MIB).to_string(), "3.0 MiB");
        // Just under a MiB rounds to 1024.0 KiB, which is printed as MiB.
        assert_eq!(Bytes(MIB - 1).to_string(), "1.0 MiB");
        assert_eq!(Bytes(1024 * MIB).to_string(), "1.0 GiB");
    }

    /// A report whose process memory, not its charged total, stood over the
    /// limit: 12 MiB resident against an 8 MiB limit with 3 MiB charged.
    fn process_memory_report() -> MemoryShortfallReport {
        MemoryShortfallReport {
            requester: Some(ConsumerLabel {
                node: "enrich".to_string(),
                surface: MemorySurface::JoinBuildSide,
            }),
            group_first_row: None,
            join_partition_distinct_keys: None,
            reading: LimitReading::ProcessMemory {
                peak_resident_bytes: 12 * MIB,
            },
            requested_bytes: 4 * MIB,
            limit: EnforcedLimit::MemoryLimit(8 * MIB),
            charged_bytes: 3 * MIB,
            private_bytes: None,
            holders: vec![HolderReport {
                node: "enrich".to_string(),
                surface: MemorySurface::JoinBuildSide,
                bytes: 3 * MIB,
                state: HolderState::CannotSpill,
            }],
            other_holders_count: 0,
            other_holders_bytes: 0,
            unattributed_bytes: 0,
            unspillable_bytes: 3 * MIB,
            reclaim: None,
            suggested_limit_bytes: 12 * MIB,
            oversized: false,
        }
    }

    #[test]
    fn a_process_memory_report_states_the_process_reading_not_a_full_limit() {
        let rendered = process_memory_report().to_string();
        assert_eq!(
            rendered,
            "E310 \"enrich\": process memory peaked at 12.0 MiB resident, over memory.limit \
             8.0 MiB, while \"enrich\" held join build side; the run had charged 3.0 MiB\
             \n  charged 3.0 MiB of 8.0 MiB (37%)\
             \n  largest holders:\
             \n    \"enrich\"  join build side  3.0 MiB  cannot spill\
             \n  reclaim: none attempted\
             \n  fix: raise the limit to at least 12M — process memory reached 12.0 MiB; \
             later stages may need more\
             \n    pipeline:\
             \n      memory: { limit: \"12M\" }\
             \n    or: --memory-limit 12M\
             \n  remedy: \"enrich\"'s join build side holds 3.0 MiB and cannot be spilled; see \
             \"Join build side\" in clinker explain --code E310\
             \n  See: clinker explain --code E310"
        );
        // Charged memory sits below the limit, so nothing may say the limit
        // is full or that the charged state fills it.
        assert!(!rendered.contains("fully held"), "{rendered}");
        assert!(!rendered.contains("fills the limit"), "{rendered}");
    }

    /// A report whose request did not fit beside the charged total: `totals`
    /// asked for 2 MiB more group state with 7.3125 MiB charged of an 8 MiB
    /// `memory.limit`, and no reclaim round ran.
    fn charged_report() -> MemoryShortfallReport {
        MemoryShortfallReport {
            requester: Some(ConsumerLabel {
                node: "totals".to_string(),
                surface: MemorySurface::GroupState,
            }),
            group_first_row: None,
            join_partition_distinct_keys: None,
            reading: LimitReading::Charged,
            requested_bytes: 2 * MIB,
            limit: EnforcedLimit::MemoryLimit(8 * MIB),
            charged_bytes: 7 * MIB + 320 * 1024,
            private_bytes: None,
            holders: vec![HolderReport {
                node: "enrich".to_string(),
                surface: MemorySurface::JoinBuildSide,
                bytes: 7 * MIB + 320 * 1024,
                state: HolderState::CannotSpill,
            }],
            other_holders_count: 0,
            other_holders_bytes: 0,
            unattributed_bytes: 0,
            unspillable_bytes: 7 * MIB + 320 * 1024,
            reclaim: None,
            suggested_limit_bytes: 10 * MIB,
            oversized: false,
        }
    }

    /// A round that asked one holder to spill and freed nothing.
    fn fruitless_round() -> ReclaimReport {
        ReclaimReport {
            holders_asked: vec!["sorted".to_string()],
            bytes_freed: 0,
            sources_paused: Vec::new(),
        }
    }

    /// The `fix:` line says what its floor is for, not that the request
    /// needed the run's whole charge: the request is on the headline and the
    /// charge on its own line.
    #[test]
    fn the_fix_line_states_what_the_floor_is_for() {
        let rendered = charged_report().to_string();
        let fix = rendered
            .split_once("\n  fix: ")
            .map(|(_, rest)| rest)
            .unwrap_or_else(|| panic!("a fix line is rendered:\n{rendered}"));
        assert!(
            fix.starts_with(
                "raise the limit to at least 10M — the smallest limit with room for this \
                 request and what the run already holds; later stages may need more\
                 \n    pipeline:\
                 \n      memory: { limit: \"10M\" }\
                 \n    or: --memory-limit 10M\n"
            ),
            "{rendered}"
        );
        assert!(!rendered.contains("this request needed"), "{rendered}");
    }

    /// A run held to a test capacity is named by that capacity in every
    /// headline form, and its fix line raises the capacity without the
    /// `memory.limit` paste lines, which would not lift it.
    #[test]
    fn a_test_capacity_is_named_as_the_test_ledger_capacity() {
        let mut report = charged_report();
        report.limit = EnforcedLimit::TestCapacity(8 * MIB);
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().next(),
            Some(
                "E310 \"totals\": needed 2.0 MiB more for group state, but only 704.0 KiB of \
                 the test ledger capacity 8.0 MiB was left"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "\n  fix: raise the test ledger capacity to at least 10M — the smallest capacity \
                 with room for this request and what the run already holds\n  remedy: "
            ),
            "{rendered}"
        );
        for absent in ["memory.limit", "pipeline:", "--memory-limit"] {
            assert!(!rendered.contains(absent), "{absent:?} in:\n{rendered}");
        }

        report.requested_bytes = 9 * MIB;
        report.oversized = true;
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().next(),
            Some(
                "E310 \"totals\": one request for group state needs 9.0 MiB, more than the test \
                 ledger capacity 8.0 MiB can hold — spilling cannot help"
            ),
            "{rendered}"
        );

        let mut report = process_memory_report();
        report.limit = EnforcedLimit::TestCapacity(8 * MIB);
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().next(),
            Some(
                "E310 \"enrich\": process memory peaked at 12.0 MiB resident, over the test \
                 ledger capacity 8.0 MiB, while \"enrich\" held join build side; the run had \
                 charged 3.0 MiB"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "\n  fix: raise the test ledger capacity to at least 12M — process memory \
                 reached 12.0 MiB\n  remedy: "
            ),
            "{rendered}"
        );
        for absent in ["memory.limit", "pipeline:", "--memory-limit"] {
            assert!(!rendered.contains(absent), "{absent:?} in:\n{rendered}");
        }
    }

    /// The headline states how much of the limit was left, and says nothing
    /// more could be spilled only when the report carries the round that
    /// tried. A refusal made with no round, as every refusal off the run's
    /// walk is, claims no spill was tried and never that the limit is full.
    #[test]
    fn a_report_with_no_reclaim_round_claims_no_spill_was_tried() {
        let report = charged_report();
        assert!(report.reclaim.is_none());
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().next(),
            Some(
                "E310 \"totals\": needed 2.0 MiB more for group state, but only 704.0 KiB of \
                 memory.limit 8.0 MiB was left"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("\n  reclaim: none attempted\n"),
            "{rendered}"
        );
        assert!(!rendered.contains("could be spilled"), "{rendered}");
        assert!(!rendered.contains("fully held"), "{rendered}");

        let mut after_round = charged_report();
        after_round.reclaim = Some(fruitless_round());
        let rendered = after_round.to_string();
        assert_eq!(
            rendered.lines().next(),
            Some(
                "E310 \"totals\": needed 2.0 MiB more for group state, but only 704.0 KiB of \
                 memory.limit 8.0 MiB was left and nothing more could be spilled"
            ),
            "{rendered}"
        );

        // With the whole limit charged, nothing was left.
        let mut full = charged_report();
        full.charged_bytes = 8 * MIB;
        full.holders[0].bytes = 8 * MIB;
        full.unspillable_bytes = 8 * MIB;
        assert_eq!(
            full.to_string().lines().next(),
            Some(
                "E310 \"totals\": needed 2.0 MiB more for group state, but none of memory.limit \
                 8.0 MiB was left"
            )
        );
        full.reclaim = Some(fruitless_round());
        assert_eq!(
            full.to_string().lines().next(),
            Some(
                "E310 \"totals\": needed 2.0 MiB more for group state, but none of memory.limit \
                 8.0 MiB was left and nothing more could be spilled"
            )
        );
    }

    /// A Source's rows are relieved by spilling the steps that hold them or
    /// by the limit the fix line gives, never by anything the author does to
    /// the Source, so the remedy passes over every holder of rows read from a
    /// source, whatever state it is listed in.
    #[test]
    fn a_source_is_never_the_remedy() {
        let source = |node: &str, state: HolderState| HolderReport {
            node: node.to_string(),
            surface: MemorySurface::RowsRead,
            bytes: 2 * MIB,
            state,
        };

        let mut report = charged_report();
        report.holders = vec![
            source("orders", HolderState::CannotSpill),
            HolderReport {
                node: "enrich".to_string(),
                surface: MemorySurface::JoinBuildSide,
                bytes: 3 * MIB,
                state: HolderState::CannotSpill,
            },
        ];
        let rendered = report.to_string();
        assert!(
            rendered.contains("\n  remedy: \"enrich\"'s join build side holds 3.0 MiB and "),
            "the remedy passes over the Source to the join:\n{rendered}"
        );
        assert!(!rendered.contains("remedy: \"orders\""), "{rendered}");

        let mut report = charged_report();
        report.holders = vec![
            source("orders", HolderState::CannotSpill),
            source("returns", HolderState::ActiveSource),
            source("accounts", HolderState::AtFloor),
        ];
        let rendered = report.to_string();
        assert!(
            !rendered.contains("\n  remedy: "),
            "with only Sources holding memory there is no holder to name:\n{rendered}"
        );
        assert!(
            rendered.contains("\n  fix: raise the limit to at least 10M"),
            "{rendered}"
        );
    }

    #[test]
    fn a_process_memory_report_without_a_requester_names_no_node() {
        let mut report = process_memory_report();
        report.requester = None;
        let rendered = report.to_string();
        let headline = rendered.lines().next().unwrap_or_default();
        assert_eq!(
            headline,
            "E310: process memory peaked at 12.0 MiB resident, over memory.limit 8.0 MiB; \
             the run had charged 3.0 MiB"
        );
    }

    #[test]
    fn a_join_partition_line_gives_its_distinct_key_estimate_after_the_headline() {
        let mut report = process_memory_report();
        report.join_partition_distinct_keys = Some(48_210);
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().nth(1),
            Some("  join partition: about 48210 distinct keys"),
            "{rendered}"
        );

        report.group_first_row = Some(RowPosition {
            source: "orders".to_string(),
            row: 4,
        });
        let rendered = report.to_string();
        assert!(
            rendered
                .lines()
                .nth(1)
                .is_some_and(|line| line.starts_with("  group: "))
                && rendered.lines().nth(2) == Some("  join partition: about 48210 distinct keys"),
            "the partition line follows the group line: {rendered}"
        );
    }

    #[test]
    fn a_join_partition_of_one_key_says_repartitioning_cannot_split_it() {
        let mut report = process_memory_report();
        report.join_partition_distinct_keys = Some(1);
        let rendered = report.to_string();
        assert_eq!(
            rendered.lines().nth(1),
            Some(
                "  join partition: about 1 distinct key; one key's rows cannot be split \
                 across partitions, so repartitioning cannot make them fit"
            ),
            "{rendered}"
        );
    }

    /// An Output's rows of a still-open document are named as waiting for
    /// the document's verdict, and their remedy is their own section, not
    /// the one for a failed document's held rows.
    #[test]
    fn open_document_rows_point_at_their_own_section() {
        assert_eq!(
            MemorySurface::OpenDocumentRows.to_string(),
            "rows held until their document is decided"
        );
        assert_eq!(
            fix_section(&MemorySurface::OpenDocumentRows),
            Some("Rows held until their document is decided")
        );
    }

    #[test]
    fn suggested_floor_saturates_instead_of_wrapping() {
        let floor = suggested_limit_floor(u64::MAX, 1);
        assert_eq!(floor % MIB, 0);
        assert!(floor > u64::MAX - MIB);
    }

    #[test]
    fn generic_io_stays_io() {
        let e = Error::new(ErrorKind::BrokenPipe, "pipe");
        assert!(matches!(
            SpillError::from_spill_dir_io(Path::new("/d"), e),
            SpillError::Io(_)
        ));
    }
}
