//! Scoped-variable registry consulted by the resolver and typechecker.
//!
//! The registry is constructed by the pipeline configuration loader from
//! the YAML `vars:` block (see `clinker_core::config::ScopedVarsDecl`) and
//! threaded into [`resolve_program_with_modules_and_vars`] /
//! [`type_check_with_vars`]. CXL doesn't depend on `clinker-core`, so the
//! types here mirror — rather than re-export — the config-side definitions.
//!
//! Three scopes match the CXL read namespaces:
//! - [`ScopedVarsRegistry::pipeline`] — read via `$pipeline.<key>`,
//!   alongside the builtin members (`start_time`, `name`,
//!   `execution_id`, `batch_id`, counter set).
//! - [`ScopedVarsRegistry::source`] — read via `$source.<key>`, alongside
//!   builtin provenance (`file`, `row`, `path`, `count`, `batch`,
//!   `ingestion_timestamp`).
//! - [`ScopedVarsRegistry::record`] — read via `$record.<key>`. No
//!   builtins. Per-record scratch state distinct from `$meta.*` (which
//!   is single-writer-within-a-CXL-program); `$record.*` is multi-writer
//!   across nodes within a record's lifetime. Naming follows the
//!   workspace's `Record` type — applies uniformly to CSV rows, JSON
//!   objects, XML elements, and fixed-width records.

use indexmap::IndexMap;

/// Scoped-variable primitive type set.
///
/// Mirrors `clinker_core::config::ScopedVarType`. Conversion to the CXL
/// [`crate::typecheck::types::Type`] happens at typecheck-time lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopedVarType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
}

/// User-declared variables partitioned by CXL read scope.
///
/// Empty by default — call sites that don't have a config-supplied
/// registry pass `&ScopedVarsRegistry::default()` and observe the
/// pre-Phase-B behavior (only builtin members of `$pipeline.*` /
/// `$source.*` resolve).
#[derive(Debug, Clone, Default)]
pub struct ScopedVarsRegistry {
    pub pipeline: IndexMap<String, ScopedVarType>,
    pub source: IndexMap<String, ScopedVarType>,
    pub record: IndexMap<String, ScopedVarType>,
}

impl ScopedVarsRegistry {
    /// True if the registry declares no variables in any scope.
    pub fn is_empty(&self) -> bool {
        self.pipeline.is_empty() && self.source.is_empty() && self.record.is_empty()
    }
}
