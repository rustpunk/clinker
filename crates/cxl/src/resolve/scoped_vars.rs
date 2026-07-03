//! Scoped-variable registry consulted by the resolver and typechecker.
//!
//! The registry is constructed by the pipeline configuration loader by
//! unioning the flat `vars:` block (`$vars.<key>` static config) with
//! every Transform's `declares:` entries (`$pipeline` / `$source` /
//! `$record` producer-declared state); see
//! `clinker_plan::config::build_scoped_vars_registry`. The result is
//! threaded into [`resolve_program_with_modules_and_vars`] /
//! [`type_check_with_vars`]. CXL doesn't depend on `clinker-plan`, so
//! the types here mirror — rather than re-export — the config-side
//! definitions.
//!
//! Three scopes match the CXL read namespaces:
//! - [`ScopedVarsRegistry::pipeline`] — read via `$pipeline.<key>`,
//!   alongside the builtin members (`start_time`, `name`,
//!   `execution_id`, `batch_id`, counter set).
//! - [`ScopedVarsRegistry::source`] — read via `$source.<key>`, alongside
//!   builtin provenance (`file`, `row`, `path`, `count`, `batch`,
//!   `ingestion_timestamp`, `name`).
//! - [`ScopedVarsRegistry::record`] — read via `$record.<key>`. No
//!   builtins. Per-record scratch state, multi-writer across nodes
//!   within a record's lifetime. Written by Transforms whose
//!   `declares:` entries have `scope: record`. Naming follows the
//!   workspace's `Record` type — applies uniformly to CSV rows, JSON
//!   objects, XML elements, and fixed-width records.

use indexmap::IndexMap;

use crate::ast::LiteralValue;

/// Scoped-variable primitive type set.
///
/// Mirrors `clinker_plan::config::ScopedVarType`. Conversion to the CXL
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
///
/// The `hidden_*` tiers carry parent-scope vars that the body is NOT
/// permitted to read, but their existence informs a more useful
/// diagnostic: composition bodies use this to surface the
/// schema-aware E173 ("you tried to read a parent var that's hidden
/// from your body — declare it in `_compose.scoped_vars` to opt in")
/// rather than the generic "unknown member" error. Empty for
/// non-composition contexts.
#[derive(Debug, Clone, Default)]
pub struct ScopedVarsRegistry {
    pub pipeline: IndexMap<String, ScopedVarType>,
    pub source: IndexMap<String, ScopedVarType>,
    pub record: IndexMap<String, ScopedVarType>,
    pub hidden_pipeline: IndexMap<String, ScopedVarType>,
    pub hidden_source: IndexMap<String, ScopedVarType>,
    pub hidden_record: IndexMap<String, ScopedVarType>,
    /// Static config knobs read via `$vars.<key>`. Channel-overridable,
    /// frozen at pipeline start. Distinct from the per-scope tiers
    /// above (which are producer-written); a `$vars.*` read is global
    /// and DAG-position-independent.
    pub static_vars: IndexMap<String, ScopedVarType>,
    /// Composition config parameters read via `$config.<param>`. Declared
    /// types drive resolve (unknown param → error) and typecheck. Empty
    /// outside a composition body, so a top-level `$config.*` read is
    /// rejected. Paired with [`Self::config_fold`], which carries each
    /// param's resolved literal for the compile-time constant fold.
    pub config: IndexMap<String, ScopedVarType>,
    /// Resolved literal value per `$config.<param>` for THIS composition
    /// instantiation (signature default, call-site `config:`, or
    /// channel/group `config:` clobber). The planner folds each
    /// `$config.<param>` to this literal after typecheck; the values are
    /// per-instantiation, so `config_fold` is never shared across
    /// composition call sites.
    pub config_fold: IndexMap<String, LiteralValue>,
}

/// Per-scope tag for `ScopedVarsRegistry::hidden_lookup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeTag {
    Pipeline,
    Source,
    Record,
}

impl ScopedVarsRegistry {
    /// Look up `key` in the hidden tier of `scope`. Returns `Some` when
    /// the parent declared the var but the composition's signature
    /// didn't opt it in — the resolver uses this to produce the
    /// composition-aware E173 diagnostic.
    pub fn hidden_lookup(&self, scope: ScopeTag, key: &str) -> Option<ScopedVarType> {
        match scope {
            ScopeTag::Pipeline => self.hidden_pipeline.get(key).copied(),
            ScopeTag::Source => self.hidden_source.get(key).copied(),
            ScopeTag::Record => self.hidden_record.get(key).copied(),
        }
    }
}
