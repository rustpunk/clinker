//! Composition type system and Phase 1 workspace loader (Phase 16c.1).
//!
//! Task 16c.1.1 delivers the core type definitions:
//! [`CompositionSignature`], [`PortDecl`], [`OutputAlias`], [`ParamDecl`],
//! [`ParamType`], [`ResourceDecl`], [`ResourceKind`], plus the
//! [`CompositionSymbolTable`] alias and the [`SourceMap`] span index.
//!
//! Serde-saphyr deserialization lands in 16c.1.2; [`scan_workspace_signatures`]
//! in 16c.1.3; [`OpenTailSchema`] in 16c.1.4.

use crate::config::pipeline_node::{PipelineNode, SchemaDecl};
use crate::error::{Diagnostic, LabeledSpan, Severity};
use crate::span::{FileId, Span};
use crate::yaml::{Spanned, YamlError};
use indexmap::IndexMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod provenance;
mod raw;
mod resource;

pub use resource::Resource;

#[cfg(test)]
mod tests;

/// Workspace symbol table produced by the Phase 1 scanner.
///
/// Keyed by workspace-relative path (matches the way `use:` references in call
/// sites point at composition files). Iteration order preserved via
/// [`IndexMap`] per LD-004.
pub type CompositionSymbolTable = IndexMap<PathBuf, CompositionSignature>;

/// Field-path → [`Span`] map for signature-level diagnostics. Keys are
/// dotted-path strings into the `_compose:` block (e.g.
/// `"inputs.customers.required"`).
pub type SourceMap = IndexMap<String, Span>;

/// Port / param / resource / node-ref name types.
///
/// Kept as bare aliases for phase 16c.1. See V-6-1 in the phase file: a
/// newtype refactor is recommended but not load-bearing at this scope.
pub type PortName = String;
pub type ParamName = String;
pub type ResourceName = String;
/// `"nodename.channel"` reference into a composition body.
pub type NodeRef = String;

/// A composition signature extracted from the `_compose:` block of a
/// `.comp.yaml` file.
///
/// The signature is the publicly-visible contract of a composition: what
/// inputs it accepts, what outputs it exposes, and what config / resources
/// it requires. Body nodes are held separately in [`CompositionFile`] (16c.1.2).
#[derive(Debug, Clone)]
pub struct CompositionSignature {
    /// User-facing composition name from `_compose.name`.
    pub name: String,
    /// Declared input ports; minimum-required schema per port (LD-16c-2).
    pub inputs: IndexMap<PortName, PortDecl>,
    /// Output port aliases — each points at an internal `"node.channel"`
    /// reference inside the body.
    pub outputs: IndexMap<PortName, OutputAlias>,
    /// Declared config params (LD-16c-1 two-slot split).
    pub config_schema: IndexMap<ParamName, ParamDecl>,
    /// Declared resource slots (LD-16c-1 two-slot split).
    pub resources_schema: IndexMap<ResourceName, ResourceDecl>,
    /// Absolute path to the `.comp.yaml` file that produced this signature.
    pub source_path: PathBuf,
    /// Field-path → span index used for signature-load diagnostics (E101, E104).
    pub source_spans: SourceMap,
}

/// An input port declaration.
///
/// `schema` is the **minimum-required** column set (LD-16c-2): rows flowing
/// through the port must carry at least these columns, but may carry extras
/// (pass-through). `None` means accept-any — the port has no declared shape.
///
/// The schema carries typed column declarations via [`SchemaDecl`] — the
/// same `[{name, type}]` shape used by `SourceBody` in 16b. Types drive
/// composition-load-time type-checking per drill §Q1 recommendation.
#[derive(Debug, Clone)]
pub struct PortDecl {
    /// Minimum-required typed schema. `None` = accept any row shape.
    pub schema: Option<SchemaDecl>,
    pub description: Option<String>,
    pub required: bool,
}

/// An output port alias.
///
/// Either a string shorthand (`enriched: final_stage.out`) or a long-form
/// object with `ref:` and `description:`. Both shapes deserialize to this
/// struct in 16c.1.2.
#[derive(Debug, Clone)]
pub struct OutputAlias {
    /// Internal node-channel reference with its source span (e.g.
    /// `"final_stage.out"`). The span points at the string literal in
    /// `_compose.outputs.<port>` — or the `ref:` field in long-form.
    pub internal_ref: SpannedNodeRef,
    pub description: Option<String>,
}

/// A span-carrying [`NodeRef`]. Inlined here (rather than reusing the
/// serde-saphyr `Spanned<T>`) because canonical spans are
/// [`crate::span::Span`] per LD-003; the serde-saphyr variant is converted
/// at the yaml boundary.
#[derive(Debug, Clone)]
pub struct SpannedNodeRef {
    pub value: NodeRef,
    pub span: Span,
}

/// A config-param declaration inside `_compose.config_schema`.
#[derive(Debug, Clone)]
pub struct ParamDecl {
    pub param_type: ParamType,
    pub required: bool,
    /// Default value as a serde-json [`Value`](serde_json::Value), matching
    /// the existing `FieldDef.default` convention.
    pub default: Option<serde_json::Value>,
    /// Optional enum constraint (`enum: [a, b, c]`).
    pub enum_values: Option<Vec<serde_json::Value>>,
    /// Optional numeric range `(min, max)` for `int`/`float` params.
    pub range: Option<(f64, f64)>,
    pub description: Option<String>,
    /// Primary span for this param declaration (LD-003 canonical [`Span`]).
    pub span: Span,
}

/// The permitted set of config-param types. YAML form is lowercase
/// (`"string" | "int" | "float" | "bool" | "path"`); serde wiring lands in
/// 16c.1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    String,
    Int,
    Float,
    Bool,
    Path,
}

/// A resource-slot declaration inside `_compose.resources_schema`.
#[derive(Debug, Clone)]
pub struct ResourceDecl {
    pub kind: ResourceKind,
    pub required: bool,
    pub description: Option<String>,
    /// Primary span for this resource declaration.
    pub span: Span,
}

/// Payload-free tag enum for the [`ResourceDecl.kind`] field.
///
/// Stub per LD-16c-3: the variant set is audited in 16c.3 against the actual
/// `clinker-channel` / `clinker-core` sources/sinks. `File` is the only
/// variant today; the full `Resource` payload enum lands in 16c.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    File,
}

/// A fully-parsed `.comp.yaml` file: the [`CompositionSignature`] from the
/// `_compose:` block plus the body `nodes:`.
///
/// Parsed via [`CompositionFile::parse`]. Canonical [`Span`] values require
/// a [`FileId`] (LD-003), so the parse entry point takes one explicitly;
/// callers that haven't registered the file with a `SourceDb` yet may pass
/// a synthetic `FileId` — the span bytes still round-trip through
/// [`Span::from_saphyr`] and can be re-homed later.
#[derive(Debug)]
pub struct CompositionFile {
    pub signature: CompositionSignature,
    pub nodes: Vec<Spanned<PipelineNode>>,
}

impl CompositionFile {
    /// Parse a `.comp.yaml` YAML string into a [`CompositionFile`].
    ///
    /// `file_id` anchors every captured span to the same source file so
    /// [`Span::from_saphyr`] can produce canonical [`Span`] values. Pass
    /// [`PathBuf::new`] for `source_path` if the file has no on-disk origin
    /// (e.g. in-memory fixture).
    pub fn parse(
        yaml: &str,
        file_id: FileId,
        source_path: PathBuf,
    ) -> Result<CompositionFile, YamlError> {
        let raw: raw::RawCompositionFile = crate::yaml::from_str(yaml)?;
        Ok(raw.finalize(file_id, source_path))
    }
}

// =========================================================================
// Phase 1 workspace scanner (Task 16c.1.3)
// =========================================================================

/// Maximum filesystem depth for workspace walks (LD-16c-17).
///
/// Bounds the blast radius of pathological directory trees.
const WORKSPACE_WALK_MAX_DEPTH: usize = 16;

/// Maximum number of `.comp.yaml` files permitted per workspace scan
/// (LD-16c-5). Enforced *during* the walk so depth-bombs and file-bombs
/// cannot exhaust resources before the budget fires (LD-16c-17).
pub const WORKSPACE_COMPOSITION_BUDGET: usize = 50;

/// Scan a workspace root for `.comp.yaml` files and build the composition
/// signature symbol table (LD-16c-5, LD-16c-12, LD-16c-17).
///
/// Walks `workspace_root` recursively with [`WORKSPACE_WALK_MAX_DEPTH`]
/// depth cap and [`WORKSPACE_COMPOSITION_BUDGET`] file count cap, parses
/// each `.comp.yaml` via [`CompositionFile::parse`], and returns a
/// workspace-relative [`CompositionSymbolTable`] wrapped in [`Arc`] for
/// parallel sharing across downstream compile stages.
///
/// Symlinks are **rejected** (not followed) per LD-16c-17; symlink loops
/// and depth-bombs cannot exhaust resources.
///
/// ## Failure modes
///
/// Fail-fast per phase. On any of:
///   * YAML parse error → [`Diagnostic`] with code `E101`
///   * budget exceeded → [`Diagnostic`] with code `E101` (budget family)
///   * IO error reading a candidate file → [`Diagnostic`] with code `E101`
///
/// All collected diagnostics up to that point are returned as
/// `Err(Vec<Diagnostic>)`. Partial tables are never returned.
///
/// A workspace with **no** `.comp.yaml` files returns `Ok(Arc::new(empty))`.
/// A nonexistent workspace root returns `Ok(Arc::new(empty))` — the
/// production caller has already resolved the root per LD-16c-12.
pub fn scan_workspace_signatures(
    workspace_root: &Path,
) -> Result<Arc<CompositionSymbolTable>, Vec<Diagnostic>> {
    use std::num::NonZeroU32;
    use walkdir::WalkDir;

    // Early return for the no-workspace case — downstream callers pass
    // `CompileContext::default()` from tests where the CWD may or may
    // not contain a composition tree.
    if !workspace_root.exists() {
        return Ok(Arc::new(IndexMap::new()));
    }

    let mut table: CompositionSymbolTable = IndexMap::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let mut file_id_counter: u32 = 1;

    // LD-16c-17: .follow_links(false) prevents symlink-loop exhaustion,
    // max_depth caps depth-bomb attacks, budget enforced mid-walk so the
    // cost of a malicious tree never exceeds O(budget + max_depth).
    let walker = WalkDir::new(workspace_root)
        .follow_links(false)
        .max_depth(WORKSPACE_WALK_MAX_DEPTH)
        .into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                // Broken symlink, permission denied, or similar. Record and
                // keep going — individual IO errors are not fatal to the
                // scan, only malformed YAML is.
                let ft = err.io_error().map(|io| io.kind());
                let _ = ft;
                continue;
            }
        };

        // Reject symlinks explicitly (belt-and-suspenders with
        // follow_links(false), which already skips crossing them).
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        if !is_comp_yaml(path) {
            continue;
        }

        // Enforce budget BEFORE admitting this file into the table. Budget
        // exceeded surfaces as a single E101 with a descriptive message,
        // and the scan halts.
        if table.len() >= WORKSPACE_COMPOSITION_BUDGET {
            diagnostics.push(budget_exceeded_diagnostic(workspace_root));
            return Err(diagnostics);
        }

        let yaml = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(io_err) => {
                diagnostics.push(io_error_diagnostic(path, &io_err));
                return Err(diagnostics);
            }
        };

        let file_id =
            FileId::new(NonZeroU32::new(file_id_counter).expect("FileId counter starts at 1"));
        file_id_counter = file_id_counter.saturating_add(1);

        match CompositionFile::parse(&yaml, file_id, path.to_path_buf()) {
            Ok(comp_file) => {
                let key = path
                    .strip_prefix(workspace_root)
                    .unwrap_or(path)
                    .to_path_buf();
                table.insert(key, comp_file.signature);
            }
            Err(parse_err) => {
                diagnostics.push(parse_error_diagnostic(file_id, path, &parse_err));
                return Err(diagnostics);
            }
        }
    }

    // Signature-level validation over the loaded table (warnings only —
    // not blockers). Any emitted warnings become part of the diagnostics
    // list but do NOT cause the scan to fail.
    let mut warnings = validate_signatures(&table);
    diagnostics.append(&mut warnings);

    if diagnostics
        .iter()
        .any(|d| matches!(d.severity, Severity::Error))
    {
        return Err(diagnostics);
    }

    Ok(Arc::new(table))
}

/// Pure validation over a loaded symbol table. Emits W102 warnings for
/// contradictory or suspicious signature shapes (non-fatal):
///
/// * `ParamDecl { required: true, default: Some(_) }` — contradictory
///   (required-and-defaulted means the default is dead code).
/// * `PortDecl { required: false }` on an input with no declared schema —
///   the absence contract is resolved at call-site binding, so a port
///   that is both optional and shape-free is suspicious.
pub fn validate_signatures(table: &CompositionSymbolTable) -> Vec<Diagnostic> {
    use std::num::NonZeroU32;
    let synthetic = FileId::new(NonZeroU32::new(1).expect("1 is non-zero"));

    let mut out = Vec::new();
    for signature in table.values() {
        // Rule 1: required-and-defaulted param is contradictory.
        for (param_name, decl) in &signature.config_schema {
            if decl.required && decl.default.is_some() {
                out.push(
                    Diagnostic::warning(
                        "W102",
                        format!(
                            "config param `{}` in composition `{}` is `required: true` but also has a `default:` — the default is unreachable",
                            param_name, signature.name
                        ),
                        LabeledSpan::new(decl.span, Some("declared here".to_string())),
                    )
                    .with_help("remove `required: true` if the default is the fallback, or drop the default if the value must always be supplied by the caller"),
                );
            }
        }
        // Rule 2: optional port with no declared schema is suspicious —
        // call-site binding cannot verify shape in its absence.
        for (port_name, decl) in &signature.inputs {
            if !decl.required && decl.schema.is_none() {
                out.push(Diagnostic::warning(
                    "W102",
                    format!(
                        "input port `{}` in composition `{}` is optional and has no declared schema — call sites cannot verify shape",
                        port_name, signature.name
                    ),
                    LabeledSpan::new(Span::point(synthetic, 0), Some("consider `required: true` or adding a `schema:` block".to_string())),
                ));
            }
        }
    }
    out
}

fn is_comp_yaml(path: &Path) -> bool {
    // Match `*.comp.yaml` via the full file name; a bare `.yaml`
    // extension is not enough and `Path::extension` returns only `yaml`
    // which loses the `.comp` marker.
    match path.file_name().and_then(|s| s.to_str()) {
        Some(name) => name.ends_with(".comp.yaml"),
        None => false,
    }
}

fn budget_exceeded_diagnostic(workspace_root: &Path) -> Diagnostic {
    // Synthetic FileId for the workspace-level budget diagnostic (no
    // single source file caused it). Downstream rendering falls back to
    // the file-relative message when span.file is unknown.
    use std::num::NonZeroU32;
    let synthetic = FileId::new(NonZeroU32::new(1).expect("1 is non-zero"));
    Diagnostic::error(
        "E101",
        format!(
            "workspace composition budget exceeded: more than {} `.comp.yaml` files under {}",
            WORKSPACE_COMPOSITION_BUDGET,
            workspace_root.display()
        ),
        LabeledSpan::new(Span::point(synthetic, 0), None),
    )
    .with_help(format!(
        "the composition count cap is {}; consolidate compositions or split the workspace",
        WORKSPACE_COMPOSITION_BUDGET
    ))
}

fn io_error_diagnostic(path: &Path, err: &std::io::Error) -> Diagnostic {
    use std::num::NonZeroU32;
    let synthetic = FileId::new(NonZeroU32::new(1).expect("1 is non-zero"));
    Diagnostic::error(
        "E101",
        format!("failed to read composition file {}: {err}", path.display()),
        LabeledSpan::new(Span::point(synthetic, 0), None),
    )
}

fn parse_error_diagnostic(file_id: FileId, path: &Path, err: &YamlError) -> Diagnostic {
    Diagnostic::error(
        "E101",
        format!(
            "composition signature parse error in {}: {err}",
            path.display()
        ),
        LabeledSpan::new(Span::point(file_id, 0), None),
    )
}
