//! Composition type system and Phase 1 workspace loader (Phase 16c.1).
//!
//! Task 16c.1.1 delivers the core type definitions:
//! [`CompositionSignature`], [`PortDecl`], [`OutputAlias`], [`ParamDecl`],
//! [`ParamType`], [`ResourceDecl`], [`ResourceKind`], plus the
//! [`CompositionSymbolTable`] alias and the [`SourceMap`] span index.
//!
//! Serde-saphyr deserialization lands in 16c.1.2; [`scan_workspace_signatures`]
//! in 16c.1.3; [`OpenTailSchema`] in 16c.1.4.

use crate::config::pipeline_node::PipelineNode;
use crate::span::{FileId, Span};
use crate::yaml::{Spanned, YamlError};
use clinker_record::Schema;
use indexmap::IndexMap;
use std::path::PathBuf;

mod raw;

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
#[derive(Debug, Clone)]
pub struct PortDecl {
    /// Minimum-required schema; [`clinker_record::Schema`] is used directly
    /// per Theme B. `None` = accept any row shape.
    pub schema: Option<Schema>,
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
