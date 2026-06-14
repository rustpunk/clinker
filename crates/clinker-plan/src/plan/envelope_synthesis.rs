//! Compiled header/footer synthesis spec for the `Envelope` node.
//!
//! An Envelope node may declare, orthogonally to its framing strategy, a set
//! of synthesized header and footer sections (`config.header` / `config.footer`
//! in the pipeline YAML). This module holds the plan-compile artifact that the
//! executor evaluates per output document:
//!
//! - **Header** sections are scalar projections evaluated once per output
//!   document at its open. Each field is a CXL expression over
//!   document-open-knowable inputs only (`$vars` / `$source` / `$pipeline` /
//!   the ambient `$doc.*` envelope); body-column references are rejected at
//!   compile time, since the header is emitted before any body record streams.
//! - **Footer** sections are streaming aggregate folds over the framed body
//!   records of one document. Each field is a distributive/algebraic aggregate
//!   (`count` / `sum` / `avg` / `min` / `max`) maintained as an O(1)
//!   accumulator per open document and emitted at the document's close.
//!
//! Synthesis layers on top of either framing strategy: under `preserve` it
//! produces a per-document header/footer; under `concat` it produces the single
//! consolidated document's header/footer. Both reduce to "group the framed body
//! by document grain, then per grain evaluate header scalars and fold footer
//! aggregates" — the strategy only decides how many grains there are.
//!
//! **Not serde-derivable.** It holds [`cxl::ast::Expr`] /
//! [`cxl::typecheck::TypedProgram`] / [`cxl::plan::CompiledAggregate`], none of
//! which serialize (the same constraint that keeps `PlanNode::Aggregation`'s
//! `compiled` field `#[serde(skip)]`). The spec lives behind `Arc` on
//! `PlanNode::Envelope` with `#[serde(skip)]` and is reconstructed by the
//! compiler, never shipped through the JSON `--explain` channel.

use std::sync::Arc;

use cxl::ast::Expr;
use cxl::plan::CompiledAggregate;
use cxl::typecheck::TypedProgram;

/// One synthesized header section: a named `EnvelopeRecord` section whose
/// fields are scalar CXL expressions evaluated against the grain's first
/// record at document open.
#[derive(Debug, Clone)]
pub struct SynthesizedHeaderSection {
    /// Target `EnvelopeRecord` section name (user-defined; the section a
    /// downstream writer renders via `header_from_doc`).
    pub section: String,
    /// The section's typed program — one synthetic `emit <field> = <expr>`
    /// statement per declared field, typechecked together against the body
    /// input schema. Shared by every field's scalar compile so the typed
    /// side-tables (regex cache, literal baking) are built once.
    pub typed: Arc<TypedProgram>,
    /// Fields in declaration order (= rendered cell order). Each carries the
    /// output field name and the expression the executor lowers to a
    /// `CompiledScalar` via [`cxl::eval::ProgramEvaluator::compile_scalar`].
    pub fields: Vec<(Box<str>, Expr)>,
}

/// One synthesized footer section: a named `EnvelopeRecord` section whose
/// fields are streaming aggregate folds over the document's framed body.
#[derive(Debug, Clone)]
pub struct SynthesizedFooterSection {
    /// Target `EnvelopeRecord` section name (the section a downstream writer
    /// renders via `footer_from_doc`).
    pub section: String,
    /// The compiled aggregate for this section — one `emit <field> = <agg>`
    /// statement per declared field, extracted with an empty group_by (the
    /// per-document grouping is external, done by the executor over grains).
    /// Each emit's `output_name` is the rendered field name.
    pub compiled: Arc<CompiledAggregate>,
}

/// Compiled declarative header/footer synthesis for one Envelope node.
///
/// Attached to `PlanNode::Envelope` as `Option<Arc<EnvelopeSynthesis>>`; `None`
/// when the node declares neither a `header:` nor a `footer:` synthesis map.
/// The executor, when present, groups the framed body by document grain and per
/// grain stamps the synthesized sections into the document's `EnvelopeRecord`.
#[derive(Debug, Clone)]
pub struct EnvelopeSynthesis {
    /// Header sections in declaration order.
    pub header: Vec<SynthesizedHeaderSection>,
    /// Footer sections in declaration order.
    pub footer: Vec<SynthesizedFooterSection>,
}

impl EnvelopeSynthesis {
    /// `true` when the node declares neither a header nor a footer section —
    /// the lowering site stores `None` rather than an empty spec in that case,
    /// so a present `EnvelopeSynthesis` always carries at least one section.
    pub fn is_empty(&self) -> bool {
        self.header.is_empty() && self.footer.is_empty()
    }
}
