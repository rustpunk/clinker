//! Leaf vocabulary shared across the Clinker pipeline crates.
//!
//! This crate sits below the orchestration layer and carries the value
//! types that diagnostics, planning, and execution all pass around:
//! source spans, structured compile-time diagnostics, the name-keyed DAG
//! graph used for cycle detection, the dead-letter-queue category enum,
//! and serialization-neutral failure classifications. It deliberately
//! holds no executor, config, schema, transport, or identity types so that
//! every higher layer can depend on it without a dependency cycle.

pub mod diagnostic;
pub mod dlq;
pub mod failure;
pub mod graph;
pub mod span;

pub use diagnostic::{Diagnostic, DiagnosticPayload, LabeledSpan, Severity};
pub use dlq::{DlqErrorCategory, stage_aggregate, stage_time_window};
pub use failure::{FailureCategory, FailureClassification, RetryAdvice};
pub use graph::NameGraph;
pub use span::{FileId, Span};
