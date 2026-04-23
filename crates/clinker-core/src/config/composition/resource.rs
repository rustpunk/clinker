//! Resource enum with typed payload variants.
//!
//! [`Resource`] carries the runtime data needed to resolve a resource at
//! execution time. [`ResourceKind`](super::ResourceKind) remains the
//! payload-free declaration-level tag in [`ResourceDecl`](super::ResourceDecl).

use crate::span::Span;
use std::path::PathBuf;

/// A resolved resource reference with its runtime payload.
///
/// Each variant carries the data needed to locate and open the resource at
/// execution time, plus a [`Span`] anchoring the value back to its YAML
/// source location for diagnostics.
///
/// File-only for now. No `#[non_exhaustive]` — add variants when real
/// implementations arrive.
#[derive(Debug, Clone)]
pub enum Resource {
    File {
        path: PathBuf,
        /// Source span of the `path:` value in the YAML file.
        span: Span,
    },
}

impl Resource {
    /// Returns the payload-free [`super::ResourceKind`] tag for this resource.
    pub fn kind(&self) -> super::ResourceKind {
        match self {
            Resource::File { .. } => super::ResourceKind::File,
        }
    }
}
