//! Re-export of the CXL row type for ergonomic access from `clinker-core`.
//!
//! The authoritative definition lives in `cxl::typecheck::row` (the CXL
//! typechecker is the primary consumer, preserving the `clinker-core → cxl`
//! dependency direction). This module provides a convenience re-export.

pub use cxl::typecheck::row::{ColumnLookup, QualifiedField, Row, RowTail, TailVarId};
