//! Compile-time row type for schema propagation across the DAG.
//!
//! A `Row` represents "the shape of a row flowing along this edge of the
//! DAG at plan time." Row-polymorphic per Leijen 2005 "Extensible records
//! with scoped labels": `declared` is the required minimum; `tail` is
//! either `Closed` (unknown columns are errors — used for Source rows with
//! declared `SchemaDecl`) or `Open(TailVarId)` (unknown columns bind to a
//! row variable and pass through — used at composition port boundaries per
//! LD-16c-2 / LD-16c-21).
//!
//! The authoritative definition lives here in the `cxl` crate because the
//! CXL typechecker is the primary consumer (preserves the existing
//! `clinker-core → cxl` dependency direction). `clinker-core` re-exports
//! via `plan::row_type`.

use indexmap::IndexMap;

use crate::lexer::Span;
use crate::typecheck::Type;

/// A compile-time row type carried on every node's input/output via
/// `BoundSchemas` (LD-16c-23).
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The declared (minimum-required) columns and their types.
    pub declared: IndexMap<String, Type>,
    /// Span pointing at the schema declaration site (for diagnostics).
    pub declared_span: Span,
    /// Whether unknown columns are errors or pass-through.
    pub tail: RowTail,
}

/// Row tail: closed rows reject unknown columns; open rows bind them to
/// a row variable that passes them through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowTail {
    /// Unknown columns are errors. Used for Source rows with declared
    /// `SchemaDecl`.
    Closed,
    /// Unknown columns bind to the row variable `ρ` identified by
    /// `TailVarId` and pass through. Used at composition port boundaries.
    Open(TailVarId),
}

/// Opaque identity token for the row variable `ρ` in Leijen's scoped
/// labels. Today: a monotonic u32 allocated per `bind_schema` run,
/// compared by identity. Tomorrow (17+): the ID of a unification
/// variable with occurs-check. Callers must not compare TailVarIds
/// across different `bind_schema` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TailVarId(pub u32);

/// Result of looking up a column name in a [`Row`].
#[derive(Debug, Clone)]
pub enum ColumnLookup<'a> {
    /// The column is explicitly declared with this type.
    Declared(&'a Type),
    /// The column is not declared but the row is open — it binds to the
    /// row variable and passes through.
    PassThrough(TailVarId),
    /// The column is not declared and the row is closed — this is an error.
    Unknown,
}

impl Row {
    /// Construct a closed row from declared columns.
    pub fn closed(declared: IndexMap<String, Type>, declared_span: Span) -> Self {
        Self {
            declared,
            declared_span,
            tail: RowTail::Closed,
        }
    }

    /// Construct an open row with a fresh tail variable. The caller
    /// supplies the `TailVarId` — the `bind_schema` pass owns the counter.
    pub fn open(declared: IndexMap<String, Type>, declared_span: Span, tail: TailVarId) -> Self {
        Self {
            declared,
            declared_span,
            tail: RowTail::Open(tail),
        }
    }

    /// Look up a column by name.
    ///
    /// - If `name ∈ self.declared`, returns `ColumnLookup::Declared(&ty)`.
    /// - If `name ∉ self.declared` and `self.tail == Closed`, returns
    ///   `ColumnLookup::Unknown`.
    /// - If `name ∉ self.declared` and `self.tail == Open(id)`, returns
    ///   `ColumnLookup::PassThrough(id)`.
    pub fn lookup(&self, name: &str) -> ColumnLookup<'_> {
        if let Some(ty) = self.declared.get(name) {
            ColumnLookup::Declared(ty)
        } else {
            match &self.tail {
                RowTail::Closed => ColumnLookup::Unknown,
                RowTail::Open(id) => ColumnLookup::PassThrough(*id),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span() -> Span {
        Span::new(0, 0)
    }

    #[test]
    fn test_row_closed_rejects_unknown_column() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let row = Row::closed(cols, span());
        assert!(matches!(row.lookup("b"), ColumnLookup::Unknown));
    }

    #[test]
    fn test_row_open_binds_unknown_to_tail() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let tail_id = TailVarId(42);
        let row = Row::open(cols, span(), tail_id);
        match row.lookup("b") {
            ColumnLookup::PassThrough(id) => assert_eq!(id, tail_id),
            other => panic!("expected PassThrough, got {other:?}"),
        }
    }

    #[test]
    fn test_row_declared_lookup_returns_type() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let row = Row::closed(cols, span());
        match row.lookup("a") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::String),
            other => panic!("expected Declared, got {other:?}"),
        }
    }

    #[test]
    fn test_open_row_declared_column_still_returns_declared() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::Int);
        let row = Row::open(cols, span(), TailVarId(0));
        match row.lookup("a") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::Int),
            other => panic!("expected Declared for known column in open row, got {other:?}"),
        }
    }

    #[test]
    fn test_closed_empty_row_rejects_everything() {
        let row = Row::closed(IndexMap::new(), span());
        assert!(matches!(row.lookup("anything"), ColumnLookup::Unknown));
    }

    #[test]
    fn test_tail_var_id_identity() {
        assert_eq!(TailVarId(1), TailVarId(1));
        assert_ne!(TailVarId(1), TailVarId(2));
    }
}
