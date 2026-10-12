//! Compile-time row type for schema propagation across the DAG.
//!
//! A `Row` represents "the shape of a row flowing along this edge of the
//! DAG at plan time." Row-polymorphic per Leijen 2005 "Extensible records
//! with scoped labels": `declared` is the required minimum; `tail` is
//! either `Closed` (unknown columns are errors — used for Source rows with
//! declared `SchemaDecl`) or `Open(TailVarId)` (unknown columns bind to a
//! row variable and pass through — used at composition port boundaries
//! where the caller's extra columns must flow through unchanged).
//!
//! The authoritative definition lives here in the `cxl` crate because the
//! CXL typechecker is the primary consumer (preserves the existing
//! `clinker-plan → cxl` dependency direction). `clinker-plan` re-exports
//! via `plan::row_type`.
//!
//! # QualifiedField
//!
//! `Row.declared` is keyed by `QualifiedField { qualifier, name }` rather
//! than raw `String`. For non-combine nodes (Source / Transform /
//! Aggregate / Route / Output / Composition) the `qualifier` is `None` —
//! these are "bare" fields and existing behaviour is preserved. For the
//! Combine node's merged input row, each field is qualified by the
//! combine's named input (e.g. `QualifiedField::qualified("orders",
//! "product_id")`) so the typechecker can resolve 2-part
//! `Expr::QualifiedFieldRef` references unambiguously.
//!
//! The `declared` map is **private**. All access goes through methods
//! (`lookup`, `lookup_qualified`, `has_field`, `field_count`, `fields`,
//! `field_names`, `into_declared`). This matches DataFusion's DFSchema
//! pattern (method-based access, not a raw field), and decouples callers
//! from the `IndexMap<QualifiedField, Type>` representation.

use std::fmt;
use std::sync::Arc;

use indexmap::IndexMap;

use crate::lexer::Span;
use crate::typecheck::Type;

/// Structured field identifier with optional input qualifier.
///
/// For non-combine nodes, `qualifier` is `None` — the field is "bare" and
/// looked up by name alone. For combine merged rows, `qualifier` identifies
/// which named input the field originates from, e.g.
/// `QualifiedField::qualified("orders", "product_id")`.
///
/// Displays as `qualifier.name` when qualified, bare `name` otherwise. The
/// pair `(qualifier, name)` is the identity used for `Hash` / `Eq`, so
/// `orders.id` and `products.id` are distinct keys in a merged row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedField {
    /// Input qualifier (e.g. "orders", "products"). `None` for
    /// unqualified fields.
    pub qualifier: Option<Arc<str>>,
    /// Field name (e.g. "product_id", "amount").
    pub name: Arc<str>,
}

impl QualifiedField {
    /// Bare (unqualified) field — used by Source, Transform, Aggregate,
    /// Route, Output, and Composition nodes.
    pub fn bare(name: impl Into<Arc<str>>) -> Self {
        Self {
            qualifier: None,
            name: name.into(),
        }
    }

    /// Qualified field — used in Combine merged rows.
    pub fn qualified(qualifier: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            qualifier: Some(qualifier.into()),
            name: name.into(),
        }
    }
}

impl fmt::Display for QualifiedField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.qualifier {
            Some(q) => write!(f, "{}.{}", q, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

impl From<&str> for QualifiedField {
    fn from(name: &str) -> Self {
        Self::bare(name)
    }
}

impl From<String> for QualifiedField {
    fn from(name: String) -> Self {
        Self::bare(name)
    }
}

impl From<&String> for QualifiedField {
    fn from(name: &String) -> Self {
        Self::bare(name.as_str())
    }
}

/// A compile-time row type threaded through the `bind_schema` walk
/// and attached to each node's `TypedProgram.output_row`. Carries the
/// declared-columns map plus the `Open`/`Closed` tail that governs
/// pass-through semantics for unknown columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Declared (minimum-required) columns and their types.
    ///
    /// Private per drill decision 13 — all access goes through the
    /// `lookup`, `has_field`, `field_count`, `fields`, `field_names`, and
    /// `into_declared` methods. Matches DataFusion DFSchema pattern.
    declared: IndexMap<QualifiedField, Type>,
    /// Span pointing at the schema declaration site (for diagnostics).
    pub declared_span: Span,
    /// Whether unknown columns are errors or pass-through.
    pub tail: RowTail,
    /// Columns that entered this row through a composition input port, each
    /// with the port it came through and whether the body may name it. Every
    /// other column belongs to the scope that binds the row. Private: a
    /// carried column is read through [`Row::carried`] and refused by
    /// [`Row::lookup`] when the port did not declare it.
    carried: IndexMap<QualifiedField, CarriedColumn>,
}

/// A column a composition body received from its caller, unchanged.
///
/// The body binds against the caller's row, so the column sits in the
/// caller's position with the caller's value. Whether the body may name it
/// is the port's declaration: a column the port does not declare passes
/// through unnamed. A column the body writes is the body's own and is no
/// longer carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarriedColumn {
    /// The composition call and input port the column entered through.
    pub port: Arc<PortOrigin>,
    /// What the port's declaration lets the body do with the column.
    pub access: PortAccess,
}

/// The composition call site and input port a carried column entered
/// through. Opaque to CXL; the planner reads it to word its diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortOrigin {
    /// The composition node's name at the call site.
    pub call: Arc<str>,
    /// The input port the column came through.
    pub port: Arc<str>,
    /// The composition file, as the author refers to it.
    pub body_file: Arc<str>,
}

/// How a port's declaration exposes one caller column to the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortAccess {
    /// The port does not declare the column: it passes through, unnamed.
    Undeclared,
    /// The port declares the column with a concrete type.
    Declared,
    /// The port declares the column as `any`: the body sees the type this
    /// call provides.
    DeclaredAny,
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
    /// Multiple declared fields match the unqualified name. Carries the
    /// full list of matching `QualifiedField`s (qualified + bare) so
    /// the caller can render a precise "ambiguous — try `orders.id` or
    /// `products.id`" diagnostic. Only produced by combine merged rows;
    /// in all other contexts every field has a unique bare name so
    /// `lookup` resolves to `Declared` or tails out.
    Ambiguous(Vec<QualifiedField>),
}

impl Row {
    /// Construct a closed row from declared columns.
    pub fn closed(declared: IndexMap<QualifiedField, Type>, declared_span: Span) -> Self {
        Self {
            declared,
            declared_span,
            tail: RowTail::Closed,
            carried: IndexMap::new(),
        }
    }

    /// Construct an open row with a fresh tail variable. The caller
    /// supplies the `TailVarId` — the `bind_schema` pass owns the counter.
    pub fn open(
        declared: IndexMap<QualifiedField, Type>,
        declared_span: Span,
        tail: TailVarId,
    ) -> Self {
        Self {
            declared,
            declared_span,
            tail: RowTail::Open(tail),
            carried: IndexMap::new(),
        }
    }

    /// Construct a row from explicit parts. Used by row-propagation
    /// helpers that need to preserve an arbitrary upstream tail.
    pub fn from_parts(
        declared: IndexMap<QualifiedField, Type>,
        declared_span: Span,
        tail: RowTail,
    ) -> Self {
        Self {
            declared,
            declared_span,
            tail,
            carried: IndexMap::new(),
        }
    }

    /// Look up a column by unqualified name. Searches declared fields by
    /// `.name` regardless of qualifier.
    ///
    /// - Zero matches: falls through to the row's tail — `Unknown` for a
    ///   closed row, `PassThrough(id)` for an open row.
    /// - Exactly one match: returns `Declared(&ty)`.
    /// - Multiple matches: returns
    ///   `Ambiguous(matching_fields)` carrying every matching field
    ///   (qualified or bare) so the caller can render a precise
    ///   "ambiguous — try `orders.id` or `products.id`" diagnostic.
    ///   This only arises for combine merged rows.
    ///
    /// A column carried through a composition port that does not declare it
    /// is never found: it is [`ColumnLookup::Unknown`], whatever the tail.
    /// The planner tells that case apart with [`Row::undeclared_carried`].
    pub fn lookup(&self, name: &str) -> ColumnLookup<'_> {
        let matches: Vec<(&QualifiedField, &Type)> = self
            .declared
            .iter()
            .filter(|(qf, _)| qf.name.as_ref() == name)
            .collect();
        match matches.len() {
            0 => match &self.tail {
                RowTail::Closed => ColumnLookup::Unknown,
                RowTail::Open(id) => ColumnLookup::PassThrough(*id),
            },
            1 if self.is_undeclared_carried(matches[0].0) => ColumnLookup::Unknown,
            1 => ColumnLookup::Declared(matches[0].1),
            _ => ColumnLookup::Ambiguous(matches.iter().map(|(qf, _)| (*qf).clone()).collect()),
        }
    }

    /// Look up a column by qualifier + name. Exact match on both sides.
    /// Used by the CXL typechecker for 2-part `Expr::QualifiedFieldRef`
    /// resolution inside a combine body / where-clause.
    ///
    /// A carried column its port does not declare is [`ColumnLookup::Unknown`].
    pub fn lookup_qualified(&self, qualifier: &str, name: &str) -> ColumnLookup<'_> {
        for (qf, ty) in &self.declared {
            if qf.qualifier.as_deref() == Some(qualifier) && qf.name.as_ref() == name {
                if self.is_undeclared_carried(qf) {
                    return ColumnLookup::Unknown;
                }
                return ColumnLookup::Declared(ty);
            }
        }
        match &self.tail {
            RowTail::Closed => ColumnLookup::Unknown,
            RowTail::Open(id) => ColumnLookup::PassThrough(*id),
        }
    }

    /// True if exactly one declared field matches `name` (ambiguous and
    /// pass-through both return false). Drop-in replacement for
    /// `row.declared.contains_key(name)` used throughout the codebase.
    pub fn has_field(&self, name: &str) -> bool {
        matches!(self.lookup(name), ColumnLookup::Declared(_))
    }

    /// Number of declared fields.
    pub fn field_count(&self) -> usize {
        self.declared.len()
    }

    /// Iterate over `(QualifiedField, Type)` pairs in declaration order.
    pub fn fields(&self) -> indexmap::map::Iter<'_, QualifiedField, Type> {
        self.declared.iter()
    }

    /// Iterate over field identifiers only, in declaration order.
    pub fn field_names(&self) -> indexmap::map::Keys<'_, QualifiedField, Type> {
        self.declared.keys()
    }

    /// Consume the Row, returning the declared field map. Escape hatch
    /// for schema serialization and migration code that needs to
    /// reconstruct a new Row from the raw map.
    pub fn into_declared(self) -> IndexMap<QualifiedField, Type> {
        self.declared
    }

    /// Borrow the declared field map. Used by row-propagation helpers
    /// that clone the upstream map to seed output rows. Prefer the
    /// iterator / lookup methods when possible.
    pub fn declared_map(&self) -> &IndexMap<QualifiedField, Type> {
        &self.declared
    }

    /// Replace the type of the declared field `field`, keeping its position.
    /// Returns `false` (and changes nothing) when the row has no such field.
    pub fn retype(&mut self, field: &QualifiedField, ty: Type) -> bool {
        match self.declared.get_mut(field) {
            Some(slot) => {
                *slot = ty;
                true
            }
            None => false,
        }
    }

    /// Mark the declared field `field` as carried through a composition
    /// port. A field the row does not declare is ignored.
    pub fn carry(&mut self, field: QualifiedField, column: CarriedColumn) {
        if self.declared.contains_key(&field) {
            self.carried.insert(field, column);
        }
    }

    /// Forget that `field` was carried: a node wrote its own column under
    /// that name, so the column is the body's from here on.
    pub fn release(&mut self, field: &QualifiedField) {
        self.carried.shift_remove(field);
    }

    /// Copy every carried mark of `upstream` onto the field of the same key
    /// in this row, for each field `keep` admits. A node whose output
    /// passes some of its input's columns through unchanged calls this so
    /// the marks travel with the columns.
    pub fn inherit_carried(&mut self, upstream: &Row, keep: impl Fn(&QualifiedField) -> bool) {
        for (field, column) in &upstream.carried {
            if keep(field) {
                self.carry(field.clone(), column.clone());
            }
        }
    }

    /// The carried mark on `field`, if it entered through a composition port.
    pub fn carried(&self, field: &QualifiedField) -> Option<&CarriedColumn> {
        self.carried.get(field)
    }

    /// Every carried column, in the order they were marked.
    pub fn carried_columns(&self) -> indexmap::map::Iter<'_, QualifiedField, CarriedColumn> {
        self.carried.iter()
    }

    /// The carried mark of the column a bare `name` resolves to, when that
    /// column entered through a port that does not declare it: the case
    /// [`Row::lookup`] reports as `Unknown` although the row carries it.
    pub fn undeclared_carried(&self, name: &str) -> Option<&CarriedColumn> {
        let mut matches = self.declared.keys().filter(|qf| qf.name.as_ref() == name);
        let field = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        self.carried
            .get(field)
            .filter(|column| column.access == PortAccess::Undeclared)
    }

    /// [`Row::undeclared_carried`] for an exact `(qualifier, name)` pair.
    pub fn undeclared_carried_qualified(
        &self,
        qualifier: &str,
        name: &str,
    ) -> Option<&CarriedColumn> {
        self.carried.iter().find_map(|(qf, column)| {
            (qf.qualifier.as_deref() == Some(qualifier)
                && qf.name.as_ref() == name
                && column.access == PortAccess::Undeclared)
                .then_some(column)
        })
    }

    fn is_undeclared_carried(&self, field: &QualifiedField) -> bool {
        self.carried
            .get(field)
            .is_some_and(|column| column.access == PortAccess::Undeclared)
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
        cols.insert(QualifiedField::bare("a"), Type::String);
        let row = Row::closed(cols, span());
        assert!(matches!(row.lookup("b"), ColumnLookup::Unknown));
    }

    #[test]
    fn test_row_open_binds_unknown_to_tail() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("a"), Type::String);
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
        cols.insert(QualifiedField::bare("a"), Type::String);
        let row = Row::closed(cols, span());
        match row.lookup("a") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::String),
            other => panic!("expected Declared, got {other:?}"),
        }
    }

    #[test]
    fn test_open_row_declared_column_still_returns_declared() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("a"), Type::Int);
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

    // --- Phase Combine C.1.0 gate tests ----------------------------------

    /// C.1.0 gate: `QualifiedField::bare` lookup works like the legacy
    /// `String`-keyed lookup — single-entry row with a bare field
    /// resolves by unqualified name.
    #[test]
    fn test_qualified_field_bare_lookup() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("a"), Type::String);
        let row = Row::closed(cols, span());
        match row.lookup("a") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::String),
            other => panic!("expected Declared, got {other:?}"),
        }
    }

    /// C.1.0 gate: `lookup_qualified` resolves a qualified field by
    /// `(qualifier, name)` pair. Two fields share the same `name` but
    /// belong to different input qualifiers.
    #[test]
    fn test_qualified_field_qualified_lookup() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::qualified("orders", "id"), Type::Int);
        cols.insert(QualifiedField::qualified("products", "id"), Type::Int);
        let row = Row::closed(cols, span());
        match row.lookup_qualified("orders", "id") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::Int),
            other => panic!("expected Declared, got {other:?}"),
        }
        match row.lookup_qualified("products", "id") {
            ColumnLookup::Declared(ty) => assert_eq!(*ty, Type::Int),
            other => panic!("expected Declared, got {other:?}"),
        }
    }

    /// C.1.0 gate: unqualified `lookup` over a merged row with two
    /// fields sharing the same name under different qualifiers returns
    /// `ColumnLookup::Ambiguous` carrying the full `QualifiedField`
    /// entries for every match (qualified + bare).
    #[test]
    fn test_qualified_field_ambiguous_unqualified() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::qualified("orders", "id"), Type::Int);
        cols.insert(QualifiedField::qualified("products", "id"), Type::Int);
        let row = Row::closed(cols, span());
        match row.lookup("id") {
            ColumnLookup::Ambiguous(matches) => {
                assert_eq!(matches.len(), 2);
                let labels: Vec<String> = matches.iter().map(|qf| qf.to_string()).collect();
                assert!(labels.contains(&"orders.id".to_string()));
                assert!(labels.contains(&"products.id".to_string()));
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }

    /// Ambiguous also fires when one match is bare and one is qualified
    /// — the carried list includes BOTH so the diagnostic can suggest
    /// both paths.
    #[test]
    fn test_lookup_ambiguous_mixed_bare_and_qualified() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("id"), Type::Int);
        cols.insert(QualifiedField::qualified("orders", "id"), Type::Int);
        let row = Row::closed(cols, span());
        match row.lookup("id") {
            ColumnLookup::Ambiguous(matches) => {
                assert_eq!(matches.len(), 2);
                let labels: Vec<String> = matches.iter().map(|qf| qf.to_string()).collect();
                assert!(labels.contains(&"id".to_string()));
                assert!(labels.contains(&"orders.id".to_string()));
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }

    /// `QualifiedField::qualified` displays as `qualifier.name`;
    /// bare displays as `name`.
    #[test]
    fn test_qualified_field_display() {
        let qf = QualifiedField::qualified("orders", "id");
        assert_eq!(format!("{qf}"), "orders.id");
        let bare = QualifiedField::bare("id");
        assert_eq!(format!("{bare}"), "id");
    }

    fn carried(access: PortAccess) -> CarriedColumn {
        CarriedColumn {
            port: Arc::new(PortOrigin {
                call: "call".into(),
                port: "inp".into(),
                body_file: "c.comp.yaml".into(),
            }),
            access,
        }
    }

    /// A column a port carries undeclared is in the row, in its position,
    /// and cannot be looked up; a declared one can.
    #[test]
    fn an_undeclared_carried_column_is_not_found_by_name() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("a"), Type::Int);
        cols.insert(QualifiedField::bare("b"), Type::String);
        let mut row = Row::closed(cols, span());
        row.carry(QualifiedField::bare("a"), carried(PortAccess::Undeclared));
        row.carry(QualifiedField::bare("b"), carried(PortAccess::Declared));
        assert!(matches!(row.lookup("a"), ColumnLookup::Unknown));
        assert!(!row.has_field("a"));
        assert!(row.undeclared_carried("a").is_some());
        assert!(matches!(
            row.lookup("b"),
            ColumnLookup::Declared(Type::String)
        ));
        assert!(row.undeclared_carried("b").is_none());
        assert_eq!(row.field_count(), 2, "the column stays in the row");
        row.release(&QualifiedField::bare("a"));
        assert!(matches!(row.lookup("a"), ColumnLookup::Declared(Type::Int)));
    }

    /// Qualified lookup refuses an undeclared carried column of one input
    /// and finds the same name on another.
    #[test]
    fn an_undeclared_carried_column_is_not_found_by_qualified_name() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::qualified("a", "x"), Type::Int);
        cols.insert(QualifiedField::qualified("b", "x"), Type::Int);
        let mut row = Row::closed(cols, span());
        row.carry(
            QualifiedField::qualified("a", "x"),
            carried(PortAccess::Undeclared),
        );
        assert!(matches!(
            row.lookup_qualified("a", "x"),
            ColumnLookup::Unknown
        ));
        assert!(row.undeclared_carried_qualified("a", "x").is_some());
        assert!(matches!(
            row.lookup_qualified("b", "x"),
            ColumnLookup::Declared(Type::Int)
        ));
    }

    /// `has_field` and `field_count` on a row with two bare fields.
    #[test]
    fn test_has_field_and_field_count() {
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::bare("a"), Type::Int);
        cols.insert(QualifiedField::bare("b"), Type::String);
        let row = Row::closed(cols, span());
        assert_eq!(row.field_count(), 2);
        assert!(row.has_field("a"));
        assert!(row.has_field("b"));
        assert!(!row.has_field("c"));
    }
}
