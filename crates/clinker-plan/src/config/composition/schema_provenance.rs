//! Per-attribute source-schema provenance.
//!
//! Where [`ProvenanceDb`](super::provenance::ProvenanceDb) tracks composition
//! config-param values keyed by `(node, param)`, this side-table tracks each
//! resolved source-schema *attribute* keyed by `(source, column, attribute)`.
//! It answers "which overlay layer set `orders.amount.scale`, and what did the
//! shadowed layers hold?" for `clinker explain --field <source>.<col>.<attr>`.
//!
//! # Reuse, not reinvention
//!
//! The merge engine is *entirely* the generic [`ResolvedValue`] /
//! [`ProvenanceLayer`] from the sibling `provenance` module — the same
//! precedence, `fixed`-lock, and single-winner logic the composition overlay
//! uses. This module only supplies a new ordered layer kind ([`SchemaLayer`]),
//! a leaf value type ([`SchemaAttr`]), and the `(source, column, attribute)`
//! keyed table plus the recorder that drives it *at the merge* — attribution is
//! a byproduct of resolution, never reconstructed by diffing base against final.
//!
//! # Layer stack
//!
//! ```text
//! Base  <  Pipeline  <  Group  <  Channel
//! ```
//!
//! - **Base** — the source's own declared `schema:` columns (seeded from the
//!   pre-override schema).
//! - **Pipeline / Group / Channel** — the `patch_schema` overlay ops of the
//!   pipeline-default, group, and channel layers, recorded as each op applies.
//!
//! The provenance table is excluded from the plan identity hash and rebuilt on
//! every compile.

use std::collections::HashMap;
use std::fmt;

use clinker_core_types::span::Span;
use clinker_format::{Column, SourceSchema};

use super::provenance::ResolvedValue;

/// The ordered schema-override layer stack. Precedence follows the derived
/// `Ord` (declaration order): `Base < Pipeline < Group < Channel`.
///
/// A single flat `Group` layer (not one-per-matching-group) is deliberate:
/// within a layer, later application wins (`intra-layer last-wins`), so several
/// matching group `patch_schema` ops resolve last-writer-wins exactly as the
/// task's layer model specifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaLayer {
    /// The source's own declared schema.
    Base,
    /// A pipeline-default `patch_schema` op.
    Pipeline,
    /// A group overlay `patch_schema` op.
    Group,
    /// A channel overlay `patch_schema` op (channel-wide or per-target).
    Channel,
}

impl fmt::Display for SchemaLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SchemaLayer::Base => "Base",
            SchemaLayer::Pipeline => "Pipeline",
            SchemaLayer::Group => "Group",
            SchemaLayer::Channel => "Channel",
        })
    }
}

/// The resolved value of one schema-attribute leaf.
///
/// Concrete attribute values are carried as normalized JSON (the same shape the
/// composition provenance uses), so `--explain` renders them uniformly. A
/// `remove` op writes the [`Removed`](SchemaAttr::Removed) tombstone at the
/// column's presence leaf, owned by the removing layer.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaAttr {
    /// A set attribute value, normalized to JSON.
    Value(serde_json::Value),
    /// The column was removed by the owning layer (presence-leaf tombstone).
    Removed,
}

impl SchemaAttr {
    /// Wrap any serializable attribute value as a JSON leaf. Serialization of a
    /// plain schema attribute never fails; a failure is an internal invariant
    /// break, surfaced as a JSON null rather than a panic.
    fn json<T: serde::Serialize>(value: &T) -> SchemaAttr {
        SchemaAttr::Value(serde_json::to_value(value).unwrap_or(serde_json::Value::Null))
    }
}

impl fmt::Display for SchemaAttr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaAttr::Removed => f.write_str("<removed>"),
            SchemaAttr::Value(serde_json::Value::String(s)) => write!(f, "\"{s}\""),
            SchemaAttr::Value(serde_json::Value::Null) => f.write_str("null"),
            SchemaAttr::Value(v) => write!(f, "{v}"),
        }
    }
}

/// Reserved attribute name for a column's existence leaf. A `remove` op writes
/// a [`SchemaAttr::Removed`] tombstone here; every live column seeds it to
/// `true` at [`SchemaLayer::Base`] (or its adding layer). Not a real `Column`
/// attribute, so it never collides with a declared attribute name.
pub const PRESENCE_ATTR: &str = "$present";

/// A resolved schema-attribute leaf together with its full layer provenance.
pub type SchemaResolvedValue = ResolvedValue<SchemaAttr, SchemaLayer>;

/// Enumerate every present attribute of a column as `(attribute-name, JSON)`.
///
/// The single source of truth for which attributes a column contributes: used
/// to seed [`SchemaLayer::Base`], to record an `add`, and by the value/provenance
/// drift guard. `name` and `type` are always present; every other attribute is
/// listed only when set, mirroring the resolved column exactly.
pub fn column_attrs(col: &Column) -> Vec<(&'static str, serde_json::Value)> {
    // Exhaustive destructure: adding a `Column` attribute breaks compilation
    // here until it is enumerated below, so Base seeding (and thus the
    // value/provenance invariant) can never silently miss a new attribute.
    let Column {
        name,
        source_name,
        ty,
        start,
        width,
        end,
        path,
        justify,
        pad,
        trim,
        truncation,
        format,
        precision,
        scale,
        required,
        default,
        coerce,
        allowed_values,
        long_unique,
    } = col;

    let mut attrs: Vec<(&'static str, serde_json::Value)> = Vec::new();
    attrs.push(("name", serde_json::Value::String(name.clone())));
    attrs.push((
        "type",
        serde_json::to_value(ty).unwrap_or(serde_json::Value::Null),
    ));
    macro_rules! opt {
        ($field:expr, $name:literal) => {
            if let Some(v) = &$field {
                attrs.push((
                    $name,
                    serde_json::to_value(v).unwrap_or(serde_json::Value::Null),
                ));
            }
        };
    }
    opt!(source_name, "source_name");
    opt!(start, "start");
    opt!(width, "width");
    opt!(end, "end");
    opt!(path, "path");
    opt!(justify, "justify");
    opt!(pad, "pad");
    opt!(trim, "trim");
    opt!(truncation, "truncation");
    opt!(format, "format");
    opt!(precision, "precision");
    opt!(scale, "scale");
    opt!(required, "required");
    opt!(default, "default");
    opt!(coerce, "coerce");
    opt!(allowed_values, "enum");
    opt!(long_unique, "long_unique");
    attrs
}

/// Side-table mapping `(source, column, attribute)` to a provenance-tracked
/// schema-attribute value. Rebuilt each compile; excluded from the identity
/// hash (it derives neither `Serialize` nor `Hash`).
#[derive(Debug, Default, Clone)]
pub struct SchemaProvenanceDb {
    entries: HashMap<(String, String, String), SchemaResolvedValue>,
}

impl SchemaProvenanceDb {
    /// Seed [`SchemaLayer::Base`] for every column of a single-record schema.
    ///
    /// Multi-record / generated / external-file schemas expose no flat column
    /// list to attribute here and are skipped (their columns are synthesized or
    /// resolved elsewhere). Base spans are honestly synthetic: a declared
    /// column carries no retained byte span once folded into the bound schema.
    pub fn seed_base(&mut self, source: &str, schema: &SourceSchema) {
        let SourceSchema::Columns(columns) = schema else {
            return;
        };
        for col in columns {
            self.entries.insert(
                (
                    source.to_owned(),
                    col.name.clone(),
                    PRESENCE_ATTR.to_owned(),
                ),
                ResolvedValue::new(
                    SchemaAttr::Value(serde_json::Value::Bool(true)),
                    SchemaLayer::Base,
                    Span::SYNTHETIC,
                ),
            );
            for (attr, value) in column_attrs(col) {
                self.entries.insert(
                    (source.to_owned(), col.name.clone(), attr.to_owned()),
                    ResolvedValue::new(
                        SchemaAttr::Value(value),
                        SchemaLayer::Base,
                        Span::SYNTHETIC,
                    ),
                );
            }
        }
    }

    /// Merge another table's entries over this one, letting the argument win per
    /// leaf key. Used to lay the overlay-recorded layered provenance over the
    /// Base-only seed the plain compile tail produces, so a source shaped by
    /// overlay ops keeps its full layer attribution while untouched sources keep
    /// their Base seed.
    pub fn merge_over(&mut self, other: SchemaProvenanceDb) {
        for (key, value) in other.entries {
            self.entries.insert(key, value);
        }
    }

    /// Look up the resolved leaf for `(source, column, attribute)`.
    pub fn get(&self, source: &str, column: &str, attribute: &str) -> Option<&SchemaResolvedValue> {
        self.entries
            .get(&(source.to_owned(), column.to_owned(), attribute.to_owned()))
    }

    /// Distinct attribute names tracked for `(source, column)`, sorted.
    pub fn attrs_for(&self, source: &str, column: &str) -> Vec<&str> {
        let mut attrs: Vec<&str> = self
            .entries
            .keys()
            .filter(|(s, c, _)| s == source && c == column)
            .map(|(_, _, a)| a.as_str())
            .collect();
        attrs.sort_unstable();
        attrs
    }

    /// Distinct column names tracked for `source`, sorted.
    pub fn columns_for(&self, source: &str) -> Vec<&str> {
        let mut cols: Vec<&str> = self
            .entries
            .keys()
            .filter(|(s, _, _)| s == source)
            .map(|(_, c, _)| c.as_str())
            .collect();
        cols.sort_unstable();
        cols.dedup();
        cols
    }

    /// Distinct source names tracked, sorted.
    pub fn sources(&self) -> Vec<&str> {
        let mut srcs: Vec<&str> = self.entries.keys().map(|(s, _, _)| s.as_str()).collect();
        srcs.sort_unstable();
        srcs.dedup();
        srcs
    }

    /// Number of tracked leaves.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Apply one layer's contribution to a leaf, at the merge. Creates the leaf
    /// if no lower layer seeded it (an attribute introduced by an override),
    /// else layers onto the existing provenance chain.
    fn apply(
        &mut self,
        source: &str,
        column: &str,
        attr: &str,
        value: SchemaAttr,
        layer: SchemaLayer,
        span: Span,
    ) {
        let key = (source.to_owned(), column.to_owned(), attr.to_owned());
        match self.entries.get_mut(&key) {
            Some(resolved) => resolved.apply_layer(value, layer, span),
            None => {
                self.entries
                    .insert(key, ResolvedValue::new(value, layer, span));
            }
        }
    }

    /// Drop every leaf of `(source, column)`. Used by a `remove` before writing
    /// the presence tombstone, so the removed column carries no stale attribute
    /// provenance.
    fn drop_column(&mut self, source: &str, column: &str) {
        self.entries
            .retain(|(s, c, _), _| !(s == source && c == column));
    }

    /// Move every leaf of `(source, from)` onto `(source, to)`, preserving each
    /// leaf's provenance chain. Used by a `rename` so the renamed column's
    /// shadowed Base values stay queryable under the new exposed name (the name
    /// the bound schema carries).
    fn rekey_column(&mut self, source: &str, from: &str, to: &str) {
        if from == to {
            return;
        }
        let moved: Vec<((String, String, String), SchemaResolvedValue)> = self
            .entries
            .keys()
            .filter(|(s, c, _)| s == source && c == from)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|k| self.entries.remove(&k).map(|v| (k, v)))
            .collect();
        for ((s, _, a), v) in moved {
            self.entries.insert((s, to.to_owned(), a), v);
        }
    }
}

/// Records one `patch_schema` op batch's contributions into a shared
/// [`SchemaProvenanceDb`] at a single `(source, layer, span)`.
///
/// Constructed per op (each `patch_schema` op is one layer + one span, over one
/// source), borrowing the shared db. Recording happens inside the same
/// `apply_schema_ops` pass that mutates the schema, so the resolved value and
/// its attribution can never drift.
pub struct SchemaProvRecorder<'a> {
    db: &'a mut SchemaProvenanceDb,
    source: String,
    layer: SchemaLayer,
    span: Span,
}

impl<'a> SchemaProvRecorder<'a> {
    /// A recorder writing to `db` for `source` at `layer`, anchoring every leaf
    /// to `span` (the op's source location).
    pub fn new(
        db: &'a mut SchemaProvenanceDb,
        source: &str,
        layer: SchemaLayer,
        span: Span,
    ) -> Self {
        Self {
            db,
            source: source.to_owned(),
            layer,
            span,
        }
    }

    /// Record a set attribute value on `column`.
    pub fn set(&mut self, column: &str, attr: &str, value: &serde_json::Value) {
        self.db.apply(
            &self.source,
            column,
            attr,
            SchemaAttr::Value(value.clone()),
            self.layer,
            self.span,
        );
    }

    /// Record a set attribute value from any serializable attribute.
    pub fn set_attr<T: serde::Serialize>(&mut self, column: &str, attr: &str, value: &T) {
        self.db.apply(
            &self.source,
            column,
            attr,
            SchemaAttr::json(value),
            self.layer,
            self.span,
        );
    }

    /// Record a whole new column (`add`): its presence plus every present
    /// attribute, all owned by this layer.
    pub fn add_column(&mut self, col: &Column) {
        self.db.apply(
            &self.source,
            &col.name,
            PRESENCE_ATTR,
            SchemaAttr::Value(serde_json::Value::Bool(true)),
            self.layer,
            self.span,
        );
        for (attr, value) in column_attrs(col) {
            self.set(&col.name, attr, &value);
        }
    }

    /// Record a column removal: drop the column's attribute leaves (a removed
    /// column has no resolved attributes) and leave a single presence-leaf
    /// [`SchemaAttr::Removed`] tombstone owned by this layer. Dropping the
    /// attribute leaves keeps `explain --field` from reporting a live value for
    /// a column that no longer exists in the bound schema.
    pub fn remove_column(&mut self, column: &str) {
        self.db.drop_column(&self.source, column);
        self.db.apply(
            &self.source,
            column,
            PRESENCE_ATTR,
            SchemaAttr::Removed,
            self.layer,
            self.span,
        );
    }

    /// Record a rename: move the column's leaves onto the new name, then set the
    /// new exposed `name` and the preserved physical `source_name` binding.
    pub fn rename_column(&mut self, from: &str, to: &str, physical: &str) {
        self.db.rekey_column(&self.source, from, to);
        self.set(to, "name", &serde_json::Value::String(to.to_owned()));
        self.set(
            to,
            "source_name",
            &serde_json::Value::String(physical.to_owned()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::Column;
    use cxl::typecheck::Type;

    fn columns(cols: Vec<Column>) -> SourceSchema {
        SourceSchema::Columns(cols)
    }

    fn winner(db: &SchemaProvenanceDb, s: &str, c: &str, a: &str) -> (SchemaLayer, SchemaAttr) {
        let r = db.get(s, c, a).expect("leaf present");
        (r.winning_layer().unwrap().kind, r.value.clone())
    }

    /// Base seeds every present attribute; a higher layer wins cross-layer.
    #[test]
    fn cross_layer_precedence() {
        let mut db = SchemaProvenanceDb::default();
        db.seed_base("src", &columns(vec![Column::bare("amount", Type::Int)]));
        // Base holds type=int.
        assert_eq!(
            winner(&db, "src", "amount", "type"),
            (SchemaLayer::Base, SchemaAttr::json(&Type::Int))
        );
        // Channel retypes to float.
        let mut rec =
            SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Channel, Span::line_only(12));
        rec.set_attr("amount", "type", &Type::Float);
        assert_eq!(
            winner(&db, "src", "amount", "type"),
            (SchemaLayer::Channel, SchemaAttr::json(&Type::Float))
        );
    }

    /// A later application within one layer replaces the earlier (last-wins).
    #[test]
    fn intra_layer_last_wins() {
        let mut db = SchemaProvenanceDb::default();
        db.seed_base("src", &columns(vec![Column::bare("amount", Type::Int)]));
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Group, Span::line_only(1));
            rec.set_attr("amount", "scale", &2u8);
        }
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Group, Span::line_only(9));
            rec.set_attr("amount", "scale", &4u8);
        }
        assert_eq!(
            winner(&db, "src", "amount", "scale"),
            (SchemaLayer::Group, SchemaAttr::json(&4u8))
        );
        // The winning span is the later application.
        assert_eq!(
            db.get("src", "amount", "scale")
                .unwrap()
                .winning_layer()
                .unwrap()
                .span,
            Span::line_only(9)
        );
    }

    /// A partial override attributes two leaves of ONE field to two layers.
    #[test]
    fn partial_override_two_leaves_two_layers() {
        let mut db = SchemaProvenanceDb::default();
        db.seed_base(
            "src",
            &columns(vec![Column {
                scale: Some(0),
                ..Column::bare("amount", Type::Int)
            }]),
        );
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Pipeline, Span::line_only(3));
            rec.set_attr("amount", "type", &Type::Float);
        }
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Channel, Span::line_only(7));
            rec.set_attr("amount", "scale", &2u8);
        }
        assert_eq!(
            winner(&db, "src", "amount", "type").0,
            SchemaLayer::Pipeline,
            "type set by pipeline"
        );
        assert_eq!(
            winner(&db, "src", "amount", "scale").0,
            SchemaLayer::Channel,
            "scale set by channel"
        );
    }

    /// `remove` writes a presence tombstone owned by the removing layer.
    #[test]
    fn remove_writes_tombstone() {
        let mut db = SchemaProvenanceDb::default();
        db.seed_base("src", &columns(vec![Column::bare("notes", Type::String)]));
        assert_eq!(
            winner(&db, "src", "notes", PRESENCE_ATTR),
            (
                SchemaLayer::Base,
                SchemaAttr::Value(serde_json::Value::Bool(true))
            )
        );
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Channel, Span::line_only(5));
            rec.remove_column("notes");
        }
        assert_eq!(
            winner(&db, "src", "notes", PRESENCE_ATTR),
            (SchemaLayer::Channel, SchemaAttr::Removed)
        );
        // The removed column's attribute leaves are dropped — no stale value for
        // a column that no longer exists in the bound schema.
        assert!(db.get("src", "notes", "type").is_none());
        assert!(db.get("src", "notes", "name").is_none());
    }

    /// `rename` re-keys leaves to the new name and records from→to plus the
    /// preserved physical `source_name`.
    #[test]
    fn rename_records_from_to_and_physical() {
        let mut db = SchemaProvenanceDb::default();
        db.seed_base("src", &columns(vec![Column::bare("cust_id", Type::String)]));
        {
            let mut rec =
                SchemaProvRecorder::new(&mut db, "src", SchemaLayer::Channel, Span::line_only(4));
            rec.rename_column("cust_id", "customer_id", "cust_id");
        }
        // New exposed name won by Channel; shadowed Base still holds the old name.
        let name = db.get("src", "customer_id", "name").expect("name leaf");
        assert_eq!(name.winning_layer().unwrap().kind, SchemaLayer::Channel);
        assert_eq!(
            name.value,
            SchemaAttr::Value(serde_json::Value::String("customer_id".into()))
        );
        assert_eq!(
            name.layer_value(SchemaLayer::Base),
            Some(&SchemaAttr::Value(serde_json::Value::String(
                "cust_id".into()
            )))
        );
        // Physical binding preserved on source_name.
        assert_eq!(
            winner(&db, "src", "customer_id", "source_name"),
            (
                SchemaLayer::Channel,
                SchemaAttr::Value(serde_json::Value::String("cust_id".into()))
            )
        );
        // Old key no longer resolves.
        assert!(db.get("src", "cust_id", "name").is_none());
    }

    /// Merging overlay-recorded entries over a Base-only seed keeps untouched
    /// sources' Base attribution and replaces the shaped source's leaves.
    #[test]
    fn merge_over_replaces_per_leaf() {
        let mut base = SchemaProvenanceDb::default();
        base.seed_base("a", &columns(vec![Column::bare("x", Type::Int)]));
        base.seed_base("b", &columns(vec![Column::bare("y", Type::Int)]));

        let mut overlay = SchemaProvenanceDb::default();
        overlay.seed_base("a", &columns(vec![Column::bare("x", Type::Int)]));
        {
            let mut rec = SchemaProvRecorder::new(
                &mut overlay,
                "a",
                SchemaLayer::Channel,
                Span::line_only(2),
            );
            rec.set_attr("x", "type", &Type::Float);
        }

        base.merge_over(overlay);
        // Source a leaf came from overlay (Channel wins).
        assert_eq!(winner(&base, "a", "x", "type").0, SchemaLayer::Channel);
        // Source b untouched (Base).
        assert_eq!(winner(&base, "b", "y", "type").0, SchemaLayer::Base);
    }
}
