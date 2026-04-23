//! Runtime field resolution for the combine executor arm.
//!
//! The combine executor stitches together a *probe* (driver) record and
//! a matched *build* record from a hash table. CXL bodies and the
//! residual predicate see a merged view via [`CombineResolver`], which
//! implements [`FieldResolver`] over both sides.
//!
//! Field resolution is compile-time resolved, runtime direct-access:
//! [`CombineResolverMapping`] is built once per combine at executor
//! start-up and walks every input's declared row to record, for every
//! `QualifiedField` visible in the merged row, which side it lives on
//! and the bare field name needed to pluck it out of the underlying
//! [`Record`]. At probe time [`CombineResolver`] borrows the mapping
//! plus the current pair of records — no per-record string parsing of
//! `source.field` and no re-walking of the row schema.
//!
//! The build slot is an [`Option`] so `on_miss: null_fields` can drive
//! the same resolver with `build_record: None`; qualified lookups
//! against the missing side return `Some(Value::Null)` instead of
//! `None`, matching the existing `LookupResolver::no_match` contract
//! (fields exist but have no value, rather than the field being absent
//! from the row).

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{FieldResolver, Record, Value};
use indexmap::IndexMap;

use crate::plan::combine::CombineInput;
use crate::plan::row_type::QualifiedField;

/// Which side of a combine a matched record came from.
///
/// `Probe` is the driver (streaming) side; `Build` is the materialized
/// hash-table side. A combine has exactly one probe qualifier and one
/// or more build qualifiers (currently gated to one at the strategy
/// post-pass via E312; N>2 lands in a later combine phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Build,
    Probe,
}

/// Compile-time resolution table built once per combine node.
///
/// Holds two indexes derived from the combine's declared inputs:
///
/// - `qualified`: every `QualifiedField` visible in the merged row
///   maps to `(side, bare_name)` — the side tells the resolver which
///   record to reach into; `bare_name` is the unqualified field name
///   on that record's own schema.
/// - `bare_to_side`: bare field name → side. Bare CXL references
///   (e.g. `product_id`) resolve through this table when exactly one
///   input declares that name; an entry is absent when two or more
///   inputs declare the same bare name, and `resolve` returns `None`
///   rather than silently preferring a side.
///
/// Rationale for indexing bare names here rather than deferring to a
/// linear scan: the typechecker already rejects ambiguous bare
/// references at compile time via [`crate::plan::row_type::ColumnLookup::Ambiguous`],
/// so a runtime bare lookup hitting an ambiguous name is a
/// typechecker bug. Returning `None` surfaces the bug as a missing
/// value rather than a silent side-preference choice.
pub(crate) struct CombineResolverMapping {
    /// `(qualifier, name)` → which side + bare field name on that side.
    qualified: HashMap<QualifiedField, (JoinSide, Arc<str>)>,
    /// Unambiguous bare name → side + bare field name. Absent when the
    /// name appears on 2+ inputs.
    bare_to_side: HashMap<Arc<str>, (JoinSide, Arc<str>)>,
    /// Every qualified field name, rendered once as `qualifier.name`
    /// for `available_fields` diagnostic output. Owned so the returned
    /// `&str` borrows tie to `&self`.
    available: Vec<String>,
}

impl CombineResolverMapping {
    /// Build a mapping from a combine's declared inputs and the driver
    /// qualifier. Walks every input's row, assigning [`JoinSide::Probe`]
    /// to fields under `driver_qualifier` and [`JoinSide::Build`] to
    /// every other input's fields.
    ///
    /// Panics in debug if `driver_qualifier` is not present in
    /// `combine_inputs`; in release it produces a mapping with no
    /// probe-side fields, which the executor will notice when every
    /// probe-side resolve returns `None`.
    pub(crate) fn new(
        combine_inputs: &IndexMap<String, CombineInput>,
        driver_qualifier: &str,
    ) -> Self {
        debug_assert!(
            combine_inputs.contains_key(driver_qualifier),
            "CombineResolverMapping::new: driver qualifier {driver_qualifier:?} absent from inputs"
        );

        let mut qualified: HashMap<QualifiedField, (JoinSide, Arc<str>)> = HashMap::new();
        let mut bare_counts: HashMap<Arc<str>, usize> = HashMap::new();
        let mut bare_first: HashMap<Arc<str>, (JoinSide, Arc<str>)> = HashMap::new();
        let mut available: Vec<String> = Vec::new();

        for (qualifier, input) in combine_inputs {
            let side = if qualifier == driver_qualifier {
                JoinSide::Probe
            } else {
                JoinSide::Build
            };
            for (field, _ty) in input.row.fields() {
                let bare = Arc::clone(&field.name);
                let qf = QualifiedField::qualified(qualifier.as_str(), Arc::clone(&bare));
                available.push(format!("{qualifier}.{bare}"));
                qualified.insert(qf, (side, Arc::clone(&bare)));
                *bare_counts.entry(Arc::clone(&bare)).or_insert(0) += 1;
                bare_first.entry(Arc::clone(&bare)).or_insert((side, bare));
            }
        }

        let bare_to_side = bare_first
            .into_iter()
            .filter(|(k, _)| bare_counts.get(k).copied() == Some(1))
            .collect();

        Self {
            qualified,
            bare_to_side,
            available,
        }
    }
}

/// Field resolver over a combine's probe + build record pair.
///
/// Borrows a precomputed [`CombineResolverMapping`] and the two records
/// for the current probe iteration. The build record is optional to
/// support `on_miss: null_fields`: when `None`, qualified lookups of
/// build-side fields return `Some(Value::Null)` (the field exists in
/// the merged row but has no underlying value), matching the existing
/// `LookupResolver::no_match` contract for the scalar-lookup case.
///
/// Bare-name lookup contract (option α): unambiguous bare names route
/// to the side declared by the mapping; ambiguous bare names return
/// `None`. The typechecker rejects ambiguous bare references at
/// compile time, so hitting `None` on a bare lookup at runtime
/// indicates a typechecker bug rather than a silent side-preference
/// choice. The driver-side does not automatically win.
pub(crate) struct CombineResolver<'a> {
    mapping: &'a CombineResolverMapping,
    probe_record: &'a Record,
    build_record: Option<&'a Record>,
}

impl<'a> CombineResolver<'a> {
    /// Construct a resolver for one probe iteration.
    pub(crate) fn new(
        mapping: &'a CombineResolverMapping,
        probe_record: &'a Record,
        build_record: Option<&'a Record>,
    ) -> Self {
        Self {
            mapping,
            probe_record,
            build_record,
        }
    }

    /// Fetch a bare field from the record on `side`. For the build
    /// side, `None` on the record itself means `on_miss: null_fields`
    /// is in force — the field is present in the merged row but has
    /// no underlying value, so we surface `&clinker_record::NULL`
    /// (the shared sentinel defined in `clinker_record::value`).
    fn fetch(&self, side: JoinSide, bare_name: &str) -> Option<&Value> {
        match side {
            JoinSide::Probe => self.probe_record.get(bare_name),
            JoinSide::Build => match self.build_record {
                Some(rec) => rec.get(bare_name),
                None => Some(&clinker_record::NULL),
            },
        }
    }
}

impl FieldResolver for CombineResolver<'_> {
    fn resolve(&self, name: &str) -> Option<&Value> {
        // $meta.* routes to the probe record's metadata; combine bodies
        // operate "in the frame of" the driving record, so meta reads
        // follow the driver.
        if name.starts_with("$meta.") {
            return self.probe_record.resolve(name);
        }
        let (side, bare) = self.mapping.bare_to_side.get(name)?;
        self.fetch(*side, bare)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<&Value> {
        let qf = QualifiedField::qualified(source, field);
        let (side, bare) = self.mapping.qualified.get(&qf)?;
        self.fetch(*side, bare)
    }

    fn available_fields(&self) -> Vec<&str> {
        self.mapping.available.iter().map(String::as_str).collect()
    }

    fn iter_fields(&self) -> Vec<(&str, &Value)> {
        // `iter_fields` returns borrowed `&str` names. The mapping's
        // `available` vec holds rendered `qualifier.name` strings with
        // stable addresses (populated at mapping construction), so we
        // borrow from it to tie the returned `&str` to `&self` without
        // allocating a fresh `String` per call.
        let mut out = Vec::with_capacity(self.mapping.available.len());
        for qualified_name in &self.mapping.available {
            let (q_part, n_part) = qualified_name
                .split_once('.')
                .expect("CombineResolverMapping renders every entry as `qualifier.name`");
            let qf = QualifiedField::qualified(q_part, n_part);
            if let Some((side, bare)) = self.mapping.qualified.get(&qf)
                && let Some(val) = self.fetch(*side, bare)
            {
                out.push((qualified_name.as_str(), val));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, Value};
    use cxl::lexer::Span as CxlSpan;
    use cxl::typecheck::Type;
    use indexmap::IndexMap;

    fn make_schema(fields: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|f| (*f).to_string().into_boxed_str())
                .collect(),
        ))
    }

    fn make_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(schema.clone(), values)
    }

    /// Build a CombineInput whose row declares the given bare fields.
    /// Types are all `Type::String` — unit under test is field routing,
    /// not type propagation.
    fn input_with_fields(name: &str, fields: &[&str]) -> CombineInput {
        let mut declared: IndexMap<QualifiedField, Type> = IndexMap::new();
        for f in fields {
            declared.insert(QualifiedField::bare(*f), Type::String);
        }
        CombineInput {
            upstream_name: Arc::from(name),
            row: crate::plan::row_type::Row::closed(declared, CxlSpan::new(0, 0)),
            estimated_cardinality: None,
        }
    }

    fn two_input_orders_products() -> IndexMap<String, CombineInput> {
        let mut m: IndexMap<String, CombineInput> = IndexMap::new();
        m.insert(
            "orders".into(),
            input_with_fields("orders_src", &["order_id", "product_id", "amount"]),
        );
        m.insert(
            "products".into(),
            input_with_fields("products_src", &["product_id", "name", "price"]),
        );
        m
    }

    #[test]
    fn test_combine_resolver_qualified_lookup_both_sides() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![
                Value::String("P42".into()),
                Value::String("Widget".into()),
                Value::Integer(9),
            ],
        );

        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));

        assert_eq!(
            resolver.resolve_qualified("orders", "order_id"),
            Some(&Value::String("O1".into()))
        );
        assert_eq!(
            resolver.resolve_qualified("orders", "amount"),
            Some(&Value::Integer(100))
        );
        assert_eq!(
            resolver.resolve_qualified("products", "name"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(
            resolver.resolve_qualified("products", "price"),
            Some(&Value::Integer(9))
        );
    }

    #[test]
    fn test_combine_resolver_bare_lookup_unambiguous() {
        // `amount` is only on orders; `name` is only on products.
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![
                Value::String("P42".into()),
                Value::String("Widget".into()),
                Value::Integer(9),
            ],
        );

        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));

        // Probe-side only
        assert_eq!(resolver.resolve("amount"), Some(&Value::Integer(100)));
        assert_eq!(
            resolver.resolve("order_id"),
            Some(&Value::String("O1".into()))
        );
        // Build-side only
        assert_eq!(
            resolver.resolve("name"),
            Some(&Value::String("Widget".into()))
        );
        assert_eq!(resolver.resolve("price"), Some(&Value::Integer(9)));
    }

    /// Ambiguous bare name: the typechecker rejects `product_id` as
    /// ambiguous at compile time. Option α — runtime resolve returns
    /// `None`, never silently picks a side.
    #[test]
    fn test_combine_resolver_bare_lookup_ambiguous_returns_none() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![
                Value::String("P42".into()),
                Value::String("Widget".into()),
                Value::Integer(9),
            ],
        );

        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));
        assert_eq!(
            resolver.resolve("product_id"),
            None,
            "ambiguous bare name must not silently pick a side"
        );
    }

    /// With `build_record: None` (on_miss: null_fields pre-emission),
    /// build-side qualified lookups return `Value::Null` — the field
    /// exists in the merged row but has no underlying value. Matches
    /// `LookupResolver::no_match` for the scalar-lookup case.
    #[test]
    fn test_combine_resolver_build_side_none_returns_null_for_build_fields() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );

        let resolver = CombineResolver::new(&mapping, &orders_rec, None);

        // Build-side qualified → Null
        assert_eq!(
            resolver.resolve_qualified("products", "name"),
            Some(&Value::Null)
        );
        assert_eq!(
            resolver.resolve_qualified("products", "price"),
            Some(&Value::Null)
        );
        // Probe-side qualified → actual value
        assert_eq!(
            resolver.resolve_qualified("orders", "amount"),
            Some(&Value::Integer(100))
        );
        // Bare build-side-only name → Null (unambiguous; builds routes to None record)
        assert_eq!(resolver.resolve("name"), Some(&Value::Null));
    }

    #[test]
    fn test_combine_resolver_unknown_field() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![
                Value::String("P42".into()),
                Value::String("Widget".into()),
                Value::Integer(9),
            ],
        );
        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));

        // Unknown qualifier
        assert_eq!(resolver.resolve_qualified("ghosts", "spooky"), None);
        // Known qualifier + unknown field
        assert_eq!(resolver.resolve_qualified("orders", "ghost_field"), None);
        // Unknown bare
        assert_eq!(resolver.resolve("ghost_field"), None);
    }

    #[test]
    fn test_combine_resolver_available_fields_enumerates_both_sides() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(&orders_schema, vec![Value::Null, Value::Null, Value::Null]);
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![Value::Null, Value::Null, Value::Null],
        );
        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));

        let available = resolver.available_fields();
        // 3 + 3 fields, each rendered `qualifier.name`.
        assert_eq!(available.len(), 6);
        assert!(available.contains(&"orders.order_id"));
        assert!(available.contains(&"orders.product_id"));
        assert!(available.contains(&"orders.amount"));
        assert!(available.contains(&"products.product_id"));
        assert!(available.contains(&"products.name"));
        assert!(available.contains(&"products.price"));
    }

    /// Mapping construction walks every input's row exactly once. After
    /// construction the mapping is a plain lookup table — repeated
    /// resolver instantiations over the same mapping do not re-walk
    /// any schema.
    #[test]
    fn test_combine_resolver_mapping_precomputed_once() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let products_schema = make_schema(&["product_id", "name", "price"]);
        // 3 orders fields + 3 products fields all surface through the
        // resolver's `available_fields`, verifying that mapping
        // construction saw every input's declared row.
        let empty_orders = make_record(&orders_schema, vec![Value::Null, Value::Null, Value::Null]);
        let empty_products = make_record(
            &products_schema,
            vec![Value::Null, Value::Null, Value::Null],
        );
        let sanity_resolver = CombineResolver::new(&mapping, &empty_orders, Some(&empty_products));
        assert_eq!(
            sanity_resolver.available_fields().len(),
            6,
            "3 orders fields + 3 products fields"
        );

        // Drive multiple synthetic probe pairs through the same mapping
        // and assert each resolves identically — the mapping is reused,
        // not rebuilt.
        for i in 0..4 {
            let orders_rec = make_record(
                &orders_schema,
                vec![
                    Value::String(format!("O{i}").into()),
                    Value::String("P42".into()),
                    Value::Integer(i as i64 * 10),
                ],
            );
            let products_rec = make_record(
                &products_schema,
                vec![
                    Value::String("P42".into()),
                    Value::String("Widget".into()),
                    Value::Integer(9),
                ],
            );
            let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));
            assert_eq!(
                resolver.resolve_qualified("orders", "order_id"),
                Some(&Value::String(format!("O{i}").into()))
            );
            assert_eq!(
                resolver.resolve_qualified("products", "name"),
                Some(&Value::String("Widget".into()))
            );
        }
    }

    /// `iter_fields` emits one entry per qualified field, rendering
    /// `qualifier.name` as the key. Values come from the backing
    /// records via the same `fetch` path as the targeted resolves.
    #[test]
    fn test_combine_resolver_iter_fields() {
        let inputs = two_input_orders_products();
        let mapping = CombineResolverMapping::new(&inputs, "orders");

        let orders_schema = make_schema(&["order_id", "product_id", "amount"]);
        let orders_rec = make_record(
            &orders_schema,
            vec![
                Value::String("O1".into()),
                Value::String("P42".into()),
                Value::Integer(100),
            ],
        );
        let products_schema = make_schema(&["product_id", "name", "price"]);
        let products_rec = make_record(
            &products_schema,
            vec![
                Value::String("P42".into()),
                Value::String("Widget".into()),
                Value::Integer(9),
            ],
        );
        let resolver = CombineResolver::new(&mapping, &orders_rec, Some(&products_rec));

        let fields: HashMap<String, Value> = resolver
            .iter_fields()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        assert_eq!(fields.len(), 6);
        assert_eq!(
            fields.get("orders.order_id"),
            Some(&Value::String("O1".into()))
        );
        assert_eq!(
            fields.get("products.name"),
            Some(&Value::String("Widget".into()))
        );
    }
}
