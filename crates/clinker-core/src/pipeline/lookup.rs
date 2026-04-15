//! Reference table lookup enrichment.
//!
//! Loads a secondary source into memory as a `LookupTable`, then for each
//! input record evaluates a CXL predicate against candidate rows to find
//! matches. Matched row fields are exposed under `source_name.field` via
//! a composite `LookupResolver`.

use std::sync::Arc;

use clinker_format::traits::FormatReader;
use clinker_record::{FieldResolver, Record, Schema, Value};

use crate::config::pipeline_node::MatchMode;

/// In-memory reference table for lookup enrichment.
///
/// Loaded once during Phase 1 from a source reader. Immutable after
/// construction. `Send + Sync` for sharing across rayon workers.
#[derive(Debug)]
pub struct LookupTable {
    /// Name of the source node (used as the field qualifier in CXL).
    source_name: String,
    /// All records from the reference source.
    records: Vec<Record>,
    /// Schema of the reference source.
    schema: Arc<Schema>,
}

impl LookupTable {
    /// Build a lookup table by reading all records from a source.
    pub fn build(
        source_name: String,
        reader: &mut dyn FormatReader,
        memory_limit: usize,
    ) -> Result<Self, LookupError> {
        let schema = reader.schema()?;
        let mut records = Vec::new();
        let mut bytes_used: usize = 0;

        while let Some(record) = reader.next_record()? {
            bytes_used += record.estimated_heap_size() + std::mem::size_of::<Record>();
            if bytes_used > memory_limit {
                return Err(LookupError::MemoryBudgetExceeded {
                    source: source_name,
                    used: bytes_used,
                    limit: memory_limit,
                });
            }
            records.push(record);
        }

        Ok(LookupTable {
            source_name,
            records,
            schema,
        })
    }

    /// The source name used as field qualifier.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Schema of the reference source.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Number of records in the lookup table.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the lookup table is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Iterate all records (for predicate evaluation).
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}

// Safety: Records are immutable after construction.
unsafe impl Send for LookupTable {}
unsafe impl Sync for LookupTable {}

/// Composite field resolver that layers a matched lookup row on top of
/// the primary input record. Bare field names resolve from the input;
/// `source_name.field` resolves from the lookup row.
pub struct LookupResolver<'a> {
    /// Primary input record resolver.
    input: &'a dyn FieldResolver,
    /// Source name for qualified field access.
    source_name: &'a str,
    /// Matched lookup row (None if on_miss = null_fields with no match).
    matched: Option<&'a Record>,
}

impl<'a> LookupResolver<'a> {
    /// Create a resolver with a matched lookup row.
    pub fn matched(
        input: &'a dyn FieldResolver,
        source_name: &'a str,
        matched: &'a Record,
    ) -> Self {
        LookupResolver {
            input,
            source_name,
            matched: Some(matched),
        }
    }

    /// Create a resolver with no match (all lookup fields resolve to Null).
    pub fn no_match(input: &'a dyn FieldResolver, source_name: &'a str) -> Self {
        LookupResolver {
            input,
            source_name,
            matched: None,
        }
    }
}

impl FieldResolver for LookupResolver<'_> {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.input.resolve(name)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        if source == self.source_name {
            match &self.matched {
                Some(record) => record.get(field).cloned(),
                None => Some(Value::Null),
            }
        } else {
            self.input.resolve_qualified(source, field)
        }
    }

    fn available_fields(&self) -> Vec<&str> {
        self.input.available_fields()
    }

    fn iter_fields(&self) -> Vec<(String, Value)> {
        self.input.iter_fields()
    }
}

/// Find matching rows in a lookup table for a given input record.
///
/// Evaluates the `where_expr` predicate against each lookup row using
/// a composite resolver that has both input fields (bare) and candidate
/// lookup fields (qualified). Returns indices of matching rows.
pub fn find_matches(
    table: &LookupTable,
    input: &dyn FieldResolver,
    where_evaluator: &mut cxl::eval::ProgramEvaluator,
    ctx: &cxl::eval::EvalContext<'_>,
    match_mode: MatchMode,
    equality_index: Option<&EqualityIndex>,
) -> Result<Vec<usize>, LookupError> {
    // Determine candidate row indices (full scan or index probe)
    let candidates: Vec<usize> = if let Some(idx) = equality_index {
        probe_equality_index(idx, input, table.len())
    } else {
        (0..table.len()).collect()
    };

    // The where predicate has a zero-column standalone OutputLayout (it's
    // a filter with no emits), so the positional input_record is unused
    // for passthrough; we pass the matched candidate as the positional
    // source for consistency with Option W's per-record shape.
    let mut matches = Vec::new();
    for i in candidates {
        let candidate = &table.records()[i];
        let resolver = LookupResolver::matched(input, table.source_name(), candidate);
        let result = where_evaluator
            .eval_record::<NullStorage>(ctx, candidate, &resolver, None)
            .map_err(|e| LookupError::PredicateError {
                source: table.source_name().to_string(),
                row: i,
                message: e.to_string(),
            })?;

        match result {
            cxl::eval::EvalResult::Emit { .. } => {
                matches.push(i);
                if match_mode == MatchMode::First {
                    break;
                }
            }
            cxl::eval::EvalResult::Skip(_) => {}
        }
    }

    Ok(matches)
}

/// Probe the equality index with input record's key values.
/// Returns candidate row indices (partition + null_rows), or full scan
/// range if the input has null key values.
fn probe_equality_index(
    idx: &EqualityIndex,
    input: &dyn FieldResolver,
    table_len: usize,
) -> Vec<usize> {
    let mut key_values = Vec::with_capacity(idx.keys.len());
    for eq_key in &idx.keys {
        match input.resolve(&eq_key.input_field) {
            Some(Value::Null) | None => {
                // Null input key → can't use index, fall back to full scan
                return (0..table_len).collect();
            }
            Some(val) => {
                match clinker_record::value_to_group_key(&val, &eq_key.input_field, None, 0) {
                    Ok(Some(gk)) => key_values.push(gk),
                    _ => return (0..table_len).collect(),
                }
            }
        }
    }

    let mut candidates: Vec<usize> = idx.partitions.get(&key_values).cloned().unwrap_or_default();

    // Always include null_rows (they might match via the full predicate)
    candidates.extend_from_slice(&idx.null_rows);
    candidates.sort_unstable();
    candidates
}

/// Null storage for lookup predicate evaluation (no window context needed).
struct NullStorage;

impl clinker_record::RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
        0
    }
}

/// Errors from lookup operations.
#[derive(Debug)]
pub enum LookupError {
    MemoryBudgetExceeded {
        source: String,
        used: usize,
        limit: usize,
    },
    PredicateError {
        source: String,
        row: usize,
        message: String,
    },
    ReadError(clinker_format::error::FormatError),
    CompileError {
        source: String,
        message: String,
    },
    SourceNotFound {
        source: String,
    },
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupError::MemoryBudgetExceeded {
                source,
                used,
                limit,
            } => {
                write!(
                    f,
                    "lookup source '{}' requires ~{}MB, exceeds memory_limit of {}MB",
                    source,
                    used / (1024 * 1024),
                    limit / (1024 * 1024),
                )
            }
            LookupError::PredicateError {
                source,
                row,
                message,
            } => {
                write!(
                    f,
                    "lookup predicate error on source '{}' row {}: {}",
                    source, row, message,
                )
            }
            LookupError::ReadError(e) => write!(f, "lookup read error: {}", e),
            LookupError::CompileError { source, message } => {
                write!(
                    f,
                    "lookup where predicate compile error for source '{}': {}",
                    source, message,
                )
            }
            LookupError::SourceNotFound { source } => {
                write!(f, "lookup source '{}' not found in pipeline", source)
            }
        }
    }
}

impl std::error::Error for LookupError {}

impl From<clinker_format::error::FormatError> for LookupError {
    fn from(e: clinker_format::error::FormatError) -> Self {
        LookupError::ReadError(e)
    }
}

// ── Equality index for hash-partition optimization ──

/// One equality condition extracted from a where predicate AST.
#[derive(Debug, Clone)]
pub struct EqualityKey {
    /// Bare field name from the input record (e.g., "product_id").
    pub input_field: String,
    /// Field name in the lookup table (e.g., "product_id" from "products.product_id").
    pub lookup_field: String,
}

/// Hash-partition index over a lookup table's rows.
///
/// Built at compile time from equality conditions in the where predicate.
/// At runtime, input records probe the index to narrow the candidate set
/// before evaluating the full predicate.
#[derive(Debug)]
pub struct EqualityIndex {
    /// The equality keys (input_field, lookup_field) pairs.
    pub keys: Vec<EqualityKey>,
    /// Maps key tuples to row indices in the lookup table.
    pub partitions: std::collections::HashMap<Vec<clinker_record::GroupByKey>, Vec<usize>>,
    /// Rows where at least one key field is null — always included in scans.
    pub null_rows: Vec<usize>,
}

/// Extract equality conditions from a where predicate AST.
///
/// Only extracts from top-level `And` chains. Returns empty if an `Or`
/// appears at the top level or if no `field == source.field` patterns
/// are found.
pub fn extract_equality_keys(expr: &cxl::ast::Expr, source_name: &str) -> Vec<EqualityKey> {
    use cxl::ast::{BinOp, Expr};
    match expr {
        Expr::Binary {
            op: BinOp::And,
            lhs,
            rhs,
            ..
        } => {
            let mut keys = extract_equality_keys(lhs, source_name);
            keys.extend(extract_equality_keys(rhs, source_name));
            keys
        }
        Expr::Binary {
            op: BinOp::Eq,
            lhs,
            rhs,
            ..
        } => {
            // Try both orderings: field == source.field, source.field == field
            if let Some(key) = try_extract_eq_pair(lhs, rhs, source_name) {
                vec![key]
            } else if let Some(key) = try_extract_eq_pair(rhs, lhs, source_name) {
                vec![key]
            } else {
                vec![]
            }
        }
        // Or, or any other expression form: no extractable keys
        _ => vec![],
    }
}

/// Check if (lhs, rhs) form a (bare_field, qualified_source_field) pair.
fn try_extract_eq_pair(
    input_side: &cxl::ast::Expr,
    lookup_side: &cxl::ast::Expr,
    source_name: &str,
) -> Option<EqualityKey> {
    use cxl::ast::Expr;
    let input_field = match input_side {
        Expr::FieldRef { name, .. } => name.to_string(),
        _ => return None,
    };
    match lookup_side {
        Expr::QualifiedFieldRef { parts, .. } if parts.len() == 2 && &*parts[0] == source_name => {
            Some(EqualityKey {
                input_field,
                lookup_field: parts[1].to_string(),
            })
        }
        _ => None,
    }
}

/// Build an equality index from a compiled where predicate and lookup table.
///
/// Returns `None` if no equality conditions can be extracted from the predicate.
pub fn build_equality_index(
    typed: &cxl::typecheck::TypedProgram,
    source_name: &str,
    table: &LookupTable,
) -> Option<EqualityIndex> {
    // Find the Filter statement's predicate
    let predicate = typed.program.statements.iter().find_map(|s| {
        if let cxl::ast::Statement::Filter { predicate, .. } = s {
            Some(predicate)
        } else {
            None
        }
    })?;

    let keys = extract_equality_keys(predicate, source_name);
    if keys.is_empty() {
        return None;
    }

    let mut partitions: std::collections::HashMap<Vec<clinker_record::GroupByKey>, Vec<usize>> =
        std::collections::HashMap::new();
    let mut null_rows: Vec<usize> = Vec::new();

    for (i, record) in table.records().iter().enumerate() {
        let mut key_values = Vec::with_capacity(keys.len());
        let mut has_null = false;

        for eq_key in &keys {
            match record.get(&eq_key.lookup_field) {
                Some(Value::Null) | None => {
                    has_null = true;
                    break;
                }
                Some(val) => {
                    match clinker_record::value_to_group_key(
                        val,
                        &eq_key.lookup_field,
                        None,
                        i as u32,
                    ) {
                        Ok(Some(gk)) => key_values.push(gk),
                        // Null or error (NaN) → can't index this row
                        _ => {
                            has_null = true;
                            break;
                        }
                    }
                }
            }
        }

        if has_null {
            null_rows.push(i);
        } else {
            partitions.entry(key_values).or_default().push(i);
        }
    }

    Some(EqualityIndex {
        keys,
        partitions,
        null_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, Value};
    use std::sync::Arc;

    fn make_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(schema.clone(), values)
    }

    fn make_schema(fields: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|f| (*f).to_string().into_boxed_str())
                .collect(),
        ))
    }

    #[test]
    fn test_lookup_resolver_qualified_access() {
        let input_schema = make_schema(&["employee_id", "pay", "ee_group"]);
        let input_record = make_record(
            &input_schema,
            vec![
                Value::String("E001".into()),
                Value::Integer(75000),
                Value::String("exempt".into()),
            ],
        );

        let lookup_schema = make_schema(&["ee_group", "min_pay", "max_pay", "rate_class"]);
        let lookup_record = make_record(
            &lookup_schema,
            vec![
                Value::String("exempt".into()),
                Value::Integer(50000),
                Value::Integer(80000),
                Value::String("tier_1".into()),
            ],
        );

        let resolver = LookupResolver::matched(&input_record, "rate_bands", &lookup_record);

        // Bare fields come from input
        assert_eq!(resolver.resolve("pay"), Some(Value::Integer(75000)));
        assert_eq!(
            resolver.resolve("employee_id"),
            Some(Value::String("E001".into()))
        );

        // Qualified fields come from lookup
        assert_eq!(
            resolver.resolve_qualified("rate_bands", "rate_class"),
            Some(Value::String("tier_1".into()))
        );
        assert_eq!(
            resolver.resolve_qualified("rate_bands", "min_pay"),
            Some(Value::Integer(50000))
        );

        // Unknown source delegates to input
        assert_eq!(resolver.resolve_qualified("other", "field"), None);
    }

    #[test]
    fn test_lookup_resolver_no_match_returns_null() {
        let input_schema = make_schema(&["employee_id"]);
        let input_record = make_record(&input_schema, vec![Value::String("E001".into())]);

        let resolver = LookupResolver::no_match(&input_record, "rate_bands");

        // Bare fields still resolve from input
        assert_eq!(
            resolver.resolve("employee_id"),
            Some(Value::String("E001".into()))
        );

        // Lookup fields return Null (not None)
        assert_eq!(
            resolver.resolve_qualified("rate_bands", "rate_class"),
            Some(Value::Null)
        );
    }

    // ── Equality index extraction tests ──

    use cxl::ast::{BinOp, Expr, NodeId};
    use cxl::lexer::Span;

    fn nid() -> NodeId {
        NodeId(0)
    }
    fn sp() -> Span {
        Span::new(0, 0)
    }
    fn field_ref(name: &str) -> Box<Expr> {
        Box::new(Expr::FieldRef {
            node_id: nid(),
            name: name.into(),
            span: sp(),
        })
    }
    fn qualified_ref(source: &str, field: &str) -> Box<Expr> {
        Box::new(Expr::QualifiedFieldRef {
            node_id: nid(),
            parts: vec![source.into(), field.into()].into_boxed_slice(),
            span: sp(),
        })
    }
    fn binary(op: BinOp, lhs: Box<Expr>, rhs: Box<Expr>) -> Expr {
        Expr::Binary {
            node_id: nid(),
            op,
            lhs,
            rhs,
            span: sp(),
        }
    }

    #[test]
    fn test_extract_single_equality() {
        // product_id == products.product_id
        let expr = binary(
            BinOp::Eq,
            field_ref("product_id"),
            qualified_ref("products", "product_id"),
        );
        let keys = extract_equality_keys(&expr, "products");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].input_field, "product_id");
        assert_eq!(keys[0].lookup_field, "product_id");
    }

    #[test]
    fn test_extract_reversed_operands() {
        // products.product_id == product_id
        let expr = binary(
            BinOp::Eq,
            qualified_ref("products", "product_id"),
            field_ref("product_id"),
        );
        let keys = extract_equality_keys(&expr, "products");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].input_field, "product_id");
    }

    #[test]
    fn test_extract_multiple_equality_with_and() {
        // a == src.a and b == src.b
        let lhs = binary(BinOp::Eq, field_ref("a"), qualified_ref("src", "a"));
        let rhs = binary(BinOp::Eq, field_ref("b"), qualified_ref("src", "b"));
        let expr = binary(BinOp::And, Box::new(lhs), Box::new(rhs));
        let keys = extract_equality_keys(&expr, "src");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_extract_mixed_equality_and_range() {
        // a == src.a and x >= src.y
        let eq = binary(BinOp::Eq, field_ref("a"), qualified_ref("src", "a"));
        let range = binary(BinOp::Gte, field_ref("x"), qualified_ref("src", "y"));
        let expr = binary(BinOp::And, Box::new(eq), Box::new(range));
        let keys = extract_equality_keys(&expr, "src");
        assert_eq!(keys.len(), 1, "only the equality condition is extracted");
        assert_eq!(keys[0].input_field, "a");
    }

    #[test]
    fn test_extract_or_returns_empty() {
        // a == src.a or b == src.b → no keys (conservative)
        let lhs = binary(BinOp::Eq, field_ref("a"), qualified_ref("src", "a"));
        let rhs = binary(BinOp::Eq, field_ref("b"), qualified_ref("src", "b"));
        let expr = binary(BinOp::Or, Box::new(lhs), Box::new(rhs));
        let keys = extract_equality_keys(&expr, "src");
        assert!(keys.is_empty(), "OR prevents equality extraction");
    }

    #[test]
    fn test_extract_wrong_source_returns_empty() {
        // a == other.a (wrong source name)
        let expr = binary(BinOp::Eq, field_ref("a"), qualified_ref("other", "a"));
        let keys = extract_equality_keys(&expr, "src");
        assert!(keys.is_empty());
    }

    #[test]
    fn test_probe_equality_index_hit() {
        let schema = make_schema(&["key", "val"]);
        let _r1 = make_record(&schema, vec![Value::String("A".into()), Value::Integer(1)]);
        let _r2 = make_record(&schema, vec![Value::String("B".into()), Value::Integer(2)]);
        let _r3 = make_record(&schema, vec![Value::String("A".into()), Value::Integer(3)]);

        let mut partitions = std::collections::HashMap::new();
        partitions.insert(
            vec![clinker_record::GroupByKey::Str("A".into())],
            vec![0, 2],
        );
        partitions.insert(vec![clinker_record::GroupByKey::Str("B".into())], vec![1]);
        let idx = EqualityIndex {
            keys: vec![EqualityKey {
                input_field: "key".to_string(),
                lookup_field: "key".to_string(),
            }],
            partitions,
            null_rows: vec![],
        };

        // Input with key="A" should return indices [0, 2]
        let input_schema = make_schema(&["key"]);
        let input = make_record(&input_schema, vec![Value::String("A".into())]);
        let candidates = probe_equality_index(&idx, &input, 3);
        assert_eq!(candidates, vec![0, 2]);

        // Input with key="B" should return index [1]
        let input_b = make_record(&input_schema, vec![Value::String("B".into())]);
        let candidates_b = probe_equality_index(&idx, &input_b, 3);
        assert_eq!(candidates_b, vec![1]);

        // Input with key="C" (no match) should return empty
        let input_c = make_record(&input_schema, vec![Value::String("C".into())]);
        let candidates_c = probe_equality_index(&idx, &input_c, 3);
        assert!(candidates_c.is_empty());
    }

    #[test]
    fn test_probe_equality_index_null_input_falls_back() {
        let idx = EqualityIndex {
            keys: vec![EqualityKey {
                input_field: "key".to_string(),
                lookup_field: "key".to_string(),
            }],
            partitions: std::collections::HashMap::new(),
            null_rows: vec![],
        };

        // Null input key → full scan fallback
        let input_schema = make_schema(&["key"]);
        let input = make_record(&input_schema, vec![Value::Null]);
        let candidates = probe_equality_index(&idx, &input, 5);
        assert_eq!(
            candidates,
            vec![0, 1, 2, 3, 4],
            "null key falls back to full scan"
        );
    }

    #[test]
    fn test_probe_equality_index_includes_null_rows() {
        let mut partitions = std::collections::HashMap::new();
        partitions.insert(vec![clinker_record::GroupByKey::Str("A".into())], vec![0]);
        let idx = EqualityIndex {
            keys: vec![EqualityKey {
                input_field: "key".to_string(),
                lookup_field: "key".to_string(),
            }],
            partitions,
            null_rows: vec![2], // row 2 had null key in lookup table
        };

        let input_schema = make_schema(&["key"]);
        let input = make_record(&input_schema, vec![Value::String("A".into())]);
        let candidates = probe_equality_index(&idx, &input, 3);
        assert_eq!(candidates, vec![0, 2], "partition [0] + null_rows [2]");
    }
}
