//! Per-record input-schema check for executor arms.
//!
//! Every operator enters the executor arm with an `expected: Arc<Schema>`
//! stamped at plan-compile time on the `PlanNode`. Records arriving from
//! upstream carry their own `Arc<Schema>` — in the steady state (same
//! upstream handing identical Arcs) the fast path is an `Arc::ptr_eq`
//! pointer compare (sub-nanosecond under branch prediction). On
//! ptr-mismatch we fall back to structural equality over the column
//! lists; any divergence surfaces as `E314` (`PipelineError::SchemaMismatch`).
//!
//! Runs per record, not amortized on an operator-state flag: streaming
//! drift surfaces (IPC boundaries, spill rehydrate, streaming restart)
//! can smuggle an arity shift after the first record, and the branch
//! predictor collapses the happy path cost to effectively zero.

use std::sync::Arc;

use clinker_record::Schema;

use crate::error::PipelineError;

/// Validate that `actual` matches `expected`. Fast-path is `Arc::ptr_eq`;
/// structural fallback compares the column-name lists. Any divergence
/// raises `E314`.
pub fn check_input_schema(
    expected: &Arc<Schema>,
    actual: &Arc<Schema>,
    operator_name: &str,
    operator_kind: &'static str,
    upstream_name: &str,
) -> Result<(), PipelineError> {
    if Arc::ptr_eq(expected, actual) {
        return Ok(());
    }
    if expected.columns() == actual.columns() {
        return Ok(());
    }
    Err(PipelineError::SchemaMismatch {
        expected: Arc::clone(expected),
        actual: Arc::clone(actual),
        operator_name: operator_name.to_string(),
        operator_kind,
        upstream_name: upstream_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Schema;

    fn s(cols: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()))
    }

    #[test]
    fn ptr_eq_fast_path_passes() {
        let schema = s(&["a", "b"]);
        assert!(check_input_schema(&schema, &schema, "op", "transform", "src").is_ok());
    }

    #[test]
    fn structural_eq_fallback_passes() {
        let a = s(&["a", "b"]);
        let b = s(&["a", "b"]);
        assert!(!Arc::ptr_eq(&a, &b));
        assert!(check_input_schema(&a, &b, "op", "transform", "src").is_ok());
    }

    #[test]
    fn diverging_columns_raise_e314() {
        let expected = s(&["a", "b"]);
        let actual = s(&["a", "c"]);
        let err =
            check_input_schema(&expected, &actual, "my_op", "transform", "upstream").unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.starts_with("E314"));
        assert!(rendered.contains("my_op"));
        assert!(rendered.contains("transform"));
        assert!(rendered.contains("upstream"));
        assert!(rendered.contains("a@0"));
        assert!(rendered.contains("b@1"));
        assert!(rendered.contains("c@1"));
    }

    #[test]
    fn diverging_arity_raises_e314() {
        let expected = s(&["a", "b", "c"]);
        let actual = s(&["a", "b"]);
        let err = check_input_schema(&expected, &actual, "op", "merge", "up").unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("E314"));
        assert!(rendered.contains("expected 3 columns"));
        assert!(rendered.contains("record has 2 columns"));
    }
}
