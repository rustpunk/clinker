use clinker_record::Value;
use indexmap::IndexMap;

use crate::config::{ValidationEntry, ValidationSeverity};

/// Result of running validations on a single record.
pub enum ValidationResult {
    /// All validations passed.
    Passed,
    /// An error-severity validation failed. Record should go to DLQ.
    Failed {
        validation_name: String,
        message: String,
    },
    /// One or more warn-severity validations failed but no errors.
    /// Record continues processing; warnings were logged.
    Warnings(Vec<String>),
}

/// Run all validations for a record. Returns on first error-severity failure (short-circuit).
/// Warn-severity failures are collected and returned if no error occurs.
///
/// `eval_check` is a closure that evaluates a CXL check expression and returns a bool.
/// The caller is responsible for resolving module function references vs inline CXL.
pub fn run_validations(
    validations: &[ValidationEntry],
    record_fields: &IndexMap<String, Value>,
    eval_check: &dyn Fn(&str, Option<&str>, &IndexMap<String, Value>) -> Result<bool, String>,
) -> ValidationResult {
    let mut warnings = Vec::new();

    for v in validations {
        let check_result = eval_check(&v.check, v.field.as_deref(), record_fields);

        match check_result {
            Ok(true) => {
                // Validation passed
            }
            Ok(false) => {
                let name = v.resolved_name();
                let msg = v.message.clone().unwrap_or_else(|| {
                    format!("validation '{}' failed", name)
                });

                match v.severity {
                    ValidationSeverity::Error => {
                        return ValidationResult::Failed {
                            validation_name: name,
                            message: msg,
                        };
                    }
                    ValidationSeverity::Warn => {
                        tracing::warn!(
                            validation = %name,
                            "{}",
                            msg
                        );
                        warnings.push(name);
                    }
                }
            }
            Err(e) => {
                // Evaluation error treated as error-severity failure
                let name = v.resolved_name();
                return ValidationResult::Failed {
                    validation_name: name,
                    message: format!("validation check error: {}", e),
                };
            }
        }
    }

    if warnings.is_empty() {
        ValidationResult::Passed
    } else {
        ValidationResult::Warnings(warnings)
    }
}

/// Check a field value against allowed_values (enum enforcement).
/// Returns Ok(()) if valid, Err(message) if invalid.
pub fn enforce_enum(
    field_name: &str,
    value: &Value,
    allowed_values: &[String],
    required: bool,
) -> Result<(), String> {
    // Null passes if field is not required
    if matches!(value, Value::Null) {
        if required {
            return Err(format!(
                "field '{}' is required but got null",
                field_name
            ));
        }
        return Ok(());
    }

    // Convert value to string for comparison
    let val_str = match value {
        Value::String(s) => s.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("{:?}", other),
    };

    if allowed_values.contains(&val_str) {
        Ok(())
    } else {
        Err(format!(
            "field '{}': value '{}' is not in allowed values {:?}",
            field_name, val_str, allowed_values
        ))
    }
}

/// Validate structure ordering of multi-record types.
/// `record_types` is the sequence of record type names encountered.
/// `constraints` defines the expected order.
/// Returns a list of warnings for out-of-order records.
pub fn validate_structure_ordering(
    record_types: &[String],
    constraints: &[(String, String)],
) -> Vec<String> {
    if constraints.is_empty() || record_types.is_empty() {
        return vec![];
    }

    let mut warnings = Vec::new();
    let mut last_constraint_idx = 0;

    for (row, record_type) in record_types.iter().enumerate() {
        // Find the constraint index for this record type
        if let Some(idx) = constraints.iter().position(|(name, _)| name == record_type) {
            if idx < last_constraint_idx {
                warnings.push(format!(
                    "row {}: record type '{}' appears after '{}' — expected order per structure constraint",
                    row + 1,
                    record_type,
                    constraints[last_constraint_idx].0,
                ));
            }
            last_constraint_idx = idx;
        }
    }

    for warning in &warnings {
        tracing::warn!("{}", warning);
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fields(pairs: &[(&str, Value)]) -> IndexMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn make_validation(check: &str, severity: ValidationSeverity) -> ValidationEntry {
        ValidationEntry {
            name: None,
            field: None,
            check: check.to_string(),
            args: None,
            severity,
            message: None,
        }
    }

    // Always-true evaluator
    fn eval_true(_check: &str, _field: Option<&str>, _record: &IndexMap<String, Value>) -> Result<bool, String> {
        Ok(true)
    }

    // Evaluator that checks if a named field > 0
    fn eval_positive(_check: &str, field: Option<&str>, record: &IndexMap<String, Value>) -> Result<bool, String> {
        if let Some(f) = field {
            match record.get(f) {
                Some(Value::Integer(i)) => Ok(*i > 0),
                Some(Value::Float(f)) => Ok(*f > 0.0),
                _ => Ok(false),
            }
        } else {
            // Cross-field: check if "Amount" > 0
            match record.get("Amount") {
                Some(Value::Integer(i)) => Ok(*i > 0),
                _ => Ok(false),
            }
        }
    }

    #[test]
    fn test_validation_inline_cxl_pass() {
        let validations = vec![make_validation("Amount > 0", ValidationSeverity::Error)];
        let fields = make_fields(&[("Amount", Value::Integer(100))]);
        let result = run_validations(&validations, &fields, &eval_true);
        assert!(matches!(result, ValidationResult::Passed));
    }

    #[test]
    fn test_validation_inline_cxl_fail_error() {
        let mut v = make_validation("Amount > 0", ValidationSeverity::Error);
        v.field = Some("Amount".to_string());
        let validations = vec![v];
        let fields = make_fields(&[("Amount", Value::Integer(-5))]);
        let result = run_validations(&validations, &fields, &eval_positive);
        assert!(matches!(result, ValidationResult::Failed { .. }));
    }

    #[test]
    fn test_validation_inline_cxl_fail_warn() {
        let mut v = make_validation("Amount > 0", ValidationSeverity::Warn);
        v.field = Some("Amount".to_string());
        let validations = vec![v];
        let fields = make_fields(&[("Amount", Value::Integer(-5))]);
        let result = run_validations(&validations, &fields, &eval_positive);
        assert!(matches!(result, ValidationResult::Warnings(_)));
    }

    #[test]
    fn test_validation_short_circuit_error() {
        // Two error-severity validations; first fails → second never runs
        let mut v1 = make_validation("check1", ValidationSeverity::Error);
        v1.field = Some("Amount".to_string());
        let v2 = make_validation("check2", ValidationSeverity::Error);
        let validations = vec![v1, v2];
        let fields = make_fields(&[("Amount", Value::Integer(-5))]);

        let result = run_validations(&validations, &fields, &eval_positive);
        match result {
            ValidationResult::Failed { validation_name, .. } => {
                assert!(validation_name.contains("check1"));
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_validation_no_short_circuit_warn() {
        // Two warn-severity validations; both should run
        let mut v1 = make_validation("check1", ValidationSeverity::Warn);
        v1.field = Some("Amount".to_string());
        let mut v2 = make_validation("check2", ValidationSeverity::Warn);
        v2.field = Some("Amount".to_string());
        let validations = vec![v1, v2];
        let fields = make_fields(&[("Amount", Value::Integer(-5))]);

        let result = run_validations(&validations, &fields, &eval_positive);
        match result {
            ValidationResult::Warnings(ws) => assert_eq!(ws.len(), 2),
            _ => panic!("expected Warnings"),
        }
    }

    #[test]
    fn test_validation_auto_name() {
        let mut v = make_validation("is_positive", ValidationSeverity::Error);
        v.field = Some("salary".to_string());
        assert_eq!(v.resolved_name(), "salary:is_positive");

        let v2 = make_validation("Amount > 0", ValidationSeverity::Error);
        assert_eq!(v2.resolved_name(), "Amount > 0");
    }

    #[test]
    fn test_validation_cross_field_no_field() {
        // Cross-field validation: no field specified, check evaluates against full record
        let v = make_validation("Amount > 0", ValidationSeverity::Error);
        let fields = make_fields(&[("Amount", Value::Integer(100))]);
        let result = run_validations(&[v], &fields, &eval_positive);
        assert!(matches!(result, ValidationResult::Passed));
    }

    #[test]
    fn test_validation_dlq_category() {
        let mut v = make_validation("check", ValidationSeverity::Error);
        v.field = Some("Amount".to_string());
        let fields = make_fields(&[("Amount", Value::Integer(-1))]);
        let result = run_validations(&[v], &fields, &eval_positive);
        // Caller would use DlqErrorCategory::ValidationFailure
        assert!(matches!(result, ValidationResult::Failed { .. }));
    }

    #[test]
    fn test_validation_dlq_detail() {
        let mut v = make_validation("check", ValidationSeverity::Error);
        v.field = Some("Amount".to_string());
        v.message = Some("Amount must be positive, got {Amount}".to_string());
        let fields = make_fields(&[("Amount", Value::Integer(-1))]);
        let result = run_validations(&[v], &fields, &eval_positive);
        match result {
            ValidationResult::Failed { message, .. } => {
                assert!(message.contains("Amount must be positive"));
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_validation_message_interpolation() {
        let mut v = make_validation("check", ValidationSeverity::Error);
        v.field = Some("Amount".to_string());
        v.message = Some("employee {employee_id} failed".to_string());
        let fields = make_fields(&[("Amount", Value::Integer(-1)), ("employee_id", Value::String("E123".into()))]);
        let result = run_validations(&[v], &fields, &eval_positive);
        match result {
            ValidationResult::Failed { message, .. } => {
                assert_eq!(message, "employee {employee_id} failed");
                // Note: template interpolation happens at the caller level (log_template),
                // not in the validation runner itself
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_validation_mixed_severity_order() {
        // warn, error, warn → first warn runs, then error stops, third never runs
        let mut v1 = make_validation("warn1", ValidationSeverity::Warn);
        v1.field = Some("Amount".to_string());
        let mut v2 = make_validation("err1", ValidationSeverity::Error);
        v2.field = Some("Amount".to_string());
        let mut v3 = make_validation("warn2", ValidationSeverity::Warn);
        v3.field = Some("Amount".to_string());
        let validations = vec![v1, v2, v3];
        let fields = make_fields(&[("Amount", Value::Integer(-1))]);

        let result = run_validations(&validations, &fields, &eval_positive);
        // Error short-circuits after warn1 runs and err1 fails
        match result {
            ValidationResult::Failed { validation_name, .. } => {
                assert!(validation_name.contains("err1"));
            }
            _ => panic!("expected Failed from error severity"),
        }
    }

    #[test]
    fn test_validation_args_treated_as_literal() {
        // Args are YAML literals, not field references (Decision #13)
        let mut v = make_validation("in_range", ValidationSeverity::Error);
        v.args = Some(IndexMap::from([
            ("max".to_string(), serde_json::json!(500000)),
            ("field_name".to_string(), serde_json::json!("Amount")),
        ]));
        // The string "Amount" is a literal, not a field reference
        assert_eq!(v.args.as_ref().unwrap()["field_name"], serde_json::json!("Amount"));
    }

    // ── Enum enforcement tests ────────────────────────────────────

    #[test]
    fn test_enum_enforcement_valid_value() {
        let result = enforce_enum("status", &Value::String("B".into()), &["A".into(), "B".into(), "C".into()], false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enum_enforcement_invalid_value() {
        let result = enforce_enum("status", &Value::String("D".into()), &["A".into(), "B".into(), "C".into()], false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not in allowed values"));
    }

    #[test]
    fn test_enum_enforcement_null_non_required() {
        let result = enforce_enum("status", &Value::Null, &["A".into(), "B".into()], false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_enum_enforcement_null_required() {
        let result = enforce_enum("status", &Value::Null, &["A".into(), "B".into()], true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("required but got null"));
    }

    #[test]
    fn test_enum_enforcement_after_coercion() {
        // Integer coerced to string representation matches enum
        let result = enforce_enum("code", &Value::Integer(1), &["1".into(), "2".into(), "3".into()], false);
        assert!(result.is_ok());
    }

    // ── Structure ordering tests ──────────────────────────────────

    #[test]
    fn test_structure_ordering_valid() {
        let record_types = vec!["header".into(), "detail".into(), "detail".into(), "footer".into()];
        let constraints = vec![
            ("header".into(), "1".into()),
            ("detail".into(), "1+".into()),
            ("footer".into(), "1".into()),
        ];
        let warnings = validate_structure_ordering(&record_types, &constraints);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_validation_order_after_let() {
        // Let bindings should be available during validation (fields map includes them)
        let fields = make_fields(&[
            ("Amount", Value::Integer(100)),
            ("doubled", Value::Integer(200)), // from let doubled = Amount * 2
        ]);
        let mut v = make_validation("doubled > 0", ValidationSeverity::Error);
        v.field = Some("doubled".to_string());
        let result = run_validations(&[v], &fields, &eval_positive);
        assert!(matches!(result, ValidationResult::Passed));
    }

    #[test]
    fn test_validation_nonexistent_field() {
        // When field doesn't exist in record, eval_check should handle gracefully
        let fields = make_fields(&[("Amount", Value::Integer(100))]);
        let mut v = make_validation("check", ValidationSeverity::Error);
        v.field = Some("nonexistent".to_string());
        // Evaluator returns false for missing field
        let result = run_validations(&[v], &fields, &|_, field, record| {
            if let Some(f) = field {
                Ok(record.contains_key(f))
            } else {
                Ok(false)
            }
        });
        assert!(matches!(result, ValidationResult::Failed { .. }));
    }

    #[test]
    fn test_structure_ordering_violation() {
        let record_types = vec!["footer".into(), "header".into(), "detail".into()];
        let constraints = vec![
            ("header".into(), "1".into()),
            ("detail".into(), "1+".into()),
            ("footer".into(), "1".into()),
        ];
        let warnings = validate_structure_ordering(&record_types, &constraints);
        assert!(!warnings.is_empty());
        assert!(warnings[0].contains("out of order") || warnings[0].contains("appears after"));
    }
}
