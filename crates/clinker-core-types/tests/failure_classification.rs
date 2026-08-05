//! Public contract tests for the shared failure taxonomy.

use std::collections::BTreeSet;

use clinker_core_types::diagnostic::{
    AttemptDiagnosticData, AttemptOperation, FinalVisibility, REGISTRY as DIAGNOSTIC_REGISTRY,
};
use clinker_core_types::{FailureCategory, FailureClassification, RetryAdvice};

mod taxonomy {
    use super::*;

    #[test]
    fn retry_advice_has_exact_wire_spellings() {
        assert_eq!(RetryAdvice::DoNotRetry.as_str(), "do_not_retry");
        assert_eq!(RetryAdvice::RetryWithBackoff.as_str(), "retry_with_backoff");
        assert_eq!(RetryAdvice::PolicyRequired.as_str(), "policy_required");
    }

    #[test]
    fn broad_categories_have_stable_wire_spellings() {
        let cases = [
            (FailureCategory::SecurityPolicy, "security_policy"),
            (FailureCategory::SourceProtocol, "source_protocol"),
            (FailureCategory::InternalInvariant, "internal_invariant"),
            (FailureCategory::Configuration, "configuration"),
            (FailureCategory::Infrastructure, "infrastructure"),
            (FailureCategory::Publication, "publication"),
            (FailureCategory::Observability, "observability"),
        ];

        for (category, wire) in cases {
            assert_eq!(category.as_str(), wire);
        }
    }

    #[test]
    fn registry_codes_are_unique_namespaced_and_cover_every_failure_family() {
        let codes: Vec<_> = FailureClassification::registered_codes().collect();
        let unique: BTreeSet<_> = codes.iter().copied().collect();
        assert_eq!(unique.len(), codes.len(), "failure codes must be unique");
        assert!(codes.iter().all(|code| {
            code.split('.').count() >= 3
                && code.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'.'
                        || byte == b'_'
                })
        }));

        for prefix in [
            "rest.security.",
            "rest.protocol.",
            "runtime.invariant.",
            "admission.configuration.",
            "infrastructure.runtime.",
            "attempt.publication.",
            "attempt.retention.",
            "observability.configuration.",
            "observability.delivery.",
        ] {
            assert!(
                codes.iter().any(|code| code.starts_with(prefix)),
                "registry has no code in required family {prefix}"
            );
        }
    }

    #[test]
    fn attempt_retention_rows_are_appended_in_exact_contract_order() {
        let codes: Vec<_> = FailureClassification::registered_codes().collect();
        let publication_end = codes
            .iter()
            .position(|code| *code == "attempt.publication.promotion_failed")
            .expect("publication family remains registered");

        assert_eq!(
            &codes[publication_end + 1..publication_end + 7],
            [
                "attempt.retention.ownership_refused",
                "attempt.retention.manifest_invalid",
                "attempt.retention.live",
                "attempt.retention.clock_ambiguous",
                "attempt.retention.budget_exhausted",
                "attempt.retention.cleanup_failed",
            ]
        );

        let cases = [
            (
                "attempt.retention.ownership_refused",
                FailureCategory::SecurityPolicy,
                RetryAdvice::PolicyRequired,
            ),
            (
                "attempt.retention.manifest_invalid",
                FailureCategory::SecurityPolicy,
                RetryAdvice::PolicyRequired,
            ),
            (
                "attempt.retention.live",
                FailureCategory::Publication,
                RetryAdvice::PolicyRequired,
            ),
            (
                "attempt.retention.clock_ambiguous",
                FailureCategory::Publication,
                RetryAdvice::PolicyRequired,
            ),
            (
                "attempt.retention.budget_exhausted",
                FailureCategory::Publication,
                RetryAdvice::RetryWithBackoff,
            ),
            (
                "attempt.retention.cleanup_failed",
                FailureCategory::Infrastructure,
                RetryAdvice::RetryWithBackoff,
            ),
        ];

        for (code, category, retry) in cases {
            let failure = FailureClassification::for_code(code).expect("registered retention row");
            assert_eq!(failure.category(), category, "category for {code}");
            assert_eq!(failure.retry_advice(), retry, "retry advice for {code}");
        }
    }

    #[test]
    fn attempt_diagnostics_bind_codes_to_failure_families_and_redacted_data() {
        let refusal = AttemptDiagnosticData::for_failure(
            "attempt.retention.ownership_refused",
            AttemptOperation::Purge,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            Some("artifact-00000001"),
            FinalVisibility::None,
            false,
            "pipelines/orders.yaml",
        )
        .expect("safe refusal data");
        assert_eq!(refusal.diagnostic_code(), "E371");
        assert_eq!(
            refusal.failure_code(),
            "attempt.retention.ownership_refused"
        );
        assert_eq!(refusal.operation(), AttemptOperation::Purge);
        assert_eq!(refusal.operation().as_str(), "purge");
        assert_eq!(
            refusal.execution_id(),
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159"
        );
        assert_eq!(refusal.artifact_id(), Some("artifact-00000001"));
        assert_eq!(refusal.final_visibility(), FinalVisibility::None);
        assert_eq!(refusal.final_visibility().as_str(), "none");
        assert!(!refusal.durability_uncertain());
        assert_eq!(refusal.retry_advice(), RetryAdvice::PolicyRequired);
        assert_eq!(
            refusal.recovery_command(),
            "clinker attempts inspect pipelines/orders.yaml --execution-id 018f47a2-9a41-7a27-b4d6-4f7137e3c159"
        );
        assert_eq!(
            refusal.recovery_argv(),
            [
                "clinker",
                "attempts",
                "inspect",
                "pipelines/orders.yaml",
                "--execution-id",
                "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            ]
        );

        let cleanup = AttemptDiagnosticData::for_failure(
            "attempt.retention.budget_exhausted",
            AttemptOperation::Purge,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            None,
            FinalVisibility::Some,
            true,
            "pipelines/orders.yaml",
        )
        .expect("safe cleanup data");
        assert_eq!(cleanup.diagnostic_code(), "E372");
        assert_eq!(cleanup.retry_advice(), RetryAdvice::RetryWithBackoff);
        assert!(cleanup.durability_uncertain());

        let diagnostic_codes: Vec<_> = DIAGNOSTIC_REGISTRY
            .iter()
            .filter(|entry| matches!(entry.code, "E371" | "E372"))
            .map(|entry| entry.code)
            .collect();
        assert_eq!(diagnostic_codes, ["E371", "E372"]);
    }

    #[test]
    fn attempt_diagnostics_reject_unregistered_mismatched_or_sensitive_data() {
        let args = || {
            (
                AttemptOperation::Inspect,
                "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
                None,
                FinalVisibility::Unknown,
                true,
            )
        };

        let (operation, execution_id, artifact_id, visibility, uncertain) = args();
        assert!(
            AttemptDiagnosticData::for_failure(
                "attempt.publication.promotion_failed",
                operation,
                execution_id,
                artifact_id,
                visibility,
                uncertain,
                "pipelines/orders.yaml",
            )
            .is_none()
        );

        for (execution_id, artifact_id, pipeline) in [
            ("token=secret", None, "pipelines/orders.yaml"),
            (
                "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
                Some("record={customer-secret}"),
                "pipelines/orders.yaml",
            ),
            (
                "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
                None,
                "/private/orders.yaml",
            ),
            (
                "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
                None,
                "../private/orders.yaml",
            ),
        ] {
            let (operation, _, _, visibility, uncertain) = args();
            assert!(
                AttemptDiagnosticData::for_failure(
                    "attempt.retention.manifest_invalid",
                    operation,
                    execution_id,
                    artifact_id,
                    visibility,
                    uncertain,
                    pipeline,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn construction_binds_registered_category_and_retry_advice() {
        let security = FailureClassification::new(
            "rest.security.cross_origin",
            "REST continuation violates same-origin policy",
        )
        .expect("registered security failure");
        assert_eq!(security.code(), "rest.security.cross_origin");
        assert_eq!(security.category(), FailureCategory::SecurityPolicy);
        assert_eq!(security.retry_advice(), RetryAdvice::DoNotRetry);
        assert_eq!(
            security.message(),
            "REST continuation violates same-origin policy"
        );

        let protocol = FailureClassification::for_code("rest.protocol.malformed_continuation")
            .expect("registered protocol failure");
        assert_eq!(protocol.category(), FailureCategory::SourceProtocol);
        assert_eq!(protocol.retry_advice(), RetryAdvice::PolicyRequired);

        assert!(FailureClassification::for_code("unregistered.code").is_none());
    }

    #[test]
    fn sanitization_rejects_sensitive_and_raw_payload_shapes() {
        let sentinels = [
            "record={ssn: 123-45-6789}",
            "password=hunter2",
            "Authorization: Bearer secret-token",
            "https://user:pass@example.test/items?token=secret",
            "/var/lib/clinker/private/customer.csv",
            r"C:\\Users\\operator\\secrets.txt",
            r"\\server\\share\\tenant\\file.csv",
            "PipelineError::Internal { raw: Some(\"tenant-value\") }",
        ];

        for sentinel in sentinels {
            let failure = FailureClassification::new("runtime.invariant.unknown", sentinel)
                .expect("registered invariant fallback");
            assert_ne!(failure.message(), sentinel);
            assert!(!failure.message().contains("secret"));
            assert!(!failure.message().contains("123-45-6789"));
            assert!(!failure.message().contains("customer.csv"));
            assert!(failure.message().len() <= FailureClassification::MAX_MESSAGE_BYTES);
        }
    }

    #[test]
    fn sanitization_bounds_and_normalizes_messages() {
        let long = "safe detail ".repeat(100);
        let failure = FailureClassification::new("infrastructure.runtime.transient", &long)
            .expect("registered infrastructure failure");
        assert!(failure.message().len() <= FailureClassification::MAX_MESSAGE_BYTES);
        assert!(!failure.message().contains('\n'));

        let normalized = FailureClassification::new(
            "infrastructure.runtime.transient",
            "temporary\nconnection\tfailure",
        )
        .expect("registered infrastructure failure");
        assert_eq!(normalized.message(), "temporary connection failure");
    }

    #[test]
    fn unknown_internal_failure_uses_one_safe_invariant_classification() {
        let failure = FailureClassification::unknown_internal(
            "DebugError { path: /tmp/customer.csv, token: secret }",
        );
        assert_eq!(failure.code(), "runtime.invariant.unknown");
        assert_eq!(failure.category(), FailureCategory::InternalInvariant);
        assert_eq!(failure.retry_advice(), RetryAdvice::PolicyRequired);
        assert_eq!(failure.message(), "internal execution invariant failed");
    }

    #[test]
    fn shared_types_are_plain_copyable_value_types_except_for_owned_message() {
        fn assert_copy<T: Copy>() {}
        fn assert_eq_hash<T: Eq + std::hash::Hash>() {}

        assert_copy::<FailureCategory>();
        assert_copy::<RetryAdvice>();
        assert_eq_hash::<FailureCategory>();
        assert_eq_hash::<RetryAdvice>();

        let failure = FailureClassification::for_code("attempt.publication.promotion_failed")
            .expect("registered publication failure");
        assert_eq!(failure.clone(), failure);
    }
}

mod conformance_fixture {
    use super::*;

    const VERSION_MARKER: &str = "# failure-classification-v1";
    const HEADER: &str = "code\tcategory\tretry_advice";
    const FIXTURE: &str = include_str!("fixtures/failure-classification-v1.tsv");

    #[derive(Debug, Eq, PartialEq)]
    struct Row<'a> {
        code: &'a str,
        category: &'a str,
        retry_advice: &'a str,
    }

    fn rows() -> Vec<Row<'static>> {
        let mut lines = FIXTURE.lines();
        assert_eq!(lines.next(), Some(VERSION_MARKER));
        assert_eq!(lines.next(), Some(HEADER));
        lines
            .map(|line| {
                let fields: Vec<_> = line.split('\t').collect();
                assert_eq!(
                    fields.len(),
                    3,
                    "fixture row must have exactly three fields"
                );
                Row {
                    code: fields[0],
                    category: fields[1],
                    retry_advice: fields[2],
                }
            })
            .collect()
    }

    #[test]
    fn conformance_fixture_matches_every_registered_mapping() {
        let rows = rows();
        let fixture_codes: Vec<_> = rows.iter().map(|row| row.code).collect();
        let registered_codes: Vec<_> = FailureClassification::registered_codes().collect();
        assert_eq!(fixture_codes, registered_codes);

        for row in rows {
            let classification = FailureClassification::for_code(row.code)
                .expect("fixture code must remain registered");
            assert_eq!(classification.category().as_str(), row.category);
            assert_eq!(classification.retry_advice().as_str(), row.retry_advice);
        }
    }

    #[test]
    fn conformance_fixture_is_canonical_and_covers_all_contract_families() {
        let rows = rows();
        let mut rendered = format!("{VERSION_MARKER}\n{HEADER}\n");
        for row in &rows {
            rendered.push_str(&format!(
                "{}\t{}\t{}\n",
                row.code, row.category, row.retry_advice
            ));
        }
        assert_eq!(rendered, FIXTURE);

        let codes: Vec<_> = rows.iter().map(|row| row.code).collect();
        for prefix in [
            "rest.security.",
            "rest.protocol.",
            "runtime.invariant.",
            "admission.configuration.",
            "infrastructure.runtime.",
            "attempt.publication.",
            "attempt.retention.",
            "observability.configuration.",
            "observability.delivery.",
        ] {
            assert!(
                codes.iter().any(|code| code.starts_with(prefix)),
                "fixture has no code in required family {prefix}"
            );
        }

        let retries: BTreeSet<_> = rows.iter().map(|row| row.retry_advice).collect();
        assert_eq!(
            retries,
            BTreeSet::from(["do_not_retry", "policy_required", "retry_with_backoff"])
        );
        assert!(codes.iter().all(|code| {
            !code.contains("cancel")
                && !code.contains("dlq")
                && !code.contains("exit")
                && !code.contains("completed")
        }));
    }

    #[test]
    fn conformance_fixture_unknown_internal_is_stable_and_sanitized() {
        let failure = FailureClassification::unknown_internal(
            "DebugError { path: /private/customer.csv, token: secret }",
        );
        assert_eq!(failure.code(), "runtime.invariant.unknown");
        assert_eq!(failure.category().as_str(), "internal_invariant");
        assert_eq!(failure.retry_advice().as_str(), "policy_required");
        assert_eq!(failure.message(), "internal execution invariant failed");
    }
}
