use clinker_format::FormatError;

#[test]
fn classified_error_preserves_the_exact_structured_code() {
    let error = FormatError::classified(
        "rest.protocol.malformed_continuation",
        "continuation metadata is malformed",
    );
    assert_eq!(
        error.classification_code(),
        Some("rest.protocol.malformed_continuation")
    );
    assert!(error.to_string().contains("continuation metadata"));
}
