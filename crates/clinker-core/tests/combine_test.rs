//! Integration test scaffold for Phase Combine.
//!
//! This file contains gate tests for Task C.0.0 of the combine-node plan.
//! The real deserialization/parse tests land in C.0.1 once the
//! `PipelineNode::Combine` variant exists. For now, fixtures are treated as
//! opaque files — `test_combine_fixtures_exist` only verifies presence and
//! non-emptiness.

#[cfg(test)]
mod tests {
    /// Verifies the test module compiles and is discovered by cargo test.
    #[test]
    fn test_combine_scaffold_compiles() {
        // This test passing means the module is correctly wired and discovered
        // by `cargo test --test combine_test`. An empty body is sufficient;
        // the signal is "this test ran", not any runtime assertion.
    }

    /// Verifies all 10 fixture YAML files exist and are readable.
    #[test]
    fn test_combine_fixtures_exist() {
        let fixture_dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/combine");
        let expected = [
            "two_input_equi.yaml",
            "two_input_range.yaml",
            "two_input_mixed.yaml",
            "three_input_shared_key.yaml",
            "match_all.yaml",
            "match_collect.yaml",
            "on_miss_skip.yaml",
            "on_miss_error.yaml",
            "drive_hint.yaml",
            "error_one_input.yaml",
        ];
        for name in expected {
            let path = fixture_dir.join(name);
            assert!(path.exists(), "missing fixture: {}", path.display());
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            assert!(!content.is_empty(), "empty fixture: {}", path.display());
        }
    }
}
