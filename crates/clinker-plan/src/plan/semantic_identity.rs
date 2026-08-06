//! Versioned semantic identity for compiled plans.

use super::CompiledPlan;

/// Versioned semantic identity for one effective compiled plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SemanticFingerprint {
    digest: [u8; 32],
}

impl SemanticFingerprint {
    /// Canonical semantic identity schema version.
    pub const VERSION: u32 = 1;
    /// Digest algorithm used by version 1.
    pub const ALGORITHM: &'static str = "blake3";

    /// Return the semantic identity schema version.
    #[must_use]
    pub const fn version(self) -> u32 {
        Self::VERSION
    }

    /// Return the digest algorithm label.
    #[must_use]
    pub const fn algorithm(self) -> &'static str {
        Self::ALGORITHM
    }

    /// Return the raw 32-byte digest.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    /// Render the digest as lowercase hexadecimal.
    #[must_use]
    pub fn digest_hex(self) -> String {
        blake3::Hash::from_bytes(self.digest).to_hex().to_string()
    }
}

/// Failure to construct a semantic plan identity.
#[derive(Debug)]
pub enum SemanticFingerprintError {
    /// The effective typed plan could not be serialized canonically.
    Serialization(serde_json::Error),
}

impl std::fmt::Display for SemanticFingerprintError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(formatter, "semantic plan serialization failed: {error}")
            }
        }
    }
}

impl std::error::Error for SemanticFingerprintError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialization(error) => Some(error),
        }
    }
}

impl From<serde_json::Error> for SemanticFingerprintError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

impl CompiledPlan {
    /// Compute the versioned semantic identity of this effective plan.
    ///
    /// # Errors
    ///
    /// Returns [`SemanticFingerprintError`] if the typed plan cannot be
    /// represented by the canonical identity schema.
    pub fn semantic_fingerprint(&self) -> Result<SemanticFingerprint, SemanticFingerprintError> {
        Ok(SemanticFingerprint { digest: [0; 32] })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::{CompileContext, load_config, parse_config};
    use crate::resources::{
        CatalogConfig, CompositionDiscovery, ModuleLimits, WorkspaceCatalog,
        collect_cxl_fields_with_composition_identities, collect_direct_imports,
        compile_module_closure,
    };

    const BASE: &str = r#"
pipeline:
  name: semantic_fixture
  batch_size: 64
  vars:
    threshold: { type: int, default: 4 }
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: map
    input: src
    config:
      cxl: "emit value = id + $vars.threshold"
  - type: output
    name: out
    input: map
    config:
      name: out
      path: output.csv
      type: csv
"#;

    fn compile(yaml: &str) -> CompiledPlan {
        parse_config(yaml)
            .expect("parse semantic fixture")
            .compile(&CompileContext::default())
            .expect("compile semantic fixture")
    }

    fn fingerprint(yaml: &str) -> SemanticFingerprint {
        compile(yaml)
            .semantic_fingerprint()
            .expect("fingerprint semantic fixture")
    }

    fn compile_workspace(workspace: &Path, pipeline: &Path) -> CompiledPlan {
        let config = load_config(pipeline).expect("load workspace pipeline");
        let pipeline_dir = pipeline
            .parent()
            .expect("pipeline parent")
            .strip_prefix(workspace)
            .expect("pipeline inside workspace");
        let mut context = CompileContext::with_pipeline_dir(workspace, pipeline_dir);
        let CompositionDiscovery { fields, identities } =
            collect_cxl_fields_with_composition_identities(
                &config.nodes,
                context.workspace_root(),
                &context.pipeline_dir,
            )
            .expect("collect executable dependency closure");
        context.composition_body_identities = identities;
        let roots = collect_direct_imports(&fields).expect("collect direct module roots");
        if !roots.is_empty() {
            let catalog = WorkspaceCatalog::load(workspace, &CatalogConfig::default())
                .expect("load workspace catalog");
            let rules_root = catalog
                .select_rules_root(None, config.pipeline.rules_path.as_deref().map(Path::new))
                .expect("resolve rules root");
            context.cxl_modules =
                compile_module_closure(&catalog, &rules_root, &roots, ModuleLimits::default())
                    .expect("compile module closure");
        }
        config.compile(&context).expect("compile workspace plan")
    }

    #[test]
    fn semantic_fingerprint_exposes_stable_v1_blake3_rendering() {
        let identity = fingerprint(BASE);
        assert_eq!(identity.version(), 1);
        assert_eq!(identity.algorithm(), "blake3");
        let rendered = identity.digest_hex();
        assert_eq!(rendered.len(), 64);
        assert!(
            rendered
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(
            rendered,
            blake3::Hash::from_bytes(identity.digest())
                .to_hex()
                .to_string()
        );
    }

    #[test]
    fn semantic_fingerprint_ignores_yaml_text_and_mapping_order() {
        let reordered = r#"
# formatting and mapping order are not execution meaning
pipeline: { vars: { threshold: { default: 4, type: int } }, batch_size: 64, name: semantic_fixture }
nodes:
  - config:
      schema: [{ type: int, name: id }]
      type: csv
      path: input.csv
      name: src
    name: src
    type: source
  - input: src
    config: { cxl: "emit value = id + $vars.threshold" }
    type: transform
    name: map
  - config: { type: csv, path: output.csv, name: out }
    input: map
    name: out
    type: output
"#;
        assert_eq!(fingerprint(BASE), fingerprint(reordered));
    }

    #[test]
    fn semantic_fingerprint_changes_for_effective_execution_meaning() {
        let base = fingerprint(BASE);
        for changed in [
            BASE.replace("semantic_fixture", "other_pipeline"),
            BASE.replace("batch_size: 64", "batch_size: 32"),
            BASE.replace("default: 4", "default: 5"),
            BASE.replace("id + $vars.threshold", "id * $vars.threshold"),
            BASE.replace("type: int }", "type: float }"),
        ] {
            assert_ne!(
                base,
                fingerprint(&changed),
                "meaningful change must invalidate v1"
            );
        }
    }

    #[test]
    fn semantic_fingerprint_preserves_authored_node_sequence() {
        let ordered = BASE.replace(
            "- { name: id, type: int }",
            "- { name: id, type: int }\n        - { name: other, type: int }",
        );
        let reordered = BASE.replace(
            "- { name: id, type: int }",
            "- { name: other, type: int }\n        - { name: id, type: int }",
        );
        assert_ne!(fingerprint(&ordered), fingerprint(&reordered));
    }

    #[test]
    fn semantic_fingerprint_excludes_physical_and_runtime_only_policy() {
        let changed = BASE
            .replace("input.csv", "mounted/elsewhere/input.csv")
            .replace("output.csv", "mounted/elsewhere/output.csv")
            .replace(
                "pipeline:\n  name: semantic_fixture",
                "pipeline:\n  name: semantic_fixture\n  memory: { limit: 1G }\n  concurrency: { threads: 7 }",
            )
            .replace(
                "nodes:",
                "error_handling:\n  strategy: continue\n  dlq: { path: deployment/a.ndjson, include_reason: true }\nnodes:",
            );
        let same_policy = changed.replace("deployment/a.ndjson", "deployment/b.ndjson");
        assert_eq!(fingerprint(&changed), fingerprint(&same_policy));

        let base_with_same_semantics = BASE.replace(
            "nodes:",
            "error_handling:\n  strategy: continue\n  dlq: { path: deployment/b.ndjson, include_reason: true }\nnodes:",
        );
        assert_eq!(
            fingerprint(&changed),
            fingerprint(&base_with_same_semantics)
        );
    }

    #[test]
    fn semantic_fingerprint_keeps_pipeline_hash_byte_oriented() {
        let workspace = tempfile::tempdir().expect("temp workspace");
        let first_path = workspace.path().join("first.yaml");
        let second_path = workspace.path().join("second.yaml");
        std::fs::write(&first_path, BASE).expect("write first pipeline");
        std::fs::write(&second_path, format!("# comment\n{BASE}")).expect("write second pipeline");
        let first = load_config(&first_path)
            .expect("load first")
            .compile(&CompileContext::default())
            .expect("compile first");
        let second = load_config(&second_path)
            .expect("load second")
            .compile(&CompileContext::default())
            .expect("compile second");
        assert_ne!(first.pipeline_hash(), second.pipeline_hash());
        assert_eq!(
            first.semantic_fingerprint().expect("first identity"),
            second.semantic_fingerprint().expect("second identity")
        );
    }

    #[test]
    fn semantic_fingerprint_includes_module_content_imports_and_visibility() {
        let workspace = tempfile::tempdir().expect("temp workspace");
        let rules = workspace.path().join("rules");
        std::fs::create_dir_all(rules.join("shared")).expect("create rules");
        std::fs::write(rules.join("shared/base.cxl"), "fn bump(x) = x + 1\n")
            .expect("write dependency module");
        std::fs::write(
            rules.join("root.cxl"),
            "use shared.base as base\nfn bump(x) = base.bump(x)\n",
        )
        .expect("write root module");
        let pipeline = workspace.path().join("pipeline.yaml");
        std::fs::write(
            &pipeline,
            BASE.replace(
                "      cxl: \"emit value = id + $vars.threshold\"",
                "      cxl: |\n        use root\n        emit value = root.bump(id)",
            ),
        )
        .expect("write module pipeline");
        let first = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("first module identity");
        std::fs::write(rules.join("shared/base.cxl"), "fn bump(x) = x + 2\n")
            .expect("change dependency module");
        let second = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("second module identity");
        assert_ne!(first, second);

        std::fs::write(
            rules.join("root.cxl"),
            "use shared.base as dependency\nfn bump(x) = dependency.bump(x)\n",
        )
        .expect("change import alias");
        let third = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("third module identity");
        assert_ne!(second, third);
    }

    #[test]
    fn semantic_fingerprint_includes_composition_body_content_without_its_path() {
        let workspace = tempfile::tempdir().expect("temp workspace");
        let pipelines = workspace.path().join("pipelines");
        let compositions = workspace.path().join("compositions");
        std::fs::create_dir_all(&pipelines).expect("create pipelines");
        std::fs::create_dir_all(&compositions).expect("create compositions");
        let body = compositions.join("gate.comp.yaml");
        let body_source = |expression: &str| {
            format!(
                "_compose:\n  name: gate\n  inputs:\n    inp:\n      schema: [{{ name: id, type: int }}]\n  outputs:\n    out: mapped\nnodes:\n  - type: transform\n    name: mapped\n    input: inp\n    config:\n      cxl: \"emit id = {expression}\"\n"
            )
        };
        std::fs::write(&body, body_source("id + 1")).expect("write body");
        let pipeline = pipelines.join("pipeline.yaml");
        let yaml = r#"
pipeline: { name: composition_identity }
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema: [{ name: id, type: int }]
  - type: composition
    name: gate_call
    input: src
    use: ../compositions/gate.comp.yaml
    inputs: { inp: src }
  - type: output
    name: out
    input: gate_call
    config: { name: out, path: output.csv, type: csv }
"#;
        std::fs::write(&pipeline, yaml).expect("write composition pipeline");
        let first = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("first body identity");
        std::fs::write(&body, body_source("id + 2")).expect("change body");
        let second = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("second body identity");
        assert_ne!(first, second);
    }
}
