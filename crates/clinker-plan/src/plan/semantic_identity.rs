//! Versioned semantic identity for compiled plans.

use super::CompiledPlan;
use clinker_record::Value;
use indexmap::IndexMap;
use serde::Serialize;

const SEMANTIC_FINGERPRINT_DOMAIN: &[u8] = b"clinker.semantic-fingerprint.v1\0";

/// Versioned semantic identity for one effective compiled plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SemanticFingerprint {
    digest: [u8; 32],
}

/// Runtime values supplied by the resolved channel/group stack.
///
/// These maps are execution inputs even though they do not live in the
/// compiled pipeline AST. Keeping them in one typed object prevents callers
/// from advertising a plan identity that omits one of the four variable
/// scopes later passed to the executor.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EffectiveRuntimeVariables {
    /// Resolved `$vars.*` values.
    pub static_vars: IndexMap<String, Value>,
    /// Resolved `$pipeline.*` values.
    pub pipeline_vars: IndexMap<String, Value>,
    /// Resolved `$source.<source>.*` values.
    pub source_vars: IndexMap<String, IndexMap<String, Value>>,
    /// Resolved `$record.*` values.
    pub record_vars: IndexMap<String, Value>,
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
        self.semantic_fingerprint_with_runtime_variables(&EffectiveRuntimeVariables::default())
    }

    /// Compute the versioned semantic identity of this effective plan and its
    /// resolved runtime-variable inputs.
    ///
    /// The effective values in [`Self::provenance`] are included without
    /// their source spans or layer-file byte identities. Together with the
    /// compiled topology and `runtime_variables`, that is the canonical
    /// semantic equivalent of the selected channel stack: relocation,
    /// comments, and layer formatting do not change the digest, while a
    /// winning config or variable value does.
    ///
    /// # Errors
    ///
    /// Returns [`SemanticFingerprintError`] if the effective inputs cannot be
    /// represented by the canonical identity schema.
    pub fn semantic_fingerprint_with_runtime_variables(
        &self,
        runtime_variables: &EffectiveRuntimeVariables,
    ) -> Result<SemanticFingerprint, SemanticFingerprintError> {
        let nodes = self
            .config()
            .nodes
            .iter()
            .map(|node| semantic_node(&node.value))
            .collect::<Result<Vec<_>, _>>()?;
        let mut error_handling = serde_json::to_value(&self.config().error_handling)?;
        if let Some(dlq) = error_handling
            .as_object_mut()
            .and_then(|error| error.get_mut("dlq"))
        {
            remove_deployment_paths(dlq);
        }

        let mut composition_bodies = self
            .composition_bodies()
            .iter()
            .map(|(id, body)| {
                serde_json::json!({
                    "ordinal": id.0,
                    "name": body.semantic_name,
                    "content_digest": digest_hex(body.content_digest),
                })
            })
            .collect::<Vec<_>>();
        composition_bodies.sort_by_key(|body| {
            body.get("ordinal")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(u64::MAX)
        });

        let modules = self
            .cxl_modules()
            .semantic_identities()
            .into_iter()
            .map(|module| {
                let imports = module
                    .imports
                    .into_iter()
                    .map(|(alias, dependency)| {
                        serde_json::json!({
                            "alias": alias,
                            "module": dependency,
                        })
                    })
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "id": module.id,
                    "content_digest": digest_hex(module.content_digest),
                    "imports": imports,
                    "program_visible": module.program_visible,
                })
            })
            .collect::<Vec<_>>();

        let mut resolved_config = self
            .provenance()
            .iter()
            .map(|(_key, address, resolved)| {
                serde_json::json!({
                    "address": address.render(),
                    "value": resolved.value,
                })
            })
            .collect::<Vec<_>>();
        resolved_config.sort_by(|left, right| {
            let left = left
                .get("address")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let right = right
                .get("address")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            left.cmp(right)
        });

        let mut identity = serde_json::json!({
            "version": SemanticFingerprint::VERSION,
            "pipeline": {
                "name": self.config().pipeline.name,
                "batch_size": self.config().pipeline.batch_size,
                "vars": self.config().pipeline.vars,
                "date_formats": self.config().pipeline.date_formats,
            },
            "nodes": nodes,
            "error_handling": error_handling,
            "bound_schemas": self.bound_schemas(),
            "resolved_config": resolved_config,
            "runtime_variables": runtime_variables,
            "dependencies": {
                "composition_bodies": composition_bodies,
                "cxl_modules": modules,
            },
        });
        canonicalize(&mut identity);
        let encoded = serde_json::to_vec(&identity)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(SEMANTIC_FINGERPRINT_DOMAIN);
        hasher.update(&(encoded.len() as u64).to_le_bytes());
        hasher.update(&encoded);
        Ok(SemanticFingerprint {
            digest: *hasher.finalize().as_bytes(),
        })
    }
}

fn semantic_node(
    node: &crate::config::PipelineNode,
) -> Result<serde_json::Value, serde_json::Error> {
    let mut value = serde_json::to_value(node)?;
    let Some(object) = value.as_object_mut() else {
        return Ok(value);
    };
    match object.get("type").and_then(serde_json::Value::as_str) {
        Some("source") => {
            if let Some(config) = object
                .get_mut("config")
                .and_then(serde_json::Value::as_object_mut)
            {
                config.remove("path");
                config.remove("paths");
                // The resolved schema is represented once in `bound_schemas`.
                config.remove("schema");
            }
        }
        Some("output") => {
            if let Some(config) = object
                .get_mut("config")
                .and_then(serde_json::Value::as_object_mut)
            {
                config.remove("path");
            }
        }
        Some("composition") => {
            object.remove("use");
            // Resource bindings are deployment locators. The body digest and
            // typed call-site config carry execution meaning separately.
            object.remove("resources");
        }
        _ => {}
    }
    Ok(value)
}

fn remove_deployment_paths(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                remove_deployment_paths(value);
            }
        }
        serde_json::Value::Object(object) => {
            object.remove("path");
            for value in object.values_mut() {
                remove_deployment_paths(value);
            }
        }
        _ => {}
    }
}

fn canonicalize(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize(value);
            }
        }
        serde_json::Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut value) in entries {
                canonicalize(&mut value);
                object.insert(key, value);
            }
        }
        _ => {}
    }
}

fn digest_hex(digest: [u8; 32]) -> String {
    blake3::Hash::from_bytes(digest).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::composition::LayerKind;
    use crate::config::{CompileContext, load_config, parse_config};
    use crate::resources::{
        CatalogConfig, CompositionDiscovery, ModuleLimits, WorkspaceCatalog,
        collect_cxl_fields_with_composition_identities, collect_direct_imports,
        compile_module_closure,
    };
    use clinker_core_types::Span;

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
        let continuing = BASE.replace(
            "nodes:",
            "error_handling:\n  strategy: continue\n  dlq: { path: deployment/dlq.ndjson }\nnodes:",
        );
        assert_ne!(base, fingerprint(&continuing));
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
    fn semantic_fingerprint_includes_every_runtime_variable_scope() {
        let plan = compile(BASE);
        let empty = EffectiveRuntimeVariables::default();
        let baseline = plan
            .semantic_fingerprint_with_runtime_variables(&empty)
            .expect("baseline identity");

        let mut static_vars = EffectiveRuntimeVariables::default();
        static_vars
            .static_vars
            .insert("threshold".to_owned(), Value::Integer(7));
        let mut pipeline_vars = EffectiveRuntimeVariables::default();
        pipeline_vars
            .pipeline_vars
            .insert("checkpoint".to_owned(), Value::Integer(7));
        let mut source_vars = EffectiveRuntimeVariables::default();
        source_vars.source_vars.insert(
            "src".to_owned(),
            IndexMap::from_iter([("partition".to_owned(), Value::Integer(7))]),
        );
        let mut record_vars = EffectiveRuntimeVariables::default();
        record_vars
            .record_vars
            .insert("quality".to_owned(), Value::Integer(7));

        for variables in [&static_vars, &pipeline_vars, &source_vars, &record_vars] {
            assert_ne!(
                baseline,
                plan.semantic_fingerprint_with_runtime_variables(variables)
                    .expect("runtime-variable identity")
            );
        }
    }

    #[test]
    fn semantic_fingerprint_includes_winning_composition_config_value() {
        let workspace = tempfile::tempdir().expect("temp workspace");
        let compositions = workspace.path().join("compositions");
        std::fs::create_dir_all(&compositions).expect("create compositions");
        std::fs::write(
            compositions.join("increment.comp.yaml"),
            r#"
_compose:
  name: increment
  inputs:
    inp:
      schema: [{ name: id, type: int }]
  outputs: { out: mapped }
  config_schema:
    amount: { type: int, default: 1 }
nodes:
  - type: transform
    name: mapped
    input: inp
    config:
      cxl: "emit id = id + $config.amount"
"#,
        )
        .expect("write composition");
        let pipeline = workspace.path().join("pipeline.yaml");
        std::fs::write(
            &pipeline,
            r#"
pipeline: { name: resolved_config_identity }
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema: [{ name: id, type: int }]
  - type: composition
    name: increment_call
    input: src
    use: compositions/increment.comp.yaml
    inputs: { inp: src }
  - type: output
    name: out
    input: increment_call
    config: { name: out, path: output.csv, type: csv }
"#,
        )
        .expect("write pipeline");

        let base = compile_workspace(workspace.path(), &pipeline);
        let mut overridden = compile_workspace(workspace.path(), &pipeline);
        overridden
            .provenance_mut()
            .get_mut("increment_call", "amount")
            .expect("composition config provenance")
            .apply_layer(
                serde_json::json!(2),
                LayerKind::ChannelWide,
                Span::SYNTHETIC,
            );

        assert_ne!(
            base.semantic_fingerprint().expect("base identity"),
            overridden
                .semantic_fingerprint()
                .expect("overridden identity")
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

        let config = load_config(&pipeline).expect("reload module pipeline");
        let mut context = CompileContext::default();
        let catalog = WorkspaceCatalog::load(workspace.path(), &CatalogConfig::default())
            .expect("reload workspace catalog");
        let rules_root = catalog
            .select_rules_root(None, None)
            .expect("resolve rules root");
        let direct_roots = [
            crate::resources::LogicalResourceId::parse("root").expect("root id"),
            crate::resources::LogicalResourceId::parse("shared.base").expect("dependency id"),
        ];
        context.cxl_modules = compile_module_closure(
            &catalog,
            &rules_root,
            &direct_roots,
            ModuleLimits::default(),
        )
        .expect("compile closure with direct dependency visibility");
        let visible_dependency = config
            .compile(&context)
            .expect("compile plan with direct dependency visibility")
            .semantic_fingerprint()
            .expect("direct visibility identity");
        assert_ne!(third, visible_dependency);
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

        let relocated = compositions.join("relocated.comp.yaml");
        std::fs::rename(&body, &relocated).expect("relocate body");
        let relocated_yaml = yaml.replace("gate.comp.yaml", "relocated.comp.yaml");
        std::fs::write(&pipeline, relocated_yaml).expect("rewrite composition locator");
        let relocated_identity = compile_workspace(workspace.path(), &pipeline)
            .semantic_fingerprint()
            .expect("relocated body identity");
        assert_eq!(second, relocated_identity);
    }
}
