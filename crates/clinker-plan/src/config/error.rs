//! Config loading, env-var interpolation, and the [`ConfigError`] hierarchy.

use super::*;
use regex::Regex;
use std::sync::LazyLock;

/// Errors during config loading and parsing.
#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Yaml(crate::yaml::YamlError),
    EnvVar { var_name: String, position: usize },
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "config I/O error: {e}"),
            Self::Yaml(e) => write!(f, "YAML parse error: {e}"),
            Self::EnvVar { var_name, position } => {
                write!(
                    f,
                    "undefined environment variable ${{{var_name}}} at position {position}"
                )
            }
            Self::Validation(msg) => write!(f, "config validation error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<crate::yaml::YamlError> for ConfigError {
    fn from(e: crate::yaml::YamlError) -> Self {
        Self::Yaml(e)
    }
}

// Regex for ${VAR} and ${VAR:-default} interpolation
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}").unwrap());

/// Pre-deserialize environment variable interpolation.
///
/// Replaces `${VAR}` with `env::var("VAR")` and `${VAR:-default}` with
/// the env value or the default. Bare text substitution — no YAML quoting
/// applied (industry standard: dbt, Docker Compose, Helm, envsubst).
///
/// `extra_vars` are checked before `std::env::var` — highest priority among
/// runtime vars. This allows channel variables to shadow system env vars.
///
/// `$${VAR}` produces literal `${VAR}` (escape, same as Docker Compose /
/// Kubernetes convention). The `$$` is replaced with a NULL byte placeholder
/// before the main regex, then restored after all substitution.
///
/// Missing var without default produces `ConfigError::EnvVar`.
pub fn interpolate_env_vars(
    yaml: &str,
    extra_vars: &[(&str, &str)],
) -> Result<String, ConfigError> {
    // Step 1: Replace $$ with NULL byte placeholder before main regex.
    // NULL bytes do not appear in valid YAML config files.
    debug_assert!(
        !yaml.contains('\0'),
        "input YAML contains NULL bytes — $$ escape placeholder collision"
    );
    let escaped = yaml.replace("$$", "\0");

    // Step 2: Run regex substitution with extra_vars priority
    let mut result = String::with_capacity(escaped.len());
    let mut last_end = 0;

    for caps in ENV_VAR_RE.captures_iter(&escaped) {
        let full_match = caps.get(0).unwrap();
        let var_name = caps.get(1).unwrap().as_str();
        let default_value = caps.get(2).map(|m| m.as_str());

        // Validate env var name: must be UPPERCASE + underscores only
        if !var_name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            || var_name.starts_with(|c: char| c.is_ascii_digit())
            || var_name.is_empty()
        {
            return Err(ConfigError::Validation(format!(
                "invalid environment variable name '{}' at position {} — must match [A-Z_][A-Z0-9_]*",
                var_name,
                full_match.start()
            )));
        }

        result.push_str(&escaped[last_end..full_match.start()]);

        // Check extra_vars first, then system env
        if let Some(value) = extra_vars
            .iter()
            .find(|(k, _)| *k == var_name)
            .map(|(_, v)| *v)
        {
            result.push_str(value);
        } else {
            match std::env::var(var_name) {
                Ok(value) => result.push_str(&value),
                Err(_) => match default_value {
                    Some(default) => result.push_str(default),
                    None => {
                        return Err(ConfigError::EnvVar {
                            var_name: var_name.to_string(),
                            position: full_match.start(),
                        });
                    }
                },
            }
        }

        last_end = full_match.end();
    }

    result.push_str(&escaped[last_end..]);

    // Step 3: Restore NULL byte placeholder → $
    Ok(result.replace('\0', "$"))
}

/// Parse a pipeline config from a YAML string (after interpolation).
///
/// All YAML parsing flows through `crate::yaml::from_str`, the single
/// chokepoint that owns the DoS-defense [`Budget`].
pub fn parse_config(yaml: &str) -> Result<PipelineConfig, ConfigError> {
    let interpolated = interpolate_env_vars(yaml, &[])?;
    let config: PipelineConfig = crate::yaml::from_str(&interpolated)?;
    validate_config(&config)?;
    Ok(config)
}

/// Read, interpolate, and parse a pipeline config from a YAML file path
/// **without** running `validate_config`.
///
/// Stamps `config.source_hash` with the BLAKE3 of the post-env-var
/// interpolated YAML so the executor can resolve `{pipeline_hash}` tokens
/// and write provenance sidecars without retaining the source bytes.
///
/// Callers that need a fully-validated config use [`load_config_with_vars`].
/// This split exists so channel source-config patches can mutate the parsed
/// typed config before validation — the patched config is then the one
/// `validate_config` and `compile()` see (see [`load_config_with_vars_and_patches`]).
pub fn parse_config_with_vars(
    path: &std::path::Path,
    extra_vars: &[(&str, &str)],
) -> Result<PipelineConfig, ConfigError> {
    let yaml = std::fs::read_to_string(path)?;
    let interpolated = interpolate_env_vars(&yaml, extra_vars)?;
    let mut config: PipelineConfig = crate::yaml::from_str(&interpolated)?;
    config.source_hash = *blake3::hash(interpolated.as_bytes()).as_bytes();
    // Resolve any external `.schema.yaml` (`SourceSchema::File`) references to
    // their inline form and fold the resolved content into `source_hash`, so
    // the referenced file's content is part of the pipeline identity and the
    // resolved schema flows to `compile` and the runtime without a re-read.
    resolve_and_hash_external_schemas(&mut config)?;
    Ok(config)
}

/// Resolve every source's external `.schema.yaml` ([`SourceSchema::File`](crate::config::SourceSchema))
/// to its inline form, in place, and fold each resolved schema's canonical
/// content into `config.source_hash`.
///
/// `source_hash` is otherwise BLAKE3 over the interpolated YAML text only, so a
/// referenced `.schema.yaml`'s content is absent from the pipeline identity and
/// editing it would silently reuse the same `{pipeline_hash}` — colliding
/// output paths and conflating lineage across schema revisions. Folding the
/// resolved content in (mirroring the channel-patch fold) closes that blind
/// spot. Resolving in place means the config that flows to `compile` and the
/// runtime carries inline columns, so no source schema file is re-read
/// downstream.
///
/// A config with no external schemas leaves `source_hash` byte-identical.
fn resolve_and_hash_external_schemas(config: &mut PipelineConfig) -> Result<(), ConfigError> {
    // Pass 1: load each external schema file once (immutable walk), collecting
    // its resolved inline form and canonical bytes for the identity fold.
    let mut resolved: Vec<(usize, crate::config::SourceSchema, Vec<u8>)> = Vec::new();
    for (idx, spanned) in config.nodes.iter().enumerate() {
        if let crate::config::PipelineNode::Source {
            header,
            config: body,
        } = &spanned.value
            && let crate::config::SourceSchema::File(path) = &body.schema
        {
            let inline =
                crate::schema::load_source_schema(std::path::Path::new(path)).map_err(|e| {
                    ConfigError::Validation(format!(
                        "source {:?}: failed to load schema file '{path}': {e}",
                        header.name
                    ))
                })?;
            let serialized = serde_json::to_vec(&inline).map_err(|e| {
                ConfigError::Validation(format!(
                    "internal: external schema identity hashing for source {:?}: {e}",
                    header.name
                ))
            })?;
            resolved.push((idx, inline, serialized));
        }
    }
    if resolved.is_empty() {
        return Ok(());
    }
    // Pass 2: fold the resolved content into the identity hash and write the
    // inline schema back onto each source node.
    let mut hasher = blake3::Hasher::new();
    hasher.update(&config.source_hash);
    for (idx, inline, serialized) in resolved {
        hasher.update(&serialized);
        if let crate::config::PipelineNode::Source { config: body, .. } =
            &mut config.nodes[idx].value
        {
            body.schema = inline;
        }
    }
    config.source_hash = *hasher.finalize().as_bytes();
    Ok(())
}

/// Load, parse, and validate a pipeline config from a YAML file path with
/// extra variables. `extra_vars` are checked before system env vars during
/// interpolation.
pub fn load_config_with_vars(
    path: &std::path::Path,
    extra_vars: &[(&str, &str)],
) -> Result<PipelineConfig, ConfigError> {
    let config = parse_config_with_vars(path, extra_vars)?;
    validate_config(&config)?;
    Ok(config)
}

/// Load and parse a pipeline config, apply channel source-config patches to
/// the parsed typed config, then validate.
///
/// The patches are applied after parse and before `validate_config`, so the
/// patched config is exactly what validation and `compile()` consume on every
/// run path. An empty patch map is a no-op equivalent to
/// [`load_config_with_vars`]. Unknown source names or ill-formed patch ops
/// surface as [`ConfigError::Validation`] before the pipeline compiles.
pub fn load_config_with_vars_and_patches(
    path: &std::path::Path,
    extra_vars: &[(&str, &str)],
    patches: &indexmap::IndexMap<String, crate::config::patch::SourceConfigPatch>,
) -> Result<PipelineConfig, ConfigError> {
    let mut config = parse_config_with_vars(path, extra_vars)?;
    crate::config::patch::apply_source_patches(&mut config, patches)?;
    // Fold the applied patches into the pipeline identity. `parse_config_with_vars`
    // stamped `source_hash` from the pre-patch source bytes, so without this a
    // channel-patched run would share the base pipeline's identity — colliding
    // `{pipeline_hash}` output paths and conflating lineage across the base and
    // its patched variants. An empty patch map contributes nothing, leaving the
    // hash byte-identical to a plain load; a non-empty map mixes a canonical
    // serialization of the patches in, so base-vs-patched and two-different-patch
    // runs get distinct hashes.
    if !patches.is_empty() {
        let serialized = serde_json::to_vec(patches).map_err(|e| {
            ConfigError::Validation(format!("internal: channel patch identity hashing: {e}"))
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&config.source_hash);
        hasher.update(&serialized);
        config.source_hash = *hasher.finalize().as_bytes();
    }
    validate_config(&config)?;
    Ok(config)
}

/// Load and parse a pipeline config from a YAML file path.
pub fn load_config(path: &std::path::Path) -> Result<PipelineConfig, ConfigError> {
    load_config_with_vars(path, &[])
}

#[cfg(test)]
mod external_schema_tests {
    use super::*;

    fn source_schema(config: &PipelineConfig) -> &crate::config::SourceSchema {
        config
            .nodes
            .iter()
            .find_map(|n| match &n.value {
                crate::config::PipelineNode::Source { config, .. } => Some(&config.schema),
                _ => None,
            })
            .expect("source node present")
    }

    fn write_pipeline_referencing(
        dir: &std::path::Path,
        schema_path: &std::path::Path,
    ) -> std::path::PathBuf {
        let pipeline = format!(
            "pipeline:\n  name: ext_schema_test\n\
             nodes:\n  - type: source\n    name: src\n    config:\n      \
             name: src\n      type: csv\n      path: /tmp/in.csv\n      \
             schema: \"{}\"\n  - type: output\n    name: out\n    input: src\n    \
             config:\n      name: out\n      type: csv\n      path: /tmp/out.csv\n",
            schema_path.display()
        );
        let p = dir.join("pipeline.yaml");
        std::fs::write(&p, pipeline).unwrap();
        p
    }

    /// A source's external `.schema.yaml` (`SourceSchema::File`) is resolved to
    /// its inline column list at config-load time, and editing that file
    /// changes `source_hash` even though the pipeline YAML text is unchanged.
    #[test]
    fn external_schema_file_resolves_inline_and_folds_hash() {
        let dir = tempfile::tempdir().unwrap();
        let schema_path = dir.path().join("cols.schema.yaml");
        std::fs::write(
            &schema_path,
            "- { name: amount, type: int }\n- { name: name, type: string }\n",
        )
        .unwrap();
        let pipeline_path = write_pipeline_referencing(dir.path(), &schema_path);

        // Resolution: the `File` reference is replaced by inline columns.
        let config = parse_config_with_vars(&pipeline_path, &[]).unwrap();
        let cols = source_schema(&config)
            .as_columns()
            .expect("external schema resolved to an inline column list");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "amount");
        assert_eq!(cols[0].ty, cxl::typecheck::Type::Int);
        let hash_v1 = config.source_hash;

        // Hash fold: the pipeline YAML is byte-identical, but the referenced
        // schema content changed, so the pipeline identity must change too.
        std::fs::write(
            &schema_path,
            "- { name: amount, type: float }\n- { name: name, type: string }\n",
        )
        .unwrap();
        let config2 = parse_config_with_vars(&pipeline_path, &[]).unwrap();
        assert_ne!(
            config2.source_hash, hash_v1,
            "editing the external schema file must change source_hash"
        );
    }

    /// With no external schema, the fold contributes nothing: `source_hash`
    /// stays exactly the BLAKE3 of the interpolated YAML text.
    #[test]
    fn inline_schema_leaves_hash_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = "pipeline:\n  name: inline\n\
             nodes:\n  - type: source\n    name: src\n    config:\n      \
             name: src\n      type: csv\n      path: /tmp/in.csv\n      \
             schema:\n        - { name: amount, type: int }\n  - type: output\n    \
             name: out\n    input: src\n    config:\n      name: out\n      \
             type: csv\n      path: /tmp/out.csv\n";
        let p = dir.path().join("pipeline.yaml");
        std::fs::write(&p, pipeline).unwrap();
        let config = parse_config_with_vars(&p, &[]).unwrap();
        let interpolated = interpolate_env_vars(pipeline, &[]).unwrap();
        assert_eq!(
            config.source_hash,
            *blake3::hash(interpolated.as_bytes()).as_bytes(),
            "an inline-schema config hashes to exactly the YAML-text BLAKE3"
        );
    }
}
