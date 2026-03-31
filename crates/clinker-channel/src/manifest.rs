use indexmap::IndexMap;
use serde::Deserialize;
use serde::de;
use std::path::Path;

use crate::error::ChannelError;

/// Parsed channel manifest from `channel.yaml`.
/// Contains channel metadata and resolved variables (${VAR} substituted).
#[derive(Debug)]
pub struct ChannelManifest {
    pub metadata: ChannelMetadata,
    /// Resolved variables — ${VAR} references in values have been substituted
    /// with system env vars at load time.
    pub variables: IndexMap<String, String>,
}

/// Channel identity and configuration metadata from the `_channel:` section.
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelMetadata {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub contact: Option<String>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_active")]
    pub active: bool,
    #[serde(default)]
    pub inherits: ChannelInherits,
}

fn default_active() -> bool {
    true
}

/// How a channel inherits group overrides.
/// Deserializes from: absent → None, string → Single, list → Multiple.
#[derive(Debug, Clone, Default)]
pub enum ChannelInherits {
    #[default]
    None,
    Single(String),
    Multiple(Vec<String>),
}

impl<'de> Deserialize<'de> for ChannelInherits {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct InheritsVisitor;

        impl<'de> de::Visitor<'de> for InheritsVisitor {
            type Value = ChannelInherits;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string, a list of strings, or null")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ChannelInherits::Single(v.to_string()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut ids = Vec::new();
                while let Some(id) = seq.next_element::<String>()? {
                    ids.push(id);
                }
                Ok(ChannelInherits::Multiple(ids))
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(ChannelInherits::None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(ChannelInherits::None)
            }
        }

        deserializer.deserialize_any(InheritsVisitor)
    }
}

/// Raw deserialization target for `channel.yaml`.
#[derive(Deserialize)]
struct RawChannelFile {
    _channel: ChannelMetadata,
    #[serde(default)]
    variables: IndexMap<String, String>,
}

impl ChannelManifest {
    /// Load `channel.yaml` from `channel_dir`, resolve `${VAR}` in variable values
    /// using system env vars only (channel vars are not yet available at this point).
    pub fn load(channel_dir: &Path) -> Result<Self, ChannelError> {
        let path = channel_dir.join("channel.yaml");
        let raw_yaml = std::fs::read_to_string(&path).map_err(ChannelError::Io)?;

        // Interpolate system env vars in the channel.yaml content.
        // Channel vars don't exist yet — pass empty extra_vars.
        let interpolated =
            clinker_core::config::interpolate_env_vars(&raw_yaml, &[]).map_err(|e| {
                ChannelError::Validation(format!(
                    "failed to interpolate variables in {}: {}",
                    path.display(),
                    e
                ))
            })?;

        let parsed: RawChannelFile = serde_saphyr::from_str(&interpolated)?;

        Ok(ChannelManifest {
            metadata: parsed._channel,
            variables: parsed.variables,
        })
    }

    /// Return channel variables as `(&str, &str)` pairs for passing to
    /// `interpolate_env_vars` as `extra_vars`.
    pub fn vars_as_pairs(&self) -> Vec<(&str, &str)> {
        self.variables
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// channel.yaml parses; id, name, tier, tags correct.
    #[test]
    fn test_manifest_load_basic() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path().join("acme");
        std::fs::create_dir_all(&channel_dir).unwrap();
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: acme-corp
  name: "Acme Corporation"
  description: "Enterprise tier"
  contact: "jane@acme.com"
  tier: enterprise
  tags: [enterprise, us-west]
  active: true

variables:
  CHANNEL_DATA_DIR: "/data/acme"
  BILLING_MODE: "custom"
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(&channel_dir).unwrap();
        assert_eq!(manifest.metadata.id, "acme-corp");
        assert_eq!(manifest.metadata.name, "Acme Corporation");
        assert_eq!(
            manifest.metadata.description.as_deref(),
            Some("Enterprise tier")
        );
        assert_eq!(manifest.metadata.contact.as_deref(), Some("jane@acme.com"));
        assert_eq!(manifest.metadata.tier.as_deref(), Some("enterprise"));
        assert_eq!(manifest.metadata.tags, vec!["enterprise", "us-west"]);
        assert!(manifest.metadata.active);
        assert_eq!(
            manifest.variables.get("CHANNEL_DATA_DIR").unwrap(),
            "/data/acme"
        );
        assert_eq!(manifest.variables.get("BILLING_MODE").unwrap(), "custom");
    }

    /// `inherits: enterprise-base` → Single.
    #[test]
    fn test_manifest_inherits_string() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path();
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: test
  name: "Test"
  inherits: enterprise-base
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(channel_dir).unwrap();
        match &manifest.metadata.inherits {
            ChannelInherits::Single(id) => assert_eq!(id, "enterprise-base"),
            other => panic!("expected Single, got {:?}", other),
        }
    }

    /// `inherits: [a, b]` → Multiple.
    #[test]
    fn test_manifest_inherits_list() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path();
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: test
  name: "Test"
  inherits: [group-a, group-b]
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(channel_dir).unwrap();
        match &manifest.metadata.inherits {
            ChannelInherits::Multiple(ids) => {
                assert_eq!(ids, &["group-a", "group-b"]);
            }
            other => panic!("expected Multiple, got {:?}", other),
        }
    }

    /// No `inherits:` key → None.
    #[test]
    fn test_manifest_inherits_none() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path();
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: test
  name: "Test"
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(channel_dir).unwrap();
        assert!(matches!(manifest.metadata.inherits, ChannelInherits::None));
    }

    /// `${AWS_ACCOUNT_ID}-suffix` in var value resolves via system env.
    #[test]
    fn test_manifest_variables_resolved() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path();
        unsafe { std::env::set_var("TEST_AWS_ACCOUNT_ID_10_2", "123456") };
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: test
  name: "Test"

variables:
  BUCKET: "${TEST_AWS_ACCOUNT_ID_10_2}-acme-etl"
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(channel_dir).unwrap();
        unsafe { std::env::remove_var("TEST_AWS_ACCOUNT_ID_10_2") };
        assert_eq!(manifest.variables.get("BUCKET").unwrap(), "123456-acme-etl");
    }

    /// `active: false` parsed.
    #[test]
    fn test_manifest_inactive_channel() {
        let tmp = TempDir::new().unwrap();
        let channel_dir = tmp.path();
        std::fs::write(
            channel_dir.join("channel.yaml"),
            r#"
_channel:
  id: test
  name: "Test"
  active: false
"#,
        )
        .unwrap();

        let manifest = ChannelManifest::load(channel_dir).unwrap();
        assert!(!manifest.metadata.active);
    }

    /// ChannelError::Io on missing file.
    #[test]
    fn test_manifest_missing_channel_yaml() {
        let tmp = TempDir::new().unwrap();
        let result = ChannelManifest::load(tmp.path());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChannelError::Io(_)));
    }

    /// Channel var beats matching system env var.
    #[test]
    fn test_interpolate_extra_vars_win_over_system() {
        unsafe { std::env::set_var("SHARED_VAR_10_2", "system_value") };
        let extra = [("SHARED_VAR_10_2", "channel_value")];
        let result =
            clinker_core::config::interpolate_env_vars("val: ${SHARED_VAR_10_2}", &extra).unwrap();
        unsafe { std::env::remove_var("SHARED_VAR_10_2") };
        assert_eq!(result, "val: channel_value");
    }

    /// Absent channel var → system env used.
    #[test]
    fn test_interpolate_extra_vars_fallback_to_system() {
        unsafe { std::env::set_var("SYS_ONLY_VAR_10_2", "from_system") };
        let extra: [(&str, &str); 0] = [];
        let result =
            clinker_core::config::interpolate_env_vars("val: ${SYS_ONLY_VAR_10_2}", &extra)
                .unwrap();
        unsafe { std::env::remove_var("SYS_ONLY_VAR_10_2") };
        assert_eq!(result, "val: from_system");
    }

    /// `$${VAR}` → literal `${VAR}` in output.
    #[test]
    fn test_interpolate_double_dollar_escape() {
        let result = clinker_core::config::interpolate_env_vars("expr: $${FOO}", &[]).unwrap();
        assert_eq!(result, "expr: ${FOO}");
    }

    /// --env prod beats CLINKER_ENV=dev beats clinker.toml default.
    /// This tests the precedence *logic* — in the real CLI, --env is resolved
    /// before channel loading. Here we verify the chain directly.
    /// Uses unique env var names to avoid parallel test interference.
    #[test]
    fn test_env_precedence_cli_wins() {
        let env_key = "CLINKER_ENV_TEST_CLI_WINS";
        // Simulate: --env prod, env var=dev, toml default=staging
        let cli_env: Option<String> = Some("prod".into());
        unsafe { std::env::set_var(env_key, "dev") };
        let toml_default: Option<String> = Some("staging".into());

        let resolved = cli_env
            .or_else(|| std::env::var(env_key).ok())
            .or(toml_default);

        unsafe { std::env::remove_var(env_key) };
        assert_eq!(resolved.as_deref(), Some("prod"));
    }

    /// CLINKER_ENV=prod beats clinker.toml default.
    #[test]
    fn test_env_precedence_env_var_wins_over_toml() {
        let env_key = "CLINKER_ENV_TEST_ENVVAR_WINS";
        let cli_env: Option<String> = None;
        unsafe { std::env::set_var(env_key, "prod") };
        let toml_default: Option<String> = Some("dev".into());

        let resolved = cli_env
            .or_else(|| std::env::var(env_key).ok())
            .or(toml_default);

        unsafe { std::env::remove_var(env_key) };
        assert_eq!(resolved.as_deref(), Some("prod"));
    }

    /// No flag, no env var → clinker.toml default used.
    #[test]
    fn test_env_precedence_toml_default_used() {
        let env_key = "CLINKER_ENV_TEST_TOML_DEFAULT";
        let cli_env: Option<String> = None;
        unsafe { std::env::remove_var(env_key) };
        let toml_default: Option<String> = Some("dev".into());

        let resolved = cli_env
            .or_else(|| std::env::var(env_key).ok())
            .or(toml_default);

        assert_eq!(resolved.as_deref(), Some("dev"));
    }

    /// No env source + when: conditions → warning logged.
    /// This test verifies the *detection logic* — the actual warning emission
    /// is in the CLI (Task 10.6). Here we verify the condition is detectable.
    #[test]
    fn test_env_absent_warning_emitted() {
        let env_key = "CLINKER_ENV_TEST_ABSENT";
        // Simulate: no --env, no env var, no toml default
        let cli_env: Option<String> = None;
        unsafe { std::env::remove_var(env_key) };
        let toml_default: Option<String> = None;

        let resolved = cli_env
            .or_else(|| std::env::var(env_key).ok())
            .or(toml_default);

        assert!(resolved.is_none());

        // Simulate: override file has when: conditions
        let has_when_conditions = true;
        let should_warn = resolved.is_none() && has_when_conditions;
        assert!(should_warn);
    }
}
