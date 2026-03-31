use std::collections::HashMap;
use std::path::Path;

use crate::config::{ConfigError, LogDirective, LogLevel, LogTiming};

/// A named log rule from an external YAML file.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRule {
    pub level: Option<LogLevel>,
    pub when: Option<LogTiming>,
    pub condition: Option<String>,
    pub message: Option<String>,
    pub fields: Option<Vec<String>>,
    pub every: Option<u64>,
}

/// Collection of named log rules loaded from a YAML file.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LogRulesFile {
    pub rules: HashMap<String, LogRule>,
}

/// Load external log rules from a YAML file.
pub fn load_log_rules(path: &Path) -> Result<LogRulesFile, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
    let rules: LogRulesFile = serde_saphyr::from_str(&content)?;
    Ok(rules)
}

/// Merge a transform-level log directive with an external rule.
/// Transform-level keys override rule defaults.
pub fn merge_with_rule(directive: &LogDirective, rule: &LogRule) -> LogDirective {
    LogDirective {
        level: directive.level, // directive always has level (required field)
        when: directive.when,   // directive always has when (required field)
        condition: directive.condition.clone().or_else(|| rule.condition.clone()),
        message: directive.message.clone(), // directive always has message (required field)
        fields: directive.fields.clone().or_else(|| rule.fields.clone()),
        every: directive.every.or(rule.every),
        log_rule: None, // resolved — no longer references rule
    }
}

/// Resolve all log_rule references in a set of directives.
/// Returns an error if a referenced rule doesn't exist.
pub fn resolve_log_rules(
    directives: &[LogDirective],
    rules: &HashMap<String, LogRule>,
    transform_name: &str,
) -> Result<Vec<LogDirective>, ConfigError> {
    directives.iter().enumerate().map(|(i, d)| {
        if let Some(ref rule_name) = d.log_rule {
            let rule = rules.get(rule_name).ok_or_else(|| {
                ConfigError::Validation(format!(
                    "transform '{}': log directive #{}: references nonexistent rule '{}'",
                    transform_name, i + 1, rule_name,
                ))
            })?;
            Ok(merge_with_rule(d, rule))
        } else {
            Ok(d.clone())
        }
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_directive(level: LogLevel, when: LogTiming, message: &str) -> LogDirective {
        LogDirective {
            level,
            when,
            condition: None,
            message: message.to_string(),
            fields: None,
            every: None,
            log_rule: None,
        }
    }

    fn make_rule() -> LogRule {
        LogRule {
            level: Some(LogLevel::Info),
            when: Some(LogTiming::PerRecord),
            condition: Some("Amount > 0".to_string()),
            message: Some("default message".to_string()),
            fields: Some(vec!["name".to_string()]),
            every: Some(10),
        }
    }

    #[test]
    fn test_log_level2_external_rules_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rules.yaml");
        std::fs::write(&path, r#"
rules:
  audit_trail:
    level: info
    when: per_record
    condition: "Amount > 1000"
    message: "audit: {Name}"
    fields: [Name, Amount]
"#).unwrap();
        let loaded = load_log_rules(&path).unwrap();
        assert!(loaded.rules.contains_key("audit_trail"));
        let rule = &loaded.rules["audit_trail"];
        assert_eq!(rule.level, Some(LogLevel::Info));
    }

    #[test]
    fn test_log_level2_rule_reference() {
        let rule = make_rule();
        let mut rules = HashMap::new();
        rules.insert("audit".to_string(), rule);

        let directives = vec![LogDirective {
            level: LogLevel::Info,
            when: LogTiming::PerRecord,
            condition: None,
            message: "processed {Name}".to_string(),
            fields: None,
            every: None,
            log_rule: Some("audit".to_string()),
        }];

        let resolved = resolve_log_rules(&directives, &rules, "t1").unwrap();
        assert_eq!(resolved.len(), 1);
        // Condition inherited from rule
        assert_eq!(resolved[0].condition.as_deref(), Some("Amount > 0"));
        // Fields inherited from rule
        assert!(resolved[0].fields.is_some());
        // Message from directive (not rule)
        assert_eq!(resolved[0].message, "processed {Name}");
    }

    #[test]
    fn test_log_level2_override_merge() {
        let rule = make_rule();
        let mut rules = HashMap::new();
        rules.insert("audit".to_string(), rule);

        let directives = vec![LogDirective {
            level: LogLevel::Warn, // override rule's Info
            when: LogTiming::PerRecord,
            condition: Some("Status == \"FAIL\"".to_string()), // override rule condition
            message: "alert".to_string(),
            fields: Some(vec!["status".to_string()]), // override rule fields
            every: Some(1), // override rule every
            log_rule: Some("audit".to_string()),
        }];

        let resolved = resolve_log_rules(&directives, &rules, "t1").unwrap();
        assert_eq!(resolved[0].level, LogLevel::Warn);
        assert_eq!(resolved[0].condition.as_deref(), Some("Status == \"FAIL\""));
        assert_eq!(resolved[0].every, Some(1));
    }

    #[test]
    fn test_log_level2_missing_rule_error() {
        let rules = HashMap::new();
        let directives = vec![LogDirective {
            level: LogLevel::Info,
            when: LogTiming::PerRecord,
            condition: None,
            message: "msg".to_string(),
            fields: None,
            every: None,
            log_rule: Some("nonexistent".to_string()),
        }];

        let err = resolve_log_rules(&directives, &rules, "t1").unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn test_log_level2_full_override() {
        let rule = make_rule();
        let mut rules = HashMap::new();
        rules.insert("r".to_string(), rule);

        // All fields overridden at directive level
        let directives = vec![LogDirective {
            level: LogLevel::Error,
            when: LogTiming::OnError,
            condition: Some("true".to_string()),
            message: "override".to_string(),
            fields: Some(vec!["x".to_string()]),
            every: None, // not valid with on_error, but merge doesn't validate
            log_rule: Some("r".to_string()),
        }];

        let resolved = resolve_log_rules(&directives, &rules, "t1").unwrap();
        assert_eq!(resolved[0].level, LogLevel::Error);
        assert_eq!(resolved[0].message, "override");
        assert_eq!(resolved[0].condition.as_deref(), Some("true"));
    }
}
