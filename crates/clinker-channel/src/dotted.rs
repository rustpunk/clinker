//! `DottedPath` — validated `alias.param` config/resource keys.
//!
//! Channel-wide, group, and per-target `config:` maps key their clobber values
//! by dotted path: `alias.param_name` addresses a composition-node parameter,
//! `param_name` alone a pipeline-level parameter. The overlay clobber engine
//! ([`crate::overlay`]) and group derivation ([`crate::derivation`]) validate
//! their raw string keys into this type before resolving them against the
//! compiled plan.

use crate::error::ChannelError;

/// A validated dotted path into a composition's declared config or resource
/// schema. At most two segments: `alias.param_name` (for composition
/// targets) or `param_name` alone (for pipeline-level config).
///
/// Rejects: empty strings, leading/trailing dots, consecutive dots, more
/// than 2 segments, and characters outside `[a-zA-Z0-9_.]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DottedPath(String);

impl DottedPath {
    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split into (alias, param_name) if two segments, or (None, param_name)
    /// if one segment.
    pub fn segments(&self) -> (Option<&str>, &str) {
        match self.0.split_once('.') {
            Some((alias, param)) => (Some(alias), param),
            None => (None, &self.0),
        }
    }
}

impl TryFrom<&str> for DottedPath {
    type Error = ChannelError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "path is empty".to_string(),
            });
        }
        if s.starts_with('.') || s.ends_with('.') {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "path must not start or end with '.'".to_string(),
            });
        }
        if s.contains("..") {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "consecutive dots are not allowed".to_string(),
            });
        }
        if !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
        {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: "only [a-zA-Z0-9_.] characters are allowed".to_string(),
            });
        }
        let segment_count = s.split('.').count();
        if segment_count > 2 {
            return Err(ChannelError::InvalidDottedPath {
                path: s.to_string(),
                reason: format!("at most 2 segments allowed, got {segment_count}"),
            });
        }
        Ok(DottedPath(s.to_string()))
    }
}

impl std::fmt::Display for DottedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_segment_parses() {
        let p = DottedPath::try_from("threshold").expect("valid");
        assert_eq!(p.segments(), (None, "threshold"));
        assert_eq!(p.as_str(), "threshold");
    }

    #[test]
    fn two_segments_parse() {
        let p = DottedPath::try_from("fraud_check.threshold").expect("valid");
        assert_eq!(p.segments(), (Some("fraud_check"), "threshold"));
    }

    #[test]
    fn empty_rejected() {
        assert!(DottedPath::try_from("").is_err());
    }

    #[test]
    fn leading_or_trailing_dot_rejected() {
        assert!(DottedPath::try_from(".threshold").is_err());
        assert!(DottedPath::try_from("threshold.").is_err());
    }

    #[test]
    fn consecutive_dots_rejected() {
        assert!(DottedPath::try_from("a..b").is_err());
    }

    #[test]
    fn three_segments_rejected() {
        let err = DottedPath::try_from("a.b.c").expect_err("too many segments");
        assert!(err.to_string().contains("at most 2 segments"), "{err}");
    }

    #[test]
    fn illegal_characters_rejected() {
        assert!(DottedPath::try_from("a-b").is_err());
        assert!(DottedPath::try_from("a b").is_err());
        assert!(DottedPath::try_from("a/b").is_err());
    }
}
