//! Output path templating with conditional emit.
//!
//! Templates appear in `OutputConfig.path` and use `{token}` braces to
//! interpolate plan-time values (source filename, channel, pipeline
//! hash, run timestamp, ids) plus the runtime collision counter `{n}`.
//! Literal braces escape as `{{` and `}}`.
//!
//! Token body shapes (split on `:`):
//!
//! - `{name}` — bare token.
//! - `{name:arg}` — argument form. Only `pipeline_hash` and
//!   `source_name` accept arguments today.
//! - `{name:prefix:suffix}` — conditional emit. Renders
//!   `prefix + value + suffix` when the value is non-empty, otherwise
//!   nothing. Inspired by bash `${VAR:+text}` collapsed to a positional
//!   form so missing values do not produce broken filenames like
//!   `report-.csv`.
//! - `{name:arg:prefix:suffix}` — argument plus conditional emit.
//!
//! Five-or-more colons in a token body are a parse error.
//!
//! Tokens supported:
//!
//! - `source_name` — DAG-traced ancestor source filename stem;
//!   `{source_name:NODE}` to disambiguate when multiple sources fan in.
//! - `channel` — active channel name; empty when no channel selected.
//! - `pipeline_hash` — 8 hex chars by default. `:full` for the full 64
//!   hex chars; `:N` for the first `N` chars (1 ≤ N ≤ 64).
//! - `timestamp` — run start, filesystem-safe ISO-8601.
//! - `execution_id`, `batch_id` — from `PipelineRunParams`.
//! - `n` — collision counter for `if_exists = unique_suffix`. Empty
//!   when no collision occurred or policy is not `unique_suffix`.

use std::collections::HashMap;

use crate::config::ConfigError;

/// Parsed path template; immutable after [`PathTemplate::parse`].
#[derive(Debug, Clone)]
pub struct PathTemplate {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone)]
enum Segment {
    Literal(String),
    Token(TokenSpec),
}

#[derive(Debug, Clone)]
struct TokenSpec {
    name: String,
    arg: Option<String>,
    conditional: Option<(String, String)>,
}

/// Runtime values bound to each render call.
///
/// `source_name_by_node` is the per-Source-node lookup used to resolve
/// the explicit-disambiguation form `{source_name:NODE}`. The
/// unqualified `{source_name}` resolves to `source_name_default`.
#[derive(Debug, Default)]
pub struct TemplateContext<'a> {
    pub source_name_default: Option<&'a str>,
    pub source_name_by_node: HashMap<String, String>,
    pub channel: Option<&'a str>,
    pub pipeline_hash: [u8; 32],
    pub timestamp: Option<&'a str>,
    pub execution_id: Option<&'a str>,
    pub batch_id: Option<&'a str>,
    /// Current collision counter when `if_exists = unique_suffix` and
    /// the bare path is taken. `None` means the bare path is free or
    /// the policy is not `unique_suffix`.
    pub n: Option<u64>,
    /// Zero-pad width for the `{n}` token. `0` = no padding.
    pub unique_suffix_width: u8,
}

impl PathTemplate {
    /// Parse a template string into a sequence of literals and tokens.
    pub fn parse(input: &str) -> Result<Self, ConfigError> {
        let mut segments: Vec<Segment> = Vec::new();
        let mut buf = String::new();
        let mut chars = input.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '{' => {
                    if chars.peek() == Some(&'{') {
                        chars.next();
                        buf.push('{');
                        continue;
                    }
                    if !buf.is_empty() {
                        segments.push(Segment::Literal(std::mem::take(&mut buf)));
                    }
                    let mut body = String::new();
                    let mut closed = false;
                    while let Some(bc) = chars.next() {
                        if bc == '}' {
                            closed = true;
                            break;
                        }
                        body.push(bc);
                    }
                    if !closed {
                        return Err(ConfigError::Validation(format!(
                            "unterminated path template token in {input:?}"
                        )));
                    }
                    segments.push(Segment::Token(parse_token_body(&body, input)?));
                }
                '}' => {
                    if chars.peek() == Some(&'}') {
                        chars.next();
                        buf.push('}');
                    } else {
                        return Err(ConfigError::Validation(format!(
                            "unmatched `}}` in path template {input:?} (use `}}}}` for a literal brace)"
                        )));
                    }
                }
                _ => buf.push(c),
            }
        }
        if !buf.is_empty() {
            segments.push(Segment::Literal(buf));
        }
        Ok(PathTemplate { segments })
    }

    /// Render the template using `ctx`. Returns the resolved path string.
    pub fn render(&self, ctx: &TemplateContext<'_>) -> Result<String, ConfigError> {
        let mut out = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => out.push_str(s),
                Segment::Token(spec) => {
                    let value = resolve_token(spec, ctx)?;
                    if let Some((prefix, suffix)) = &spec.conditional {
                        if !value.is_empty() {
                            out.push_str(prefix);
                            out.push_str(&value);
                            out.push_str(suffix);
                        }
                    } else {
                        out.push_str(&value);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Whether the template body references the named token at least once.
    pub fn contains_token(&self, name: &str) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s, Segment::Token(t) if t.name == name))
    }

    /// Names of every distinct token referenced (for compile-time
    /// validation against the known token set).
    pub fn referenced_tokens(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .segments
            .iter()
            .filter_map(|s| match s {
                Segment::Token(t) => Some(t.name.as_str()),
                _ => None,
            })
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Source-node arg references in `{source_name:NODE}` form, in order
    /// of appearance. Used by compile-time validation to confirm each
    /// referenced node is a real Source ancestor of the output.
    pub fn source_name_args(&self) -> Vec<&str> {
        self.segments
            .iter()
            .filter_map(|s| match s {
                Segment::Token(t) if t.name == "source_name" => t.arg.as_deref(),
                _ => None,
            })
            .collect()
    }

    /// Whether the template has at least one `{source_name}` token
    /// without an explicit `:NODE` argument. Used to gate the
    /// "ambiguous source" diagnostic for outputs that fan in from
    /// multiple Source ancestors.
    pub fn has_unqualified_source_name(&self) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s, Segment::Token(t) if t.name == "source_name" && t.arg.is_none()))
    }
}

/// Render every Output node's `path:` template in place, with `n` set
/// to `None` so collision counters do not appear in the resolved base
/// path. Both the non-split file opener (in `clinker::main`) and the
/// split file factory (in `executor::build_format_writer`) consume the
/// rendered string downstream — pre-resolving here keeps tokens out of
/// the executor.
///
/// `{source_name}` resolution is per-output: ancestry traces backward
/// through node `input:` references, and the unqualified token requires
/// exactly one Source ancestor. Multi-source fan-in forces the user to
/// disambiguate via `{source_name:NODE}`, which is in turn validated
/// against the ancestor set.
///
/// `ctx.source_name_default` and `ctx.source_name_by_node` are ignored;
/// per-output ancestry supersedes them.
pub fn resolve_output_path_templates_in_place(
    config: &mut crate::config::PipelineConfig,
    ctx: &TemplateContext<'_>,
) -> Result<(), crate::config::ConfigError> {
    use crate::config::ConfigError;
    use crate::config::PipelineNode;

    let output_names: Vec<String> = config
        .nodes
        .iter()
        .filter_map(|s| match &s.value {
            PipelineNode::Output { config: body, .. } => Some(body.output.name.clone()),
            _ => None,
        })
        .collect();

    let ancestry: HashMap<String, Vec<SourceAncestor>> = output_names
        .iter()
        .map(|name| (name.clone(), source_ancestors_of(config, name)))
        .collect();

    for spanned in config.nodes.iter_mut() {
        let PipelineNode::Output { config: body, .. } = &mut spanned.value else {
            continue;
        };
        let output_name = body.output.name.clone();
        let template = PathTemplate::parse(&body.output.path)
            .map_err(|e| ConfigError::Validation(format!("output {output_name:?}: {e}")))?;

        let ancestors = ancestry.get(&output_name).cloned().unwrap_or_default();
        let mut by_node: HashMap<String, String> = HashMap::new();
        for a in &ancestors {
            by_node.insert(a.node_name.clone(), a.stem.clone());
        }
        let unique_default = if ancestors.len() == 1 {
            Some(ancestors[0].stem.clone())
        } else {
            None
        };

        if template.contains_token("source_name") {
            for arg in template.source_name_args() {
                if !by_node.contains_key(arg) {
                    let names: Vec<&str> = ancestors.iter().map(|a| a.node_name.as_str()).collect();
                    return Err(ConfigError::Validation(format!(
                        "output {output_name:?}: {{source_name:{arg}}} references {arg:?}, \
                         which is not a Source ancestor; ancestors are: [{}]",
                        names.join(", ")
                    )));
                }
            }
            let total_source_name_tokens = template
                .referenced_tokens()
                .iter()
                .filter(|t| **t == "source_name")
                .count();
            // referenced_tokens deduplicates; check for any unqualified token
            // by counting source_name appearances in segments directly.
            if template.has_unqualified_source_name() && unique_default.is_none() {
                let names: Vec<&str> = ancestors.iter().map(|a| a.node_name.as_str()).collect();
                return Err(ConfigError::Validation(format!(
                    "output {output_name:?}: {{source_name}} is ambiguous — output traces \
                     back to {} sources [{}]; use {{source_name:NODE}} to disambiguate",
                    ancestors.len(),
                    names.join(", "),
                )));
            }
            let _ = total_source_name_tokens;
        }

        let local_ctx = TemplateContext {
            source_name_default: unique_default.as_deref(),
            source_name_by_node: by_node,
            channel: ctx.channel,
            pipeline_hash: ctx.pipeline_hash,
            timestamp: ctx.timestamp,
            execution_id: ctx.execution_id,
            batch_id: ctx.batch_id,
            n: None,
            unique_suffix_width: 0,
        };
        let resolved = template
            .render(&local_ctx)
            .map_err(|e| ConfigError::Validation(format!("output {output_name:?}: {e}")))?;
        body.output.path = resolved;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SourceAncestor {
    node_name: String,
    stem: String,
}

/// BFS backward through node `input:` references from `output_name` to
/// every Source ancestor. Returns each ancestor's node name plus its
/// file-path stem.
fn source_ancestors_of(
    config: &crate::config::PipelineConfig,
    output_name: &str,
) -> Vec<SourceAncestor> {
    use crate::config::PipelineNode;
    use crate::config::node_header::NodeInput;
    use std::collections::{HashSet, VecDeque};

    let by_name: HashMap<&str, &PipelineNode> = config
        .nodes
        .iter()
        .map(|s| (s.value.name(), &s.value))
        .collect();

    let mut visited: HashSet<String> = HashSet::new();
    let mut sources: Vec<SourceAncestor> = Vec::new();
    let mut queue: VecDeque<String> = VecDeque::from([output_name.to_string()]);

    let target_name_of = |inp: &NodeInput| -> String {
        match inp {
            NodeInput::Single(s) => s.clone(),
            NodeInput::Port { node, .. } => node.clone(),
        }
    };

    while let Some(name) = queue.pop_front() {
        if !visited.insert(name.clone()) {
            continue;
        }
        let Some(node) = by_name.get(name.as_str()) else {
            continue;
        };
        match node {
            PipelineNode::Source { config: src, .. } => {
                let stem = std::path::Path::new(&src.source.path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                sources.push(SourceAncestor {
                    node_name: src.source.name.clone(),
                    stem,
                });
            }
            PipelineNode::Transform { header, .. }
            | PipelineNode::Aggregate { header, .. }
            | PipelineNode::Route { header, .. }
            | PipelineNode::Output { header, .. }
            | PipelineNode::Composition { header, .. } => {
                queue.push_back(target_name_of(&header.input.value));
            }
            PipelineNode::Merge { header, .. } => {
                for inp in &header.inputs {
                    queue.push_back(target_name_of(&inp.value));
                }
            }
            PipelineNode::Combine { header, .. } => {
                for inp in header.input.values() {
                    queue.push_back(target_name_of(&inp.value));
                }
            }
        }
    }
    sources
}

const KNOWN_TOKENS: &[&str] = &[
    "source_name",
    "channel",
    "pipeline_hash",
    "timestamp",
    "execution_id",
    "batch_id",
    "n",
];

fn parse_token_body(body: &str, full: &str) -> Result<TokenSpec, ConfigError> {
    let parts: Vec<&str> = body.split(':').collect();
    let (name, arg, conditional) = match parts.len() {
        1 => (parts[0].to_string(), None, None),
        2 => (parts[0].to_string(), Some(parts[1].to_string()), None),
        3 => (
            parts[0].to_string(),
            None,
            Some((parts[1].to_string(), parts[2].to_string())),
        ),
        4 => (
            parts[0].to_string(),
            Some(parts[1].to_string()),
            Some((parts[2].to_string(), parts[3].to_string())),
        ),
        _ => {
            return Err(ConfigError::Validation(format!(
                "path template token {{{body}}} in {full:?} has too many `:` separators (max 3)"
            )));
        }
    };
    if name.is_empty() {
        return Err(ConfigError::Validation(format!(
            "empty token name in path template {full:?}"
        )));
    }
    if !KNOWN_TOKENS.contains(&name.as_str()) {
        return Err(ConfigError::Validation(format!(
            "unknown path template token {{{name}}} in {full:?}; known: {}",
            KNOWN_TOKENS.join(", ")
        )));
    }
    if arg.is_some() && !matches!(name.as_str(), "source_name" | "pipeline_hash") {
        return Err(ConfigError::Validation(format!(
            "token {{{name}}} does not accept an argument (got `:{}`) in {full:?}",
            arg.as_deref().unwrap_or("")
        )));
    }
    if name == "pipeline_hash"
        && let Some(a) = arg.as_deref()
        && a != "full"
    {
        let n: usize = a.parse().map_err(|_| {
            ConfigError::Validation(format!(
                "{{pipeline_hash:{a}}} expects `full` or 1..=64; got {a:?}"
            ))
        })?;
        if !(1..=64).contains(&n) {
            return Err(ConfigError::Validation(format!(
                "{{pipeline_hash:{a}}} length must be 1..=64; got {n}"
            )));
        }
    }
    Ok(TokenSpec {
        name,
        arg,
        conditional,
    })
}

fn resolve_token(spec: &TokenSpec, ctx: &TemplateContext<'_>) -> Result<String, ConfigError> {
    match spec.name.as_str() {
        "source_name" => match spec.arg.as_deref() {
            Some(node) => Ok(ctx
                .source_name_by_node
                .get(node)
                .cloned()
                .unwrap_or_default()),
            None => Ok(ctx.source_name_default.unwrap_or("").to_string()),
        },
        "channel" => Ok(ctx.channel.unwrap_or("").to_string()),
        "pipeline_hash" => {
            let hex = hex_lower(&ctx.pipeline_hash);
            let n = match spec.arg.as_deref() {
                Some("full") => 64,
                Some(s) => s.parse::<usize>().unwrap_or(8),
                None => 8,
            };
            Ok(hex[..n.min(hex.len())].to_string())
        }
        "timestamp" => Ok(ctx.timestamp.unwrap_or("").to_string()),
        "execution_id" => Ok(ctx.execution_id.unwrap_or("").to_string()),
        "batch_id" => Ok(ctx.batch_id.unwrap_or("").to_string()),
        "n" => Ok(match ctx.n {
            None => String::new(),
            Some(v) if ctx.unique_suffix_width == 0 => v.to_string(),
            Some(v) => format!("{:0>width$}", v, width = ctx.unique_suffix_width as usize),
        }),
        other => Err(ConfigError::Validation(format!(
            "internal: token {{{other}}} reached resolver but is not implemented"
        ))),
    }
}

fn hex_lower(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with_hash(hash: [u8; 32]) -> TemplateContext<'static> {
        TemplateContext {
            pipeline_hash: hash,
            ..Default::default()
        }
    }

    #[test]
    fn literal_only_renders_unchanged() {
        let t = PathTemplate::parse("plain.csv").unwrap();
        assert_eq!(t.render(&TemplateContext::default()).unwrap(), "plain.csv");
    }

    #[test]
    fn escaped_braces() {
        let t = PathTemplate::parse("a{{b}}c.csv").unwrap();
        assert_eq!(t.render(&TemplateContext::default()).unwrap(), "a{b}c.csv");
    }

    #[test]
    fn channel_token_with_value() {
        let t = PathTemplate::parse("out-{channel}.csv").unwrap();
        let ctx = TemplateContext {
            channel: Some("acme"),
            ..Default::default()
        };
        assert_eq!(t.render(&ctx).unwrap(), "out-acme.csv");
    }

    #[test]
    fn channel_token_empty_breaks_filename() {
        let t = PathTemplate::parse("out-{channel}.csv").unwrap();
        assert_eq!(
            t.render(&TemplateContext::default()).unwrap(),
            "out-.csv",
            "demonstrates the broken-filename pitfall conditional emit fixes"
        );
    }

    #[test]
    fn channel_conditional_emit_when_present() {
        let t = PathTemplate::parse("out{channel:-:}.csv").unwrap();
        let ctx = TemplateContext {
            channel: Some("acme"),
            ..Default::default()
        };
        assert_eq!(t.render(&ctx).unwrap(), "out-acme.csv");
    }

    #[test]
    fn channel_conditional_emit_when_absent() {
        let t = PathTemplate::parse("out{channel:-:}.csv").unwrap();
        assert_eq!(t.render(&TemplateContext::default()).unwrap(), "out.csv");
    }

    #[test]
    fn pipeline_hash_default_is_8_hex() {
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        hash[1] = 0xcd;
        hash[2] = 0xef;
        hash[3] = 0x12;
        let t = PathTemplate::parse("h-{pipeline_hash}.csv").unwrap();
        assert_eq!(t.render(&ctx_with_hash(hash)).unwrap(), "h-abcdef12.csv");
    }

    #[test]
    fn pipeline_hash_full_is_64_hex() {
        let hash = [0xaa; 32];
        let t = PathTemplate::parse("h-{pipeline_hash:full}.csv").unwrap();
        let rendered = t.render(&ctx_with_hash(hash)).unwrap();
        assert_eq!(rendered.len(), "h-".len() + 64 + ".csv".len());
        assert!(rendered.starts_with("h-aa"));
    }

    #[test]
    fn pipeline_hash_arg_n_takes_first_n() {
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        hash[1] = 0xcd;
        let t = PathTemplate::parse("h-{pipeline_hash:4}.csv").unwrap();
        assert_eq!(t.render(&ctx_with_hash(hash)).unwrap(), "h-abcd.csv");
    }

    #[test]
    fn pipeline_hash_arg_rejects_zero_and_overflow() {
        assert!(PathTemplate::parse("{pipeline_hash:0}").is_err());
        assert!(PathTemplate::parse("{pipeline_hash:65}").is_err());
        assert!(PathTemplate::parse("{pipeline_hash:abc}").is_err());
    }

    #[test]
    fn n_token_padded_with_width() {
        let t = PathTemplate::parse("file-{n}.csv").unwrap();
        let ctx = TemplateContext {
            n: Some(3),
            unique_suffix_width: 4,
            ..Default::default()
        };
        assert_eq!(t.render(&ctx).unwrap(), "file-0003.csv");
    }

    #[test]
    fn n_token_empty_when_no_collision() {
        let t = PathTemplate::parse("file{n:-:}.csv").unwrap();
        assert_eq!(t.render(&TemplateContext::default()).unwrap(), "file.csv");
    }

    #[test]
    fn source_name_explicit_node() {
        let mut by_node: HashMap<String, String> = HashMap::new();
        by_node.insert("primary".into(), "customers".into());
        let t = PathTemplate::parse("{source_name:primary}-out.csv").unwrap();
        let ctx = TemplateContext {
            source_name_by_node: by_node,
            ..Default::default()
        };
        assert_eq!(t.render(&ctx).unwrap(), "customers-out.csv");
    }

    #[test]
    fn unknown_token_rejected_at_parse_time() {
        let err = PathTemplate::parse("{nonsense}.csv").unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation(ref m) if m.contains("unknown path template token"))
        );
    }

    #[test]
    fn channel_with_arg_is_rejected() {
        let err = PathTemplate::parse("{channel:foo}.csv").unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation(ref m) if m.contains("does not accept an argument"))
        );
    }

    #[test]
    fn unterminated_token_errors() {
        let err = PathTemplate::parse("file-{channel.csv").unwrap_err();
        assert!(matches!(err, ConfigError::Validation(ref m) if m.contains("unterminated")));
    }

    #[test]
    fn referenced_tokens_lists_used_names() {
        let t = PathTemplate::parse("{source_name}-{channel}-{pipeline_hash}.csv").unwrap();
        assert_eq!(
            t.referenced_tokens(),
            vec!["channel", "pipeline_hash", "source_name"]
        );
    }

    #[test]
    fn source_name_args_collected() {
        let t = PathTemplate::parse("{source_name:left}-{source_name:right}.csv").unwrap();
        assert_eq!(t.source_name_args(), vec!["left", "right"]);
    }

    #[test]
    fn has_unqualified_source_name_detects_bare_token() {
        let t = PathTemplate::parse("{source_name}.csv").unwrap();
        assert!(t.has_unqualified_source_name());
        let t = PathTemplate::parse("{source_name:foo}.csv").unwrap();
        assert!(!t.has_unqualified_source_name());
        let t = PathTemplate::parse("{source_name}-{source_name:foo}.csv").unwrap();
        assert!(t.has_unqualified_source_name());
    }

    #[test]
    fn arg_plus_conditional() {
        let t = PathTemplate::parse("{pipeline_hash:4:[:]}").unwrap();
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        hash[1] = 0xcd;
        let rendered = t.render(&ctx_with_hash(hash)).unwrap();
        assert_eq!(rendered, "[abcd]");
    }
}
