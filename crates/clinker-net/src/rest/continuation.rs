//! Normalized REST continuation and redirect admission.
//!
//! This module is the single trust boundary for every server-directed follow-up
//! request. It resolves RFC 8288 link targets and redirect locations against
//! the effective response URL, normalizes the destination origin, and rejects
//! foreign or downgraded targets before the caller can construct a request.

use std::fmt;

use clinker_core_types::{FailureCategory, FailureClassification, RetryAdvice};
use ureq::http::Uri;

/// A normalized origin used to authorize every request in one REST pull.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Origin {
    scheme: String,
    host: String,
    effective_port: u16,
}

/// A URL that has passed syntax, scheme, origin, and downgrade checks.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AuthorizedUrl {
    uri: Uri,
    rendered: String,
}

impl AuthorizedUrl {
    pub(crate) fn as_str(&self) -> &str {
        &self.rendered
    }

    /// Render only the request coordinates safe for diagnostics. Query
    /// parameters are intentionally excluded because they commonly carry
    /// cursors, signatures, and vendor credentials.
    pub(crate) fn diagnostic_target(&self) -> String {
        format!("{}://{}{}", self.scheme(), self.authority(), self.path())
    }

    fn path(&self) -> &str {
        self.uri.path()
    }

    fn scheme(&self) -> &str {
        self.uri
            .scheme_str()
            .expect("authorized URLs always have a scheme")
    }

    fn authority(&self) -> &str {
        self.uri
            .authority()
            .expect("authorized URLs always have an authority")
            .as_str()
    }
}

/// A stable, sanitized continuation failure from the shared registry.
///
/// API classification: workspace-internal exposed API. The type is visible
/// only inside `clinker-net`; callers outside this crate receive the rendered
/// classification through the existing `FormatError` transport boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContinuationError {
    classification: FailureClassification,
}

impl ContinuationError {
    /// Build a registered REST failure for the workspace-internal exposed API.
    pub(crate) fn for_code(code: &'static str) -> Self {
        let classification = FailureClassification::for_code(code)
            .expect("REST continuation codes are registered in clinker-core-types");
        Self { classification }
    }
}

impl fmt::Display for ContinuationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category: FailureCategory = self.classification.category();
        let retry: RetryAdvice = self.classification.retry_advice();
        write!(
            formatter,
            "[{}] category={} retry={}: {}",
            self.classification.code(),
            category.as_str(),
            retry.as_str(),
            self.classification.message()
        )
    }
}

impl std::error::Error for ContinuationError {}

/// Parse and authorize the configured starting URL.
pub(crate) fn authorize_initial(raw: &str) -> Result<(Origin, AuthorizedUrl), ContinuationError> {
    let url = parse_absolute(raw, "rest.protocol.unresolvable_continuation")?;
    let origin = origin_of(&url)?;
    Ok((origin, url))
}

/// Resolve a reference against an effective response URL and authorize it.
pub(crate) fn resolve_and_authorize(
    base: &AuthorizedUrl,
    reference: &str,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, ContinuationError> {
    let reference = reference.trim();
    if reference.is_empty() || reference.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ContinuationError::for_code(
            "rest.protocol.unresolvable_continuation",
        ));
    }
    if reference.contains('#') {
        return Err(ContinuationError::for_code(
            "rest.protocol.unsupported_continuation",
        ));
    }

    let rendered = if reference.starts_with("//") {
        format!("{}:{reference}", base.scheme())
    } else if reference
        .parse::<Uri>()
        .ok()
        .and_then(|uri| uri.scheme().cloned())
        .is_some()
    {
        reference.to_owned()
    } else if reference.starts_with('?') {
        format!(
            "{}://{}{}{}",
            base.scheme(),
            base.authority(),
            base.path(),
            reference
        )
    } else if reference.starts_with('/') {
        format!("{}://{}{}", base.scheme(), base.authority(), reference)
    } else {
        let directory = base
            .path()
            .rsplit_once('/')
            .map_or("/", |(directory, _)| directory);
        let separator = if directory.ends_with('/') { "" } else { "/" };
        format!(
            "{}://{}{}{}{}",
            base.scheme(),
            base.authority(),
            directory,
            separator,
            reference
        )
    };

    let candidate = parse_absolute(&rendered, "rest.protocol.unresolvable_continuation")?;
    authorize_origin(candidate, admitted_origin)
}

/// Parse every Link header and resolve exactly one `rel=next` target.
pub(crate) fn next_link(
    headers: &ureq::http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<Option<AuthorizedUrl>, ContinuationError> {
    let mut targets = Vec::new();
    for value in headers.get_all(ureq::http::header::LINK) {
        let value = value
            .to_str()
            .map_err(|_| ContinuationError::for_code("rest.protocol.malformed_continuation"))?;
        targets.extend(parse_link_field(value)?);
    }
    match targets.as_slice() {
        [] => Ok(None),
        [target] => resolve_and_authorize(effective_url, target, admitted_origin).map(Some),
        _ => Err(ContinuationError::for_code(
            "rest.protocol.conflicting_continuation",
        )),
    }
}

/// Read one redirect Location value and authorize it.
pub(crate) fn redirect_location(
    headers: &ureq::http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, ContinuationError> {
    let mut locations = headers.get_all(ureq::http::header::LOCATION).iter();
    let location = locations
        .next()
        .ok_or_else(|| ContinuationError::for_code("rest.protocol.malformed_continuation"))?;
    if locations.next().is_some() {
        return Err(ContinuationError::for_code(
            "rest.protocol.conflicting_continuation",
        ));
    }
    let location = location
        .to_str()
        .map_err(|_| ContinuationError::for_code("rest.protocol.malformed_continuation"))?;
    resolve_and_authorize(effective_url, location, admitted_origin)
}

fn parse_absolute(
    raw: &str,
    failure_code: &'static str,
) -> Result<AuthorizedUrl, ContinuationError> {
    if raw.contains('#') || raw.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ContinuationError::for_code(failure_code));
    }
    let mut uri = raw
        .parse::<Uri>()
        .map_err(|_| ContinuationError::for_code(failure_code))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| ContinuationError::for_code(failure_code))?
        .to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") {
        return Err(ContinuationError::for_code(
            "rest.protocol.unsupported_continuation",
        ));
    }
    let authority = uri
        .authority()
        .ok_or_else(|| ContinuationError::for_code(failure_code))?;
    if authority.as_str().contains('@') || uri.host().is_none() {
        return Err(ContinuationError::for_code(failure_code));
    }
    let host = authority.host().to_ascii_lowercase();
    let port = authority.port_u16();
    let normalized_path = normalize_path(uri.path());
    let query = uri
        .query()
        .map_or(String::new(), |query| format!("?{query}"));
    let authority = port.map_or(host.clone(), |port| format!("{host}:{port}"));
    let normalized = format!("{scheme}://{authority}{normalized_path}{query}");
    uri = normalized
        .parse::<Uri>()
        .map_err(|_| ContinuationError::for_code(failure_code))?;
    let rendered = uri.to_string();
    Ok(AuthorizedUrl { uri, rendered })
}

fn origin_of(url: &AuthorizedUrl) -> Result<Origin, ContinuationError> {
    let scheme = url.scheme().to_ascii_lowercase();
    let host = url
        .uri
        .host()
        .ok_or_else(|| ContinuationError::for_code("rest.protocol.unresolvable_continuation"))?
        .to_ascii_lowercase();
    let effective_port = url.uri.port_u16().unwrap_or(match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => {
            return Err(ContinuationError::for_code(
                "rest.protocol.unsupported_continuation",
            ));
        }
    });
    Ok(Origin {
        scheme,
        host,
        effective_port,
    })
}

fn authorize_origin(
    candidate: AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, ContinuationError> {
    let candidate_origin = origin_of(&candidate)?;
    if admitted_origin.scheme == "https" && candidate_origin.scheme == "http" {
        return Err(ContinuationError::for_code("rest.security.https_downgrade"));
    }
    if &candidate_origin != admitted_origin {
        return Err(ContinuationError::for_code("rest.security.cross_origin"));
    }
    Ok(candidate)
}

fn normalize_path(path: &str) -> String {
    if !path.split('/').any(|segment| matches!(segment, "." | "..")) {
        return if path.is_empty() {
            "/".to_owned()
        } else {
            path.to_owned()
        };
    }
    let trailing_slash = path.ends_with('/');
    let mut segments = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }
    let mut normalized = format!("/{}", segments.join("/"));
    if trailing_slash && normalized != "/" {
        normalized.push('/');
    }
    normalized
}

fn parse_link_field(field: &str) -> Result<Vec<String>, ContinuationError> {
    let entries = split_link_values(field)?;
    let mut next_targets = Vec::new();
    for entry in entries {
        let entry = entry.trim();
        let Some(after_open) = entry.strip_prefix('<') else {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        };
        let Some(close) = after_open.find('>') else {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        };
        let target = &after_open[..close];
        if target.is_empty() {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        }
        let mut rel = None;
        let parameters = after_open[close + 1..].trim();
        if !parameters.is_empty() {
            if !parameters.starts_with(';') {
                return Err(ContinuationError::for_code(
                    "rest.protocol.malformed_continuation",
                ));
            }
            for parameter in parameters.split(';').skip(1) {
                let parameter = parameter.trim();
                if parameter.is_empty() {
                    return Err(ContinuationError::for_code(
                        "rest.protocol.malformed_continuation",
                    ));
                }
                let Some((name, value)) = parameter.split_once('=') else {
                    continue;
                };
                if name.trim().eq_ignore_ascii_case("rel") {
                    if rel.is_some() {
                        return Err(ContinuationError::for_code(
                            "rest.protocol.malformed_continuation",
                        ));
                    }
                    rel = Some(unquote(value.trim())?);
                }
            }
        }
        if rel.as_deref().is_some_and(|relations| {
            relations
                .split_ascii_whitespace()
                .any(|rel| rel.eq_ignore_ascii_case("next"))
        }) {
            next_targets.push(target.to_owned());
        }
    }
    Ok(next_targets)
}

fn split_link_values(field: &str) -> Result<Vec<&str>, ContinuationError> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut in_angle = false;
    let mut in_quote = false;
    let mut escaped = false;
    for (index, character) in field.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if in_quote => escaped = true,
            '"' if !in_angle => in_quote = !in_quote,
            '<' if !in_quote => {
                if in_angle {
                    return Err(ContinuationError::for_code(
                        "rest.protocol.malformed_continuation",
                    ));
                }
                in_angle = true;
            }
            '>' if !in_quote => {
                if !in_angle {
                    return Err(ContinuationError::for_code(
                        "rest.protocol.malformed_continuation",
                    ));
                }
                in_angle = false;
            }
            ',' if !in_angle && !in_quote => {
                entries.push(&field[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    if in_angle || in_quote || escaped {
        return Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
    }
    entries.push(&field[start..]);
    if entries.iter().any(|entry| entry.trim().is_empty()) {
        return Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
    }
    Ok(entries)
}

fn unquote(value: &str) -> Result<String, ContinuationError> {
    if let Some(quoted) = value.strip_prefix('"') {
        let Some(quoted) = quoted.strip_suffix('"') else {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        };
        let mut output = String::with_capacity(quoted.len());
        let mut escaped = false;
        for character in quoted.chars() {
            if escaped {
                output.push(character);
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else {
                output.push(character);
            }
        }
        if escaped {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        }
        Ok(output)
    } else if value.contains('"') {
        Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ))
    } else {
        Ok(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_default_port_is_part_of_normalized_origin() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let explicit = resolve_and_authorize(
            &base,
            "https://API.EXAMPLE.TEST:443/v1/items?page=2",
            &origin,
        )
        .expect("explicit default port is the same origin");
        assert_eq!(
            explicit.as_str(),
            "https://api.example.test:443/v1/items?page=2"
        );
    }

    #[test]
    fn https_to_http_is_classified_as_downgrade_before_origin_mismatch() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let error =
            resolve_and_authorize(&base, "http://api.example.test/v1/items?page=2", &origin)
                .expect_err("HTTPS downgrade must fail");
        assert_eq!(error.classification.code(), "rest.security.https_downgrade");
        assert_eq!(
            error.classification.category(),
            FailureCategory::SecurityPolicy
        );
        assert_eq!(error.classification.retry_advice(), RetryAdvice::DoNotRetry);
    }

    #[test]
    fn relative_dot_segments_are_normalized_before_admission() {
        let (origin, base) =
            authorize_initial("http://api.example.test/v1/items/page").expect("initial URL");
        let resolved =
            resolve_and_authorize(&base, "../next?page=2", &origin).expect("relative continuation");
        assert_eq!(resolved.as_str(), "http://api.example.test/v1/next?page=2");
    }

    #[test]
    fn relation_token_lists_select_exactly_one_next_target() {
        let (origin, base) =
            authorize_initial("http://api.example.test/v1/items").expect("initial URL");
        let mut headers = ureq::http::HeaderMap::new();
        headers.append(
            ureq::http::header::LINK,
            "</v1/items?page=1>; rel=prev"
                .parse()
                .expect("previous Link"),
        );
        headers.append(
            ureq::http::header::LINK,
            "</v1/items?page=2>; rel=\"alternate next\""
                .parse()
                .expect("next Link"),
        );
        let next = next_link(&headers, &base, &origin)
            .expect("valid Link metadata")
            .expect("next target");
        assert_eq!(next.as_str(), "http://api.example.test/v1/items?page=2");
    }
}
