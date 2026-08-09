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

    pub(crate) const fn classification_code(&self) -> &'static str {
        self.classification.code()
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
    match resolve_then_authorize(base, reference, admitted_origin) {
        Ok(url) => Ok(url),
        Err((_, error)) => Err(error),
    }
}

/// Resolve and authorize, keeping what resolution produced when authorization
/// then refuses it.
///
/// The caller comparing targets needs the resolved URL of a refused one, and
/// resolving a second time to recover it repeated the whole parse for a value
/// that was already in hand.
fn resolve_then_authorize(
    base: &AuthorizedUrl,
    reference: &str,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, (Option<String>, ContinuationError)> {
    let candidate = resolve(base, reference).map_err(|error| (None, error))?;
    let rendered = candidate.rendered.clone();
    authorize_origin(candidate, admitted_origin).map_err(|error| (Some(rendered), error))
}

/// Resolve a reference against the effective response URL, without judging
/// which origin it landed on. Syntax only; [`resolve_and_authorize`] adds the
/// origin and downgrade rules on top.
fn resolve(base: &AuthorizedUrl, reference: &str) -> Result<AuthorizedUrl, ContinuationError> {
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

    parse_absolute(&rendered, "rest.protocol.unresolvable_continuation")
}

/// Parse every Link header and resolve exactly one `rel=next` target.
///
/// The conflict this refuses is a reply naming two different next pages, which
/// no client can resolve. Naming the same page twice is not that: a reverse
/// proxy that merges or repeats response headers produces it routinely, and
/// refusing it aborted pagination after the first page and discarded the
/// partial extract over an answer that was never ambiguous.
pub(crate) fn next_link(
    headers: &ureq::http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<Option<AuthorizedUrl>, ContinuationError> {
    // Compared after resolution, not before. A proxy that repeats the header
    // may normalize one copy -- a relative reference beside the absolute URL
    // it resolves to -- and comparing the raw text called those two different
    // pages, which is the same false conflict as the duplicate itself.
    let mut targets = TargetSet::default();
    for value in headers.get_all(ureq::http::header::LINK) {
        let Ok(value) = value.to_str() else {
            targets.offer_unreadable(value.as_bytes());
            continue;
        };
        for target in parse_link_field(value)? {
            targets.offer(&target, effective_url, admitted_origin);
        }
    }
    targets.into_one()
}

/// The distinct targets a reply named, and the first reason one of them was
/// refused.
///
/// Shared by both continuation readers rather than written twice. How many
/// pages a reply names has to be decided before any one of them is judged --
/// otherwise a reply naming two different targets is reported under whichever
/// rule the first one happened to break, and the operator is told about the
/// wrong rule. That was fixed once in the link reader and left standing in the
/// redirect reader, so the two disagreed about the same reply.
#[derive(Default)]
struct TargetSet {
    /// Resolved where resolution succeeded; otherwise the best identity
    /// available, because a target that will not resolve is still a target and
    /// still counts toward how many the reply named.
    seen: Vec<Result<AuthorizedUrl, Option<String>>>,
    refusal: Option<ContinuationError>,
}

impl TargetSet {
    /// Record a header value this client cannot read at all.
    ///
    /// It counts: a reply naming a page in bytes we refuse is still a reply
    /// that named a page, and returning on it instead reported a reply naming
    /// two different places under whichever rule the first one broke. The
    /// bytes are the only identity available, so two copies of one unreadable
    /// value are still one target.
    fn offer_unreadable(&mut self, bytes: &[u8]) {
        self.refusal.get_or_insert_with(|| {
            ContinuationError::for_code("rest.protocol.malformed_continuation")
        });
        let entry = Err(Some(format!("{bytes:?}")));
        if !self.seen.contains(&entry) {
            self.seen.push(entry);
        }
    }

    fn offer(&mut self, reference: &str, base: &AuthorizedUrl, admitted_origin: &Origin) {
        let entry = match resolve_then_authorize(base, reference, admitted_origin) {
            Ok(resolved) => Ok(resolved),
            Err((rendered, error)) => {
                self.refusal.get_or_insert(error);
                // A reference that will not resolve has no identity of its
                // own, and the text the reply happened to use is not one: the
                // same unusable target written relatively and absolutely
                // compares unequal that way, so one bad target was counted as
                // two and reported as a reply naming two different pages. All
                // of them collapse to a single unresolvable target instead,
                // which is what the reply names -- one page nobody can fetch.
                Err(rendered)
            }
        };
        if !self.seen.contains(&entry) {
            self.seen.push(entry);
        }
    }

    fn into_one(mut self) -> Result<Option<AuthorizedUrl>, ContinuationError> {
        if self.seen.len() > 1 {
            return Err(ContinuationError::for_code(
                "rest.protocol.conflicting_continuation",
            ));
        }
        match self.seen.pop() {
            Some(Ok(target)) => Ok(Some(target)),
            Some(Err(_)) => Err(self.refusal.unwrap_or_else(|| {
                ContinuationError::for_code("rest.protocol.unresolvable_continuation")
            })),
            None => Ok(None),
        }
    }
}

/// Read one redirect Location value and authorize it.
///
/// Two `Location` headers naming the same target are the same redirect said
/// twice — the header-repeating proxies that produce duplicate `Link` headers
/// produce these too, and only a reply naming two different targets is the
/// ambiguity this refuses.
pub(crate) fn redirect_location(
    headers: &ureq::http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, ContinuationError> {
    let mut targets = TargetSet::default();
    for value in headers.get_all(ureq::http::header::LOCATION) {
        let Ok(value) = value.to_str() else {
            targets.offer_unreadable(value.as_bytes());
            continue;
        };
        targets.offer(value, effective_url, admitted_origin);
    }
    let resolved = targets.into_one()?;
    resolved.ok_or_else(|| ContinuationError::for_code("rest.protocol.malformed_continuation"))
}

/// The port a scheme implies when none is written.
const fn default_port(scheme: &str) -> Option<u16> {
    match scheme.as_bytes() {
        b"https" => Some(443),
        b"http" => Some(80),
        _ => None,
    }
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
    // An explicitly written default port is dropped, so one page has one
    // rendering. The origin already treats `https://host` and
    // `https://host:443` as the same place, and leaving the identity
    // disagreeing with that made two spellings of one target count as two --
    // refusing a reply as naming two next pages when it named one, and
    // slipping past both the redirect-cycle guard and the visited-page set,
    // which recognise a page by this string.
    let authority = match port {
        Some(port) if Some(port) == default_port(scheme.as_str()) => host.clone(),
        Some(port) => format!("{host}:{port}"),
        None => host.clone(),
    };
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
            for parameter in split_parameters(parameters)? {
                let parameter = parameter.trim();
                if parameter.is_empty() {
                    continue;
                }
                let Some((name, value)) = parameter.split_once('=') else {
                    continue;
                };
                // The first `rel` wins and later ones are ignored, which is
                // what RFC 8288 says to do with a repeated parameter. A
                // gateway that appends a canonical `rel` to a link that
                // already carried one produces this, and refusing it ended
                // the pull and discarded every record already extracted.
                if name.trim().eq_ignore_ascii_case("rel") && rel.is_none() {
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
    // A blank element is noise, not a malformed reply: an empty `Link` header
    // and a trailing comma both produce one, and both are gateways saying
    // there is no next page. Refusing the whole field ended the pull with a
    // protocol error and threw away every record already pulled.
    entries.retain(|entry| !entry.trim().is_empty());
    Ok(entries)
}

/// Split a link's parameters on `;`, ignoring separators inside a quoted
/// value.
///
/// A quoted parameter may contain anything, `;` and `rel=` included. Splitting
/// on the character alone read one such value as a second `rel`, and a reply
/// carrying a title or type that happened to contain a semicolon aborted the
/// pull and discarded everything already extracted.
fn split_parameters(parameters: &str) -> Result<Vec<&str>, ContinuationError> {
    let mut parts = Vec::new();
    let mut start = 0_usize;
    let mut in_quote = false;
    let mut escaped = false;
    for (index, character) in parameters.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' if in_quote => escaped = true,
            '"' => in_quote = !in_quote,
            ';' if !in_quote => {
                parts.push(&parameters[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    if in_quote || escaped {
        return Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
    }
    parts.push(&parameters[start..]);
    // The leading empty part before the first `;`, which the caller has
    // already required to be there.
    Ok(parts.split_off(1))
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

    fn headers_of(name: ureq::http::HeaderName, values: &[&str]) -> ureq::http::HeaderMap {
        let mut headers = ureq::http::HeaderMap::new();
        for value in values {
            headers.append(name.clone(), value.parse().expect("a valid header value"));
        }
        headers
    }

    /// Header noise a client can resolve is not a malformed reply. A gateway
    /// with no next page emits an empty `Link` or a trailing comma, and a
    /// quoted parameter may contain anything the grammar allows -- including
    /// the characters the field is split on. Refusing either ended the pull
    /// with a protocol error and discarded every record already extracted.
    #[test]
    fn noise_a_client_can_resolve_does_not_end_the_pull() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let next = "<https://api.example.test/v1/items?page=2>; rel=\"next\"";

        for spelling in ["", "   ", &format!("{next},"), &format!(",{next}")] {
            let read = next_link(
                &headers_of(ureq::http::header::LINK, &[spelling]),
                &base,
                &origin,
            )
            .unwrap_or_else(|error| panic!("{spelling:?} is readable, got {error}"));
            let expected = spelling
                .contains("rel=")
                .then_some("https://api.example.test/v1/items?page=2");
            assert_eq!(read.as_ref().map(AuthorizedUrl::as_str), expected);
        }

        // A repeated `rel` leaves the target perfectly readable; the format
        // says to take the first. A title carrying bytes outside visible
        // ASCII is refused instead -- see open question 53.
        let repeated_rel = "<https://api.example.test/v1/items?page=2>; rel=\"next\"; rel=\"next\"";
        let read = next_link(
            &headers_of(ureq::http::header::LINK, &[repeated_rel]),
            &base,
            &origin,
        )
        .unwrap_or_else(|error| panic!("a repeated `rel` is readable, got {error}"))
        .expect("there is a next page");
        assert_eq!(read.as_str(), "https://api.example.test/v1/items?page=2");

        // A `;` and a `rel=` inside a quoted value belong to the value.
        let quoted = "<https://api.example.test/v1/items?page=2>; \
                      title=\"a; rel=first\"; rel=\"next\"";
        let read = next_link(
            &headers_of(ureq::http::header::LINK, &[quoted]),
            &base,
            &origin,
        )
        .expect("a quoted parameter is one parameter")
        .expect("there is a next page");
        assert_eq!(read.as_str(), "https://api.example.test/v1/items?page=2");
    }

    /// A reply that names one next page twice is not a reply that names two.
    /// Header-merging proxies repeat `Link` and `Location` routinely, and
    /// treating a repeat as a conflict aborted pagination after the first page
    /// and threw away everything already pulled. Both readers are checked
    /// because the rule held at one of them and not its sibling.
    #[test]
    fn one_target_named_twice_is_not_a_conflict() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let next = "<https://api.example.test/v1/items?page=2>; rel=\"next\"";

        let repeated = next_link(
            &headers_of(ureq::http::header::LINK, &[next, next]),
            &base,
            &origin,
        )
        .expect("the same next page named twice resolves")
        .expect("there is a next page");
        assert_eq!(
            repeated.as_str(),
            "https://api.example.test/v1/items?page=2"
        );

        let conflicting = next_link(
            &headers_of(
                ureq::http::header::LINK,
                &[
                    next,
                    "<https://api.example.test/v1/items?page=9>; rel=\"next\"",
                ],
            ),
            &base,
            &origin,
        )
        .expect_err("two different next pages are still unresolvable");
        assert_eq!(
            conflicting.classification.code(),
            "rest.protocol.conflicting_continuation"
        );

        let target = "https://api.example.test/v1/items?page=2";
        let redirect = redirect_location(
            &headers_of(ureq::http::header::LOCATION, &[target, target]),
            &base,
            &origin,
        )
        .expect("the same redirect target named twice resolves");
        assert_eq!(redirect.as_str(), target);

        let disagreeing = redirect_location(
            &headers_of(
                ureq::http::header::LOCATION,
                &[target, "https://api.example.test/v1/items?page=9"],
            ),
            &base,
            &origin,
        )
        .expect_err("two different redirect targets are still unresolvable");
        assert_eq!(
            disagreeing.classification.code(),
            "rest.protocol.conflicting_continuation"
        );
    }

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
        // And the same page: a default port written out is dropped, so one
        // target has one identity however the reply spelled it.
        assert_eq!(
            explicit.as_str(),
            "https://api.example.test/v1/items?page=2"
        );

        let implicit =
            resolve_and_authorize(&base, "https://api.example.test/v1/items?page=2", &origin)
                .expect("the same page without the port");
        assert_eq!(explicit, implicit, "which is what makes them one target");
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
