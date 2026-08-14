//! Normalized REST continuation and redirect admission.
//!
//! This module is the single trust boundary for every server-directed follow-up
//! request. It resolves RFC 8288 link targets and redirect locations against
//! the effective response URL, normalizes the destination origin, and rejects
//! foreign or downgraded targets before the caller can construct a request.

use std::fmt;

use clinker_core_types::{FailureCategory, FailureClassification, RetryAdvice};
use http::Uri;

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
    headers: &http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<Option<AuthorizedUrl>, ContinuationError> {
    // Compared after resolution, not before. A proxy that repeats the header
    // may normalize one copy -- a relative reference beside the absolute URL
    // it resolves to -- and comparing the raw text called those two different
    // pages, which is the same false conflict as the duplicate itself.
    //
    // A link-value this client cannot parse is held rather than returned on.
    // It is not offered as a target either: an unreadable link-value may have
    // named no next page, one, or several, and calling it one invented a page
    // a readable `rel=next` beside it then conflicted with. Holding it keeps
    // the count honest -- a reply that genuinely names two different pages is
    // still reported as the conflict it is -- while a reply whose continuation
    // cannot be established is still refused.
    //
    // The field is read as bytes. A field value is opaque data, not text, and
    // decoding the whole of it made a `title` carrying one non-ASCII byte end
    // the pull; only the target has to become a URL, so only the target is
    // decoded.
    let mut targets = TargetSet::default();
    let mut unreadable: Option<ContinuationError> = None;
    for value in headers.get_all(http::header::LINK) {
        let parsed = parse_link_field(value.as_bytes());
        for target in parsed.next_targets {
            targets.offer(&target, effective_url, admitted_origin);
        }
        if let Some(error) = parsed.refusal {
            unreadable.get_or_insert(error);
        }
    }
    let resolved = targets.into_one()?;
    // Only once no conflict was found: a field we could not read might have
    // named the page this one did not.
    if let Some(error) = unreadable {
        return Err(error);
    }
    Ok(resolved)
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
    headers: &http::HeaderMap,
    effective_url: &AuthorizedUrl,
    admitted_origin: &Origin,
) -> Result<AuthorizedUrl, ContinuationError> {
    let mut targets = TargetSet::default();
    let mut unreadable: Option<ContinuationError> = None;
    for value in headers.get_all(http::header::LOCATION) {
        let Ok(value) = value.to_str() else {
            unreadable.get_or_insert_with(|| {
                ContinuationError::for_code("rest.protocol.malformed_continuation")
            });
            continue;
        };
        targets.offer(value, effective_url, admitted_origin);
    }
    let resolved = targets.into_one()?;
    if let Some(error) = unreadable {
        return Err(error);
    }
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

/// Resolve `.` and `..` out of a path, by the algorithm RFC 3986 §5.2.4
/// specifies.
///
/// This output is both the URL that gets requested and the identity a page is
/// recognised by, so a deviation from the specified algorithm costs twice
/// over. Splitting on `/` and rebuilding from the segments that survived
/// discarded the empty ones, which turned `/a//b/../c` into `/a/c`: a
/// different resource on any server that does not collapse a doubled slash,
/// and at the same time the identity `/a/b/../c` already had, so a reply
/// naming both ended a valid pull as a continuation cycle. The same shorthand
/// lost the trailing slash that a path ending in `..` resolves to, and it ran
/// only when a dot segment was present, so one path normalized two ways
/// depending on syntax elsewhere in it.
///
/// The specified algorithm moves whole segments between two buffers instead.
/// An empty segment is carried like any other, and the trailing `/` is
/// whatever the last move left rather than something reapplied afterwards.
fn normalize_path(path: &str) -> String {
    let mut input = path;
    let mut output = String::with_capacity(path.len());
    while !input.is_empty() {
        // Steps B and C replace a leading `/./` or `/../` with `/`. That `/`
        // belongs to the segment that follows, so it is left on the front of
        // the input for the move below rather than written out here.
        if let Some(rest) = input.strip_prefix("../") {
            input = rest;
        } else if let Some(rest) = input.strip_prefix("./") {
            input = rest;
        } else if input.starts_with("/./") {
            input = &input["/.".len()..];
        } else if input == "/." {
            input = "/";
        } else if input.starts_with("/../") {
            input = &input["/..".len()..];
            remove_last_segment(&mut output);
        } else if input == "/.." {
            input = "/";
            remove_last_segment(&mut output);
        } else if input == "." || input == ".." {
            input = "";
        } else {
            // One segment: the leading `/`, if any, plus everything up to the
            // next one. An empty segment is the two-slash case and moves as
            // itself.
            let after_leading_slash = usize::from(input.starts_with('/'));
            let end = input[after_leading_slash..]
                .find('/')
                .map_or(input.len(), |offset| after_leading_slash + offset);
            output.push_str(&input[..end]);
            input = &input[end..];
        }
    }
    // A path that resolved to nothing is the origin's root, and the origin
    // check downstream reads the authority out of `scheme://authority{path}`:
    // a path that does not begin with `/` would move the authority's boundary.
    if output.starts_with('/') {
        output
    } else {
        format!("/{output}")
    }
}

/// Drop the last segment written to `output`, and the `/` that preceded it.
fn remove_last_segment(output: &mut String) {
    match output.rfind('/') {
        Some(index) => output.truncate(index),
        None => output.clear(),
    }
}

/// What one `Link` field named, and the first reason a link-value in it could
/// not be read.
///
/// Both halves are returned because either alone loses a reply the other
/// answers. RFC 8288 Appendix B says to carry on with the remaining
/// link-values when one cannot be parsed, so the readable ones are still
/// counted -- without which a reply naming two different next pages beside one
/// piece of junk was reported as junk rather than as the conflict it is. The
/// refusal is still carried out, because a link-value nobody can read may have
/// named a page the readable ones did not, and treating it as silence ended
/// pagination as though the server had said there was no next page.
#[derive(Default)]
struct ParsedLinkField {
    next_targets: Vec<String>,
    refusal: Option<ContinuationError>,
}

/// Read a `Link` field's bytes and collect the targets it marks `rel=next`.
///
/// The field is bytes, not text. RFC 9110 makes bytes 0x80-0xFF in a field
/// value opaque data rather than an error, and RFC 8187 makes ignoring a
/// parameter that cannot be decoded the correct response to one -- so every
/// parameter here is compared and discarded as bytes, and never decoded. Only
/// the target inside `<...>` is decoded, because only the target has to become
/// a URL.
fn parse_link_field(field: &[u8]) -> ParsedLinkField {
    let mut parsed = ParsedLinkField::default();
    let entries = match split_link_values(field) {
        Ok(entries) => entries,
        Err(error) => {
            parsed.refusal = Some(error);
            return parsed;
        }
    };
    for entry in entries {
        match parse_link_value(entry) {
            Ok(Some(target)) => parsed.next_targets.push(target),
            Ok(None) => {}
            Err(error) => {
                parsed.refusal.get_or_insert(error);
            }
        }
    }
    parsed
}

/// Read one link-value, returning its target when it names the `next`
/// relation.
fn parse_link_value(entry: &[u8]) -> Result<Option<String>, ContinuationError> {
    let entry = entry.trim_ascii();
    let Some(after_open) = entry.strip_prefix(b"<") else {
        return Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
    };
    let Some(close) = after_open.iter().position(|byte| *byte == b'>') else {
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
    let mut rel: Option<Vec<u8>> = None;
    let parameters = after_open[close + 1..].trim_ascii();
    if !parameters.is_empty() {
        if !parameters.starts_with(b";") {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        }
        for parameter in split_parameters(parameters)? {
            let parameter = parameter.trim_ascii();
            if parameter.is_empty() {
                continue;
            }
            let Some(equals) = parameter.iter().position(|byte| *byte == b'=') else {
                continue;
            };
            let (name, value) = parameter.split_at(equals);
            // The first `rel` wins and later ones are ignored, which is what
            // RFC 8288 says to do with a repeated parameter. A gateway that
            // appends a canonical `rel` to a link that already carried one
            // produces this, and refusing it ended the pull and discarded
            // every record already extracted.
            if name.trim_ascii().eq_ignore_ascii_case(b"rel") && rel.is_none() {
                rel = Some(unquote(value[1..].trim_ascii())?);
            }
        }
    }
    let names_next = rel.as_deref().is_some_and(|relations| {
        relations
            .split(u8::is_ascii_whitespace)
            .any(|rel| rel.eq_ignore_ascii_case(b"next"))
    });
    if !names_next {
        return Ok(None);
    }
    // The one place decoding is owed: a target that is not UTF-8 cannot be a
    // URL, so this is a link the reader could not follow however it were
    // spelled, and refusing it is the honest answer rather than a decoding
    // limitation showing through.
    let Ok(target) = std::str::from_utf8(target) else {
        return Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
    };
    Ok(Some(target.to_owned()))
}

fn split_link_values(field: &[u8]) -> Result<Vec<&[u8]>, ContinuationError> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut in_angle = false;
    let mut in_quote = false;
    let mut escaped = false;
    // Splitting on bytes rather than characters is safe here and everywhere
    // below: every byte of a multi-byte UTF-8 sequence is 0x80 or above, so
    // none of them can be mistaken for one of the ASCII delimiters this
    // walks, and a byte that is not part of any character cannot be either.
    for (index, byte) in field.iter().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match *byte {
            b'\\' if in_quote => escaped = true,
            b'"' if !in_angle => in_quote = !in_quote,
            b'<' if !in_quote => {
                if in_angle {
                    return Err(ContinuationError::for_code(
                        "rest.protocol.malformed_continuation",
                    ));
                }
                in_angle = true;
            }
            b'>' if !in_quote => {
                if !in_angle {
                    return Err(ContinuationError::for_code(
                        "rest.protocol.malformed_continuation",
                    ));
                }
                in_angle = false;
            }
            b',' if !in_angle && !in_quote => {
                entries.push(&field[start..index]);
                start = index + 1;
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
    entries.retain(|entry| !entry.trim_ascii().is_empty());
    Ok(entries)
}

/// Split a link's parameters on `;`, ignoring separators inside a quoted
/// value.
///
/// A quoted parameter may contain anything, `;` and `rel=` included. Splitting
/// on the character alone read one such value as a second `rel`, and a reply
/// carrying a title or type that happened to contain a semicolon aborted the
/// pull and discarded everything already extracted.
fn split_parameters(parameters: &[u8]) -> Result<Vec<&[u8]>, ContinuationError> {
    let mut parts = Vec::new();
    let mut start = 0_usize;
    let mut in_quote = false;
    let mut escaped = false;
    for (index, byte) in parameters.iter().enumerate() {
        match *byte {
            _ if escaped => escaped = false,
            b'\\' if in_quote => escaped = true,
            b'"' => in_quote = !in_quote,
            b';' if !in_quote => {
                parts.push(&parameters[start..index]);
                start = index + 1;
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

/// Undo quoted-string quoting, keeping the value as bytes.
///
/// The result is never decoded. A `rel` is a list of ASCII tokens compared
/// case-insensitively, which bytes answer exactly; anything else the value
/// carries is data this reader has no business interpreting.
fn unquote(value: &[u8]) -> Result<Vec<u8>, ContinuationError> {
    if let Some(quoted) = value.strip_prefix(b"\"") {
        let Some(quoted) = quoted.strip_suffix(b"\"") else {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        };
        let mut output = Vec::with_capacity(quoted.len());
        let mut escaped = false;
        for byte in quoted {
            if escaped {
                output.push(*byte);
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else {
                output.push(*byte);
            }
        }
        if escaped {
            return Err(ContinuationError::for_code(
                "rest.protocol.malformed_continuation",
            ));
        }
        Ok(output)
    } else if value.contains(&b'"') {
        Err(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ))
    } else {
        Ok(value.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_of(name: http::HeaderName, values: &[&str]) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
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
            let read = next_link(&headers_of(http::header::LINK, &[spelling]), &base, &origin)
                .unwrap_or_else(|error| panic!("{spelling:?} is readable, got {error}"));
            let expected = spelling
                .contains("rel=")
                .then_some("https://api.example.test/v1/items?page=2");
            assert_eq!(read.as_ref().map(AuthorizedUrl::as_str), expected);
        }

        // A repeated `rel` leaves the target perfectly readable; the format
        // says to take the first.
        let repeated_rel = "<https://api.example.test/v1/items?page=2>; rel=\"next\"; rel=\"next\"";
        let read = next_link(
            &headers_of(http::header::LINK, &[repeated_rel]),
            &base,
            &origin,
        )
        .unwrap_or_else(|error| panic!("a repeated `rel` is readable, got {error}"))
        .expect("there is a next page");
        assert_eq!(read.as_str(), "https://api.example.test/v1/items?page=2");

        // A `;` and a `rel=` inside a quoted value belong to the value.
        let quoted = "<https://api.example.test/v1/items?page=2>; \
                      title=\"a; rel=first\"; rel=\"next\"";
        let read = next_link(&headers_of(http::header::LINK, &[quoted]), &base, &origin)
            .expect("a quoted parameter is one parameter")
            .expect("there is a next page");
        assert_eq!(read.as_str(), "https://api.example.test/v1/items?page=2");
    }

    /// A header this client cannot read is not a page it named. Counting one
    /// as a target invented a next page that a readable `rel=next` beside it
    /// then conflicted with -- so a reply naming exactly one page was refused
    /// as naming two, under the wrong rule. The refusal is still made, after
    /// the readable targets have been counted.
    ///
    /// The unreadable header here is one whose target is not UTF-8, which is
    /// now the whole of what "cannot read" means: a target that cannot become
    /// a URL. A parameter carrying the same bytes used to land here too, and
    /// [`a_parameter_this_reader_never_decodes_cannot_end_the_pull`] is the
    /// same header proving it no longer does.
    #[test]
    fn an_unreadable_header_is_not_a_page_the_reply_named() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let mut headers = headers_of(
            http::header::LINK,
            &["<https://api.example.test/v1/items?page=2>; rel=\"next\""],
        );
        headers.append(
            http::header::LINK,
            http::HeaderValue::from_bytes(b"<https://api.example.test/v1/\xff>; rel=\"next\"")
                .expect("a header carrying a byte outside visible ASCII"),
        );

        let refused = next_link(&headers, &base, &origin)
            .expect_err("a field that cannot be read leaves the continuation unestablished");
        assert_eq!(
            refused.classification_code(),
            "rest.protocol.malformed_continuation",
            "and it is named as unreadable, not as a conflict between two pages"
        );
    }

    /// A field value is opaque bytes, and a parameter this reader never looks
    /// at may carry any of them. Decoding the whole field as visible ASCII
    /// made one accented character in a `title` end the pull and discard every
    /// record already extracted -- the header below is the one the sibling
    /// test above used to be built on.
    #[test]
    fn a_parameter_this_reader_never_decodes_cannot_end_the_pull() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let mut headers = http::HeaderMap::new();
        headers.append(
            http::header::LINK,
            http::HeaderValue::from_bytes(
                b"<https://api.example.test/v1/items?page=2>; rel=\"next\"; title=\"caf\xc3\xa9\"",
            )
            .expect("a header carrying valid UTF-8 outside ASCII"),
        );
        headers.append(
            http::header::LINK,
            http::HeaderValue::from_bytes(b"</v1/prev>; rel=\"prev\"; title=\"\xff\"")
                .expect("a header carrying a byte that is not UTF-8 at all"),
        );

        let read = next_link(&headers, &base, &origin)
            .expect("neither parameter has to be decoded to find the next page")
            .expect("there is a next page");
        assert_eq!(read.as_str(), "https://api.example.test/v1/items?page=2");
    }

    /// Only the target is decoded, and it is decoded as UTF-8 rather than as
    /// visible ASCII: a target outside ASCII still has to become a URL, and a
    /// target that is not UTF-8 cannot become one however it is spelled.
    #[test]
    fn only_the_target_is_decoded_and_it_is_decoded_as_utf8() {
        let parsed = parse_link_field("<https://api.example.test/caf\u{e9}>; rel=next".as_bytes());
        assert_eq!(parsed.next_targets, ["https://api.example.test/café"]);
        assert!(parsed.refusal.is_none(), "a UTF-8 target is readable");
        // And it is then judged as a target rather than discarded as an
        // unreadable header: the URL parser takes the byte in a path, and the
        // origin rules still decide whether the page may be fetched. Nothing
        // outside the path is loosened -- an ASCII control byte, which is what
        // a request could be split on, is still refused before this point.
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let resolved = resolve_and_authorize(&base, &parsed.next_targets[0], &origin)
            .expect("a page on the admitted origin");
        assert_eq!(resolved.as_str(), "https://api.example.test/café");
        let foreign = resolve_and_authorize(&base, "https://elsewhere.test/café", &origin)
            .expect_err("the origin rules do not care what the path spells");
        assert_eq!(foreign.classification_code(), "rest.security.cross_origin");

        let parsed = parse_link_field(b"<https://api.example.test/\xff>; rel=next");
        assert!(
            parsed.next_targets.is_empty(),
            "a target that cannot become a URL is not offered as one"
        );
        assert_eq!(
            parsed
                .refusal
                .as_ref()
                .map(ContinuationError::classification_code),
            Some("rest.protocol.malformed_continuation")
        );
    }

    /// RFC 8288 Appendix B: a link-value that cannot be parsed is skipped and
    /// the rest of the field is still read. Discarding the whole field instead
    /// reported a reply that genuinely named two different next pages as
    /// malformed, telling the operator about the wrong rule -- and the refusal
    /// the junk earns is still made, so nothing is read past silently.
    #[test]
    fn one_unparseable_link_value_does_not_discard_the_others() {
        let junk = "notalink; rel=\"next\"";
        let second = "<https://api.example.test/v1/items?page=2>; rel=\"next\"";

        let parsed = parse_link_field(format!("{junk}, {second}").as_bytes());
        assert_eq!(
            parsed.next_targets,
            ["https://api.example.test/v1/items?page=2"],
            "the readable link-value is still read"
        );
        assert_eq!(
            parsed
                .refusal
                .as_ref()
                .map(ContinuationError::classification_code),
            Some("rest.protocol.malformed_continuation"),
            "and the unreadable one is still refused"
        );

        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let third = "<https://api.example.test/v1/items?page=9>; rel=\"next\"";
        let conflicting = next_link(
            &headers_of(http::header::LINK, &[&format!("{junk}, {second}, {third}")]),
            &base,
            &origin,
        )
        .expect_err("two different next pages are unresolvable");
        assert_eq!(
            conflicting.classification_code(),
            "rest.protocol.conflicting_continuation",
            "reported as the conflict it is, not as the junk beside it"
        );
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
            &headers_of(http::header::LINK, &[next, next]),
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
                http::header::LINK,
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
            &headers_of(http::header::LOCATION, &[target, target]),
            &base,
            &origin,
        )
        .expect("the same redirect target named twice resolves");
        assert_eq!(redirect.as_str(), target);

        let disagreeing = redirect_location(
            &headers_of(
                http::header::LOCATION,
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

    /// The algorithm RFC 3986 §5.2.4 specifies, against the cases an ad-hoc
    /// one gets wrong: an empty segment is a segment, and the trailing slash
    /// a `..` resolves to is part of the path it names.
    #[test]
    fn dot_segments_resolve_the_way_the_specification_says() {
        for (path, expected) in [
            // The reviewer's case. Rebuilding from non-empty segments dropped
            // the doubled slash and named a different resource.
            ("/a//b/../c", "/a//c"),
            ("/a//b/../", "/a//"),
            // A `..` in final position resolves to a directory, so the path
            // ends in the slash the algorithm's last move wrote.
            ("/a/b/..", "/a/"),
            ("/a/b/.", "/a/b/"),
            ("/a/b/../..", "/"),
            // Traversal cannot climb above the root.
            ("/..", "/"),
            ("/../..", "/"),
            ("/a/../../b", "/b"),
            // No dot segment at all: unchanged, including every empty
            // segment. This used to be a separate early return, which is how
            // one path normalized two ways depending on syntax elsewhere.
            ("/a//b//c", "/a//b//c"),
            ("//a", "//a"),
            ("/a/", "/a/"),
            ("/", "/"),
            ("", "/"),
            // RFC 3986 §5.2.4's own worked example.
            ("/a/b/c/./../../g", "/a/g"),
            ("/b/c/./../../g", "/g"),
        ] {
            assert_eq!(
                normalize_path(path),
                expected,
                "{path:?} does not resolve the way §5.2.4 resolves it"
            );
        }
    }

    /// A page is recognised by its normalized URL, so two paths that name two
    /// resources have to normalize to two strings. Collapsing empty segments
    /// gave these one identity between them, and a reply naming the second
    /// after the first ended a valid pull as a continuation cycle.
    #[test]
    fn two_distinct_pages_do_not_normalize_to_one_identity() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        let doubled = resolve_and_authorize(&base, "/v1//items/../page2", &origin)
            .expect("a doubled slash is an ordinary path");
        let single = resolve_and_authorize(&base, "/v1/items/../page2", &origin)
            .expect("and so is the path without it");

        assert_eq!(
            doubled.as_str(),
            "https://api.example.test/v1//page2",
            "the request goes to the resource the reply named"
        );
        assert_eq!(single.as_str(), "https://api.example.test/v1/page2");
        assert_ne!(
            doubled.as_str(),
            single.as_str(),
            "two resources must not share one page identity"
        );
    }

    /// Preserving empty segments means a path can now resolve to one that
    /// begins with `//`, and the origin check reads the authority back out of
    /// `scheme://authority{path}`. A leading empty segment must therefore stay
    /// inside the path and never move where the authority ends.
    ///
    /// Each of these is an absolute-path reference — the authority is the
    /// base's, and what follows is only a path — so each stays on the admitted
    /// origin however its dot segments resolve. A reference that names an
    /// authority of its own is a different thing and is still refused, whether
    /// it writes the scheme out or arrives as a network-path reference.
    #[test]
    fn resolving_dot_segments_cannot_move_the_origin() {
        let (origin, base) =
            authorize_initial("https://api.example.test/v1/items").expect("initial URL");
        for (reference, expected) in [
            (
                "/x/..//evil.example.test/data",
                "https://api.example.test//evil.example.test/data",
            ),
            (
                "/..//evil.example.test/data",
                "https://api.example.test//evil.example.test/data",
            ),
            (
                "/v1/../..//evil.example.test",
                "https://api.example.test//evil.example.test",
            ),
        ] {
            let resolved = resolve_and_authorize(&base, reference, &origin)
                .unwrap_or_else(|error| panic!("{reference:?} is same-origin, got {error}"));
            assert_eq!(
                resolved.as_str(),
                expected,
                "{reference:?} names a path on the admitted origin"
            );
            assert_eq!(
                origin_of(&resolved).expect("a resolved target has an origin"),
                origin,
                "{reference:?} must still read back as the admitted origin"
            );
        }

        for foreign in [
            "https://evil.example.test/data",
            "//evil.example.test/../data",
        ] {
            let refused = resolve_and_authorize(&base, foreign, &origin)
                .expect_err("a target naming its own authority is still refused");
            assert_eq!(
                refused.classification.code(),
                "rest.security.cross_origin",
                "{foreign:?} names another origin"
            );
        }
    }

    /// The bytes most likely to break a field walker: the delimiters it
    /// splits on, the escape it honours, obs-text on both sides of the UTF-8
    /// boundary, a lone continuation byte belonging to no character, and NUL.
    const INTERESTING: [u8; 16] = [
        b'<', b'>', b'"', b'\\', b';', b',', b'=', b' ', b'\t', b'r', b'n', 0x00, 0x7f, 0x80, 0xc3,
        0xff,
    ];

    /// A deterministic pseudorandom source. A fixed seed is the point: a
    /// corpus that differs between runs turns a reproducible failure into a
    /// flake, and there is no fuzzing harness in this workspace to replay a
    /// crashing input from.
    fn scramble(state: &mut u64) -> u64 {
        let mut bits = *state;
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        *state = bits;
        bits
    }

    /// Every byte sequence the parser is asked to survive, built the same way
    /// on every run.
    fn adversarial_corpus() -> Vec<Vec<u8>> {
        let mut corpus: Vec<Vec<u8>> = Vec::new();

        // Exhaustive over the delimiter alphabet up to three bytes: every
        // ordering of an angle bracket, a quote, an escape, and a separator
        // against each other, including the unbalanced ones.
        corpus.push(Vec::new());
        let mut previous: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..3 {
            let mut grown = Vec::new();
            for prefix in &previous {
                for byte in INTERESTING {
                    let mut candidate = prefix.clone();
                    candidate.push(byte);
                    grown.push(candidate);
                }
            }
            corpus.extend(grown.iter().cloned());
            previous = grown;
        }

        // Every single-byte mutation of a well-formed field, plus every
        // truncation of it: the boundaries a walker reaches only when a
        // structure it was part-way through ends early.
        let base = b"<https://h/a?p=1>; rel=\"next\"; title=\"t\"".to_vec();
        for index in 0..=base.len() {
            corpus.push(base[..index].to_vec());
            for byte in INTERESTING {
                let mut inserted = base.clone();
                inserted.insert(index, byte);
                corpus.push(inserted);
                if index < base.len() {
                    let mut replaced = base.clone();
                    replaced[index] = byte;
                    corpus.push(replaced);
                }
            }
        }

        // Hand-written cases the generators above are unlikely to reach.
        for case in [
            &b"<>"[..],
            b"<>; rel=next",
            b"<a>; rel=\"",
            b"<a>; rel=\"next",
            b"<a>; rel=\"\\\"",
            b"<a>; rel=\"ne\\\"xt\"",
            b"<a>; rel=next; rel=prev",
            b"<a>; rel=\"NEXT\"",
            b"<a>; rel=\"first next last\"",
            b"<a>; rel=\"next\", <b>; rel=\"next\"",
            b"<a>,,,<b>; rel=next",
            b"<a\xff>; rel=next",
            b"<a\xc3\xa9>; rel=next",
            b"<a>; title=\"\xff\"; rel=next",
            b"<a>; title=\"\x00\"; rel=next",
            b"<a\x00b>; rel=next",
            b"<a>; rel\xff=next",
            b"<a>; =next",
            b"<a>; rel=",
            b"<<a>>; rel=next",
            b"<a>>; rel=next",
            b"a>; rel=next",
            b"\\",
            b"\"\\\"",
        ] {
            corpus.push(case.to_vec());
        }

        // Unbounded-looking inputs: proof the walkers are linear and hold no
        // recursion, and that a long quoted run does not blow the stack.
        for pattern in [&b"<a>; rel=next, "[..], b"\\", b"\"", b"<", b"\xff"] {
            corpus.push(pattern.repeat(20_000));
        }

        // Random bytes over the full range, so nothing above is load-bearing.
        let mut state = 0x5eed_1eaf_c0ff_ee01_u64;
        for _ in 0..20_000 {
            let length = usize::try_from(scramble(&mut state) % 48).expect("a small length");
            let mut case = Vec::with_capacity(length);
            for _ in 0..length {
                let bits = scramble(&mut state);
                // Half drawn from the delimiter alphabet so structure appears
                // often enough to reach the deeper states, half from anywhere.
                let byte = if bits & 1 == 0 {
                    INTERESTING[usize::try_from(bits >> 8).expect("a usize") % INTERESTING.len()]
                } else {
                    u8::try_from((bits >> 8) & 0xff).expect("one byte")
                };
                case.push(byte);
            }
            corpus.push(case);
        }

        corpus
    }

    /// The parser is hand-written against a grammar it is fed by strangers, so
    /// the adversary is automated rather than imagined. Every case runs to
    /// completion -- the test hanging or aborting is the failure -- and what
    /// it returns has to be usable: a target is a URL reference this reader
    /// can go on to resolve, never a fragment of the syntax around it, and a
    /// refusal is one of the registered continuation codes rather than a
    /// panic wearing a different name.
    #[test]
    fn no_byte_sequence_panics_or_yields_an_unusable_target() {
        for case in adversarial_corpus() {
            let parsed = parse_link_field(&case);

            for target in &parsed.next_targets {
                assert!(
                    !target.is_empty(),
                    "an empty target is not a page: {case:?}"
                );
                assert!(
                    !target.contains(['<', '>']),
                    "a target is what was inside the brackets, not the brackets: \
                     {target:?} from {case:?}"
                );
            }
            assert!(
                parsed.next_targets.len() <= case.iter().filter(|byte| **byte == b',').count() + 1,
                "no more targets than the field has link-values: {case:?}"
            );
            if let Some(refusal) = &parsed.refusal {
                assert_eq!(
                    refusal.classification_code(),
                    "rest.protocol.malformed_continuation",
                    "an unreadable field has exactly one name: {case:?}"
                );
            }

            // A reply's meaning may not depend on how many times it is read.
            let again = parse_link_field(&case);
            assert_eq!(again.next_targets, parsed.next_targets, "{case:?}");
            assert_eq!(
                again
                    .refusal
                    .as_ref()
                    .map(ContinuationError::classification_code),
                parsed
                    .refusal
                    .as_ref()
                    .map(ContinuationError::classification_code),
                "{case:?}"
            );
        }
    }

    #[test]
    fn relation_token_lists_select_exactly_one_next_target() {
        let (origin, base) =
            authorize_initial("http://api.example.test/v1/items").expect("initial URL");
        let mut headers = http::HeaderMap::new();
        headers.append(
            http::header::LINK,
            "</v1/items?page=1>; rel=prev"
                .parse()
                .expect("previous Link"),
        );
        headers.append(
            http::header::LINK,
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
