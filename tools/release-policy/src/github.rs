//! Bounded argv-only GitHub API transport.

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
use tempfile::{Builder, NamedTempFile};

use crate::child::{self, ChildSpec, Termination};
use crate::digest;
use crate::error::GateError;
use crate::limits::DEFAULT_CHILD_OUTPUT_BYTES;

/// Maximum admitted size of one freshly downloaded release archive.
pub const MAX_RELEASE_ASSET_BYTES: u64 = 512 * 1024 * 1024;

/// Supported GitHub API verbs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// Read an immutable remote observation.
    Get,
    /// Create a remote object or dispatch.
    Post,
    /// Change the exact authorized remote object.
    Patch,
}

impl Method {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Patch => "PATCH",
        }
    }
}

/// One explicit API request. Fields are emitted as individual argv entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// HTTP method.
    pub method: Method,
    /// Repository-relative API endpoint.
    pub endpoint: String,
    /// Form fields in stable key order.
    pub fields: BTreeMap<String, String>,
    /// Explicit HTTP headers in stable key order.
    pub headers: BTreeMap<String, String>,
    /// Return raw bounded bytes instead of parsing JSON.
    pub raw_response: bool,
    /// Optional regular file supplied as the request body.
    pub input_file: Option<PathBuf>,
    /// Per-call wall-clock deadline.
    pub deadline: Duration,
}

impl Request {
    /// Construct a request without fields.
    #[must_use]
    pub fn new(method: Method, endpoint: impl Into<String>, deadline: Duration) -> Self {
        Self {
            method,
            endpoint: endpoint.into(),
            fields: BTreeMap::new(),
            headers: BTreeMap::new(),
            raw_response: false,
            input_file: None,
            deadline,
        }
    }

    /// Add one explicit form field.
    #[must_use]
    pub fn field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Add one explicit HTTP header.
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Request bounded raw response bytes.
    #[must_use]
    pub fn raw(mut self) -> Self {
        self.raw_response = true;
        self
    }

    /// Supply a local regular file as the request body.
    #[must_use]
    pub fn input_file(mut self, path: PathBuf) -> Self {
        self.input_file = Some(path);
        self
    }
}

/// Bounded JSON observation returned by the transport.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    /// Parsed JSON response, or null for a successful empty response.
    pub body: Value,
    /// Bounded raw response bytes when requested.
    pub raw: Option<Vec<u8>>,
}

/// One bounded release asset streamed into an anonymous temporary file.
#[derive(Debug)]
pub struct DownloadedAsset {
    _temporary: NamedTempFile,
    length: u64,
    sha256: String,
}

impl DownloadedAsset {
    /// Number of bytes admitted from the response body.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    /// SHA-256 digest of the completed temporary file.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Compare the completed download with in-memory authority without loading
    /// the downloaded file back into memory.
    #[must_use]
    pub fn matches_bytes(&self, expected: &[u8]) -> bool {
        self.length == expected.len() as u64 && self.sha256 == digest::sha256_hex(expected)
    }

    fn from_bytes(bytes: &[u8], byte_limit: u64) -> Result<Self, GateError> {
        if bytes.len() as u64 > byte_limit {
            return Err(policy("release asset exceeded its explicit byte limit"));
        }
        let mut temporary = new_asset_temporary()?;
        temporary
            .write_all(bytes)
            .and_then(|()| temporary.flush())
            .and_then(|()| temporary.as_file().sync_all())
            .map_err(|error| GateError::io("write downloaded release asset", &error))?;
        Ok(Self {
            _temporary: temporary,
            length: bytes.len() as u64,
            sha256: digest::sha256_hex(bytes),
        })
    }
}

/// GitHub API boundary used by publication policy.
pub trait GitHubTransport {
    /// Execute one exact request.
    fn send(&mut self, request: &Request) -> Result<Response, GateError>;

    /// Download one raw response under an explicit byte ceiling.
    ///
    /// Production transports should override this method to avoid retaining the
    /// response in memory. The default keeps scripted transports concise while
    /// enforcing the same byte ceiling.
    fn download(
        &mut self,
        request: &Request,
        byte_limit: u64,
    ) -> Result<DownloadedAsset, GateError> {
        let response = self.send(request)?;
        let bytes = response
            .raw
            .ok_or_else(|| policy("release asset transport did not return raw bytes"))?;
        DownloadedAsset::from_bytes(&bytes, byte_limit)
    }
}

/// Production transport implemented through the bounded child runner.
pub struct ChildGitHubTransport {
    program: PathBuf,
    environment: BTreeMap<OsString, OsString>,
}

impl ChildGitHubTransport {
    /// Build the production transport from the allowlisted process environment.
    #[must_use]
    pub fn from_environment() -> Self {
        let mut environment = BTreeMap::new();
        for name in ["GH_TOKEN", "GITHUB_TOKEN", "NO_COLOR", "PATH"] {
            if let Some(value) = std::env::var_os(name) {
                environment.insert(OsString::from(name), value);
            }
        }
        Self {
            program: PathBuf::from("gh"),
            environment,
        }
    }

    /// Construct a child transport with an explicit executable and environment.
    #[doc(hidden)]
    #[must_use]
    pub fn with_program(program: PathBuf, environment: BTreeMap<OsString, OsString>) -> Self {
        Self {
            program,
            environment,
        }
    }
}

impl GitHubTransport for ChildGitHubTransport {
    fn send(&mut self, request: &Request) -> Result<Response, GateError> {
        let arguments = request_arguments(request)?;
        let result = child::run(ChildSpec {
            program: self.program.clone(),
            arguments,
            environment: self.environment.clone(),
            timeout: request.deadline,
            output_limit: DEFAULT_CHILD_OUTPUT_BYTES,
        })?;
        if result.termination == Termination::TimedOut {
            return Err(policy("GitHub API request exceeded its deadline"));
        }
        if result.termination != Termination::Exited(Some(0)) {
            return Err(policy("GitHub API request failed"));
        }
        if result.stdout_truncated || result.stderr_truncated {
            return Err(policy(
                "GitHub API response exceeded the bounded output limit",
            ));
        }
        if request.raw_response {
            return Ok(Response {
                body: Value::Null,
                raw: Some(result.stdout),
            });
        }
        let body = if result.stdout.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&result.stdout)
                .map_err(|_| policy("GitHub API returned malformed JSON"))?
        };
        Ok(Response { body, raw: None })
    }

    fn download(
        &mut self,
        request: &Request,
        byte_limit: u64,
    ) -> Result<DownloadedAsset, GateError> {
        if !request.raw_response || request.method != Method::Get || request.input_file.is_some() {
            return Err(policy(
                "release asset downloads require a raw GET request without an input body",
            ));
        }
        let arguments = request_arguments(request)?;
        let temporary = new_asset_temporary()?;
        let output = temporary
            .reopen()
            .map_err(|error| GateError::io("open release asset temporary", &error))?;
        let result = child::run_stdout_to_file(
            ChildSpec {
                program: self.program.clone(),
                arguments,
                environment: self.environment.clone(),
                timeout: request.deadline,
                output_limit: DEFAULT_CHILD_OUTPUT_BYTES,
            },
            output,
            byte_limit,
        )?;
        if result.termination == Termination::TimedOut {
            return Err(policy("release asset download exceeded its deadline"));
        }
        if result.termination != Termination::Exited(Some(0)) {
            return Err(policy("release asset download failed"));
        }
        if result.stderr_truncated {
            return Err(policy(
                "release asset diagnostics exceeded the bounded output limit",
            ));
        }
        if result.stdout_truncated {
            return Err(policy("release asset exceeded its explicit byte limit"));
        }
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| GateError::io("sync downloaded release asset", &error))?;
        let metadata = temporary
            .as_file()
            .metadata()
            .map_err(|error| GateError::io("inspect downloaded release asset", &error))?;
        if !metadata.is_file() || metadata.len() > byte_limit {
            return Err(policy(
                "downloaded release asset is not a bounded regular file",
            ));
        }
        let reader = temporary
            .reopen()
            .map_err(|error| GateError::io("reopen downloaded release asset", &error))?;
        let sha256 = digest::sha256_reader(reader)
            .map_err(|error| GateError::io("hash downloaded release asset", &error))?;
        Ok(DownloadedAsset {
            _temporary: temporary,
            length: metadata.len(),
            sha256,
        })
    }
}

fn request_arguments(request: &Request) -> Result<Vec<OsString>, GateError> {
    if !request.endpoint.starts_with("repos/")
        && !request
            .endpoint
            .starts_with("https://uploads.github.com/repos/")
    {
        return Err(policy("GitHub endpoint must be repository-relative"));
    }
    let mut arguments = vec![
        OsString::from("api"),
        OsString::from("--method"),
        OsString::from(request.method.as_str()),
        OsString::from(&request.endpoint),
    ];
    for (key, value) in &request.fields {
        arguments.push(OsString::from("-f"));
        arguments.push(OsString::from(format!("{key}={value}")));
    }
    for (key, value) in &request.headers {
        arguments.push(OsString::from("-H"));
        arguments.push(OsString::from(format!("{key}: {value}")));
    }
    if let Some(path) = &request.input_file {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|error| GateError::io("inspect GitHub API input", &error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(policy(
                "GitHub API input must be a regular non-symlink file",
            ));
        }
        arguments.push(OsString::from("--input"));
        arguments.push(path.as_os_str().to_os_string());
    }
    Ok(arguments)
}

fn new_asset_temporary() -> Result<NamedTempFile, GateError> {
    let temporary = Builder::new()
        .prefix("clinker-release-asset.")
        .tempfile()
        .map_err(|error| GateError::io("create release asset temporary", &error))?;
    let metadata = std::fs::symlink_metadata(temporary.path())
        .map_err(|error| GateError::io("inspect release asset temporary", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(policy(
            "release asset temporary must be a regular non-symlink file",
        ));
    }
    Ok(temporary)
}

/// One deterministic request/response exchange for contract tests.
#[derive(Debug, Clone)]
pub struct ScriptedExchange {
    /// Request that must be observed exactly.
    pub request: Request,
    /// Response or stable failure returned to the caller.
    pub response: Result<Response, &'static str>,
}

/// Deterministic fake transport which rejects missing, extra, or reordered calls.
#[derive(Debug, Default)]
pub struct ScriptedTransport {
    exchanges: VecDeque<ScriptedExchange>,
    observed: Vec<Request>,
}

impl ScriptedTransport {
    /// Create a deterministic script.
    #[must_use]
    pub fn new(exchanges: impl IntoIterator<Item = ScriptedExchange>) -> Self {
        Self {
            exchanges: exchanges.into_iter().collect(),
            observed: Vec::new(),
        }
    }

    /// Requests observed so far.
    #[must_use]
    pub fn observed(&self) -> &[Request] {
        &self.observed
    }

    /// True only when every scripted exchange was consumed.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.exchanges.is_empty()
    }
}

impl GitHubTransport for ScriptedTransport {
    fn send(&mut self, request: &Request) -> Result<Response, GateError> {
        self.observed.push(request.clone());
        let exchange = self
            .exchanges
            .pop_front()
            .ok_or_else(|| policy("unexpected GitHub API request"))?;
        if exchange.request != *request {
            return Err(policy(
                "GitHub API request did not match the scripted authority",
            ));
        }
        exchange
            .response
            .map_err(|detail| policy(format!("scripted GitHub API failure: {detail}")))
    }
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("publication.github", detail)
}
