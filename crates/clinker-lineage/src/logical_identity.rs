//! Stable external dataset identities and explicitly authorized identity facts.
//!
//! External lineage is deliberately independent of a worker's filesystem.  A
//! context contains one exact identity for each logical source or output node:
//! either an independently reconstructible canonical datasource identifier or
//! an explicit catalog namespace/name pair.  Concrete collection members and
//! alternate identifiers are represented separately, so neither can change the
//! collection's dataset identity.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use clinker_plan::config::{
    LineageDatasetIdentity, LineageIdentityMode, ResolvedLineageDeliveryPolicy,
};
use serde::{Deserialize, Serialize};

use crate::dataset::{DatasetId, RECORD_TYPE_SEPARATOR};

/// Whether a concrete logical subset was consumed or produced by the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DatasetSubsetDirection {
    /// A concrete member read from a stable input collection.
    Input,
    /// A concrete member written beneath a stable output collection.
    Output,
}

/// One authorized logical member of a stable collection dataset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatasetSubset {
    direction: DatasetSubsetDirection,
    identifier: String,
}

impl DatasetSubset {
    /// Construct an input subset from a location or partition identifier that
    /// is independent of worker storage.
    pub fn input(identifier: impl Into<String>) -> Result<Self, LineageIdentityError> {
        Self::new(DatasetSubsetDirection::Input, identifier.into())
    }

    /// Construct an output subset from a location or partition identifier that
    /// is independent of worker storage.
    pub fn output(identifier: impl Into<String>) -> Result<Self, LineageIdentityError> {
        Self::new(DatasetSubsetDirection::Output, identifier.into())
    }

    fn new(
        direction: DatasetSubsetDirection,
        identifier: String,
    ) -> Result<Self, LineageIdentityError> {
        if !valid_logical_subset(&identifier) {
            return Err(LineageIdentityError::InvalidSubset);
        }
        Ok(Self {
            direction,
            identifier,
        })
    }

    /// Input or output role of this concrete subset.
    pub fn direction(&self) -> DatasetSubsetDirection {
        self.direction
    }

    /// Stable logical partition or location identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// Standard OpenLineage identifier kind used by the symlinks dataset facet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DatasetIdentifierType {
    /// A logical catalog/table alias.
    Table,
    /// A physical or logical location alias.
    Location,
}

/// One explicitly authorized alternate identifier for a dataset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SymlinkIdentifier {
    namespace: String,
    name: String,
    #[serde(rename = "type")]
    identifier_type: DatasetIdentifierType,
}

impl SymlinkIdentifier {
    /// Validate and retain an alternate namespace/name/type exactly.
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        identifier_type: DatasetIdentifierType,
    ) -> Result<Self, LineageIdentityError> {
        let namespace = namespace.into();
        let name = name.into();
        if !valid_identity_component(&namespace) || !valid_identity_component(&name) {
            return Err(LineageIdentityError::InvalidSymlink);
        }
        Ok(Self {
            namespace,
            name,
            identifier_type,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn identifier_type(&self) -> DatasetIdentifierType {
        self.identifier_type
    }
}

/// Closed external identity choice admitted for one dataset node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalDatasetIdentity {
    /// Namespace/name reconstructed from one canonical datasource identifier.
    CanonicalDatasource { dataset: DatasetId },
    /// Exact operator-supplied catalog namespace/name pair.
    Catalog { dataset: DatasetId },
}

impl ExternalDatasetIdentity {
    /// Parse a canonical datasource identifier according to the OpenLineage
    /// naming convention: `scheme://authority` is the namespace and the
    /// non-empty remainder is the dataset name.
    pub fn canonical(identifier: &str) -> Result<Self, LineageIdentityError> {
        let (scheme, remainder) = identifier
            .split_once("://")
            .ok_or(LineageIdentityError::InvalidCanonicalDatasource)?;
        let (authority, name) = remainder
            .split_once('/')
            .ok_or(LineageIdentityError::InvalidCanonicalDatasource)?;
        let valid_scheme = !scheme.is_empty()
            && scheme.bytes().enumerate().all(|(index, byte)| match index {
                0 => byte.is_ascii_alphabetic(),
                _ => byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'),
            });
        if !valid_scheme || !valid_identity_component(authority) || name.starts_with('/') {
            return Err(LineageIdentityError::InvalidCanonicalDatasource);
        }
        if !valid_identity_name(name) {
            return Err(name_error(
                name,
                LineageIdentityError::InvalidCanonicalDatasource,
                "canonical_datasource",
            ));
        }
        Ok(Self::CanonicalDatasource {
            dataset: DatasetId {
                namespace: format!("{scheme}://{authority}"),
                name: name.to_string(),
            },
        })
    }

    /// Retain an exact catalog namespace/name pair.
    pub fn catalog(
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, LineageIdentityError> {
        let namespace = namespace.into();
        let name = name.into();
        if !valid_identity_component(&namespace) {
            return Err(LineageIdentityError::InvalidCatalogIdentity);
        }
        if !valid_identity_name(&name) {
            return Err(name_error(
                &name,
                LineageIdentityError::InvalidCatalogIdentity,
                "catalog_name",
            ));
        }
        Ok(Self::Catalog {
            dataset: DatasetId { namespace, name },
        })
    }

    /// Exact OpenLineage namespace/name selected by this identity.
    pub fn dataset_id(&self) -> &DatasetId {
        match self {
            Self::CanonicalDatasource { dataset } | Self::Catalog { dataset } => dataset,
        }
    }
}

/// One node identity plus explicitly authorized subset and alias facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageNodeBinding {
    node: String,
    identity: ExternalDatasetIdentity,
    subsets: Vec<DatasetSubset>,
    symlinks: Vec<SymlinkIdentifier>,
}

/// Standard identity facets attached to one input or output dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatasetIdentityFacets {
    subsets: Vec<DatasetSubset>,
    symlinks: Vec<SymlinkIdentifier>,
}

impl DatasetIdentityFacets {
    pub fn subsets(&self) -> &[DatasetSubset] {
        &self.subsets
    }

    pub fn symlinks(&self) -> &[SymlinkIdentifier] {
        &self.symlinks
    }
}

impl LineageNodeBinding {
    pub fn new(node: impl Into<String>, identity: ExternalDatasetIdentity) -> Self {
        Self {
            node: node.into(),
            identity,
            subsets: Vec::new(),
            symlinks: Vec::new(),
        }
    }

    pub fn with_subset(mut self, subset: DatasetSubset) -> Self {
        self.subsets.push(subset);
        self
    }

    pub fn with_symlink(mut self, symlink: SymlinkIdentifier) -> Self {
        self.symlinks.push(symlink);
        self
    }
}

/// Fully validated external identity facts for a compiled lineage build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageIdentityContext {
    bindings: BTreeMap<String, LineageNodeBinding>,
}

impl LineageIdentityContext {
    /// Validate an exact set of external logical-node bindings.
    pub fn external(
        bindings: impl IntoIterator<Item = LineageNodeBinding>,
    ) -> Result<Self, LineageIdentityError> {
        let mut by_node = BTreeMap::new();
        for mut binding in bindings {
            if !valid_node(&binding.node) {
                return Err(LineageIdentityError::InvalidNode);
            }
            binding.subsets.sort();
            binding.subsets.dedup();
            binding.symlinks.sort();
            binding.symlinks.dedup();
            let node = binding.node.clone();
            if by_node.insert(node.clone(), binding).is_some() {
                return Err(LineageIdentityError::DuplicateNode { node });
            }
        }
        Ok(Self { bindings: by_node })
    }

    /// Consume the immutable external identity portion of a resolved 03-41
    /// policy.  The policy already rejects partial and ambiguous author forms;
    /// this boundary additionally validates canonical naming syntax.
    pub fn from_resolved(
        policy: &ResolvedLineageDeliveryPolicy,
    ) -> Result<Self, LineageIdentityError> {
        if policy.identity_mode() != LineageIdentityMode::External {
            return Err(LineageIdentityError::ExternalModeRequired);
        }
        let mut bindings = Vec::with_capacity(policy.datasets().len());
        for configured in policy.datasets() {
            let identity = match configured.identity() {
                LineageDatasetIdentity::CanonicalDatasource { identifier } => {
                    ExternalDatasetIdentity::canonical(identifier)?
                }
                LineageDatasetIdentity::Catalog { namespace, name } => {
                    ExternalDatasetIdentity::catalog(namespace.as_ref(), name.as_ref())?
                }
            };
            bindings.push(LineageNodeBinding::new(configured.node(), identity));
        }
        Self::external(bindings)
    }

    /// Look up one exact logical node, failing rather than synthesizing an ID.
    pub fn require(&self, node: &str) -> Result<&LineageNodeBinding, LineageIdentityError> {
        self.bindings
            .get(node)
            .ok_or_else(|| LineageIdentityError::MissingNode {
                node: node.to_string(),
            })
    }

    /// Prove that all source/output nodes that will be emitted have identities.
    pub fn validate_required<'a>(
        &self,
        nodes: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), LineageIdentityError> {
        let required: BTreeSet<&str> = nodes.into_iter().collect();
        for node in required {
            self.require(node)?;
        }
        Ok(())
    }
}

impl LineageNodeBinding {
    pub fn dataset_id(&self) -> &DatasetId {
        self.identity.dataset_id()
    }

    pub fn subsets(&self) -> &[DatasetSubset] {
        &self.subsets
    }

    pub fn symlinks(&self) -> &[SymlinkIdentifier] {
        &self.symlinks
    }

    pub(crate) fn facets(&self) -> DatasetIdentityFacets {
        DatasetIdentityFacets {
            subsets: self.subsets.clone(),
            symlinks: self.symlinks.clone(),
        }
    }
}

/// Sanitized external lineage identity failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineageIdentityError {
    InvalidCanonicalDatasource,
    InvalidCatalogIdentity,
    InvalidNode,
    InvalidSubset,
    InvalidSymlink,
    ExternalModeRequired,
    /// An authored dataset name carries the reserved record-type separator, so
    /// it could name a per-record-type dataset of some other source. `field` is
    /// the configuration key that supplied it.
    ReservedRecordTypeSeparator {
        field: &'static str,
    },
    DuplicateNode {
        node: String,
    },
    MissingNode {
        node: String,
    },
}

impl fmt::Display for LineageIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCanonicalDatasource => f.write_str(
                "canonical datasource must be `scheme://authority/name` with a non-empty logical name",
            ),
            Self::InvalidCatalogIdentity => {
                f.write_str("catalog namespace and name must both be non-empty logical text")
            }
            Self::InvalidNode => f.write_str("lineage node must be a non-empty logical node name"),
            Self::InvalidSubset => f.write_str(
                "dataset subset must be a logical partition/location identifier, not a worker or attempt path",
            ),
            Self::InvalidSymlink => {
                f.write_str("symlink namespace and name must both be non-empty logical text")
            }
            Self::ExternalModeRequired => {
                f.write_str("external lineage identity requires `identity_mode = \"external\"`")
            }
            Self::ReservedRecordTypeSeparator { field } => write!(
                f,
                "`{field}` must not contain `{RECORD_TYPE_SEPARATOR}`: it is reserved to compose \
                 a multi-record source's per-record-type dataset name \
                 (`<name>{RECORD_TYPE_SEPARATOR}<record type>`), so an authored name carrying one \
                 would name the same dataset as a record type of some other source. Rewrite the \
                 name without it — `payments_detail`, not `payments{RECORD_TYPE_SEPARATOR}detail`"
            ),
            Self::DuplicateNode { node } => {
                write!(f, "lineage node `{node}` has more than one identity binding")
            }
            Self::MissingNode { node } => write!(
                f,
                "lineage node `{node}` is missing an external identity; add one canonical datasource or one complete catalog namespace/name pair"
            ),
        }
    }
}

impl std::error::Error for LineageIdentityError {}

fn valid_identity_component(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && !value.chars().any(char::is_control)
        && value.len() <= 1_024
}

/// A dataset's *name* half, which additionally may not carry the reserved
/// [`RECORD_TYPE_SEPARATOR`].
///
/// Only the name is restricted. A per-record-type dataset keeps its base's
/// namespace and appends the separator plus the record type id to the name
/// alone, so a separator anywhere else cannot produce the composed form.
fn valid_identity_name(value: &str) -> bool {
    valid_identity_component(value) && !value.contains(RECORD_TYPE_SEPARATOR)
}

/// Pick the failure that explains why `name` was refused: the reserved
/// separator when that is what it carries, and `generic` for every other way a
/// name can be malformed.
fn name_error(
    name: &str,
    generic: LineageIdentityError,
    field: &'static str,
) -> LineageIdentityError {
    if name.contains(RECORD_TYPE_SEPARATOR) {
        LineageIdentityError::ReservedRecordTypeSeparator { field }
    } else {
        generic
    }
}

fn valid_node(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/'))
}

fn valid_logical_subset(value: &str) -> bool {
    if !valid_identity_component(value)
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains("://")
        || value.split(['/', '\\']).any(|part| part == "..")
    {
        return false;
    }
    let lower = value.to_ascii_lowercase();
    if lower.starts_with("worker-")
        || lower.starts_with("attempt-")
        || lower.starts_with(".clinker-attempts/")
    {
        return false;
    }
    // A `C:/` or `C:\` prefix is a worker-local absolute path, never a logical
    // subset identity.
    let drive_prefixed = value.len() >= 3
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value.as_bytes()[1] == b':'
        && matches!(value.as_bytes()[2], b'/' | b'\\');
    !drive_prefixed
}
