//! Record-level sort specifications: the authored sort field every ordering
//! surface parses, and the placement-only form an ordering-only field is
//! validated into.

use clinker_core_types::QuoteName;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// An authored sort field, as written in YAML on every ordering surface.
/// Ordering-only surfaces validate it into an [`OrderField`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field: String,
    #[serde(default = "default_sort_order")]
    pub order: SortOrder,
    /// Null handling during sort. None for output sorting; Some(Last) default for windows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_order: Option<NullOrder>,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Accepts either a plain string shorthand or a full SortField object in YAML.
///
/// Shorthand: `"field_name"` expands to `SortField { field: "field_name", order: Asc, null_order: None }`.
/// Full: `{ field: "name", order: desc, null_order: first }` deserializes as SortField.
///
/// Custom Deserialize: visit_str -> Short, visit_map -> Full.
/// This gives specific error messages instead of serde(untagged)'s generic "no variant matched".
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SortFieldSpec {
    Short(String),
    Full(SortField),
}

impl<'de> Deserialize<'de> for SortFieldSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SortFieldSpecVisitor;

        impl<'de> Visitor<'de> for SortFieldSpecVisitor {
            type Value = SortFieldSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a field name (string) or a sort field object (map with 'field', 'order', 'null_order')")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SortFieldSpec::Short(v.to_owned()))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let sf = SortField::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SortFieldSpec::Full(sf))
            }
        }

        deserializer.deserialize_any(SortFieldSpecVisitor)
    }
}

impl SortFieldSpec {
    /// Resolve to a concrete SortField.
    pub fn into_sort_field(self) -> SortField {
        match self {
            SortFieldSpec::Short(name) => SortField {
                field: name,
                order: SortOrder::Asc,
                null_order: None,
            },
            SortFieldSpec::Full(sf) => sf,
        }
    }
}

fn default_sort_order() -> SortOrder {
    SortOrder::Asc
}

/// Authored null handling on a sort field.
///
/// This is the vocabulary an author writes. `Drop` belongs to a Sink
/// `sort_order`, whose job includes excluding rows. A Cull or Reshape
/// `order_by`, a Source `sort_order` and a Transform
/// `analytic_window.sort_by` only order rows: they convert through
/// [`OrderField::from_authored`], which refuses `Drop`, so their validated
/// forms cannot hold it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NullOrder {
    /// Nulls sort before all non-null values.
    First,
    /// Nulls sort after all non-null values (SQL convention default).
    #[default]
    Last,
    /// Exclude records whose key is null before sorting.
    Drop,
}

/// Where nulls go among rows that a field only orders: the placement-only
/// counterpart of [`NullOrder`], with no way to remove a row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPlacement {
    /// Nulls sort before all non-null values.
    First,
    /// Nulls sort after all non-null values; the placement of a field whose
    /// author wrote no `null_order`.
    #[default]
    Last,
}

impl From<NullPlacement> for NullOrder {
    fn from(placement: NullPlacement) -> Self {
        match placement {
            NullPlacement::First => NullOrder::First,
            NullPlacement::Last => NullOrder::Last,
        }
    }
}

/// A validated field of an ordering that places rows and never removes
/// them.
///
/// Built by [`OrderField::from_authored`], so the placement is always
/// resolved (an omitted `null_order` is [`NullPlacement::Last`]) and `drop`
/// cannot be represented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderField {
    pub field: String,
    pub order: SortOrder,
    pub null_order: NullPlacement,
}

/// The kind of ordering-only field an authored [`SortField`] came from. It
/// selects the reason and the fix a refused `drop` gives the author.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingSite {
    /// A Cull or Reshape `order_by`: orders the rows of one group, by the
    /// same value order a Sink `sort_order` uses.
    GroupOrderBy,
    /// A Source `sort_order`: the order its records are verified against.
    SourceSortOrder,
    /// A Transform `analytic_window.sort_by`: orders one window partition.
    WindowSortBy,
}

/// An authored `null_order: drop` on a field that only orders rows.
///
/// `Display` is the author-facing message for the site: the rule, the
/// reason, and one fix. Callers prefix the node through the shared quoting
/// helper (`cull "name": `, `source "name": `) and add nothing else, so this
/// is the one place the wording lives. The field name prints through the
/// same helper.
///
/// The fix is the upstream filter: delete `null_order: drop` and add a
/// Transform holding the printed `config:` line before the node, or after a
/// Source. That removes the null-keyed rows, which is what `drop` asked for.
/// `first` and `last` appear only in the reason, to explain that the field
/// places nulls rather than removing them; they are not offered as fixes.
///
/// The filter is printed only when CXL can write the field as a bare name
/// ([`cxl::lexer::is_bare_field_name`]). Any other name gets no CXL at all:
/// a name with a space or a keyword would not parse, and a flattened
/// `Address.City` would parse as a path to another value, so the pasted
/// filter would silently drop every row. For such a field the one next step
/// is the Source schema's `source_name` rename, printed as a
/// `source_name:` line built from the field itself; planning again after the
/// rename prints the filter on the new name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropNotAllowed {
    pub field: String,
    pub site: OrderingSite,
}

impl std::fmt::Display for DropNotAllowed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field = &self.field;
        let quoted = field.quoted_name();
        let (key, reason, filter_at) = match self.site {
            OrderingSite::GroupOrderBy => (
                "order_by",
                "`order_by` only orders the rows of a group, placing nulls `first` or `last`, \
                 and cannot remove a row",
                "before this node",
            ),
            OrderingSite::SourceSortOrder => (
                "sort_order",
                "a Source `sort_order` only states the order its records arrive in, placing \
                 nulls `first` or `last`, and verifying it cannot discard a record",
                "after this source",
            ),
            OrderingSite::WindowSortBy => (
                "analytic_window.sort_by",
                "`sort_by` only orders the rows of a window partition, placing nulls `first` or \
                 `last`, and cannot remove a row",
                "before this node",
            ),
        };
        write!(
            f,
            "`null_order: drop` is not allowed on `{key}` for field {quoted}: {reason}. To \
             remove the rows whose {quoted} is null, "
        )?;
        if cxl::lexer::is_bare_field_name(field) {
            // A bare name is identifier-shaped, so it needs no escaping
            // inside the double-quoted `cxl` string.
            write!(
                f,
                "delete `null_order: drop` and add a Transform {filter_at} with \
                 `config: {{ cxl: \"filter not {field}.is_null()\" }}`."
            )
        } else {
            // A JSON string is a valid YAML double-quoted scalar for every
            // name, so the printed line pastes back as exactly this column.
            let source_name = serde_json::to_string(field).map_err(|_| std::fmt::Error)?;
            write!(
                f,
                "first give the column a name CXL can write: in its Source schema entry, set \
                 `name` to a new identifier and add `source_name: {source_name}`, then use the \
                 new name wherever the pipeline names this column; planning again prints the \
                 filter to add. A CXL name is one identifier of ASCII letters, digits and `_`, \
                 not starting with a digit and not a CXL keyword."
            )
        }
    }
}

impl std::error::Error for DropNotAllowed {}

impl OrderField {
    /// Convert an authored field of the ordering-only `site` into its
    /// placement-only form. An omitted `null_order` becomes
    /// [`NullPlacement::Last`]; `drop` is refused with the site's message.
    ///
    /// Every ordering-only site converts through this one function, so the
    /// rule and its wording cannot drift between nodes.
    pub fn from_authored(field: SortField, site: OrderingSite) -> Result<Self, DropNotAllowed> {
        let null_order = match field.null_order {
            None | Some(NullOrder::Last) => NullPlacement::Last,
            Some(NullOrder::First) => NullPlacement::First,
            Some(NullOrder::Drop) => {
                return Err(DropNotAllowed {
                    field: field.field,
                    site,
                });
            }
        };
        Ok(OrderField {
            field: field.field,
            order: field.order,
            null_order,
        })
    }
}

impl From<&OrderField> for SortField {
    /// The sort key form the executor's comparators take, with the
    /// placement written out.
    fn from(field: &OrderField) -> Self {
        SortField {
            field: field.field.clone(),
            order: field.order,
            null_order: Some(field.null_order.into()),
        }
    }
}

/// Deserialize a list whose entries are each a field name or a full sort
/// field object into `Vec<SortField>`: the two spellings [`SortFieldSpec`]
/// gives a Sink or Source `sort_order`. Used through
/// `#[serde(deserialize_with)]` by a Cull or Reshape `order_by`, so those
/// lists take both spellings too. A window's `sort_by` does not use it and
/// takes the full sort field form only.
pub fn deserialize_sort_field_list<'de, D>(deserializer: D) -> Result<Vec<SortField>, D::Error>
where
    D: Deserializer<'de>,
{
    let specs = Vec::<SortFieldSpec>::deserialize(deserializer)?;
    Ok(specs
        .into_iter()
        .map(SortFieldSpec::into_sort_field)
        .collect())
}
