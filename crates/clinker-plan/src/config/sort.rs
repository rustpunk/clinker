//! Record-level sort specifications: the authored sort field every ordering
//! surface parses, and the placement-only form an ordering-only field is
//! validated into.

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
    /// A Cull or Reshape `order_by`: orders the rows of one group.
    GroupOrderBy,
    /// A Source `sort_order`: the order its records are verified against.
    SourceSortOrder,
    /// A Transform `analytic_window.sort_by`: orders one window partition.
    WindowSortBy,
}

/// An authored `null_order: drop` on a field that only orders rows.
///
/// `Display` is the author-facing message for the site: the rule, the
/// reason, and how to remove null-keyed rows instead. Callers prefix the
/// node (`cull "name": `, `source "name": `) and add nothing else, so this
/// is the one place the wording lives.
///
/// The fix is a paste-able `filter` only when CXL can write the field as a
/// bare name ([`cxl::lexer::is_bare_field_name`]). Any other name gets no
/// CXL at all: a name with a space or a keyword would not parse, and a
/// flattened `Address.City` would parse as a path to another value, so the
/// pasted filter would silently drop every row. Those fields are sent to the
/// Source schema's `source_name` rename, which exposes the column under an
/// identifier a filter can then name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropNotAllowed {
    pub field: String,
    pub site: OrderingSite,
}

impl std::fmt::Display for DropNotAllowed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field = &self.field;
        let (key, reason, filter_at) = match self.site {
            OrderingSite::GroupOrderBy => (
                "order_by",
                "`order_by` only orders rows within a group and cannot remove them",
                "before this node",
            ),
            OrderingSite::SourceSortOrder => (
                "sort_order",
                "source verification cannot discard records",
                "after this source",
            ),
            OrderingSite::WindowSortBy => (
                "analytic_window.sort_by",
                "`sort_by` only orders rows within a window partition and cannot remove them",
                "before this node",
            ),
        };
        write!(
            f,
            "`null_order: drop` is not allowed on `{key}` for field '{field}': {reason}. Use \
             `null_order: first` or `null_order: last`"
        )?;
        if cxl::lexer::is_bare_field_name(field) {
            write!(
                f,
                "; to exclude rows whose '{field}' is null, add a Transform {filter_at} with \
                 `filter not {field}.is_null()`."
            )
        } else {
            write!(
                f,
                ". CXL cannot name the field '{field}': a CXL field name is one identifier of \
                 ASCII letters, digits and `_`, not starting with a digit and not a CXL keyword. \
                 To exclude rows whose '{field}' is null, rename the column to such a name in its \
                 Source schema entry and keep reading the input column through `source_name` \
                 (for example `{{ name: order_id, type: string, source_name: \"order id\" }}`), \
                 then filter on the new name in a Transform {filter_at}."
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
/// `#[serde(deserialize_with)]` so every authored ordering list takes both.
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
