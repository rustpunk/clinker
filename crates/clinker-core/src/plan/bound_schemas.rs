//! Per-node row type persistence (LD-16c-23).
//!
//! `BoundSchemas` stores the `Row` type bound at each node's output
//! during the `bind_schema` pass. It is carried on `CompileArtifacts`
//! and persisted on `CompiledPlan` so that downstream phases (16c.2+)
//! never need to recompute schema propagation.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::schema::Schema;
use cxl::typecheck::row::{Row, TailVarId};

/// Per-node bound row types and canonical output schemas produced by
/// `bind_schema`.
///
/// Keyed by node name (matching `CompileArtifacts.typed`). The
/// `next_tail_var` counter allocates fresh `TailVarId` values; IDs
/// are only comparable within a single `BoundSchemas` instance (i.e.
/// within one `compile()` run).
///
/// **Option-W single-oracle invariant:** `schemas[name]` is the one and
/// only `Arc<Schema>` for records produced at `name`'s output. The
/// CXL `TypedProgram::output_layout` for that node holds the same Arc;
/// the executor's walker asserts `Arc::ptr_eq` between them at dispatch.
#[derive(Debug, Clone, Default)]
pub struct BoundSchemas {
    /// Output row for each node, keyed by node name.
    output: HashMap<String, Row>,
    /// Canonical output `Arc<Schema>` for each node, keyed by node name.
    /// Built once when `set_output` is called — the same Arc is shared
    /// with every `OutputLayout` and every record produced at this node.
    schemas: HashMap<String, Arc<Schema>>,
    /// Monotonic counter for fresh tail variable IDs.
    next_tail_var: u32,
}

impl BoundSchemas {
    /// Allocate a fresh tail variable ID.
    pub fn fresh_tail(&mut self) -> TailVarId {
        let id = TailVarId(self.next_tail_var);
        self.next_tail_var += 1;
        id
    }

    /// Store the output row for a node. Builds the canonical `Arc<Schema>`
    /// from the row's declared columns when the caller has no pre-built
    /// one. Most call sites within `bind_schema` use
    /// [`Self::set_output_with_schema`] to pass in the same Arc used by
    /// the node's `OutputLayout` — maintaining Arc identity end-to-end.
    pub fn set_output(&mut self, node_name: String, row: Row) {
        let cols: Vec<Box<str>> = row
            .declared
            .keys()
            .map(|k| Box::<str>::from(k.as_str()))
            .collect();
        let schema = Arc::new(Schema::new(cols));
        self.output.insert(node_name.clone(), row);
        self.schemas.insert(node_name, schema);
    }

    /// Store the output row for a node together with its canonical
    /// `Arc<Schema>`. Preferred entry point — preserves Arc identity
    /// between `TypedProgram::output_layout.schema` and
    /// `BoundSchemas::schema_of(name)`, so the executor's
    /// `Arc::ptr_eq` drift guardrail (Failure mode #5) holds.
    pub fn set_output_with_schema(&mut self, node_name: String, row: Row, schema: Arc<Schema>) {
        debug_assert_eq!(
            row.declared.len(),
            schema.column_count(),
            "set_output_with_schema: row.declared.len() != schema.column_count() for {node_name}"
        );
        self.output.insert(node_name.clone(), row);
        self.schemas.insert(node_name, schema);
    }

    /// Retrieve the output row for a node by name.
    pub fn output_of(&self, node_name: &str) -> Option<&Row> {
        self.output.get(node_name)
    }

    /// Return the declared column names (in declaration order) of a
    /// node's bound output row, or `None` if the node has no recorded
    /// output row.
    ///
    /// This is the Option-W positional slot table: the column ordering
    /// here is the runtime `Record::values` layout contract for records
    /// produced by `node_name`.
    pub fn columns_of(&self, node_name: &str) -> Option<Vec<String>> {
        self.output
            .get(node_name)
            .map(|row| row.declared.keys().cloned().collect())
    }

    /// Iterate `(node_name, row)` pairs for every recorded output.
    pub fn iter_outputs(&self) -> impl Iterator<Item = (&String, &Row)> {
        self.output.iter()
    }

    /// Return the canonical `Arc<Schema>` for a node's bound output row.
    ///
    /// Every transform/source/aggregate/route/merge node has an entry;
    /// records produced at the node's output carry this exact `Arc` on
    /// `Record::schema`. Callers may `Arc::clone` it — reference identity
    /// is the executor's drift guardrail (Failure mode #5).
    pub fn schema_of(&self, node_name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(node_name).cloned()
    }
}
