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

/// Per-node bound row types produced by `bind_schema`.
///
/// Keyed by node name (matching `CompileArtifacts.typed`). The
/// `next_tail_var` counter allocates fresh `TailVarId` values; IDs
/// are only comparable within a single `BoundSchemas` instance (i.e.
/// within one `compile()` run).
#[derive(Debug, Clone, Default)]
pub struct BoundSchemas {
    /// Output row for each node, keyed by node name.
    output: HashMap<String, Row>,
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

    /// Store the output row for a node.
    pub fn set_output(&mut self, node_name: String, row: Row) {
        self.output.insert(node_name, row);
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

    /// Materialize an `Arc<Schema>` for a node's bound output row.
    ///
    /// Every transform/source/aggregate/route node in the DAG has an
    /// entry; records produced at the node's output carry this schema
    /// on `Record::schema` and are positionally aligned to it.
    pub fn schema_of(&self, node_name: &str) -> Option<Arc<Schema>> {
        self.columns_of(node_name).map(|cols| {
            let boxed: Vec<Box<str>> = cols.into_iter().map(|c| c.into_boxed_str()).collect();
            Arc::new(Schema::new(boxed))
        })
    }
}
