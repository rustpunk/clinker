//! Per-node row type persistence (LD-16c-23).
//!
//! `BoundSchemas` stores the `Row` type bound at each node's output
//! during the `bind_schema` pass. It is carried on `CompileArtifacts`
//! and persisted on `CompiledPlan` so that downstream phases (16c.2+)
//! never need to recompute schema propagation.

use std::collections::HashMap;

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
}
