//! Plan-time gate on a source's `record_path` option (E363).
//!
//! The XML and JSON readers reject a malformed `record_path` at construction,
//! which closes the hole for every caller that builds a reader config directly.
//! This gate is what gives a pipeline author the diagnostic instead: a code, a
//! source span, and a corrected path, reported before any input is opened —
//! rather than an unspanned format error from the middle of a run.

use clinker_format::{RecordPath, RecordPathSyntax};

use crate::config::InputFormat;
use crate::config::multi_value::NodeFault;
use crate::config::pipeline_node::PipelineNode;
use crate::yaml::Spanned;

/// Every source whose `record_path` is not a path in its format's grammar.
///
/// Takes a node LIST rather than the pipeline, for the same reason the
/// multi-value gates do: a composition body's nodes need the identical check
/// and never appear in the call-site pipeline's `nodes:`.
pub fn record_path_faults(nodes: &[Spanned<PipelineNode>]) -> Vec<NodeFault> {
    let mut faults = Vec::new();
    for (node_index, spanned) in nodes.iter().enumerate() {
        let PipelineNode::Source {
            header,
            config: body,
        } = &spanned.value
        else {
            continue;
        };
        let declared = match &body.source.format {
            InputFormat::Json(opts) => opts
                .as_ref()
                .and_then(|o| o.record_path.as_deref())
                .map(|p| (RecordPathSyntax::Json, p)),
            InputFormat::Xml(opts) => opts
                .as_ref()
                .and_then(|o| o.record_path.as_deref())
                .map(|p| (RecordPathSyntax::Xml, p)),
            _ => None,
        };
        let Some((syntax, raw)) = declared else {
            continue;
        };
        if let Err(e) = RecordPath::parse(syntax, raw) {
            faults.push(NodeFault {
                node_index,
                code: "E363",
                message: format!("source '{}': {e}", header.name),
                help: e.help(),
            });
        }
    }
    faults
}
