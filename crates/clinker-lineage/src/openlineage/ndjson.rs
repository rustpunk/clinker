//! Newline-delimited JSON serialization of OpenLineage run events.

use std::io::{self, Write};

use super::RunEvent;

/// Writes `events` as newline-delimited JSON: one compact event per line, each
/// terminated by `\n`. This is the conventional OpenLineage file/console
/// transport for a finite batch run.
///
/// Each event is serialized fully in memory before any byte reaches `writer`, so
/// a serialization failure cannot leave a partial line on the sink. The writer is
/// flushed before returning, so a buffered sink's truncated tail surfaces as an
/// error rather than being lost when the writer is dropped.
///
/// # Errors
///
/// Returns any I/O error from writing to or flushing `writer`, or an
/// [`io::ErrorKind::Other`] error if an event cannot be serialized.
pub fn write_ndjson<W: Write>(events: &[RunEvent], mut writer: W) -> io::Result<()> {
    for event in events {
        // `to_vec` is compact (no pretty-printing), so each event is one line; serializing
        // to an owned buffer first keeps a serde failure from emitting a partial line.
        let mut line = serde_json::to_vec(event).map_err(io::Error::other)?;
        line.push(b'\n');
        writer.write_all(&line)?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openlineage::{EventType, Job, Run};

    fn minimal(event_type: EventType) -> RunEvent {
        RunEvent {
            event_time: "2020-02-22T22:42:42Z".to_string(),
            producer: "https://example/producer".to_string(),
            schema_url: "https://example/schema".to_string(),
            event_type,
            run: Run {
                run_id: "0190b7e0-0000-7000-8000-000000000000".to_string(),
            },
            job: Job {
                namespace: "clinker".to_string(),
                name: "orders".to_string(),
            },
            inputs: vec![],
            outputs: vec![],
        }
    }

    #[test]
    fn writes_each_event_in_order_one_per_line() {
        let events = vec![minimal(EventType::Start), minimal(EventType::Complete)];
        let mut buf = Vec::new();
        write_ndjson(&events, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();

        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line is a standalone compact JSON object, emitted in input order.
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(first["eventType"], "START");
        assert_eq!(second["eventType"], "COMPLETE");
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn emits_nothing_for_empty_slice() {
        let mut buf = Vec::new();
        write_ndjson(&[], &mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
