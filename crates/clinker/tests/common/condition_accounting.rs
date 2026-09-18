//! Independent accounting oracle for the concurrent authored-condition fixture.

use std::collections::{BTreeMap, BTreeSet};

use clinker_exec::telemetry::SpanName;
use serde_json::Value;

fn require(condition: bool, detail: impl Into<String>) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(detail.into())
    }
}

fn equal(label: &str, actual: u64, expected: u64) -> Result<(), String> {
    require(
        actual == expected,
        format!("{label}: actual {actual}, expected {expected}"),
    )
}

fn count(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .ok_or_else(|| format!("expected unsigned count, got {value}"))
}

fn array(value: &Value) -> Result<&[Value], String> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("expected array, got {value}"))
}

fn string(value: &Value) -> Result<&str, String> {
    value
        .as_str()
        .ok_or_else(|| format!("expected string, got {value}"))
}

fn add(left: u64, right: u64) -> Result<u64, String> {
    left.checked_add(right)
        .ok_or_else(|| "count overflow".to_owned())
}

fn subtract(left: u64, right: u64) -> Result<u64, String> {
    left.checked_sub(right)
        .ok_or_else(|| format!("impossible negative count: {left} - {right}"))
}

fn span_family(span: SpanName) -> (&'static str, &'static [&'static str]) {
    const TERMINALS: &[&str] = &["completed", "failed", "interrupted"];
    match span {
        SpanName::Transform => ("clinker.transform", &["completed"]),
        SpanName::CredentialResolve => ("clinker.credential.resolve", TERMINALS),
        SpanName::ResourceOpen => ("clinker.resource.open", TERMINALS),
        SpanName::CredentialRenew => ("clinker.credential.renew", TERMINALS),
        SpanName::CredentialRevoke => ("clinker.credential.revoke", TERMINALS),
        SpanName::Source => ("clinker.source", TERMINALS),
        SpanName::Guess => (
            "clinker.guess",
            &["completed", "unresolved", "failed", "interrupted"],
        ),
        SpanName::Sink => ("clinker.sink", TERMINALS),
        SpanName::WriterAdmission => ("clinker.writer.admission", TERMINALS),
        SpanName::WriterStage => (
            "clinker.writer.stage",
            &["completed", "failed", "interrupted", "dropped"],
        ),
        SpanName::WriterSpill => ("clinker.writer.spill", TERMINALS),
        SpanName::WriterCleanup => ("clinker.writer.cleanup", TERMINALS),
    }
}

#[derive(Default)]
struct Captured {
    customers: BTreeSet<String>,
    spans: BTreeMap<String, u64>,
    metrics: BTreeMap<String, u64>,
    metric_points: u64,
    requests: BTreeMap<String, u64>,
}

impl Captured {
    fn log(&mut self, log: &Value) -> Result<(), String> {
        require(
            log["body"]["stringValue"] == "customer processed",
            "authored literal log body changed",
        )?;
        let mut fields = BTreeMap::new();
        for attribute in array(&log["attributes"])? {
            let key = string(&attribute["key"])?;
            let value = string(&attribute["value"]["stringValue"])?;
            require(
                fields.insert(key, value).is_none(),
                format!("duplicate log attribute: {key}"),
            )?;
        }
        require(
            fields.get("clinker.event") == Some(&"transform.customer_seen"),
            "unexpected authored event",
        )?;
        require(
            !fields.contains_key("amount"),
            "condition-only amount was exported",
        )?;
        require(
            fields.keys().copied().collect::<BTreeSet<_>>()
                == BTreeSet::from([
                    "clinker.event",
                    "clinker.execution_id",
                    "clinker.batch_id",
                    "clinker.pipeline_name",
                    "customer_id",
                ]),
            "unexpected/missing log attributes",
        )?;
        let customer = fields["customer_id"];
        require(
            matches!(customer, "customer-2" | "customer-4"),
            format!("excluded customer exported: {customer}"),
        )?;
        require(
            self.customers.insert(customer.to_owned()),
            format!("duplicate customer event: {customer}"),
        )
    }

    fn metric(&mut self, metric: &Value) -> Result<(), String> {
        let name = string(&metric["name"])?;
        require(
            metric["sum"]["aggregationTemporality"] == 1,
            format!("non-DELTA metric: {name}"),
        )?;
        require(
            metric["sum"]["isMonotonic"] == true,
            format!("non-monotonic metric: {name}"),
        )?;
        for point in array(&metric["sum"]["dataPoints"])? {
            let delta = string(&point["asInt"])?
                .parse::<u64>()
                .map_err(|error| format!("invalid metric {name}: {error}"))?;
            let total = self.metrics.entry(name.to_owned()).or_default();
            *total = add(*total, delta)?;
            self.metric_points = add(self.metric_points, 1)?;
        }
        Ok(())
    }

    fn read(entries: &[Value]) -> Result<Self, String> {
        let mut captured = Self::default();
        for entry in entries {
            require(
                entry["authentication"] == "none",
                "unexpected capture authentication",
            )?;
            let signal = string(&entry["signal"])?;
            let (resources, scopes, records) = match signal {
                "logs" => ("resourceLogs", "scopeLogs", "logRecords"),
                "metrics" => ("resourceMetrics", "scopeMetrics", "metrics"),
                "traces" => ("resourceSpans", "scopeSpans", "spans"),
                other => return Err(format!("unknown signal: {other}")),
            };
            let requests = captured.requests.entry(signal.to_owned()).or_default();
            *requests = add(*requests, 1)?;
            for resource in array(&entry["payload"][resources])? {
                for scope in array(&resource[scopes])? {
                    for record in array(&scope[records])? {
                        match signal {
                            "logs" => captured.log(record)?,
                            "metrics" => captured.metric(record)?,
                            "traces" => {
                                let name = string(&record["name"])?;
                                require(
                                    name == "clinker.run"
                                        || SpanName::ALL
                                            .into_iter()
                                            .any(|span| span_family(span).0 == name),
                                    format!("unknown span: {name}"),
                                )?;
                                let total = captured.spans.entry(name.to_owned()).or_default();
                                *total = add(*total, 1)?;
                            }
                            _ => unreachable!("signal validated above"),
                        }
                    }
                }
            }
        }
        Ok(captured)
    }
}

pub(super) fn reconcile(terminal: &Value, entries: &[Value]) -> Result<(), String> {
    require(
        terminal["event"] == "completed" && terminal["result"] == "success",
        "unsuccessful machine terminal",
    )?;
    let summary = &terminal["observability"];
    let admission = &summary["admission"];
    require(summary["flush_complete"] == true, "incomplete flush")?;
    require(
        admission["counts_complete"] == true,
        "incomplete admission counters",
    )?;
    for key in ["retained_bytes", "arena_recoveries"] {
        equal(key, count(&admission[key])?, 0)?;
    }
    equal(
        "arena capacity",
        count(&admission["capacity_bytes"])?,
        64_000,
    )?;
    for lane in ["ordinary", "high_severity"] {
        equal(
            "lane retained bytes",
            count(&admission["lanes"][lane]["retained_bytes"])?,
            0,
        )?;
    }
    for field in ["missing", "denied", "truncated", "limit_dropped"] {
        equal(field, count(&admission["fields"][field])?, 0)?;
    }
    let dropped = &admission["dropped"];
    for reason in [
        "sampled",
        "rate_limited",
        "oversize",
        "invalid_identity",
        "undecodable",
    ] {
        equal(reason, count(&dropped[reason])?, 0)?;
    }
    // Undecodable slots were already accepted; forbid that loss above rather
    // than counting it twice as a refusal here.
    let refusals = add(
        count(&dropped["contended"])?,
        count(&dropped["queue_full"])?,
    )?;
    let captured = Captured::read(entries)?;
    let metric = |name: &str| captured.metrics.get(name).copied().unwrap_or(0);
    for name in [
        "clinker.source.started",
        "clinker.transform.started",
        "clinker.sink.started",
    ] {
        equal(name, metric(name), 1)?;
    }
    equal("processed records", metric("clinker.transform.records"), 4)?;
    let mut started = 0;
    let mut traces = 0;
    // Fixed counters are independent of arena admission. Count every work
    // family's attempts, including spans that never reached the collector.
    for span in SpanName::ALL {
        let (name, terminals) = span_family(span);
        let starts = metric(&format!("{name}.started"));
        let mut finishes = 0;
        for terminal in terminals {
            finishes = add(finishes, metric(&format!("{name}.{terminal}")))?;
        }
        equal(name, starts, finishes)?;
        let delivered = captured.spans.get(name).copied().unwrap_or(0);
        require(
            delivered <= starts,
            format!("more spans than work starts: {name}"),
        )?;
        started = add(started, starts)?;
        traces = add(traces, delivered)?;
    }
    equal(
        "lifecycle span outside arena",
        captured.spans.get("clinker.run").copied().unwrap_or(0),
        1,
    )?;
    let logs = u64::try_from(captured.customers.len()).map_err(|error| error.to_string())?;
    for (signal, items) in [
        ("logs", logs),
        ("metrics", captured.metric_points),
        ("traces", add(traces, 1)?),
    ] {
        let delivery = &summary[signal];
        equal(
            &format!("{signal} failures"),
            count(&delivery["failures"])?,
            0,
        )?;
        equal(
            &format!("{signal} rejected"),
            count(&delivery["rejected"])?,
            0,
        )?;
        equal(
            &format!("{signal} accepted"),
            count(&delivery["accepted"])?,
            items,
        )?;
        equal(
            &format!("{signal} requests"),
            count(&delivery["attempts"])?,
            captured.requests.get(signal).copied().unwrap_or(0),
        )?;
    }
    let accepted = count(&admission["accepted"])?;
    equal("A = L + T", accepted, add(logs, traces)?)?;
    equal(
        "A + D = S + 2 eligible attempts",
        add(accepted, refusals)?,
        add(started, 2)?,
    )?;
    equal(
        "missing logs = refusals minus missing spans",
        subtract(2, logs)?,
        subtract(refusals, subtract(started, traces)?)?,
    )
}
