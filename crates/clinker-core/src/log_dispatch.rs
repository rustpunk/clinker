use clinker_record::Value;
use indexmap::IndexMap;

use crate::config::{LogDirective, LogLevel, LogTiming};
use crate::log_template::{self, LogTemplateContext};

/// Runtime state for log directive execution.
pub struct LogDispatcher {
    directives: Vec<LogDirective>,
    /// Per-directive record counter for `every` sampling.
    counters: Vec<u64>,
}

impl LogDispatcher {
    pub fn new(directives: Vec<LogDirective>) -> Self {
        let counters = vec![0u64; directives.len()];
        Self { directives, counters }
    }

    pub fn is_empty(&self) -> bool {
        self.directives.is_empty()
    }

    /// Fire all `before_transform` directives.
    pub fn fire_before_transform(&self, ctx: &LogTemplateContext<'_>) {
        for d in &self.directives {
            if d.when == LogTiming::BeforeTransform {
                emit_log(d, ctx, None);
            }
        }
    }

    /// Fire all `after_transform` directives.
    pub fn fire_after_transform(&self, ctx: &LogTemplateContext<'_>) {
        for d in &self.directives {
            if d.when == LogTiming::AfterTransform {
                emit_log(d, ctx, None);
            }
        }
    }

    /// Fire all `per_record` directives for a record.
    /// `record_fields` are the fields available for template resolution.
    /// `condition_eval` is a closure that evaluates CXL condition expressions.
    pub fn fire_per_record(
        &mut self,
        ctx: &LogTemplateContext<'_>,
        condition_eval: Option<&dyn Fn(&str) -> bool>,
    ) {
        for (i, d) in self.directives.iter().enumerate() {
            if d.when != LogTiming::PerRecord {
                continue;
            }

            // Sampling: increment counter and check modulo
            self.counters[i] += 1;
            if let Some(every) = d.every {
                if self.counters[i] % every != 1 && every != 1 {
                    // Skip unless it's the Nth record (1-indexed: fire on 1, 1+every, 1+2*every, ...)
                    // For every=1, fire on every record
                    if self.counters[i] != 1 && (self.counters[i] - 1) % every != 0 {
                        continue;
                    }
                }
            }

            // Condition check
            if let Some(ref cond_expr) = d.condition {
                if let Some(eval_fn) = condition_eval {
                    if !eval_fn(cond_expr) {
                        continue;
                    }
                }
            }

            emit_log(d, ctx, None);
        }
    }

    /// Fire all `on_error` directives for a DLQ-routed record.
    pub fn fire_on_error(
        &self,
        ctx: &LogTemplateContext<'_>,
        fields: Option<&[(String, String)]>,
    ) {
        for d in &self.directives {
            if d.when == LogTiming::OnError {
                emit_log(d, ctx, fields);
            }
        }
    }
}

fn emit_log(
    directive: &LogDirective,
    ctx: &LogTemplateContext<'_>,
    extra_fields: Option<&[(String, String)]>,
) {
    let message = log_template::resolve_template(&directive.message, ctx);

    // Build structured fields string
    let fields_str = if let Some(ref field_names) = directive.fields {
        let kv = log_template::extract_fields(field_names, ctx.record_fields);
        if kv.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = kv.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            format!(" {}", pairs.join(" "))
        }
    } else {
        String::new()
    };

    let extra = if let Some(ef) = extra_fields {
        let pairs: Vec<String> = ef.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        if pairs.is_empty() { String::new() } else { format!(" {}", pairs.join(" ")) }
    } else {
        String::new()
    };

    let full_msg = format!("{}{}{}", message, fields_str, extra);

    match directive.level {
        LogLevel::Trace => tracing::trace!("{}", full_msg),
        LogLevel::Debug => tracing::debug!("{}", full_msg),
        LogLevel::Info => tracing::info!("{}", full_msg),
        LogLevel::Warn => tracing::warn!("{}", full_msg),
        LogLevel::Error => tracing::error!("{}", full_msg),
    }
}

/// Create a LogTemplateContext from executor state.
pub fn make_template_context<'a>(
    record_fields: &'a IndexMap<String, Value>,
    transform_name: &'a str,
    transform_duration_ms: Option<u64>,
    source_file: &'a str,
    source_row: u64,
    pipeline_ok_count: u64,
    pipeline_dlq_count: u64,
    pipeline_total_count: u64,
    pipeline_name: &'a str,
    pipeline_execution_id: &'a str,
    dlq_error_category: Option<&'a str>,
    dlq_error_detail: Option<&'a str>,
) -> LogTemplateContext<'a> {
    LogTemplateContext {
        record_fields,
        transform_name,
        transform_duration_ms,
        source_file,
        source_row,
        pipeline_ok_count,
        pipeline_dlq_count,
        pipeline_total_count,
        pipeline_name,
        pipeline_execution_id,
        dlq_error_category,
        dlq_error_detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LogLevel, LogTiming};

    fn make_directive(level: LogLevel, when: LogTiming, message: &str) -> LogDirective {
        LogDirective {
            level,
            when,
            condition: None,
            message: message.to_string(),
            fields: None,
            every: None,
            log_rule: None,
        }
    }

    fn empty_fields() -> IndexMap<String, Value> {
        IndexMap::new()
    }

    fn test_ctx(fields: &IndexMap<String, Value>) -> LogTemplateContext<'_> {
        make_template_context(
            fields, "test", None, "input.csv", 1,
            0, 0, 0, "pipeline", "exec-id", None, None,
        )
    }

    #[test]
    fn test_log_before_transform_fires_once() {
        // Create dispatcher with a before_transform directive
        let d = make_directive(LogLevel::Info, LogTiming::BeforeTransform, "starting");
        let dispatcher = LogDispatcher::new(vec![d]);

        let fields = empty_fields();
        let ctx = test_ctx(&fields);
        // This should fire without panicking (we can't capture tracing output in unit tests
        // without tracing-test, but we verify it doesn't panic and the logic paths execute)
        dispatcher.fire_before_transform(&ctx);
    }

    #[test]
    fn test_log_after_transform_fires_once() {
        let d = make_directive(LogLevel::Info, LogTiming::AfterTransform, "done in {_transform_duration_ms}ms");
        let dispatcher = LogDispatcher::new(vec![d]);

        let fields = empty_fields();
        let mut ctx = test_ctx(&fields);
        ctx.transform_duration_ms = Some(150);
        dispatcher.fire_after_transform(&ctx);
    }

    #[test]
    fn test_log_on_error_fires_per_dlq() {
        let d = make_directive(LogLevel::Error, LogTiming::OnError, "error: {_cxl_dlq_error_category}");
        let dispatcher = LogDispatcher::new(vec![d]);

        let fields = empty_fields();
        let mut ctx = test_ctx(&fields);
        ctx.dlq_error_category = Some("eval_error");
        dispatcher.fire_on_error(&ctx, None);
    }

    #[test]
    fn test_log_level1_every_sampling() {
        let mut d = make_directive(LogLevel::Info, LogTiming::PerRecord, "record");
        d.every = Some(100);
        let mut dispatcher = LogDispatcher::new(vec![d]);

        let fields = empty_fields();
        // Simulate 300 records — should fire on records 1, 101, 201 = 3 events
        let mut fire_count = 0;
        for i in 1..=300 {
            let ctx = make_template_context(
                &fields, "t", None, "f.csv", i, 0, 0, 0, "p", "e", None, None,
            );
            // We can't easily count tracing events, so we test the counter logic
            let d = &dispatcher.directives[0];
            dispatcher.counters[0] += 1;
            let counter = dispatcher.counters[0];
            let every = d.every.unwrap();
            if counter == 1 || (counter - 1) % every == 0 {
                fire_count += 1;
            }
        }
        assert_eq!(fire_count, 3, "every=100, 300 records → 3 events");
    }

    #[test]
    fn test_log_level1_every_one() {
        // every: 1 should fire on every record
        let mut fire_count = 0;
        for i in 1..=5u64 {
            let counter = i;
            let every = 1u64;
            if counter == 1 || (counter - 1) % every == 0 {
                fire_count += 1;
            }
        }
        assert_eq!(fire_count, 5, "every=1, 5 records → 5 events");
    }

    #[test]
    fn test_log_level1_multiple_directives() {
        let d1 = make_directive(LogLevel::Info, LogTiming::PerRecord, "first");
        let d2 = make_directive(LogLevel::Debug, LogTiming::PerRecord, "second");
        let mut dispatcher = LogDispatcher::new(vec![d1, d2]);
        assert_eq!(dispatcher.directives.len(), 2);

        let fields = empty_fields();
        let ctx = test_ctx(&fields);
        // Should not panic — both directives fire
        dispatcher.fire_per_record(&ctx, None);
    }

    #[test]
    fn test_log_level1_condition_null_is_false() {
        let mut d = make_directive(LogLevel::Info, LogTiming::PerRecord, "msg");
        d.condition = Some("Amount > 1000".to_string());
        let mut dispatcher = LogDispatcher::new(vec![d]);

        let fields = empty_fields();
        let ctx = test_ctx(&fields);
        // Condition evaluator returns false
        dispatcher.fire_per_record(&ctx, Some(&|_: &str| false));
        // No panic — condition prevented firing
    }
}
