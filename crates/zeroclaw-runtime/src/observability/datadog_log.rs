//! Structured JSON log observer for Datadog.
//!
//! Emits one JSON line per lifecycle event to stdout, matching the La Haus
//! `lahaus-logger` Python schema so Datadog Log Management can parse, index,
//! and correlate logs with APM traces without extra pipelines.
//!
//! Correlation: every line carries `dd.trace_id`/`dd.span_id` read from the
//! [`SharedTraceContext`] written by the [`super::otel::OtelObserver`] on
//! `AgentStart`. We deliberately do *not* rely on the global OpenTelemetry
//! context because the OtelObserver is instance-scoped and never registers its
//! spans as the global current span. When compiled without the
//! `observability-otel` feature (or when no shared context is attached) the
//! correlation ids fall back to `"0"`.
//!
//! Metrics are a no-op — they flow through the OTEL pipeline instead.

use super::SharedTraceContext;
use super::traits::{Observer, ObserverEvent, ObserverMetric};
use serde_json::json;
use std::any::Any;

/// Observer that writes structured JSON logs to stdout for Datadog ingestion.
pub struct DatadogLogObserver {
    service: String,
    version: String,
    env: String,
    /// Shared trace context written by `OtelObserver`, giving us the active
    /// `(trace_id_64, span_id)` without relying on the global OpenTelemetry
    /// context.
    trace_context: Option<SharedTraceContext>,
}

impl Default for DatadogLogObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl DatadogLogObserver {
    /// Creates a new observer, reading `DD_SERVICE`, `DD_VERSION`, `DD_ENV`
    /// from the environment (Datadog Unified Service Tagging).
    pub fn new() -> Self {
        Self {
            service: std::env::var("DD_SERVICE").unwrap_or_else(|_| "zeroclaw".into()),
            version: std::env::var("DD_VERSION").unwrap_or_else(|_| "unknown".into()),
            env: std::env::var("DD_ENV").unwrap_or_else(|_| "development".into()),
            trace_context: None,
        }
    }

    /// Attach a shared trace context for log↔trace correlation. When set,
    /// [`current_trace_context`](Self::current_trace_context) reads the
    /// `(trace_id, span_id)` written by `OtelObserver`.
    #[must_use]
    pub fn with_trace_context(mut self, ctx: SharedTraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// Current OTEL `(trace_id_64bit, span_id)` for Datadog correlation, as
    /// decimal strings. Returns `("0", "0")` when unavailable.
    fn current_trace_context(&self) -> (String, String) {
        // Prefer the shared trace context written by OtelObserver.
        if let Some(ref tc) = self.trace_context {
            let pair = tc.lock();
            if pair.0 != "0" {
                return pair.clone();
            }
        }

        // Fallback: read from the global OpenTelemetry context. Usually "0"
        // because the instance-scoped OtelObserver does not register spans
        // globally, but kept for standalone use without a shared context.
        #[cfg(feature = "observability-otel")]
        {
            use opentelemetry::trace::TraceContextExt;
            let ctx = opentelemetry::Context::current();
            let span = ctx.span();
            let sc = span.span_context();
            if sc.is_valid() {
                // Datadog uses 64-bit trace IDs: take the lower 64 bits of the
                // 128-bit OTEL trace ID.
                let trace_id_128 = u128::from_be_bytes(sc.trace_id().to_bytes());
                let trace_id_64 = (trace_id_128 & ((1u128 << 64) - 1)) as u64;
                let span_id = u64::from_be_bytes(sc.span_id().to_bytes());
                return (trace_id_64.to_string(), span_id.to_string());
            }
        }
        ("0".to_string(), "0".to_string())
    }

    fn emit(&self, level: &str, msg: &str, attributes: serde_json::Value) {
        let (trace_id, span_id) = self.current_trace_context();
        let line = json!({
            "time": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "level": level,
            "service": &self.service,
            "msg": msg,
            "dd.service": &self.service,
            "dd.version": &self.version,
            "dd.env": &self.env,
            "dd.trace_id": trace_id,
            "dd.span_id": span_id,
            "attributes": attributes,
        });
        println!("{line}");
    }
}

impl Observer for DatadogLogObserver {
    fn record_event(&self, event: &ObserverEvent) {
        match event {
            ObserverEvent::AgentStart {
                model_provider,
                model,
                channel,
                agent_alias,
                turn_id,
                user_id,
                session_id,
                message_id,
            } => {
                let mut attrs = json!({
                    "provider": model_provider,
                    "model": model,
                });
                if let Some(uid) = user_id {
                    attrs["user_id"] = json!(uid);
                }
                if let Some(sid) = session_id {
                    attrs["session_id"] = json!(sid);
                }
                if let Some(mid) = message_id {
                    attrs["message_id"] = json!(mid);
                }
                if let Some(ch) = channel {
                    attrs["channel"] = json!(ch);
                }
                if let Some(alias) = agent_alias {
                    attrs["agent_alias"] = json!(alias);
                }
                if let Some(tid) = turn_id {
                    attrs["turn_id"] = json!(tid);
                }
                self.emit("info", "agent.start", attrs);
            }

            ObserverEvent::LlmRequest {
                model_provider,
                model,
                messages_count,
                ..
            } => {
                self.emit(
                    "info",
                    "llm.call.start",
                    json!({
                        "provider": model_provider,
                        "model": model,
                        "messages_count": messages_count,
                    }),
                );
            }

            ObserverEvent::LlmResponse {
                model_provider,
                model,
                duration,
                success,
                error_message,
                input_tokens,
                output_tokens,
                ..
            } => {
                let level = if *success { "info" } else { "error" };
                let mut attrs = json!({
                    "provider": model_provider,
                    "model": model,
                    "duration_ms": duration.as_millis() as u64,
                    "success": success,
                });
                if let Some(input) = input_tokens {
                    attrs["input_tokens"] = json!(input);
                }
                if let Some(output) = output_tokens {
                    attrs["output_tokens"] = json!(output);
                }
                if let (Some(input), Some(output)) = (input_tokens, output_tokens) {
                    attrs["total_tokens"] = json!(input + output);
                }
                if let Some(err) = error_message {
                    attrs["error_message"] = json!(err);
                }
                self.emit(level, "llm.call.complete", attrs);
            }

            ObserverEvent::AgentEnd {
                model_provider,
                model,
                duration,
                tokens_used,
                cost_usd,
                ..
            } => {
                let mut attrs = json!({
                    "provider": model_provider,
                    "model": model,
                    "duration_ms": duration.as_millis() as u64,
                });
                if let Some(tokens) = tokens_used {
                    attrs["input_tokens"] = json!(tokens.input_tokens);
                    attrs["output_tokens"] = json!(tokens.output_tokens);
                    attrs["total_tokens"] = json!(tokens.input_tokens + tokens.output_tokens);
                }
                if let Some(cost) = cost_usd {
                    attrs["cost_usd"] = json!(cost);
                }
                self.emit("info", "agent.end", attrs);
            }

            ObserverEvent::ToolCallStart { tool, .. } => {
                self.emit(
                    "info",
                    "tool.call.start",
                    json!({
                        "tool": tool,
                    }),
                );
            }

            ObserverEvent::ToolCall {
                tool,
                duration,
                success,
                result,
                ..
            } => {
                let level = if *success { "info" } else { "error" };
                let mut attrs = json!({
                    "tool": tool,
                    "duration_ms": duration.as_millis() as u64,
                    "success": success,
                });
                // On failure, carry the scrubbed tool output as the error reason.
                if !*success && let Some(res) = result {
                    attrs["error_message"] = json!(res);
                }
                self.emit(level, "tool.call.complete", attrs);
            }

            ObserverEvent::Error { component, message } => {
                self.emit(
                    "error",
                    "agent.error",
                    json!({
                        "component": component,
                        "error_message": message,
                    }),
                );
            }

            ObserverEvent::ChannelMessage { channel, direction } => {
                self.emit(
                    "debug",
                    "channel.message",
                    json!({
                        "channel": channel,
                        "direction": direction,
                    }),
                );
            }

            // Skip noisy/low-value events and any future (`#[non_exhaustive]`)
            // variants added by upstream `zeroclaw-api`.
            _ => {}
        }
    }

    fn record_metric(&self, _metric: &ObserverMetric) {
        // Metrics go through OTEL, not structured logs.
    }

    fn name(&self) -> &str {
        "datadog-log"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::traits::TurnTokenUsage;
    use std::time::Duration;

    fn agent_start() -> ObserverEvent {
        ObserverEvent::AgentStart {
            model_provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            channel: Some("wss".into()),
            agent_alias: Some("default".into()),
            turn_id: Some("turn-1".into()),
            user_id: Some("user-42".into()),
            session_id: Some("session-7".into()),
            message_id: Some("msg-9".into()),
        }
    }

    #[test]
    fn name_returns_datadog_log() {
        let obs = DatadogLogObserver::new();
        assert_eq!(obs.name(), "datadog-log");
    }

    #[test]
    fn all_events_without_panic() {
        let obs = DatadogLogObserver::new();
        obs.record_event(&agent_start());
        obs.record_event(&ObserverEvent::LlmRequest {
            model_provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            messages_count: 5,
            channel: None,
            agent_alias: None,
            turn_id: None,
            prompt_content: None,
        });
        obs.record_event(&ObserverEvent::LlmResponse {
            model_provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            duration: Duration::from_millis(1500),
            success: true,
            error_message: None,
            input_tokens: Some(1000),
            output_tokens: Some(200),
            channel: None,
            agent_alias: None,
            turn_id: None,
            response_content: None,
        });
        obs.record_event(&ObserverEvent::ToolCall {
            tool: "shell".into(),
            tool_call_id: Some("call_1".into()),
            duration: Duration::from_millis(50),
            success: false,
            arguments: Some(r#"{"command":"false"}"#.into()),
            result: Some("exit status 1".into()),
            channel: None,
            agent_alias: None,
            turn_id: None,
        });
        obs.record_event(&ObserverEvent::Error {
            component: "model_provider".into(),
            message: "rate limited".into(),
        });
        obs.record_event(&ObserverEvent::AgentEnd {
            model_provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            duration: Duration::from_secs(30),
            tokens_used: Some(TurnTokenUsage {
                input_tokens: 1000,
                output_tokens: 200,
            }),
            cost_usd: Some(0.015),
            channel: None,
            agent_alias: None,
            turn_id: None,
        });
        obs.record_event(&ObserverEvent::HeartbeatTick);
    }

    #[test]
    fn reads_trace_id_from_shared_context() {
        let ctx: SharedTraceContext =
            std::sync::Arc::new(parking_lot::Mutex::new(("12345".into(), "67890".into())));
        let obs = DatadogLogObserver::new().with_trace_context(ctx);
        let (trace_id, span_id) = obs.current_trace_context();
        assert_eq!(trace_id, "12345");
        assert_eq!(span_id, "67890");
    }

    #[test]
    fn unset_shared_context_falls_back_to_zero() {
        let ctx: SharedTraceContext =
            std::sync::Arc::new(parking_lot::Mutex::new(("0".into(), "0".into())));
        let obs = DatadogLogObserver::new().with_trace_context(ctx);
        // No OTEL global span in this unit test → "0".
        let (trace_id, span_id) = obs.current_trace_context();
        assert_eq!(trace_id, "0");
        assert_eq!(span_id, "0");
    }

    #[test]
    fn flush_is_noop() {
        let obs = DatadogLogObserver::new();
        obs.flush();
    }
}
