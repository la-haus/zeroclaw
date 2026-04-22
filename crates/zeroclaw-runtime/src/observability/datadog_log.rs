//! Structured JSON log observer for Datadog.
//!
//! Emits one JSON line per lifecycle event to stdout, matching the La Haus
//! `lahaus-logger` Python schema so Datadog Log Management can parse, index,
//! and correlate logs with APM traces without extra pipelines.
//!
//! When compiled with `observability-otel`, logs include `dd.trace_id` and
//! `dd.span_id` extracted from the active OTEL span context — enabling
//! automatic log↔trace correlation in Datadog.
//!
//! Metrics are a no-op — they go through the OTEL pipeline instead.

use super::traits::{Observer, ObserverEvent, ObserverMetric};
use serde_json::json;
use std::any::Any;

/// Observer that writes structured JSON logs to stdout for Datadog ingestion.
pub struct DatadogLogObserver {
    service: String,
    version: String,
    env: String,
}

impl Default for DatadogLogObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl DatadogLogObserver {
    /// Creates a new observer, reading DD_SERVICE, DD_VERSION, DD_ENV from env.
    pub fn new() -> Self {
        Self {
            service: std::env::var("DD_SERVICE").unwrap_or_else(|_| "zeroclaw".into()),
            version: std::env::var("DD_VERSION").unwrap_or_else(|_| "unknown".into()),
            env: std::env::var("DD_ENV").unwrap_or_else(|_| "development".into()),
        }
    }

    /// Get current OTEL trace_id and span_id for Datadog correlation.
    /// Returns (trace_id_64bit, span_id) as strings, or ("0", "0") if unavailable.
    fn current_trace_context(&self) -> (String, String) {
        #[cfg(feature = "observability-otel")]
        {
            use opentelemetry::trace::TraceContextExt;
            let ctx = opentelemetry::Context::current();
            let span = ctx.span();
            let sc = span.span_context();
            if sc.is_valid() {
                // Datadog uses 64-bit trace IDs: take lower 64 bits of 128-bit OTEL trace ID
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
        println!("{}", line);
    }
}

impl Observer for DatadogLogObserver {
    fn record_event(&self, event: &ObserverEvent) {
        match event {
            ObserverEvent::AgentStart {
                provider,
                model,
                user_id,
            } => {
                let mut attrs = json!({
                    "provider": provider,
                    "model": model,
                });
                if let Some(uid) = user_id {
                    attrs["user_id"] = json!(uid);
                }
                self.emit("info", "agent.start", attrs);
            }

            ObserverEvent::LlmRequest {
                provider,
                model,
                messages_count,
                ..
            } => {
                self.emit(
                    "info",
                    "llm.call.start",
                    json!({
                        "provider": provider,
                        "model": model,
                        "messages_count": messages_count,
                    }),
                );
            }

            ObserverEvent::LlmResponse {
                provider,
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
                    "provider": provider,
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
                provider,
                model,
                duration,
                tokens_used,
                cost_usd,
            } => {
                let mut attrs = json!({
                    "provider": provider,
                    "model": model,
                    "duration_ms": duration.as_millis() as u64,
                });
                if let Some(tokens) = tokens_used {
                    attrs["total_tokens"] = json!(tokens);
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
                ..
            } => {
                let level = if *success { "info" } else { "error" };
                self.emit(
                    level,
                    "tool.call.complete",
                    json!({
                        "tool": tool,
                        "duration_ms": duration.as_millis() as u64,
                        "success": success,
                    }),
                );
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

            ObserverEvent::HandStarted { hand_name } => {
                self.emit(
                    "info",
                    "hand.start",
                    json!({
                        "hand": hand_name,
                    }),
                );
            }

            ObserverEvent::HandCompleted {
                hand_name,
                duration_ms,
                findings_count,
            } => {
                self.emit(
                    "info",
                    "hand.complete",
                    json!({
                        "hand": hand_name,
                        "duration_ms": duration_ms,
                        "findings_count": findings_count,
                    }),
                );
            }

            ObserverEvent::HandFailed {
                hand_name,
                error,
                duration_ms,
            } => {
                self.emit(
                    "error",
                    "hand.failed",
                    json!({
                        "hand": hand_name,
                        "error_message": error,
                        "duration_ms": duration_ms,
                    }),
                );
            }

            // Skip noisy/low-value events
            ObserverEvent::HeartbeatTick
            | ObserverEvent::TurnComplete
            | ObserverEvent::CacheHit { .. }
            | ObserverEvent::CacheMiss { .. }
            | ObserverEvent::DeploymentStarted { .. }
            | ObserverEvent::DeploymentCompleted { .. }
            | ObserverEvent::DeploymentFailed { .. }
            | ObserverEvent::RecoveryCompleted { .. } => {}
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
    use std::time::Duration;

    #[test]
    fn name_returns_datadog_log() {
        let obs = DatadogLogObserver::new();
        assert_eq!(obs.name(), "datadog-log");
    }

    #[test]
    fn all_events_without_panic() {
        let obs = DatadogLogObserver::new();
        obs.record_event(&ObserverEvent::AgentStart {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            user_id: None,
        });
        obs.record_event(&ObserverEvent::LlmRequest {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            messages_count: 5,
            prompt_content: None,
        });
        obs.record_event(&ObserverEvent::LlmResponse {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            duration: Duration::from_millis(1500),
            success: true,
            error_message: None,
            input_tokens: Some(1000),
            output_tokens: Some(200),
            response_content: None,
        });
        obs.record_event(&ObserverEvent::ToolCallStart {
            tool: "shell".into(),
            arguments: None,
        });
        obs.record_event(&ObserverEvent::ToolCall {
            tool: "shell".into(),
            duration: Duration::from_millis(50),
            success: true,
            output: None,
        });
        obs.record_event(&ObserverEvent::Error {
            component: "provider".into(),
            message: "rate limited".into(),
        });
        obs.record_event(&ObserverEvent::AgentEnd {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            duration: Duration::from_secs(30),
            tokens_used: Some(5000),
            cost_usd: Some(0.015),
        });
        obs.record_event(&ObserverEvent::HeartbeatTick);
    }

    #[test]
    fn flush_is_noop() {
        let obs = DatadogLogObserver::new();
        obs.flush();
    }
}
