pub mod datadog_log;
pub mod dora;
pub mod log;
pub mod multi;
pub mod noop;
#[cfg(feature = "observability-otel")]
pub mod otel;
#[cfg(feature = "observability-prometheus")]
pub mod prometheus;
pub mod runtime_trace;
pub mod traits;
pub mod verbose;

#[allow(unused_imports)]
pub use self::log::LogObserver;
#[allow(unused_imports)]
pub use self::multi::MultiObserver;
pub use datadog_log::DatadogLogObserver;
pub use noop::NoopObserver;
#[cfg(feature = "observability-otel")]
pub use otel::OtelObserver;
#[cfg(feature = "observability-prometheus")]
pub use prometheus::PrometheusObserver;
pub use traits::{Observer, ObserverEvent};
#[allow(unused_imports)]
pub use verbose::VerboseObserver;

use zeroclaw_config::schema::ObservabilityConfig;

/// Shared trace context for log↔trace correlation.
///
/// Holds `(trace_id_64bit, span_id)` as decimal strings.
/// Updated by `OtelObserver` on `AgentStart`/`AgentEnd` and read by
/// `DatadogLogObserver` so structured logs carry the correct `dd.trace_id`
/// and `dd.span_id` without relying on the global OpenTelemetry context
/// (which OtelObserver never registers spans into).
pub type SharedTraceContext = std::sync::Arc<parking_lot::Mutex<(String, String)>>;

/// Factory: create the right observer from config
pub fn create_observer(config: &ObservabilityConfig) -> Box<dyn Observer> {
    match config.backend.as_str() {
        "log" => Box::new(LogObserver::new()),
        "verbose" => Box::new(VerboseObserver::new()),
        "datadog-log" => Box::new(DatadogLogObserver::new()),
        "prometheus" => {
            #[cfg(feature = "observability-prometheus")]
            {
                Box::new(PrometheusObserver::new())
            }
            #[cfg(not(feature = "observability-prometheus"))]
            {
                tracing::warn!(
                    "Prometheus backend requested but this build was compiled without `observability-prometheus`; falling back to noop."
                );
                Box::new(NoopObserver)
            }
        }
        "otel" | "opentelemetry" | "otlp" => {
            #[cfg(feature = "observability-otel")]
            {
                let mut observers: Vec<Box<dyn Observer>> = Vec::new();

                // Shared trace context so DatadogLogObserver can read the
                // active trace_id/span_id written by OtelObserver.
                let shared_ctx: SharedTraceContext =
                    std::sync::Arc::new(parking_lot::Mutex::new(("0".into(), "0".into())));

                // Build consolidated endpoint list
                let mut endpoints: Vec<(
                    String,
                    Option<std::collections::HashMap<String, String>>,
                )> = Vec::new();

                // Legacy single-endpoint config (backwards compat)
                if let Some(ref ep) = config.otel_endpoint {
                    endpoints.push((ep.clone(), config.otel_headers.clone()));
                }

                // New multi-endpoint config
                for ep_cfg in &config.otel_endpoints {
                    // Deduplicate: skip if same endpoint already added from legacy config
                    if !endpoints.iter().any(|(e, _)| e == &ep_cfg.endpoint) {
                        endpoints.push((ep_cfg.endpoint.clone(), ep_cfg.headers.clone()));
                    }
                }

                // If no endpoints configured at all, use default
                if endpoints.is_empty() {
                    endpoints.push(("http://localhost:4318".into(), None));
                }

                for (i, (endpoint, headers)) in endpoints.iter().enumerate() {
                    let instance_name = format!("otel-{}", i);
                    // Only the first observer (Datadog) writes to shared trace context.
                    // Others (LangSmith/Langfuse) generate different trace IDs that would
                    // overwrite Datadog's, breaking log↔trace correlation.
                    let ctx_for_observer = if i == 0 { Some(shared_ctx.clone()) } else { None };
                    match OtelObserver::new(
                        Some(endpoint.as_str()),
                        config.otel_service_name.as_deref(),
                        headers.clone(),
                        Some(&instance_name),
                        ctx_for_observer,
                    ) {
                        Ok(obs) => {
                            tracing::info!(
                                endpoint = %endpoint,
                                instance = %instance_name,
                                "OpenTelemetry observer initialized"
                            );
                            observers.push(Box::new(obs));
                        }
                        Err(e) => {
                            tracing::error!("Failed to create OTel observer for {}: {e}", endpoint);
                        }
                    }
                }

                // Always include DatadogLogObserver for structured stdout logging
                // with shared trace context for log↔trace correlation.
                observers.push(Box::new(
                    DatadogLogObserver::new().with_trace_context(shared_ctx.clone()),
                ));

                if observers.is_empty() {
                    Box::new(NoopObserver)
                } else if observers.len() == 1 {
                    observers.pop().unwrap()
                } else {
                    Box::new(MultiObserver::new(observers))
                }
            }
            #[cfg(not(feature = "observability-otel"))]
            {
                tracing::warn!(
                    "OpenTelemetry backend requested but this build was compiled without `observability-otel`; falling back to noop."
                );
                Box::new(NoopObserver)
            }
        }
        "none" | "noop" => Box::new(NoopObserver),
        _ => {
            tracing::warn!(
                "Unknown observability backend '{}', falling back to noop",
                config.backend
            );
            Box::new(NoopObserver)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_none_returns_noop() {
        let cfg = ObservabilityConfig {
            backend: "none".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "noop");
    }

    #[test]
    fn factory_noop_returns_noop() {
        let cfg = ObservabilityConfig {
            backend: "noop".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "noop");
    }

    #[test]
    fn factory_log_returns_log() {
        let cfg = ObservabilityConfig {
            backend: "log".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "log");
    }

    #[test]
    fn factory_verbose_returns_verbose() {
        let cfg = ObservabilityConfig {
            backend: "verbose".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "verbose");
    }

    #[test]
    fn factory_datadog_log_returns_datadog_log() {
        let cfg = ObservabilityConfig {
            backend: "datadog-log".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "datadog-log");
    }

    #[test]
    fn factory_prometheus_returns_prometheus() {
        let cfg = ObservabilityConfig {
            backend: "prometheus".into(),
            ..ObservabilityConfig::default()
        };
        let expected = if cfg!(feature = "observability-prometheus") {
            "prometheus"
        } else {
            "noop"
        };
        assert_eq!(create_observer(&cfg).name(), expected);
    }

    #[test]
    fn factory_otel_returns_multi_with_datadog_log() {
        let cfg = ObservabilityConfig {
            backend: "otel".into(),
            otel_endpoint: Some("http://127.0.0.1:19999".into()),
            otel_service_name: Some("test".into()),
            ..ObservabilityConfig::default()
        };
        let obs = create_observer(&cfg);
        // With otel feature: returns "multi" (OtelObserver + DatadogLogObserver)
        // Without: returns "noop"
        let expected = if cfg!(feature = "observability-otel") {
            "multi"
        } else {
            "noop"
        };
        assert_eq!(obs.name(), expected);
    }

    #[test]
    fn factory_opentelemetry_alias() {
        let cfg = ObservabilityConfig {
            backend: "opentelemetry".into(),
            otel_endpoint: Some("http://127.0.0.1:19999".into()),
            otel_service_name: Some("test".into()),
            ..ObservabilityConfig::default()
        };
        let obs = create_observer(&cfg);
        let expected = if cfg!(feature = "observability-otel") {
            "multi"
        } else {
            "noop"
        };
        assert_eq!(obs.name(), expected);
    }

    #[test]
    fn factory_otlp_alias() {
        let cfg = ObservabilityConfig {
            backend: "otlp".into(),
            otel_endpoint: Some("http://127.0.0.1:19999".into()),
            otel_service_name: Some("test".into()),
            ..ObservabilityConfig::default()
        };
        let obs = create_observer(&cfg);
        let expected = if cfg!(feature = "observability-otel") {
            "multi"
        } else {
            "noop"
        };
        assert_eq!(obs.name(), expected);
    }

    #[test]
    fn factory_unknown_falls_back_to_noop() {
        let cfg = ObservabilityConfig {
            backend: "xyzzy_unknown".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "noop");
    }

    #[test]
    fn factory_empty_string_falls_back_to_noop() {
        let cfg = ObservabilityConfig {
            backend: String::new(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "noop");
    }

    #[test]
    fn factory_garbage_falls_back_to_noop() {
        let cfg = ObservabilityConfig {
            backend: "xyzzy_garbage_123".into(),
            ..ObservabilityConfig::default()
        };
        assert_eq!(create_observer(&cfg).name(), "noop");
    }
}
