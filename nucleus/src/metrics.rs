use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::net::proto;

pub fn init_telemetry() {
    // Define a subscriber.
    let subscriber = Registry::default().with(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::TRACE.into())
            .from_env_lossy()
            .add_directive("h2=info".parse().unwrap())
            .add_directive("hyper_util=info".parse().unwrap())
            .add_directive("tonic=info".parse().unwrap())
            .add_directive("tower=info".parse().unwrap())
            .add_directive("hyper=info".parse().unwrap()),
    );

    global::set_text_map_propagator(TraceContextPropagator::new());

    subscriber.init()
}

pub(crate) struct TraceContextInjector<'a> {
    pub(crate) trace_parent: &'a mut Option<proto::core::TraceParent>,
    pub(crate) otel_is_shit: String,
}

impl<'a> TraceContextInjector<'a> {
    pub fn new(parent: &'a mut Option<proto::core::TraceParent>) -> Self {
        let header_value = if let Some(t) = parent {
            let trace_id_a = t.trace_id_a;
            let trace_id_b = t.trace_id_b;

            let trace_id: u128 = ((trace_id_a as u128) << 64) | (trace_id_b as u128);

            format!("{:02x}-{:032x}-{:016x}-{:02x}", 0, trace_id, t.span_id, 0)
        } else {
            String::new()
        };

        Self {
            otel_is_shit: header_value,
            trace_parent: parent,
        }
    }
}

impl Injector for TraceContextInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if key == "tracestate" {
            return; // drop silently
        }

        if key != "traceparent" {
            error!("<hazard> Invalid key for trace context: {}", key);
            // panic!("Injecting invalid key for trace context - {}", key);
            return;
        }

        if key == "traceparent" {
            // value is in format {:02x}-{}-{}-{:02x}, parse out the middle two parts into u128 and u64
            let parts: Vec<&str> = value.split('-').collect();
            if parts.len() != 4 {
                panic!("<hazard> Invalid traceparent format: {}", value);
            }
            let trace_id = match u128::from_str_radix(parts[1], 16) {
                Ok(id) => id,
                Err(e) => {
                    panic!(
                        "<hazard> Invalid trace_id in traceparent: {}: {}",
                        parts[1], e
                    );
                }
            };

            let span_id = match u64::from_str_radix(parts[2], 16) {
                Ok(id) => id,
                Err(e) => {
                    panic!(
                        "<hazard> Invalid span_id in traceparent: {}: {}",
                        parts[2], e
                    );
                }
            };

            let trace_id_a = (trace_id >> 64) as u64;
            let trace_id_b = (trace_id & 0xFFFFFFFFFFFFFFFF) as u64;

            *self.trace_parent = Some(proto::core::TraceParent {
                trace_id_a,
                trace_id_b,
                span_id,
            });
        } else {
            // error!("<hazard> Invalid key for trace context: {}", key);
            panic!("Injecting invalid key for trace context - {}", key);
        }
    }
}

impl Extractor for TraceContextInjector<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key == "traceparent" {
            if self.otel_is_shit.is_empty() {
                None
            } else {
                Some(self.otel_is_shit.as_str())
            }
        } else if key == "tracestate" {
            None // drop silently
        } else {
            error!("<hazard> Invalid key for trace context: {}", key);
            // panic!("Extracting invalid key for trace context - {}", key);
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut res = Vec::with_capacity(1);

        if self.trace_parent.is_some() {
            res.push("traceparent");
        }

        res
    }
}
