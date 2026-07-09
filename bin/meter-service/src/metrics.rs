//! Prometheus metrics: global recorder install, `/metrics` render, and an HTTP
//! request-tracking middleware.
//!
//! Labels use the matched **route template** (`/api/v1/meters/{id}`), never the raw
//! request path, so a flood of distinct IDs can't explode label cardinality.

use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use metrics::{counter, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Process-global Prometheus handle. Set once by [`install_recorder`]; read by
/// [`render`] to serve `/metrics`.
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus recorder.
///
/// Must be called **once, at startup, before any metric is emitted** — metrics
/// emitted before a recorder is installed go to the no-op recorder and are lost.
/// Idempotent; non-fatal on failure (metrics degrade to empty).
pub fn install_recorder() {
    if PROMETHEUS_HANDLE.get().is_some() {
        return;
    }
    match PrometheusBuilder::new().install_recorder() {
        Ok(handle) => {
            let _ = PROMETHEUS_HANDLE.set(handle);
        }
        Err(e) => tracing::error!("failed to install Prometheus recorder: {e}"),
    }
}

/// Render metrics in Prometheus text-exposition format. Empty if the recorder was
/// never installed.
pub fn render() -> String {
    PROMETHEUS_HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// Axum middleware: count every HTTP request and time its latency, labelled by
/// method, matched route template, and response status.
pub async fn track_http(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().as_str().to_owned();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| req.uri().path().to_owned(), |m| m.as_str().to_owned());

    let response = next.run(req).await;
    let status = response.status().as_u16().to_string();

    counter!("http_requests_total",
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status,
    )
    .increment(1);
    histogram!("http_request_duration_seconds",
        "method" => method,
        "path" => path,
    )
    .record(start.elapsed().as_secs_f64());

    response
}
