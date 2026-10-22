use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;

/// Records a request-duration histogram and a request/error counter for
/// every routed request. Applied via `route_layer` (not `layer`) so
/// `MatchedPath` is populated — it's only inserted once Axum has matched
/// the request to one of the declared routes, which also means unmatched
/// (404) requests aren't labeled here and don't pollute the metric with an
/// unbounded set of garbage path labels.
pub async fn track_metrics(req: Request, next: Next) -> Response {
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let method = req.method().to_string();

    let start = Instant::now();
    let response = next.run(req).await;
    let latency_secs = start.elapsed().as_secs_f64();

    let status = response.status().as_u16().to_string();

    metrics::histogram!(
        "query_api_request_duration_seconds",
        "path" => path.clone(),
        "method" => method.clone(),
    )
    .record(latency_secs);

    metrics::counter!(
        "query_api_requests_total",
        "path" => path.clone(),
        "method" => method.clone(),
        "status" => status.clone(),
    )
    .increment(1);

    if !response.status().is_success() {
        metrics::counter!(
            "query_api_errors_total",
            "path" => path,
            "method" => method,
            "status" => status,
        )
        .increment(1);
    }

    response
}
