use crate::cache::{
    CacheDebugSummary, CachePlan, CacheResult, MemoryCache, cache_debug_summary,
    commit_prepared_cache_update, compute_request_cache_plan, extract_api_key, lookup_or_create,
    prepare_cache_update,
};
use crate::cch::strip_cch_from_request_body;
use crate::response_patch::{
    inject_json_cache_fields, json_usage_total_input_tokens, normalize_cache_result,
    response_has_cache_usage_fields,
};
use crate::sse::{passthrough_sse, should_commit_streamed_sse_cache, stream_sse_with_cache_patch};
use anyhow::{Context, Result, anyhow};
use axum::{
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, Method, Request, Response, StatusCode, header},
    response::IntoResponse,
};
use serde_json::json;
use tokio_stream::wrappers::ReceiverStream;

const MAX_BODY_SIZE: usize = 50 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) upstream: String,
    pub(crate) client: reqwest::Client,
    pub(crate) cache: MemoryCache,
    pub(crate) debug_cache_keys: bool,
    pub(crate) simulate_cache: bool,
    pub(crate) strip_cch: bool,
}

pub(crate) async fn proxy(State(state): State<AppState>, req: Request<Body>) -> Response<Body> {
    match proxy_inner(state, req).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "proxy error");
            let body = json!({
                "error": {
                    "type": "proxy_error",
                    "message": error.to_string()
                }
            });
            (StatusCode::BAD_GATEWAY, axum::Json(body)).into_response()
        }
    }
}

async fn proxy_inner(state: AppState, req: Request<Body>) -> Result<Response<Body>> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let path = uri.path().to_string();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let api_key = extract_api_key(&headers);
    let body = to_bytes(req.into_body(), MAX_BODY_SIZE)
        .await
        .context("failed to read request body")?;
    let body = if state.strip_cch {
        strip_cch_from_request_body(&method, &path, &headers, body)
    } else {
        body
    };

    let cache_plan = state
        .simulate_cache
        .then(|| compute_request_cache_plan(&method, &path, &headers, &body, &api_key))
        .flatten();
    let upstream = forward_request(&state, &method, path_and_query, &headers, body.clone()).await?;

    let status = upstream.status();
    let response_headers = upstream.headers().clone();

    let is_sse = response_headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains("text/event-stream");

    if is_sse {
        handle_sse_streaming(
            state,
            upstream,
            status,
            response_headers,
            cache_plan,
            method,
            path,
        )
        .await
    } else {
        handle_buffered(
            state,
            upstream,
            status,
            response_headers,
            cache_plan,
            method,
            path,
        )
        .await
    }
}

async fn handle_buffered(
    state: AppState,
    upstream: reqwest::Response,
    status: reqwest::StatusCode,
    response_headers: reqwest::header::HeaderMap,
    cache_plan: Option<CachePlan>,
    method: Method,
    path: String,
) -> Result<Response<Body>> {
    let response_body = upstream
        .bytes()
        .await
        .context("failed to read upstream response body")?;
    let upstream_has_cache_usage =
        response_has_cache_usage_fields(&response_headers, &response_body);

    let mut cache_log = None;
    let cache_result = match cache_plan {
        Some(plan) if status.is_success() && !upstream_has_cache_usage => {
            let debug = make_debug(&state, &plan);
            let result = lookup_or_create(
                &state.cache,
                &plan.api_key,
                &plan.prefixes,
                &plan.breakpoints,
                plan.total_input_tokens,
            )
            .await;
            let log_result = json_usage_total_input_tokens(&response_body)
                .map(|total_input_tokens| normalize_cache_result(&result, total_input_tokens))
                .unwrap_or_else(|| result.clone());
            cache_log = Some((true, plan.breakpoints.len(), log_result, debug));
            Some(result)
        }
        Some(plan) => {
            let debug = make_debug(&state, &plan);
            cache_log = Some((false, plan.breakpoints.len(), CacheResult::default(), debug));
            None
        }
        None => None,
    };

    let body = if let Some(cache_result) = cache_result {
        inject_json_cache_fields(response_body, &cache_result)
    } else {
        response_body
    };

    emit_cache_log(&method, &path, status.as_u16(), cache_log);
    build_response(status, &response_headers, body)
}

async fn handle_sse_streaming(
    state: AppState,
    upstream: reqwest::Response,
    status: reqwest::StatusCode,
    response_headers: reqwest::header::HeaderMap,
    cache_plan: Option<CachePlan>,
    method: Method,
    path: String,
) -> Result<Response<Body>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    let stream_body = Body::from_stream(ReceiverStream::new(rx));

    let Some(plan_for_log) = cache_plan else {
        tokio::spawn(async move {
            passthrough_sse(upstream, tx).await;
        });
        return build_response(status, &response_headers, stream_body);
    };

    let state_for_task = state.clone();
    let method_for_task = method.clone();
    let path_for_task = path.clone();

    if !status.is_success() {
        tokio::spawn(async move {
            passthrough_sse(upstream, tx).await;
            let debug = make_debug(&state_for_task, &plan_for_log);
            emit_cache_log(
                &method_for_task,
                &path_for_task,
                status.as_u16(),
                Some((
                    false,
                    plan_for_log.breakpoints.len(),
                    CacheResult::default(),
                    debug,
                )),
            );
        });
        return build_response(status, &response_headers, stream_body);
    }

    tokio::spawn(async move {
        let prepared = prepare_cache_update(
            &state_for_task.cache,
            &plan_for_log.api_key,
            &plan_for_log.prefixes,
            &plan_for_log.breakpoints,
            plan_for_log.total_input_tokens,
        )
        .await;
        let preview = prepared.result.clone();
        let mut cache_result: Option<CacheResult> = None;
        let streamed = stream_sse_with_cache_patch(upstream, &tx, &preview).await;
        let should_apply_cache = should_commit_streamed_sse_cache(
            streamed.is_complete,
            !tx.is_closed(),
            streamed.upstream_has_cache,
            streamed.has_patch_target,
        );

        if should_apply_cache {
            commit_prepared_cache_update(&state_for_task.cache, &plan_for_log.api_key, &prepared)
                .await;
            let result = streamed
                .upstream_total_input_tokens
                .map(|total_input_tokens| normalize_cache_result(&preview, total_input_tokens))
                .unwrap_or(preview);
            cache_result = Some(result);
        }

        let debug = make_debug(&state_for_task, &plan_for_log);
        let cache_log = match cache_result {
            Some(result) => Some((true, plan_for_log.breakpoints.len(), result, debug)),
            None => Some((
                false,
                plan_for_log.breakpoints.len(),
                CacheResult::default(),
                debug,
            )),
        };

        emit_cache_log(&method_for_task, &path_for_task, status.as_u16(), cache_log);
    });

    build_response(status, &response_headers, stream_body)
}

fn make_debug(state: &AppState, plan: &CachePlan) -> Option<CacheDebugSummary> {
    state
        .debug_cache_keys
        .then(|| cache_debug_summary(&plan.api_key, &plan.prefixes, &plan.breakpoints))
}

fn emit_cache_log(
    method: &Method,
    path: &str,
    status: u16,
    cache_log: Option<(bool, usize, CacheResult, Option<CacheDebugSummary>)>,
) {
    let Some((applied, breakpoint_count, result, debug)) = cache_log else {
        return;
    };

    if let Some(debug_summary) = debug {
        tracing::info!(
            method = %method,
            path = %path,
            status = status,
            cache_applied = applied,
            breakpoints = breakpoint_count,
            cache_read_input_tokens = result.cache_read_input_tokens,
            cache_creation_input_tokens = result.cache_creation_input_tokens,
            uncached_input_tokens = result.uncached_input_tokens,
            api_key_hash = %debug_summary.api_key_hash,
            prefix_count = debug_summary.prefix_count,
            breakpoint_prefixes = %debug_summary.breakpoint_prefixes,
            prefix_sample = %debug_summary.prefix_sample,
            "cache calculation completed after proxy pass-through"
        );
    } else {
        tracing::info!(
            method = %method,
            path = %path,
            status = status,
            cache_applied = applied,
            breakpoints = breakpoint_count,
            cache_read_input_tokens = result.cache_read_input_tokens,
            cache_creation_input_tokens = result.cache_creation_input_tokens,
            uncached_input_tokens = result.uncached_input_tokens,
            "cache calculation completed after proxy pass-through"
        );
    }
}

async fn forward_request(
    state: &AppState,
    method: &Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response> {
    let url = format!("{}{}", state.upstream, path_and_query);
    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let mut request = state.client.request(reqwest_method, url);

    for (name, value) in headers {
        if should_forward_request_header(name.as_str()) {
            request = request.header(name.as_str(), value.clone());
        }
    }

    request
        .body(body)
        .send()
        .await
        .context("upstream request failed")
}

fn build_response(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: impl Into<Body>,
) -> Result<Response<Body>> {
    let mut builder = Response::builder().status(StatusCode::from_u16(status.as_u16())?);
    let response_headers = builder
        .headers_mut()
        .ok_or_else(|| anyhow!("failed to access response headers"))?;

    for (name, value) in headers {
        if should_forward_response_header(name.as_str()) {
            response_headers.insert(
                header::HeaderName::from_bytes(name.as_str().as_bytes())?,
                header::HeaderValue::from_bytes(value.as_bytes())?,
            );
        }
    }

    Ok(builder.body(body.into())?)
}

fn should_forward_request_header(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "host" | "content-length" | "connection"
    )
}

fn should_forward_response_header(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "content-length" | "transfer-encoding" | "connection"
    )
}
