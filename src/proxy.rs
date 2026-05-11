use crate::cache::{
    CacheDebugSummary, CachePlan, CacheResult, MemoryCache, cache_debug_summary,
    compute_request_cache_plan, extract_api_key, lookup_or_create,
};
use crate::cch::strip_cch_from_request_body;
use crate::response_patch::{
    inject_json_cache_fields, patch_sse_line, response_has_cache_usage_fields,
    sse_line_has_cache_usage_fields, sse_line_is_done, sse_line_is_patchable_final_usage,
};
use anyhow::{Context, Result, anyhow};
use axum::{
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, Method, Request, Response, StatusCode, header},
    response::IntoResponse,
};
use futures_util::StreamExt;
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
            let breakpoint_count = plan.breakpoints.len();
            let debug = state
                .debug_cache_keys
                .then(|| cache_debug_summary(&plan.api_key, &plan.prefixes, &plan.breakpoints));
            let result = lookup_or_create(
                &state.cache,
                &plan.api_key,
                &plan.prefixes,
                &plan.breakpoints,
                plan.total_input_tokens,
            )
            .await;
            cache_log = Some((true, breakpoint_count, result.clone(), debug));
            Some(result)
        }
        Some(plan) if upstream_has_cache_usage => {
            let debug = state
                .debug_cache_keys
                .then(|| cache_debug_summary(&plan.api_key, &plan.prefixes, &plan.breakpoints));
            cache_log = Some((false, plan.breakpoints.len(), CacheResult::default(), debug));
            None
        }
        Some(plan) => {
            let debug = state
                .debug_cache_keys
                .then(|| cache_debug_summary(&plan.api_key, &plan.prefixes, &plan.breakpoints));
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
            let debug = state_for_task.debug_cache_keys.then(|| {
                cache_debug_summary(
                    &plan_for_log.api_key,
                    &plan_for_log.prefixes,
                    &plan_for_log.breakpoints,
                )
            });
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
        let mut cache_result: Option<CacheResult> = None;
        let streamed = stream_sse_with_deferred_cache(upstream, &tx).await;
        let should_apply_cache = should_apply_deferred_sse_cache(
            streamed.is_complete,
            !tx.is_closed(),
            streamed.upstream_has_cache,
            streamed.has_patch_target,
        );

        if should_apply_cache {
            let result = lookup_or_create(
                &state_for_task.cache,
                &plan_for_log.api_key,
                &plan_for_log.prefixes,
                &plan_for_log.breakpoints,
                plan_for_log.total_input_tokens,
            )
            .await;
            send_deferred_sse_lines(&tx, streamed.deferred_lines, Some(&result)).await;
            cache_result = Some(result);
        } else {
            send_deferred_sse_lines(&tx, streamed.deferred_lines, None).await;
        }

        let cache_log = match cache_result {
            Some(result) => {
                let debug = state_for_task.debug_cache_keys.then(|| {
                    cache_debug_summary(
                        &plan_for_log.api_key,
                        &plan_for_log.prefixes,
                        &plan_for_log.breakpoints,
                    )
                });
                Some((true, plan_for_log.breakpoints.len(), result, debug))
            }
            None if streamed.upstream_has_cache => {
                let debug = state_for_task.debug_cache_keys.then(|| {
                    cache_debug_summary(
                        &plan_for_log.api_key,
                        &plan_for_log.prefixes,
                        &plan_for_log.breakpoints,
                    )
                });
                Some((
                    false,
                    plan_for_log.breakpoints.len(),
                    CacheResult::default(),
                    debug,
                ))
            }
            None => {
                let debug = state_for_task.debug_cache_keys.then(|| {
                    cache_debug_summary(
                        &plan_for_log.api_key,
                        &plan_for_log.prefixes,
                        &plan_for_log.breakpoints,
                    )
                });
                Some((
                    false,
                    plan_for_log.breakpoints.len(),
                    CacheResult::default(),
                    debug,
                ))
            }
        };

        emit_cache_log(&method_for_task, &path_for_task, status.as_u16(), cache_log);
    });

    build_response(status, &response_headers, stream_body)
}

struct PendingSseLine {
    line: String,
    had_newline: bool,
}

struct StreamedSseState {
    is_complete: bool,
    upstream_has_cache: bool,
    has_patch_target: bool,
    deferred_lines: Vec<PendingSseLine>,
}

async fn stream_sse_with_deferred_cache(
    upstream: reqwest::Response,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> StreamedSseState {
    let mut byte_stream = upstream.bytes_stream();
    let mut byte_buffer: Vec<u8> = Vec::new();
    let mut state = StreamedSseState {
        is_complete: true,
        upstream_has_cache: false,
        has_patch_target: false,
        deferred_lines: Vec::new(),
    };

    while let Some(chunk_result) = byte_stream.next().await {
        if tx.is_closed() {
            state.is_complete = false;
            return state;
        }
        let Ok(chunk) = chunk_result else {
            state.is_complete = false;
            break;
        };
        byte_buffer.extend_from_slice(&chunk);

        while let Some(newline_pos) = byte_buffer.iter().position(|&b| b == b'\n') {
            let line_bytes = byte_buffer[..newline_pos].to_vec();
            byte_buffer = byte_buffer[newline_pos + 1..].to_vec();
            let line = String::from_utf8_lossy(&line_bytes).into_owned();
            if !handle_sse_line_for_streaming(tx, &mut state, line, true).await {
                state.is_complete = false;
                return state;
            }
        }
    }

    if !byte_buffer.is_empty() && state.is_complete {
        let line = String::from_utf8_lossy(&byte_buffer).into_owned();
        if !handle_sse_line_for_streaming(tx, &mut state, line, false).await {
            state.is_complete = false;
        }
    }

    state
}

async fn handle_sse_line_for_streaming(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    state: &mut StreamedSseState,
    line: String,
    had_newline: bool,
) -> bool {
    if state.upstream_has_cache {
        return send_sse_line(tx, PendingSseLine { line, had_newline }).await;
    }

    if sse_line_has_cache_usage_fields(&line) {
        state.upstream_has_cache = true;
        if !send_deferred_sse_lines(tx, std::mem::take(&mut state.deferred_lines), None).await {
            return false;
        }
        return send_sse_line(tx, PendingSseLine { line, had_newline }).await;
    }

    if sse_line_is_patchable_final_usage(&line) {
        state.has_patch_target = true;
        state
            .deferred_lines
            .push(PendingSseLine { line, had_newline });
        return true;
    }

    if sse_line_is_done(&line) {
        state
            .deferred_lines
            .push(PendingSseLine { line, had_newline });
        return true;
    }

    send_sse_line(tx, PendingSseLine { line, had_newline }).await
}

async fn send_deferred_sse_lines(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    lines: Vec<PendingSseLine>,
    cache: Option<&CacheResult>,
) -> bool {
    for line in lines {
        let bytes = if let Some(cache) = cache {
            patched_sse_line_bytes(&line, cache)
        } else {
            sse_line_bytes(&line)
        };
        if tx.send(Ok(bytes)).await.is_err() {
            return false;
        }
    }
    true
}

async fn send_sse_line(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    line: PendingSseLine,
) -> bool {
    tx.send(Ok(sse_line_bytes(&line))).await.is_ok()
}

fn sse_line_bytes(line: &PendingSseLine) -> Bytes {
    if line.had_newline {
        Bytes::from(format!("{}\n", line.line))
    } else {
        Bytes::from(line.line.clone())
    }
}

fn patched_sse_line_bytes(line: &PendingSseLine, cache: &CacheResult) -> Bytes {
    let mut output = patch_sse_line(&line.line, cache);
    if !line.had_newline && output.ends_with('\n') {
        output.pop();
    }
    Bytes::from(output)
}

async fn passthrough_sse(
    upstream: reqwest::Response,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut byte_stream = upstream.bytes_stream();
    while let Some(chunk_result) = byte_stream.next().await {
        let Ok(chunk) = chunk_result else {
            break;
        };
        if tx.send(Ok(chunk)).await.is_err() {
            break;
        }
    }
}

pub(crate) fn should_apply_deferred_sse_cache(
    is_complete: bool,
    downstream_open: bool,
    upstream_has_cache: bool,
    has_patch_target: bool,
) -> bool {
    is_complete && downstream_open && !upstream_has_cache && has_patch_target
}

fn emit_cache_log(
    method: &Method,
    path: &str,
    status: u16,
    cache_log: Option<(bool, usize, CacheResult, Option<CacheDebugSummary>)>,
) {
    if let Some((applied, breakpoint_count, result, debug)) = cache_log {
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
