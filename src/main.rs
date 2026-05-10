use anyhow::{Context, Result, anyhow};
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, Method, Request, Response, StatusCode, header},
    response::IntoResponse,
    routing::any,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf, sync::Arc, time::Instant};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::cors::CorsLayer;

const DEFAULT_TTL_SECS: u64 = 5 * 60;
const EXTENDED_TTL_SECS: u64 = 60 * 60;
const MAX_BODY_SIZE: usize = 50 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "kiro-rs-cache-simulator")]
#[command(about = "Pure in-memory prompt-cache simulator for a running kiro-rs service")]
struct Args {
    #[arg(short, long, default_value = "cache-simulator.json")]
    config: PathBuf,

    #[arg(long)]
    upstream: Option<String>,

    #[arg(long, hide = true)]
    source_url: Option<String>,

    #[arg(long)]
    host: Option<String>,

    #[arg(long)]
    port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileConfig {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default)]
    upstream: Option<String>,
    #[serde(default)]
    source_url: Option<String>,
}

#[derive(Debug, Clone)]
struct Config {
    host: String,
    port: u16,
    upstream: String,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8990
}

impl Config {
    fn load(args: Args) -> Result<Self> {
        let mut file_config = if args.config.exists() {
            let content = std::fs::read_to_string(&args.config)
                .with_context(|| format!("failed to read config {}", args.config.display()))?;
            serde_json::from_str::<FileConfig>(&content)
                .with_context(|| format!("failed to parse config {}", args.config.display()))?
        } else {
            FileConfig {
                host: default_host(),
                port: default_port(),
                upstream: None,
                source_url: None,
            }
        };

        if let Some(host) = args.host {
            file_config.host = host;
        }
        if let Some(port) = args.port {
            file_config.port = port;
        }

        let upstream = first_non_empty([
            args.upstream,
            args.source_url,
            env::var("UPSTREAM").ok(),
            file_config.upstream,
            file_config.source_url,
        ])
        .ok_or_else(|| {
            anyhow!(
                "upstream is required via UPSTREAM, --upstream, or {}",
                args.config.display()
            )
        })?;

        Ok(Config {
            host: file_config.host,
            port: file_config.port,
            upstream: upstream.trim_end_matches('/').to_string(),
        })
    }
}

fn first_non_empty(candidates: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    candidates
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

#[derive(Clone)]
struct AppState {
    upstream: String,
    client: reqwest::Client,
    cache: MemoryCache,
}

type MemoryCache = Arc<Mutex<HashMap<String, CacheEntry>>>;

#[derive(Debug, Clone)]
struct CacheEntry {
    tokens: i32,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct CacheBreakpoint {
    hash: String,
    tokens: i32,
    ttl_secs: u64,
}

#[derive(Debug, Clone)]
struct CachePlan {
    api_key: String,
    breakpoints: Vec<CacheBreakpoint>,
    total_input_tokens: i32,
}

#[derive(Debug, Clone, Default)]
struct CacheResult {
    cache_read_input_tokens: i32,
    cache_creation_input_tokens: i32,
    uncached_input_tokens: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "error,kiro_rs_cache_simulator=info".into()),
        )
        .init();

    let config = Config::load(Args::parse())?;
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .with_context(|| format!("invalid listen address {}:{}", config.host, config.port))?;

    let state = AppState {
        upstream: config.upstream.clone(),
        client: reqwest::Client::builder().build()?,
        cache: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .fallback(any(proxy))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn proxy(State(state): State<AppState>, req: Request<Body>) -> Response<Body> {
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

    let cache_plan = compute_request_cache_plan(&method, &path, &headers, &body, &api_key);
    let upstream = forward_request(&state, &method, path_and_query, &headers, body.clone()).await?;

    let status = upstream.status();
    let response_headers = upstream.headers().clone();
    let response_body = upstream
        .bytes()
        .await
        .context("failed to read upstream response body")?;

    let mut cache_log = None;
    let cache_result = match cache_plan {
        Some(plan) if status.is_success() => {
            let breakpoint_count = plan.breakpoints.len();
            let result = lookup_or_create(
                &state.cache,
                &plan.api_key,
                &plan.breakpoints,
                plan.total_input_tokens,
            )
            .await;
            cache_log = Some((true, breakpoint_count, result.clone()));
            Some(result)
        }
        Some(plan) => {
            cache_log = Some((false, plan.breakpoints.len(), CacheResult::default()));
            None
        }
        None => None,
    };

    let body = if let Some(cache_result) = cache_result {
        inject_cache_fields(&response_headers, response_body, &cache_result)
    } else {
        response_body
    };

    if let Some((applied, breakpoint_count, result)) = cache_log {
        tracing::info!(
            method = %method,
            path = %path,
            status = status.as_u16(),
            cache_applied = applied,
            breakpoints = breakpoint_count,
            cache_read_input_tokens = result.cache_read_input_tokens,
            cache_creation_input_tokens = result.cache_creation_input_tokens,
            uncached_input_tokens = result.uncached_input_tokens,
            "cache calculation completed after proxy pass-through"
        );
    }

    build_response(status, &response_headers, body)
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
    body: Bytes,
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

    Ok(builder.body(Body::from(body))?)
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

fn compute_request_cache_plan(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
    api_key: &str,
) -> Option<CachePlan> {
    if method != Method::POST || !matches!(path, "/v1/messages" | "/cc/v1/messages") {
        return None;
    }

    if !is_json_request(headers) {
        return None;
    }

    let request: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return None,
    };

    if is_web_search_request(&request) {
        return None;
    }

    let breakpoints = compute_cache_breakpoints(&request);
    let total_input_tokens = count_all_tokens(&request).max(1);
    Some(CachePlan {
        api_key: api_key.to_string(),
        breakpoints,
        total_input_tokens,
    })
}

fn is_json_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(true)
}

fn is_web_search_request(request: &Value) -> bool {
    let Some(tools) = request.get("tools").and_then(Value::as_array) else {
        return false;
    };
    if tools.len() != 1 {
        return false;
    }
    let tool = &tools[0];
    let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
    let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or_default();
    name == "web_search" || tool_type.starts_with("web_search")
}

async fn lookup_or_create(
    cache: &MemoryCache,
    api_key: &str,
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> CacheResult {
    if breakpoints.is_empty() {
        return CacheResult {
            uncached_input_tokens: total_input_tokens,
            ..CacheResult::default()
        };
    }

    let mut cache = cache.lock().await;
    let now = Instant::now();
    cache.retain(|_, entry| entry.expires_at > now);

    let mut result = CacheResult::default();
    let mut hit_index = None;

    for (index, breakpoint) in breakpoints.iter().enumerate().rev() {
        let key = cache_key(api_key, &breakpoint.hash);
        if let Some(entry) = cache.get_mut(&key) {
            entry.expires_at = now + std::time::Duration::from_secs(breakpoint.ttl_secs);
            result.cache_read_input_tokens = entry.tokens;
            hit_index = Some(index);
            break;
        }
    }

    if let Some(index) = hit_index {
        let mut previous_tokens = result.cache_read_input_tokens;
        for breakpoint in breakpoints.iter().skip(index + 1) {
            let additional_tokens = breakpoint.tokens - previous_tokens;
            cache.insert(
                cache_key(api_key, &breakpoint.hash),
                CacheEntry {
                    tokens: breakpoint.tokens,
                    expires_at: now + std::time::Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            result.cache_creation_input_tokens += additional_tokens.max(0);
            previous_tokens = breakpoint.tokens;
        }
    } else {
        let mut previous_tokens = 0;
        for breakpoint in breakpoints {
            let additional_tokens = breakpoint.tokens - previous_tokens;
            cache.insert(
                cache_key(api_key, &breakpoint.hash),
                CacheEntry {
                    tokens: breakpoint.tokens,
                    expires_at: now + std::time::Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            result.cache_creation_input_tokens += additional_tokens.max(0);
            previous_tokens = breakpoint.tokens;
        }
    }

    let cached_tokens = result.cache_read_input_tokens + result.cache_creation_input_tokens;
    result.uncached_input_tokens = (total_input_tokens - cached_tokens).max(0);
    result
}

fn cache_key(api_key: &str, hash: &str) -> String {
    format!("cache:{}:{}", api_key, hash)
}

fn extract_api_key(headers: &HeaderMap) -> String {
    if let Some(api_key) = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
    {
        return api_key.to_string();
    }

    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        let mut parts = value.split_whitespace();
        if let (Some(scheme), Some(token), None) = (parts.next(), parts.next(), parts.next()) {
            if scheme.eq_ignore_ascii_case("Bearer") {
                return token.to_string();
            }
        }
    }

    "anonymous".to_string()
}

fn compute_cache_breakpoints(request: &Value) -> Vec<CacheBreakpoint> {
    let mut hasher = Sha256::new();
    let mut breakpoints = Vec::new();
    let mut cumulative_tokens = 0;

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        let mut sorted_tools: Vec<&Value> = tools.iter().collect();
        sorted_tools.sort_by(|left, right| tool_name(left).cmp(tool_name(right)));

        for tool in sorted_tools {
            let normalized = normalize_tool(tool);
            hasher.update(normalized.as_bytes());
            cumulative_tokens += count_tokens(&normalized);

            if let Some(cache_control) = tool.get("cache_control") {
                breakpoints.push(CacheBreakpoint {
                    hash: finalize_hash(&hasher),
                    tokens: cumulative_tokens,
                    ttl_secs: parse_ttl(cache_control),
                });
            }
        }
    }

    match request.get("system") {
        Some(Value::String(text)) => {
            hasher.update(text.as_bytes());
            cumulative_tokens += count_tokens(text);
        }
        Some(Value::Array(system_messages)) => {
            for message in system_messages {
                let text = message
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                hasher.update(text.as_bytes());
                cumulative_tokens += count_tokens(text);

                if let Some(cache_control) = message.get("cache_control") {
                    breakpoints.push(CacheBreakpoint {
                        hash: finalize_hash(&hasher),
                        tokens: cumulative_tokens,
                        ttl_secs: parse_ttl(cache_control),
                    });
                }
            }
        }
        _ => {}
    }

    if let Some(messages) = request.get("messages").and_then(Value::as_array) {
        for message in messages {
            match message.get("content") {
                Some(Value::Array(blocks)) => {
                    for block in blocks {
                        let block_json = serde_json::to_string(block).unwrap_or_default();
                        hasher.update(block_json.as_bytes());

                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            cumulative_tokens += count_tokens(text);
                        }

                        if let Some(cache_control) = block.get("cache_control") {
                            breakpoints.push(CacheBreakpoint {
                                hash: finalize_hash(&hasher),
                                tokens: cumulative_tokens,
                                ttl_secs: parse_ttl(cache_control),
                            });
                        }
                    }
                }
                Some(Value::String(text)) => {
                    hasher.update(text.as_bytes());
                    cumulative_tokens += count_tokens(text);
                }
                _ => {}
            }
        }
    }

    breakpoints
}

fn finalize_hash(hasher: &Sha256) -> String {
    format!("{:x}", hasher.clone().finalize())
}

fn parse_ttl(cache_control: &Value) -> u64 {
    match cache_control.get("ttl").and_then(Value::as_str) {
        Some("1h") => EXTENDED_TTL_SECS,
        _ => DEFAULT_TTL_SECS,
    }
}

fn normalize_tool(tool: &Value) -> String {
    let mut parts = Vec::new();
    parts.push(format!("name:{}", tool_name(tool)));

    if let Some(description) = tool.get("description").and_then(Value::as_str) {
        if !description.is_empty() {
            parts.push(format!("desc:{}", description));
        }
    }

    if let Some(input_schema) = tool.get("input_schema") {
        if !input_schema.as_object().map(Map::is_empty).unwrap_or(true) {
            let sorted = sort_json_value(input_schema);
            if let Ok(serialized) = serde_json::to_string(&sorted) {
                parts.push(format!("schema:{}", serialized));
            }
        }
    }

    parts.join("|")
}

fn tool_name(tool: &Value) -> &str {
    tool.get("name").and_then(Value::as_str).unwrap_or_default()
}

fn sort_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let sorted = keys
                .into_iter()
                .filter_map(|key| {
                    map.get(key)
                        .map(|value| (key.clone(), sort_json_value(value)))
                })
                .collect();
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(sort_json_value).collect()),
        _ => value.clone(),
    }
}

fn count_all_tokens(request: &Value) -> i32 {
    let mut total = 0;

    match request.get("system") {
        Some(Value::String(text)) => total += count_tokens(text),
        Some(Value::Array(messages)) => {
            for message in messages {
                if let Some(text) = message.get("text").and_then(Value::as_str) {
                    total += count_tokens(text);
                }
            }
        }
        _ => {}
    }

    if let Some(messages) = request.get("messages").and_then(Value::as_array) {
        for message in messages {
            match message.get("content") {
                Some(Value::String(text)) => total += count_tokens(text),
                Some(Value::Array(blocks)) => {
                    for block in blocks {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            total += count_tokens(text);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        for tool in tools {
            total += count_tokens(tool_name(tool));
            total += count_tokens(
                tool.get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            if let Some(input_schema) = tool.get("input_schema") {
                total += count_tokens(&serde_json::to_string(input_schema).unwrap_or_default());
            }
        }
    }

    total.max(1)
}

fn count_tokens(text: &str) -> i32 {
    let char_units: f64 = text
        .chars()
        .map(|ch| if is_non_western_char(ch) { 4.0 } else { 1.0 })
        .sum();
    let tokens = char_units / 4.0;
    let adjusted = if tokens < 100.0 {
        tokens * 1.5
    } else if tokens < 200.0 {
        tokens * 1.3
    } else if tokens < 300.0 {
        tokens * 1.25
    } else if tokens < 800.0 {
        tokens * 1.2
    } else {
        tokens
    };
    adjusted as i32
}

fn is_non_western_char(ch: char) -> bool {
    !matches!(ch,
        '\u{0000}'..='\u{007F}' |
        '\u{0080}'..='\u{00FF}' |
        '\u{0100}'..='\u{024F}' |
        '\u{1E00}'..='\u{1EFF}' |
        '\u{2C60}'..='\u{2C7F}' |
        '\u{A720}'..='\u{A7FF}' |
        '\u{AB30}'..='\u{AB6F}'
    )
}

fn inject_cache_fields(
    headers: &reqwest::header::HeaderMap,
    body: Bytes,
    cache: &CacheResult,
) -> Bytes {
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.contains("text/event-stream") {
        inject_sse_cache_fields(body, cache)
    } else if content_type.contains("application/json") || content_type.is_empty() {
        inject_json_cache_fields(body, cache)
    } else {
        body
    }
}

fn inject_json_cache_fields(body: Bytes, cache: &CacheResult) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };

    if let Some(usage) = value.get_mut("usage").and_then(Value::as_object_mut) {
        patch_usage(usage, cache);
        match serde_json::to_vec(&value) {
            Ok(bytes) => Bytes::from(bytes),
            Err(_) => body,
        }
    } else {
        body
    }
}

fn inject_sse_cache_fields(body: Bytes, cache: &CacheResult) -> Bytes {
    let Ok(text) = String::from_utf8(body.to_vec()) else {
        return body;
    };

    let mut output = String::with_capacity(text.len() + 128);
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data == "[DONE]" {
                output.push_str(line);
                output.push('\n');
                continue;
            }

            if let Ok(mut event) = serde_json::from_str::<Value>(data) {
                patch_sse_event(&mut event, cache);
                if let Ok(serialized) = serde_json::to_string(&event) {
                    output.push_str("data: ");
                    output.push_str(&serialized);
                    output.push('\n');
                    continue;
                }
            }
        }

        output.push_str(line);
        output.push('\n');
    }

    Bytes::from(output)
}

fn patch_sse_event(event: &mut Value, cache: &CacheResult) {
    match event.get("type").and_then(Value::as_str) {
        Some("message_start") => {
            if let Some(usage) = event
                .get_mut("message")
                .and_then(|message| message.get_mut("usage"))
                .and_then(Value::as_object_mut)
            {
                patch_usage(usage, cache);
            }
        }
        Some("message_delta") => {
            if let Some(usage) = event.get_mut("usage").and_then(Value::as_object_mut) {
                patch_usage(usage, cache);
            }
        }
        _ => {}
    }
}

fn patch_usage(usage: &mut Map<String, Value>, cache: &CacheResult) {
    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache.cache_read_input_tokens),
    );

    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if input_tokens == 0 {
        usage.insert(
            "input_tokens".to_string(),
            Value::from(cache.uncached_input_tokens),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_cache_records_then_hits_breakpoint() {
        let request = json!({
            "system": [{"text": "You are helpful", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let breakpoints = compute_cache_breakpoints(&request);
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let first =
            lookup_or_create(&cache, "sk-test", &breakpoints, count_all_tokens(&request)).await;
        assert!(first.cache_creation_input_tokens > 0);
        assert_eq!(first.cache_read_input_tokens, 0);

        let second =
            lookup_or_create(&cache, "sk-test", &breakpoints, count_all_tokens(&request)).await;
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[test]
    fn injects_json_usage_cache_fields() {
        let body = Bytes::from_static(br#"{"usage":{"input_tokens":0,"output_tokens":2}}"#);
        let cache = CacheResult {
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 4,
            uncached_input_tokens: 5,
        };

        let patched = inject_json_cache_fields(body, &cache);
        let value: Value = serde_json::from_slice(&patched).expect("valid patched JSON");
        assert_eq!(value["usage"]["cache_read_input_tokens"], 3);
        assert_eq!(value["usage"]["cache_creation_input_tokens"], 4);
        assert_eq!(value["usage"]["input_tokens"], 5);
    }

    #[test]
    fn injects_sse_message_start_usage_cache_fields() {
        let body = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n",
        );
        let cache = CacheResult {
            cache_read_input_tokens: 7,
            cache_creation_input_tokens: 0,
            uncached_input_tokens: 1,
        };

        let patched = inject_sse_cache_fields(body, &cache);
        let text = String::from_utf8(patched.to_vec()).expect("valid utf8");
        assert!(text.contains("\"cache_read_input_tokens\":7"));
        assert!(text.contains("\"cache_creation_input_tokens\":0"));
    }

    #[tokio::test]
    async fn cache_plan_does_not_mutate_memory_cache_before_application() {
        let body = br#"{
            "system": [{"text": "You are helpful", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        }"#;
        let headers = HeaderMap::new();
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let plan =
            compute_request_cache_plan(&Method::POST, "/v1/messages", &headers, body, "sk-test")
                .expect("cache plan should be prepared");

        assert!(plan.total_input_tokens > 0);
        assert!(cache.lock().await.is_empty());
    }

    #[test]
    fn bearer_api_key_extraction_is_case_and_whitespace_tolerant() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_static("bearer    sk-lowercase"),
        );

        assert_eq!(extract_api_key(&headers), "sk-lowercase");
    }

    #[test]
    fn file_config_accepts_upstream_and_legacy_source_url() {
        let modern: FileConfig = serde_json::from_value(json!({
            "upstream": "http://127.0.0.1:8080"
        }))
        .expect("modern config should parse");
        assert_eq!(modern.upstream.as_deref(), Some("http://127.0.0.1:8080"));

        let legacy: FileConfig = serde_json::from_value(json!({
            "sourceUrl": "http://127.0.0.1:8081"
        }))
        .expect("legacy config should parse");
        assert_eq!(legacy.source_url.as_deref(), Some("http://127.0.0.1:8081"));
    }

    #[test]
    fn first_non_empty_trims_and_skips_empty_values() {
        assert_eq!(
            first_non_empty([
                None,
                Some("   ".to_string()),
                Some(" http://upstream/ ".to_string())
            ]),
            Some("http://upstream/".to_string())
        );
    }
}
