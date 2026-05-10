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
const CACHE_LOOKBACK_BLOCKS: usize = 20;
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
    debug_cache_keys: bool,
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
            debug_cache_keys: env_flag("CACHE_SIMULATOR_DEBUG_KEYS"),
        })
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
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
    debug_cache_keys: bool,
}

type MemoryCache = Arc<Mutex<HashMap<String, CacheEntry>>>;

#[derive(Debug, Clone)]
struct CacheEntry {
    tokens: i32,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct CachePrefix {
    hash: String,
    tokens: i32,
}

#[derive(Debug, Clone)]
struct CacheBreakpoint {
    prefix_index: usize,
    ttl_secs: u64,
}

#[derive(Debug, Clone)]
struct CachePlan {
    api_key: String,
    prefixes: Vec<CachePrefix>,
    breakpoints: Vec<CacheBreakpoint>,
    total_input_tokens: i32,
}

#[derive(Debug, Clone, Default)]
struct CachePath {
    prefixes: Vec<CachePrefix>,
    breakpoints: Vec<CacheBreakpoint>,
}

#[derive(Debug, Clone, Default)]
struct CacheResult {
    cache_read_input_tokens: i32,
    cache_creation_input_tokens: i32,
    uncached_input_tokens: i32,
}

#[derive(Debug, Clone)]
struct CacheDebugSummary {
    api_key_hash: String,
    prefix_count: usize,
    breakpoint_prefixes: String,
}

fn cache_debug_summary(
    api_key: &str,
    prefixes: &[CachePrefix],
    breakpoints: &[CacheBreakpoint],
) -> CacheDebugSummary {
    let breakpoint_prefixes = breakpoints
        .iter()
        .filter_map(|breakpoint| {
            prefixes.get(breakpoint.prefix_index).map(|prefix| {
                format!(
                    "{}:{}:{}",
                    breakpoint.prefix_index,
                    prefix.tokens,
                    short_hash(&prefix.hash)
                )
            })
        })
        .collect::<Vec<_>>()
        .join(",");

    CacheDebugSummary {
        api_key_hash: short_hash(&sha256_hex(api_key.as_bytes())),
        prefix_count: prefixes.len(),
        breakpoint_prefixes,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
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
        debug_cache_keys: config.debug_cache_keys,
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
        inject_cache_fields(&response_headers, response_body, &cache_result)
    } else {
        response_body
    };

    if let Some((applied, breakpoint_count, result, debug)) = cache_log {
        if let Some(debug_summary) = debug {
            tracing::info!(
                method = %method,
                path = %path,
                status = status.as_u16(),
                cache_applied = applied,
                breakpoints = breakpoint_count,
                cache_read_input_tokens = result.cache_read_input_tokens,
                cache_creation_input_tokens = result.cache_creation_input_tokens,
                uncached_input_tokens = result.uncached_input_tokens,
                api_key_hash = %debug_summary.api_key_hash,
                prefix_count = debug_summary.prefix_count,
                breakpoint_prefixes = %debug_summary.breakpoint_prefixes,
                "cache calculation completed after proxy pass-through"
            );
        } else {
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

    let mut cache_path = compute_cache_path(&request);
    let minimum_cache_tokens = minimum_cache_tokens(&request);
    cache_path.breakpoints.retain(|breakpoint| {
        cache_path.prefixes[breakpoint.prefix_index].tokens >= minimum_cache_tokens
    });
    let total_input_tokens = count_all_tokens(&request).max(1);
    Some(CachePlan {
        api_key: api_key.to_string(),
        prefixes: cache_path.prefixes,
        breakpoints: cache_path.breakpoints,
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
    prefixes: &[CachePrefix],
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> CacheResult {
    if prefixes.is_empty() || breakpoints.is_empty() {
        return CacheResult {
            uncached_input_tokens: total_input_tokens,
            ..CacheResult::default()
        };
    }

    let mut cache = cache.lock().await;
    let now = Instant::now();
    cache.retain(|_, entry| entry.expires_at > now);

    let mut result = CacheResult::default();
    let mut read_tokens = 0;

    'lookup: for breakpoint in breakpoints.iter().rev() {
        let start = breakpoint.prefix_index;
        let end = start.saturating_sub(CACHE_LOOKBACK_BLOCKS);
        for prefix_index in (end..=start).rev() {
            let prefix = &prefixes[prefix_index];
            let key = cache_key(api_key, &prefix.hash);
            if let Some(entry) = cache.get_mut(&key) {
                entry.expires_at = now + std::time::Duration::from_secs(breakpoint.ttl_secs);
                read_tokens = entry.tokens;
                break 'lookup;
            }
        }
    }
    result.cache_read_input_tokens = read_tokens;

    let mut largest_written_tokens = read_tokens;
    for breakpoint in breakpoints {
        let prefix = &prefixes[breakpoint.prefix_index];
        if prefix.tokens > read_tokens {
            cache.insert(
                cache_key(api_key, &prefix.hash),
                CacheEntry {
                    tokens: prefix.tokens,
                    expires_at: now + std::time::Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            largest_written_tokens = largest_written_tokens.max(prefix.tokens);
        }
    }
    result.cache_creation_input_tokens = (largest_written_tokens - read_tokens).max(0);

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

fn compute_cache_path(request: &Value) -> CachePath {
    let mut hasher = Sha256::new();
    let mut path = CachePath::default();
    let mut cumulative_tokens = 0;

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        let mut sorted_tools: Vec<&Value> = tools.iter().collect();
        sorted_tools.sort_by(|left, right| tool_name(left).cmp(tool_name(right)));

        for tool in sorted_tools {
            let normalized = normalize_tool(tool);
            push_prefix(
                &mut path,
                &mut hasher,
                &mut cumulative_tokens,
                &normalized,
                count_tokens(&normalized),
            );

            if let Some(cache_control) = tool.get("cache_control") {
                push_breakpoint(&mut path, parse_ttl(cache_control));
            }
        }
    }

    match request.get("system") {
        Some(Value::String(text)) => {
            push_prefix(
                &mut path,
                &mut hasher,
                &mut cumulative_tokens,
                text,
                count_tokens(text),
            );
        }
        Some(Value::Array(system_messages)) => {
            for message in system_messages {
                let text = message
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_prefix(
                    &mut path,
                    &mut hasher,
                    &mut cumulative_tokens,
                    text,
                    count_tokens(text),
                );

                if let Some(cache_control) = message.get("cache_control") {
                    push_breakpoint(&mut path, parse_ttl(cache_control));
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
                        let block_json = serde_json::to_string(&block_cache_hash_material(block))
                            .unwrap_or_default();
                        let block_tokens = block
                            .get("text")
                            .and_then(Value::as_str)
                            .map(count_tokens)
                            .unwrap_or_default();
                        push_prefix(
                            &mut path,
                            &mut hasher,
                            &mut cumulative_tokens,
                            &block_json,
                            block_tokens,
                        );

                        if let Some(cache_control) = block.get("cache_control") {
                            push_breakpoint(&mut path, parse_ttl(cache_control));
                        }
                    }
                }
                Some(Value::String(text)) => {
                    push_prefix(
                        &mut path,
                        &mut hasher,
                        &mut cumulative_tokens,
                        text,
                        count_tokens(text),
                    );
                }
                _ => {}
            }
        }
    }

    if let Some(cache_control) = request.get("cache_control") {
        push_breakpoint(&mut path, parse_ttl(cache_control));
    }

    path
}

fn push_prefix(
    path: &mut CachePath,
    hasher: &mut Sha256,
    cumulative_tokens: &mut i32,
    hash_material: &str,
    token_count: i32,
) {
    hasher.update(hash_material.as_bytes());
    *cumulative_tokens += token_count;
    path.prefixes.push(CachePrefix {
        hash: finalize_hash(hasher),
        tokens: *cumulative_tokens,
    });
}

fn push_breakpoint(path: &mut CachePath, ttl_secs: u64) {
    let Some(prefix_index) = path.prefixes.len().checked_sub(1) else {
        return;
    };
    if path
        .breakpoints
        .iter()
        .any(|breakpoint| breakpoint.prefix_index == prefix_index)
    {
        return;
    }
    path.breakpoints.push(CacheBreakpoint {
        prefix_index,
        ttl_secs,
    });
}

fn block_cache_hash_material(block: &Value) -> Value {
    let Value::Object(map) = block else {
        return block.clone();
    };

    let mut hashable = map.clone();
    hashable.remove("cache_control");
    sort_json_value(&Value::Object(hashable))
}

fn minimum_cache_tokens(request: &Value) -> i32 {
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    if contains_any(
        &model,
        &[
            "opus-4-6",
            "opus-4.6",
            "opus-4-5",
            "opus-4.5",
            "opus-4-7",
            "opus-4.7",
            "haiku-4-5",
            "haiku-4.5",
        ],
    ) {
        4096
    } else if contains_any(&model, &["sonnet-4-6", "sonnet-4.6"]) {
        2048
    } else {
        1024
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
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
    if response_has_cache_usage_fields(headers, &body) {
        return body;
    }

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

fn response_has_cache_usage_fields(headers: &reqwest::header::HeaderMap, body: &Bytes) -> bool {
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.contains("text/event-stream") {
        return sse_has_cache_usage_fields(body);
    }

    json_body_has_cache_usage_fields(body)
}

fn json_body_has_cache_usage_fields(body: &Bytes) -> bool {
    serde_json::from_slice::<Value>(body)
        .map(|value| json_has_cache_usage_fields(&value))
        .unwrap_or(false)
}

fn sse_has_cache_usage_fields(body: &Bytes) -> bool {
    let Ok(text) = std::str::from_utf8(body) else {
        return false;
    };

    text.lines().any(|line| {
        let Some(data) = line.strip_prefix("data:") else {
            return false;
        };
        let data = data.trim();
        data != "[DONE]"
            && serde_json::from_str::<Value>(data)
                .map(|event| json_has_cache_usage_fields(&event))
                .unwrap_or(false)
    })
}

fn json_has_cache_usage_fields(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            usage_has_cache_fields(map) || map.values().any(json_has_cache_usage_fields)
        }
        Value::Array(values) => values.iter().any(json_has_cache_usage_fields),
        _ => false,
    }
}

fn usage_has_cache_fields(usage: &Map<String, Value>) -> bool {
    usage.contains_key("cache_creation_input_tokens")
        || usage.contains_key("cache_read_input_tokens")
        || usage.contains_key("cache_creation")
}

fn patch_usage(usage: &mut Map<String, Value>, cache: &CacheResult) {
    if usage_has_cache_fields(usage) {
        return;
    }

    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache.cache_read_input_tokens),
    );

    usage.insert(
        "input_tokens".to_string(),
        Value::from(cache.uncached_input_tokens),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_cache_records_then_hits_breakpoint() {
        let prompt = long_text();
        let request = json!({
            "system": [{"text": prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let cache_path = compute_cache_path(&request);
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let first = lookup_or_create(
            &cache,
            "sk-test",
            &cache_path.prefixes,
            &cache_path.breakpoints,
            count_all_tokens(&request),
        )
        .await;
        assert!(first.cache_creation_input_tokens > 0);
        assert_eq!(first.cache_read_input_tokens, 0);

        let second = lookup_or_create(
            &cache,
            "sk-test",
            &cache_path.prefixes,
            &cache_path.breakpoints,
            count_all_tokens(&request),
        )
        .await;
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[tokio::test]
    async fn lookback_reuses_prior_prefix_and_writes_extension() {
        let prefix = long_text();
        let extension = long_text();
        let first_request = json!({
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": prefix, "cache_control": {"type": "ephemeral"}}]
            }]
        });
        let second_request = json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prefix, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": extension, "cache_control": {"type": "ephemeral"}}
                ]
            }]
        });
        let first_path = compute_cache_path(&first_request);
        let second_path = compute_cache_path(&second_request);
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let first = lookup_or_create(
            &cache,
            "sk-test",
            &first_path.prefixes,
            &first_path.breakpoints,
            count_all_tokens(&first_request),
        )
        .await;
        assert!(first.cache_creation_input_tokens > 0);

        let second = lookup_or_create(
            &cache,
            "sk-test",
            &second_path.prefixes,
            &second_path.breakpoints,
            count_all_tokens(&second_request),
        )
        .await;
        assert!(second.cache_read_input_tokens > 0);
        assert!(second.cache_creation_input_tokens > 0);
        assert!(second.cache_creation_input_tokens < first.cache_creation_input_tokens * 2);
    }

    #[tokio::test]
    async fn cache_control_metadata_does_not_change_message_block_hash() {
        let prompt = long_text();
        let first_request = json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let second_request = json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral", "ttl": "1h"}
                }]
            }]
        });
        let first_path = compute_cache_path(&first_request);
        let second_path = compute_cache_path(&second_request);
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let first = lookup_or_create(
            &cache,
            "sk-test",
            &first_path.prefixes,
            &first_path.breakpoints,
            count_all_tokens(&first_request),
        )
        .await;
        assert!(first.cache_creation_input_tokens > 0);

        let second = lookup_or_create(
            &cache,
            "sk-test",
            &second_path.prefixes,
            &second_path.breakpoints,
            count_all_tokens(&second_request),
        )
        .await;
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[tokio::test]
    async fn message_block_object_key_order_does_not_change_hash() {
        let prompt = long_text();
        let first_request = json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let second_request = json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "cache_control": {"type": "ephemeral"},
                    "text": prompt,
                    "type": "text"
                }]
            }]
        });
        let first_path = compute_cache_path(&first_request);
        let second_path = compute_cache_path(&second_request);
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let first = lookup_or_create(
            &cache,
            "sk-test",
            &first_path.prefixes,
            &first_path.breakpoints,
            count_all_tokens(&first_request),
        )
        .await;
        assert!(first.cache_creation_input_tokens > 0);

        let second = lookup_or_create(
            &cache,
            "sk-test",
            &second_path.prefixes,
            &second_path.breakpoints,
            count_all_tokens(&second_request),
        )
        .await;
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[test]
    fn short_prompts_do_not_create_breakpoints_under_minimum() {
        let body = br#"{
            "model": "claude-sonnet-4-6",
            "system": [{"text": "short", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        }"#;
        let headers = HeaderMap::new();

        let plan =
            compute_request_cache_plan(&Method::POST, "/v1/messages", &headers, body, "sk-test")
                .expect("cache plan should be prepared");

        assert!(plan.breakpoints.is_empty());
    }

    #[test]
    fn injects_json_usage_cache_fields() {
        let body = Bytes::from_static(br#"{"usage":{"input_tokens":123,"output_tokens":2}}"#);
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
    fn leaves_json_usage_unchanged_when_upstream_already_has_cache_fields() {
        let body = Bytes::from_static(
            br#"{"usage":{"input_tokens":11,"cache_read_input_tokens":99,"output_tokens":2}}"#,
        );
        let cache = CacheResult {
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 4,
            uncached_input_tokens: 5,
        };

        let patched = inject_json_cache_fields(body, &cache);
        let value: Value = serde_json::from_slice(&patched).expect("valid patched JSON");
        assert_eq!(value["usage"]["cache_read_input_tokens"], 99);
        assert!(value["usage"].get("cache_creation_input_tokens").is_none());
        assert_eq!(value["usage"]["input_tokens"], 11);
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

    #[test]
    fn detects_sse_upstream_cache_usage_fields() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("text/event-stream"),
        );
        let body = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1,\"cache_read_input_tokens\":42}}}\n\n",
        );

        assert!(response_has_cache_usage_fields(&headers, &body));
    }

    #[tokio::test]
    async fn cache_plan_does_not_mutate_memory_cache_before_application() {
        let prompt = long_text();
        let body = format!(
            r#"{{
            "system": [{{"text": {prompt:?}, "cache_control": {{"type": "ephemeral"}}}}],
            "messages": [{{"role": "user", "content": "hello"}}]
        }}"#
        );
        let headers = HeaderMap::new();
        let cache: MemoryCache = Arc::new(Mutex::new(HashMap::new()));

        let plan = compute_request_cache_plan(
            &Method::POST,
            "/v1/messages",
            &headers,
            body.as_bytes(),
            "sk-test",
        )
        .expect("cache plan should be prepared");

        assert!(plan.total_input_tokens > 0);
        assert!(!plan.breakpoints.is_empty());
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

    fn long_text() -> String {
        "cacheable prompt material ".repeat(240)
    }
}
