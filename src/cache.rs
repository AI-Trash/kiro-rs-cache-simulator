use crate::request::is_json_request;
use http::{HeaderMap, Method, header};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;

const DEFAULT_TTL_SECS: u64 = 5 * 60;
const EXTENDED_TTL_SECS: u64 = 60 * 60;
const CACHE_LOOKBACK_BLOCKS: usize = 20;
const CACHE_PURGE_INTERVAL_SECS: u64 = 5;

pub(crate) type MemoryCache = Arc<Mutex<HashMap<String, CacheEntry>>>;

#[derive(Debug, Clone)]
pub(crate) struct CacheEntry {
    pub(crate) tokens: i32,
    pub(crate) ttl_secs: u64,
    pub(crate) expires_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct CachePrefix {
    pub(crate) hash: String,
    pub(crate) tokens: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct CacheBreakpoint {
    pub(crate) prefix_index: usize,
    pub(crate) ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CachePlan {
    pub(crate) api_key: String,
    pub(crate) prefixes: Vec<CachePrefix>,
    pub(crate) breakpoints: Vec<CacheBreakpoint>,
    pub(crate) total_input_tokens: i32,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CachePath {
    pub(crate) prefixes: Vec<CachePrefix>,
    pub(crate) breakpoints: Vec<CacheBreakpoint>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CacheResult {
    pub(crate) cache_read_input_tokens: i32,
    pub(crate) cache_creation_input_tokens: i32,
    pub(crate) uncached_input_tokens: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedCacheUpdate {
    pub(crate) result: CacheResult,
    read: Option<CacheWrite>,
    writes: Vec<CacheWrite>,
}

#[derive(Debug, Clone)]
struct CacheWrite {
    hash: String,
    tokens: i32,
    ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CacheDebugSummary {
    pub(crate) api_key_hash: String,
    pub(crate) prefix_count: usize,
    pub(crate) breakpoint_prefixes: String,
    pub(crate) prefix_sample: String,
}

pub(crate) fn cache_debug_summary(
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
        prefix_sample: prefix_sample(prefixes),
    }
}

pub(crate) fn spawn_expiry_purger(cache: MemoryCache) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(CACHE_PURGE_INTERVAL_SECS));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            purge_expired_entries(&cache).await;
        }
    });
}

pub(crate) async fn purge_expired_entries(cache: &MemoryCache) -> usize {
    let mut cache = cache.lock().await;
    remove_expired_entries(&mut cache, Instant::now())
}

fn remove_expired_entries(cache: &mut HashMap<String, CacheEntry>, now: Instant) -> usize {
    let before = cache.len();
    cache.retain(|_, entry| entry.expires_at > now);
    before - cache.len()
}

fn prefix_sample(prefixes: &[CachePrefix]) -> String {
    let prefix_count = prefixes.len();
    let mut sampled = Vec::new();
    for (index, prefix) in prefixes.iter().enumerate() {
        if prefix_count <= 24 || index < 12 || index + 12 >= prefix_count {
            sampled.push(format!(
                "{}:{}:{}",
                index,
                prefix.tokens,
                short_hash(&prefix.hash)
            ));
        } else if sampled.last().is_none_or(|entry| entry != "...") {
            sampled.push("...".to_string());
        }
    }
    sampled.join(",")
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

pub(crate) fn compute_request_cache_plan(
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
    cache_path.breakpoints.truncate(4);
    let total_input_tokens = count_all_tokens(&request).max(1);
    Some(CachePlan {
        api_key: api_key.to_string(),
        prefixes: cache_path.prefixes,
        breakpoints: cache_path.breakpoints,
        total_input_tokens,
    })
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

pub(crate) async fn lookup_or_create(
    cache: &MemoryCache,
    api_key: &str,
    prefixes: &[CachePrefix],
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> CacheResult {
    let prepared =
        prepare_cache_update(cache, api_key, prefixes, breakpoints, total_input_tokens).await;
    commit_prepared_cache_update(cache, api_key, &prepared).await;
    prepared.result
}

pub(crate) async fn prepare_cache_update(
    cache: &MemoryCache,
    api_key: &str,
    prefixes: &[CachePrefix],
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> PreparedCacheUpdate {
    if prefixes.is_empty() || breakpoints.is_empty() {
        return PreparedCacheUpdate {
            result: CacheResult {
                uncached_input_tokens: total_input_tokens,
                ..CacheResult::default()
            },
            read: None,
            writes: Vec::new(),
        };
    }

    let mut cache = cache.lock().await;
    let now = Instant::now();
    remove_expired_entries(&mut cache, now);

    let mut result = CacheResult::default();
    let mut read_tokens = 0;
    let mut read = None;

    'lookup: for breakpoint in breakpoints.iter().rev() {
        let start = breakpoint.prefix_index;
        let end = start.saturating_sub(CACHE_LOOKBACK_BLOCKS);
        for prefix_index in (end..=start).rev() {
            let prefix = &prefixes[prefix_index];
            let key = cache_key(api_key, &prefix.hash);
            if let Some(entry) = cache.get(&key) {
                read_tokens = entry.tokens;
                read = Some(CacheWrite {
                    hash: prefix.hash.clone(),
                    tokens: entry.tokens,
                    ttl_secs: entry.ttl_secs,
                });
                break 'lookup;
            }
        }
    }
    result.cache_read_input_tokens = read_tokens;

    let mut largest_written_tokens = read_tokens;
    let mut writes = Vec::new();
    for breakpoint in breakpoints {
        let prefix = &prefixes[breakpoint.prefix_index];
        if prefix.tokens > read_tokens {
            writes.push(CacheWrite {
                hash: prefix.hash.clone(),
                tokens: prefix.tokens,
                ttl_secs: breakpoint.ttl_secs,
            });
            largest_written_tokens = largest_written_tokens.max(prefix.tokens);
        }
    }
    result.cache_creation_input_tokens = (largest_written_tokens - read_tokens).max(0);

    let cached_tokens = result.cache_read_input_tokens + result.cache_creation_input_tokens;
    result.uncached_input_tokens = (total_input_tokens - cached_tokens).max(0);

    PreparedCacheUpdate {
        result,
        read,
        writes,
    }
}

pub(crate) async fn commit_prepared_cache_update(
    cache: &MemoryCache,
    api_key: &str,
    prepared: &PreparedCacheUpdate,
) {
    let mut cache = cache.lock().await;
    let now = Instant::now();
    remove_expired_entries(&mut cache, now);

    if let Some(read) = &prepared.read {
        cache.insert(
            cache_key(api_key, &read.hash),
            CacheEntry {
                tokens: read.tokens,
                ttl_secs: read.ttl_secs,
                expires_at: now + Duration::from_secs(read.ttl_secs),
            },
        );
    }

    for write in &prepared.writes {
        cache.insert(
            cache_key(api_key, &write.hash),
            CacheEntry {
                tokens: write.tokens,
                ttl_secs: write.ttl_secs,
                expires_at: now + Duration::from_secs(write.ttl_secs),
            },
        );
    }
}

fn cache_key(api_key: &str, hash: &str) -> String {
    format!("cache:{}:{}", api_key, hash)
}

pub(crate) fn extract_api_key(headers: &HeaderMap) -> String {
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
        if let (Some(scheme), Some(token), None) = (parts.next(), parts.next(), parts.next())
            && scheme.eq_ignore_ascii_case("Bearer")
        {
            return token.to_string();
        }
    }

    "anonymous".to_string()
}

pub(crate) fn compute_cache_path(request: &Value) -> CachePath {
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
            "mythos-preview",
            "mythos preview",
        ],
    ) {
        4096
    } else if contains_any(
        &model,
        &["sonnet-4-6", "sonnet-4.6", "haiku-3-5", "haiku-3.5"],
    ) {
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

    if let Some(description) = tool.get("description").and_then(Value::as_str)
        && !description.is_empty()
    {
        parts.push(format!("desc:{}", description));
    }

    if let Some(input_schema) = tool.get("input_schema")
        && !input_schema.as_object().map(Map::is_empty).unwrap_or(true)
    {
        let sorted = sort_json_value(input_schema);
        if let Ok(serialized) = serde_json::to_string(&sorted) {
            parts.push(format!("schema:{}", serialized));
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

pub(crate) fn count_all_tokens(request: &Value) -> i32 {
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
