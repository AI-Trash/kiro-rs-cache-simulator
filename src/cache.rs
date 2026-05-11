use crate::cache_plan::{
    compute_cache_path, count_all_tokens, is_web_search_request, minimum_cache_tokens,
};
use crate::request::is_json_request;
use http::{HeaderMap, Method, header};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;

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

const CACHE_LOOKBACK_BLOCKS: usize = 20;

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
    let min_tokens = minimum_cache_tokens(&request);
    cache_path
        .breakpoints
        .retain(|breakpoint| cache_path.prefixes[breakpoint.prefix_index].tokens >= min_tokens);
    cache_path.breakpoints.truncate(4);
    let total_input_tokens = count_all_tokens(&request).max(1);
    Some(CachePlan {
        api_key: api_key.to_string(),
        prefixes: cache_path.prefixes,
        breakpoints: cache_path.breakpoints,
        total_input_tokens,
    })
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

fn cache_key(api_key: &str, hash: &str) -> String {
    format!("cache:{}:{}", api_key, hash)
}
