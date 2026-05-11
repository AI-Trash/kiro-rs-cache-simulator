use crate::cache::CacheResult;
use axum::body::Bytes;
use serde_json::{Map, Value};

/// Treat upstream `usage.input_tokens` as the total input token count when the
/// upstream response has no cache usage fields, then split that total into the
/// simulator's uncached/read/creation proportions.
pub(crate) fn normalize_cache_result(
    cache: &CacheResult,
    upstream_total_input_tokens: i32,
) -> CacheResult {
    let upstream_total = upstream_total_input_tokens.max(0);
    let estimated_read = cache.cache_read_input_tokens.max(0);
    let estimated_creation = cache.cache_creation_input_tokens.max(0);
    let estimated_uncached = cache.uncached_input_tokens.max(0);
    let estimated_cached = estimated_read + estimated_creation;
    let estimated_total = estimated_cached + estimated_uncached;

    if upstream_total == 0 || estimated_cached == 0 || estimated_total == 0 {
        return CacheResult {
            uncached_input_tokens: upstream_total,
            ..CacheResult::default()
        };
    }

    let normalized_cached =
        proportional_round(upstream_total, estimated_cached, estimated_total).min(upstream_total);
    let normalized_read = if estimated_read == 0 {
        0
    } else if estimated_creation == 0 {
        normalized_cached
    } else {
        proportional_round(normalized_cached, estimated_read, estimated_cached)
    };
    let normalized_creation = (normalized_cached - normalized_read).max(0);

    CacheResult {
        cache_read_input_tokens: normalized_read,
        cache_creation_input_tokens: normalized_creation,
        uncached_input_tokens: (upstream_total - normalized_cached).max(0),
    }
}

pub(crate) fn json_usage_total_input_tokens(body: &Bytes) -> Option<i32> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    let usage = value.get("usage")?.as_object()?;
    total_input_tokens_from_usage(usage)
}

pub(crate) fn inject_json_cache_fields(body: Bytes, cache: &CacheResult) -> Bytes {
    if json_body_has_cache_usage_fields(&body) {
        return body;
    }

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

pub(crate) fn patch_sse_line(line: &str, cache: &CacheResult) -> String {
    let Some(data) = extract_sse_data(line) else {
        return format!("{}\n", line);
    };
    if data == "[DONE]" {
        return format!("{}\n", line);
    }
    let Ok(mut event) = serde_json::from_str::<Value>(data) else {
        return format!("{}\n", line);
    };
    patch_sse_event(&mut event, cache);
    match serde_json::to_string(&event) {
        Ok(serialized) => format!("data: {}\n", serialized),
        Err(_) => format!("{}\n", line),
    }
}

pub(crate) fn sse_line_has_cache_usage_fields(line: &str) -> bool {
    let Some(data) = extract_sse_data(line) else {
        return false;
    };
    data != "[DONE]"
        && serde_json::from_str::<Value>(data)
            .map(|event| json_has_cache_usage_fields(&event))
            .unwrap_or(false)
}

pub(crate) fn sse_line_is_patchable_final_usage(line: &str) -> bool {
    sse_line_is_patchable_usage_of_type(line, "message_delta")
}

pub(crate) fn sse_line_is_patchable_start_usage(line: &str) -> bool {
    sse_line_is_patchable_usage_of_type(line, "message_start")
}

pub(crate) fn sse_line_usage_total_input_tokens(line: &str) -> Option<i32> {
    let data = extract_sse_data(line)?;
    if data == "[DONE]" {
        return None;
    }
    let event = serde_json::from_str::<Value>(data).ok()?;
    event_usage(&event).and_then(total_input_tokens_from_usage)
}

fn sse_line_is_patchable_usage_of_type(line: &str, event_type: &str) -> bool {
    let Some(data) = extract_sse_data(line) else {
        return false;
    };
    if data == "[DONE]" {
        return false;
    }
    serde_json::from_str::<Value>(data)
        .map(|event| {
            event.get("type").and_then(Value::as_str) == Some(event_type)
                && event_usage(&event)
                    .and_then(total_input_tokens_from_usage)
                    .is_some()
                && !json_has_cache_usage_fields(&event)
        })
        .unwrap_or(false)
}

fn event_usage(event: &Value) -> Option<&Map<String, Value>> {
    match event.get("type").and_then(Value::as_str) {
        Some("message_start") => event
            .get("message")
            .and_then(|message| message.get("usage"))
            .and_then(Value::as_object),
        Some("message_delta") => event.get("usage").and_then(Value::as_object),
        _ => None,
    }
}

pub(crate) fn sse_line_is_done(line: &str) -> bool {
    extract_sse_data(line) == Some("[DONE]")
}

/// Extract the data payload from an SSE line, supporting both `data: ...` and `data:...`.
pub(crate) fn extract_sse_data(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("data:")?;
    Some(rest.strip_prefix(' ').unwrap_or(rest))
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

pub(crate) fn response_has_cache_usage_fields(
    headers: &reqwest::header::HeaderMap,
    body: &Bytes,
) -> bool {
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

pub(crate) fn json_has_cache_usage_fields(value: &Value) -> bool {
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

    let Some(upstream_total_input_tokens) = total_input_tokens_from_usage(usage) else {
        return;
    };
    let normalized = normalize_cache_result(cache, upstream_total_input_tokens);

    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(normalized.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(normalized.cache_read_input_tokens),
    );
    usage.insert(
        "input_tokens".to_string(),
        Value::from(normalized.uncached_input_tokens),
    );
}

fn total_input_tokens_from_usage(usage: &Map<String, Value>) -> Option<i32> {
    let value = usage.get("input_tokens")?.as_i64()?;
    i32::try_from(value).ok().map(|tokens| tokens.max(0))
}

fn proportional_round(total: i32, part: i32, denominator: i32) -> i32 {
    if total <= 0 || part <= 0 || denominator <= 0 {
        return 0;
    }
    let total = i64::from(total);
    let part = i64::from(part);
    let denominator = i64::from(denominator);
    let rounded = (total * part + denominator / 2) / denominator;
    i32::try_from(rounded).unwrap_or(i32::MAX)
}
