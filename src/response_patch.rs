use crate::cache::CacheResult;
use axum::body::Bytes;
use serde_json::{Map, Value};

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
                && event_usage(&event).is_some()
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

    let cached_tokens = cache.cache_read_input_tokens + cache.cache_creation_input_tokens;

    // Use the real input_tokens from Anthropic (which is the true total when upstream
    // has no cache) and subtract our simulated cached portion to get uncached.
    let original_input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0) as i32;
    let uncached = (original_input_tokens - cached_tokens).max(0);

    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache.cache_read_input_tokens),
    );
    usage.insert("input_tokens".to_string(), Value::from(uncached));
}
