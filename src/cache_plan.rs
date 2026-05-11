use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::cache::{CacheBreakpoint, CachePath, CachePrefix};

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

pub(crate) fn minimum_cache_tokens(request: &Value) -> i32 {
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

pub(crate) fn is_web_search_request(request: &Value) -> bool {
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

fn finalize_hash(hasher: &Sha256) -> String {
    format!("{:x}", hasher.clone().finalize())
}

fn parse_ttl(cache_control: &Value) -> u64 {
    match cache_control.get("ttl").and_then(Value::as_str) {
        Some("1h") => 60 * 60,
        _ => 5 * 60,
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

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
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
