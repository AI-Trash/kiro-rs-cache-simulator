use crate::cache::{
    CacheResult, MemoryCache, compute_request_cache_plan, extract_api_key, lookup_or_create,
    purge_expired_entries,
};
use crate::cache_plan::{compute_cache_path, count_all_tokens};
use crate::cch::strip_cch_from_request_body;
use crate::config::{Args, Config, first_non_empty};
use crate::response_patch::{
    extract_sse_data, inject_json_cache_fields, patch_sse_line, response_has_cache_usage_fields,
    sse_line_has_cache_usage_fields, sse_line_is_patchable_final_usage,
    sse_line_is_patchable_start_usage,
};
use crate::sse::{patch_sse_event_text_for_test, should_apply_deferred_sse_cache};
use axum::body::Bytes;
use http::{HeaderMap, Method, header};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

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
async fn cache_hit_renews_entry_using_original_write_ttl() {
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

    {
        let mut entries = cache.lock().await;
        for entry in entries.values_mut() {
            entry.expires_at = Instant::now() + Duration::from_secs(1);
        }
    }

    let second = lookup_or_create(
        &cache,
        "sk-test",
        &second_path.prefixes,
        &second_path.breakpoints,
        count_all_tokens(&second_request),
    )
    .await;
    assert!(second.cache_read_input_tokens > 0);
    assert_eq!(second.cache_creation_input_tokens, 0);

    let renewed_expires_at = cache
        .lock()
        .await
        .values()
        .map(|entry| entry.expires_at)
        .max()
        .expect("cache entry should remain after renewal");
    assert!(renewed_expires_at > Instant::now() + Duration::from_secs(4 * 60));
    assert!(renewed_expires_at < Instant::now() + Duration::from_secs(10 * 60));
}

#[tokio::test]
async fn one_hour_cache_entry_keeps_one_hour_ttl_on_later_hit() {
    let prompt = long_text();
    let first_request = json!({
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": prompt,
                "cache_control": {"type": "ephemeral", "ttl": "1h"}
            }]
        }]
    });
    let second_request = json!({
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": prompt,
                "cache_control": {"type": "ephemeral"}
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

    {
        let mut entries = cache.lock().await;
        for entry in entries.values_mut() {
            entry.expires_at = Instant::now() + Duration::from_secs(1);
        }
    }

    let second = lookup_or_create(
        &cache,
        "sk-test",
        &second_path.prefixes,
        &second_path.breakpoints,
        count_all_tokens(&second_request),
    )
    .await;
    assert!(second.cache_read_input_tokens > 0);
    assert_eq!(second.cache_creation_input_tokens, 0);

    let renewed_expires_at = cache
        .lock()
        .await
        .values()
        .map(|entry| entry.expires_at)
        .max()
        .expect("cache entry should remain after renewal");
    assert!(renewed_expires_at > Instant::now() + Duration::from_secs(55 * 60));
}

#[tokio::test]
async fn expired_cache_entries_are_removed_before_lookup() {
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

    {
        let mut entries = cache.lock().await;
        assert!(!entries.is_empty());
        for entry in entries.values_mut() {
            entry.expires_at = Instant::now() - Duration::from_secs(1);
        }
    }

    let removed = purge_expired_entries(&cache).await;
    assert_eq!(removed, cache_path.breakpoints.len());
    assert!(cache.lock().await.is_empty());

    let second = lookup_or_create(
        &cache,
        "sk-test",
        &cache_path.prefixes,
        &cache_path.breakpoints,
        count_all_tokens(&request),
    )
    .await;
    assert_eq!(second.cache_read_input_tokens, 0);
    assert!(second.cache_creation_input_tokens > 0);

    let entries = cache.lock().await;
    assert_eq!(entries.len(), cache_path.breakpoints.len());
    assert!(
        entries
            .values()
            .all(|entry| entry.expires_at > Instant::now())
    );
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
fn strip_cch_removes_dynamic_segment_from_forwarded_body_and_cache_hash() {
    let headers = HeaderMap::new();
    let first_body = claude_code_cch_request("dee71");
    let second_body = claude_code_cch_request("460a0");

    let first_stripped =
        strip_cch_from_request_body(&Method::POST, "/v1/messages", &headers, first_body);
    let second_stripped =
        strip_cch_from_request_body(&Method::POST, "/v1/messages", &headers, second_body);
    let value: Value = serde_json::from_slice(&first_stripped).expect("valid stripped JSON");

    let system_text = value["system"][0]["text"]
        .as_str()
        .expect("system text exists");
    assert!(system_text.contains("x-anthropic-billing-header"));
    assert!(!system_text.contains("cch="));

    let first_plan = compute_request_cache_plan(
        &Method::POST,
        "/v1/messages",
        &headers,
        &first_stripped,
        "sk-test",
    )
    .expect("first plan should be prepared");
    let second_plan = compute_request_cache_plan(
        &Method::POST,
        "/v1/messages",
        &headers,
        &second_stripped,
        "sk-test",
    )
    .expect("second plan should be prepared");
    let first_breakpoint = &first_plan.breakpoints[0];
    let second_breakpoint = &second_plan.breakpoints[0];

    assert_eq!(
        first_plan.prefixes[first_breakpoint.prefix_index].hash,
        second_plan.prefixes[second_breakpoint.prefix_index].hash
    );
}

#[test]
fn strip_cch_is_scoped_to_claude_messages_json_requests() {
    let headers = HeaderMap::new();
    let body = claude_code_cch_request("abc12");
    let stripped = strip_cch_from_request_body(&Method::POST, "/v1/other", &headers, body.clone());

    assert_eq!(stripped, body);
}

#[test]
fn short_prompts_do_not_create_breakpoints_under_minimum() {
    let body = br#"{
            "model": "claude-sonnet-4-6",
            "system": [{"text": "short", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        }"#;
    let headers = HeaderMap::new();

    let plan = compute_request_cache_plan(&Method::POST, "/v1/messages", &headers, body, "sk-test")
        .expect("cache plan should be prepared");

    assert!(plan.breakpoints.is_empty());
}

#[test]
fn haiku_3_5_uses_2048_token_cache_minimum() {
    let headers = HeaderMap::new();
    let prompt = "token ".repeat(1_200);
    let body = format!(
        r#"{{
            "model": "claude-haiku-3-5",
            "system": [{{"text": {prompt:?}, "cache_control": {{"type": "ephemeral"}}}}],
            "messages": [{{"role": "user", "content": "hello"}}]
        }}"#
    );

    let plan = compute_request_cache_plan(
        &Method::POST,
        "/v1/messages",
        &headers,
        body.as_bytes(),
        "sk-test",
    )
    .expect("cache plan should be prepared");

    assert!(plan.breakpoints.is_empty());
}

#[test]
fn mythos_preview_uses_4096_token_cache_minimum() {
    let headers = HeaderMap::new();
    let prompt = "token ".repeat(2_500);
    let body = format!(
        r#"{{
            "model": "claude-mythos-preview",
            "system": [{{"text": {prompt:?}, "cache_control": {{"type": "ephemeral"}}}}],
            "messages": [{{"role": "user", "content": "hello"}}]
        }}"#
    );

    let plan = compute_request_cache_plan(
        &Method::POST,
        "/v1/messages",
        &headers,
        body.as_bytes(),
        "sk-test",
    )
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
    assert_eq!(value["usage"]["input_tokens"], 123 - 3 - 4);
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
    let line = "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100}}}";
    let cache = CacheResult {
        cache_read_input_tokens: 7,
        cache_creation_input_tokens: 3,
        uncached_input_tokens: 1,
    };

    let patched = patch_sse_line(line, &cache);
    assert!(sse_line_is_patchable_start_usage(line));
    assert!(patched.contains("\"cache_read_input_tokens\":7"));
    assert!(patched.contains("\"cache_creation_input_tokens\":3"));
    assert!(patched.contains("\"input_tokens\":90"));
}

#[test]
fn sse_data_parsing_accepts_missing_space_after_colon() {
    let line = "data:{\"type\":\"message_delta\",\"usage\":{\"input_tokens\":20}}";
    let cache = CacheResult {
        cache_read_input_tokens: 7,
        cache_creation_input_tokens: 3,
        uncached_input_tokens: 1,
    };

    assert_eq!(
        extract_sse_data(line),
        Some("{\"type\":\"message_delta\",\"usage\":{\"input_tokens\":20}}")
    );

    let patched = patch_sse_line(line, &cache);
    assert!(patched.contains("\"cache_read_input_tokens\":7"));
    assert!(patched.contains("\"cache_creation_input_tokens\":3"));
    assert!(patched.contains("\"input_tokens\":10"));
}

#[test]
fn sse_line_detection_finds_late_upstream_cache_fields() {
    let early_usage =
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}";
    let late_upstream_cache = "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":1,\"cache_read_input_tokens\":42}}";

    assert!(!sse_line_has_cache_usage_fields(early_usage));
    assert!(sse_line_has_cache_usage_fields(late_upstream_cache));
}

#[test]
fn sse_line_patching_handles_final_line_without_newline() {
    let line = "data:{\"type\":\"message_delta\",\"usage\":{\"input_tokens\":12}}";
    let cache = CacheResult {
        cache_read_input_tokens: 7,
        cache_creation_input_tokens: 0,
        uncached_input_tokens: 5,
    };

    assert!(sse_line_is_patchable_final_usage(line));

    let mut patched = patch_sse_line(line, &cache);
    if patched.ends_with('\n') {
        patched.pop();
    }

    assert!(!patched.ends_with('\n'));
    assert!(patched.contains("\"cache_read_input_tokens\":7"));
    assert!(patched.contains("\"input_tokens\":5"));
}

#[test]
fn sse_event_patching_preserves_event_and_data_frame_together() {
    let event = concat!(
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":6,\"input_tokens\":20}}\n",
        "\n",
    );
    let cache = CacheResult {
        cache_read_input_tokens: 7,
        cache_creation_input_tokens: 3,
        uncached_input_tokens: 5,
    };

    let patched = patch_sse_event_text_for_test(event, &cache);

    assert!(patched.starts_with("event: message_delta\ndata: "));
    assert!(patched.ends_with("\n\n"));
    assert!(patched.contains("\"output_tokens\":6"));
    assert!(patched.contains("\"cache_read_input_tokens\":7"));
    assert!(patched.contains("\"input_tokens\":10"));
}

#[test]
fn deferred_sse_cache_applies_only_for_complete_open_streams_with_patch_target() {
    assert!(should_apply_deferred_sse_cache(true, true, false, true));
    assert!(!should_apply_deferred_sse_cache(false, true, false, true));
    assert!(!should_apply_deferred_sse_cache(true, false, false, true));
    assert!(!should_apply_deferred_sse_cache(true, true, true, true));
    assert!(!should_apply_deferred_sse_cache(true, true, false, false));
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
fn config_loads_cli_values_without_config_file() {
    let config = Config::load(Args {
        upstream: Some(" http://127.0.0.1:8080/ ".to_string()),
        host: Some("127.0.0.1".to_string()),
        port: Some(8991),
        simulate_cache: false,
        no_simulate_cache: false,
        strip_cch: false,
        no_strip_cch: false,
    })
    .expect("CLI config should load");

    assert_eq!(config.upstream, "http://127.0.0.1:8080");
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 8991);
    assert!(config.simulate_cache);
    assert!(config.strip_cch);
}

#[test]
fn feature_toggles_can_be_disabled_by_cli() {
    let config = Config::load(Args {
        upstream: Some("http://127.0.0.1:8080".to_string()),
        host: None,
        port: None,
        simulate_cache: false,
        no_simulate_cache: true,
        strip_cch: false,
        no_strip_cch: true,
    })
    .expect("CLI config should load");

    assert!(!config.simulate_cache);
    assert!(!config.strip_cch);
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

fn claude_code_cch_request(cch: &str) -> Bytes {
    let request = json!({
        "model": "claude-opus-4-7",
        "system": [
            {"text": format!("x-anthropic-billing-header: cc_version=2.1.138.9e3; cc_entrypoint=sdk-cli; cch={cch};")},
            {"text": long_text().repeat(4), "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [{"role": "user", "content": "hello"}]
    });
    Bytes::from(serde_json::to_vec(&request).expect("request JSON should serialize"))
}
