use crate::request::is_json_request;
use axum::body::Bytes;
use http::{HeaderMap, Method};
use serde_json::Value;

pub(crate) fn strip_cch_from_request_body(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Bytes {
    if method != Method::POST || !matches!(path, "/v1/messages" | "/cc/v1/messages") {
        return body;
    }

    if !is_json_request(headers) {
        return body;
    }

    let Ok(mut request) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };

    if !strip_cch_from_request(&mut request) {
        return body;
    }

    match serde_json::to_vec(&request) {
        Ok(bytes) => Bytes::from(bytes),
        Err(_) => body,
    }
}

fn strip_cch_from_request(request: &mut Value) -> bool {
    match request.get_mut("system") {
        Some(Value::String(text)) => strip_cch_from_string(text),
        Some(Value::Array(system_messages)) => {
            let mut changed = false;
            for message in system_messages {
                if let Some(Value::String(text)) = message.get_mut("text") {
                    changed |= strip_cch_from_string(text);
                }
            }
            changed
        }
        _ => false,
    }
}

fn strip_cch_from_string(text: &mut String) -> bool {
    if !text.contains("x-anthropic-billing-header") {
        return false;
    }
    let stripped = strip_cch_segments(text);
    if stripped == *text {
        false
    } else {
        *text = stripped;
        true
    }
}

fn strip_cch_segments(text: &str) -> String {
    let mut changed = false;
    let segments = text
        .split(';')
        .filter(|segment| {
            let is_cch = segment.trim_start().starts_with("cch=");
            changed |= is_cch;
            !is_cch
        })
        .collect::<Vec<_>>();

    if changed {
        segments.join(";")
    } else {
        text.to_string()
    }
}
