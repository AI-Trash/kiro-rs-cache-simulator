use crate::cache::CacheResult;
use crate::response_patch::{
    patch_sse_line, sse_line_has_cache_usage_fields, sse_line_is_done,
    sse_line_is_patchable_final_usage, sse_line_is_patchable_start_usage,
    sse_line_usage_total_input_tokens,
};
use axum::body::Bytes;
use futures_util::StreamExt;

pub(crate) struct PendingSseLine {
    pub(crate) line: String,
    pub(crate) had_newline: bool,
}

pub(crate) struct PendingSseEvent {
    pub(crate) lines: Vec<PendingSseLine>,
}

pub(crate) struct StreamedSseState {
    pub(crate) is_complete: bool,
    pub(crate) upstream_has_cache: bool,
    pub(crate) has_patch_target: bool,
    pub(crate) upstream_total_input_tokens: Option<i32>,
}

pub(crate) async fn stream_sse_with_cache_patch(
    upstream: reqwest::Response,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    cache_preview: &CacheResult,
) -> StreamedSseState {
    let mut byte_stream = upstream.bytes_stream();
    let mut byte_buffer: Vec<u8> = Vec::new();
    let mut event_lines = Vec::new();
    let mut state = StreamedSseState {
        is_complete: true,
        upstream_has_cache: false,
        has_patch_target: false,
        upstream_total_input_tokens: None,
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
            event_lines.push(PendingSseLine {
                line: line.clone(),
                had_newline: true,
            });
            if !line.trim_end_matches('\r').is_empty() {
                continue;
            }

            let event = PendingSseEvent {
                lines: std::mem::take(&mut event_lines),
            };
            if !handle_sse_event_for_streaming(tx, &mut state, event, cache_preview).await {
                state.is_complete = false;
                return state;
            }
        }
    }

    if !byte_buffer.is_empty() && state.is_complete {
        let line = String::from_utf8_lossy(&byte_buffer).into_owned();
        event_lines.push(PendingSseLine {
            line,
            had_newline: false,
        });
    }

    if !event_lines.is_empty() && state.is_complete {
        let event = PendingSseEvent { lines: event_lines };
        if !handle_sse_event_for_streaming(tx, &mut state, event, cache_preview).await {
            state.is_complete = false;
        }
    }

    state
}

async fn handle_sse_event_for_streaming(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    state: &mut StreamedSseState,
    event: PendingSseEvent,
    cache_preview: &CacheResult,
) -> bool {
    if state.upstream_has_cache {
        return send_sse_event(tx, event).await;
    }

    if sse_event_has_cache_usage_fields(&event) {
        state.upstream_has_cache = true;
        return send_sse_event(tx, event).await;
    }

    if sse_event_is_patchable_start_usage(&event) {
        state.has_patch_target = true;
        if let Some(total_input_tokens) = sse_event_usage_total_input_tokens(&event) {
            state.upstream_total_input_tokens = Some(total_input_tokens);
        }
        return send_patched_sse_event(tx, event, cache_preview).await;
    }

    if sse_event_is_patchable_final_usage(&event) {
        state.has_patch_target = true;
        if let Some(total_input_tokens) = sse_event_usage_total_input_tokens(&event) {
            state.upstream_total_input_tokens = Some(total_input_tokens);
        }
        return send_patched_sse_event(tx, event, cache_preview).await;
    }

    if sse_event_is_done(&event) {
        return send_sse_event(tx, event).await;
    }

    send_sse_event(tx, event).await
}

fn sse_event_has_cache_usage_fields(event: &PendingSseEvent) -> bool {
    event
        .lines
        .iter()
        .any(|line| sse_line_has_cache_usage_fields(&line.line))
}

fn sse_event_is_patchable_start_usage(event: &PendingSseEvent) -> bool {
    event
        .lines
        .iter()
        .any(|line| sse_line_is_patchable_start_usage(&line.line))
}

fn sse_event_is_patchable_final_usage(event: &PendingSseEvent) -> bool {
    event
        .lines
        .iter()
        .any(|line| sse_line_is_patchable_final_usage(&line.line))
}

fn sse_event_is_done(event: &PendingSseEvent) -> bool {
    event.lines.iter().any(|line| sse_line_is_done(&line.line))
}

fn sse_event_usage_total_input_tokens(event: &PendingSseEvent) -> Option<i32> {
    event
        .lines
        .iter()
        .find_map(|line| sse_line_usage_total_input_tokens(&line.line))
}

async fn send_sse_event(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    event: PendingSseEvent,
) -> bool {
    tx.send(Ok(sse_event_bytes(&event))).await.is_ok()
}

async fn send_patched_sse_event(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    event: PendingSseEvent,
    cache: &CacheResult,
) -> bool {
    tx.send(Ok(patched_sse_event_bytes(&event, cache)))
        .await
        .is_ok()
}

fn sse_event_bytes(event: &PendingSseEvent) -> Bytes {
    Bytes::from(
        event
            .lines
            .iter()
            .flat_map(|line| sse_line_bytes(line).to_vec())
            .collect::<Vec<u8>>(),
    )
}

fn patched_sse_event_bytes(event: &PendingSseEvent, cache: &CacheResult) -> Bytes {
    Bytes::from(
        event
            .lines
            .iter()
            .flat_map(|line| {
                if sse_line_is_patchable_start_usage(&line.line)
                    || sse_line_is_patchable_final_usage(&line.line)
                {
                    patched_sse_line_bytes(line, cache).to_vec()
                } else {
                    sse_line_bytes(line).to_vec()
                }
            })
            .collect::<Vec<u8>>(),
    )
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

pub(crate) async fn passthrough_sse(
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

pub(crate) fn should_commit_streamed_sse_cache(
    is_complete: bool,
    downstream_open: bool,
    upstream_has_cache: bool,
    has_patch_target: bool,
) -> bool {
    is_complete && downstream_open && !upstream_has_cache && has_patch_target
}

#[cfg(test)]
pub(crate) fn patch_sse_event_text_for_test(event_text: &str, cache: &CacheResult) -> String {
    let mut lines = Vec::new();
    for segment in event_text.split_inclusive('\n') {
        let had_newline = segment.ends_with('\n');
        let line = segment.strip_suffix('\n').unwrap_or(segment).to_string();
        lines.push(PendingSseLine { line, had_newline });
    }

    if !event_text.ends_with('\n') && lines.is_empty() {
        lines.push(PendingSseLine {
            line: event_text.to_string(),
            had_newline: false,
        });
    }

    let patched = patched_sse_event_bytes(&PendingSseEvent { lines }, cache);
    String::from_utf8(patched.to_vec()).expect("patched SSE event should be UTF-8")
}
