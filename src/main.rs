mod cache;
mod cache_plan;
mod cch;
mod config;
mod proxy;
mod request;
mod response_patch;
mod sse;

use anyhow::{Context, Result};
use axum::{Router, routing::any};
use cache::spawn_expiry_purger;
use clap::Parser;
use config::{Args, Config};
use proxy::{AppState, proxy};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "error,claude_cache_simulator=info".into()),
        )
        .init();

    let config = Config::load(Args::parse())?;
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .with_context(|| format!("invalid listen address {}:{}", config.host, config.port))?;

    let cache = Arc::new(Mutex::new(HashMap::new()));
    if config.simulate_cache {
        spawn_expiry_purger(cache.clone());
    }

    let state = AppState {
        upstream: config.upstream.clone(),
        client: reqwest::Client::builder().build()?,
        cache,
        debug_cache_keys: config.debug_cache_keys,
        simulate_cache: config.simulate_cache,
        strip_cch: config.strip_cch,
    };

    let app = Router::new()
        .fallback(any(proxy))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
