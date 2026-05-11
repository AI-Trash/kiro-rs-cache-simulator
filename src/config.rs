use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Parser};
use std::env;

#[derive(Debug, Parser)]
#[command(name = "claude-cache-simulator")]
#[command(about = "Pure in-memory Claude prompt-cache simulator proxy")]
pub(crate) struct Args {
    #[arg(long)]
    pub(crate) upstream: Option<String>,

    #[arg(long)]
    pub(crate) host: Option<String>,

    #[arg(long)]
    pub(crate) port: Option<u16>,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_simulate_cache")]
    pub(crate) simulate_cache: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) no_simulate_cache: bool,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_strip_cch")]
    pub(crate) strip_cch: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) no_strip_cch: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) upstream: String,
    pub(crate) debug_cache_keys: bool,
    pub(crate) simulate_cache: bool,
    pub(crate) strip_cch: bool,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8990
}

impl Config {
    pub(crate) fn load(args: Args) -> Result<Self> {
        let host = first_non_empty([args.host, env::var("HOST").ok()]).unwrap_or_else(default_host);
        let port = match args.port {
            Some(port) => port,
            None => env::var("PORT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| {
                    value
                        .trim()
                        .parse::<u16>()
                        .with_context(|| format!("invalid PORT value {value:?}"))
                })
                .transpose()?
                .unwrap_or_else(default_port),
        };
        let upstream = first_non_empty([args.upstream, env::var("UPSTREAM").ok()])
            .ok_or_else(|| anyhow!("upstream is required via --upstream or UPSTREAM"))?;

        Ok(Config {
            host,
            port,
            upstream: upstream.trim_end_matches('/').to_string(),
            debug_cache_keys: env_flag("CACHE_SIMULATOR_DEBUG_KEYS"),
            simulate_cache: resolve_bool_toggle(
                args.simulate_cache,
                args.no_simulate_cache,
                "SIMULATE_CACHE",
                true,
            ),
            strip_cch: resolve_bool_toggle(args.strip_cch, args.no_strip_cch, "STRIP_CCH", true),
        })
    }
}

fn resolve_bool_toggle(enable_cli: bool, disable_cli: bool, env_name: &str, default: bool) -> bool {
    if enable_cli {
        true
    } else if disable_cli {
        false
    } else {
        env_bool(env_name).unwrap_or(default)
    }
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| parse_bool(value.trim()))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_flag(name: &str) -> bool {
    env_bool(name).unwrap_or(false)
}

pub(crate) fn first_non_empty(
    candidates: impl IntoIterator<Item = Option<String>>,
) -> Option<String> {
    candidates
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
