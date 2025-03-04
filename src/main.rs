mod agent;
mod config;
mod graph;
mod graph_source;
mod heuristics;
mod rpc;
mod traits;
mod utils;

use anyhow::Result;
use ckb_sdk::CkbRpcAsyncClient;
use clap::Parser;
use config::{AgentConfig, Config, SourceConfig};
use graph_source::{mock::MockGraphSource, rpc::RPCGraphSource};
use rpc::client::RPCClient;
use std::{fmt::Debug, fs};
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use traits::GraphSource;

/// This is a simple program to demonstrate clap derive usage
#[derive(Parser, Debug)]
#[command(
    version = "1.0",
    about = "Fiber autopilot, automatically open channels with peers"
)]
struct Args {
    /// Sets a custom config file
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "fiber-autopilot.toml"
    )]
    config: String,
}

fn init_log() {
    if let Ok(level) = std::env::var("RUST_LOG") {
        tracing_subscriber::fmt()
            .pretty()
            .with_env_filter(EnvFilter::new(format!(
                "{}={level}",
                env!("CARGO_PKG_NAME").replace("-", "_"),
            )))
            .init();
    } else {
        tracing_subscriber::fmt().pretty().init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_log();

    let args = Args::parse();
    info!("Using config file: {}", args.config);

    let data = fs::read_to_string(&args.config)?;
    let config: Config = toml::from_str(&data)?;
    match config.source {
        SourceConfig::Rpc(rpc_config) => {
            let fiber_client = RPCClient::new(&rpc_config.fiber.url);
            let ckb_client = CkbRpcAsyncClient::new(&rpc_config.ckb.url);
            let source = RPCGraphSource::new(fiber_client, ckb_client);
            run_agents(config.agents, source).await?;
        }
        SourceConfig::Mock(mock_config) => {
            let source = MockGraphSource::from_config(mock_config);
            run_agents(config.agents, source).await?;
        }
    }

    Ok(())
}

async fn run_agents<GS>(agents: Vec<AgentConfig>, source: GS) -> Result<()>
where
    GS: GraphSource + Send + Clone + Debug + 'static,
{
    let handle: JoinSet<_> = agents
        .into_iter()
        .enumerate()
        .map(|(index, config)| {
            let name = format!("agent-{index}");
            let source = source.clone();
            tokio::spawn(async {
                let token = config.token.name().to_string();
                match agent::Agent::setup(name, config, source).await {
                    Ok(agent) => {
                        agent.run().await;
                    }
                    Err(err) => {
                        error!("Failed to setup agent {token} error {err:?}");
                    }
                }
            })
        })
        .collect();

    info!("All agents are started {}", handle.len());

    for r in handle.join_all().await {
        if let Err(err) = r {
            error!("join error {err:?}");
        }
    }

    Ok(())
}
