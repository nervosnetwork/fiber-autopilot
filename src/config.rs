use std::path::PathBuf;

use ckb_jsonrpc_types::Script;
use fnn::{fiber::serde_utils::U128Hex, rpc::peer::MultiAddr};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(Serialize, Deserialize)]
pub struct Config {
    #[serde(flatten)]
    pub source: SourceConfig,
    pub agents: Vec<AgentConfig>,
}

#[derive(Serialize, Deserialize)]
pub enum SourceConfig {
    Rpc(RpcSourceConfig),
    Mock(MockSourceConfig),
}

#[derive(Serialize, Deserialize)]
pub struct RpcSourceConfig {
    pub fiber: FiberConfig,
    pub ckb: CkbConfig,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct MockSourceConfig {
    #[serde_as(as = "U128Hex")]
    pub balance: u128,
    pub graph_data: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub struct FiberConfig {
    pub url: String,
}

#[derive(Serialize, Deserialize)]
pub struct CkbConfig {
    pub url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Heuristic {
    Random,
    Centrality,
    Richness,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeuristicItem {
    pub heuristic: Heuristic,
    pub weight: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeuristicConfig {
    pub heuristics: Vec<HeuristicItem>,
}

impl Default for HeuristicConfig {
    fn default() -> Self {
        Self {
            heuristics: vec![HeuristicItem {
                heuristic: Heuristic::Centrality,
                weight: 1.0,
            }],
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum TokenType {
    Ckb,
    Udt { name: String, script: Script },
}

impl TokenType {
    pub fn name(&self) -> &str {
        match self {
            Self::Ckb => "ckb",
            Self::Udt { name, .. } => name,
        }
    }

    pub fn is_token(&self, script: Option<Script>) -> bool {
        match (self, script) {
            (Self::Ckb, None) => true,
            (Self::Udt { script, .. }, Some(udt)) => script == &udt,
            (Self::Ckb, Some(_)) | (Self::Udt { .. }, None) => false,
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AgentConfig {
    /// Set token type
    pub token: TokenType,
    /// Open channals to external nodes without scoring
    pub external_nodes: Vec<MultiAddr>,
    /// Max channels
    pub max_chan_num: usize,
    /// Interval seconds
    pub interval: u64,
    /// Max pending channels
    pub max_pending: usize,
    /// Minimal chan size
    #[serde_as(as = "U128Hex")]
    pub min_chan_funds: u128,
    /// Max chan size
    #[serde_as(as = "U128Hex")]
    pub max_chan_funds: u128,
    #[serde(default, flatten)]
    pub heuristics: HeuristicConfig,
    /// Exit after reach maximum number of channels
    #[serde(default)]
    pub exit_after_max: bool,
}
