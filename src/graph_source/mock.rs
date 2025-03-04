use std::{fmt::Debug, future::Future, str::FromStr, sync::Arc, time::Instant};

use anyhow::Result;
use ckb_jsonrpc_types::{JsonBytes, Script, ScriptHashType};
use ckb_sdk::constants::ONE_CKB;
use ckb_types::{
    packed::OutPoint,
    prelude::{hex_string, Pack},
};
use fnn::{
    fiber::types::{Hash256, Pubkey},
    rpc::{
        channel::{Channel, ChannelState, OpenChannelParams},
        graph::{ChannelInfo, NodeInfo, UdtCfgInfos},
        info::NodeInfoResult,
        peer::{MultiAddr, PeerId},
    },
};
use rand::Rng;
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use tokio::sync::RwLock;

use crate::{
    config::{MockSourceConfig, TokenType},
    traits::GraphSource,
    utils::{conv, to_fiber},
};

use super::graph_data::GraphData;

fn random_pubkey() -> Pubkey {
    let secp = Secp256k1::new();
    let secret_key = SecretKey::new(&mut secp256k1::rand::thread_rng());
    PublicKey::from_secret_key(&secp, &secret_key).into()
}

pub struct State {
    pub node_info: NodeInfoResult,
    pub balance: u128,
    pub local_channels: Vec<Channel>,
    pub nodes: Vec<NodeInfo>,
    pub channels: Vec<ChannelInfo>,
}

impl State {
    pub fn new(node_info: NodeInfoResult, balance: u128) -> Self {
        let timestamp = Instant::now().elapsed().as_millis();
        let nodes = vec![NodeInfo {
            node_id: node_info.node_id,
            node_name: node_info.node_name.clone().unwrap_or("mock".to_string()),
            addresses: node_info.addresses.clone(),
            chain_hash: node_info.chain_hash,
            timestamp: timestamp as u64,
            auto_accept_min_ckb_funding_amount: 0,
            udt_cfg_infos: node_info.udt_cfg_infos.clone(),
        }];
        Self {
            node_info,
            balance,
            local_channels: vec![],
            nodes,
            channels: vec![],
        }
    }

    pub fn with_balance(balance: u128) -> Self {
        let mut rng = rand::rng();
        let commit_hash = hex_string(&rng.random::<[u8; 8]>());
        let node_id = random_pubkey();
        let default_funding_lock_script = conv!(Script {
            code_hash: rng.random::<[u8; 32]>().into(),
            hash_type: ScriptHashType::Data,
            args: JsonBytes::from_vec(vec![]),
        });
        let addresses = vec![MultiAddr::from_str(&format!(
            "/ip4/127.0.0.1/tcp/{}",
            rng.random_range(1000..65535)
        ))
        .unwrap()];
        let node_info = NodeInfoResult {
            version: "0.1.0".to_string(),
            commit_hash,
            node_id,
            node_name: Some("mock".to_string()),
            addresses,
            chain_hash: rng.random::<[u8; 32]>().into(),
            auto_accept_channel_ckb_funding_amount: 0,
            channel_count: 0,
            default_funding_lock_script,
            open_channel_auto_accept_min_ckb_funding_amount: 0,
            tlc_expiry_delta: 0,
            tlc_min_value: 0,
            tlc_max_value: 0,
            udt_cfg_infos: UdtCfgInfos(vec![]),
            peers_count: 0,
            pending_channel_count: 0,
            tlc_fee_proportional_millionths: 0,
        };
        Self::new(node_info, balance)
    }

    pub fn load_graph_data(&mut self, graph_data: GraphData) {
        let mut rng = rand::rng();
        let chain_hash = self.node_info.chain_hash;
        let udt_cfg_infos = self.node_info.udt_cfg_infos.clone();
        self.nodes.extend(graph_data.nodes.into_iter().map(|node| {
            // random generate addresses
            let address = MultiAddr::from_str(&format!(
                "/ip4/127.0.0.1/tcp/{}",
                rng.random_range(1000..65535)
            ))
            .unwrap();
            NodeInfo {
                node_id: random_pubkey(),
                node_name: format!("node-{}", node.id),
                addresses: vec![address],
                chain_hash,
                auto_accept_min_ckb_funding_amount: 0,
                timestamp: 0,
                udt_cfg_infos: udt_cfg_infos.clone(),
            }
        }));

        self.channels
            .extend(graph_data.links.into_iter().map(|link| ChannelInfo {
                channel_outpoint: to_fiber(OutPoint::new(rng.random::<[u8; 32]>().pack(), 0)),
                node1: self.nodes[link.source].node_id,
                node2: self.nodes[link.target].node_id,
                created_timestamp: 0,
                last_updated_timestamp_of_node1: None,
                last_updated_timestamp_of_node2: None,
                fee_rate_of_node1: None,
                fee_rate_of_node2: None,
                capacity: (link.weight * ONE_CKB as f64) as u128,
                chain_hash,
                udt_type_script: None,
            }));

        // Find channels where one of the nodes is self
        self.local_channels.extend(
            self.channels
                .iter()
                .filter(|channel| {
                    channel.node1 == self.node_info.node_id
                        || channel.node2 == self.node_info.node_id
                })
                .map(|channel| {
                    let peer_id = if channel.node1 == self.node_info.node_id {
                        PeerId::from_public_key(&channel.node2.into())
                    } else {
                        PeerId::from_public_key(&channel.node1.into())
                    };
                    Channel {
                        peer_id,
                        channel_id: rand::rng().random::<[u8; 32]>().into(),
                        is_public: true,
                        channel_outpoint: Some(channel.channel_outpoint.clone()),
                        funding_udt_type_script: channel.udt_type_script.clone(),
                        state: ChannelState::ChannelReady(),
                        local_balance: channel.capacity / 2,
                        remote_balance: channel.capacity / 2,
                        created_at: channel.created_timestamp,
                        latest_commitment_transaction_hash: None,
                        offered_tlc_balance: 0,
                        received_tlc_balance: 0,
                    }
                }),
        );

        // Update node info with local channels
        self.node_info.channel_count = self.local_channels.len() as u32;
        self.node_info.peers_count = self.local_channels.len() as u32;
    }
}
#[derive(Clone)]
pub struct MockGraphSource {
    state: Arc<RwLock<State>>,
}

impl Debug for MockGraphSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockGraphSource").finish()
    }
}

impl MockGraphSource {
    pub fn new(state: State) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn from_config(config: MockSourceConfig) -> Self {
        let mut state = State::with_balance(config.balance);
        let graph_data = std::fs::read_to_string(config.graph_data).unwrap();
        let graph_data: GraphData = serde_json::from_str(&graph_data).unwrap();
        state.load_graph_data(graph_data);
        Self::new(state)
    }
}

#[allow(clippy::manual_async_fn)]
impl GraphSource for MockGraphSource {
    fn node_info(&self) -> impl Future<Output = Result<NodeInfoResult>> {
        async { Ok(self.state.read().await.node_info.clone()) }
    }

    fn graph_nodes(&self) -> impl Future<Output = Result<Vec<NodeInfo>>> {
        // fetch all nodes
        async { Ok(self.state.read().await.nodes.clone()) }
    }

    fn graph_channels(&self) -> impl Future<Output = Result<Vec<ChannelInfo>>> {
        // fetch all channels
        async { Ok(self.state.read().await.channels.clone()) }
    }

    fn local_channels(&self) -> impl Future<Output = Result<Vec<Channel>>> {
        async {
            let state = self.state.read().await;
            Ok(state.local_channels.clone())
        }
    }

    fn connect_peer(&self, _addr: MultiAddr) -> impl Future<Output = Result<()>> {
        // Peer always connected
        async move {
            let mut state = self.state.write().await;
            state.node_info.peers_count += 1;
            Ok(())
        }
    }

    fn open_channel(&self, params: OpenChannelParams) -> impl Future<Output = Result<Hash256>> {
        async move {
            let mut state = self.state.write().await;
            // check balance
            let balance = state.balance;
            if balance < params.funding_amount {
                return Err(anyhow::anyhow!("balance not enough"));
            }
            // find the peer in nodes
            let Some(peer) = state
                .nodes
                .iter()
                .find(|node| PeerId::from_public_key(&node.node_id.into()) == params.peer_id)
            else {
                return Err(anyhow::anyhow!("peer not found"));
            };
            let mut rng = rand::rng();
            // build a local channel to peer
            let node1 = state.node_info.node_id;
            let node2 = peer.node_id;
            let peer_id = PeerId::from_public_key(&peer.node_id.into());

            let channel_outpoint = OutPoint::new(rng.random::<[u8; 32]>().pack(), 0);
            let channel_id = rng.random::<[u8; 32]>().into();
            let created_at = Instant::now().elapsed().as_millis();
            let local_balance = params.funding_amount;
            // Assume remote node has the same balance
            let remote_balance = params.funding_amount;
            let channel = Channel {
                peer_id: peer_id.clone(),
                channel_id,
                is_public: params.public.unwrap_or(true),
                channel_outpoint: Some(to_fiber(channel_outpoint.clone())),
                funding_udt_type_script: params.funding_udt_type_script.clone(),
                state: ChannelState::ChannelReady(),
                local_balance,
                remote_balance,
                created_at: created_at as u64,
                latest_commitment_transaction_hash: None,
                offered_tlc_balance: 0,
                received_tlc_balance: 0,
            };

            state.local_channels.push(channel);
            // also add channel to channels
            let channel = ChannelInfo {
                channel_outpoint: to_fiber(channel_outpoint),
                node1,
                node2,
                created_timestamp: created_at as u64,
                last_updated_timestamp_of_node1: None,
                last_updated_timestamp_of_node2: None,
                fee_rate_of_node1: None,
                fee_rate_of_node2: None,
                capacity: local_balance + remote_balance,
                chain_hash: state.node_info.chain_hash,
                udt_type_script: params.funding_udt_type_script,
            };
            state.channels.push(channel);
            // update balance
            state.balance -= params.funding_amount;
            state.node_info.channel_count += 1;
            Ok(channel_id)
        }
    }

    fn get_balance(
        &self,
        lock: Script,
        _token: TokenType,
    ) -> impl Future<Output = Result<u128>> + Send {
        async move {
            let state = self.state.read().await;
            let node_lock = conv!(state.node_info.default_funding_lock_script.clone());
            if lock == node_lock {
                Ok(state.balance)
            } else {
                Err(anyhow::anyhow!("balance not found"))
            }
        }
    }
}
