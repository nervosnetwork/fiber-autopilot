use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use fnn::{
    fiber::types::{Hash256, Pubkey},
    rpc::{
        channel::{Channel, OpenChannelParams},
        graph::NodeInfo,
        peer::{MultiAddr, PeerId},
    },
};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    config::{AgentConfig, TokenType},
    graph::Graph,
    traits::GraphSource,
    utils::{choice_n, conv, get_peer_id_from_addr},
};

#[derive(Debug, Clone)]
struct OpenChannelCmd {
    peer: PeerId,
    funds: u128,
    token: TokenType,
    addresses: Vec<MultiAddr>,
}

/// Autopilot agent
pub struct Agent<GS> {
    pub name: String,
    /// The id of the autopilot node
    pub self_id: Pubkey,
    pub config: AgentConfig,
    pub pending: HashSet<PeerId>,
    pub source: GS,
}

impl<GS> Debug for Agent<GS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Agent")
            .field("name", &self.name)
            .field("token", &self.config.token.name())
            .finish()
    }
}

impl<GS: GraphSource + Send + Clone + Debug + 'static> Agent<GS> {
    pub fn new(name: String, self_id: Pubkey, config: AgentConfig, source: GS) -> Self {
        Agent {
            name,
            self_id,
            config,
            pending: Default::default(),
            source,
        }
    }

    #[instrument]
    pub async fn setup(name: String, config: AgentConfig, source: GS) -> Result<Self> {
        let node_info = source.node_info().await?;
        let self_id = node_info.node_id;
        Ok(Self::new(name, self_id, config, source))
    }

    #[instrument]
    pub async fn run(mut self) {
        info!("Start autopilot agent");

        let heuristics_weight = self
            .config
            .heuristics
            .heuristics
            .iter()
            .map(|h| h.weight)
            .sum::<f32>();
        if heuristics_weight != 1.0 {
            warn!(
                "Total heuristics weight is {} expected 1.0",
                heuristics_weight
            );
        }

        loop {
            if let Err(err) = self.run_once().await {
                error!("Run once {err:?}");
            }

            if self.config.interval > 0 {
                let interval = Duration::from_secs(self.config.interval);
                tokio::time::sleep(interval).await;
            }

            // exit after reach maximum number of channels
            if self.config.exit_after_max {
                let local_channels = self
                    .source
                    .local_channels()
                    .await
                    .expect("get local channels");
                if local_channels.len() >= self.config.max_chan_num {
                    info!("Reach maximum number of channels, exit...");
                    // output self node channels count
                    info!("local channels count {}", local_channels.len());
                    // output self node scores
                    let nodes = self.source.graph_nodes().await.expect("get graph nodes");
                    assert!(nodes.iter().any(|n| n.node_id == self.self_id));
                    let channels = self
                        .source
                        .graph_channels()
                        .await
                        .expect("get graph channels");
                    let graph = Arc::new(Graph::build(nodes, channels));
                    let self_id = PeerId::from_public_key(&self.self_id.into());
                    let scores = crate::heuristics::get_node_scores(
                        &self.config.heuristics,
                        graph.clone(),
                        [self_id].into_iter().collect(),
                    )
                    .await
                    .expect("get node scores");
                    for (node, score) in scores {
                        info!("Node {:?} score {}", node, score);
                    }
                    // output network nodes count
                    info!("network nodes count {}", graph.nodes().len());
                    // output network channels count
                    info!("network channels count {}", graph.channels().len());
                    break;
                }
            }
        }
    }

    pub async fn run_once(&mut self) -> Result<()> {
        let nodes = self.source.graph_nodes().await?;
        for n in &nodes {
            trace!(
                "Peer {:?}-{} {}",
                PeerId::from_public_key(&n.node_id.into()),
                n.node_name,
                n.timestamp
            );
        }
        let channels = self.source.graph_channels().await?;
        for c in &channels {
            trace!(
                "Channel {:?} {} {} {:?}",
                c.channel_outpoint,
                c.created_timestamp,
                c.capacity,
                c.udt_type_script,
            );
        }

        let local_channels = self.source.local_channels().await?;

        for c in &local_channels {
            trace!(
                "Local channel {:?} {} {}",
                c.channel_outpoint,
                c.peer_id,
                c.channel_id
            );
        }

        info!(
            "Query {} nodes {} channels {} locals from the network",
            nodes.len(),
            channels.len(),
            local_channels.len()
        );
        let graph = Arc::new(Graph::build(nodes, channels));

        // query available funds
        let self_node = self.source.node_info().await?;
        let lock = conv!(self_node.default_funding_lock_script);
        let available_funds = self
            .source
            .get_balance(lock, self.config.token.clone())
            .await?;
        let num = (self
            .config
            .max_chan_num
            .saturating_sub(local_channels.len()))
        .min(20);
        if num > 0 {
            self.open_channels(available_funds, num, graph, local_channels)
                .await
        } else {
            // output debug info
            info!(
                "Stop open channels, available_funds {} max_chan_num {} local_channels {}",
                available_funds,
                self.config.max_chan_num,
                local_channels.len()
            );
            Ok(())
        }
    }

    pub(crate) async fn open_channels(
        &mut self,
        mut available_funds: u128,
        num: usize,
        graph: Arc<Graph>,
        local_channels: Vec<Channel>,
    ) -> Result<()> {
        info!(
            "Open channels token {} available_funds {available_funds:?} num {num:?} local channels {} pendings {}",
            self.config.token.name(),
            local_channels.len(),self.pending.len()
        );
        // check connected pending channels
        for c in local_channels.iter() {
            if self.pending.remove(&c.peer_id) {
                debug!(
                    "Successfully open channel {:?} {:?} with {:?} funds {} {}",
                    c.channel_id,
                    c.channel_outpoint,
                    c.peer_id,
                    c.local_balance,
                    self.config.token.name()
                );
            }
        }

        let chan_funds = self.config.max_chan_funds.min(available_funds);
        if chan_funds < self.config.min_chan_funds {
            bail!(
                "Not enough funds to open channel, token {} available {} required {}",
                self.config.token.name(),
                available_funds,
                self.config.min_chan_funds
            );
        }

        if self.pending.len() >= self.config.max_pending {
            debug!(
                "Stop open connections since we had too many pending channels {} max_pending {}",
                self.pending.len(),
                self.config.max_pending
            );
            return Ok(());
        }

        // open channels up to max_pending
        // TODO: We should stop open channel and remove peer from pending after timeout
        let num = num.min(self.config.max_pending - self.pending.len());

        let mut ignored: HashSet<PeerId> = local_channels
            .into_iter()
            .filter_map(|c| {
                // only ignore same token local channels
                if self
                    .config
                    .token
                    .is_token(c.funding_udt_type_script.map(|s| conv!(s)))
                {
                    Some(c.peer_id)
                } else {
                    None
                }
            })
            .chain(self.pending.clone().into_iter())
            .collect();
        ignored.insert(PeerId::from_public_key(&self.self_id.into()));

        let mut nodes: HashSet<PeerId> = HashSet::default();
        let mut addresses: HashMap<PeerId, Vec<MultiAddr>> = HashMap::default();

        for node in graph.nodes() {
            // skip ignored
            let peer = PeerId::from_public_key(&node.node_id.into());
            if ignored.contains(&peer) {
                trace!("Skiping node {peer:?}");
                continue;
            }

            // skip unknown addresses
            if node.addresses.is_empty() {
                trace!("Skiping node {peer:?} has no known addresses");
                continue;
            }

            let Some(min_funding_amount) = get_min_funding_amount(&self.config.token, node) else {
                trace!(
                    "Skiping node {peer:?} since it does not support funding with {}",
                    self.config.token.name()
                );
                continue;
            };

            if min_funding_amount > chan_funds {
                trace!(
                    "Skiping node {peer:?} require high funding, peer required token {} {} chan_funds {}",
                    self.config.token.name(),
                    min_funding_amount,
                    chan_funds,
                );
                continue;
            }

            // store addresses
            addresses
                .entry(peer.clone())
                .or_default()
                .extend(node.addresses.clone());
            nodes.insert(peer);
        }

        let mut scores: Vec<(PeerId, f64)> =
            crate::heuristics::get_node_scores(&self.config.heuristics, graph, nodes)
                .await?
                .into_iter()
                .collect();

        // Insert external nodes scores
        for addr in &self.config.external_nodes {
            let Some(peer) = get_peer_id_from_addr(addr) else {
                warn!("Can't find peer id from external address {addr:?}");
                continue;
            };
            if !ignored.contains(&peer) {
                scores.push((peer.clone(), 1.0));
                addresses.insert(peer, vec![addr.to_owned()]);
            }
        }

        debug!("Get {} scores", scores.len());
        for (peer, s) in scores.iter() {
            trace!("{peer:?} {s}");
        }
        let mut candidates: Vec<OpenChannelCmd> = Vec::default();

        for (peer, _) in choice_n(scores, num) {
            let chan_funds = available_funds.min(chan_funds);
            available_funds -= chan_funds;

            if chan_funds < self.config.min_chan_funds {
                trace!(
                    "Chan funds too small chan_funds {} required {} {}",
                    chan_funds,
                    self.config.min_chan_funds,
                    self.config.token.name(),
                );
                break;
            }

            let addresses = addresses[&peer].clone();
            let token = self.config.token.clone();
            let cmd = OpenChannelCmd {
                peer,
                funds: chan_funds,
                token,
                addresses,
            };
            candidates.push(cmd);
        }

        debug!(
            "Get {} candidates, query num {} pending {}/{}",
            candidates.len(),
            num,
            self.pending.len(),
            self.config.max_pending
        );

        let mut handles = Vec::default();

        // start cmd
        for cmd in candidates {
            let peer = cmd.peer.clone();
            if self.pending.contains(&peer) {
                info!("Skipping pending connection {:?}", peer);
                continue;
            }

            self.pending.insert(peer);

            let handle = tokio::spawn(Self::execute(cmd.clone(), self.source.clone()));
            handles.push((cmd, handle));
        }

        // resolve handles
        for (cmd, handle) in handles {
            let OpenChannelCmd {
                peer,
                funds,
                addresses,
                token,
            } = cmd;
            match handle.await {
                Ok(Ok(temp_channel_id)) => {
                    debug!("Initial open channel {temp_channel_id:?} with {peer:?} {addresses:?} funds {funds} {}",token.name());
                    // We must wait for peer to accept the channel
                }
                Ok(Err(err)) => {
                    error!("Failed to open channel {peer:?} {addresses:?} {err:?}");
                    self.pending.remove(&peer);
                }
                Err(err) => {
                    error!("Failed to execute {peer:?} {addresses:?} {err:?}");
                    self.pending.remove(&peer);
                }
            }
        }

        Ok(())
    }

    async fn execute(cmd: OpenChannelCmd, source: GS) -> Result<Hash256> {
        let OpenChannelCmd {
            peer,
            funds,
            addresses,
            token,
        } = cmd;

        let address = addresses
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("No address"))?;

        source.connect_peer(address).await.context("connect peer")?;

        let funding_udt_type_script = match token {
            TokenType::Ckb => None,
            TokenType::Udt { script, .. } => Some(conv!(script)),
        };

        let params = OpenChannelParams {
            peer_id: peer,
            funding_amount: funds,
            funding_udt_type_script,
            commitment_fee_rate: None,
            public: None,
            funding_fee_rate: None,
            commitment_delay_epoch: None,
            shutdown_script: None,
            max_tlc_value_in_flight: None,
            max_tlc_number_in_flight: None,
            tlc_expiry_delta: None,
            tlc_fee_proportional_millionths: None,
            tlc_min_value: None,
        };
        let temporary_channel_id = source.open_channel(params).await.context("open channel")?;
        Ok(temporary_channel_id)
    }
}

fn get_min_funding_amount(token: &TokenType, node: &NodeInfo) -> Option<u128> {
    match token {
        TokenType::Ckb => Some(node.auto_accept_min_ckb_funding_amount as u128),
        TokenType::Udt { .. } => node.udt_cfg_infos.0.iter().find_map(|cfg| {
            if token.is_token(Some(conv!(&cfg.script))) {
                cfg.auto_accept_amount
            } else {
                None
            }
        }),
    }
}
