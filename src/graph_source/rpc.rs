use std::{fmt::Debug, future::Future, time::Duration};

use anyhow::Result;
use ckb_jsonrpc_types::Script;
use ckb_sdk::{
    rpc::ckb_indexer::{Order, ScriptType, SearchKey, SearchKeyFilter, SearchMode},
    CkbRpcAsyncClient,
};
use fnn::{
    fiber::types::Hash256,
    rpc::{
        channel::{Channel, ListChannelsParams, OpenChannelParams},
        graph::{ChannelInfo, GraphChannelsParams, GraphNodesParams, NodeInfo},
        info::NodeInfoResult,
        peer::{ConnectPeerParams, MultiAddr},
    },
};

use crate::{config::TokenType, rpc::client::RPCClient, traits::GraphSource};

#[derive(Clone)]
pub struct RPCGraphSource {
    fiber_client: RPCClient,
    ckb_client: CkbRpcAsyncClient,
}

impl Debug for RPCGraphSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RPCGraphSource").finish()
    }
}

impl RPCGraphSource {
    pub fn new(fiber_client: RPCClient, ckb_client: CkbRpcAsyncClient) -> Self {
        Self {
            fiber_client,
            ckb_client,
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl GraphSource for RPCGraphSource {
    fn node_info(&self) -> impl Future<Output = Result<NodeInfoResult>> {
        async { self.fiber_client.node_info().await }
    }

    fn graph_nodes(&self) -> impl Future<Output = Result<Vec<NodeInfo>>> {
        // TODO: fetch all nodes
        async {
            self.fiber_client
                .graph_nodes(GraphNodesParams {
                    limit: None,
                    after: None,
                })
                .await
                .map(|r| r.nodes)
        }
    }

    fn graph_channels(&self) -> impl Future<Output = Result<Vec<ChannelInfo>>> {
        // TODO: fetch all channels
        async {
            self.fiber_client
                .graph_channels(GraphChannelsParams {
                    limit: None,
                    after: None,
                })
                .await
                .map(|r| r.channels)
        }
    }

    fn local_channels(&self) -> impl Future<Output = Result<Vec<Channel>>> {
        async {
            self.fiber_client
                .list_channels(ListChannelsParams {
                    peer_id: None,
                    include_closed: Some(false),
                })
                .await
                .map(|r| r.channels)
        }
    }

    fn connect_peer(&self, addr: MultiAddr) -> impl Future<Output = Result<()>> {
        async {
            self.fiber_client
                .connect_peer(ConnectPeerParams {
                    address: addr,
                    save: Some(true),
                })
                .await?;

            // wait
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        }
    }

    fn open_channel(&self, params: OpenChannelParams) -> impl Future<Output = Result<Hash256>> {
        async {
            self.fiber_client
                .open_channel(params)
                .await
                .map(|r| r.temporary_channel_id)
        }
    }

    fn get_balance(
        &self,
        lock: Script,
        token: TokenType,
    ) -> impl Future<Output = Result<u128>> + Send {
        async {
            match token {
                TokenType::Ckb => {
                    let search_key = SearchKey {
                        script: lock,
                        script_type: ScriptType::Lock,
                        script_search_mode: Some(SearchMode::Exact),
                        filter: Some(SearchKeyFilter {
                            script: None,
                            script_len_range: Some([0u64.into(), 1u64.into()]),
                            output_data: None,
                            output_data_len_range: Some([0u64.into(), 1u64.into()]),
                            output_data_filter_mode: None,
                            output_capacity_range: None,
                            block_range: None,
                        }),
                        with_data: None,
                        group_by_transaction: None,
                    };
                    let source = self.clone();
                    let r = source.ckb_client.get_cells_capacity(search_key).await?;
                    let capacity = r.map(|cell| cell.capacity.value()).unwrap_or_default();
                    Ok(capacity.into())
                }
                TokenType::Udt { name: _, script } => {
                    let search_key = SearchKey {
                        script: lock,
                        script_type: ScriptType::Lock,
                        script_search_mode: Some(SearchMode::Exact),
                        filter: Some(SearchKeyFilter {
                            script: Some(script),
                            script_len_range: None,
                            output_data: None,
                            output_data_len_range: Some([16u64.into(), u64::MAX.into()]),
                            output_data_filter_mode: None,
                            output_capacity_range: None,
                            block_range: None,
                        }),
                        with_data: Some(true),
                        group_by_transaction: None,
                    };
                    let source = self.clone();
                    // TODO: handle paginate
                    let r = source
                        .ckb_client
                        .get_cells(search_key, Order::Desc, 1000u32.into(), None)
                        .await?;
                    let capacity = r
                        .objects
                        .iter()
                        .filter_map(|cell| {
                            let data = cell.output_data.as_ref()?.as_bytes();
                            if data.len() > 16 {
                                let buf: [u8; 16] = data[..16].try_into().ok()?;
                                let amount = u128::from_le_bytes(buf);
                                Some(amount)
                            } else {
                                None
                            }
                        })
                        .sum::<u128>();
                    Ok(capacity)
                }
            }
        }
    }
}
