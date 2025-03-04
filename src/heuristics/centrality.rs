//! Compute node centrality
//!
//! Some algorithm is learned from lnd project.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use fnn::rpc::{graph::NodeInfo, peer::PeerId};

use crate::graph::Graph;
use anyhow::Result;

pub async fn get_node_scores(
    graph: Arc<Graph>,
    nodes: HashSet<PeerId>,
) -> Result<HashMap<PeerId, f64>> {
    let bc = BetweennessCentrality::build(graph).await?;
    let centrality = bc.get(true);
    let scores = nodes
        .into_iter()
        .map(|peer| {
            let c = centrality.get(&peer).cloned().expect("missing score");
            (peer, c)
        })
        .collect();
    Ok(scores)
}

pub struct BetweennessCentrality {
    centrality: HashMap<PeerId, f64>,
    min: f64,
    max: f64,
}

impl BetweennessCentrality {
    pub async fn build(graph: Arc<Graph>) -> Result<Self> {
        // compute centrality for all ndoes
        let tasks = (0..graph.nodes().len()).map(|id| {
            let graph = Arc::clone(&graph);
            tokio::task::spawn_blocking(move || centrality(graph.nodes(), graph.edges(), id))
        });

        // Aggregate centrality

        let mut centrality = vec![0f64; graph.nodes().len()];

        for task in tasks {
            let p = task.await?;
            debug_assert_eq!(p.len(), graph.nodes().len(), "partial len");
            for (n_idx, c) in p.into_iter().enumerate() {
                centrality[n_idx] += c;
            }
        }

        // Track min and max value
        let mut min = 0.0;
        let mut max = 0.0;
        for v in centrality.iter().cloned() {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }

        // Convert to pubkey to centrality
        // We use half of c since each channel count twice
        let centrality = centrality
            .into_iter()
            .enumerate()
            .map(|(n_idx, c)| {
                let node_id = graph.nodes()[n_idx].node_id;
                let peer = PeerId::from_public_key(&node_id.into());
                (peer, c * 0.5)
            })
            .collect();
        Ok(Self {
            centrality,
            min: min * 0.5,
            max: max * 0.5,
        })
    }

    /// Normalize centrality to 0.0 ~ 1.0 if normalize is passed
    pub fn get(&self, normalize: bool) -> HashMap<PeerId, f64> {
        let z = 1.0 / (self.max - self.min).max(1.0);

        let mut centrality = HashMap::with_capacity(self.centrality.len());

        for (k, v) in self.centrality.iter() {
            let k = k.to_owned();
            let mut v = *v;
            if normalize {
                v = (v - self.min) * z;
            }
            centrality.insert(k, v);
        }

        centrality
    }
}

// Brandes algorithm to calculate centrality
// https://www.cl.cam.ac.uk/teaching/1617/MLRD/handbook/brandes.html
//
// # Arguments
//
// - nodes: all nodes in the network
// - edges: node edges
// - s: the start node
//
fn centrality(nodes: &[NodeInfo], edges: &[Vec<usize>], s: usize) -> Vec<f64> {
    let mut centrality: Vec<f64> = vec![0.0; nodes.len()];
    // distance from s to node v
    let mut dist: Vec<i32> = vec![-1; nodes.len()];
    // precede shortest path list from s to t
    let mut pred: Vec<Vec<usize>> = vec![Vec::default(); nodes.len()];
    let mut sigma: Vec<usize> = vec![0; nodes.len()];

    let mut queue = VecDeque::default();
    let mut stack = VecDeque::default();

    // start with s
    queue.push_back(s);
    dist.insert(s, 0);
    sigma[s] = 1;

    while let Some(v) = queue.pop_front() {
        stack.push_back(v);
        for w in edges[v].clone() {
            if dist[w] == -1 {
                dist[w] = dist[v] + 1;
                queue.push_back(w);
            }
            if dist[w] == dist[v] + 1 {
                sigma[w] += sigma[v];
                pred[w].push(v);
            }
        }
    }

    let mut delta: Vec<f64> = vec![0.0; nodes.len()];

    while let Some(w) = stack.pop_back() {
        for v in pred[w].clone() {
            delta[v] += (sigma[v] as f64 / sigma[w] as f64) * (1.0 + delta[w]);
        }
        if w != s {
            centrality[w] += delta[w];
        }
    }

    centrality
}
