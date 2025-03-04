use std::str::FromStr;

use fnn::rpc::peer::{MultiAddr, PeerId};
use rand::distr::{weighted::WeightedIndex, Distribution};

// TODO: Remove after upgrade ckb_json_type to the same version
macro_rules! conv {
    ( $x:expr ) => {{
        let v = serde_json::to_value($x).expect("conv");
        serde_json::from_value(v).expect("conv")
    }};
}

pub(crate) use conv;

// TODO: Remove after upgrade ckb_gen_types to the same version
pub fn to_fiber<T: ckb_types::prelude::Entity, D: fiber_ckb_types::prelude::Entity>(x: T) -> D {
    let v = x.as_slice();
    D::from_slice(v).expect("to fiber ckb types")
}

pub fn choice_n<T: Clone>(items: Vec<(T, f64)>, n: usize) -> Vec<(T, f64)> {
    // return all items if less than n
    if items.len() < n || n == 0 {
        return items;
    }

    let mut rng = rand::rng();
    let mut dist = WeightedIndex::new(items.iter().map(|item| item.1)).unwrap();
    let mut samples = Vec::default();
    while samples.len() < n {
        let i = dist.sample(&mut rng);
        samples.push(items[i].clone());
        match dist.update_weights(&[(i, &0.0)]) {
            Ok(_) => {
                // do nothing
            }
            Err(rand::distr::weighted::Error::InsufficientNonZero) => {
                break;
            }
            Err(e) => {
                panic!("Failed to update weights: {e}");
            }
        }
    }
    samples
}

pub fn get_peer_id_from_addr(addr: &MultiAddr) -> Option<PeerId> {
    let addr_str = addr.to_string();
    let parts: Vec<&str> = addr_str.split("/").collect();
    let index = parts.iter().position(|s| *s == "p2p")?;
    let p2p_str = parts.get(index + 1)?;
    PeerId::from_str(p2p_str).ok()
}
