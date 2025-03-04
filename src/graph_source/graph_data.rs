use serde::Deserialize;

#[derive(Deserialize)]
pub struct Node {
    pub id: usize,
}

#[derive(Deserialize)]
pub struct Link {
    pub source: usize,
    pub target: usize,
    pub weight: f64,
}

#[derive(Deserialize)]
pub struct GraphData {
    pub nodes: Vec<Node>,
    pub links: Vec<Link>,
}
