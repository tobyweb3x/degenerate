pub mod kalshi;
pub mod polymarket;
use std::sync::Arc;
mod utils;

pub struct Platfroms {
    kalshi: Arc<kalshi::MyKalshiClient>,
    polymarket: Arc<polymarket::MyPolymarketClient>,
}

impl Platfroms {
    pub fn new(k: kalshi::MyKalshiClient, p: polymarket::MyPolymarketClient) -> Self {
        Self {
            kalshi: Arc::new(k),
            polymarket: Arc::new(p),
        }
    }

    pub fn kalshi(&self) -> &kalshi::MyKalshiClient {
        &self.kalshi
    }

    pub fn polymarket(&self) -> &polymarket::MyPolymarketClient {
        &self.polymarket
    }
}
