pub mod kalshi;
pub mod polymarket;

pub struct Platfroms {
    kalshi: kalshi::MyKalshiClient,
    polymarket: polymarket::MyPolymarketClient,
}

impl Platfroms {
    pub fn new(k: kalshi::MyKalshiClient, p: polymarket::MyPolymarketClient) -> Self {
        Self {
            kalshi: k,
            polymarket: p,
        }
    }
}
