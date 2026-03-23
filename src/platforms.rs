pub mod kalshi;
pub mod polymarket;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use polymarket_hft::client::polymarket::clob;
use std::{sync::Arc, time::Duration};

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

pub enum WsEventMessage {
    Polymarket(clob::ws::WsMessage),
    Kalshi(KalshiSocketMessage),
}

fn format_duration_ago(duration: Duration) -> String {
    let secs = duration.as_secs();

    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let minutes = (secs % 3_600) / 60;

    if days > 0 {
        format!("{}d {}h ago", days, hours)
    } else if hours > 0 {
        format!("{}h {}m ago", hours, minutes)
    } else if minutes > 0 {
        format!("{}m ago", minutes)
    } else {
        format!("{}s ago", secs)
    }
}

// tracing::info!(
//     "🚨 ARB FOUND! [{}] Total Cost: ${:.3} | Expected Payout: $1.00 | Max Size: {}",
//     cid,
//     total_cost,
//     max_size
// );

// tracing::info!(
//     "   -> Leg 1 [{:?}]: Buy {} @ ${:.3}",
//     watch.arb.anchor.platform.as_str_name(),
//     max_size,
//     a_price.best_ask
// );
// tracing::info!(
//     "   -> Leg 2 [{:?}]: Buy {} @ ${:.3}",
//     watch.arb.r#match.platform.as_str_name(),
//     max_size,
//     m_price.best_ask
// );
