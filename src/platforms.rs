pub mod kalshi;
pub mod polymarket;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use polymarket_hft::client::polymarket::clob;
use std::time::Duration;

#[derive(Clone)]
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

// mod ok_bet_types {
//     use anyhow::Ok;
//     use polymarket_hft::client::polymarket::clob;
//     use serde::Deserialize;
//
//     #[derive(Debug, Deserialize)]
//     #[serde(tag = "type")]
//     pub enum WsMessage {
//         #[serde(rename = "ping")]
//         Ping,
//
//         #[serde(rename = "market.price")]
//         MarketPrice {
//             channel: String,
//             timestamp: u64,
//             data: PriceData,
//         },
//
//         Other(serde_json::Value),
//     }
//
//     #[derive(Debug, Deserialize)]
//     pub struct PriceData {
//         pub token_id: String,
//         pub platform: String,
//         pub price: String,
//         pub best_bid: String,
//         pub best_ask: String,
//         pub timestamp: String,
//     }
//
//     impl TryFrom<PriceData> for clob::ws::WsMessage {
//         type Error = anyhow::Error;
//
//         fn try_from(value: PriceData) -> Result<Self, Self::Error> {
//             if value.best_ask.is_empty() {
//                 anyhow::bail!("PriceData from okbet has empty best_ask")
//             }
//
//             if value.best_bid.is_empty() {
//                 anyhow::bail!("PriceData from okbet has empty best_bid")
//             }
//
//             if value.token_id.is_empty() {
//                 anyhow::bail!("PriceData from okbet has empty token_id")
//             }
//
//             Ok(clob::ws::WsMessage::BestBidAsk(
//                 clob::ws::BestBidAskMessage {
//                     asset_id: value.token_id,
//                     best_ask: value.best_ask,
//                     best_bid: value.best_bid,
//                     timestamp: value.timestamp,
//                     spread: String::new(),
//                     market: String::new(),
//                 },
//             ))
//         }
//     }
// }
