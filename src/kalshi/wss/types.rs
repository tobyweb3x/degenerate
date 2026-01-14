// use serde::{Deserialize, Serialize};

// #[derive(Debug, Clone, Serialize)]
// pub struct SubscriptionSchema {
//     /// unique ID of the command request.
//     pub id: u64,
//     /// subscription type.
//     pub cmd: String,
//     //
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub params: Option<SubscriptionParams>,
// }

// #[derive(Debug, Clone, Serialize)]
// #[serde(untagged)]
// pub enum SubscriptionParams {
//     SubscribeCmd(SubscribeCmdParams),
// }

// #[derive(Debug, Clone, Serialize)]
// pub struct SubscribeCmdParams {
//     /// list of channels to subscribe to.
//     channels: Vec<String>,
//     /// Subscribe to a single/multiple market.
//     #[serde(flatten)]
//     pub markets: MarketSelector,
// }

// #[derive(Debug, Clone, Serialize)]
// #[serde(untagged)]
// pub enum MarketSelector {
//     /// Subscribe to a single market. Type: string. Example: "KXBTCD-25AUG0517-T114999.99"
//     MarketTicker { market_ticker: String },

//     /// Subscribe to multiple markets. Type: array of strings. Example: ["KXBTCD-25AUG0517-T114999.99", "KXETHD-25AUG0517-T3749.99"]
//     MarketTickers { market_tickers: Vec<String> },
// }
