#![allow(dead_code)]

pub const RTDS_WSS_BASE: &str = "wss://ws-live-data.polymarket.com";

/// CLOB WebSocket URL - Order book updates, price changes, user events.
pub const POLYMARKET_WSS_MARKET_CHANNEL: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/market";

// Payload Field Names
pub const FIELD_UUID: &str = "uuid";
pub const FIELD_MARKET_CATEGORY: &str = "market_category";
pub const FIELD_PLATFORM: &str = "platform";
pub const FIELD_MARKET_SUBCATEGORY: &str = "market_subcategory";
pub const FIELD_END_DATE: &str = "end_date";
