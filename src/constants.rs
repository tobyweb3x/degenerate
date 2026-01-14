#![allow(dead_code)]

pub const RTDS_WSS_BASE: &str = "wss://ws-live-data.polymarket.com";

/// CLOB WebSocket URL - Order book updates, price changes, user events.
pub const POLYMARKET_WSS_MARKET_CHANNEL: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/market";
