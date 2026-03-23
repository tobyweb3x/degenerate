use crate::models::{self, protos};
use crate::platforms;
use anyhow::{self, Context};
use kalshi_rs::websocket::models::KalshiSocketMessage;
use polymarket_hft::client::polymarket::clob;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing;

pub struct Picker {
    platforms: platforms::Platfroms,
    rx: mpsc::Receiver<protos::ServerEbo>,
    tx: mpsc::Sender<protos::ClientEbo>,
    ws_rx: mpsc::Receiver<platforms::WsEventMessage>,
    engine: ArbEngine,
}

impl Picker {
    pub fn new(
        platforms: platforms::Platfroms,
        rx: mpsc::Receiver<protos::ServerEbo>,
        tx: mpsc::Sender<protos::ClientEbo>,
        ws_rx: mpsc::Receiver<platforms::WsEventMessage>,
    ) -> Self {
        Self {
            platforms,
            rx,
            tx,
            ws_rx,
            engine: ArbEngine::new(),
        }
    }

    pub async fn run_picker(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    break;
                }


                msg = self.ws_rx.recv() => { // from platforms ws
                     let Some(ev_msg) = msg else {
                        tracing::info!("got a None instead of todo, picker closed");
                        break;
                    };

                    match ev_msg {
                       platforms::WsEventMessage::Polymarket(data) => {
                           if let Err(e) = self.handle_poly_messages(data) {
                            tracing::error!("error from fn handle_poly_messages: {e:#?}");
                           };
                        },

                        platforms::WsEventMessage::Kalshi(data) => {
                            if let Err(e) = self.handle_kalshi_messages(data){
                                tracing::error!("error from fn handle_kalshi_messages: {e:#?}");
                            };
                        },
                    }
                },

                msg = self.rx.recv() => { // from grpc server
                    let Some(ebo) = msg else {
                        tracing::info!("got a None instead of todo, picker closed");
                        break;
                    };

                    let Some(action) = ebo.action else {
                        tracing::warn!("got a None-action");
                        continue;
                    };

                    let correlation_id = ebo.correlation_id;
                    match action {
                        protos::server_ebo::Action::CrossPlatformArbDiscovery(list)
                        | protos::server_ebo::Action::IntraPlatformArbDiscovery(list) => {
                            self.resolve_discovered_list(list).await
                        },

                        protos::server_ebo::Action::ConfirmedAndRun(arb ) => {
                            println!("got a ConfirmedAndRun, {arb:#?}");
                            if let Err(e) = self.find_arb(arb, correlation_id).await {
                                tracing::error!("error from find_arb: {e:#?}")
                            }
                        },
                    }

                }
            }
        }

        Ok(())
    }

    fn handle_poly_messages(&mut self, msg: clob::ws::WsMessage) -> anyhow::Result<()> {
        match msg {
            clob::ws::WsMessage::BestBidAsk(best) => {
                self.engine.on_price_update(
                    models::make_market_key(&best.asset_id.as_str(), protos::Platform::Polymarket),
                    models::TopOfBook {
                        best_bid: models::parse_f32(&best.best_bid)?,
                        best_ask: models::parse_f32(&best.best_ask)?,
                        spread: models::parse_f32(&best.spread)?,
                        timestamp_ms: best
                            .timestamp
                            .parse()
                            .context("erorr parsing timestamp to i64")?,
                        ..Default::default()
                    },
                );
            }
            _ => return Ok(()),
        }

        Ok(())
    }

    fn handle_kalshi_messages(&mut self, msg: KalshiSocketMessage) -> anyhow::Result<()> {
        match msg {
            KalshiSocketMessage::TickerUpdate(ticker) => {
                self.engine.on_price_update(
                    models::make_market_key(&ticker.msg.market_ticker, protos::Platform::Kalshi),
                    models::TopOfBook {
                        best_bid: models::parse_f32(&ticker.msg.yes_bid_dollars)?,
                        best_ask: models::parse_f32(&ticker.msg.yes_ask_dollars)?,
                        bid_size: models::parse_f32(&ticker.msg.yes_bid_size_fp)?,
                        ask_size: models::parse_f32(&ticker.msg.yes_ask_size_fp)?,
                        spread: models::parse_f32(&ticker.msg.yes_ask_dollars)?
                            - models::parse_f32(&ticker.msg.yes_bid_dollars)?,
                        timestamp_ms: ticker.msg.ts * 1_000,
                    },
                );
            }
            _ => {
                return Ok(());
            }
        }

        Ok(())
    }

    async fn find_arb(&mut self, arb: protos::Arb, correlation_id: String) -> anyhow::Result<()> {
        let (anchor_platform, anchor_token_id) = (
            arb.anchor
                .as_ref()
                .and_then(|a| a.discovery.as_ref())
                .and_then(|d| d.market_info.as_ref())
                .and_then(|m| protos::Platform::try_from(m.platform).ok())
                .context("anchor missing")?,
            arb.anchor
                .as_ref()
                .context("anchor missing")?
                .token_id
                .clone(),
        );

        let (match_platform, match_token_id) = (
            arb.r#match
                .as_ref()
                .and_then(|a| a.discovery.as_ref())
                .and_then(|d| d.market_info.as_ref())
                .and_then(|m| protos::Platform::try_from(m.platform).ok())
                .context("match missing")?,
            arb.r#match
                .as_ref()
                .context("match missing")?
                .token_id
                .clone(),
        );

        let entries = [
            (anchor_platform, anchor_token_id),
            (match_platform, match_token_id),
        ];

        // subscribe to market channel
        for (platform, token_id) in entries {
            match platform {
                protos::Platform::Polymarket => {
                    self.platforms
                        .polymarket()
                        .ws_client()
                        .subscribe_market(vec![token_id], true)
                        .await
                        .context("error subscribing polymarket token_id to ws market channel")?;
                }

                protos::Platform::Kalshi => {
                    self.platforms
                        .kalshi()
                        .ws_client()
                        .subscribe(vec!["ticker", "orderbook_delta"], vec![token_id.as_str()])
                        .await
                        .context("error subscribing kalshi ticker to ws market channel")?;
                }
            }
        }

        self.engine.register_arb(models::ArbWatch {
            correlation_id,
            arb: arb.try_into().context("error creating arbWatch")?,
        });

        Ok(())
    }

    async fn resolve_discovered_list(&self, list: protos::DiscoveredArbList) {
        for discovered_arb in list.arbs {
            let Some(anchor) = discovered_arb.anchor else {
                continue;
            };

            let Some(r#match) = discovered_arb.r#match else {
                continue;
            };

            let arb =
                match tokio::try_join!(self.get_arb_market(anchor), self.get_arb_market(r#match)) {
                    Ok((anchor_market, r#match_market)) => protos::Arb {
                        anchor: Some(anchor_market),
                        r#match: Some(match_market),
                        scored: discovered_arb.scored,
                    },

                    Err(e) => {
                        tracing::error!("discovered_arb dropped due to err: {e:#?}");
                        continue;
                    }
                };

            let _ = self
                .tx
                .send(protos::ClientEbo {
                    correlation_id: models::generate_uuid_v5(format!(
                        "{}:{}",
                        &arb.anchor.as_ref().unwrap().token_id,
                        &arb.r#match.as_ref().unwrap().token_id
                    )),
                    found_at: chrono::Utc::now().timestamp_millis(),
                    action: Some(protos::client_ebo::Action::CrossPlatformArb(arb)),
                })
                .await
                .inspect_err(|e| {
                    tracing::error!("error sending to grpc server from picker tx: {e:#?}")
                });
        }
    }

    async fn get_arb_market(
        &self,
        discovery: protos::Discovery,
    ) -> anyhow::Result<protos::ArbEssentials> {
        let market_info = discovery
            .market_info
            .as_ref()
            .context("market_info should not be None")?;

        let platform = protos::Platform::try_from(market_info.platform)?;

        // just in-case anything has been updated
        let (mut new_market, token_id) = match platform {
            protos::Platform::Kalshi => {
                self.get_arb_entity_detail_kalshi(market_info.market_id.as_str())
                    .await?
            }

            protos::Platform::Polymarket => {
                self.get_arb_entity_detail_polymarket(
                    market_info.market_id.as_str(),
                    discovery.leg(),
                )
                .await?
            }
        };

        new_market.market_category = market_info.market_category.to_string();
        new_market.market_subcategory = market_info.market_subcategory.to_string();

        Ok(protos::ArbEssentials {
            discovery: Some(protos::Discovery {
                market_info: Some(new_market),
                leg: discovery.leg,
                leg_str: discovery.leg_str,
            }),

            token_id: token_id,
        })
    }

    async fn get_arb_entity_detail_kalshi(
        &self,
        ticker: &str,
    ) -> anyhow::Result<(protos::MarketInfo, String)> {
        let market = self
            .platforms
            .kalshi()
            .http_client()
            .get_market(ticker)
            .await
            .map_err(|e| anyhow::anyhow!("error from picker kalshi get market: {e}"))?;

        anyhow::Ok((market.market.into(), ticker.to_string()))
    }

    async fn get_arb_entity_detail_polymarket(
        &self,
        slug: &str,
        leg: protos::Leg,
    ) -> anyhow::Result<(protos::MarketInfo, String)> {
        let market = self
            .platforms
            .polymarket()
            .gamma_client()
            .get_market_by_slug(slug, Some(true))
            .await
            .map_err(|e| anyhow::anyhow!("error from picker polymarket get market: {e}"))?;

        let clob_tokens = market
            .clob_token_ids
            .as_ref()
            .context("polymarket clob_token_ids should not be empty")?;

        let parsed_tokens: Vec<String> = serde_json::from_str(clob_tokens)
            .context("failed to parse clob_token_ids JSON array")?;

        let get_leg_token_id = move |leg: protos::Leg| -> anyhow::Result<String> {
            let index = leg as usize;
            parsed_tokens.get(index).cloned().ok_or_else(|| {
                anyhow::anyhow!("leg index {} is out of bounds for clob_tokens", index)
            })
        };

        anyhow::Ok((market.into(), get_leg_token_id(leg)?))
    }
}

fn needed_ask_price(tob: &models::TopOfBook, arb: &models::ArbMinifiedInfo) -> f32 {
    match arb.platform {
        protos::Platform::Kalshi => match arb.leg {
            protos::Leg::Left => tob.best_ask,
            protos::Leg::Right => 1.0 - tob.best_bid,
        },

        protos::Platform::Polymarket => tob.best_ask,
    }
}

pub struct ArbEngine {
    top_of_book: HashMap<models::MarketKey, models::TopOfBook>,
    registry: HashMap<models::MarketKey, HashSet<models::correlationID>>,
    records: HashMap<models::correlationID, models::ArbWatch>,
}

impl Default for ArbEngine {
    fn default() -> Self {
        Self {
            top_of_book: HashMap::with_capacity(512),
            registry: HashMap::with_capacity(512),
            records: HashMap::with_capacity(512),
        }
    }
}

impl ArbEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_arb(&mut self, watch: models::ArbWatch) {
        let cid = watch.correlation_id.clone();
        let anchor_key = watch.arb.anchor.market_key.clone();
        let match_key = watch.arb.r#match.market_key.clone();

        self.registry
            .entry(anchor_key)
            .or_insert_with(|| HashSet::with_capacity(10))
            .insert(cid.clone());

        self.registry
            .entry(match_key)
            .or_insert_with(|| HashSet::with_capacity(10))
            .insert(cid.clone());

        self.records.insert(cid, watch);
    }

    pub fn unregister_arb(&mut self, cid: &models::correlationID) {
        if let Some(watch) = self.records.remove(cid) {
            if let Some(set) = self.registry.get_mut(&watch.arb.anchor.market_key) {
                set.remove(cid);
            }
            if let Some(set) = self.registry.get_mut(&watch.arb.r#match.market_key) {
                set.remove(cid);
            }
        }
    }

    pub fn on_price_update(&mut self, market_key: models::MarketKey, tob: models::TopOfBook) {
        self.top_of_book.insert(market_key.clone(), tob);

        if let Some(cids) = self.registry.get(&market_key) {
            for cid in cids {
                self.evaluate_arb(&cid);
            }
        }
    }

    fn evaluate_arb(&self, cid: &models::correlationID) {
        let Some(watch) = self.records.get(cid) else {
            return;
        };

        let anchor_tob = self.top_of_book.get(watch.arb.anchor.market_key.as_str());
        let match_tob = self.top_of_book.get(watch.arb.r#match.market_key.as_str());

        if let (Some(a_tob), Some(m_tob)) = (anchor_tob, match_tob) {
            let anchor_best_ask = needed_ask_price(&a_tob, &watch.arb.anchor);
            let match_best_ask = needed_ask_price(&m_tob, &watch.arb.r#match);

            let total_cost = anchor_best_ask + match_best_ask;

            if total_cost > 0.0 && total_cost < 0.85 { // or some later threshold

                // let now = chrono::Utc::now().timestamp_millis();
                // if (a_tob.timestamp_ms - m_tob.timestamp_ms).abs() > 10 * 60 * 1000
                //     || now - a_tob.timestamp_ms > 30 * 60 * 1000
                //     || now - m_tob.timestamp_ms > 30 * 60 * 1000
                // {
                //     return;
                // }

                // TODO: Send to Execution Engine (FAK Orders)
                // execution_tx.send(ExecutionRequest { ... })
            }
        }
    }
}
