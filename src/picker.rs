pub mod comms {
    use crate::{
        models::{self, make_market_key, protos},
        platforms,
    };
    use anyhow::{self, Context};
    use chrono::{DateTime, Duration, Utc};
    use kalshi_rs::websocket::models::KalshiSocketMessage;
    use parking_lot::RwLock;
    use polymarket_hft::client::polymarket::clob;
    use rust_decimal::Decimal;
    use rust_decimal::prelude::FromPrimitive;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration as StdDuration, Instant};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing;

    pub type SharedTob = Arc<RwLock<HashMap<models::MarketKey, models::TopOfBook>>>;
    pub type SharedInflight = Arc<RwLock<HashSet<models::CorrelationId>>>;
    pub type SharedCooldown = Arc<RwLock<HashMap<models::CorrelationId, Instant>>>;

    mod book {
        use std::collections::{HashMap, HashSet};

        /// In-memory state for all active arb watches.
        ///
        /// Two indexes are kept in sync at all times:
        ///
        /// ```text
        /// registry:  MarketKey  →  {CorrelationId, …}
        /// records:   CorrelationId  →  ArbWatch (both legs)
        /// ```
        ///
        /// **registry** is the *reverse* index used by the WS price path.
        /// When a price update arrives for a market, we look it up here to
        /// find every arb that has that market as a leg, then call
        /// `evaluate_arb` on each one.  A single market can be a leg in
        /// multiple arbs simultaneously, so the value is a set.
        ///
        /// **records** is the *forward* index used everywhere else.  Given a
        /// correlation-ID we can retrieve the full `ArbWatch`, which carries
        /// both leg's `ArbMinifiedInfo` (market key, platform, close time,
        /// leg side, etc.).
        ///
        /// ### Lifecycle
        ///
        /// `register_arb`   — inserts `cid` into `registry` for **both** legs
        ///                    (anchor and match), stores the full record.
        ///
        /// `unregister_arb` — removes `cid` from `records` (retrieves both
        ///                    leg keys from the record), then removes `cid`
        ///                    from each leg's registry set.  Only when a set
        ///                    goes **empty** (no other arb still watches that
        ///                    market) do we evict the market key from
        ///                    `registry`, remove it from `top_of_book`, and
        ///                    fire the WS unsubscribe — preserving live data
        ///                    for any other arb still on that market.
        pub(super) struct Book {
            /// Reverse index: market → all arb IDs watching it.
            pub registry: HashMap<super::models::MarketKey, HashSet<super::models::CorrelationId>>,
            /// Forward index: arb ID → full watch record with both legs.
            pub records: HashMap<super::models::CorrelationId, super::models::ArbWatch>,
        }

        impl Default for Book {
            fn default() -> Self {
                Self {
                    registry: HashMap::with_capacity(512),
                    records: HashMap::with_capacity(512),
                }
            }
        }

        impl Book {
            pub fn new() -> Self {
                Self::default()
            }
        }
    }

    pub struct PickerComms {
        platforms: platforms::Platfroms,
        rx: mpsc::Receiver<protos::ServerEbo>,
        tx: mpsc::Sender<protos::ClientEbo>,
        ws_rx: mpsc::Receiver<platforms::WsEventMessage>,
        execution_tx: mpsc::Sender<models::ExecutionRequest>,
        top_of_book: SharedTob,
        in_flight: SharedInflight,
        cooldown: SharedCooldown,
        cooldown_duration: StdDuration,
        book: book::Book,
    }

    impl PickerComms {
        pub fn new(
            platforms: platforms::Platfroms,
            rx: mpsc::Receiver<protos::ServerEbo>,
            tx: mpsc::Sender<protos::ClientEbo>,
            ws_rx: mpsc::Receiver<platforms::WsEventMessage>,
            execution_tx: mpsc::Sender<models::ExecutionRequest>,
            top_of_book: SharedTob,
            in_flight: SharedInflight,
            cooldown: SharedCooldown,
        ) -> Self {
            let cooldown_secs = std::env::var("ARB_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(30);
            tracing::info!("arb cooldown after failed execution: {cooldown_secs}s");

            Self {
                platforms,
                rx,
                tx,
                ws_rx,
                execution_tx,
                top_of_book,
                in_flight,
                cooldown,
                cooldown_duration: StdDuration::from_secs(cooldown_secs),
                book: book::Book::new(),
            }
        }

        pub async fn run_picker_comms(
            &mut self,
            shutdown: CancellationToken,
        ) -> anyhow::Result<()> {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::trace!("polymarket received shutdown");
                        break;
                    }

                    msg = self.ws_rx.recv() => { // from platforms ws
                         let Some(ev_msg) = msg else {
                            tracing::info!("got a None instead of todo, picker_comms closed");
                            break;
                        };

                        // sync work
                        match ev_msg {
                           platforms::WsEventMessage::Polymarket(data) => {
                               if let Err(e) = self.handle_poly_messages(data) {
                                tracing::error!("error from fn picker:handle_poly_messages: {e:#?}");
                               };
                            },

                            platforms::WsEventMessage::Kalshi(data) => {
                                if let Err(e) = self.handle_kalshi_messages(data){
                                    tracing::error!("error from fn picker:handle_kalshi_messages: {e:#?}");
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

                        // async work
                        match action {
                            protos::server_ebo::Action::CrossPlatformArbDiscovery(list)
                            | protos::server_ebo::Action::IntraPlatformArbDiscovery(list) => {
                                self.resolve_discovered_list(list).await
                            },

                            protos::server_ebo::Action::RunningArbsResponse(arbs) => {
                                tracing::info!("got {} running arbs from server", arbs.confirmed_and_run.len());
                                for (arb, correlation_id) in arbs.confirmed_and_run.into_iter().zip(arbs.correlation_ids) {
                                    if let Err(e) = self.find_arb(arb, correlation_id).await {
                                        tracing::error!("error from find_arb(confirmed_and_run_response): {e:#?}")
                                    }
                                }
                            },

                            protos::server_ebo::Action::ConfirmedAndRun(arb ) => {
                                tracing::info!("got a ConfirmedAndRun");
                                if let Err(e) = self.find_arb(arb, correlation_id).await {
                                    tracing::error!("error from find_arb: {e:#?}")
                                }
                            },

                            protos::server_ebo::Action::DeleteRunningArbs(arb) => {
                                for correlation_id in arb.correlation_ids {
                                    if let Err(e) = self.delete_arb(correlation_id.clone()).await {
                                        tracing::error!("{e:#?} for correlation_id:{correlation_id}")
                                    }
                                }
                            }

                        }

                    }
                }
            }

            Ok(())
        }

        async fn find_arb(
            &mut self,
            arb: protos::Arb,
            correlation_id: String,
        ) -> anyhow::Result<()> {
            let (anchor_platform, anchor_close_time, anchor_token_id) = {
                let a = arb.anchor.as_ref().context("anchor missing")?;
                let d = a.discovery.as_ref().context("anchor discovery missing")?;
                let m = d
                    .market_info
                    .as_ref()
                    .context("anchor market_info missing")?;

                (
                    protos::Platform::try_from(m.platform).context("anchor platform missing")?,
                    DateTime::from_timestamp_millis(m.close_time_ms)
                        .context("anchor close_time_ms missing")?,
                    a.token_id.clone(),
                )
            };

            let (match_platform, match_close_time, match_token_id) = {
                let m_arb = arb.r#match.as_ref().context("match missing")?;
                let d = m_arb
                    .discovery
                    .as_ref()
                    .context("match discovery missing")?;
                let m = d
                    .market_info
                    .as_ref()
                    .context("match market_info missing")?;

                (
                    protos::Platform::try_from(m.platform).context("match platform missing")?,
                    DateTime::from_timestamp_millis(m.close_time_ms)
                        .context("match close_time_ms missing")?,
                    m_arb.token_id.clone(),
                )
            };

            let now = Utc::now();
            let threshold = now + Duration::hours(1);
            if (anchor_close_time > now && anchor_close_time <= threshold)
                || (match_close_time > now && match_close_time <= threshold)
            {
                tracing::warn!("arb dropped, close_time within 1hr: {}", correlation_id);
                return self.delete_arb(correlation_id).await;
            }

            let entries = [
                (anchor_platform, anchor_token_id),
                (match_platform, match_token_id),
            ];

            let mut subscribed_count = 0usize;
            for (platform, token_id) in entries {
                match platform {
                    protos::Platform::Polymarket => {
                        if let Err(e) = self
                            .platforms
                            .polymarket()
                            .add_market_ticker(&token_id)
                            .await
                        {
                            tracing::error!(
                                "error add_market_ticker for polymarket {token_id}: {e}"
                            );
                            continue;
                        };
                        subscribed_count += 1;
                        tracing::info!("[{token_id}:POLY] succesfully subscribe")
                    }

                    protos::Platform::Kalshi => {
                        if let Err(e) = self.platforms.kalshi().add_market_ticker(&token_id).await {
                            tracing::error!("error add_market_ticker for kalshi {token_id}: {e}");
                            continue;
                        }
                        subscribed_count += 1;
                        tracing::info!("[{token_id}:KALSHI] successfully subscribed")
                    }
                }
            }

            if subscribed_count < 2 {
                tracing::warn!(
                    "arb {} dropped: only {subscribed_count}/2 legs subscribed successfully",
                    correlation_id
                );
                return Ok(());
            }

            let arb_pair: models::ArbMinifiedInfoPair =
                arb.try_into().context("error creating arbWatch")?;

            self.register_arb(models::ArbWatch {
                correlation_id,
                arb: arb_pair.clone(),
            });

            self.seed_tob_from_http(&arb_pair.anchor).await;
            self.seed_tob_from_http(&arb_pair.r#match).await;

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

                let arb = match tokio::try_join!(
                    self.get_arb_market(anchor),
                    self.get_arb_market(r#match)
                ) {
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
                        action_at: chrono::Utc::now().timestamp_millis(),
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

                token_id,
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

        async fn delete_arb(&mut self, correlation_id: String) -> anyhow::Result<()> {
            self.unregister_arb(&correlation_id);

            self.tx
                .send(protos::ClientEbo {
                    action: Some(protos::client_ebo::Action::DeleteRunningArbs(
                        protos::DeleteRunningArbRequest {
                            correlation_ids: vec![correlation_id],
                        },
                    )),
                    ..Default::default()
                })
                .await
                .context("error sending DeleteRunningArbs")
        }

        fn handle_poly_messages(&mut self, msg: clob::ws::WsMessage) -> anyhow::Result<()> {
            match msg {
                clob::ws::WsMessage::BestBidAsk(best) => {
                    let best_bid = match models::parse_f32(&best.best_bid) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("BestBidAsk: failed to parse best_bid, skipping: {e}");
                            return Ok(());
                        }
                    };
                    let best_ask = match models::parse_f32(&best.best_ask) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("BestBidAsk: failed to parse best_ask, skipping: {e}");
                            return Ok(());
                        }
                    };
                    let tob_timestamp_ms = match best.timestamp.parse() {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("BestBidAsk: failed to parse timestamp, skipping: {e}");
                            return Ok(());
                        }
                    };
                    self.on_price_update(
                        models::make_market_key(&best.asset_id, protos::Platform::Polymarket),
                        models::TopOfBook {
                            best_bid,
                            best_ask,
                            spread: best_ask - best_bid,
                            tob_timestamp_ms,
                            ..Default::default()
                        },
                    );
                }

                clob::ws::WsMessage::PriceChange(changes) => {
                    // println!(
                    //     "we're getting poly PriceChange update for:{}",
                    //     changes
                    //         .price_changes
                    //         .iter()
                    //         .map(|v| v.asset_id.to_string())
                    //         .collect::<Vec<_>>()
                    //         .join(", ")
                    // );

                    for change in changes.price_changes {
                        let best_bid = match models::parse_f32(&change.best_bid) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("failed to parse best_bid, skipping: {e}");
                                continue;
                            }
                        };

                        let best_ask = match models::parse_f32(&change.best_ask) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("failed to parse best_ask, skipping: {e}");
                                continue;
                            }
                        };

                        let timestamp = match changes.timestamp.parse() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("failed to parse timestamp, skipping: {e}");
                                continue;
                            }
                        };

                        self.on_price_update(
                            models::make_market_key(&change.asset_id, protos::Platform::Polymarket),
                            models::TopOfBook {
                                best_bid,
                                best_ask,
                                spread: best_ask - best_bid,
                                tob_timestamp_ms: timestamp,
                                ..Default::default()
                            },
                        );
                    }
                }

                clob::ws::WsMessage::MarketResolved(resolve) => {
                    println!("we got a polymarket MarketResolved: {resolve:#?}");
                    for asset_id in resolve.asset_ids {
                        let market_key = make_market_key(&asset_id, protos::Platform::Polymarket);

                        let cids = match self.book.registry.get(&market_key) {
                            Some(set) => set.clone(),
                            None => continue,
                        };

                        for cid in cids {
                            self.unregister_arb(&cid);
                        }
                    }
                }

                _ => return Ok(()),
            }

            Ok(())
        }

        fn handle_kalshi_messages(&mut self, msg: KalshiSocketMessage) -> anyhow::Result<()> {
            match msg {
                KalshiSocketMessage::TickerUpdate(ticker) => {
                    let market_key = models::make_market_key(
                        &ticker.msg.market_ticker,
                        protos::Platform::Kalshi,
                    );

                    if !self.book.registry.contains_key(&market_key) {
                        return Ok(());
                    }

                    // price fields have #[serde(default)] so they're "" when absent —
                    // skip silently rather than erroring and leaving the TOB stale
                    let (best_bid, best_ask) = match (
                        models::parse_f32(&ticker.msg.yes_bid_dollars),
                        models::parse_f32(&ticker.msg.yes_ask_dollars),
                    ) {
                        (Ok(bid), Ok(ask)) => (bid, ask),
                        _ => return Ok(()),
                    };

                    let bid_size = models::parse_f32(&ticker.msg.yes_bid_size_fp).unwrap_or(0.0);
                    let ask_size = models::parse_f32(&ticker.msg.yes_ask_size_fp).unwrap_or(0.0);

                    self.on_price_update(
                        market_key,
                        models::TopOfBook {
                            best_bid,
                            best_ask,
                            bid_size,
                            ask_size,
                            spread: best_ask - best_bid,
                            tob_timestamp_ms: ticker.msg.ts * 1_000,
                            sid: Some(ticker.sid),
                        },
                    );
                }

                KalshiSocketMessage::MarketLifecycleV2(event) => {
                    // println!(
                    //     "we got a kalshi MarketLifecycleV2, event_type: {}",
                    //     &event.msg.event_type
                    // );

                    let market_key =
                        make_market_key(&event.msg.market_ticker, protos::Platform::Kalshi);

                    let Some(set) = self.book.registry.get(&market_key) else {
                        return Ok(());
                    };
                    let cids = set.clone();

                    for cid in cids {
                        let event_type = event.msg.event_type.as_str();
                        if event_type == "close_date_updated" {
                            let Some(arb) = self.book.records.get_mut(&cid) else {
                                continue;
                            };

                            let Some(new_close_time) = event.msg.close_ts else {
                                continue;
                            };

                            if arb.arb.anchor.market_key == market_key {
                                arb.arb.anchor.close_time_ms = new_close_time * 1_000;
                            }

                            if arb.arb.r#match.market_key == market_key {
                                arb.arb.r#match.close_time_ms = new_close_time * 1_000;
                            }
                        } else if event_type == "determined"
                            || event_type == "settled"
                            || event_type == "deactivated"
                        {
                            self.unregister_arb(&cid);
                        }
                    }
                }

                _ => {
                    return Ok(());
                }
            }
            Ok(())
        }

        fn register_arb(&mut self, watch: models::ArbWatch) {
            let cid = watch.correlation_id.clone();
            let anchor_market_key = watch.arb.anchor.market_key.clone();
            let match_market_key = watch.arb.r#match.market_key.clone();

            self.book
                .registry
                .entry(anchor_market_key)
                .or_insert_with(|| HashSet::with_capacity(10))
                .insert(cid.clone());

            self.book
                .registry
                .entry(match_market_key)
                .or_insert_with(|| HashSet::with_capacity(10))
                .insert(cid.clone());

            self.book.records.insert(cid, watch);
        }

        fn unregister_arb(&mut self, cid: &models::CorrelationId) {
            if let Some(watch) = self.book.records.remove(cid) {
                for key in [&watch.arb.anchor, &watch.arb.r#match] {
                    // Drop cid from this leg's watcher set; flag if now empty.
                    let should_remove =
                        if let Some(set) = self.book.registry.get_mut(key.market_key.as_str()) {
                            set.remove(cid);
                            set.is_empty() // true → no other arb watches this market
                        } else {
                            false
                        };

                    if should_remove {
                        self.book.registry.remove(key.market_key.as_str());
                        self.top_of_book.write().remove(key.market_key.as_str());

                        match key.platform {
                            protos::Platform::Kalshi => {
                                let kalshi = self.platforms.kalshi().clone();
                                let token_id = key.token_id.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = kalshi.del_market_ticker(&token_id).await {
                                        tracing::error!(
                                            "failed del_market_ticker for kalshi {token_id}: {e}"
                                        );
                                    } else {
                                        tracing::info!(
                                            "successfully del_market_ticker for kalshi {token_id}"
                                        );
                                    }
                                });
                            }

                            protos::Platform::Polymarket => {
                                let polymarket = self.platforms.polymarket().clone();
                                let token_id = key.token_id.clone();

                                tokio::spawn(async move {
                                    if let Err(e) =
                                        polymarket.del_market_ticker(vec![token_id.clone()]).await
                                    {
                                        tracing::error!(
                                            "Failed background unsubscribe for polymarket: {e}"
                                        );
                                        return;
                                    }

                                    tracing::info!(
                                        "succesfully unsubcribe for polymark: asset_id {token_id}"
                                    );
                                });
                            }
                        }
                    }
                }
            }
        }

        async fn seed_tob_from_http(&mut self, leg: &models::ArbMinifiedInfo) {
            let tob = match leg.platform {
                protos::Platform::Polymarket => {
                    let summary = match self
                        .platforms
                        .polymarket()
                        .get_order_book(&leg.token_id)
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!(
                                "[http-seed:POLY] {} order book failed: {e}",
                                leg.token_id
                            );
                            return;
                        }
                    };

                    // asks: lowest first per docs → first = best ask (required to proceed)
                    // bids: highest first per docs → first = best bid (nice to have, default 0.0)
                    let Some(best_ask) = summary
                        .asks
                        .first()
                        .and_then(|l| l.price.parse::<f32>().ok())
                    else {
                        tracing::warn!("[http-seed:POLY] {} asks empty — skipping", leg.token_id);
                        return;
                    };

                    let best_bid = summary
                        .bids
                        .first()
                        .and_then(|l| l.price.parse::<f32>().ok())
                        .unwrap_or(0.0);

                    // Use the orderbook's own timestamp so it sits in the same
                    // time domain as the WS BestBidAsk/PriceChange timestamps.
                    let tob_timestamp_ms = summary
                        .timestamp
                        .parse::<i64>()
                        .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());

                    tracing::info!("[http-seed:POLY:{}] ask={best_ask:.4}", leg.market_key);
                    models::TopOfBook {
                        best_ask,
                        best_bid,
                        spread: best_ask - best_bid,
                        tob_timestamp_ms,
                        ..Default::default()
                    }
                }

                protos::Platform::Kalshi => {
                    let ob = match self
                        .platforms
                        .kalshi()
                        .http_client()
                        .get_market_orderbook(&leg.market_id, Some(90))
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                "[http-seed:KALSHI] {} order book failed: {e}",
                                leg.market_id
                            );
                            return;
                        }
                    };

                    // YES ask = 1 − best_NO_bid  (required — cheapest way to buy YES)
                    // YES bid = best_YES_bid      (optional — only needed for Right/NO leg eval)
                    // Both Kalshi books are sorted ascending → last = highest (best) bid.
                    let Some(yes_ask) = ob
                        .orderbook_fp
                        .no_dollars
                        .as_deref()
                        .and_then(|v| v.last())
                        .and_then(|(p, _)| p.parse::<f32>().ok())
                        .map(|no_bid| 1.0_f32 - no_bid)
                    else {
                        tracing::warn!(
                            "[http-seed:KALSHI] {} NO book empty — skipping",
                            leg.market_id
                        );
                        return;
                    };

                    let yes_bid = ob
                        .orderbook_fp
                        .yes_dollars
                        .as_deref()
                        .and_then(|v| v.last())
                        .and_then(|(p, _)| p.parse::<f32>().ok())
                        .unwrap_or(0.0);

                    tracing::info!("[http-seed:KALSHI:{}] ask={yes_ask:.4}", leg.market_key);
                    models::TopOfBook {
                        best_ask: yes_ask,
                        best_bid: yes_bid,
                        spread: yes_ask - yes_bid,
                        tob_timestamp_ms: chrono::Utc::now().timestamp_millis(),
                        ..Default::default()
                    }
                }
            };

            self.on_price_update(leg.market_key.clone(), tob);
        }

        pub fn on_price_update(&mut self, market_key: models::MarketKey, tob: models::TopOfBook) {
            let now_ms = chrono::Utc::now().timestamp_millis();

            {
                let guard = self.top_of_book.read();
                if let Some(existing) = guard.get(&market_key) {
                    if tob.tob_timestamp_ms <= existing.tob_timestamp_ms {
                        let stale_by_secs =
                            (existing.tob_timestamp_ms - tob.tob_timestamp_ms) as f64 / 1_000.0;
                        // tracing::warn!(
                        //     "[{}:tob STALE dropped] stale_by={:.1}s",
                        //     market_key,
                        //     stale_by_secs,
                        // );
                        return;
                    }

                    let gap_secs =
                        (tob.tob_timestamp_ms - existing.tob_timestamp_ms) as f64 / 1_000.0;
                    let age_secs = (now_ms - tob.tob_timestamp_ms) as f64 / 1_000.0;
                    // tracing::info!(
                    //     "[{}:tob update] gap={:.1}s ago={:.1}s ask={:.4}",
                    //     market_key,
                    //     gap_secs,
                    //     age_secs,
                    //     tob.best_ask,
                    // );
                }
            }

            self.top_of_book.write().insert(market_key.clone(), tob);

            if let Some(cids) = self.book.registry.get(&market_key) {
                for cid in cids {
                    self.evaluate_arb(cid);
                }
            }
        }

        fn evaluate_arb(&self, cid: &models::CorrelationId) {
            if self.in_flight.read().contains(cid) {
                return;
            }

            // cooldown: skip arbs that recently failed at execution
            {
                let guard = self.cooldown.read();
                if let Some(&cooled_at) = guard.get(cid) {
                    if cooled_at.elapsed() < self.cooldown_duration {
                        return;
                    }
                    drop(guard);
                    self.cooldown.write().remove(cid);
                }
            }

            let Some(watch) = self.book.records.get(cid) else {
                return;
            };

            // close-time guard: skip arbs within 10mins of expiry
            let now = Utc::now();
            let threshold = now + Duration::minutes(10);
            let anchor_close = DateTime::from_timestamp_millis(watch.arb.anchor.close_time_ms);
            let match_close = DateTime::from_timestamp_millis(watch.arb.r#match.close_time_ms);
            if let (Some(ac), Some(mc)) = (anchor_close, match_close) {
                if (ac > now && ac <= threshold) || (mc > now && mc <= threshold) {
                    tracing::warn!(
                        "evaluate_arb: arb {cid} close_time within 10mins, skipping execution"
                    );
                    return;
                }
            }

            let tob = self.top_of_book.read();
            let anchor_tob = tob.get(watch.arb.anchor.market_key.as_str());
            let match_tob = tob.get(watch.arb.r#match.market_key.as_str());

            if let (Some(a_tob), Some(m_tob)) = (anchor_tob, match_tob) {
                let anchor_best_ask = needed_ask_price(a_tob, &watch.arb.anchor);
                let match_best_ask = needed_ask_price(m_tob, &watch.arb.r#match);

                let total_cost = anchor_best_ask + match_best_ask;

                if total_cost > 0.0 && total_cost < 0.95 {
                    let anchor_ws_price =
                        Decimal::from_f32(anchor_best_ask).unwrap_or(Decimal::ZERO);
                    let match_ws_price = Decimal::from_f32(match_best_ask).unwrap_or(Decimal::ZERO);
                    let anchor_ws_size =
                        Decimal::from_f32(needed_ask_size(a_tob, &watch.arb.anchor))
                            .unwrap_or(Decimal::ZERO);
                    let match_ws_size =
                        Decimal::from_f32(needed_ask_size(m_tob, &watch.arb.r#match))
                            .unwrap_or(Decimal::ZERO);

                    if let Err(e) = self.execution_tx.try_send(models::ExecutionRequest {
                        correlation_id: cid.clone(),
                        anchor: watch.arb.anchor.clone(),
                        r#match: watch.arb.r#match.clone(),
                        anchor_ws_price,
                        match_ws_price,
                        anchor_ws_size,
                        match_ws_size,
                    }) {
                        tracing::warn!(
                            "evaluate_arb: execution channel full, arb opportunity dropped for {cid}: {e:#?}"
                        );
                    } else {
                        self.in_flight.write().insert(cid.clone());
                    }
                }
            }
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

    fn needed_ask_size(tob: &models::TopOfBook, arb: &models::ArbMinifiedInfo) -> f32 {
        match arb.platform {
            protos::Platform::Kalshi => match arb.leg {
                protos::Leg::Left => tob.ask_size,
                protos::Leg::Right => tob.bid_size,
            },
            protos::Platform::Polymarket => tob.ask_size,
        }
    }
}

pub mod exec {
    use crate::{
        models::{self, protos},
        platforms,
    };
    use alloy_primitives::U256;
    use alloy_signer_local::PrivateKeySigner;
    use anyhow::{self, Context};
    use kalshi_rs::portfolio::models::CreateOrderRequest;
    use parking_lot::Mutex;
    use polymarket_client_sdk::{
        auth::{Normal, state::Authenticated},
        clob::{
            self,
            types::{AssetType, request::BalanceAllowanceRequest},
        },
    };
    use rust_decimal::prelude::FromStr;
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing;
    use uuid::Uuid;

    // Defaults used when the env vars are absent or invalid
    const DEFAULT_TRADE_FRACTION: Decimal = Decimal::from_parts(20, 0, 0, false, 2); // 0.20
    const DEFAULT_MIN_TRADE_SIZE: Decimal = Decimal::from_parts(2, 0, 0, false, 0); // $2

    /// Read a `Decimal` from an env var.
    /// Empty / whitespace-only / unparseable values all fall back to `default`.
    /// `TRADE_FRACTION` is clamped to (0, 1). `MIN_TRADE_SIZE` must be > 0.
    fn parse_env_decimal(key: &str, default: Decimal) -> Decimal {
        let raw = match std::env::var(key) {
            Ok(s) if !s.trim().is_empty() => s,
            _ => {
                tracing::info!("{key} not set, using default {default}");
                return default;
            }
        };

        match Decimal::from_str(raw.trim()) {
            Ok(v) if v > Decimal::ZERO && (key != "TRADE_FRACTION" || v < Decimal::ONE) => {
                tracing::info!("{key}={v} (from env)");
                v
            }
            Ok(v) => {
                tracing::warn!(
                    "{key}={v} is out of range (must be > 0{}), using default {default}",
                    if key == "TRADE_FRACTION" {
                        " and < 1"
                    } else {
                        ""
                    }
                );
                default
            }
            Err(e) => {
                tracing::warn!("{key}={raw:?} could not be parsed ({e}), using default {default}");
                default
            }
        }
    }

    /// Tracks available capital per platform.
    ///
    /// Uses `parking_lot::Mutex<Decimal>` so `deduct_fill` / `set_*` can be called
    /// from `&self` methods — both `Send + Sync`, safe across tokio::spawn futures.
    ///
    /// `trade_fraction` and `min_trade_size` are loaded from env at construction:
    ///   TRADE_FRACTION  — fraction of the smaller balance per trade (default 0.20)
    ///   MIN_TRADE_SIZE  — minimum contract count to bother trading   (default 5)
    #[derive(Debug)]
    pub struct BalanceTracker {
        kalshi_usdc: Mutex<Decimal>,
        polymarket_usdc: Mutex<Decimal>,
        trade_fraction: Decimal,
        min_trade_size: Decimal,
    }

    impl BalanceTracker {
        pub fn new() -> Self {
            Self {
                kalshi_usdc: Mutex::new(Decimal::ZERO),
                polymarket_usdc: Mutex::new(Decimal::ZERO),
                trade_fraction: parse_env_decimal("TRADE_FRACTION", DEFAULT_TRADE_FRACTION),
                min_trade_size: parse_env_decimal("MIN_TRADE_SIZE", DEFAULT_MIN_TRADE_SIZE),
            }
        }

        pub fn kalshi(&self) -> Decimal {
            *self.kalshi_usdc.lock()
        }
        pub fn poly(&self) -> Decimal {
            *self.polymarket_usdc.lock()
        }
        pub fn set_kalshi(&self, v: Decimal) {
            *self.kalshi_usdc.lock() = v;
        }
        pub fn set_poly(&self, v: Decimal) {
            *self.polymarket_usdc.lock() = v;
        }

        pub fn max_trade_dollars(&self) -> Decimal {
            let effective = self.kalshi().min(self.poly());
            let budget = (effective * self.trade_fraction).floor();
            budget
        }

        /// Compute execution size (contracts) given prices and market liquidity.
        ///
        /// Returns `None` when the total dollar cost of the trade would be below
        /// `min_trade_size` — keeping the floor in dollars on both sides.
        pub fn compute_size(
            &self,
            anchor_price: Decimal,
            match_price: Decimal,
            market_liq: Decimal,
        ) -> Option<Decimal> {
            let budget = self.max_trade_dollars();
            let cost_per_unit = anchor_price + match_price;
            if cost_per_unit.is_zero() {
                return None;
            }

            let size = (budget / cost_per_unit).floor().min(market_liq.floor());
            let trade_dollars = size * cost_per_unit;
            if trade_dollars < self.min_trade_size {
                None
            } else {
                Some(size)
            }
        }

        /// Deduct the dollar cost of a fill from the correct platform balance.
        /// Platform-aware: each leg debits its own side.
        pub fn deduct_fill(
            &self,
            anchor_platform: protos::Platform,
            anchor_price: Decimal,
            match_platform: protos::Platform,
            match_price: Decimal,
            size: Decimal,
        ) {
            let deduct = |platform: protos::Platform, price: Decimal| {
                let cost = price * size;
                match platform {
                    protos::Platform::Kalshi => *self.kalshi_usdc.lock() -= cost,
                    protos::Platform::Polymarket => *self.polymarket_usdc.lock() -= cost,
                }
            };
            deduct(anchor_platform, anchor_price);
            deduct(match_platform, match_price);
        }
    }

    pub struct PickerExec {
        platforms: platforms::Platfroms,
        execution_rx: mpsc::Receiver<models::ExecutionRequest>,
        polymarket_clob_client: clob::Client<Authenticated<Normal>>,
        balances: BalanceTracker,
        poly_signer: PrivateKeySigner,
        tx: mpsc::Sender<protos::ClientEbo>,
        top_of_book: super::comms::SharedTob,
        in_flight: super::comms::SharedInflight,
        cooldown: super::comms::SharedCooldown,
        optimistic: bool,
    }

    impl PickerExec {
        pub fn new(
            platforms: platforms::Platfroms,
            execution_rx: mpsc::Receiver<models::ExecutionRequest>,
            polymarket_clob_client: clob::Client<Authenticated<Normal>>,
            poly_signer: PrivateKeySigner,
            tx: mpsc::Sender<protos::ClientEbo>,
            top_of_book: super::comms::SharedTob,
            in_flight: super::comms::SharedInflight,
            cooldown: super::comms::SharedCooldown,
            optimistic: bool,
        ) -> Self {
            Self {
                platforms,
                execution_rx,
                polymarket_clob_client,
                poly_signer,
                tx,
                top_of_book,
                in_flight,
                cooldown,
                optimistic,
                balances: BalanceTracker::new(),
            }
        }

        pub async fn run_picker_exe(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
            // Seed balances before we start evaluating any trades
            if let Err(e) = self.refresh_and_set_balances().await {
                tracing::warn!("initial balance fetch failed (will retry after first trade): {e}");
            }

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::trace!("picker_exec received shutdown");
                        break;
                    }

                    msg = self.execution_rx.recv() => {
                        let Some(ev_msg) = msg else {
                            tracing::info!("got a None instead of todo, picker_executions closed");
                            break;
                        };

                        if let Err(e) = self.handle_execution_request(ev_msg).await {
                            tracing::error!("err from handle_execution_request: {e:#?}");
                        }

                        // Re-sync true balances from the exchange after every trade
                        // (covers fills, partial fills, and any settled hedge sells)
                        if let Err(e) = self.refresh_and_set_balances().await {
                            tracing::warn!("post-trade balance refresh failed: {e}");
                        }
                    }
                }
            }
            Ok(())
        }

        async fn refresh_and_set_balances(&self) -> anyhow::Result<()> {
            // Kalshi: balance is returned in cents
            let kalshi_bal = self
                .platforms
                .kalshi()
                .http_client()
                .get_balance()
                .await
                .context("failed to fetch kalshi balance")?;
            self.balances
                .set_kalshi(Decimal::from(kalshi_bal.balance) / Decimal::ONE_HUNDRED);

            // Polymarket: USDC collateral balance via CLOB balance-allowance endpoint
            match self
                .polymarket_clob_client
                .balance_allowance(
                    BalanceAllowanceRequest::builder()
                        .asset_type(AssetType::Collateral)
                        .build(),
                )
                .await
            {
                Ok(resp) => self.balances.set_poly(resp.balance),
                Err(e) => tracing::warn!(
                    "polymarket balance fetch failed, keeping last known ${}: {e}",
                    self.balances.poly()
                ),
            }

            tracing::info!(
                "balances — Kalshi: ${}, Polymarket: ${}",
                self.balances.kalshi(),
                self.balances.poly()
            );
            Ok(())
        }

        async fn handle_execution_request(
            &self,
            req: models::ExecutionRequest,
        ) -> anyhow::Result<()> {
            tracing::info!(
                "[{}] from tob ({}) | anchor: ({:?} ws_price={}) | match: ({:?} ws_price={})",
                req.correlation_id,
                if self.optimistic {
                    "optimistic"
                } else {
                    "http"
                },
                req.anchor.platform,
                req.anchor_ws_price,
                req.r#match.platform,
                req.match_ws_price,
            );

            let (anchor_price, anchor_size, match_price, match_size) = if self.optimistic {
                (
                    req.anchor_ws_price,
                    req.anchor_ws_size,
                    req.match_ws_price,
                    req.match_ws_size,
                )
            } else {
                let (anchor_metrics, match_metrics) = match tokio::try_join!(
                    self.get_execution_metrics_from_http(req.anchor.clone()),
                    self.get_execution_metrics_from_http(req.r#match.clone())
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        self.in_flight.write().remove(&req.correlation_id);
                        return Err(e).context("Failed to fetch HTTP execution metrics");
                    }
                };
                tracing::info!(
                    "[{}] from http | anchor: (price={}, size={}) | match: (price={}, size={})",
                    req.correlation_id,
                    anchor_metrics.0,
                    anchor_metrics.1,
                    match_metrics.0,
                    match_metrics.1,
                );
                (
                    anchor_metrics.0,
                    anchor_metrics.1,
                    match_metrics.0,
                    match_metrics.1,
                )
            };

            let total_cost = {
                let total_cost = anchor_price + match_price;
                if total_cost <= Decimal::ZERO || total_cost > Decimal::new(985, 3) {
                    if total_cost >= Decimal::ONE {
                        tracing::warn!(
                            "[{}] gone at execution: total_cost({total_cost}): anchor_{anchor_price} + match_{match_price} — cooling down",
                            req.correlation_id
                        );
                        self.cooldown
                            .write()
                            .insert(req.correlation_id.clone(), Instant::now());
                    } else {
                        tracing::warn!(
                            "arb for {} too close to 1, not worth it: total_cost({total_cost})",
                            req.correlation_id
                        );
                    }

                    self.in_flight.write().remove(&req.correlation_id);
                    return Ok(());
                }

                total_cost
            };

            let execution_size = {
                let market_liq = anchor_size.min(match_size);
                match self
                    .balances
                    .compute_size(anchor_price, match_price, market_liq)
                {
                    Some(s) => s,
                    None => {
                        tracing::warn!(
                            "arb {} skipped: insufficient balance or below min size \
                             (kalshi=${}, poly=${}, budget={:?})",
                            req.correlation_id,
                            self.balances.kalshi(),
                            self.balances.poly(),
                            self.balances.max_trade_dollars(),
                        );
                        self.in_flight.write().remove(&req.correlation_id);
                        return Ok(());
                    }
                }
            };

            tracing::info!(
                "📊 Last Look [{}]: Cost = ${total_cost} | Size = {execution_size}",
                req.correlation_id,
            );

            let (
                anchor_filled,
                match_filled,
                _anchor_remaining,
                _match_remaining,
                anchor_order_id,
                match_order_id,
            ) = {
                let anchor_order = self.make_fak_order(
                    &req.anchor,
                    execution_size,
                    anchor_price,
                    &req.correlation_id,
                );

                let match_order = self.make_fak_order(
                    &req.r#match,
                    execution_size,
                    match_price,
                    &req.correlation_id,
                );

                let (anchor_res, match_res) = tokio::join!(anchor_order, match_order);

                let (anchor_filled, anchor_remaining, anchor_order_id) = match &anchor_res {
                    Ok(res) => res.clone(),
                    Err(e) => {
                        tracing::error!("Anchor Order Failed: {:?}", e);
                        (Decimal::ZERO, Decimal::ZERO, "".to_string())
                    }
                };

                let (match_filled, match_remaining, match_order_id) = match &match_res {
                    Ok(res) => res.clone(),
                    Err(e) => {
                        tracing::error!("Match Order Failed: {:?}", e);
                        (Decimal::ZERO, Decimal::ZERO, "".to_string())
                    }
                };

                (
                    anchor_filled,
                    match_filled,
                    anchor_remaining,
                    match_remaining,
                    anchor_order_id,
                    match_order_id,
                )
            };

            // Deduct from the correct platform balance immediately after fills land.
            // This prevents a second concurrent trade from over-spending while
            // the post-trade API refresh is still in-flight.
            self.balances.deduct_fill(
                req.anchor.platform,
                anchor_price,
                req.r#match.platform,
                match_price,
                anchor_filled.min(match_filled),
            );

            tracing::info!(
                "📊 Executed [{}] - Anchor Filled: {}, Match Filled: {}",
                req.correlation_id,
                anchor_filled,
                match_filled
            );

            if anchor_filled == match_filled {
                if anchor_filled == Decimal::ZERO {
                    tracing::warn!(
                        "arb {} missed completely — both sides filled 0 (liquidity vanished) — cooling down",
                        req.correlation_id
                    );
                    self.cooldown
                        .write()
                        .insert(req.correlation_id.clone(), Instant::now());
                    self.in_flight.write().remove(&req.correlation_id);
                    return Ok(());
                }

                tracing::info!("PERFECT ARB! Both sides filled exactly {}.", anchor_filled);
                self.in_flight.write().remove(&req.correlation_id);
                return self
                    .tx
                    .send(protos::ClientEbo {
                        correlation_id: Uuid::new_v4().to_string(),
                        action_at: chrono::Utc::now().timestamp_millis(),
                        action: Some(protos::client_ebo::Action::OrderSubmitted(
                            protos::OrderMade {
                                anchor_cost: anchor_price.to_f32().unwrap(),
                                match_cost: match_price.to_f32().unwrap(),
                                anchor_fill: anchor_filled.to_f32().unwrap(),
                                match_fill: match_filled.to_f32().unwrap(),
                                anchor_order_id: anchor_order_id,
                                match_order_id: match_order_id,
                                arb_correlation_id: req.correlation_id,
                                ..Default::default()
                            },
                        )),
                    })
                    .await
                    .context("rror sending msg to gprc: ebo::OrderSubmitted");
            }

            let excess = anchor_filled - match_filled;
            let correlation_id = uuid::Uuid::new_v4().to_string();

            tracing::info!("there was excess of {excess}");

            if excess > Decimal::ZERO {
                tracing::warn!("LEGGED: Anchor excess of {excess}. Initiating Hedge Sell.");

                let send_order_task = async {
                    if let Err(e) = self
                        .tx
                        .send(protos::ClientEbo {
                            correlation_id: correlation_id.clone(),
                            action_at: chrono::Utc::now().timestamp_millis(),
                            action: Some(protos::client_ebo::Action::OrderSubmitted(
                                protos::OrderMade {
                                    anchor_cost: anchor_price.to_f32().unwrap_or(0.0),
                                    match_cost: match_price.to_f32().unwrap_or(0.0),
                                    anchor_fill: anchor_filled.to_f32().unwrap_or(0.0),
                                    match_fill: match_filled.to_f32().unwrap_or(0.0),
                                    anchor_order_id: anchor_order_id.clone(),
                                    match_order_id: match_order_id.clone(),
                                    arb_correlation_id: req.correlation_id.clone(),
                                    excess_fill: excess.to_f32().unwrap_or(0.0),
                                },
                            )),
                        })
                        .await
                    {
                        tracing::error!("Failed to send OrderSubmitted to gRPC: {e}");
                    }
                };

                let hedge_task = async {
                    let hedge_price = anchor_price + Decimal::new(2, 2); // match_price + 0.02

                    match self
                        .hedge_sell_order(&req.anchor, excess, hedge_price)
                        .await
                    {
                        Ok(result) => {
                            if let Err(e) = self
                                .tx
                                .send(protos::ClientEbo {
                                    correlation_id: correlation_id.clone(),
                                    action_at: chrono::Utc::now().timestamp_millis(),
                                    action: Some(protos::client_ebo::Action::ExcessFillSubmitted(
                                        protos::ExcessFillOrderMade {
                                            order_id: result.0,
                                            platform: result.1 as i32,
                                            excess_fill_size: result.2.to_f32().unwrap_or(0.0),
                                            excess_fill_cost: result.3.to_f32().unwrap_or(0.0),
                                        },
                                    )),
                                })
                                .await
                            {
                                // backup log: sqllite3 db
                                tracing::error!("Failed to send ExcessFillSubmitted to gRPC: {e}");
                            }
                        }
                        Err(e) => {
                            // backup log: sqllite3 db
                            tracing::error!(
                                "Anchor hedge_sell_order failed: {e} for correlation_id {correlation_id}"
                            );
                        }
                    }
                };

                tokio::join!(send_order_task, hedge_task);
            } else if excess < Decimal::ZERO {
                let hedge_price = match_price + Decimal::new(2, 2); // match_price + 0.02

                let excess_abs = excess.abs();
                tracing::warn!("LEGGED: Match excess of {excess_abs}. Initiating Hedge Sell.");

                let send_order_task = async {
                    if let Err(e) = self
                        .tx
                        .send(protos::ClientEbo {
                            correlation_id: correlation_id.clone(),
                            action_at: chrono::Utc::now().timestamp_millis(),
                            action: Some(protos::client_ebo::Action::OrderSubmitted(
                                protos::OrderMade {
                                    anchor_cost: anchor_price.to_f32().unwrap_or(0.0),
                                    match_cost: match_price.to_f32().unwrap_or(0.0),
                                    anchor_fill: anchor_filled.to_f32().unwrap_or(0.0),
                                    match_fill: match_filled.to_f32().unwrap_or(0.0),
                                    anchor_order_id: anchor_order_id.clone(),
                                    match_order_id: match_order_id.clone(),
                                    excess_fill: excess_abs.to_f32().unwrap_or(0.0),
                                    arb_correlation_id: req.correlation_id.clone(),
                                },
                            )),
                        })
                        .await
                    {
                        tracing::error!("Failed to send OrderSubmitted to gRPC: {e}");
                    }
                };

                let hedge_task = async {
                    match self
                        .hedge_sell_order(&req.r#match, excess_abs, hedge_price)
                        .await
                    {
                        Ok(result) => {
                            if let Err(e) = self
                                .tx
                                .send(protos::ClientEbo {
                                    correlation_id: correlation_id.clone(),
                                    action_at: chrono::Utc::now().timestamp_millis(),
                                    action: Some(protos::client_ebo::Action::ExcessFillSubmitted(
                                        protos::ExcessFillOrderMade {
                                            order_id: result.0,
                                            platform: result.1 as i32,
                                            excess_fill_size: result.2.to_f32().unwrap_or(0.0),
                                            excess_fill_cost: result.3.to_f32().unwrap_or(0.0),
                                        },
                                    )),
                                })
                                .await
                            {
                                // backup log: sqllite3 db
                                tracing::error!("Failed to send ExcessFillSubmitted to gRPC: {e}");
                            }
                        }
                        Err(e) => {
                            // backup log: sqllite3 db
                            tracing::error!("Match hedge_sell_order failed: {e}");
                        }
                    }
                };

                tokio::join!(send_order_task, hedge_task);
            }

            self.in_flight.write().remove(&req.correlation_id);
            Ok(())
        }

        async fn make_fak_order(
            &self,
            leg: &models::ArbMinifiedInfo,
            size: Decimal,
            limit_price: Decimal,
            correlation_id: &models::CorrelationId,
        ) -> anyhow::Result<(Decimal, Decimal, String)> {
            tracing::info!(
                "making hedge_sell/-order: size: {size}, limit_price: {limit_price} platform: {}",
                leg.platform
            );

            match leg.platform {
                protos::Platform::Kalshi => {
                    let side_str = match leg.leg {
                        protos::Leg::Left => "yes",
                        protos::Leg::Right => "no",
                    };

                    let safe_price = (limit_price * Decimal::ONE_HUNDRED)
                        .floor()
                        .to_u64()
                        .context("error converting decimal to u64")?;

                    let safe_size = size.to_u64().context("Size too large or negative")?;

                    if safe_size == 0 {
                        anyhow::bail!("Calculated size should not 0");
                    }

                    let client_order_id = models::generate_uuid_v5(format!(
                        "{}:{}:{}",
                        correlation_id, safe_size, limit_price
                    ));

                    let req = CreateOrderRequest {
                        ticker: leg.token_id.clone(),
                        action: "buy".to_string(),
                        side: side_str.to_string(),
                        count: Some(safe_size),
                        r#type: "limit".to_string(),

                        yes_price: if side_str == "yes" {
                            Some(safe_price)
                        } else {
                            None
                        },
                        no_price: if side_str == "no" {
                            Some(safe_price)
                        } else {
                            None
                        },

                        client_order_id: Some(client_order_id),
                        post_only: Some(false),

                        // FAK
                        time_in_force: Some("immediate_or_cancel".to_string()),

                        ..Default::default()
                    };

                    let order_response = self
                        .platforms
                        .kalshi()
                        .http_client()
                        .create_order(&req)
                        .await
                        .context("error creating kalshi order")?;

                    let filled =
                        models::opt_str_to_decimal_strict(&order_response.order.fill_count_fp)?;

                    let remaining = models::opt_str_to_decimal_strict(
                        &order_response.order.remaining_count_fp,
                    )?;

                    tracing::info!(
                        "Kalshi Order Executed: Requested {size}, Filled {filled}, Remaining {remaining}"
                    );

                    Ok((filled, remaining, order_response.order.order_id))
                }

                protos::Platform::Polymarket => {
                    let poly_scale = Decimal::new(1_000_000, 0);

                    let order = self
                        .polymarket_clob_client
                        .limit_order()
                        .token_id(U256::from_str(&leg.token_id).context("invalid hex token_id")?)
                        .size(size.round_dp(2))
                        .price(limit_price.round_dp(2))
                        .side(clob::types::Side::Buy)
                        .order_type(clob::types::OrderType::FAK)
                        .build()
                        .await
                        .context("error creating polymarket order")?;

                    let signed_order = self
                        .polymarket_clob_client
                        .sign(&self.poly_signer, order)
                        .await?;
                    let response = self.polymarket_clob_client.post_order(signed_order).await?;

                    if !response.success {
                        let err_msg = response
                            .error_msg
                            .unwrap_or_else(|| "Unknown error".to_string());
                        anyhow::bail!("Polymarket order failed: {}", err_msg);
                    }

                    let raw_filled = response.taking_amount;
                    let filled = raw_filled / poly_scale;

                    let remaining = size - filled;

                    tracing::info!(
                        "Polymarket Order Executed: Requested {size}, Filled {filled}, Remaining {remaining}",
                    );

                    if response.status == clob::types::OrderStatusType::Unmatched
                        || filled == Decimal::ZERO
                    {
                        tracing::warn!(
                            "Polymarket order was completely unmatched (Liquidity vanished)."
                        );
                    }

                    Ok((filled, remaining, response.order_id))
                }
            }
        }

        async fn hedge_sell_order(
            &self,
            leg: &models::ArbMinifiedInfo,
            excess_size: Decimal,
            sell_price_dollars: Decimal,
        ) -> anyhow::Result<(String, protos::Platform, Decimal, Decimal)> {
            tracing::info!(
                "making hedge_sell/-order: excess_size: {excess_size}, sell_price_dollars{sell_price_dollars} platform: {}",
                leg.platform
            );

            let safe_sell_price = sell_price_dollars.min(Decimal::new(99, 2));

            match leg.platform {
                protos::Platform::Kalshi => {
                    let side_str = match leg.leg {
                        protos::Leg::Left => "yes",
                        protos::Leg::Right => "no",
                    };

                    let price_cents = (safe_sell_price * Decimal::ONE_HUNDRED)
                        .floor()
                        .to_u64()
                        .unwrap_or(99);

                    let safe_excess_size = excess_size.floor().to_u64().unwrap_or(0);

                    if safe_excess_size == 0 {
                        anyhow::bail!("Hedge size too small (rounds to 0). Skipping.");
                    }

                    let req = CreateOrderRequest {
                        ticker: leg.token_id.clone(),
                        action: "sell".to_string(),
                        side: side_str.to_string(),
                        count: Some(safe_excess_size),
                        r#type: "limit".to_string(),

                        yes_price: if side_str == "yes" {
                            Some(price_cents)
                        } else {
                            None
                        },
                        no_price: if side_str == "no" {
                            Some(price_cents)
                        } else {
                            None
                        },

                        client_order_id: Some(uuid::Uuid::new_v4().to_string()),
                        post_only: Some(true),

                        time_in_force: Some("good_till_canceled".to_string()),
                        ..Default::default()
                    };

                    let order_response = self
                        .platforms
                        .kalshi()
                        .http_client()
                        .create_order(&req)
                        .await
                        .context("Failed to post kalshi hedge order")?;

                    tracing::info!("Hedge sell order placed on Kalshi");

                    Ok((
                        order_response.order.order_id,
                        protos::Platform::Kalshi,
                        excess_size,
                        safe_sell_price,
                    ))
                }

                protos::Platform::Polymarket => {
                    let order = self
                        .polymarket_clob_client
                        .limit_order()
                        .token_id(U256::from_str(&leg.token_id).context("invalid hex token_id")?)
                        .size(excess_size.round_dp(2))
                        .price(safe_sell_price.round_dp(2))
                        .side(clob::types::Side::Sell)
                        .order_type(clob::types::OrderType::GTC)
                        .build()
                        .await
                        .context("Failed to build Polymarket hedge order")?;

                    let signed_order = self
                        .polymarket_clob_client
                        .sign(&self.poly_signer, order)
                        .await
                        .context("Failed to sign Polymarket hedge order")?;

                    let response = self
                        .polymarket_clob_client
                        .post_order(signed_order)
                        .await
                        .context("Failed to post Polymarket hedge order")?;

                    if !response.success {
                        let err_msg = response
                            .error_msg
                            .unwrap_or_else(|| "Unknown error".to_string());
                        tracing::error!("Polymarket rejected hedge sell order: {}", err_msg);
                        anyhow::bail!("Polymarket hedge failed: {}", err_msg)
                    }

                    tracing::info!(
                        "Hedge SELL order placed on Polymarket at ${}",
                        safe_sell_price
                    );

                    Ok((
                        response.order_id,
                        protos::Platform::Polymarket,
                        excess_size,
                        safe_sell_price,
                    ))
                }
            }
        }

        async fn get_execution_metrics_from_http(
            &self,
            arb: models::ArbMinifiedInfo,
        ) -> anyhow::Result<(Decimal, Decimal)> {
            match arb.platform {
                protos::Platform::Polymarket => {
                    let summary = self
                        .platforms
                        .polymarket()
                        .get_order_book(&arb.token_id)
                        .await?;

                    let best_ask = summary
                        .asks
                        .first()
                        .context("Polymarket ask book is empty!")?;

                    let price = Decimal::from_str(&best_ask.price)
                        .context("Failed to parse Polymarket price into Decimal")?;

                    let size = Decimal::from_str(&best_ask.size)
                        .context("Failed to parse Polymarket size into Decimal")?;

                    Ok((price, size))
                }

                protos::Platform::Kalshi => {
                    let ob_response = self
                        .platforms
                        .kalshi()
                        .http_client()
                        .get_market_orderbook(&arb.market_id, Some(90))
                        .await
                        .context("err getting kalshi orderbook")?;

                    // Create a perfect 1.0 representation for the Kalshi math
                    let one_dollar = Decimal::ONE;

                    match arb.leg {
                        // BUYING YES: We trade against the Best NO Bid
                        protos::Leg::Left => {
                            let no_book = ob_response
                                .orderbook_fp
                                .no_dollars
                                .context("Kalshi NO orderbook missing")?;

                            let best_no_bid = no_book.last().context("Kalshi NO bids empty")?;

                            let no_bid_price = Decimal::from_str(&best_no_bid.0)
                                .context("Failed to parse Kalshi NO bid price")?;

                            let no_bid_size = Decimal::from_str(&best_no_bid.1)
                                .context("Failed to convert Kalshi NO bid size")?;

                            let yes_ask_price = one_dollar - no_bid_price;

                            Ok((yes_ask_price, no_bid_size))
                        }

                        // BUYING NO: We trade against the Best YES Bid
                        protos::Leg::Right => {
                            let yes_book = ob_response
                                .orderbook_fp
                                .yes_dollars
                                .context("Kalshi YES orderbook missing")?;

                            let best_yes_bid = yes_book.last().context("Kalshi YES bids empty")?;

                            let yes_bid_price = Decimal::from_str(&best_yes_bid.0)
                                .context("Failed to parse Kalshi YES bid price")?;

                            let yes_bid_size = Decimal::from_str(&best_yes_bid.1)
                                .context("Failed to convert Kalshi YES bid size")?;

                            let no_ask_price = one_dollar - yes_bid_price;

                            Ok((no_ask_price, yes_bid_size))
                        }
                    }
                }
            }
        }
    }
}
