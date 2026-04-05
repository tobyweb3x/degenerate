pub mod comms {
    use crate::{
        models::{self, make_market_key, protos},
        platforms,
    };
    use anyhow::{self, Context};
    use chrono::{DateTime, Duration, Utc};
    use kalshi_rs::{
        portfolio::models::CreateOrderRequest, websocket::models::KalshiSocketMessage,
    };
    use polymarket_hft::client::polymarket::clob;
    use std::collections::HashSet;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing;
    mod book {
        use std::collections::{HashMap, HashSet};

        pub(super) struct Book {
            pub top_of_book: HashMap<super::models::MarketKey, super::models::TopOfBook>,
            pub registry: HashMap<super::models::MarketKey, HashSet<super::models::CorrelationId>>,
            pub records: HashMap<super::models::CorrelationId, super::models::ArbWatch>,
        }

        impl Default for Book {
            fn default() -> Self {
                Self {
                    top_of_book: HashMap::with_capacity(512),
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
        book: book::Book,
    }

    impl PickerComms {
        pub fn new(
            platforms: platforms::Platfroms,
            rx: mpsc::Receiver<protos::ServerEbo>,
            tx: mpsc::Sender<protos::ClientEbo>,
            ws_rx: mpsc::Receiver<platforms::WsEventMessage>,
            execution_tx: mpsc::Sender<models::ExecutionRequest>,
        ) -> Self {
            Self {
                platforms,
                rx,
                tx,
                ws_rx,
                book: book::Book::new(),
                execution_tx,
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
    }

    impl PickerComms {
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

            // subscribe to market channel
            for (platform, token_id) in entries {
                match platform {
                    protos::Platform::Polymarket => {
                        self.platforms
                            .polymarket()
                            .ws_client()
                            .subscribe_market(vec![token_id.clone()], true)
                            .await
                            .context(
                                "error subscribing polymarket token_id to ws market channel",
                            )?;
                        tracing::info!("succesfully subscribe for {} on Polymarket", token_id,)
                    }

                    protos::Platform::Kalshi => {
                        self.platforms
                            .kalshi()
                            .ws_client()
                            .subscribe(vec!["ticker"], vec![token_id.as_str()])
                            .await
                            .context("error subscribing kalshi ticker to ws market channel")?;
                        tracing::info!("succesfully subscribe for {} on kalshi", token_id,)
                    }
                }
            }

            self.register_arb(models::ArbWatch {
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

            let ebo = protos::ClientEbo {
                action: Some(protos::client_ebo::Action::DeleteRunningArbs(
                    protos::DeleteRunningArbRequest {
                        correlation_ids: vec![correlation_id],
                    },
                )),
                ..Default::default()
            };

            self.tx
                .send(ebo)
                .await
                .context("error sending DeleteRunningArbs")
        }
    }

    impl PickerComms {
        fn handle_poly_messages(&mut self, msg: clob::ws::WsMessage) -> anyhow::Result<()> {
            match msg {
                clob::ws::WsMessage::BestBidAsk(best) => {
                    println!(
                        "we're getting poly BestBidAsk update for:{}",
                        best.asset_id.as_str()
                    );
                    self.on_price_update(
                        models::make_market_key(&best.asset_id, protos::Platform::Polymarket),
                        models::TopOfBook {
                            best_bid: models::parse_f32(&best.best_bid)?,
                            best_ask: models::parse_f32(&best.best_ask)?,
                            spread: models::parse_f32(&best.spread)?,
                            tob_timestamp_ms: best
                                .timestamp
                                .parse()
                                .context("erorr parsing timestamp to i64")?,
                            ..Default::default()
                        },
                    );
                }

                clob::ws::WsMessage::MarketResolved(resolve) => {
                    // TODO: try resolve wins from here if possible

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
                    println!(
                        "we're getting kalshi TickerUpdate update for:{}",
                        ticker.msg.market_ticker.as_str()
                    );
                    self.on_price_update(
                        models::make_market_key(
                            &ticker.msg.market_ticker,
                            protos::Platform::Kalshi,
                        ),
                        models::TopOfBook {
                            best_bid: models::parse_f32(&ticker.msg.yes_bid_dollars)?,
                            best_ask: models::parse_f32(&ticker.msg.yes_ask_dollars)?,
                            bid_size: models::parse_f32(&ticker.msg.yes_bid_size_fp)?,
                            ask_size: models::parse_f32(&ticker.msg.yes_ask_size_fp)?,
                            spread: models::parse_f32(&ticker.msg.yes_ask_dollars)?
                                - models::parse_f32(&ticker.msg.yes_bid_dollars)?,
                            tob_timestamp_ms: ticker.msg.ts * 1_000,
                            sid: Some(ticker.sid),
                        },
                    );
                }

                KalshiSocketMessage::MarketLifecycleV2(event) => {
                    // TODO: try resolve wins from here if possible

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
                    let should_remove =
                        if let Some(set) = self.book.registry.get_mut(key.market_key.as_str()) {
                            set.remove(cid);
                            set.is_empty()
                        } else {
                            false
                        };

                    if should_remove {
                        self.book.registry.remove(key.market_key.as_str());
                        let deleted_tob = self.book.top_of_book.remove(key.market_key.as_str());

                        // ✅ extract owned data BEFORE spawn
                        let platform = key.platform;
                        let market_id = key.market_id.clone();
                        let token_id = key.token_id.clone();

                        let sid = deleted_tob.and_then(|t| t.sid);

                        // ✅ clone clients (must be Arc or cheap clone)
                        let kalshi = self.platforms.kalshi().ws_client().clone();
                        let polymarket = self.platforms.polymarket().ws_client().clone();

                        tokio::spawn(async move {
                            match platform {
                                protos::Platform::Kalshi => {
                                    if let Some(sid) = sid {
                                        if let Err(e) = kalshi.unsubscribe(vec![sid as u64]).await {
                                            tracing::error!(
                                                "Failed background unsubscribe for Kalshi SID {}: {}",
                                                sid,
                                                e
                                            );
                                        }
                                    } else {
                                        tracing::debug!(
                                            "Skipping Kalshi unsubscribe for {}: No SID found.",
                                            market_id
                                        );
                                    }
                                }

                                protos::Platform::Polymarket => {
                                    if let Err(e) = polymarket
                                        .unsubscribe_assets_from_market_channel(vec![token_id])
                                        .await
                                    {
                                        tracing::error!(
                                            "Failed background unsubscribe for Polymarket: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }

        pub fn on_price_update(&mut self, market_key: models::MarketKey, tob: models::TopOfBook) {
            self.book.top_of_book.insert(market_key.clone(), tob);

            if let Some(cids) = self.book.registry.get(&market_key) {
                for cid in cids {
                    self.evaluate_arb(cid);
                }
            }
        }

        fn evaluate_arb(&self, cid: &models::CorrelationId) {
            let Some(watch) = self.book.records.get(cid) else {
                return;
            };

            let anchor_tob = self
                .book
                .top_of_book
                .get(watch.arb.anchor.market_key.as_str());
            let match_tob = self
                .book
                .top_of_book
                .get(watch.arb.r#match.market_key.as_str());

            if let (Some(a_tob), Some(m_tob)) = (anchor_tob, match_tob) {
                let anchor_best_ask = needed_ask_price(a_tob, &watch.arb.anchor);
                let match_best_ask = needed_ask_price(m_tob, &watch.arb.r#match);

                let total_cost = anchor_best_ask + match_best_ask;

                if total_cost > 0.0 && total_cost < 0.95 {
                    // let now = chrono::Utc::now().timestamp_millis();
                    // if (a_tob.timestamp_ms - m_tob.timestamp_ms).abs() > 10 * 60 * 1000
                    //     || now - a_tob.timestamp_ms > 30 * 60 * 1000
                    //     || now - m_tob.timestamp_ms > 30 * 60 * 1000
                    // {
                    //     return;
                    // }

                    if let Err(e) = self.execution_tx.try_send(models::ExecutionRequest {
                        correlation_id: cid.clone(),
                        anchor: watch.arb.anchor.clone(),
                        r#match: watch.arb.r#match.clone(),
                        anchor_price: anchor_best_ask,
                        match_price: match_best_ask,
                    }) {
                        tracing::error!("error sending to picker_execution: {e:#?}");
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
    use polymarket_client_sdk::{
        auth::{Normal, state::Authenticated},
        clob,
    };
    use rust_decimal::prelude::FromStr;
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing;

    #[derive(Debug, Clone)]
    pub enum ExecStatus {
        Pending,
        FilledMatched, // Both legs filled the exact same amount
        LeggedHedging, // Mismatched fills, currently placing a sell order for the excess
        Hedged,        // Excess was successfully placed on the order book
        Failed,        // Something went wrong
    }

    #[derive(Debug, Clone)]
    pub struct ArbTradeRecord {
        pub req: models::ExecutionRequest,
        pub target_size: u64,
        pub anchor_filled: u64,
        pub match_filled: u64,
        pub status: ExecStatus,
    }

    pub struct PickerExec {
        platforms: platforms::Platfroms,
        execution_rx: mpsc::Receiver<models::ExecutionRequest>,
        polymarket_clob_client: clob::Client<Authenticated<Normal>>,
        trade_state: HashMap<models::CorrelationId, ArbTradeRecord>,
        poly_signer: PrivateKeySigner,
    }

    impl PickerExec {
        pub fn new(
            platforms: platforms::Platfroms,
            execution_rx: mpsc::Receiver<models::ExecutionRequest>,
            polymarket_clob_client: clob::Client<Authenticated<Normal>>,
            poly_signer: PrivateKeySigner,
        ) -> Self {
            Self {
                platforms,
                execution_rx,
                trade_state: HashMap::with_capacity(512),
                polymarket_clob_client,
                poly_signer,
            }
        }

        pub async fn run_picker_exe(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::trace!("polymarket received shutdown");
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
                    }
                }
            }
            Ok(())
        }

        async fn handle_execution_request(
            &self,
            req: models::ExecutionRequest,
        ) -> anyhow::Result<()> {
            tracing::info!(
                "🔍 Fetching live HTTP orderbooks for [{}]",
                req.correlation_id
            );

            let (anchor_metrics, match_metrics) = tokio::try_join!(
                self.get_execution_metrics_from_http(req.anchor.clone()),
                self.get_execution_metrics_from_http(req.r#match.clone())
            )
            .context("Failed to fetch HTTP execution metrics")?;

            let (anchor_price, anchor_size) = anchor_metrics;
            let (match_price, match_size) = match_metrics;

            let total_cost = {
                let total_cost = anchor_price + match_price;
                if total_cost >= Decimal::ONE {
                    tracing::warn!(
                        "arb for {} gone at execution: total_cost({total_cost})",
                        req.correlation_id,
                    );
                    return Ok(());
                }

                if !(total_cost > Decimal::ZERO && total_cost <= Decimal::new(985, 3)) {
                    tracing::warn!(
                        "Arb total_cost:${total_cost} is to close to 1, not worth it. Aborting."
                    );
                    return Ok(());
                }

                total_cost
            };

            let execution_size = {
                let execution_size = anchor_size.min(match_size).floor();
                if execution_size <= Decimal::ONE {
                    tracing::warn!(
                        "low liquidity for execution: size({}:{execution_size})",
                        req.correlation_id,
                    );
                    return Ok(());
                }
                execution_size
            };

            tracing::info!(
                "📊 Last Look [{}]: Cost = ${total_cost} | Size = {execution_size}",
                req.correlation_id,
            );

            let (anchor_filled, match_filled) = {
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

                let anchor_filled = match &anchor_res {
                    Ok((filled, _)) => *filled,
                    Err(e) => {
                        tracing::error!("🚨 Anchor Order Failed: {:?}", e);
                        Decimal::ZERO
                    }
                };

                let match_filled = match &match_res {
                    Ok((filled, _)) => *filled,
                    Err(e) => {
                        tracing::error!("🚨 Match Order Failed: {:?}", e);
                        Decimal::ZERO
                    }
                };

                (anchor_filled, match_filled)
            };

            tracing::info!(
                "📊 Executed [{}] - Anchor Filled: {}, Match Filled: {}",
                req.correlation_id,
                anchor_filled,
                match_filled
            );

            if anchor_filled == match_filled {
                if anchor_filled > Decimal::ZERO {
                    tracing::info!(
                        "🎉 PERFECT ARB! Both sides filled exactly {}.",
                        anchor_filled
                    );
                } else {
                    tracing::warn!("💨 Missed completely. Both sides filled 0.");
                }
                return Ok(());
            }

            let excess = anchor_filled - match_filled;

            if excess > Decimal::ZERO {
                tracing::warn!("🚨 LEGGED: Anchor excess of {excess}. Initiating Hedge Sell.");

                // add 50 cents to buy price
                let hedge_price = anchor_price + Decimal::new(2, 2); // anchor_price + 0.02

                self.hedge_sell_order(&req.anchor, excess, hedge_price)
                    .await
                    .context("anchor hedge_sell_order failed")?;
            } else if excess < Decimal::ZERO {
                let excess_abs = excess.abs();
                tracing::warn!("🚨 LEGGED: Match excess of {excess_abs}. Initiating Hedge Sell.");

                let hedge_price = match_price + Decimal::new(2, 2); // match_price + 0.02

                self.hedge_sell_order(&req.r#match, excess_abs, hedge_price)
                    .await
                    .context("match hedge_sell_order failed")?;
            }

            Ok(())
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
                        .await?;

                    // Create a perfect 1.0 representation for the Kalshi math
                    let one_dollar = Decimal::ONE;

                    match arb.leg {
                        // BUYING YES: We trade against the Best NO Bid
                        protos::Leg::Left => {
                            let no_book = ob_response
                                .orderbook
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
                                .orderbook
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

        async fn make_fak_order(
            &self,
            leg: &models::ArbMinifiedInfo,
            size: Decimal,
            limit_price: Decimal,
            correlation_id: &models::CorrelationId,
        ) -> anyhow::Result<(Decimal, Decimal)> {
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
                        return Err(anyhow::anyhow!("Calculated size should not 0"));
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

                    Ok((filled, remaining))
                }

                protos::Platform::Polymarket => {
                    let poly_scale = Decimal::new(1_000_000, 0);

                    let order = self
                        .polymarket_clob_client
                        .limit_order()
                        .token_id(U256::from_str(&leg.token_id).context("invalid hex token_id")?)
                        .size(size)
                        .price(limit_price.round_dp(4))
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
                        tracing::error!("❌ Polymarket rejected order: {}", err_msg);
                        return Err(anyhow::anyhow!("Polymarket order failed: {}", err_msg));
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

                    Ok((filled, remaining))
                }
            }
        }

        async fn hedge_sell_order(
            &self,
            leg: &models::ArbMinifiedInfo,
            excess_size: Decimal,
            sell_price_dollars: Decimal,
        ) -> anyhow::Result<()> {
            let safe_sell_price = sell_price_dollars.min(Decimal::new(99, 2)); // incase the pump w/ 5cents goes above $1
            let price_cents = (safe_sell_price * Decimal::ONE_HUNDRED)
                .floor()
                .to_u64()
                .unwrap_or(99);

            match leg.platform {
                protos::Platform::Kalshi => {
                    let side_str = match leg.leg {
                        protos::Leg::Left => "yes",
                        protos::Leg::Right => "no",
                    };

                    let safe_excess_size = excess_size.floor().to_u64().unwrap_or(0);

                    if safe_excess_size == 0 {
                        tracing::warn!("Hedge size too small (rounds to 0). Skipping.");
                        return Ok(());
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

                    self.platforms
                        .kalshi()
                        .http_client()
                        .create_order(&req)
                        .await
                        .context("Failed to post kalshi hedge order")?;

                    tracing::info!("✅ Hedge sell order placed on Kalshi");
                    Ok(())
                }

                protos::Platform::Polymarket => {
                    let order = self
                        .polymarket_clob_client
                        .limit_order()
                        .token_id(U256::from_str(&leg.token_id).context("invalid hex token_id")?)
                        .size(excess_size.floor())
                        .price(safe_sell_price)
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
                        return Err(anyhow::anyhow!("Polymarket hedge failed: {}", err_msg));
                    }

                    tracing::info!(
                        "✅ Hedge SELL order placed on Polymarket at ${}",
                        safe_sell_price
                    );
                    Ok(())
                }
            }
        }
    }
}
