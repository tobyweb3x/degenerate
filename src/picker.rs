pub mod comms {
    use crate::{
        models::{self, CorrelationId, make_market_key, protos},
        platforms,
    };
    use alloy::network::any;
    use anyhow::{self, Context};
    use chrono::{DateTime, Duration, Utc};
    use kalshi_rs::websocket::models::KalshiSocketMessage;
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
                    self.evaluate_arb(&cid);
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
                let anchor_best_ask = needed_ask_price(&a_tob, &watch.arb.anchor);
                let match_best_ask = needed_ask_price(&m_tob, &watch.arb.r#match);

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
    use anyhow::{self, Context};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing;

    pub struct PickerExec {
        platforms: platforms::Platfroms,
        execution_rx: mpsc::Receiver<models::ExecutionRequest>,
    }

    impl PickerExec {
        pub fn new(
            platforms: platforms::Platfroms,
            execution_rx: mpsc::Receiver<models::ExecutionRequest>,
        ) -> Self {
            Self {
                platforms,
                execution_rx,
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

            let total_cost = anchor_price + match_price;
            if total_cost >= 1.0 {
                tracing::warn!(
                    "arb for {} gone at execution: {total_cost}",
                    req.correlation_id,
                );
                return Ok(());
            }

            let execution_size = anchor_size.min(match_size);

            if execution_size <= 1.0 {
                tracing::warn!(
                    "low liquidity for execution({}:{execution_size})",
                    req.correlation_id,
                );
                return Ok(());
            }

            tracing::info!(
                "📊 Last Look [{}]: Cost = ${:.3} | Size = {}",
                req.correlation_id,
                total_cost,
                execution_size
            );

            if total_cost > 0.0 && total_cost <= 0.985 {
                tracing::info!("✅ FIRING FAK ORDERS FOR [{}] !!", req.correlation_id);

                // TODO: Fire your actual HTTP POST orders to Kalshi and Polymarket here!
                // You can use tokio::try_join! here again to place both orders simultaneously!
                //
                // let (kalshi_res, poly_res) = tokio::try_join!(
                //     self.platforms.kalshi().place_order(...),
                //     self.platforms.polymarket().place_order(...)
                // )?;
            } else {
                // The spread closed while we were fetching the orderbooks
                tracing::warn!(
                    "❌ Arb closed before execution. New cost: ${:.3}. Aborting.",
                    total_cost
                );
            }

            Ok(())
        }

        async fn get_execution_metrics_from_http(
            &self,
            arb: models::ArbMinifiedInfo,
        ) -> anyhow::Result<(f32, f32)> {
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

                    let price = best_ask
                        .price
                        .parse::<f32>()
                        .context("Parse Poly price error")?;
                    let size = best_ask
                        .size
                        .parse::<f32>()
                        .context("Parse Poly size error")?;

                    Ok((price, size))
                }

                protos::Platform::Kalshi => {
                    let ob_response = self
                        .platforms
                        .kalshi()
                        .http_client()
                        .get_market_orderbook(&arb.market_id, Some(90)) // market_id here is actually the ticker
                        .await?;

                    match arb.leg {
                        // BUYING YES: We trade against the Best NO Bid
                        protos::Leg::Left => {
                            let no_book = ob_response
                                .orderbook
                                .no_dollars
                                .context("Kalshi NO orderbook missing")?;

                            let best_no_bid = no_book.last().context("Kalshi NO bids empty")?;

                            let no_bid_price = best_no_bid.0.parse::<f32>()?;

                            let no_bid_size = best_no_bid.1 as f32;

                            Ok((1.0 - no_bid_price, no_bid_size))
                        }

                        // BUYING NO: We trade against the Best YES Bid
                        protos::Leg::Right => {
                            let yes_book = ob_response
                                .orderbook
                                .yes_dollars
                                .context("Kalshi YES orderbook missing")?;

                            let best_yes_bid = yes_book.last().context("Kalshi YES bids empty")?;

                            let yes_bid_price = best_yes_bid.0.parse::<f32>()?;
                            let yes_bid_size = best_yes_bid.1 as f32;

                            Ok((1.0 - yes_bid_price, yes_bid_size))
                        }
                    }
                }
            }
        }

        // Simulates walking two order books to find the maximum profitable arbitrage size.
        //
        // * `anchor_asks`: The list of available asks for Leg 1 (sorted lowest price to highest)
        // * `match_asks`: The list of available asks for Leg 2 (sorted lowest price to highest)
        // * `max_cost`: The maximum combined price you are willing to pay (e.g., 0.985)
        //
        // Returns: (Total Contracts to Buy, Total Cost per Contract, Total Capital Required)
        // pub fn walk_books(
        //     mut anchor_asks: Vec<PriceLevel>,
        //     mut match_asks: Vec<PriceLevel>,
        //     max_cost_threshold: f32,
        // ) -> Option<(f32, f32, f32)> {
        //     let mut total_contracts = 0.0;
        //     let mut total_capital_spent = 0.0;

        //     // Use pointers to track which level we are currently eating
        //     let mut anchor_idx = 0;
        //     let mut match_idx = 0;

        //     // Keep looping as long as we have levels left on BOTH exchanges
        //     while anchor_idx < anchor_asks.len() && match_idx < match_asks.len() {
        //         let a_level = &mut anchor_asks[anchor_idx];
        //         let m_level = &mut match_asks[match_idx];

        //         // 1. Check Profitability
        //         let current_cost = a_level.price + m_level.price;

        //         // If the current combination of levels is too expensive, we stop walking.
        //         if current_cost >= max_cost_threshold {
        //             break;
        //         }

        //         // 2. Determine Executable Size
        //         // We can only buy as much as the weakest link at this specific price level
        //         let matched_size = a_level.size.min(m_level.size);

        //         if matched_size < 1.0 {
        //             // Move past dust
        //             if a_level.size < 1.0 {
        //                 anchor_idx += 1;
        //             }
        //             if m_level.size < 1.0 {
        //                 match_idx += 1;
        //             }
        //             continue;
        //         }

        //         // 3. "Execute" the simulated trade
        //         total_contracts += matched_size;
        //         total_capital_spent += matched_size * current_cost;

        //         // 4. Deplete the sizes from the current levels
        //         a_level.size -= matched_size;
        //         m_level.size -= matched_size;

        //         // 5. Advance the pointers if a level is completely eaten
        //         if a_level.size <= 0.001 {
        //             anchor_idx += 1;
        //         }
        //         if m_level.size <= 0.001 {
        //             match_idx += 1;
        //         }
        //     }

        //     if total_contracts < 1.0 {
        //         return None; // No meaningful arb found
        //     }

        //     let average_cost_per_contract = total_capital_spent / total_contracts;

        //     Some((
        //         total_contracts,
        //         average_cost_per_contract,
        //         total_capital_spent,
        //     ))
        // }
    }

    // #[derive(Debug, Clone)]
    // pub struct PriceLevel {
    //     pub price: f32,
    //     pub size: f32,
    // }
}
