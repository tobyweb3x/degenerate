use super::{WsEventMessage, format_duration_ago};
use crate::models::{self, QdrantMarketConverter, protos};
use crate::vector_store;
use anyhow::Context;
use kalshi_rs::{
    KalshiClient, KalshiWebsocketClient,
    markets::models::MarketsQuery,
    ratelimiter::{RateLimitTier, RateLimiterConfig},
    series::models::{Series, SeriesQuery},
    websocket::models::KalshiSocketMessage,
};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

const KALSHI_WS_MAX_RECONNECT_ATTEMPTS: u32 = 5;

#[derive(Clone)]
pub struct MyKalshiClient {
    inner: Arc<KalshiInner>,
}

struct KalshiInner {
    http_client: KalshiClient,
    ws_client: KalshiWebsocketClient,
    qdrant_client: vector_store::VectorStore,
    ws_tx: mpsc::Sender<WsEventMessage>,
    grpc_tx: mpsc::Sender<protos::ClientEbo>,
    subscribed_assets: RwLock<HashSet<String>>,
    /// SID for the `ticker` channel (stored for logging / reconnect; update_subscription
    /// is NOT supported on this channel — "Unsupported action").  We subscribe with no
    /// market filter so all ticker updates flow in automatically.
    ticker_sid: RwLock<Option<u64>>,
    /// SID for the persistent `orderbook_delta` channel subscription.
    /// Lazily initialized on the first `add_market_ticker` call (Kalshi rejects an
    /// empty-market subscribe for this channel).  Subsequent calls use `add_markets`.
    orderbook_sid: RwLock<Option<u64>>,
}

impl MyKalshiClient {
    pub fn new(
        account: kalshi_rs::Account,
        qdrant_client: vector_store::VectorStore,
        ws_tx: mpsc::Sender<WsEventMessage>,
        grpc_tx: mpsc::Sender<protos::ClientEbo>,
    ) -> Self {
        let http_client = KalshiClient::new_with_config(
            account.clone(),
            None,
            Some(RateLimitTier::Custom {
                read_rps: 4,
                write_rps: 2,
            }),
            Some(RateLimiterConfig::default()),
        );

        let ws_client = KalshiWebsocketClient::new(account);

        Self {
            inner: Arc::new(KalshiInner {
                http_client,
                ws_client,
                qdrant_client,
                ws_tx,
                grpc_tx,
                subscribed_assets: RwLock::new(HashSet::with_capacity(512)),
                ticker_sid: RwLock::new(None),
                orderbook_sid: RwLock::new(None),
            }),
        }
    }

    pub fn http_client(&self) -> &KalshiClient {
        &self.inner.http_client
    }

    pub fn is_asset_subscribed(&self, asset_id: &str) -> bool {
        self.inner.subscribed_assets.read().contains(asset_id)
    }

    pub fn insert_subscribed_asset(&self, asset_id: String) -> bool {
        self.inner.subscribed_assets.write().insert(asset_id)
    }

    pub fn remove_subscribed_asset(&self, asset_id: &str) -> bool {
        self.inner.subscribed_assets.write().remove(asset_id)
    }

    /// Add `ticker` to the persistent `ticker` channel subscription.
    ///
    /// Lazy-init: the first call opens the subscription via `subscribe` (which gives a SID
    /// captured by the `SubscribedResponse` handler); subsequent calls use `add_markets`.
    async fn subscribe_ticker_channel(&self, ticker: &str) -> anyhow::Result<()> {
        let tsid = *self.inner.ticker_sid.read();
        match tsid {
            None => self
                .inner
                .ws_client
                .subscribe(vec!["ticker"], vec![ticker])
                .await
                .context("kalshi ticker initial subscribe failed"),
            Some(tsid) => self
                .inner
                .ws_client
                .add_markets(vec![tsid], vec![ticker])
                .await
                .context("kalshi ticker add_markets failed"),
        }
    }

    /// Add `ticker` to the persistent `orderbook_delta` channel subscription.
    ///
    /// Same lazy-init pattern as `subscribe_ticker_channel`.
    async fn subscribe_orderbook_channel(&self, ticker: &str) -> anyhow::Result<()> {
        let osid = *self.inner.orderbook_sid.read();
        match osid {
            None => self
                .inner
                .ws_client
                .subscribe(vec!["orderbook_delta"], vec![ticker])
                .await
                .context("kalshi orderbook_delta initial subscribe failed"),
            Some(osid) => self
                .inner
                .ws_client
                .add_markets(vec![osid], vec![ticker])
                .await
                .context("kalshi orderbook_delta add_markets failed"),
        }
    }

    pub async fn add_market_ticker(&self, ticker: &str) -> anyhow::Result<()> {
        if self.is_asset_subscribed(ticker) {
            tracing::warn!("kalshi: {ticker} already in subscribed_assets, skipping");
            return Ok(());
        }

        tokio::try_join!(
            // self.subscribe_ticker_channel(ticker),
            self.subscribe_orderbook_channel(ticker),
        )?;

        self.insert_subscribed_asset(ticker.to_string());
        Ok(())
    }

    pub async fn del_market_ticker(&self, ticker: &str) -> anyhow::Result<()> {
        self.remove_subscribed_asset(ticker);

        // Copy SID values out before any .await — parking_lot guards must not cross await points.
        let tsid = *self.inner.ticker_sid.read();
        let osid = *self.inner.orderbook_sid.read();

        if let Some(tsid) = tsid {
            if let Err(e) = self
                .inner
                .ws_client
                .del_markets(vec![tsid], vec![ticker])
                .await
            {
                tracing::warn!("kalshi: ticker del_markets({ticker}) failed: {e}");
            }
        }

        if let Some(osid) = osid {
            if let Err(e) = self
                .inner
                .ws_client
                .del_markets(vec![osid], vec![ticker])
                .await
            {
                tracing::warn!("kalshi: orderbook_delta del_markets({ticker}) failed: {e}");
            }
        }

        Ok(())
    }

    pub async fn test_ws_connect(&self) -> anyhow::Result<()> {
        self.inner
            .ws_client
            .connect()
            .await
            .context("failed to establish Kalshi WebSocket connection")?;

        Ok(())
    }

    pub async fn run_kalshi(&self, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.inner
            .ws_client
            .subscribe(vec!["market_lifecycle_v2"], vec![])
            .await?;

        self.inner
            .ws_client
            .subscribe(vec!["ticker"], vec![])
            .await?;

        let duration = Duration::from_secs(60 * 60 + 10 * 60);
        let mut one_hour_ten_min_ticker = tokio::time::interval(duration);

        one_hour_ten_min_ticker.tick().await; // fires first ticks
        loop {
            tokio::select! {
                  _ = shutdown.cancelled() => {
                        tracing::trace!("kalshi received shutdown");
                        self.inner.ws_client.disconnect().await;
                        break;
                  }

                _ = one_hour_ten_min_ticker.tick() => {
                    println!("kalshi one_hour_ten_min_ticker triggers");
                        let _ = self.backfill_funcs( shutdown.clone(), duration,).await
                        .inspect_err(|e|  tracing::error!("error backfilling kalshi: {e:?}"));
                     }


                  kalshi_ws_msgs = self.inner.ws_client.next_message_two() => {
                  let Some(msg) = kalshi_ws_msgs else {
                      tracing::warn!("kalshi WS stream ended unexpectedly, attempting reconnect...");
                      if let Err(e) = self.reconnect_ws().await {
                          anyhow::bail!("kalshi WS reconnect failed: {e}");
                      }
                      continue;
                  };

                  match msg {
                      Ok(msg) => {
                        let _ = self.handle_kalshi_wss_message(msg).await
                        .inspect_err(|e|  tracing::error!("error handling kalshi msg(wss): {e:?}"));
                      }

                      Err(e) => {
                          let err_str = format!("{e:?}");
                          if err_str.contains("transport") || err_str.contains("Io(") || err_str.contains("TLS") {
                              tracing::warn!("kalshi WS transport error, reconnecting: {e:?}");
                              if let Err(re) = self.reconnect_ws().await {
                                  anyhow::bail!("kalshi WS reconnect failed after transport error: {re}");
                              }
                          } else {
                              tracing::error!("kalshi ws error: {e:?}");
                          }
                      }
                  }
              }
            }
        }

        tracing::info!("kalshi live mode got shutdown");
        Ok(())
    }

    async fn handle_kalshi_wss_message(&self, msg: KalshiSocketMessage) -> anyhow::Result<()> {
        match msg {
            KalshiSocketMessage::SubscribedResponse(resp) => {
                let sid = resp.msg.sid as u64;
                match resp.msg.channel.as_str() {
                    "ticker" => {
                        let mut guard = self.inner.ticker_sid.write();
                        if guard.is_none() {
                            *guard = Some(sid);
                            tracing::info!("kalshi: ticker channel SID={sid}");
                        } else {
                            tracing::debug!("kalshi: ticker secondary SID={sid} (ignored)");
                        }
                    }
                    "orderbook_delta" => {
                        let mut guard = self.inner.orderbook_sid.write();
                        if guard.is_none() {
                            *guard = Some(sid);
                            tracing::info!("kalshi: orderbook_delta channel SID={sid}");
                        } else {
                            tracing::debug!(
                                "kalshi: orderbook_delta secondary SID={sid} (ignored)"
                            );
                        }
                    }
                    other => {
                        tracing::debug!("kalshi: subscribed channel={other} SID={sid}");
                    }
                }
                return Ok(());
            }

            KalshiSocketMessage::MarketLifecycleV2(event) => {
                if matches!(event.msg.event_type.as_str(), "created" | "activated") {
                    // market → event → series are sequential dependencies
                    let Ok(value) = self
                        .inner
                        .http_client
                        .get_market(event.msg.market_ticker.as_str())
                        .await
                    else {
                        return Ok(());
                    };

                    let Ok(event_resp) = self
                        .inner
                        .http_client
                        .get_event(value.market.event_ticker.as_str())
                        .await
                    else {
                        return Ok(());
                    };

                    let Ok(series) = self
                        .inner
                        .http_client
                        .get_series_by_ticker(event_resp.event.series_ticker.as_str())
                        .await
                    else {
                        return Ok(());
                    };

                    let Some(market) = Self::sort_kalshi_tags(series.series) else {
                        return Ok(());
                    };

                    let qdrant_payload = protos::QdrantPayload::from_market(
                        value.market,
                        market.info().market_category,
                        market.info().market_subcategory,
                    );

                    if let Some(ebo) = self
                        .inner
                        .qdrant_client
                        .search_and_insert(
                            qdrant_payload,
                            vector_store::SIMILARITY_SCORE_THRESHOLD,
                            vector_store::VectorStore::create_cross_platform_filter(
                                Some(protos::Platform::Kalshi),
                                &market,
                            ),
                        )
                        .await?
                    {
                        self.inner
                            .grpc_tx
                            .send(ebo)
                            .await
                            .context("kalshi: error forwarding discovery hit to grpc")?;
                    }

                    return Ok(());
                }

                if matches!(
                    event.msg.event_type.as_str(),
                    "deactivated" | "close_date_updated" | "determined" | "settled"
                ) {
                    self.inner
                        .ws_tx
                        .send(WsEventMessage::Kalshi(
                            KalshiSocketMessage::MarketLifecycleV2(event),
                        ))
                        .await?;
                }
            }

            KalshiSocketMessage::TickerUpdate(ticker) => {
                // tracing::info!(
                //     "kalshi ticker: {} yes_bid={} yes_ask={}",
                //     ticker.msg.market_ticker,
                //     ticker.msg.yes_bid_dollars,
                //     ticker.msg.yes_ask_dollars,
                // );
                self.inner
                    .ws_tx
                    .send(WsEventMessage::Kalshi(KalshiSocketMessage::TickerUpdate(
                        ticker,
                    )))
                    .await?
            }

            KalshiSocketMessage::Ping(payload) => self
                .inner
                .ws_client
                .send_pong(payload)
                .await
                .context("error sending pong(kalshi)")?,

            KalshiSocketMessage::Close(close) => {
                tracing::warn!("kalshi WS received close frame: {close:?}");
                self.reconnect_ws()
                    .await
                    .context("kalshi WS reconnect after Close failed")?;
            }

            KalshiSocketMessage::ErrorResponse(err) => {
                if err
                    .msg
                    .msg
                    .eq_ignore_ascii_case("unable to process message")
                {
                    return Ok(());
                }
                tracing::error!("Kalshi error {}: {}", err.msg.code, err.msg.msg);
            }

            KalshiSocketMessage::Unhandled(val) => {
                // Log anything that arrives but couldn't be parsed into a known variant.
                // This catches ticker / orderbook messages whose struct fields don't
                // match what the server actually sends.
                tracing::warn!(
                    "kalshi: unhandled WS message (type={:?}): {}",
                    val.get("type").and_then(|v| v.as_str()).unwrap_or("?"),
                    val
                );
            }

            _ => {}
        }

        Ok(())
    }

    /// Disconnects and reconnects the Kalshi WebSocket with exponential backoff,
    /// then re-subscribes all channels.  SIDs are reset here and will be refreshed
    /// when the `SubscribedResponse` messages arrive in the main loop.
    async fn reconnect_ws(&self) -> anyhow::Result<()> {
        self.inner.ws_client.disconnect().await;

        // Invalidate stale SIDs so no del_markets call goes out with old IDs.
        *self.inner.ticker_sid.write() = None;
        *self.inner.orderbook_sid.write() = None;

        let assets: Vec<String> = self
            .inner
            .subscribed_assets
            .read()
            .iter()
            .cloned()
            .collect();

        for attempt in 1..=KALSHI_WS_MAX_RECONNECT_ATTEMPTS {
            let delay = Duration::from_millis(500 * 2u64.pow(attempt - 1));
            tracing::info!(
                "Kalshi WS reconnect attempt {}/{} in {:?}",
                attempt,
                KALSHI_WS_MAX_RECONNECT_ATTEMPTS,
                delay
            );
            tokio::time::sleep(delay).await;

            if let Err(e) = self.inner.ws_client.connect().await {
                tracing::warn!("Kalshi WS reconnect attempt {} failed: {e}", attempt);
                continue;
            }

            self.inner
                .ws_client
                .subscribe(vec!["market_lifecycle_v2"], vec![])
                .await
                .context("failed to resubscribe market_lifecycle_v2 after kalshi reconnect")?;

            // Re-subscribe both channels with all currently tracked assets.
            // Fresh SIDs arrive via SubscribedResponse and update ticker_sid / orderbook_sid.
            // If there are no tracked assets yet, both SIDs stay None and will be lazily
            // initialised on the next add_market_ticker call.
            let asset_refs: Vec<&str> = assets.iter().map(|s| s.as_str()).collect();
            if !asset_refs.is_empty() {
                if let Err(e) = self
                    .inner
                    .ws_client
                    .subscribe(vec!["ticker"], asset_refs.clone())
                    .await
                {
                    tracing::warn!("kalshi: ticker bulk resubscribe after reconnect failed: {e}");
                }
                if let Err(e) = self
                    .inner
                    .ws_client
                    .subscribe(vec!["orderbook_delta"], asset_refs)
                    .await
                {
                    tracing::warn!(
                        "kalshi: orderbook_delta bulk resubscribe after reconnect failed: {e}"
                    );
                }
            }

            tracing::info!("Kalshi WS reconnected successfully");
            return Ok(());
        }

        anyhow::bail!(
            "Kalshi WS reconnect failed after {} attempts",
            KALSHI_WS_MAX_RECONNECT_ATTEMPTS
        )
    }

    fn sort_kalshi_tags(series_data: Series) -> Option<models::MarketTag> {
        let tags = series_data.tags?;

        for tag in tags {
            if let Some(market) = models::KALSHI_TAG_LOOKUP.get(tag.trim()) {
                return Some(*market);
            }
        }

        None
    }

    pub async fn backfill_kalshi_politics_history(
        &self,
        duration: Duration,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();

        for category in models::MarketTag::Politics.identifiers(protos::Platform::Kalshi) {
            let this = self.clone();
            let shutdown = shutdown.clone();

            join_set.spawn(async move {
                let result = this
                    .inner
                    .http_client
                    .get_all_series(SeriesQuery {
                        category: Some(category.to_string()),
                        include_product_metadata: Some(true),
                        include_volume: Some(true),
                        ..Default::default()
                    })
                    .await?;

                let tickers = result.series.into_iter().map(|s| s.ticker).collect();

                this.backfill_kalshi_for_market_tag(
                    duration,
                    models::MarketTag::Politics,
                    tickers,
                    shutdown,
                )
                .await
            });
        }

        let mut error_count = 0;
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let in_hrs = duration.as_secs() / (60 * 60);
                    tracing::error!(
                        "kalshi backfill(Politics) failed for duration({in_hrs}hrs): {e:?}"
                    );
                    error_count += 1;
                }
                Err(join_err) => {
                    tracing::error!("kalshi backfill(Politics) task panicked: {join_err:?}");
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            return Err(anyhow::anyhow!(
                "Kalshi backfill(Politics) completed with errors. See logs.",
            ));
        }

        tracing::info!("kalshi backfill(Politics) completed successfully");
        Ok(())
    }

    pub async fn backfill_kalshi_sport_history(
        &self,
        duration: Duration,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let result = self
            .inner
            .http_client
            .get_all_series(SeriesQuery {
                category: Some("Sports".to_string()),
                ..Default::default()
            })
            .await?;

        let mut soccer = Vec::with_capacity(result.series.len() / 3);
        let mut football = Vec::with_capacity(result.series.len() / 3);
        let mut basketball = Vec::with_capacity(result.series.len() / 3);
        {
            for market in result.series {
                let Some(tags) = &market.tags else { continue };

                for tag in tags {
                    match tag.as_str().trim() {
                        "Soccer" => {
                            soccer.push(market.ticker);
                            break;
                        }
                        "Football" => {
                            football.push(market.ticker);
                            break;
                        }
                        "Basketball" => {
                            basketball.push(market.ticker);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        let mut join_set = JoinSet::new();

        for (tag, markets) in [
            (models::MarketTag::Soccer, soccer),
            (models::MarketTag::Nfl, football),
            (models::MarketTag::Nba, basketball),
        ] {
            let this = self.clone();
            let shutdown = shutdown.clone();
            join_set.spawn(async move {
                this.backfill_kalshi_for_market_tag(duration, tag, markets, shutdown)
                    .await
            });
        }

        let mut error_count = 0;
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let in_hrs = duration.as_secs() / (60 * 60);
                    tracing::error!(
                        "kalshi sport backfill(Sports) failed for duration({in_hrs}hrs) : {e:?}"
                    );
                    error_count += 1;
                }
                Err(join_err) => {
                    tracing::error!("kalshi sport backfill(Sports) task panicked: {join_err:?}");
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            return Err(anyhow::anyhow!(
                "Kalshi sport backfill(Sports) completed, but with errors. See logs.",
            ));
        }

        tracing::info!("kalshi sport backfill(Sports) completed successfully");
        Ok(())
    }

    pub async fn backfill_kalshi_for_market_tag(
        &self,
        duration: Duration,
        market: models::MarketTag,
        tickers: Vec<String>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("time went backwards")?
            .as_secs();

        let min_created_timestamp = now - duration.as_secs();

        let mut container = Vec::with_capacity(3_000);

        let max_retries = 3;

        'outer: for series_ticker in tickers {
            let mut cursor: Option<String> = None;

            loop {
                if shutdown.is_cancelled() {
                    tracing::info!("shutdown received, stopping kalshi backfill");
                    break 'outer;
                }

                let mut retries = 0;

                let result = loop {
                    if shutdown.is_cancelled() {
                        tracing::info!("shutdown received, stopping kalshi backfill");
                        break 'outer;
                    }

                    match self
                        .inner
                        .http_client
                        .get_all_markets(&MarketsQuery {
                            limit: Some(100),
                            cursor: cursor.clone(),
                            status: Some("open".to_string()),
                            series_ticker: Some(series_ticker.clone()),
                            min_created_ts: Some(min_created_timestamp as i64),
                            ..Default::default()
                        })
                        .await
                    {
                        Ok(value) => break Some(value),

                        Err(e) => {
                            retries += 1;
                            let msg = e.to_string();
                            let is_rate_limited =
                                msg.contains("429") || msg.contains("too_many_requests");

                            if !is_rate_limited {
                                tracing::error!("kalshi get_all_markets err(non-429): {e}");
                                break None;
                            }

                            if retries >= max_retries {
                                tracing::error!(
                                    "kalshi get_all_markets gave up after {} attempts (ticker: {}, cursor: {:?}): {}",
                                    retries,
                                    series_ticker,
                                    cursor,
                                    e
                                );
                                break None;
                            }

                            let sleep_ms = if is_rate_limited {
                                2_000 * retries
                            } else {
                                200 * retries
                            };
                            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        }
                    }
                };

                // skip failed page safely
                let result = match result {
                    Some(r) => r,
                    None => break,
                };

                container.extend(result.markets);

                match result.cursor {
                    Some(next) if !next.is_empty() => {
                        cursor = Some(next);
                    }
                    _ => break,
                }
            }
        }

        if container.is_empty() {
            tracing::info!("no kalshi markets found for backfill");
            return Ok(());
        }

        let qdrant_payloads = protos::QdrantPayload::from_markets(
            container,
            market.info().market_category,
            market.info().market_subcategory,
        );

        let needed: Vec<_> = qdrant_payloads
            .into_iter()
            .map(|point| {
                let filter = vector_store::VectorStore::create_cross_platform_filter(
                    Some(protos::Platform::Kalshi),
                    &market,
                );
                (point, filter)
            })
            .collect();

        let ebos = self
            .inner
            .qdrant_client
            .multiple_search_and_insert(needed, 100)
            .await
            .context("qdrant multiple_search_and_insert failed during kalshi backfill")?;

        for ebo in ebos {
            if let Err(e) = self.inner.grpc_tx.send(ebo).await {
                tracing::error!("kalshi backfill: failed to forward discovery ebo to grpc: {e}");
            }
        }

        Ok(())
    }

    pub async fn backfill_kalshi_history(&self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let last_insert = self
            .inner
            .qdrant_client
            .get_last_insert_time(protos::Platform::Kalshi)
            .await?;

        let duration = match last_insert {
            Some(date) => {
                let diff = chrono::Utc::now() - date;

                diff.to_std().unwrap_or(std::time::Duration::from_secs(0))
                    + std::time::Duration::from_secs(3600)
            }
            None => {
                tracing::info!(
                    "no history found in Qdrant for kalshi, doing full initial backfill."
                );
                std::time::Duration::from_secs(25 * 3600 * 5)
            }
        };

        tracing::info!(
            "starting backfill for kalshi from {}",
            format_duration_ago(duration)
        );

        self.backfill_funcs(shutdown, duration).await
    }

    async fn backfill_funcs(
        &self,
        shutdown: CancellationToken,
        duration: Duration,
    ) -> anyhow::Result<()> {
        let (r1, r2) = tokio::join!(
            self.backfill_kalshi_sport_history(duration, shutdown.clone()),
            self.backfill_kalshi_politics_history(duration, shutdown),
        );

        let mut errors = Vec::new();
        for (name, res) in [("sport", r1), ("politics", r2)] {
            if let Err(e) = res {
                errors.push(e.context(format!("kalshi {name} backfill failed")));
            }
        }

        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "multiple backfill errors:\n{}",
                errors
                    .into_iter()
                    .map(|e| format!("- {}", e))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }

        tracing::info!("kalshi backfill completed");
        Ok(())
    }
}
