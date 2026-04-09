use super::{WsEventMessage, format_duration_ago};
use crate::models::{self, QdrantMarketConverter, protos};
use crate::vector_store;
use anyhow::{Context, Result};
use kalshi_rs::{
    KalshiClient, KalshiWebsocketClient,
    markets::models::MarketsQuery,
    ratelimiter::{RateLimitTier, RateLimiterConfig},
    series::models::{Series, SeriesQuery},
    websocket::models::KalshiSocketMessage,
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct MyKalshiClient {
    http_client: KalshiClient,
    ws_client: Arc<KalshiWebsocketClient>,
    qdrant_client: vector_store::VectorStore,
    ws_tx: mpsc::Sender<WsEventMessage>,
}

impl MyKalshiClient {
    pub fn new(
        account: kalshi_rs::Account,
        qdrant_client: vector_store::VectorStore,
        ws_tx: mpsc::Sender<WsEventMessage>,
    ) -> Self {
        let kalshi_http = KalshiClient::new_with_config(
            account.clone(),
            None,
            Some(RateLimitTier::Custom {
                read_rps: 4,
                write_rps: 2,
            }),
            Some(RateLimiterConfig::default()),
        );

        let kalshi_ws = KalshiWebsocketClient::new(account);

        Self {
            http_client: kalshi_http,
            ws_client: Arc::new(kalshi_ws),
            qdrant_client,
            ws_tx,
        }
    }

    pub async fn test_ws_connect(&self) -> Result<()> {
        self.ws_client
            .connect()
            .await
            .context("failed to establish Kalshi WebSocket connection")?;

        Ok(())
    }

    pub fn ws_client(&self) -> Arc<KalshiWebsocketClient> {
        self.ws_client.clone()
    }

    pub fn http_client(&self) -> &KalshiClient {
        &self.http_client
    }

    pub async fn run_kalshi(&self, shutdown: CancellationToken) -> Result<()> {
        self.ws_client
            .subscribe(vec!["market_lifecycle_v2"], vec![])
            .await?;

        let duration = Duration::from_secs(60 * 60 + 10 * 60);
        let mut one_hour_ten_min_ticker = tokio::time::interval(duration);
        let mut ws_is_alive = true;

        one_hour_ten_min_ticker.tick().await; // fires first ticks
        loop {
            tokio::select! {
                  _ = shutdown.cancelled() => {
                        tracing::trace!("kalshi received shutdown");
                        self.ws_client.disconnect().await;
                        break;
                  }

                _ = one_hour_ten_min_ticker.tick() => {
                    println!("kalshi one_hour_ten_min_ticker triggers");
                        let _ = self.backfill_kalshi_sport_history(duration, CancellationToken::new()).await
                        .inspect_err(|e|  tracing::error!("error handling kalshi msg(backfill): {e:?}"));
                     }


                  result = self.ws_client.next_message_two(), if ws_is_alive => {
                  let Some(result) = result else {
                      tracing::error!("kalshi WS connection was ended");
                      ws_is_alive = false;
                      continue;
                  };

                  match result {
                      Ok(msg) => {
                        let _ = self.handle_kalshi_wss_message(msg).await
                        .inspect_err(|e|  tracing::error!("error handling kalshi msg(wss): {e:?}"));

                      }

                      Err(e) => {
                          tracing::error!("kalshi ws error: {e:?}");
                      }
                  }
              }
            }
        }

        tracing::info!("kalshi live mode got shutdown");
        Ok(())
    }

    async fn handle_kalshi_wss_message(&self, msg: KalshiSocketMessage) -> Result<()> {
        match msg {
            // !!: this does not make sense no more, let just poll, new market is def. illiquid
            KalshiSocketMessage::MarketLifecycleV2(event) => {
                if matches!(event.msg.event_type.as_str(), "created" | "activated") {
                    let Ok(value) = self
                        .http_client
                        .get_market(event.msg.market_ticker.as_str())
                        .await
                    else {
                        return Ok(());
                    };

                    let event = self
                        .http_client
                        .get_event(value.market.event_ticker.as_str())
                        .await?;

                    let series = self
                        .http_client
                        .get_series_by_ticker(event.event.series_ticker.as_str())
                        .await?;

                    let Some(market) = Self::sort_kalshi_tags(series.series) else {
                        return Ok(());
                    };

                    let qdrant_payload = protos::QdrantPayload::from_market(
                        value.market,
                        market.info().market_category,
                        market.info().market_subcategory,
                    );

                    self.qdrant_client
                        .search_and_insert(
                            qdrant_payload,
                            vector_store::SIMILARITY_SCORE_THRESHOLD,
                            vector_store::VectorStore::create_cross_platform_filter(
                                Some(protos::Platform::Kalshi),
                                &market,
                            ),
                        )
                        .await?;
                    return Ok(());
                }

                if matches!(
                    event.msg.event_type.as_str(),
                    "deactivated" | "close_date_updated" | "determined" | "settled"
                ) {
                    self.ws_tx
                        .send(WsEventMessage::Kalshi(
                            KalshiSocketMessage::MarketLifecycleV2(event),
                        ))
                        .await?;
                }
            }

            KalshiSocketMessage::Ping(payload) => self
                .ws_client
                .send_pong(payload)
                .await
                .context("error sending pong(kalshi)")?,

            KalshiSocketMessage::TickerUpdate(ticker) => {
                self.ws_tx
                    .send(WsEventMessage::Kalshi(KalshiSocketMessage::TickerUpdate(
                        ticker,
                    )))
                    .await?
            }

            KalshiSocketMessage::ErrorResponse(err) => {
                if err
                    .msg
                    .msg
                    .eq_ignore_ascii_case("unable to process message")
                // very common useless err
                {
                    return Ok(());
                }
                tracing::error!("Kalshi error {}: {}", err.msg.code, err.msg.msg);
            }

            _ => {}
        }

        Ok(())
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
    ) -> Result<()> {
        let mut join_set = tokio::task::JoinSet::new();

        for category in models::MarketTag::Politics.identifiers(protos::Platform::Kalshi) {
            let this = self.clone();
            let shutdown = shutdown.clone();

            join_set.spawn(async move {
                let result = this
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
    ) -> Result<()> {
        let result = self
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

    async fn backfill_kalshi_for_market_tag(
        &self,
        duration: Duration,
        market: models::MarketTag,
        tickers: Vec<String>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("time went backwards")?
            .as_secs();

        let min_created_timestamp = now - duration.as_secs();

        let mut container = Vec::with_capacity(3_000);
        for series_ticker in tickers {
            let mut cursor: Option<String> = None;
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                let result = match self
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
                    Ok(value) => value,
                    Err(e) => {
                        tracing::error!("errors from kalshi backfill get_all_markets: {e:?}");
                        continue;
                    }
                };

                container.extend(result.markets);

                match result.cursor {
                    Some(next) if !next.is_empty() => cursor = Some(next),
                    _ => break,
                }
            }
        }

        let qdrant_payloads = protos::QdrantPayload::from_markets(
            container,
            market.info().market_category,
            market.info().market_subcategory,
        );

        let needed = qdrant_payloads
            .into_iter()
            .map(|point| {
                let filter = vector_store::VectorStore::create_cross_platform_filter(
                    Some(protos::Platform::Kalshi),
                    &market,
                );
                (point, filter)
            })
            .collect();

        self.qdrant_client
            .multiple_search_and_inserth(needed, 100)
            .await
    }

    pub async fn backfill_kalshi_history(&self, shutdown: CancellationToken) -> Result<()> {
        let last_insert = self
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

        self.backfill_kalshi_sport_history(duration, shutdown.clone())
            .await
            .context("kalshi sport backfill errored")?;

        self.backfill_kalshi_politics_history(duration, shutdown)
            .await
            .context("kalshi politics backfill errored")?;

        // others in the future
        tracing::info!("kalshi backfill completed");

        Ok(())
    }
}
