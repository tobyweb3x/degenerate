use super::utils;
use crate::constants;
use crate::models::{self, QdrantMarketConverter, protos};
use crate::vector_store;
use anyhow::{Context, Ok, Result};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use kalshi_rs::{
    KalshiClient, KalshiWebsocketClient,
    markets::models::MarketsQuery,
    series::models::{Series, SeriesQuery},
    websocket::models::KalshiSocketMessage,
};
use std::num::NonZeroU32;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct MyKalshiClient {
    http_client: Arc<KalshiClient>,
    ws_client: Arc<KalshiWebsocketClient>,
    qdrant_client: vector_store::VectorStore,
    account: kalshi_rs::Account,
    read_rate_limiter: Arc<DefaultDirectRateLimiter>,
}

impl MyKalshiClient {
    pub fn new(account: kalshi_rs::Account, qdrant_client: vector_store::VectorStore) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(15).unwrap());
        let read_rate_limiter = RateLimiter::direct(quota);

        let kalshi_http = KalshiClient::new(account.clone());
        let kalshi_ws = KalshiWebsocketClient::new(account.clone());

        Self {
            account,
            http_client: Arc::new(kalshi_http),
            ws_client: Arc::new(kalshi_ws),
            qdrant_client,
            read_rate_limiter: Arc::new(read_rate_limiter),
        }
    }

    pub async fn test_ws_connect(&self) -> Result<()> {
        self.ws_client
            .connect()
            .await
            .context("failed to establish Kalshi WebSocket connection")?;

        Ok(())
    }

    pub async fn run_kalshi(&self, shutdown: CancellationToken) -> Result<()> {
        self.ws_client
            .subscribe(vec!["market_lifecycle_v2"], vec![])
            .await?;

        let duration = Duration::from_mins(6);
        let mut six_minute_ticker = tokio::time::interval(duration);
        let mut ws_is_alive = true;
        
        six_minute_ticker.tick().await; // fires first tick
        loop {
            tokio::select! {
                  _ = shutdown.cancelled() => {
                        tracing::trace!("kalshi received shutdown");
                        self.ws_client.disconnect().await;
                        break;
                  }

                _ = six_minute_ticker.tick() => {
                    println!("kalshi six min triggers");
                        let _ = self.backfill_kalshi_sport_history(duration, CancellationToken::new()).await
                        .inspect_err(|e|  tracing::error!("error handling kalshi msg: {e:?}"));
                     }


                  result = self.ws_client.next_message_two(), if ws_is_alive => {
                  let Some(result) = result else {
                      tracing::error!("kalshi WS connection was ended");
                      ws_is_alive = false;
                      continue;
                  };

                  match result {
                      std::result::Result::Ok(msg) => {
                        let _ = self.handle_kalshi_wss_message(msg).await
                        .inspect_err(|e|  tracing::error!("error handling kalshi msg: {e:?}"));

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
            KalshiSocketMessage::SubscribedResponse(res) => {
                println!("Subscribed: {:#?}", res);
            }

            KalshiSocketMessage::OkResponse(res) => {
                println!("OK response: {:#?}", res);
            }

            KalshiSocketMessage::Ping(payload) => {
                if let Err(e) = self.ws_client.send_pong(payload).await {
                    tracing::error!("error sending pong: {e}");
                }
            }

            KalshiSocketMessage::MarketLifecycleV2(event) => {
                if matches!(
                    event.msg.event_type.as_str(),
                    "settled" | "determined" | "close_date_updated" | "deactivated"
                ) {
                    return Ok(());
                }

                let value = self
                    .http_client
                    .get_market(&event.msg.market_ticker)
                    .await?;

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
                    market.info().category,
                    market.info().subcategory,
                );

                self.qdrant_client
                    .insert_and_search(
                        qdrant_payload,
                        vector_store::SIMILARITY_SCORE_THRESHOLD,
                        vector_store::VectorStore::create_cross_platform_filter(
                            Some(constants::PLATFORM_KALSHI.to_string()),
                            &market,
                        ),
                    )
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

            others => {
                tracing::info!("others from kalshi: {:#?}", others);
            }
        }

        Ok(())
    }

    fn sort_kalshi_tags(series_data: Series) -> Option<models::MarketTag> {
        let Some(tags) = series_data.tags else {
            return None;
        };

        for tag in tags {
            match tag.as_str().trim() {
                tag if tag == models::MarketTag::EPL.info().kalshi_identifier => {
                    return Some(models::MarketTag::EPL);
                }
                tag if tag == models::MarketTag::NBA.info().kalshi_identifier => {
                    return Some(models::MarketTag::NBA);
                }
                tag if tag == models::MarketTag::NFL.info().kalshi_identifier => {
                    return Some(models::MarketTag::NFL);
                }

                _ => {}
            }
        }

        // anyhow::bail!("no supported tag found(kalshi")
        None
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
                include_product_metadata: Some(true),
                include_volume: Some(true),
                ..Default::default()
            })
            .await?;

        let mut soccer = Vec::with_capacity(result.series.len() / 3);
        let mut football = Vec::with_capacity(result.series.len() / 3);
        let mut basketball = Vec::with_capacity(result.series.len() / 3);

        for market in result.series {
            let Some(tags) = &market.tags else { continue };

            if market.category.trim() != "Sports" {
                continue;
            }

            for tag in tags {
                match tag.as_str().trim() {
                    "Soccer" => {
                        soccer.push(market.ticker.clone());
                        break;
                    }
                    "Football" => {
                        football.push(market.ticker.clone());
                        break;
                    }
                    "Basketball" => {
                        basketball.push(market.ticker.clone());
                        break;
                    }
                    _ => {}
                }
            }
        }

        let mut join_set = JoinSet::new();
        let cloned_shutdown = shutdown.clone();
        let this = self.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.backfill_kalshi_sport_for_market_tag(
                duration,
                models::MarketTag::EPL,
                soccer,
                cloned_shutdown,
            )
            .await
        });

        let this = self.clone();
        let cloned_shutdown = shutdown.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.backfill_kalshi_sport_for_market_tag(
                duration,
                models::MarketTag::NFL,
                football,
                cloned_shutdown,
            )
            .await
        });

        let this = self.clone();
        let cloned_shutdown = shutdown.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.backfill_kalshi_sport_for_market_tag(
                duration,
                models::MarketTag::NBA,
                basketball,
                cloned_shutdown,
            )
            .await
        });

        while let Some(res) = join_set.join_next().await {
            match res {
                std::result::Result::Ok(std::result::Result::Ok(())) => {}
                std::result::Result::Ok(std::result::Result::Err(e)) => {
                    tracing::error!("kalshi sport backfill failed: {e:?}")
                }
                std::result::Result::Err(join_err) => {
                    tracing::error!("kalshi sport backfill task panicked: {join_err:?}")
                }
            }
        }

        tracing::info!("kalshi sport backfill completed");
        Ok(())
    }

    async fn backfill_kalshi_sport_for_market_tag(
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

                let result = self
                    .http_client
                    .get_all_markets(&MarketsQuery {
                        limit: Some(100),
                        cursor: cursor.clone(),
                        status: Some("open".to_string()),
                        series_ticker: Some(series_ticker.clone()),
                        min_created_ts: Some(min_created_timestamp as i64),
                        ..Default::default()
                    })
                    .await?;

                container.extend(result.markets);

                match result.cursor {
                    Some(next) if !next.is_empty() => cursor = Some(next),
                    _ => break,
                }
            }
        }

        let qdrant_payloads = protos::QdrantPayload::from_markets(
            container,
            market.info().category,
            market.info().subcategory,
        );

        let needed = qdrant_payloads
            .into_iter()
            .map(|point| {
                let filter = vector_store::VectorStore::create_cross_platform_filter(
                    Some(constants::PLATFORM_KALSHI.to_string()),
                    &market,
                );
                (point, filter)
            })
            .collect();

        self.qdrant_client.insert_many_and_search(needed, 100).await
    }

    pub async fn backfill_kalshi_history(&self, shutdown: CancellationToken) -> Result<()> {
        let last_insert = self
            .qdrant_client
            .get_last_insert_time(constants::PLATFORM_KALSHI)
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
            utils::format_duration_ago(duration)
        );

        self.backfill_kalshi_sport_history(duration, shutdown)
            .await
            .context("kalshi sport backfill failed")?;

        // others in the future
        Ok(())
    }
}
