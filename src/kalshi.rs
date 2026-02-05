use crate::models::{self, MarketTag, QdrantMarketConverter};
use crate::qdrant;
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
    qdrant_client: qdrant::VectorStore,
    account: kalshi_rs::Account,
    read_rate_limiter: Arc<DefaultDirectRateLimiter>,
}

impl MyKalshiClient {
    pub fn new(account: kalshi_rs::Account, qdrant_client: qdrant::VectorStore) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(15).unwrap());
        let read_rate_limiter = RateLimiter::direct(quota);

        let kalshi_http = KalshiClient::new(account.clone());
        let kalshi_ws = KalshiWebsocketClient::new(account.clone());

        tracing::debug!("MyKalshiClient setuped");
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

    pub async fn run_kalshi_ws(&self, shutdown: CancellationToken) -> Result<()> {
        self.ws_client
            .subscribe(vec!["market_lifecycle_v2"], vec![])
            .await?;

        let duration = Duration::from_mins(7);
        let mut seven_minute_ticker = tokio::time::interval(duration);
        let mut ws_is_alive = true;
        loop {
            tokio::select! {
                  _ = shutdown.cancelled() => {
                        tracing::trace!("kalshi received shutdown");
                        self.ws_client.disconnect().await;
                        break;
                  }

                _ = seven_minute_ticker.tick() => {
                    println!("another seven minute: kalshi");
                     }


                  result = self.ws_client.next_message_two(), if ws_is_alive => {
                  let Some(result) = result else {
                      tracing::error!("kalshi WS connection was ended");
                      ws_is_alive = false;
                      continue;
                  };

                  match result {
                      std::result::Result::Ok(msg) => {
                        if let Err(e) = self.handle_kalshi_message(msg).await {
                          tracing::error!("error handling kalshi msg: {e}");
                        }
                      }

                      Err(e) => {
                          tracing::error!("kalshi ws error: {e}");
                      }
                  }
              }
            }
        }

        tracing::debug!("kalshi live mode got shutdown");
        Ok(())
    }

    async fn handle_kalshi_message(&self, msg: KalshiSocketMessage) -> Result<()> {
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

                let market = Self::sort_kalshi_tags(series.series)?;
                let payload = models::QdrantPayload::from_market(
                    value.market,
                    market.info().category,
                    market.info().subcategory,
                );

                let data = models::QdrantPointData::new(payload)?;
                let (point_struct, _) = self.qdrant_client.create_point(data).await?;

                self.qdrant_client.insert(point_struct).await?
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
                tracing::debug!("others from kalshi: {:#?}", others);
            }
        }

        Ok(())
    }

    fn sort_kalshi_tags(series_data: Series) -> Result<MarketTag> {
        let Some(tags) = series_data.tags else {
            anyhow::bail!("no supported tag found")
        };

        for tag in tags {
            match tag.as_str().trim() {
                tag if tag == MarketTag::EPL.info().kalshi_identifier => {
                    return Ok(MarketTag::EPL);
                }
                tag if tag == MarketTag::NBA.info().kalshi_identifier => {
                    return Ok(MarketTag::NBA);
                }
                tag if tag == MarketTag::NFL.info().kalshi_identifier => {
                    return Ok(MarketTag::NFL);
                }

                _ => {}
            }
        }

        anyhow::bail!("no supported tag found")
    }

    pub async fn backfill_kalshi_sport_history(&self) -> Result<()> {
        let result = self
            .http_client
            .get_all_series(SeriesQuery {
                category: Some("Sports".to_string()),
                // tag: Some("Soccer".to_string()),
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

        let this = self.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.resolve_past_sport(MarketTag::EPL, soccer).await
        });

        let this = self.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.resolve_past_sport(MarketTag::NFL, football).await
        });

        let this = self.clone();
        join_set.spawn(async move {
            this.read_rate_limiter.until_ready().await;
            this.resolve_past_sport(MarketTag::NBA, basketball).await
        });

        while let Some(res) = join_set.join_next().await {
            match res {
                std::result::Result::Ok(std::result::Result::Ok(())) => {}
                std::result::Result::Ok(std::result::Result::Err(e)) => {
                    tracing::error!("backfill failed: {e:?}")
                }
                std::result::Result::Err(join_err) => {
                    tracing::error!("backfill task panicked: {join_err:?}")
                }
            }
        }

        tracing::info!("kalshi backfill completed, entering live mode");
        Ok(())
    }

    pub async fn backfill_kalshi_history(&self) -> Result<()> {
        self.backfill_kalshi_sport_history()
            .await
            .context("kalshi sport backfill failed")?;

        // others in the future
        Ok(())
    }

    async fn resolve_past_sport(&self, market: MarketTag, tickers: Vec<String>) -> Result<()> {
        let account = kalshi_rs::Account::new("".to_string(), "".to_string());
        let client = KalshiClient::new(account);

        let mut container = Vec::with_capacity(3_000);
        for series_ticker in tickers {
            let mut cursor: Option<String> = None;
            loop {
                let result = client
                    .get_all_markets(&MarketsQuery {
                        limit: Some(100),
                        cursor: cursor.clone(),
                        status: Some("open".to_string()),
                        series_ticker: Some(series_ticker.clone()),
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

        let payload = models::QdrantPayload::from_markets(
            container,
            market.info().category,
            market.info().subcategory,
        );

        let data_list = models::QdrantPointData::new_many(payload)?;
        let result = self.qdrant_client.create_points(data_list).await?;

        let point_structs = result.into_iter().map(|(point, _)| point).collect();
        self.qdrant_client.insert_many(point_structs, 100).await
    }
}
