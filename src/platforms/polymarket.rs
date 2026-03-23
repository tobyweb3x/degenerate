use super::{WsEventMessage, format_duration_ago};
use crate::models::QdrantMarketConverter;
use crate::models::{self, protos};
use crate::vector_store;
use alloy::{hex, signers::local::PrivateKeySigner};
use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use polymarket_hft::client::polymarket::{clob, clob::ws::WsMessage, gamma};
use std::{fs, path::Path, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

const PRIVATE_KEY_FILE: &str = "privateKey.hex";

#[derive(Clone)]
pub struct MyPolymarketClient {
    gamma_client: gamma::Client,
    clob_ws_client: clob::ws::ClobWsClient,
    qdrant_client: vector_store::VectorStore,
    ws_tx: mpsc::Sender<WsEventMessage>,
}

impl MyPolymarketClient {
    pub fn new(
        qdrant_client: vector_store::VectorStore,
        ws_tx: mpsc::Sender<WsEventMessage>,
    ) -> Self {
        Self {
            gamma_client: gamma::Client::new(),
            clob_ws_client: clob::ws::ClobWsClient::new(),
            qdrant_client,
            ws_tx,
        }
    }

    pub fn gamma_client(&self) -> &gamma::Client {
        &self.gamma_client
    }

    pub fn ws_client(&self) -> clob::ws::ClobWsClient {
        self.clob_ws_client.clone()
    }

    pub async fn run_polymarket(&mut self, shutdown: CancellationToken) -> Result<()> {
        let _signer = get_signer()?;

        self.clob_ws_client.subscribe_market(vec![], true).await?;

        let duration = Duration::from_mins(5);
        let mut five_minute_ticker = tokio::time::interval(duration);
        let mut ws_is_alive = true;

        five_minute_ticker.tick().await; // fires first tick
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    self.clob_ws_client.disconnect().await;
                    break;
                }

                _ = five_minute_ticker.tick() => {
                    println!("polymarket five min triggers");
                    let _ = self.backfill_polymarket_sport_history(duration, CancellationToken::new()).await
                    .inspect_err(|e| tracing::error!("error handling polymarket msg(backfill): {e:?}"));
                }

                result = self.clob_ws_client.next_message(), if ws_is_alive => {
                      let Some(msg) = result else {
                      tracing::error!("polymarket WS connection was ended");
                      ws_is_alive = false;
                      continue;
                  };

                    let _ = self.handle_polymarket_wss_message(msg).await
                    .inspect_err(|e| tracing::error!("error handling polymarket msg(wss): {e:?}"));
                }
            }
        }

        tracing::info!("polymarket live mode is shutting down");
        Ok(())
    }

    async fn handle_polymarket_wss_message(&self, msg: WsMessage) -> Result<()> {
        match msg {
            clob::ws::WsMessage::NewMarket(msg) => {
                let value = self
                    .gamma_client
                    .get_market_by_slug(msg.slug.as_str(), Some(true))
                    .await
                    .context("error getting polymarket data by slug")?;

                let Some(ref tags) = value.tags else {
                    anyhow::bail!("tags are missing")
                };

                let Some(market) = Self::sort_polymarket_tags(tags) else {
                    return Ok(());
                };

                let qdrant_payload = protos::QdrantPayload::from_market(
                    value,
                    market.info().market_category,
                    market.info().market_subcategory,
                );

                self.qdrant_client
                    .search_and_insert(
                        qdrant_payload,
                        vector_store::SIMILARITY_SCORE_THRESHOLD,
                        vector_store::VectorStore::create_cross_platform_filter(
                            Some(protos::Platform::Polymarket),
                            &market,
                        ),
                    )
                    .await?
            }

            clob::ws::WsMessage::BestBidAsk(best) => {
                self.ws_tx
                    .send(WsEventMessage::Polymarket(clob::ws::WsMessage::BestBidAsk(
                        best,
                    )))
                    .await?
            }

            clob::ws::WsMessage::PriceChange(change) => {
                self.ws_tx
                    .send(WsEventMessage::Polymarket(
                        clob::ws::WsMessage::PriceChange(change),
                    ))
                    .await?
            }

            other => {
                tracing::info!("ws message: {other:#?}")
            }
        }

        Ok(())
    }

    fn sort_polymarket_tags(tags: &[gamma::Tag]) -> Option<models::MarketTag> {
        for tag in tags {
            match tag.id.as_str().trim() {
                id if id == models::MarketTag::EPL.info().polymarket_identifier => {
                    return Some(models::MarketTag::EPL);
                }
                id if id == models::MarketTag::NBA.info().polymarket_identifier => {
                    return Some(models::MarketTag::NBA);
                }
                id if id == models::MarketTag::NFL.info().polymarket_identifier => {
                    return Some(models::MarketTag::NFL);
                }
                _ => {
                    return None;
                }
            }
        }

        None
    }

    async fn backfill_polymarket_sport_history(
        &self,
        duration_in_secs: Duration,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let tags = [
            models::MarketTag::EPL,
            models::MarketTag::NBA,
            models::MarketTag::NFL,
        ];

        let mut join_set = JoinSet::new();
        let duration = duration_in_secs.as_secs();
        for tag in tags {
            let client = self.clone();
            let cloned_shutdown = shutdown.clone();
            join_set.spawn(async move {
                client
                    .backfill_polymakert_sport_for_market_tag(duration, tag, cloned_shutdown)
                    .await
            });
        }

        let mut error_count = 0;
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let in_hrs = duration / (60 * 60);
                    tracing::error!(
                        "polymarket sport backfill failed for duration({in_hrs}hrs) : {e:?}"
                    );
                    error_count += 1;
                }

                Err(join_err) => {
                    tracing::error!("polymarket sport backfill task panicked: {join_err:?}");
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            return Err(anyhow::anyhow!(
                "polymarket sport backfill completed, but with errors. See logs.",
            ));
        }

        tracing::info!("polymarket sport backfill completed successfully");
        Ok(())
    }

    async fn backfill_polymakert_sport_for_market_tag(
        &self,
        seconds_duration: u64,
        market: models::MarketTag,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let time_diff = Utc::now() - ChronoDuration::seconds(seconds_duration as i64);
        let formatted_iso = time_diff.to_rfc3339_opts(SecondsFormat::Secs, true);

        let (mut offset, limit): (usize, usize) = (0, 100);

        let mut container = Vec::with_capacity(limit);
        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let markets = self
                .gamma_client
                .get_markets(gamma::GetMarketsRequest {
                    limit: Some(limit as u32),
                    offset: Some(offset as u32),
                    tag_id: Some(market.info().polymarket_identifier),
                    closed: Some(false),
                    ascending: Some(false),
                    related_tags: Some(true),
                    include_tag: Some(true),
                    start_date_min: Some(formatted_iso.as_str()),
                    ..Default::default()
                })
                .await?;

            let market_len = markets.len();
            container.extend(markets);

            offset += limit;
            if market_len < limit {
                break;
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
                    Some(protos::Platform::Polymarket),
                    &market,
                );
                (point, filter)
            })
            .collect();

        self.qdrant_client
            .multiple_search_and_inserth(needed, 100)
            .await
    }

    pub async fn backfill_polymarket_history(&self, shutdown: CancellationToken) -> Result<()> {
        let last_insert = self
            .qdrant_client
            .get_last_insert_time(protos::Platform::Polymarket)
            .await?;

        let duration = match last_insert {
            Some(date) => {
                let diff = chrono::Utc::now() - date;

                diff.to_std().unwrap_or(std::time::Duration::from_secs(0))
                    + std::time::Duration::from_secs(3600)
            }
            None => {
                tracing::info!(
                    "no history found in Qdrant for polymarket, doing full initial backfill."
                );
                std::time::Duration::from_secs(25 * 3600 * 5)
            }
        };

        tracing::info!(
            "starting backfill for polymarket from {}",
            format_duration_ago(duration)
        );

        // sports
        self.backfill_polymarket_sport_history(duration, shutdown.clone())
            .await
            .context("polymarket sport backfill errored")?;

        // others in the future
        tracing::info!("polymarket backfill completed");
        Ok(())
    }
}

fn get_signer() -> Result<PrivateKeySigner> {
    let signer = if Path::new(PRIVATE_KEY_FILE).exists() {
        load_wallet()?
    } else {
        write_new_wallet()?
    };

    Ok(signer)
}

fn write_new_wallet() -> Result<PrivateKeySigner> {
    let signer = PrivateKeySigner::random();
    let hex_encodeded_private_key = hex::encode(signer.to_bytes());

    tracing::info!("New wallet created");
    tracing::info!("Address: {}", signer.address());

    fs::write(Path::new(PRIVATE_KEY_FILE), hex_encodeded_private_key)?;
    Ok(signer)
}

fn load_wallet() -> Result<PrivateKeySigner> {
    let data = fs::read_to_string(PRIVATE_KEY_FILE)?;
    let signer = data.trim().parse::<PrivateKeySigner>()?;

    Ok(signer)
}
