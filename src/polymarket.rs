use crate::models::{QdrantMarketConverter, QdrantPointData};
use crate::vector_store;
use crate::{constants, models};
use alloy::{hex, signers::local::PrivateKeySigner};
use anyhow::{Context, Ok, Result};
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
    tx: mpsc::Sender<models::Todos>,
}

impl MyPolymarketClient {
    pub fn new(qdrant_client: vector_store::VectorStore, tx: mpsc::Sender<models::Todos>) -> Self {
        tracing::debug!("MyPolymarketClient setupped");

        Self {
            gamma_client: gamma::Client::new(),
            clob_ws_client: clob::ws::ClobWsClient::new(),
            qdrant_client,
            tx,
        }
    }

    pub async fn run_polymarket(&mut self, shutdown: CancellationToken) -> Result<()> {
        let signer = get_signer()?;

        println!("address is - {}", signer.address());
        self.clob_ws_client.subscribe_market(vec![], true).await?;

        let duration = Duration::from_mins(5);
        let mut five_minute_ticker = tokio::time::interval(duration);
        let mut ws_is_alive = true;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    self.clob_ws_client.disconnect().await;
                    break;
                }

                _ = five_minute_ticker.tick() => {
                    println!("another five minutes: polymarket");
                }

                msg = self.clob_ws_client.next_message(), if ws_is_alive => {
                    match msg {
                        Some(msg) => {
                            if let Err(e) = self.handle_polymarket_wss_message(msg).await {
                                tracing::error!("error from fn wss handler: {e}");
                            }
                        }

                        None => {
                            tracing::error!("polymarket WS connection was ended");
                            ws_is_alive = false;
                        }
                    }
                }
            }
        }

        tracing::debug!("polymarket live mode is shutting down");
        Ok(())
    }

    async fn handle_polymarket_wss_message(&self, msg: WsMessage) -> Result<()> {
        match msg {
            clob::ws::WsMessage::NewMarket(msg) => {
                let value = self
                    .gamma_client
                    .get_market_by_id(msg.id.as_str(), Some(true))
                    .await?;

                let Some(ref tags) = value.tags else {
                    anyhow::bail!("tags are missing")
                };

                let market = Self::sort_polymarket_tags(tags)?;

                let payload = models::QdrantPayload::from_market(
                    value,
                    market.info().category,
                    market.info().subcategory,
                );

                let data = models::QdrantPointData::new(payload)?;

                self.qdrant_client
                    .insert_and_search(
                        data,
                        constants::SIMILARITY_SCORE_THRESHOLD,
                        vector_store::VectorStore::create_cross_platform_filter(
                            Some(constants::PLATFORM_POLYMARKET.to_string()),
                            &market,
                        ),
                    )
                    .await?
            }

            other => {
                anyhow::bail!("other Polymarket message: {:#?}\n", other)
            }
        }

        Ok(())
    }

    fn sort_polymarket_tags(tags: &[gamma::Tag]) -> Result<models::MarketTag> {
        for tag in tags {
            match tag.id.as_str().trim() {
                id if id == models::MarketTag::EPL.info().polymarket_identifier => {
                    return Ok(models::MarketTag::EPL);
                }
                id if id == models::MarketTag::NBA.info().polymarket_identifier => {
                    return Ok(models::MarketTag::NBA);
                }
                id if id == models::MarketTag::NFL.info().polymarket_identifier => {
                    return Ok(models::MarketTag::NFL);
                }
                _ => {}
            }
        }

        anyhow::bail!("no supported tag found")
    }

    async fn backfill_polymarket_sport_history(&self) -> Result<()> {
        let duration = Duration::from_hours(24 * 5); // past 5 days
        let tags = [
            models::MarketTag::EPL,
            models::MarketTag::NBA,
            models::MarketTag::NFL,
        ];

        let mut join_set = JoinSet::new();

        for tag in tags {
            let client = self.clone();
            let duration_secs = duration.as_secs();

            join_set.spawn(async move { client.resolve_past_sport(duration_secs, tag).await });
        }

        while let Some(res) = join_set.join_next().await {
            match res {
                std::result::Result::Ok(std::result::Result::Ok(())) => {}
                std::result::Result::Ok(std::result::Result::Err(e)) => {
                    tracing::error!("backfill failed: {e:?}")
                }
                std::result::Result::Err(join_err) => {
                    tracing::error!("task panicked: {join_err:?}")
                }
            }
        }

        tracing::info!("polymarket backfill completed, entering live mode");
        Ok(())
    }

    async fn resolve_past_sport(
        &self,
        seconds_duration: u64,
        market: models::MarketTag,
    ) -> Result<()> {
        let time_diff = Utc::now() - ChronoDuration::seconds(seconds_duration as i64);
        let formatted_iso = time_diff.to_rfc3339_opts(SecondsFormat::Secs, true);

        let (mut offset, limit): (usize, usize) = (0, 100);

        let mut container = Vec::with_capacity(limit);
        loop {
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
                    // volume_num_min: Some(1_000.0),
                    // liquidity_num_min: Some(100.0),
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

        let payload = models::QdrantPayload::from_markets(
            container,
            market.info().category,
            market.info().subcategory,
        );

        let data_list = models::QdrantPointData::new_many(payload)?;

        let needed = data_list
            .into_iter()
            .map(|point| {
                let filter = vector_store::VectorStore::create_cross_platform_filter(
                    Some(constants::PLATFORM_POLYMARKET.to_string()),
                    &market,
                );
                (point, filter)
            })
            .collect();

        self.qdrant_client.insert_many_and_search(needed, 100).await
    }

    pub async fn backfill_polymarket_history(&self) -> Result<()> {
        self.backfill_polymarket_sport_history()
            .await
            .context("kalshi sport backfill failed")?;

        // others in the future
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

    println!("New wallet created");
    println!("Address: {}", signer.address());

    fs::write(Path::new(PRIVATE_KEY_FILE), hex_encodeded_private_key)?;
    Ok(signer)
}

fn load_wallet() -> Result<PrivateKeySigner> {
    let data = fs::read_to_string(PRIVATE_KEY_FILE)?;
    let signer = data.trim().parse::<PrivateKeySigner>()?;

    Ok(signer)
}
