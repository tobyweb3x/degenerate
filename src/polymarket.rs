use crate::models;
use crate::qdrant;
use alloy::{hex, signers::local::PrivateKeySigner};
use anyhow::{Ok, Result};
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use polymarket_hft::client::polymarket::{clob, clob::ws::WsMessage, gamma};
use std::fs as std_fs;
use std::io::Write;
use std::{fs, path::Path, time::Duration};
use tokio::io::{AsyncWriteExt, BufWriter as tokio_bufWriter};
use tokio::{self, fs as tokio_fs};
use tokio_util::sync::CancellationToken;
const PRIVATE_KEY_FILE: &str = "privateKey.hex";
use serde::{Deserialize, Serialize};

pub struct PolymarketClient {
    gamma_client: gamma::Client,
    clob_client: clob::ws::ClobWsClient,
    qdrant_client: qdrant::VectorStore,
}

impl PolymarketClient {
    pub fn new(qdrant_client: qdrant::VectorStore) -> Self {
        Self {
            gamma_client: gamma::Client::new(),
            clob_client: clob::ws::ClobWsClient::new(),
            qdrant_client,
        }
    }

    pub async fn run_polymarket(&mut self, shutdown: CancellationToken) -> Result<()> {
        let signer = get_signer()?;

        println!("address is - {}", signer.address());
        self.clob_client.subscribe_market(vec![], true).await?;

        let duration = Duration::from_mins(2);
        let mut five_minute_ticker = tokio::time::interval(duration);
        loop {
            tokio::select! {
                // biased;
                _ = shutdown.cancelled() => {
                    println!("received shutdown");
                    self.clob_client.disconnect().await;
                    break;
                }

                _ = five_minute_ticker.tick() => {
                    println!("another five seconds");
                    let ll = self.poll_polymarket_historical_data(duration.as_secs(), MarketTag::EPL).await;
                }

                msg = self.clob_client.next_message() => {
                    match msg {
                        Some(msg) => {
                            if let Err(e) = self.handle_polymarket_message(msg).await {
                                tracing::error!("error from fn handle_polymarket_message: {e}");
                            }
                        }

                        None => {
                            println!("polymarket WS connection ended");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_polymarket_message(&self, msg: WsMessage) -> Result<()> {
        match msg {
            clob::ws::WsMessage::NewMarket(msg) => {
                let result = self
                    .gamma_client
                    .get_market_by_id(msg.id.as_str(), Some(true))
                    .await?;
                let json = serde_json::to_string_pretty(&result)?;
                let mut file = std_fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("polymarketNewEvent.ndjson")?;
                writeln!(file, "{json}")?;
            }

            other => {
                println!("other Polymarket message: {:#?}\n", other);
            }
        }

        Ok(())
    }

    pub async fn poll_polymarket_historical_data(
        &self,
        seconds_duration: u64,
        market: MarketTag,
    ) -> Result<()> {
        // let file = tokio_fs::OpenOptions::new()
        //     .create(true)
        //     .append(true)
        //     .open("PolymarketSportEPL.ndjson")
        //     .await?;
        // let mut writer = tokio_bufWriter::with_capacity(64 * 1024, file);

        let time_diff = Utc::now() - ChronoDuration::seconds(seconds_duration as i64);
        let formatted_iso = time_diff.to_rfc3339_opts(SecondsFormat::Secs, true);

        let (mut offset, limit): (usize, usize) = (0, 100);
        let (mut count, mut num) = (0, 0);
        println!("number of count, {:?}", count);

        let mut container = Vec::with_capacity(limit);
        loop {
            let markets = self
                .gamma_client
                .get_markets(gamma::GetMarketsRequest {
                    limit: Some(limit as u32),
                    offset: Some(offset as u32),
                    tag_id: Some(market.info().tag_id),
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
            println!("len of market, {:?}", market_len);
            if market_len == 0 {
                println!("exiting loop, market_len is zero");
                break;
            }

            container.extend(markets);
            // for market in markets {
            // let json = serde_json::to_string(&market)?;
            // writer.write_all(json.as_bytes()).await?;
            // writer.write_all(b"\n").await?;
            // }

            count += 1;
            num += market_len;
            println!("number of count/num, {}/{}", count, num);

            offset += limit;
            if market_len < limit {
                println!("exiting loop, market_len is less than limit");
                break;
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

        // writer.flush().await?;
        // writer.get_ref().sync_all().await?;
        // Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MarketTag {
    EPL,
    NBA,
    NFL,
}

pub struct MarketTagInfo {
    pub tag_id: &'static str,
    pub category: &'static str,
    pub subcategory: &'static str,
}

impl MarketTag {
    pub fn info(&self) -> MarketTagInfo {
        match self {
            Self::EPL => MarketTagInfo {
                tag_id: "306",
                category: "sport",
                subcategory: "EPL",
            },
            Self::NBA => MarketTagInfo {
                tag_id: "745",
                category: "sport",
                subcategory: "NBA",
            },
            Self::NFL => MarketTagInfo {
                tag_id: "450",
                category: "crypto",
                subcategory: "NFL",
            },
        }
    }
}
impl std::fmt::Display for MarketTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = self.info();
        write!(f, "{}_{}_{}", info.category, info.subcategory, info.tag_id)
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
