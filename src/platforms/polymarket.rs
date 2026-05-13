use super::{WsEventMessage, format_duration_ago};
use crate::models::{self, QdrantMarketConverter, protos};
use crate::vector_store;
use anyhow::Context;
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
// use futures_util::{SinkExt, StreamExt, stream};
use parking_lot::RwLock;
use polymarket_hft::client::polymarket::{
    clob::{
        self,
        ws::{Channel, WsMessage},
    },
    gamma,
};
use std::sync::Arc;
use std::time;
use std::{collections::HashSet, fs, time::Duration};
// use tokio::net::TcpStream;
use tokio::{sync::mpsc, task::JoinSet};
// use tokio_tungstenite::{
//     MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
// };
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
#[error("polymarket clob reconnect failed")]
pub struct PolymarketReconnectError;

#[derive(Clone)]
pub struct MyPolymarketClient {
    inner: Arc<PolymarketInner>,
}
pub struct PolymarketInner {
    gamma_client: gamma::Client,
    clob_ws_client: clob::ws::ClobWsClient,
    qdrant_client: vector_store::VectorStore,
    ws_msg_tx: mpsc::Sender<WsEventMessage>,
    grpc_tx: mpsc::Sender<crate::models::protos::ClientEbo>,
    order_book_http: clob::Client,
    // okbet_tx: Arc<
    //     Mutex<
    //         Option<
    //             stream::SplitSink<
    //                 WebSocketStream<MaybeTlsStream<TcpStream>>,
    //                 tokio_tungstenite::tungstenite::Message,
    //             >,
    //         >,
    //     >,
    // >,
    // okbet_rx: Arc<Mutex<Option<stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>>,
    subscribed_assets: RwLock<HashSet<String>>,
}

impl MyPolymarketClient {
    pub fn new(
        qdrant_client: vector_store::VectorStore,
        ws_msg_tx: mpsc::Sender<WsEventMessage>,
        grpc_tx: mpsc::Sender<crate::models::protos::ClientEbo>,
    ) -> Self {
        Self {
            inner: Arc::new(PolymarketInner {
                gamma_client: gamma::Client::new(),
                clob_ws_client: clob::ws::ClobWsClient::new(),
                qdrant_client,
                ws_msg_tx,
                grpc_tx,
                order_book_http: clob::Client::new(),
                subscribed_assets: RwLock::new(HashSet::with_capacity(512)),
            }),
        }
    }

    pub fn gamma_client(&self) -> &gamma::Client {
        &self.inner.gamma_client
    }

    pub fn is_asset_subscribed(&self, asset_id: &str) -> bool {
        self.inner.subscribed_assets.read().contains(asset_id)
    }

    pub fn insert_subscribed_asset(&self, asset_id: String) {
        self.inner.subscribed_assets.write().insert(asset_id);
    }

    pub fn remove_subscribed_asset(&self, asset_id: &str) -> bool {
        self.inner.subscribed_assets.write().remove(asset_id)
    }

    pub async fn get_order_book(&self, token_id: &str) -> anyhow::Result<clob::OrderBookSummary> {
        self.inner
            .order_book_http
            .get_order_book(token_id)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn test_ws_connect(&self) -> anyhow::Result<()> {
        self.inner
            .clob_ws_client
            .connect_to_channel(Channel::Market)
            .await
            .context("error connecting ws to polymarket")?;

        // self.connect_okbet_ws()
        //     .await
        //     .context("error connecting ws to okbet")?;
        Ok(())
    }

    pub async fn add_market_ticker(&self, asset_id: &str) -> anyhow::Result<()> {
        if self.is_asset_subscribed(&asset_id) {
            tracing::warn!("polymarket: {asset_id} already in subscribed_assets, skipping");
            return Ok(());
        };

        self.inner
            .clob_ws_client
            .dynamic_subscribe_market_channel(vec![asset_id.to_string()], true)
            .await
            .map_err(anyhow::Error::from)?;

        self.insert_subscribed_asset(asset_id.to_string());
        Ok(())
    }

    pub async fn del_market_ticker(
        &self,
        asset_ids: Vec<String>,
    ) -> anyhow::Result<()> {
        let asset_ids: Vec<String> = asset_ids
            .into_iter()
            .filter(|id| self.is_asset_subscribed(id))
            .collect();

        if asset_ids.is_empty() {
            return Ok(());
        }

        // let payload = serde_json::json!({
        //     "action": "unsubscribe",
        //     "platform": "polymarket",
        //     "markets": asset_ids,
        // });

        self.inner
            .clob_ws_client
            .dynamic_unsubscribe_market_channel(asset_ids.clone())
            .await
            .map_err(anyhow::Error::from)?;

        let mut map = self.inner.subscribed_assets.write();
        asset_ids.iter().for_each(|id| {
            map.remove(id.as_str());
        });

        Ok(())
    }

    pub async fn run_polymarket(&self, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.inner
            .clob_ws_client
            .subscribe_market(vec![], true)
            .await?; // sub for new market & market resolve

        let mut keep_ws_alive = tokio::time::interval(std::time::Duration::from_secs(10));

        let duration = std::time::Duration::from_secs(60 * 60 + 30 * 60);
        let mut one_hour_thirty_min_ticker = tokio::time::interval(duration);

        // let mut okbet_rx_stream = {
        //     let mut guard = self.inner.okbet_rx.lock().await;
        //     guard
        //         .take()
        //         .context("Okbet WebSocket receiver is missing!")?
        // };
        // let mut ok_bet_ws_is_alive = true;

        one_hour_thirty_min_ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                     self.inner.clob_ws_client.disconnect().await;
                    break;
                },

                _ = keep_ws_alive.tick() => {
                    let _ = self.inner.clob_ws_client.send_ping().await
                    .inspect_err(|e| tracing::error!("error sending pong, polymarket: {e}"));
                }

                _ = one_hour_thirty_min_ticker.tick() => {
                    println!("polymarket one_hour_thirty_min_ticker triggers");
                    if let Err(e) = self.backfill_funcs(shutdown.clone(), duration).await {
                         tracing::error!("error backfilling polymarket: {e:?}");
                    };
                }

                polymark_ws_msgs = self.inner.clob_ws_client.new_message_two() => {
                      let Some(msg) = polymark_ws_msgs else {
                        tracing::warn!("polymarket WS stream ended, reconnecting...");

                        if let Err(e) = self.inner.clob_ws_client.reconnect().await {
                            return Err(anyhow::Error::new(PolymarketReconnectError).context(e));
                        }

                        let assets: Vec<String> = self.inner.subscribed_assets.read().iter().cloned().collect();
                        if !assets.is_empty() {
                            if let Err(e) = self.inner.clob_ws_client
                                .dynamic_subscribe_market_channel(assets, true).await {
                                return Err(anyhow::anyhow!("resubscribe after stream-end failed: {e}"));
                            }
                        }

                        continue;
                  };

                  match msg {
                      Ok(msg) => {
                        let Err(err) = self.handle_polymarket_wss_message(msg).await else {
                          continue;
                        };

                        if err.downcast_ref::<PolymarketReconnectError>().is_some() {
                            anyhow::bail!("ws disconnected: {err:?}");
                        }

                        tracing::error!("error handling polymarket msg(wss): {err:?}");
                      }

                      Err(e) => {
                          let err_str = format!("{e:?}");

                          if err_str.contains("transport") || err_str.contains("Io(") || err_str.contains("TLS") || err_str.contains("BadRecordMac") {
                              tracing::warn!("polymarket WS transport error, reconnecting: {e:?}");

                              if let Err(re) = self.inner.clob_ws_client.reconnect().await {
                                  return Err(anyhow::Error::new(PolymarketReconnectError).context(re));
                              }

                              let assets: Vec<String> = self.inner.subscribed_assets.read().iter().cloned().collect();
                              if !assets.is_empty() {
                                  if let Err(e) = self.inner.clob_ws_client.dynamic_subscribe_market_channel(assets, true).await {
                                      return Err(anyhow::anyhow!("resubscribe after transport error failed: {e}"));
                                  }
                              }
                          } else {
                              tracing::error!("polymarket ws error: {e:?}");
                          }
                      }
                  }
                }

                // okbet_ws_msgs = okbet_rx_stream.next() => {
                //     let Some(msg) = okbet_ws_msgs else {
                //         anyhow::bail!("okbet WS connection was ended");
                //         // ok_bet_ws_is_alive = false;
                //         // continue;
                //     };
                //
                //     match msg {
                //         Ok(msg) => {
                //             let _ = self.handle_okbet_messages(msg).await
                //             .inspect_err(|e| tracing::error!("error from handle_okbet_messages:{e}"));
                //
                //         },
                //         Err(e) => {
                //             tracing::error!("err from okbet_rx_stream: {e}")
                //         }
                //     }
                // }
            }
        }

        tracing::info!("polymarket live mode is shutting down");
        Ok(())
    }

    async fn handle_polymarket_wss_message(&self, msg: WsMessage) -> anyhow::Result<()> {
        match msg {
            clob::ws::WsMessage::NewMarket(msg) => {
                let value = self
                    .inner
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

                if let Some(ebo) = self
                    .inner
                    .qdrant_client
                    .search_and_insert(
                        qdrant_payload,
                        vector_store::SIMILARITY_SCORE_THRESHOLD,
                        vector_store::VectorStore::create_cross_platform_filter(
                            Some(protos::Platform::Polymarket),
                            &market,
                        ),
                    )
                    .await?
                {
                    if let Err(e) = self.inner.grpc_tx.send(ebo).await {
                        tracing::error!(
                            "polymarket new_market: failed to forward discovery ebo to grpc: {e}"
                        );
                    }
                }
            }

            clob::ws::WsMessage::BestBidAsk(best) => {
                self.inner
                    .ws_msg_tx
                    .send(WsEventMessage::Polymarket(clob::ws::WsMessage::BestBidAsk(
                        best,
                    )))
                    .await?
            }

            clob::ws::WsMessage::PriceChange(change) => {
                self.inner
                    .ws_msg_tx
                    .send(WsEventMessage::Polymarket(
                        clob::ws::WsMessage::PriceChange(change),
                    ))
                    .await?
            }

            clob::ws::WsMessage::MarketResolved(resolved) => {
                self.inner
                    .ws_msg_tx
                    .send(WsEventMessage::Polymarket(
                        clob::ws::WsMessage::MarketResolved(resolved),
                    ))
                    .await?
            }

            clob::ws::WsMessage::Ping(ping) => self
                .inner
                .clob_ws_client
                .send_pong(ping.into())
                .await
                .context("polymarket error sending pong")?,

            clob::ws::WsMessage::Close(close) => {
                tracing::warn!("polymarket clob client received a close msg: {close:?}");
                self.inner
                    .clob_ws_client
                    .reconnect()
                    .await
                    .map_err(|e| anyhow::Error::new(PolymarketReconnectError).context(e))?;

                let assets: Vec<String> = self
                    .inner
                    .subscribed_assets
                    .read()
                    .iter()
                    .cloned()
                    .collect();

                if assets.is_empty() {
                    tracing::info!("polymarket reconnected; no assets to resubscribe");
                    return Ok(());
                }

                tracing::info!(
                    "polymarket reconnected; resubscribing to {} assets",
                    assets.len()
                );

                if let Err(e) = self
                    .inner
                    .clob_ws_client
                    .dynamic_subscribe_market_channel(assets, true)
                    .await
                {
                    tracing::error!("failed to resubscribe polymarket clob after reconnect: {e}");
                    return Err(anyhow::anyhow!("polymarket resubscribe failed: {e}"));
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn sort_polymarket_tags(tags: &[gamma::Tag]) -> Option<models::MarketTag> {
        for tag in tags {
            if let Some(market) = models::POLYMARKET_TAG_LOOKUP.get(tag.id.trim()) {
                return Some(*market);
            }
        }
        None
    }

    async fn backfill_polymarket_politics_history(
        &self,
        duration_in_secs: Duration,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let duration = duration_in_secs.as_secs();

        self.backfill_polymarket_for_market_tag(duration, models::MarketTag::Politics, shutdown)
            .await?;

        tracing::info!("polymarket politics backfill completed successfully");
        Ok(())
    }

    async fn backfill_polymarket_sport_history(
        &self,
        duration_in_secs: Duration,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let tags = [
            models::MarketTag::Soccer,
            models::MarketTag::Nba,
            models::MarketTag::Nfl,
        ];

        let mut join_set = JoinSet::new();
        let duration = duration_in_secs.as_secs();
        for tag in tags {
            let client = self.clone();
            let cloned_shutdown = shutdown.clone();
            join_set.spawn(async move {
                client
                    .backfill_polymarket_for_market_tag(duration, tag, cloned_shutdown)
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

    pub async fn backfill_polymarket_for_market_tag(
        &self,
        seconds_duration: u64,
        market: models::MarketTag,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let time_diff = Utc::now() - ChronoDuration::seconds(seconds_duration as i64);
        let formatted_iso = time_diff.to_rfc3339_opts(SecondsFormat::Secs, true);

        let tags = market.identifiers(protos::Platform::Polymarket);

        let mut container = Vec::with_capacity(tags.len() * 128);
        let mut seen_markets = HashSet::with_capacity(tags.len() * 128);

        let max_retries = 3;

        'outer: for &tag_id in tags {
            let mut offset: usize = 0;
            let limit: usize = 100;

            loop {
                if shutdown.is_cancelled() {
                    tracing::info!("shutdown received, stopping backfill");
                    break 'outer;
                }

                let mut retries = 0;

                let markets = loop {
                    if shutdown.is_cancelled() {
                        tracing::info!("shutdown received, stopping backfill");
                        break 'outer;
                    }

                    match self
                        .inner
                        .gamma_client
                        .get_markets(gamma::GetMarketsRequest {
                            limit: Some(limit as u32),
                            offset: Some(offset as u32),
                            tag_id: Some(tag_id),
                            closed: Some(false),
                            ascending: Some(false),
                            include_tag: Some(true),
                            start_date_min: Some(formatted_iso.as_str()),
                            ..Default::default()
                        })
                        .await
                    {
                        Ok(markets) => break markets,

                        Err(e) => {
                            retries += 1;
                            let msg = e.to_string();
                            let is_transient = msg.contains("error sending request")
                                || msg.contains("error decoding response body");

                            if !is_transient {
                                tracing::error!(
                                    "polymarket get_market failed(non-transient) (tag: {}, offset: {}, attempt: {}): {}",
                                    tag_id,
                                    offset,
                                    retries,
                                    e
                                );
                                break Vec::new();
                            }

                            if retries >= max_retries {
                                tracing::error!(
                                    "polymarket get_markets gave up after {} attempts (tag: {}, offset: {}): {}",
                                    retries,
                                    tag_id,
                                    offset,
                                    e
                                );
                                break Vec::new();
                            }

                            tokio::time::sleep(Duration::from_millis(200 * retries)).await;
                        }
                    }
                };

                let market_len = markets.len();

                for m in markets {
                    let condition_id = m.condition_id.clone().unwrap_or_default();

                    if seen_markets.insert(condition_id) {
                        container.push(m);
                    }
                }

                offset += limit;

                if market_len < limit {
                    break;
                }
            }
        }

        if container.is_empty() {
            tracing::info!("no markets found for backfill");
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
                    Some(protos::Platform::Polymarket),
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
            .context("qdrant multiple_search_and_insert failed during polymarket backfill")?;

        for ebo in ebos {
            if let Err(e) = self.inner.grpc_tx.send(ebo).await {
                tracing::error!(
                    "polymarket backfill: failed to forward discovery ebo to grpc: {e}"
                );
            }
        }

        Ok(())
    }

    pub async fn initial_backfill_polymarket_history(
        &self,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let last_insert = self
            .inner
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

        self.backfill_funcs(shutdown, duration).await
    }

    async fn backfill_funcs(
        &self,
        shutdown: CancellationToken,
        duration: time::Duration,
    ) -> anyhow::Result<()> {
        let (r1, r2) = tokio::join!(
            self.backfill_polymarket_sport_history(duration, shutdown.clone()),
            self.backfill_polymarket_politics_history(duration, shutdown.clone()),
        );

        let mut errors = Vec::new();
        for (name, res) in [("sport", r1), ("politics", r2)] {
            if let Err(e) = res {
                errors.push(e.context(format!("polymarket {name} backfill failed")));
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

        tracing::info!("polymarket backfill completed");
        Ok(())
    }
}

// impl MyPolymarketClient {
//     async fn connect_okbet_ws(&self) -> anyhow::Result<()> {
//         let url = "wss://okbet.trade/api/public/ws/markets";
//         let (ws_stream, _response) = connect_async(url)
//             .await
//             .context("Failed to connect to Okbet WebSocket")?;
//
//         let (write, read) = ws_stream.split();
//
//         *self.inner.okbet_tx.lock().await = Some(write);
//         *self.inner.okbet_rx.lock().await = Some(read);
//         Ok(())
//     }
//
//     pub async fn send_to_okbet_wss(&self, payload: impl Into<String>) -> anyhow::Result<()> {
//         let mut guard = self.inner.okbet_tx.lock().await;
//
//         let sender = guard
//             .as_mut()
//             .ok_or_else(|| anyhow::anyhow!("okbet_tx not initialized"))?;
//
//         let msg = Message::Text(payload.into().into());
//         sender.send(msg).await.context("okbet_tx: cannot send")?;
//
//         Ok(())
//     }
//
//     async fn handle_okbet_messages(&self, msg: Message) -> anyhow::Result<()> {
//         match msg {
//             Message::Ping(data) => {
//                 let mut guard = self.inner.okbet_tx.lock().await;
//
//                 let sender = guard
//                     .as_mut()
//                     .ok_or_else(|| anyhow::anyhow!("okbet_tx not initialized"))?;
//
//                 let msg = Message::Pong(data);
//                 sender.send(msg).await.context("okbet_tx: cannot send")?;
//
//                 return Ok(());
//             }
//
//             Message::Pong(_) => {
//                 tracing::info!("okbet_rx_stream: got a Pong");
//             }
//
//             Message::Close(_) => {
//                 tracing::info!("okbet_rx_stream: got a close")
//             }
//
//             Message::Text(text) => {
//                 tracing::info!("okbet_rx_stream: Received: {}", text);
//                 match serde_json::from_str::<ok_bet_types::WsMessage>(&text) {
//                     Ok(msg) => match msg {
//                         ok_bet_types::WsMessage::Ping => {
//                             let pong = r#"{"type":"pong"}"#;
//                             if let Err(e) = self.send_to_okbet_wss(pong).await {
//                                 tracing::error!("okbet_rx_stream: error sending pong text: {e}");
//                             };
//                         }
//
//                         ok_bet_types::WsMessage::MarketPrice { data, .. } => {
//                             tracing::info!(
//                                 "PRICE => token: {}, price: {}, bid: {}, ask: {}",
//                                 data.token_id,
//                                 data.price,
//                                 data.best_bid,
//                                 data.best_ask
//                             );
//
//                             self.inner
//                                 .ws_msg_tx
//                                 .send(WsEventMessage::Polymarket(data.try_into().context(
//                                     "okbet_rx_stream: error converting okbet msg into polymark msg",
//                                 )?))
//                                 .await?
//                         }
//
//                         ok_bet_types::WsMessage::Other(_) => {}
//                     },
//                     Err(e) => {
//                         tracing::error!("okbet_rx_stream: Failed to parse message: {e}");
//                     }
//                 }
//             }
//
//             others => {
//                 tracing::info!("okbet_rx_stream: Received others: {}", others);
//             }
//         }
//
//         Ok(())
//     }
// }

pub fn get_signer() -> anyhow::Result<String> {
    let data = fs::read_to_string("privateKey.hex").context("error getting private_key")?;
    Ok(data)
}
