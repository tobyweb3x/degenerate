use crate::models::{self, protos};
use crate::platforms;
use anyhow::{self, Context};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
// use tracing;

pub struct Picker {
    platforms: platforms::Platfroms,
    rx: mpsc::Receiver<protos::Ebo>,
    tx: mpsc::Sender<protos::Ebo>,
}

impl Picker {
    pub fn new(
        platforms: platforms::Platfroms,
        rx: mpsc::Receiver<protos::Ebo>,
        tx: mpsc::Sender<protos::Ebo>,
    ) -> Self {
        Self { platforms, rx, tx }
    }

    pub async fn run_picker(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    break;
                }

                msg = self.rx.recv() => {
                    let Some(ebo) = msg else {
                        tracing::info!("got a None instead of todo, picker closed");
                        break;
                    };

                    let Some(action) = ebo.action else {
                        tracing::warn!("got a None-action");
                        continue;
                    };

                    match action {
                        protos::ebo::Action::CrossPlatformArbDiscovery(list) => {
                            self.resolve_discovered_list(list).await
                        }

                        others => { println!("got other grpc messages {others:#?}")}
                    }

                }
            }
        }

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

            let arb =
                match tokio::try_join!(self.get_arb_market(anchor), self.get_arb_market(r#match)) {
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
                .send(protos::Ebo {
                    correlation_id: models::generate_uuid_v5(format!(
                        "{}:{}",
                        &arb.anchor.as_ref().unwrap().token_id,
                        &arb.r#match.as_ref().unwrap().token_id
                    )),
                    found_at: chrono::Utc::now().timestamp_millis(),
                    action: Some(protos::ebo::Action::CrossPlatformArb(arb)),
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
        let (new_market, token_id) = match platform {
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
}
