use crate::qdrant;
use alloy::{hex, signers::local::PrivateKeySigner};
use anyhow::{Ok, Result, bail, ensure};
use chrono::{Duration as ChronoDuration, Utc};
use kalshi_rs::{
    KalshiClient, KalshiWebsocketClient, auth::auth_loader, markets::models::MarketsQuery,
    websocket::models::KalshiSocketMessage,
};
use std::fs as std_fs;
use std::io::Write;
use tokio::fs as tokio_fs;
use tokio::io::{AsyncWriteExt, BufWriter as tokio_bufWriter};
use tokio_util::sync::CancellationToken;

pub async fn run_kalshi_ws(shutdown: CancellationToken) -> Result<()> {
    let account = auth_loader::load_auth_from_file()?;

    let kalshi_http = KalshiClient::new(account.clone());
    let kalshi_ws = KalshiWebsocketClient::new(account.clone());
    kalshi_ws.connect().await?;

    kalshi_ws
        .subscribe(vec!["market_lifecycle_v2"], vec![])
        .await?;

    loop {
        tokio::select! {
        _ = shutdown.cancelled() => {
            println!("Kalshi WS shutting down");
            // kalshi_ws.disconnect();
            break;
        }

        result = kalshi_ws.next_message() => {
            let val = match result {
                std::result::Result::Ok(v) => v,
                Err(e) => {
                    tracing::error!("websocket error: {e:?}");
                    tracing::debug!("closing ws connection");
                    break;
                },
            };

            match val {
                KalshiSocketMessage::Ping => {
                if let Err(e) = kalshi_ws.send_pong().await {
                    tracing::error!("error sending pong: {e}");
                };
                },

                KalshiSocketMessage::Pong => {
                    tracing::trace!("Pong received");
                },

                KalshiSocketMessage::Close(frame) => {
                    tracing::info!("WebSocket closed by server: {:?}", frame);
                    break;
                },

                KalshiSocketMessage::Binary(binary) => {
                  tracing::debug!( "Received binary WebSocket message ({} bytes)", binary.len());
              },

                KalshiSocketMessage::Frame(frame) => {
                  tracing::trace!("Received raw WebSocket frame: {:?}", frame);
              },

                msg => {
                   if let Err(e) = handle_kalshi_message(msg, &kalshi_http).await {
                        tracing::error!("error from fn handle_kalshi_message: {e:?}");
                    }
                },
            }
        }
        }
    }

    Ok(())
}

async fn handle_kalshi_message(msg: KalshiSocketMessage, kalshi_http: &KalshiClient) -> Result<()> {
    match msg {
        KalshiSocketMessage::SubscribedResponse(res) => {
            println!("Subscribed: {:#?}", res);
        }

        KalshiSocketMessage::UnsubscribedResponse(res) => {
            println!("Unsubscribed: {:#?}", res);
        }

        KalshiSocketMessage::OkResponse(res) => {
            println!("OK response: {:#?}", res);
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

        KalshiSocketMessage::MarketLifecycleV2(event) => {
            if matches!(
                event.msg.event_type.as_str(),
                "settled" | "determined" | "close_date_updated" | "deactivated"
            ) {
                return Ok(());
            }

            let result = kalshi_http.get_market(&event.msg.market_ticker).await?;
            let json = serde_json::to_string_pretty(&result)?;
            let mut file = std_fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("kalshiNewEvent.ndjson")?;
            writeln!(file, "{json}")?;
        }

        KalshiSocketMessage::OrderbookSnapshot(snapshot) => {
            println!("OrderbookSnapshot: {:#?}", snapshot);
        }

        KalshiSocketMessage::OrderbookDelta(delta) => {
            println!("OrderbookDelta: {:#?}", delta);
        }

        KalshiSocketMessage::TradeUpdate(trade) => {
            println!("Trade update: {:#?}", trade);
        }

        KalshiSocketMessage::TickerUpdate(ticker) => {
            println!("Ticker update: {:#?}", ticker);
        }

        KalshiSocketMessage::UserFill(fill) => {
            println!("User fill: {:#?}", fill);
        }

        KalshiSocketMessage::MarketPosition(pos) => {
            println!("Market position update: {:#?}", pos);
        }

        KalshiSocketMessage::EventLifecycle(event) => {
            println!("EventLifecycle: {:#?}", event);
        }

        KalshiSocketMessage::Unhandled(value) => {
            tracing::warn!("Unhandled WS payload:\n{:#?}", value);
        }

        others => {
            println!("others: {:#?}", others);
        }
    }

    Ok(())
}

async fn poll_kalshi_for_historical_data(kalshi_http: &KalshiClient) -> Result<()> {
    let needed_time = (Utc::now() - ChronoDuration::hours(24)).timestamp();
    let mut cursor: Option<String> = None;
    let file = tokio_fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("kalshiNewEvent2.ndjson")
        .await?;

    let mut writer = tokio_bufWriter::with_capacity(64 * 1024, file);
    let (mut count, mut num): (u64, u64) = (0, 0);
    println!("number of count/num, {}/{}", count, num);

    loop {
        let result = kalshi_http
            .get_all_markets(&MarketsQuery {
                limit: Some(100),
                status: Some("open".to_string()),
                min_created_ts: Some(needed_time),
                cursor: cursor.clone(),
                ..Default::default()
            })
            .await?;
        // let Result =  kalshi_http.get_all_events(&)

        let market_len = result.markets.len() as u64;
        println!("len of market, {:?}", market_len);
        for market in result.markets {
            let json = serde_json::to_string(&market)?;
            writer.write_all(json.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }

        count += 1;
        num += market_len;
        println!("number of count/num, {}/{}", count, num);

        match result.cursor {
            Some(next) if !next.is_empty() => cursor = Some(next),
            _ => break,
        }
    }

    writer.flush().await?;
    Ok(())
}
