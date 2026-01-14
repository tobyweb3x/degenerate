mod constants;
use alloy::{hex, signers::local::PrivateKeySigner};
use anyhow::{self, Ok};
use futures_util::future::ok;
use kalshi_rs::{KalshiWebsocketClient, auth::auth_loader, websocket::models::KalshiSocketMessage};
mod kalshi;
use polymarket_hft::client::polymarket;
use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
const PRIVATE_KEY_FILE: &str = "privateKey.hex";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    dotenvy::dotenv().ok();

    if let Err(e) = run().await {
        eprintln!("Error: {:#?}", e);
        std::process::exit(1)
    };

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();

    let kalshi_shutdown = shutdown.clone();
    let _kalshi_handle = tokio::spawn(async move {
        if let Err(e) = run_kalshi_ws(kalshi_shutdown).await {
            tracing::error!("Kalshi lifecycle task exited: {e:?}");
        }
    });

    let polymarket_shutdown = shutdown.clone();
    let _polymarket_handle = tokio::spawn(async move {
        if let Err(e) = run_polymarket_ws(polymarket_shutdown).await {
            tracing::error!("Polymarket WS task exited: {e:?}");
        }
    });

    tokio::signal::ctrl_c().await?;
    println!("received SIGINT, shutting down");
    shutdown.cancel();

    println!("all done");
    Ok(())
}

async fn run_kalshi_ws(shutdown: CancellationToken) -> anyhow::Result<()> {
    let account = auth_loader::load_auth_from_file()?;

    let kalshi = KalshiWebsocketClient::new(account.clone());
    kalshi.connect().await?;

    kalshi
        .subscribe(vec!["market_lifecycle_v2"], vec![])
        .await?;

    loop {
        tokio::select! {
        _ = shutdown.cancelled() => {
            println!("Kalshi WS shutting down");
            break;
        }

        result = kalshi.next_message() => {
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
                if let Err(e) = kalshi.send_pong().await {
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
                   if let Err(e) = handle_kalshi_message(msg).await {
                        tracing::error!("message handler error: {e:?}");
                    }
                },
            }
        }
        }
    }

    Ok(())
}

async fn run_polymarket_ws(shutdown: CancellationToken) -> anyhow::Result<()> {
    let signer = get_signer()?;
    println!("address is - {}", signer.address());

    let mut websocket_client = polymarket::clob::ws::ClobWsClient::new();
    websocket_client.subscribe_market(vec![], true).await?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                println!("Polymarket WS shutting down");
                break;
            }

            msg = websocket_client.next_message() => {
                match msg {
                    Some(msg) => {
                        match &msg {
                            polymarket::clob::ws::WsMessage::NewMarket(msg) => {
                                let secs = minutes_ago_from_ms(&msg.timestamp)
                                    .map(|m| m.to_string())
                                    .unwrap_or_else(|| "?".into());

                                println!(
                                    "New market: {:#?} — {}secs ago\n",
                                    msg, secs
                                );
                            }

                            other => {
                                println!("Other Polymarket message: {:#?}\n", other);
                            }
                        }
                    }

                    None => {
                        println!("Polymarket WS connection ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn minutes_ago_from_ms(timestamp_ms: &str) -> Option<u128> {
    let msg_ms = timestamp_ms.parse::<u128>().ok()?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();

    let diff_ms = now_ms.saturating_sub(msg_ms);
    Some((diff_ms) / 1000)
}

fn write_new_wallet() -> anyhow::Result<PrivateKeySigner> {
    let signer = PrivateKeySigner::random();
    let hex_encodeded_private_key = hex::encode(signer.to_bytes());

    println!("New wallet created");
    println!("Address: {}", signer.address());

    fs::write(Path::new(PRIVATE_KEY_FILE), hex_encodeded_private_key)?;
    Ok(signer)
}

fn load_wallet() -> anyhow::Result<PrivateKeySigner> {
    let data = fs::read_to_string(PRIVATE_KEY_FILE)?;
    let signer = data.trim().parse::<PrivateKeySigner>()?;

    Ok(signer)
}

fn get_signer() -> anyhow::Result<PrivateKeySigner> {
    let signer = if Path::new(PRIVATE_KEY_FILE).exists() {
        load_wallet()?
    } else {
        write_new_wallet()?
    };

    Ok(signer)
}

async fn handle_kalshi_message(msg: KalshiSocketMessage) -> anyhow::Result<(), anyhow::Error> {
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
            println!("MarketLifecycleV2: {:#?}", event);
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

        KalshiSocketMessage::Unhandled(value) => {
            tracing::warn!("Unhandled WS payload:\n{:#?}", value);
        }

        others => {
            println!("others: {:#?}", others);
        }
    }

    Ok(())
}
