use std::time::Duration;

use super::ws::ManageWS;
use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

pub async fn wss_market_lifecycle(shutdown: CancellationToken) -> Result<()> {
    let base_url = "wss://api.elections.kalshi.com/market_lifecycle_v2";
    let market_and_event_lifecycle = ManageWS::new(base_url, Duration::from_secs(10), true);

    market_and_event_lifecycle.connect().await?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                println!("Kalshi WS shutting down");
                break;
            }

            msg = market_and_event_lifecycle.next_message() => {
                match msg {
                    Ok(msg) => {
                        println!("\n\n\nKalshi market lifecycle: {:#?}\n\n", msg);
                    }
                    Err(e) => {
                        tracing::error!("Kalshi WS error: {e:?}");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
