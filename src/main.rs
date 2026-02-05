use anyhow::{Context, Ok, Result};
use kalshi_rs::auth::auth_loader;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
mod constants;
mod kalshi;
mod models;
mod polymarket;
mod qdrant;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // rustls::crypto::CryptoProvider::install_default().unwrap();

    tracing_subscriber::fmt::init();
    dotenvy::dotenv().ok();

    let shutdown = CancellationToken::new();

    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        let mut sig_term = signal(SignalKind::terminate()).expect("failed to register SIGTERM");
        let mut sig_int = signal(SignalKind::interrupt()).expect("failed to register SIGINT");

        tokio::select! {
            _ = sig_term.recv() => tracing::info!("Received SIGTERM"),
            _ = sig_int.recv() => tracing::info!("Received SIGINT"),
        }

        signal_token.cancel();
    });

    let app_startup = async {
        let vector_store = qdrant::VectorStore::new_metal("http://localhost:6334", "arb_hit")
            .await
            .context("error setting up qdrant")?;

        vector_store.disable_hnsw().await?;
        vector_store.setup_qdrant_payload_index().await?;

        let account =
            auth_loader::load_auth_from_file().context("error loading kalshi auth file")?;
        let kalshi_vs = vector_store.clone();
        let kalshi_client = kalshi::MyKalshiClient::new(account, kalshi_vs);
        kalshi_client.test_ws_connect().await?;
        tracing::debug!("MyKalshiClient.backfill_kalshi_history started");
        kalshi_client
            .backfill_kalshi_history()
            .await
            .context("kalshi backfill failed")?;

        vector_store.enable_hnsw(32).await?;

        let polymarket_vs = vector_store.clone();
        let mut polymarket_client = polymarket::MyPolymarketClient::new(polymarket_vs);
        tracing::debug!("MyPolymarketClient.backfill_polymarket_history started");
        polymarket_client
            .backfill_polymarket_history()
            .await
            .context("polymarket backfill failed")?;

        let polymarket_shutdown = shutdown.clone();
        let polymarket_handle = tokio::spawn(async move {
            tracing::debug!("polymarket thread spawn");
            if let Err(e) = polymarket_client.run_polymarket(polymarket_shutdown).await {
                tracing::error!("polymarket lifecycle failed: {e:?}");
            }
        });

        let kalshi_shutdown = shutdown.clone();
        let kalshi_handle = tokio::spawn(async move {
            tracing::debug!("kalshi thread spawn");
            if let Err(e) = kalshi_client.run_kalshi_ws(kalshi_shutdown).await {
                tracing::error!("Kalshi lifecycle task exited: {e:?}");
            }
        });

        tracing::debug!("would be going live mode now");
        Ok((kalshi_handle, polymarket_handle))
    };

    let handles = tokio::select! {
        res = app_startup => res?,

        _ = shutdown.cancelled() => {
            tracing::warn!("Shutdown signal received during initialization. Exiting...");
            return Ok(());
        }
    };

    let (kalshi_handle, polymarket_handle) = handles;
    tokio::select! {
        (poly_res, kalshi_res) = async {
            tokio::join!(polymarket_handle, kalshi_handle)
        } => {
            if let Err(e) = poly_res { tracing::error!("polymarket task panicked: {e:?}"); }
            if let Err(e) = kalshi_res { tracing::error!("kalshi task panicked: {e:?}"); }
            tracing::info!("shutdown complete");
        }

        _ = async {
            shutdown.cancelled().await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await; // just wait for cleanup (if ever)
        } => {
            tracing::warn!("shutdown timed out, forcing exit");
        }
    }

    println!("i hope arb was 👍🏿 and 💋");
    Ok(())
}
