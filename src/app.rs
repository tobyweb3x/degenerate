use anyhow::{Context, Ok, Result};
use kalshi_rs::auth::auth_loader;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::arb;
use crate::kalshi;
use crate::models::Todos;
use crate::polymarket;
use crate::vector_store;

pub async fn app_startup(
    shutdown: CancellationToken,
) -> Result<(JoinHandle<()>, JoinHandle<()>, JoinHandle<()>)> {
    let (tx, mut rx) = mpsc::channel::<Todos>(1_000);

    // vector store
    let vector_store =
        vector_store::VectorStore::new_metal("http://localhost:6334", "arb_hit", tx.clone())
            .await
            .context("error setting up qdrant")?;

    vector_store.disable_hnsw().await?;
    vector_store.setup_qdrant_payload_index().await?;

    let kalshi_account =
        auth_loader::load_auth_from_file().context("error loading kalshi auth file")?;
    let kalshi_account_clone = kalshi_account.clone();

    // picker
    let picker_shutdown = shutdown.clone();
    let picker_handle = tokio::spawn(async move {
        tracing::debug!("picker thread spawn");
        let mut picker = arb::Picker::new(kalshi_account_clone, rx);
        if let Err(e) = picker.run_picker(picker_shutdown).await {
            tracing::error!("picker lifecycle failed: {e:?}");
        }
    });

    // kalshi
    let kalshi_vs = vector_store.clone();
    let kalshi_client = kalshi::MyKalshiClient::new(kalshi_account, kalshi_vs, tx.clone());
    kalshi_client.test_ws_connect().await?;
    tracing::debug!("MyKalshiClient.backfill_kalshi_history started");
    kalshi_client
        .backfill_kalshi_history()
        .await
        .context("kalshi backfill failed")?;

    // enable index
    vector_store.enable_hnsw(32).await?;

    // polymarket
    let polymarket_vs = vector_store.clone();
    let mut polymarket_client = polymarket::MyPolymarketClient::new(polymarket_vs, tx);
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

    Ok((kalshi_handle, polymarket_handle, picker_handle))
}
