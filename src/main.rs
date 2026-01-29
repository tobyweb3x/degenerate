use anyhow::{Ok, Result};
use qdrant_client::qdrant::FieldType;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
mod constants;
mod kalshi;
mod models;
mod polymarket;
mod qdrant;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    dotenvy::dotenv().ok();

    // let account = auth_loader::load_auth_from_file()?;
    // let kalshi_http = KalshiClient::new(account.clone());
    // if let Err(e) = poll_kalshi_for_historical_data(&kalshi_http).await {
    //     tracing::error!("{e}");
    // };

    // if let Err(e) = polymarket::poll_polymarket_historical_data(&polymarket_gamma_api).await {
    //     tracing::error!("{e}");
    // };

    let mut sig_term = signal(SignalKind::terminate()).expect("error creating sig_term handler");
    let mut sig_int = signal(SignalKind::interrupt()).expect("error creating sig_int handler");

    let vector_store = qdrant::VectorStore::new_metal("http://localhost:6333", "arb_hit").await?;
    vector_store.disable_hnsw().await?;

    // Use the constants instead of raw strings
    vector_store
        .index_payload(constants::FIELD_UUID, FieldType::Uuid)
        .await?;
    vector_store
        .index_payload(constants::FIELD_MARKET_CATEGORY, FieldType::Keyword)
        .await?;
    vector_store
        .index_payload(constants::FIELD_PLATFORM, FieldType::Keyword)
        .await?;
    vector_store
        .index_payload(constants::FIELD_MARKET_SUBCATEGORY, FieldType::Keyword)
        .await?;
    vector_store
        .index_payload(constants::FIELD_END_DATE, FieldType::Datetime)
        .await?;

    let shutdown = CancellationToken::new();

    let polymarket_vs = vector_store.clone();
    let polymarket_shutdown = shutdown.clone();
    let polymarket_handle = tokio::spawn(async move {
        let mut polymarket_client = polymarket::PolymarketClient::new(polymarket_vs);
        if let Err(e) = polymarket_client.run_polymarket(polymarket_shutdown).await {
            tracing::error!("polymarket task exited: {e:?}");
        }
    });

    // let kalshi_vs = vector_store.clone();
    // let kalshi_shutdown = shutdown.clone();
    // let kalshi_handle = tokio::spawn(async move {
    //     if let Err(e) = kalshi::run_kalshi_ws(kalshi_shutdown).await {
    //         tracing::error!("Kalshi lifecycle task exited: {e:?}");
    //     }
    // });

    vector_store.enable_hnsw(32).await?;

    tokio::select! {
        _ = sig_term.recv() => {
            tracing::info!("received SIGTERM");
        }

        _ = sig_int.recv() => {
            tracing::info!("received SIGINT");
        }
    }

    shutdown.cancel();

    tokio::select! {
        _ = polymarket_handle => tracing::info!("polymarket finished clean"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => tracing::warn!("shutdown timed out, forcing exit"),
    }
    println!("i hope arb was 👍🏿💋");

    Ok(())
}
