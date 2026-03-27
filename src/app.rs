use crate::grpc;
use crate::picker;
use crate::platforms;
use crate::vector_store;
use anyhow::{Context, Ok, Result};
use kalshi_rs::auth::auth_loader;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub async fn app_startup(shutdown: CancellationToken) -> Result<OponIfa> {
    let kalshi_account =
        auth_loader::load_auth_from_file().context("error loading kalshi auth file")?;

    let (bot_to_grpc_tx, bot_to_grpc_rx) = mpsc::channel(1_024);
    let (grpc_to_bot_tx, grpc_to_bot_rx) = mpsc::channel(1_024);

    // vector store
    let vector_store = vector_store::VectorStore::new_metal(
        "http://localhost:6334",
        vector_store::COLLECTION_NAME,
        bot_to_grpc_tx.clone(),
    )
    .await
    .context("error setting up qdrant")?;

    // vector_store.disable_hnsw().await?;
    vector_store.setup_qdrant_payload_index().await?;
    vector_store.enable_hnsw(32).await?;

    let kalshi_vs = vector_store.clone();
    let polymarket_vs = vector_store.clone();

    let (ws_tx, ws_rx) = mpsc::channel::<platforms::WsEventMessage>(10_000);

    let kalshi_client =
        platforms::kalshi::MyKalshiClient::new(kalshi_account, kalshi_vs, ws_tx.clone());
    kalshi_client.test_ws_connect().await?;

    let mut polymarket_client =
        platforms::polymarket::MyPolymarketClient::new(polymarket_vs, ws_tx.clone());

    let platform =
        platforms::Platfroms::new(kalshi_client.clone(), polymarket_client.clone()).clone();
    let platform_for_picker_comms = platform.clone();
    let (execution_tx, execution_rx) = mpsc::channel(512);

    // picker exec
    let picker_comms_shutdown = shutdown.clone();
    let clone_bot_to_grpc_tx = bot_to_grpc_tx.clone();
    let picker_comms_handle = tokio::spawn(async move {
        let mut picker = picker::comms::PickerComms::new(
            platform_for_picker_comms,
            grpc_to_bot_rx,
            clone_bot_to_grpc_tx,
            ws_rx,
            execution_tx,
        );
        picker.run_picker_comms(picker_comms_shutdown).await
    });

    // picker exec
    let picker_exce_shutdown = shutdown.clone();
    let picker_exec_handle = tokio::spawn(async move {
        let mut picker = picker::exec::PickerExec::new(platform, execution_rx);
        picker.run_picker_exe(picker_exce_shutdown).await
    });

    // grpc client
    let clone_grpc_to_bot_tx = grpc_to_bot_tx.clone();
    let grpc_shutdown = shutdown.clone();
    let grpc_handle = tokio::spawn(async move {
        grpc::run_grpc_client(grpc_shutdown, bot_to_grpc_rx, clone_grpc_to_bot_tx)
            .await
            .map_err(anyhow::Error::from)
    });

    // // kalshi backfill
    let kalshi_backfill_shutdown = shutdown.clone();
    let cloned_kalshi_client = kalshi_client.clone();
    tokio::spawn(async move {
        let _ = cloned_kalshi_client
            .backfill_kalshi_history(kalshi_backfill_shutdown)
            .await
            .inspect_err(|e| tracing::error!("kalshi backfill failed: {e}"));
    });

    // polymarket bacfill
    let polymarket_backfill_shutdown = shutdown.clone();
    let cloned_polymarket_client = polymarket_client.clone();
    tokio::spawn(async move {
        let _ = cloned_polymarket_client
            .backfill_polymarket_history(polymarket_backfill_shutdown.clone())
            .await
            .inspect_err(|e| tracing::error!("polymarket backfill failed: {e}"));
    });

    let polymarket_shutdown = shutdown.clone();
    let polymarket_handle = tokio::spawn(async move {
        polymarket_client
            .run_polymarket(polymarket_shutdown)
            .await
            .map_err(anyhow::Error::from)
    });

    let kalshi_shutdown = shutdown.clone();
    let kalshi_handle = tokio::spawn(async move {
        kalshi_client
            .run_kalshi(kalshi_shutdown)
            .await
            .map_err(anyhow::Error::from)
    });

    tracing::info!("app startup done");
    Ok(OponIfa {
        kalshi_handle,
        polymarket_handle,
        picker_comms_handle,
        picker_exec_handle,
        grpc_handle,
    })
}

pub struct OponIfa {
    pub kalshi_handle: JoinHandle<Result<()>>,
    pub polymarket_handle: JoinHandle<Result<()>>,
    pub picker_comms_handle: JoinHandle<Result<()>>,
    pub picker_exec_handle: JoinHandle<Result<()>>,
    pub grpc_handle: JoinHandle<Result<()>>,
}
