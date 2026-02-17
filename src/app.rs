use crate::arb;
use crate::grpc;
use crate::kalshi;
use crate::models::{self, protos};
use crate::polymarket;
use crate::vector_store;
use anyhow::{Context, Ok, Result};
use kalshi_rs::auth::auth_loader;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{codec::CompressionEncoding, transport::Server};

pub async fn app_startup(shutdown: CancellationToken) -> Result<OponIfa> {
    let (tx, rx) = mpsc::channel::<models::Todos>(1_000);

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
        picker.run_picker(picker_shutdown).await
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
        polymarket_client
            .run_polymarket(polymarket_shutdown)
            .await
            .map_err(anyhow::Error::from)
    });

    let kalshi_shutdown = shutdown.clone();
    let kalshi_handle = tokio::spawn(async move {
        tracing::debug!("kalshi thread spawn");
        kalshi_client
            .run_kalshi_ws(kalshi_shutdown)
            .await
            .map_err(anyhow::Error::from)
    });

    // grpc server
    let (bot_tx, bot_rx) = mpsc::channel::<protos::Ebo>(1024);
    let grpc_service = grpc::GrpcServer::new(bot_tx.clone());
    let server_shared = Arc::new(grpc_service);
    let grpc_server_ref = server_shared.clone();

    let addr = "0.0.0.0:50051".parse()?;
    println!("🚀 gRPC Server listening on {}", addr);

    let esu_service = protos::esu_odara_server::EsuOdaraServer::from_arc(server_shared);

    let esu_service = esu_service
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);

    let grpc_shutdown = shutdown.clone();
    let bot_rx_shutdown = shutdown.clone();

    tokio::spawn(async move {
        grpc_server_ref
            .handle_bot_flow(bot_rx_shutdown, bot_rx)
            .await
    });

    let grpc_handle = tokio::spawn(async move {
        tracing::debug!("grpc thread spawn");

        Server::builder()
            .add_service(esu_service)
            .serve_with_shutdown(addr, async move {
                grpc_shutdown.cancelled().await;
                tracing::trace!("grpc received shutdown");
            })
            .await
            .map_err(anyhow::Error::from)
    });

    tracing::debug!("would be going live mode now");

    Ok(OponIfa {
        kalshi_handle,
        polymarket_handle,
        picker_handle,
        grpc_handle,
    })
}

pub struct OponIfa {
    pub kalshi_handle: JoinHandle<Result<()>>,
    pub polymarket_handle: JoinHandle<Result<()>>,
    pub picker_handle: JoinHandle<()>,
    pub grpc_handle: JoinHandle<Result<()>>,
}
