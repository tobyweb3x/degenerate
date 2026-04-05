use crate::grpc;
use crate::picker;
use crate::platforms;
use crate::vector_store;
use anyhow::{Context, Ok, Result};
use kalshi_rs::auth::auth_loader;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use alloy::signers::Signer as _;
use alloy_signer_local::PrivateKeySigner;
use polymarket_client_sdk::{POLYGON, clob};
use std::str::FromStr;

pub async fn app_startup(shutdown: CancellationToken) -> Result<OponIfa> {
    let kalshi_account =
        auth_loader::load_auth_from_file().context("error loading kalshi auth file")?;

    let (bot_to_grpc_tx, bot_to_grpc_rx) = mpsc::channel(1_024);
    let (grpc_to_bot_tx, grpc_to_bot_rx) = mpsc::channel(1_024);
    let (ws_tx, ws_rx) = mpsc::channel::<platforms::WsEventMessage>(1_024); // takes platform wss messages from platform to picker-comms
    let (execution_tx, execution_rx) = mpsc::channel(512); // takes messages from picker-comms to picker-exec

    // vector store
    let (vector_store, vector_store_cleanup_handle) = {
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

        let vector_store_cleanup_handle = tokio::spawn({
            let clone_vector_store_shutdown = shutdown.clone();
            let vector_store_clone = vector_store.clone();

            async move {
                vector_store_clone
                    .run_delete_old_points(clone_vector_store_shutdown)
                    .await
            }
        });

        (vector_store, vector_store_cleanup_handle)
    };

    // platforms
    let (mut polymarket_client, kalshi_client) = {
        let kalshi_vs = vector_store.clone();
        let polymarket_vs = vector_store.clone();

        let kalshi_client =
            platforms::kalshi::MyKalshiClient::new(kalshi_account, kalshi_vs, ws_tx.clone());
        kalshi_client.test_ws_connect().await?;

        let polymarket_client =
            platforms::polymarket::MyPolymarketClient::new(polymarket_vs, ws_tx.clone());

        (polymarket_client, kalshi_client)
    };

    // picker-comms
    let picker_comms_handle = tokio::spawn({
        let picker_comms_shutdown = shutdown.clone();
        let clone_bot_to_grpc_tx = bot_to_grpc_tx.clone();
        let platform =
            platforms::Platfroms::new(kalshi_client.clone(), polymarket_client.clone()).clone();
        async move {
            let mut picker = picker::comms::PickerComms::new(
                platform,
                grpc_to_bot_rx,
                clone_bot_to_grpc_tx,
                ws_rx,
                execution_tx,
            );
            picker.run_picker_comms(picker_comms_shutdown).await
        }
    });

    // picker-exec
    let picker_exec_handle = tokio::spawn({
        let picker_exce_shutdown = shutdown.clone();
        let platform =
            platforms::Platfroms::new(kalshi_client.clone(), polymarket_client.clone()).clone();

        let private_key = platforms::polymarket::get_signer()?;
        let signer = PrivateKeySigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));

        let polymarket_clob_client =
            clob::Client::new("https://clob.polymarket.com", clob::Config::default())?
                .authentication_builder(&signer)
                .authenticate()
                .await?;

        async move {
            let mut picker = picker::exec::PickerExec::new(
                platform,
                execution_rx,
                polymarket_clob_client,
                signer,
            );
            picker.run_picker_exe(picker_exce_shutdown).await
        }
    });

    // grpc client
    let grpc_handle = tokio::spawn({
        let clone_grpc_to_bot_tx = grpc_to_bot_tx.clone();
        let grpc_shutdown = shutdown.clone();
        async move { grpc::run_grpc_client(grpc_shutdown, bot_to_grpc_rx, clone_grpc_to_bot_tx).await }
    });

    // kalshi backfill
    tokio::spawn({
        let kalshi_backfill_shutdown = shutdown.clone();
        let cloned_kalshi_client = kalshi_client.clone();
        async move {
            let _ = cloned_kalshi_client
                .backfill_kalshi_history(kalshi_backfill_shutdown)
                .await
                .inspect_err(|e| tracing::error!("kalshi backfill failed: {e}"));
        }
    });

    // polymarket bacfill
    tokio::spawn({
        let polymarket_backfill_shutdown = shutdown.clone();
        let cloned_polymarket_client = polymarket_client.clone();
        async move {
            let _ = cloned_polymarket_client
                .backfill_polymarket_history(polymarket_backfill_shutdown.clone())
                .await
                .inspect_err(|e| tracing::error!("polymarket backfill failed: {e}"));
        }
    });

    // polymarket new market poll & ws connection keep-alive
    let polymarket_handle = tokio::spawn({
        let polymarket_shutdown = shutdown.clone();
        async move { polymarket_client.run_polymarket(polymarket_shutdown).await }
    });

    // kalshi new market poll & ws connection keep-alive
    let kalshi_handle = tokio::spawn({
        let kalshi_shutdown = shutdown.clone();
        async move { kalshi_client.run_kalshi(kalshi_shutdown).await }
    });

    tracing::info!("app startup done");
    Ok(OponIfa {
        kalshi_handle,
        polymarket_handle,
        picker_comms_handle,
        picker_exec_handle,
        grpc_handle,
        vector_store_cleanup_handle,
    })
}

pub struct OponIfa {
    pub kalshi_handle: JoinHandle<Result<()>>,
    pub polymarket_handle: JoinHandle<Result<()>>,
    pub picker_comms_handle: JoinHandle<Result<()>>,
    pub picker_exec_handle: JoinHandle<Result<()>>,
    pub grpc_handle: JoinHandle<Result<()>>,
    pub vector_store_cleanup_handle: JoinHandle<Result<()>>,
}
