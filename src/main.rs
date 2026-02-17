use anyhow::{Ok, Result};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber;

mod app;
mod arb;
mod constants;
mod grpc;
mod kalshi;
mod models;
mod polymarket;
mod vector_store;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

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

    let app_runtime = app::app_startup(shutdown.clone()).await?;

    tokio::select! {
        biased;
        (poly_res, kalshi_res, picker_res, grpc_res) = async {
            tokio::join!(app_runtime.polymarket_handle, app_runtime.kalshi_handle, app_runtime.picker_handle,app_runtime.grpc_handle)
        } => {
            if let Err(e) = poly_res { tracing::error!("polymarket task panicked: {e:?}"); }
            if let Err(e) = kalshi_res { tracing::error!("kalshi task panicked: {e:?}"); }
            if let Err(e) = picker_res { tracing::error!("picker task panicked: {e:?}"); }
            if let Err(e) = grpc_res { tracing::error!("grpc task panicked: {e:?}"); }
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
