use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber;
use tracing_subscriber::{EnvFilter, fmt};

mod app;
mod grpc;
mod models;
mod picker;
mod platforms;
mod vector_store;
mod execution;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // rustls::crypto::CryptoProvider::install_default().unwrap();

    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
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
        res = async {
            tokio::try_join!(
                flatten(app_runtime.polymarket_handle),
                flatten(app_runtime.kalshi_handle),
                flatten(app_runtime.grpc_handle),
                flatten(app_runtime.picker_handle)
            )
        } => {
            match res {
                Ok(_) => tracing::info!("all tasks finished successfully"),
                Err(e) => {
                    tracing::error!("application crashed, a task failed: {:?}", e);
                    shutdown.cancel();
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await; // just wait for cleanup (if ever)
                }
            }
        }

        _ = async {
            shutdown.cancelled().await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await; // just wait for cleanup (if ever)
        } => {
            tracing::warn!("shutdown timed out, forcing exit");
        }
    }

    tracing::info!("i hope arb was 👍🏿 and 💋");
    Ok(())
}

async fn flatten(handle: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    match handle.await {
        Ok(inner_result) => inner_result,
        Err(join_err) => Err(anyhow::anyhow!("Task Panicked/Cancelled: {}", join_err)),
    }
}
