use crate::models::protos::{self, RunningArbsRequest, client_ebo};
use anyhow::Context;
use std::time::{self, Duration};
use tokio::{sync::mpsc, time as tokio_time};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

const GRPC_URL: &str = "http://127.0.0.1:50051";
const KEEPALIVE_TIME: time::Duration = time::Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: time::Duration = time::Duration::from_secs(2);
const MAX_RETRIES: usize = 30;
const RETRY_DELAY: time::Duration = time::Duration::from_secs(2);

pub async fn run_grpc_client(
    shutdown: CancellationToken,
    mut bot_outbound_rx: mpsc::Receiver<protos::ClientEbo>,
    bot_inbound_tx: mpsc::Sender<protos::ServerEbo>,
) -> anyhow::Result<()> {
    let mut retries = 0;

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        tracing::info!(
            "connecting to gRPC (Attempt {}/{})...",
            retries + 1,
            MAX_RETRIES
        );

        let session_start = time::Instant::now();

        let endpoint = Endpoint::from_static(GRPC_URL)
            .http2_keep_alive_interval(KEEPALIVE_TIME)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_timeout(time::Duration::from_secs(5));

        let session_result = connect_and_run_session(
            endpoint,
            shutdown.clone(),
            &mut bot_outbound_rx,
            &bot_inbound_tx,
        )
        .await;

        match session_result {
            Ok(_) => {
                tracing::info!("gRPC client stopped cleanly");
                return Ok(());
            }
            Err(e) => {
                tracing::error!("gRPC stream ended with error: {:?}", e);
            }
        }

        if session_start.elapsed() > time::Duration::from_secs(60) {
            tracing::info!("Connection was stable for >60s before dropping. Resetting retries.");
            retries = 0;
        } else {
            retries += 1;
        }

        if retries >= MAX_RETRIES {
            anyhow::bail!("max gRPC retries ({}) reached. Aborting.", MAX_RETRIES)
        }

        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio_time::sleep(RETRY_DELAY) => continue,
        }
    }
}

async fn connect_and_run_session(
    endpoint: Endpoint,
    shutdown: CancellationToken,
    bot_outbound_rx: &mut mpsc::Receiver<protos::ClientEbo>,
    bot_inbound_tx: &mpsc::Sender<protos::ServerEbo>,
) -> anyhow::Result<()> {
    let channel = endpoint
        .connect()
        .await
        .context("failed to connect to grpc server")?;
    let mut client = protos::esu_odara_client::EsuOdaraClient::new(channel);

    let (network_tx, network_rx) = mpsc::channel(128);
    let rx_stream = ReceiverStream::new(network_rx);
    let response = client
        .esu(rx_stream)
        .await
        .context("failed to open stream")?;

    tracing::info!("gRPC Stream established");
    let mut inbound_stream = response.into_inner();

    let clone_nerwork_tx = network_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        if let Err(e) = clone_nerwork_tx
            .send(protos::ClientEbo {
                action: Some(client_ebo::Action::GetRunningArbs(RunningArbsRequest {
                    arb_type: protos::ArbType::CrossPlatform.into(),
                })),
                ..Default::default()
            })
            .await
        {
            tracing::error!("failed to send GetRunningArbs request: {:?}", e);
        }
    });

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                return Ok(());
            }

            msg = bot_outbound_rx.recv() => { // from bot
                match msg {
                    Some(ebo) => {
                        if network_tx.send(ebo).await.is_err() { // to grpc server
                            return Err(anyhow::anyhow!("failed to send to grpc network (pipe broken)"));
                        }
                    }
                    None => {
                        tracing::info!("bot_outbound_rx channel closed");
                        return Ok(());
                    }
                }
            }

            server_msg = inbound_stream.message() => { // from grpc server
                match server_msg {
                    Ok(Some(ebo)) => {
                        if bot_inbound_tx.send(ebo).await.is_err() { // to bot (picker)
                            return Err(anyhow::anyhow!("bot inbound receiver dropped"));
                        }
                    }
                    Ok(None) => {
                        return Err(anyhow::anyhow!("server closed stream (EOF)"));
                    }
                    Err(status) => {
                        return Err(anyhow::anyhow!("gRPC Error: {:?}", status));
                    }
                }
            }
        }
    }
}
