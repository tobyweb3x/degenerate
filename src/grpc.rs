use crate::models::protos;
use anyhow::Context;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

pub struct GrpcServer {
    broadcast_tx: broadcast::Sender<protos::Ebo>,
    inbound_messages_tx: mpsc::Sender<protos::Ebo>,
}

impl GrpcServer {
    pub fn new(inbound_messages_tx: mpsc::Sender<protos::Ebo>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(100);

        Self {
            broadcast_tx,
            inbound_messages_tx,
        }
    }

    pub fn broadcast_to_receiver(&self, ebo: protos::Ebo) -> anyhow::Result<()> {
        self.broadcast_tx
            .send(ebo)
            .context("error broadcasting message")?;

        Ok(())
    }

    pub async fn handle_bot_flow(
        &self,
        bot_rx_shutdown: CancellationToken,
        mut bot_rx: mpsc::Receiver<protos::Ebo>,
    ) {
        loop {
            tokio::select! {
                _ = bot_rx_shutdown.cancelled() => {
                    tracing::trace!("bot_rx received shutdown");
                    break;
                },

                maybe_msg = bot_rx.recv(), if !bot_rx.is_closed() => {
                    match maybe_msg {
                        Some(msg) => {
                            // based on message, do some work &
                            // send back apt response to the broadcast (i.e. grpc server)
                            if let Err(e)= self.broadcast_to_receiver(msg) {
                                eprintln!("{e}")
                            }
                        }


                        None => {
                            tracing::info!("Bot channel closed (All senders dropped). Stopping.");
                            break;
                        },
                    }
                },
            }
        }
    }
}

#[tonic::async_trait]
impl protos::esu_odara_server::EsuOdara for GrpcServer {
    type EsuStream = ReceiverStream<Result<protos::Ebo, Status>>;

    async fn esu(
        &self,
        request: Request<Streaming<protos::Ebo>>,
    ) -> Result<Response<Self::EsuStream>, Status> {
        let mut global_rx = self.broadcast_tx.subscribe();

        let (per_client_tx, per_client_rx) = mpsc::channel(128);

        let mut inbound_grpc_stream = request.into_inner();
        let cloned_tx = self.inbound_messages_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {

                    // BOT → CLIENT
                    broadcast_msg = global_rx.recv() => {
                        match broadcast_msg {
                            Ok(msg) => {
                               if let Err(status) = per_client_tx.send(Ok(msg)).await {
                                    eprintln!(
                                        "gRPC: error sending to client Receiver, dropping sender. error: {:?}",
                                        status
                                    );
                                    break;
                                }

                            }

                            Err(broadcast::error::RecvError::Lagged(dropped)) => {
                                eprintln!("client is too slow,{dropped} skip messages")
                            }

                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }

                    // CLIENT → BOT
                    grpc_msg = inbound_grpc_stream.message() => {
                        match grpc_msg {
                            Ok(Some(msg)) => {
                                if let Err(e) = cloned_tx.send(msg).await {
                                    eprintln!("gRPC: error sending to bot, error: {:?}", e);
                                    break;
                                }
                            }

                            Ok(None) => {
                                println!("Client closed stream");
                                break;
                            }

                            Err(status) => {
                                eprintln!(
                                    "gRPC: error from client. code: {:?}, message: {:?}",
                                    status.code().description(),
                                    status.message()
                                );
                                break;
                            }
                        }
                    }
                }
            }

            println!("Connection task ended");
        });

        Ok(Response::new(ReceiverStream::new(per_client_rx)))
    }
}
