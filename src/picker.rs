use crate::models::protos;
use crate::platforms;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct Picker {
    platforms: platforms::Platfroms,
    rx: mpsc::Receiver<protos::Ebo>,
    tx: mpsc::Sender<protos::Ebo>,
}

impl Picker {
    pub fn new(
        platforms: platforms::Platfroms,
        rx: mpsc::Receiver<protos::Ebo>,
        tx: mpsc::Sender<protos::Ebo>,
    ) -> Self {
        Self { platforms, rx, tx }
    }

    pub async fn run_picker(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    break;
                }

                msg = self.rx.recv() => {
                    let Some(ebo) = msg else {
                        println!("got a None instead of todo, picker closed");
                        break;
                    };

                    println!("got from grpc {:?}", ebo);
                }
            }
        }

        Ok(())
    }
}
