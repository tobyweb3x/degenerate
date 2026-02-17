use crate::models;
use crate::models::Todos;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use kalshi_rs::{Account, KalshiClient};
use polymarket_hft::client::polymarket::gamma;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct Picker {
    kalshi_http_client: Arc<KalshiClient>,
    read_rate_limiter_for_kalshi: Arc<DefaultDirectRateLimiter>,

    polymarket_gamma: gamma::Client,

    rx: mpsc::Receiver<Todos>,
}

impl Picker {
    pub fn new(kalshi_accout: Account, rx: mpsc::Receiver<Todos>) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(15).unwrap());
        let read_rate_limiter = RateLimiter::direct(quota);

        let kalshi_http = KalshiClient::new(kalshi_accout.clone());
        Self {
            polymarket_gamma: gamma::Client::new(),
            read_rate_limiter_for_kalshi: Arc::new(read_rate_limiter),
            kalshi_http_client: Arc::new(kalshi_http),
            rx,
        }
    }

    pub async fn run_picker(&mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::trace!("polymarket received shutdown");
                    break;
                }

                msg = self.rx.recv() => {
                    let Some(todo) = msg else {
                        println!("got a None instead of todo");
                        continue;
                    };

                   match todo {
                       models::Todos::CrossPlatformSimilarityHit(value) => {println!("{}", value)},
                       _ => {}
                   }
                }
            }
        }
    }
}
