use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Attempting to connect.
    Connecting,
    /// Successfully connected.
    Connected,
    /// Disconnected from server.
    Disconnected,
}

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>;
type WsReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct ManageWS {
    base_url: String,
    ping_interval: Duration,
    auto_reconnect: bool,
    status: Arc<Mutex<ConnectionStatus>>,
    writer: Arc<Mutex<Option<WsWriter>>>,
    reader: Arc<Mutex<Option<WsReader>>>,
}

impl Clone for ManageWS {
    fn clone(&self) -> Self {
        Self {
            base_url: self.base_url.clone(),
            ping_interval: self.ping_interval,
            auto_reconnect: self.auto_reconnect,
            status: Arc::clone(&self.status),
            reader: Arc::clone(&self.reader),
            writer: Arc::clone(&self.writer),
        }
    }
}
// use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

impl ManageWS {
    pub fn new(base_url: impl Into<String>, ping_interval: Duration, auto_reconnect: bool) -> Self {
        Self {
            base_url: base_url.into(),
            ping_interval,
            auto_reconnect,
            status: Arc::new(Mutex::new(ConnectionStatus::Disconnected)),
            writer: Arc::new(Mutex::new(None)),
            reader: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        let (ws, _response) = connect_async(&self.base_url)
            .await
            .with_context(|| format!("webSocket handshake failed: {}", self.base_url))?;

        let (writer, reader) = ws.split();

        *self.writer.lock().await = Some(writer);
        *self.reader.lock().await = Some(reader);
        *self.status.lock().await = ConnectionStatus::Connected;

        self.start_ping_loop();

        Ok(())
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<()> {
        self.ensure_connected().await?;

        let mut writer_guard = self.writer.lock().await;

        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("webSocket writer missing"))?;

        writer
            .send(TungsteniteMessage::Text(text.into().into()))
            .await
            .with_context(|| "webSocket send failed")?;

        Ok(())
    }

    pub async fn send_ping(&self) -> Result<()> {
        self.ensure_connected().await?;

        let mut writer_guard = self.writer.lock().await;
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("webSocket writer missing"))?;

        writer
            .send(TungsteniteMessage::Ping(Vec::new().into()))
            .await?;

        Ok(())
    }

    pub async fn send_pong(&self, data: Vec<u8>) -> Result<()> {
        self.ensure_connected().await?;

        let mut writer_guard = self.writer.lock().await;

        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("webSocket writer missing"))?;

        writer
            .send(TungsteniteMessage::Pong(data.into()))
            .await
            .with_context(|| "error sending pong")?;

        Ok(())
    }

    pub async fn send_subscription<T: serde::Serialize>(&self, subscription: &T) -> Result<()> {
        self.ensure_connected().await?;

        let json = serde_json::to_string(subscription)?;
        tracing::trace!("Sending subscription: {}", json);
        self.send_text(json).await
    }

    async fn set_status(&self, status: ConnectionStatus) {
        *self.status.lock().await = status;
    }

    async fn ensure_connected(&self) -> Result<()> {
        let status = *self.status.lock().await;
        if status != ConnectionStatus::Connected {
            anyhow::bail!("webSocket not connected");
        }
        Ok(())
    }

    pub async fn next_message(&self) -> Result<serde_json::Value> {
        self.ensure_connected().await?;

        loop {
            let msg = {
                let mut reader_guard = self.reader.lock().await;
                let reader = reader_guard
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("webSocket reader missing"))?;
                reader.next().await
            };

            match msg {
                Some(Ok(TungsteniteMessage::Text(text))) => {
                    if text == "PONG" || text.is_empty() {
                        tracing::trace!("received PONG");
                        continue;
                    }

                    let value = serde_json::from_str::<serde_json::Value>(&text)
                        .with_context(|| format!("failed to parse WS text: {text}"))?;

                    return Ok(value);
                }

                Some(Ok(TungsteniteMessage::Ping(data))) => {
                    tracing::trace!("received ping, sending pong");
                    if let Err(e) = self.send_pong(data.to_vec()).await {
                        tracing::error!("Failed to send pong: {e}");
                    }
                }

                Some(Ok(TungsteniteMessage::Pong(_))) => {
                    tracing::trace!("pong received");
                }

                Some(Ok(TungsteniteMessage::Close(frame))) => {
                    self.set_status(ConnectionStatus::Disconnected).await;
                    return Err(anyhow::anyhow!("WebSocket closed: {:?}", frame));
                }

                Some(Err(e)) => {
                    self.set_status(ConnectionStatus::Disconnected).await;
                    return Err(e).context("WebSocket stream error");
                }

                None => {
                    self.set_status(ConnectionStatus::Disconnected).await;
                    return Err(anyhow::anyhow!("WebSocket stream ended"));
                }

                _ => {
                    // ignore binary & other message types
                }
            }
        }
    }

    fn start_ping_loop(&self) {
        let client = self.clone();

        tokio::spawn(async move {
            let mut ticker = interval(client.ping_interval);

            loop {
                ticker.tick().await;

                let status = *client.status.lock().await;
                if status != ConnectionStatus::Connected {
                    break;
                }

                if let Err(e) = client.send_ping().await {
                    tracing::warn!("ping failed in ping_loop: {e}");
                    break;
                }

                tracing::trace!("ping sent in ping_loop")
            }
        });
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn ping_interval(&self) -> Duration {
        self.ping_interval
    }

    pub fn auto_reconnect(&self) -> bool {
        self.auto_reconnect
    }
}
