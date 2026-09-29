
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tracing::debug;

use crate::ble::BleManager;
use crate::session;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_HEADER: usize = 8192;
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Serves one TCP connection: plain HTTP GET -> "200 OK" health check
/// (same as the reference server), WebSocket upgrade -> BLE session.
pub async fn handle_connection(mut stream: TcpStream, manager: Arc<BleManager>) -> anyhow::Result<()> {
    // Read the HTTP request head byte-by-byte so we never consume bytes that
    // belong to the first WebSocket frame.
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    tokio::time::timeout(HEADER_TIMEOUT, async {
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() >= MAX_HEADER {
                anyhow::bail!("HTTP header too large");
            }
            if stream.read(&mut byte).await? == 0 {
                anyhow::bail!("connection closed before headers complete");
            }
            head.push(byte[0]);
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;

    let head = String::from_utf8_lossy(&head);
    debug!("request head:\n{head}");

    let key = websocket_key(&head);
    if key.is_none() {
        // Plain HTTP request: health check (original server answers "OK").
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            .await?;
        return Ok(());
    }
    let key = key.unwrap();

    let accept = B64.encode(Sha1::digest(format!("{key}{WS_GUID}").as_bytes()));
    stream
        .write_all(
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await?;

    let ws = WebSocketStream::from_raw_socket(stream, tokio_tungstenite::tungstenite::protocol::Role::Server, None).await;
    session::run(ws, manager).await
}

/// Returns the Sec-WebSocket-Key if this request head is a websocket upgrade.
fn websocket_key(head: &str) -> Option<String> {
    let mut upgrade = false;
    let mut key = None;
    for (i, line) in head.lines().enumerate() {
        let line = line.trim();
        if i == 0 {
            if !line.to_ascii_uppercase().starts_with("GET") {
                return None;
            }
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "upgrade" if value.trim().eq_ignore_ascii_case("websocket") => upgrade = true,
                "sec-websocket-key" => key = Some(value.trim().to_string()),
                _ => {}
            }
        }
    }
    if upgrade { key } else { None }
}
