use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use btleplug::api::{CentralEvent, CharPropFlags, Peripheral as _, WriteType};
use btleplug::platform::Peripheral;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::ble::BleManager;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

type WsTx = mpsc::UnboundedSender<Message>;
type Ws = WebSocketStream<TcpStream>;

/// One WebSocket connection = one Scratch Link session.
/// A session is either discovering (scanning + announcing peripherals) or
/// bound to exactly one connected peripheral (GATT operations).
pub async fn run(ws: Ws, manager: Arc<BleManager>) -> anyhow::Result<()> {
    let (mut ws_write, mut ws_read) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_write.send(msg).await.is_err() {
                break;
            }
        }
        let _ = ws_write.close().await;
    });

    let mut session = Session::new(tx, manager);
    while let Some(msg) = ws_read.next().await {
        match msg {
            Ok(Message::Text(text)) => session.handle_text(&text).await,
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    session.cleanup().await;
    drop(session);
    let _ = writer.await;
    Ok(())
}

struct Session {
    tx: WsTx,
    manager: Arc<BleManager>,
    /// Active discovery announcement task (spawned per `discover` request).
    discover_task: Option<JoinHandle<()>>,
    discovering: bool,
    device: Option<Device>,
}

struct Device {
    peripheral: Peripheral,
    /// Tasks forwarding notifications / watching disconnect. Aborted on cleanup.
    tasks: Vec<JoinHandle<()>>,
    /// Characteristics with active notify subscriptions (for re-subscribe
    /// after a transparent reconnect).
    subscribed: Arc<std::sync::Mutex<HashSet<(Uuid, Uuid)>>>,
}

/// A Web-BLE-style discovery filter entry: all given fields must match.
#[derive(Default)]
struct FilterEntry {
    services: HashSet<Uuid>,
    name: Option<String>,
    name_prefix: Option<String>,
}

impl FilterEntry {
    fn matches(&self, name: Option<&str>, advertised: &[Uuid]) -> bool {
        if let Some(want) = &self.name {
            if name != Some(want.as_str()) {
                return false;
            }
        }
        if let Some(prefix) = &self.name_prefix {
            if !name
                .map(|n| n.starts_with(prefix.as_str()))
                .unwrap_or(false)
            {
                return false;
            }
        }
        if !self.services.is_empty() && !self.services.iter().all(|s| advertised.contains(s)) {
            return false;
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.services.is_empty() && self.name.is_none() && self.name_prefix.is_none()
    }
}

struct Filters(Vec<FilterEntry>);

impl Filters {
    fn matches(&self, name: Option<&str>, advertised: &[Uuid]) -> bool {
        if self.0.is_empty() {
            return true; // no filters given -> announce everything
        }
        self.0
            .iter()
            .any(|f| !f.is_empty() && f.matches(name, advertised))
            || self.0.iter().all(|f| f.is_empty())
    }
}

impl Session {
    fn new(tx: WsTx, manager: Arc<BleManager>) -> Self {
        Self {
            tx,
            manager,
            discover_task: None,
            discovering: false,
            device: None,
        }
    }

    async fn handle_text(&mut self, text: &str) {
        let msg: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                warn!("bad JSON: {e}");
                return;
            }
        };
        let id = msg.get("id").cloned();
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        debug!("rpc {method} id={id:?}");

        let result = self.dispatch(method, &params).await;
        if let Some(id) = id {
            let reply = match result {
                Ok(r) => {
                    let mut reply = json!({ "jsonrpc": "2.0", "id": id, "result": r });
                    // Reference impl quirk: read replies carry "encoding" as a
                    // sibling of "result", not inside it.
                    if method == "read" {
                        reply["encoding"] = json!("base64");
                    }
                    reply
                }
                Err(e) => json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32603, "message": e.to_string() }
                }),
            };
            let _ = self.tx.send(Message::Text(reply.to_string().into()));
        }
    }

    async fn dispatch(&mut self, method: &str, params: &Value) -> anyhow::Result<Value> {
        match method {
            "getVersion" => Ok(json!({ "protocol": "1.3" })),
            "ping" => Ok(json!(42)),
            "discover" => self.handle_discover(params).await.map(|_| Value::Null),
            "connect" => self.handle_connect(params).await.map(|_| Value::Null),
            "disconnect" => self.handle_disconnect_rpc().await.map(|_| Value::Null),
            "read" => self.handle_read(params).await,
            "write" => self.handle_write(params).await,
            "startNotifications" => self.handle_notify(params, true).await.map(|_| Value::Null),
            "stopNotifications" => self.handle_notify(params, false).await.map(|_| Value::Null),
            "getServices" => self.handle_get_services().await,
            "getCharacteristics" => self.handle_get_characteristics(params).await,
            _ => Err(anyhow::anyhow!("Method not found: {method}")),
        }
    }

    // --- discovery -------------------------------------------------------

    async fn handle_discover(&mut self, params: &Value) -> anyhow::Result<()> {
        let filters = parse_filters(params);
        debug!(
            "discover filters={:?}",
            filters
                .0
                .iter()
                .map(|f| f.services.len())
                .collect::<Vec<_>>()
        );

        if self.discover_task.is_none() {
            self.manager.start_discovery().await;
            self.discovering = true;
        }
        if let Some(t) = self.discover_task.take() {
            t.abort();
        }

        let tx = self.tx.clone();
        let manager = self.manager.clone();
        let mut events = manager.subscribe_events();
        self.discover_task = Some(tokio::spawn(async move {
            // 1. announce already-known (cached) peripherals matching the filter
            for p in manager.peripherals().await {
                announce_peripheral(&manager, &p, &filters, &tx).await;
            }
            // 2. forward events while scanning
            loop {
                match events.recv().await {
                    Ok(CentralEvent::DeviceDiscovered(id))
                    | Ok(CentralEvent::DeviceUpdated(id))
                    | Ok(CentralEvent::ServicesAdvertisement { id, .. })
                    | Ok(CentralEvent::RssiUpdate { id, .. }) => {
                        let token = manager.token(id).await;
                        if let Some(p) = manager.peripheral_for_token(&token).await {
                            announce_peripheral(&manager, &p, &filters, &tx).await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) if is_lagged(&e) => continue,
                    Err(_) => break, // channel closed
                }
            }
        }));
        Ok(())
    }

    // --- connect / disconnect ---------------------------------------------

    async fn handle_connect(&mut self, params: &Value) -> anyhow::Result<()> {
        if self.device.is_some() {
            anyhow::bail!("session already connected");
        }
        let token = params
            .get("peripheralId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("missing peripheralId"))?
            .to_string();

        let peripheral = self.manager.connect(&token, CONNECT_TIMEOUT).await?;

        // Forward characteristic notifications.
        let mut tasks = Vec::new();
        if let Ok(mut stream) = peripheral.notifications().await {
            let tx = self.tx.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(n) = stream.next().await {
                    let msg = json!({
                        "jsonrpc": "2.0",
                        "method": "characteristicDidChange",
                        "params": {
                            "serviceId": n.service_uuid.to_string(),
                            "characteristicId": n.uuid.to_string(),
                            "message": B64.encode(&n.value),
                            "encoding": "base64",
                        }
                    });
                    if tx.send(Message::Text(msg.to_string().into())).is_err() {
                        break;
                    }
                }
            }));
        }

        // BLE disconnect -> close the WebSocket (same contract as the JS impl),
        // with one transparent reconnect for early drops: BlueZ sometimes
        // bounces a fresh link ~1-2s after connect (stale ACL state).
        {
            let mut events = self.manager.subscribe_events();
            let id = peripheral.id();
            let tx = self.tx.clone();
            let dev_periph = peripheral.clone();
            let subscribed = Arc::new(std::sync::Mutex::new(HashSet::new()));
            let connected_at = Instant::now();
            let reconnect_tried = Arc::new(AtomicBool::new(false));
            let watch_subscribed = subscribed.clone();
            let watch_retried = reconnect_tried.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(CentralEvent::DeviceDisconnected(d)) if d == id => {
                            let early = connected_at.elapsed() < Duration::from_secs(6);
                            if early
                                && !watch_retried.swap(true, Ordering::SeqCst)
                                && reconnect(&dev_periph, &watch_subscribed).await.is_ok()
                            {
                                info!("peripheral {id}: transparent reconnect succeeded");
                                continue;
                            }
                            info!("peripheral {id} disconnected, closing session");
                            let _ = tx.send(Message::Close(None));
                            break;
                        }
                        Ok(_) => {}
                        Err(e) if is_lagged(&e) => continue,
                        Err(_) => break,
                    }
                }
            }));
            self.device = Some(Device {
                peripheral,
                tasks,
                subscribed,
            });
        }

        info!("connected {token}");
        Ok(())
    }

    async fn handle_disconnect_rpc(&mut self) -> anyhow::Result<()> {
        if let Some(dev) = self.device.take() {
            for t in &dev.tasks {
                t.abort();
            }
            self.manager.disconnect(&dev.peripheral).await;
        }
        Ok(())
    }

    // --- GATT --------------------------------------------------------------

    fn device(&self) -> anyhow::Result<&Peripheral> {
        self.device
            .as_ref()
            .map(|d| &d.peripheral)
            .ok_or_else(|| anyhow::anyhow!("not connected"))
    }

    fn find_characteristic(
        peripheral: &Peripheral,
        service_id: &str,
        characteristic_id: &str,
    ) -> anyhow::Result<btleplug::api::Characteristic> {
        let service = parse_uuid(service_id)?;
        let characteristic = parse_uuid(characteristic_id)?;
        peripheral
            .characteristics()
            .into_iter()
            .find(|c| c.service_uuid == service && c.uuid == characteristic)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "characteristic {characteristic_id} on service {service_id} not found"
                )
            })
    }

    async fn handle_read(&mut self, params: &Value) -> anyhow::Result<Value> {
        let peripheral = self.device()?.clone();
        let c = Self::find_characteristic(
            &peripheral,
            str_param(params, "serviceId")?,
            str_param(params, "characteristicId")?,
        )?;
        if params.get("startNotifications").and_then(Value::as_bool) == Some(true) {
            peripheral.subscribe(&c).await?;
            if let Some(dev) = &self.device {
                dev.subscribed
                    .lock()
                    .unwrap()
                    .insert((c.service_uuid, c.uuid));
            }
        }
        let value = peripheral.read(&c).await?;
        Ok(Value::String(B64.encode(&value)))
    }

    async fn handle_write(&mut self, params: &Value) -> anyhow::Result<Value> {
        let peripheral = self.device()?.clone();
        let c = Self::find_characteristic(
            &peripheral,
            str_param(params, "serviceId")?,
            str_param(params, "characteristicId")?,
        )?;
        let data = decode_message(params)?;
        let write_type = match params.get("withResponse").and_then(Value::as_bool) {
            Some(true) => WriteType::WithResponse,
            Some(false) => WriteType::WithoutResponse,
            None => {
                if c.properties.contains(CharPropFlags::WRITE_WITHOUT_RESPONSE)
                    && !c.properties.contains(CharPropFlags::WRITE)
                {
                    WriteType::WithoutResponse
                } else {
                    WriteType::WithResponse
                }
            }
        };
        let len = data.len();
        peripheral.write(&c, &data, write_type).await?;
        Ok(json!(len))
    }

    async fn handle_notify(&mut self, params: &Value, enable: bool) -> anyhow::Result<()> {
        let peripheral = self.device()?.clone();
        let c = Self::find_characteristic(
            &peripheral,
            str_param(params, "serviceId")?,
            str_param(params, "characteristicId")?,
        )?;
        if enable {
            peripheral.subscribe(&c).await?;
        } else {
            peripheral.unsubscribe(&c).await?;
        }
        if let Some(dev) = &self.device {
            let mut sub = dev.subscribed.lock().unwrap();
            if enable {
                sub.insert((c.service_uuid, c.uuid));
            } else {
                sub.remove(&(c.service_uuid, c.uuid));
            }
        }
        Ok(())
    }

    async fn handle_get_services(&mut self) -> anyhow::Result<Value> {
        let peripheral = self.device()?;
        let services: Vec<String> = peripheral
            .services()
            .iter()
            .map(|s| s.uuid.to_string())
            .collect();
        Ok(json!(services))
    }

    async fn handle_get_characteristics(&mut self, params: &Value) -> anyhow::Result<Value> {
        let peripheral = self.device()?;
        let service = parse_uuid(str_param(params, "serviceId")?)?;
        let chars: Vec<String> = peripheral
            .characteristics()
            .iter()
            .filter(|c| c.service_uuid == service)
            .map(|c| c.uuid.to_string())
            .collect();
        Ok(json!(chars))
    }

    // --- cleanup ------------------------------------------------------------

    async fn cleanup(&mut self) {
        if let Some(t) = self.discover_task.take() {
            t.abort();
        }
        if self.discovering {
            self.manager.stop_discovery().await;
            self.discovering = false;
        }
        if let Some(dev) = self.device.take() {
            for t in dev.tasks {
                t.abort();
            }
            info!(
                "client socket closed, disconnecting {}",
                dev.peripheral.id()
            );
            self.manager.disconnect(&dev.peripheral).await;
        }
    }
}

async fn announce_peripheral(
    manager: &Arc<BleManager>,
    peripheral: &Peripheral,
    filters: &Filters,
    tx: &WsTx,
) {
    let props = match peripheral.properties().await {
        Ok(Some(p)) => p,
        _ => return,
    };
    let name = props
        .local_name
        .clone()
        .or(props.advertisement_name.clone());
    if !filters.matches(name.as_deref(), &props.services) {
        return;
    }
    let token = manager.token(peripheral.id()).await;
    let msg = json!({
        "jsonrpc": "2.0",
        "method": "didDiscoverPeripheral",
        "params": {
            "peripheralId": token,
            "name": name,
            "rssi": props.rssi,
        }
    });
    let _ = tx.send(Message::Text(msg.to_string().into()));
}

/// Reconnect after an early link drop: fresh connect, re-run service
/// discovery and re-subscribe all characteristics that had notifications on.
async fn reconnect(
    peripheral: &Peripheral,
    subscribed: &std::sync::Mutex<HashSet<(Uuid, Uuid)>>,
) -> anyhow::Result<()> {
    peripheral
        .connect_with_timeout(Duration::from_secs(10))
        .await?;
    peripheral.discover_services().await?;
    let chars: Vec<(Uuid, Uuid)> = subscribed.lock().unwrap().iter().copied().collect();
    for (service_uuid, char_uuid) in chars {
        if let Some(c) = peripheral
            .characteristics()
            .iter()
            .find(|c| c.service_uuid == service_uuid && c.uuid == char_uuid)
        {
            if let Err(e) = peripheral.subscribe(c).await {
                warn!("re-subscribe {char_uuid} failed: {e}");
            }
        }
    }
    Ok(())
}

fn is_lagged(e: &tokio::sync::broadcast::error::RecvError) -> bool {
    matches!(e, tokio::sync::broadcast::error::RecvError::Lagged(_))
}

fn str_param<'a>(params: &'a Value, name: &str) -> anyhow::Result<&'a str> {
    params
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing param {name}"))
}

fn parse_uuid(s: &str) -> anyhow::Result<Uuid> {
    let s = s.trim().trim_start_matches("0x");
    let expanded = match s.len() {
        4 => format!("0000{s}-0000-1000-8000-00805f9b34fb"),
        8 => format!("{s}-0000-1000-8000-00805f9b34fb"),
        _ => s.to_string(),
    };
    Uuid::parse_str(&expanded).map_err(|e| anyhow::anyhow!("bad uuid '{s}': {e}"))
}

fn parse_filters(params: &Value) -> Filters {
    let mut entries = Vec::new();
    if let Some(filters) = params.get("filters").and_then(Value::as_array) {
        for f in filters {
            let mut entry = FilterEntry::default();
            if let Some(services) = f.get("services").and_then(Value::as_array) {
                for s in services {
                    if let Some(s) = s.as_str() {
                        if let Ok(u) = parse_uuid(s) {
                            entry.services.insert(u);
                        }
                    }
                }
            }
            entry.name = f.get("name").and_then(Value::as_str).map(str::to_string);
            entry.name_prefix = f
                .get("namePrefix")
                .and_then(Value::as_str)
                .map(str::to_string);
            entries.push(entry);
        }
    }
    Filters(entries)
}

fn decode_message(params: &Value) -> anyhow::Result<Vec<u8>> {
    let msg = params
        .get("message")
        .ok_or_else(|| anyhow::anyhow!("missing message"))?;
    match msg {
        Value::String(s) => {
            if params.get("encoding").and_then(Value::as_str) == Some("base64") {
                B64.decode(s)
                    .map_err(|e| anyhow::anyhow!("bad base64: {e}"))
            } else {
                Ok(s.clone().into_bytes())
            }
        }
        Value::Array(arr) => arr
            .iter()
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| anyhow::anyhow!("bad byte value in message"))
            })
            .collect(),
        _ => Err(anyhow::anyhow!("unsupported message type")),
    }
}
