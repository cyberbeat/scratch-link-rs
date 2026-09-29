use std::collections::HashMap;
use std::time::Duration;

use btleplug::api::{Central, CentralEvent, Manager as _, Peripheral as _, ScanFilter};
use btleplug::platform::{Adapter, Manager, Peripheral, PeripheralId};
use futures_util::StreamExt;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};

const EVENT_CHANNEL: usize = 512;

/// Shared BLE adapter state. Owns the central event stream (rebroadcast to all
/// sessions) and refcounted scan control. Scanning is paused while a connect
/// is in flight (BlueZ cannot reliably connect during active LE discovery).
pub struct BleManager {
    adapter: Adapter,
    events_tx: broadcast::Sender<CentralEvent>,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Stable token -> platform PeripheralId, for resolving client-side IDs.
    registry: HashMap<String, PeripheralId>,
    /// Number of sessions with an active `discover`.
    scan_subscribers: usize,
    /// In-flight connect operations; scanning stays off while > 0.
    connect_pause: usize,
    scanning: bool,
}

impl BleManager {
    pub async fn new() -> btleplug::Result<Self> {
        let manager = Manager::new().await?;
        let adapter = manager
            .adapters()
            .await?
            .into_iter()
            .next()
            .ok_or(btleplug::Error::DeviceNotFound)?;

        let mut events = adapter.events().await?;
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL);
        let tx = events_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = events.next().await {
                let _ = tx.send(ev); // lagging receivers skip events
            }
        });

        Ok(Self {
            adapter,
            events_tx,
            inner: Mutex::new(Inner::default()),
        })
    }

    pub async fn adapter_info(&self) -> btleplug::Result<String> {
        self.adapter.adapter_info().await
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<CentralEvent> {
        self.events_tx.subscribe()
    }

    /// Stable string token for a peripheral id (used as `peripheralId` in the
    /// Scratch Link protocol). Registers the id for later lookup.
    pub async fn token(&self, id: PeripheralId) -> String {
        let token = id.to_string();
        self.inner.lock().await.registry.insert(token.clone(), id);
        token
    }

    pub async fn peripheral_for_token(&self, token: &str) -> Option<Peripheral> {
        let id = self.inner.lock().await.registry.get(token).cloned();
        let id = match id {
            Some(id) => id,
            None => {
                // Unknown token: rescan known peripherals, register, retry.
                self.refresh_registry().await;
                self.inner.lock().await.registry.get(token).cloned()?
            }
        };
        match self.adapter.peripheral(&id).await {
            Ok(p) => Some(p),
            Err(e) => {
                warn!("peripheral lookup failed for {token}: {e}");
                None
            }
        }
    }

    /// Snapshot of all known peripherals (includes BlueZ-cached devices).
    pub async fn peripherals(&self) -> Vec<Peripheral> {
        let list = self.adapter.peripherals().await.unwrap_or_default();
        for p in &list {
            self.token(p.id()).await;
        }
        list
    }

    async fn refresh_registry(&self) {
        let _ = self.peripherals().await;
    }

    /// One more session wants discovery.
    pub async fn start_discovery(&self) {
        self.inner.lock().await.scan_subscribers += 1;
        self.reconcile_scan().await;
    }

    /// Session no longer wants discovery (socket closed / refresh replaced).
    pub async fn stop_discovery(&self) {
        let mut inner = self.inner.lock().await;
        inner.scan_subscribers = inner.scan_subscribers.saturating_sub(1);
        drop(inner);
        self.reconcile_scan().await;
    }

    /// Pause scanning while connecting (BlueZ quirk) and run connect +
    /// service discovery. Always resumes scanning afterwards.
    /// Clears stale/zombie links first: a still-"connected" BlueZ entry makes
    /// the next connect bounce ~2s after it appears to succeed.
    pub async fn connect(&self, token: &str, timeout: Duration) -> anyhow::Result<Peripheral> {
        let peripheral = self
            .peripheral_for_token(token)
            .await
            .ok_or_else(|| anyhow::anyhow!("peripheral {token} not found"))?;

        {
            self.inner.lock().await.connect_pause += 1;
            self.reconcile_scan().await;
        }
        // Let the adapter settle after discovery stops before connecting.
        tokio::time::sleep(Duration::from_millis(250)).await;
        if peripheral.is_connected().await.unwrap_or(false) {
            debug!("{token}: clearing stale connection before connect");
            let _ = peripheral.disconnect().await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let result = async {
            info!("connect {token}");
            peripheral.connect_with_timeout(timeout).await?;
            peripheral.discover_services().await?;
            anyhow::Ok(())
        }
        .await;
        {
            let mut inner = self.inner.lock().await;
            inner.connect_pause = inner.connect_pause.saturating_sub(1);
            drop(inner);
            self.reconcile_scan().await;
        }

        if let Err(e) = result {
            warn!("connect {token} failed: {e}");
            let _ = peripheral.disconnect().await;
            return Err(e);
        }
        debug!("connect {token} ok");
        Ok(peripheral)
    }

    pub async fn disconnect(&self, peripheral: &Peripheral) {
        let _ = peripheral.disconnect().await;
    }

    /// Graceful shutdown: drop every live BLE link. BlueZ keeps connections
    /// alive even when the requesting D-Bus client exits, so without this a
    /// killed daemon would leave hubs connected until they time out.
    pub async fn disconnect_all(&self) {
        for p in self.peripherals().await {
            if p.is_connected().await.unwrap_or(false) {
                info!("shutdown: disconnecting {}", p.id());
                let _ = p.disconnect().await;
            }
        }
    }

    async fn reconcile_scan(&self) {
        let (desired, active) = {
            let inner = self.inner.lock().await;
            let desired = inner.scan_subscribers > 0 && inner.connect_pause == 0;
            (desired, inner.scanning)
        };
        if desired == active {
            return;
        }
        let result = if desired {
            debug!("start_scan");
            self.adapter.start_scan(ScanFilter::default()).await
        } else {
            debug!("stop_scan");
            self.adapter.stop_scan().await
        };
        match result {
            Ok(()) => self.inner.lock().await.scanning = desired,
            Err(e) => {
                // BlueZ stops discovery on its own when a connect happens, so
                // our flag can desync. A failed stop means "not scanning" in
                // practice — converge the flag to the desired state.
                debug!("scan {} failed (converging flag): {e}", if desired { "start" } else { "stop" });
                self.inner.lock().await.scanning = desired;
            }
        }
    }
}
