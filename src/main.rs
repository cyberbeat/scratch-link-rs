mod ble;
mod session;
mod ws;

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "scratch_link_rs=info,btleplug=warn".parse().unwrap()),
        )
        .init();

    let manager = match ble::BleManager::new().await {
        Ok(m) => Arc::new(m),
        Err(e) => {
            error!("BLE init failed: {e}");
            std::process::exit(1);
        }
    };
    match manager.adapter_info().await {
        Ok(info) => info!("BLE adapter: {info}"),
        Err(e) => warn!("adapter info unavailable: {e}"),
    }

    let listener = match TcpListener::bind(("127.0.0.1", 20111)).await {
        Ok(l) => l,
        Err(e) => {
            error!("cannot bind 127.0.0.1:20111: {e} (another scratch-link running?)");
            std::process::exit(1);
        }
    };
    info!("listening on ws://127.0.0.1:20111/scratch/ble");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, addr)) => {
                        let mgr = manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = ws::handle_connection(stream, mgr).await {
                                warn!("connection {addr}: {e}");
                            }
                        });
                    }
                    Err(e) => error!("accept failed: {e}"),
                }
            }
            _ = shutdown_signal() => {
                info!("shutting down, disconnecting peripherals");
                manager.disconnect_all().await;
                break;
            }
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = term.recv() => {},
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
