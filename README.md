# scratch-link-rs

Multiplatform Scratch Link replacement in Rust. Speaks the Scratch Link
JSON-RPC protocol over WebSocket on `ws://127.0.0.1:20111/scratch/ble` and
bridges it to BLE via [btleplug](https://github.com/deviceplug/btleplug)
(Windows/WinRT, macOS+ iOS/CoreBluetooth, Linux/BlueZ, Android).

## Features

- Multiple simultaneous BLE devices: each WebSocket connection is an isolated
  session (discovery session or one connected peripheral), like the original.
- Discovery with Web-BLE-style filters (`services`, `name`, `namePrefix`),
  announces `didDiscoverPeripheral` including already-cached devices.
- GATT: `read`, `write` (with/without response), `startNotifications`,
  `stopNotifications`, `getServices`, `getCharacteristics`.
- `characteristicDidChange` notifications (base64 payloads).
- `DeviceDisconnected` closes the session socket, matching the JS reference.
- Plain `GET http://127.0.0.1:20111` answers `200 OK` (health check).
- Scanning is refcounted across sessions and paused while a connect is in
  flight (BlueZ cannot connect reliably during active LE discovery).

## Build & Run

```bash
cargo build --release
./target/release/scratch-link-rs
```

Logging via `RUST_LOG`, default `scratch_link_rs=info`.

## Protocol surface

| Method | Notes |
|---|---|
| `getVersion` | `{protocol: "1.3"}` |
| `ping` | `42` |
| `discover` | starts/refreshes scanning, replies `null`, then `didDiscoverPeripheral` notifications |
| `connect` | `{peripheralId}`, 15 s timeout + service discovery, replies `null` |
| `disconnect` | drops the session's peripheral |
| `read` | `{serviceId, characteristicId, startNotifications?}` → base64 result (+ `encoding` sibling, like the reference impl) |
| `write` | `{serviceId, characteristicId, message, encoding?, withResponse?}` → bytes written |
| `startNotifications` / `stopNotifications` | `{serviceId, characteristicId}` |
| `getServices` / `getCharacteristics` | UUID lists |

UUID params accept 128-bit strings or 16/32-bit shorthand (expanded to the
Bluetooth base UUID).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE),
at your option.
