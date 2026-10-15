# Opera Hub

Opera Hub is a small self-hosted server that keeps your [Opera](https://github.com/MaestroAI-Company/opera) devices in sync. Install it on a computer you own, pair your phone, desktop and browser, and the hub handles the rest:

- **Sync**: conversations, settings and AI providers move between your devices.
- **Shared AI**: Ollama, LM Studio and llama.cpp running on the hub's computer can be used from every paired device, even outside your home network.
- **Local models**: the hub can download and run [llama.cpp](https://github.com/ggml-org/llama.cpp) models itself. It picks the GPU backend for you, and you manage it from the Opera app.

Your data stays on your hardware. There is no account and no Opera server in the middle.

## Install

### Windows

Download `opera-hub-setup-windows-x86_64.exe` from the [latest release](https://github.com/MaestroAI-Company/opera-hub/releases/latest) and run it. It installs for your user only, in `%LOCALAPPDATA%\MaestroAI-Company\Opera Hub`, and starts the hub with your session. A tray icon shows the pairing code and offers updates.

You can also host the hub from the Opera desktop app: open **Settings → Opera Hub → Host the hub here**.

### macOS (Apple silicon)

Download `opera-hub-macos-aarch64.dmg` from the latest release. To install from a terminal instead:

```sh
curl -fsSL https://github.com/MaestroAI-Company/opera-hub/releases/latest/download/install.sh | sh
```

### Linux (x86_64 and arm64)

```sh
curl -fsSL https://github.com/MaestroAI-Company/opera-hub/releases/latest/download/install.sh | sh
```

The script checks every file against the release `SHA256SUMS`. It installs `opera-hub` in `~/.local/bin` (change it with `OPERA_HUB_DIR`) and registers a systemd user service, or a launchd agent on macOS, so the hub starts with your session.

## Pair a device

On the hub's computer:

```sh
opera-hub pair
```

The command prints a QR code and an 8-character code, both valid for a single pairing:

- **phone**: scan the QR code from **Settings → Opera Hub → Pair a hub**;
- **desktop or browser**: type the code in the same place.

On the same network, Opera also finds the hub by itself and lists it under **Nearby hubs**. You still have to enter the code; being on the network grants nothing.

On Windows and macOS, the tray icon shows the same code.

## Commands

| Command | What it does |
|---|---|
| `opera-hub run` | Run the hub in the foreground |
| `opera-hub pair` | Show a one-time QR code and code to pair a device |
| `opera-hub devices` | List paired devices |
| `opera-hub devices remove <id>` | Unpair a device by id or id prefix |
| `opera-hub service install` | Start the hub with your session, now and at every login |
| `opera-hub service start` / `stop` / `status` | Control the background service |
| `opera-hub service uninstall` | Stop the hub and remove it from your session |
| `opera-hub ticket` | Print the hub ticket, for `ping` |
| `opera-hub ping <ticket>` | Check that a hub is reachable |

Every command takes `--data-dir <path>` to use another data directory, for example to run two hubs side by side.

## Managing the hub from Opera

Phones and desktops paired to the hub can manage it from **Settings → Opera Hub**:

- choose what each device syncs;
- rename or remove paired devices. Devices not seen for 30 days are marked inactive, and reinstalling Opera on the same device replaces its old entry;
- turn local network discovery and provider sharing on or off;
- install hub updates. The hub checks GitHub every 6 hours and Opera offers the new version;
- download, run and delete llama.cpp models.

Browsers can sync but never manage the hub.

## Local models with llama.cpp

The first model you download from Opera makes the hub install the official `llama-server` build that suits the machine: CUDA, Vulkan or Metal when available, CPU otherwise. Every download is checked against the digest GitHub publishes. You then pick a recommended model or search Hugging Face for any GGUF model and version.

The model is served to all your devices as the **llama.cpp** provider of the hub. One model is loaded at a time.

## How it works

- **Transport**: devices reach the hub through [iroh](https://github.com/n0-computer/iroh) QUIC connections, encrypted end to end and authenticated by key. They connect directly when they can and through a relay otherwise, so the hub works from anywhere without opening a port.
- **Pairing**: the code is stretched with Argon2id into a temporary key that opens a one-time pairing endpoint. A wrong code closes it after 5 attempts.
- **Access**: every request is denied by default. Each paired device has its own key and its own permissions, and access is checked again on every stream, so a removed device is cut off at once.
- **Storage**: the hub keeps its data in an SQLite database, each row encrypted with XChaCha20-Poly1305. The key lives in the OS keychain, or in a key file when no keychain is available.

The hub keeps its data here:

| OS | Data directory |
|---|---|
| Windows | `%LOCALAPPDATA%\MaestroAI-Company\Opera Hub\data` |
| macOS | `~/Library/Application Support/company.MaestroAI-Company.Opera-Hub` |
| Linux | `~/.local/share/operahub` |

## Build from source

You need a recent stable Rust toolchain.

```sh
cargo build --release -p opera-hub                         # opera-hub and opera-hubw, the tray app
cargo build --release -p opera-hub --no-default-features   # opera-hub only, for headless servers
cargo test --workspace
```

The repository holds four crates:

| Crate | Role |
|---|---|
| `opera-hub` | The hub: CLI, background service, tray, sync store, llama.cpp runtime |
| `opera-hub-proto` | Protocol types shared by the hub and its clients |
| `opera-hub-client` | The client library Opera uses, with Kotlin bindings (uniffi) for Android and a WebAssembly build for browsers |
| `uniffi-bindgen` | Generates the Kotlin bindings |

## License

[GNU AGPL v3](LICENSE)
