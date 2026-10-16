//installs real mcp servers, run with --ignored
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use opera_hub::mcp::{Mcp, Source, Spec};
use opera_hub_client::{HubClient, HubError, PairedHub, ProxyInfo};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

//a dependency free stdio server
const ECHO_SERVER: &str = r#"
const readline = require("readline");
const send = (m) => process.stdout.write(JSON.stringify(m) + "\n");
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const m = JSON.parse(line);
  if (m.id === undefined) return;
  if (m.method === "initialize") {
    send({ jsonrpc: "2.0", id: m.id, result: { protocolVersion: m.params.protocolVersion,
      capabilities: { tools: {} }, serverInfo: { name: "echo", version: "1.0.0" } } });
  } else if (m.method === "tools/list") {
    send({ jsonrpc: "2.0", id: m.id, result: { tools: [{ name: "echo",
      inputSchema: { type: "object", properties: { text: { type: "string" } } } }] } });
  } else if (m.method === "tools/call") {
    const text = process.env.ECHO_PREFIX + m.params.arguments.text;
    send({ jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text }] } });
  } else {
    send({ jsonrpc: "2.0", id: m.id, error: { code: -32601, message: "unknown" } });
  }
});
"#;

const ECHO_MANIFEST: &str = r#"{
  "manifest_version": "0.3",
  "name": "echo",
  "version": "1.0.0",
  "server": {
    "type": "node",
    "entry_point": "server/index.js",
    "mcp_config": {
      "command": "node",
      "args": ["${__dirname}/server/index.js"],
      "env": { "ECHO_PREFIX": "${user_config.prefix}" }
    }
  },
  "user_config": { "prefix": { "type": "string", "required": true } }
}"#;

fn random_hex<const N: usize>() -> String {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).unwrap();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn new_client() -> Arc<HubClient> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).unwrap();
    HubClient::new(key.to_vec()).unwrap()
}

fn bundle() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, body) in [
        ("manifest.json", ECHO_MANIFEST),
        ("server/index.js", ECHO_SERVER),
    ] {
        writer
            .start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(body.as_bytes()).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

//serves one file to the hub
async fn serve_once(bytes: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            bytes.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(&bytes).await.unwrap();
    });
    format!("http://127.0.0.1:{port}/echo.mcpb")
}

async fn pair(client: &HubClient, data_dir: &std::path::Path, name: &str) -> PairedHub {
    let link = opera_hub::hub::pairing_link(data_dir).unwrap().to_string();
    for _ in 0..10 {
        match client
            .pair(link.clone(), name.into(), "phone".into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
            result => return result.unwrap(),
        }
    }
    panic!("hub never became reachable");
}

//one json-rpc call through the device proxy
async fn rpc(proxy: &ProxyInfo, provider: &str, message: Value) -> Value {
    let body = message.to_string();
    let mut socket = TcpStream::connect(("127.0.0.1", proxy.port)).await.unwrap();
    let request = format!(
        "POST /{}/{provider}/ HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\naccept: application/json, text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        proxy.token,
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        unchunk(body)
    } else {
        body.to_string()
    };
    serde_json::from_str(&body).unwrap()
}

fn unchunk(mut body: &str) -> String {
    let mut out = String::new();
    loop {
        let (size, rest) = body.split_once("\r\n").unwrap();
        let size = usize::from_str_radix(size.trim(), 16).unwrap();
        if size == 0 {
            return out;
        }
        out.push_str(&rest[..size]);
        body = &rest[size + 2..];
    }
}

async fn call_tool(
    proxy: &ProxyInfo,
    provider: &str,
    id: u64,
    name: &str,
    arguments: Value,
) -> String {
    let answer = rpc(
        proxy,
        provider,
        json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": { "name": name, "arguments": arguments } }),
    )
    .await;
    assert_eq!(answer["id"], id, "{answer}");
    answer["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{answer}"))
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads node, uv and mcp servers"]
async fn hub_runs_mcp_servers() {
    let data_dir = std::env::temp_dir().join(format!("opera-hub-mcp-{}", random_hex::<8>()));
    let http = opera_hub::secrets::http_client().unwrap();
    {
        let mcp = Mcp::new(&data_dir, http);
        let bytes = bundle();
        let sha256: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let specs = [
            Spec {
                name: "Echo".into(),
                source: Source::Mcpb {
                    url: serve_once(bytes).await,
                    sha256,
                },
                args: Vec::new(),
                config: BTreeMap::from([("prefix".into(), "hub says ".into())]),
            },
            Spec {
                name: "Everything".into(),
                source: Source::Npm {
                    package: "@modelcontextprotocol/server-everything".into(),
                    version: "1.0.0".into(),
                },
                args: Vec::new(),
                config: BTreeMap::new(),
            },
            Spec {
                name: "Time".into(),
                source: Source::Pypi {
                    package: "mcp-server-time".into(),
                    version: "2026.10.10".into(),
                },
                args: vec!["--local-timezone".into(), "Europe/Paris".into()],
                config: BTreeMap::new(),
            },
        ];
        for spec in specs {
            mcp.install(spec).await.unwrap();
        }
        //the hub starts them again on its own
        mcp.stop_all();
    }

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            opera_hub::hub::run(&data_dir, async {
                let _ = stop_rx.await;
            })
            .await
        }
    });
    let phone = new_client();
    let laptop = new_client();
    let phone_hub = pair(&phone, &data_dir, "phone").await;
    let laptop_hub = pair(&laptop, &data_dir, "laptop").await;

    let mut servers = Vec::new();
    for _ in 0..120 {
        servers = phone.providers(phone_hub.clone()).await.unwrap();
        servers.retain(|p| p.kind == "mcp");
        if servers.len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(servers.len(), 3, "{servers:?}");
    let id_of = |name: &str| servers.iter().find(|p| p.name == name).unwrap().id.clone();
    let (echo, everything, time) = (id_of("Echo"), id_of("Everything"), id_of("Time"));

    let phone_proxy = phone.clone().start_proxy(phone_hub).await.unwrap();
    let laptop_proxy = laptop.clone().start_proxy(laptop_hub).await.unwrap();
    for (proxy, server) in [
        (&phone_proxy, &echo),
        (&laptop_proxy, &echo),
        (&phone_proxy, &time),
    ] {
        let init = rpc(
            proxy,
            server,
            json!({ "jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": { "protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1" } } }),
        )
        .await;
        assert!(init["result"]["serverInfo"].is_object(), "{init}");
    }
    let tools = rpc(
        &laptop_proxy,
        &everything,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(names.contains(&"echo"), "{names:?}");

    //two devices, same request ids, at once
    let (from_phone, from_laptop, everything_echo, now) = tokio::join!(
        call_tool(&phone_proxy, &echo, 1, "echo", json!({ "text": "phone" })),
        call_tool(&laptop_proxy, &echo, 1, "echo", json!({ "text": "laptop" })),
        call_tool(
            &laptop_proxy,
            &everything,
            2,
            "echo",
            json!({ "message": "hello" })
        ),
        call_tool(
            &phone_proxy,
            &time,
            3,
            "get_current_time",
            json!({ "timezone": "Europe/Paris" })
        ),
    );
    assert_eq!(from_phone, "hub says phone");
    assert_eq!(from_laptop, "hub says laptop");
    assert!(everything_echo.contains("hello"), "{everything_echo}");
    assert!(now.contains("Europe/Paris"), "{now}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
