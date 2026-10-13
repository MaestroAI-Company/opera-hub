//needs network for address lookup
use std::sync::Arc;
use std::time::Duration;

use opera_hub_client::{HubClient, HubError, PairedHub, SyncRecord};

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

async fn pair(client: &HubClient, data_dir: &std::path::Path, name: &str) -> PairedHub {
    let link = opera_hub::hub::pairing_link(data_dir).unwrap().to_string();
    //fresh hubs publish their address slowly
    for _ in 0..10 {
        match client.pair(link.clone(), name.into(), "phone".into()).await {
            Err(HubError::Unreachable(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
            result => return result.unwrap(),
        }
    }
    panic!("hub never became reachable");
}

fn record(id: &str, updated_at: u64, payload: String) -> SyncRecord {
    SyncRecord {
        id: id.into(),
        updated_at,
        deleted: false,
        payload: Some(payload),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn data_sync_and_watch() {
    let data_dir = std::env::temp_dir().join(format!("opera-hub-sync-{}", random_hex::<8>()));
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
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    pair(&laptop, &data_dir, "laptop").await;

    //the laptop wakes up as soon as the phone writes
    let watcher = tokio::spawn({
        let laptop = laptop.clone();
        let hub_ref = hub_ref.clone();
        async move { laptop.watch(hub_ref, 0).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    phone
        .push(
            hub_ref.clone(),
            "conversations".into(),
            vec![record("conv", 10, r#"{"name":"hello"}"#.into())],
        )
        .await
        .unwrap();
    let cursor = watcher.await.unwrap().unwrap();
    assert!(cursor > 0);
    let page = laptop
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    //larger than the old 1 mib frame limit
    let image = "a".repeat(3 * 1024 * 1024);
    phone
        .push(
            hub_ref.clone(),
            "blobs".into(),
            vec![record("img", 1, image.clone())],
        )
        .await
        .unwrap();
    let page = laptop
        .pull(hub_ref.clone(), "blobs".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records[0].payload.as_deref(), Some(image.as_str()));

    //the cloud account rides the secrets scope
    phone
        .push(
            hub_ref.clone(),
            "accounts".into(),
            vec![record("cloud", 5, r#"{"provider":"nextcloud"}"#.into())],
        )
        .await
        .unwrap();
    let page = laptop
        .pull(hub_ref.clone(), "accounts".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records[0].id, "cloud");

    //the hub lists the same id
    let devices = laptop.devices(hub_ref.clone()).await.unwrap();
    assert!(devices.iter().any(|d| d.id == phone.device_id()));

    //a browser reads the list without admin rights
    let browser = new_client();
    let link = opera_hub::hub::pairing_link(&data_dir).unwrap().to_string();
    browser
        .pair(link, "browser".into(), "web".into())
        .await
        .unwrap();
    assert_eq!(browser.devices(hub_ref.clone()).await.unwrap().len(), 3);

    let unknown = laptop.pull(hub_ref.clone(), "nope".into(), 0).await;
    assert!(matches!(unknown, Err(HubError::Internal(_))), "{unknown:?}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_by_code() {
    let data_dir = std::env::temp_dir().join(format!("opera-hub-code-{}", random_hex::<8>()));
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

    //a paired phone asks the hub for a code
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    let invite = phone.invite(hub_ref.clone()).await.unwrap();

    //a wrong code never reaches the invite
    let stranger = new_client();
    assert!(
        stranger
            .pair_with_code("0000-0000".into(), "stranger".into(), "web".into())
            .await
            .is_err()
    );

    //typed in lower case with spaces
    let browser = new_client();
    let typed = invite.code.to_lowercase().replace('-', " ");
    let paired = browser
        .pair_with_code(typed, "browser".into(), "web".into())
        .await
        .unwrap();
    assert_eq!(paired.id, hub_ref.id);
    let devices = browser.devices(paired).await.unwrap();
    assert!(devices.iter().any(|d| d.id == browser.device_id()));

    //the code works once
    let late = new_client();
    assert!(
        late.pair_with_code(invite.code, "late".into(), "web".into())
            .await
            .is_err()
    );

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
