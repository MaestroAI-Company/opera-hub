//browser bindings, relays only
use std::sync::Arc;

use js_sys::{Promise, Uint8Array};
use opera_hub_proto::ForwardHead;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::{Forwarded, HubClient, HubError, McpGrant, PairedHub, SyncRecord};

const CHUNK_BYTES: usize = 16 * 1024;

#[wasm_bindgen]
pub struct WebHubClient {
    inner: Arc<HubClient>,
}

enum Body {
    Stream(iroh::endpoint::RecvStream),
    //the hub explains a refused request
    Text(Vec<u8>),
    Done,
}

//a provider answer, the body is read chunk by chunk
#[wasm_bindgen]
pub struct WebResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Arc<Mutex<Body>>,
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(JsValue::from)
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(JsValue::from)
}

fn hub_error(error: HubError) -> JsValue {
    to_js(&error).unwrap_or_else(|e| e)
}

//rust futures become js promises
fn promise<T, F>(future: F) -> Promise
where
    T: Serialize,
    F: Future<Output = Result<T, HubError>> + 'static,
{
    future_to_promise(async move { to_js(&future.await.map_err(hub_error)?) })
}

#[wasm_bindgen]
impl WebHubClient {
    #[wasm_bindgen(constructor)]
    pub fn new(secret_key: Vec<u8>) -> Result<WebHubClient, JsValue> {
        Ok(Self {
            inner: HubClient::new(secret_key).map_err(hub_error)?,
        })
    }

    #[wasm_bindgen(js_name = deviceId)]
    pub fn device_id(&self) -> String {
        self.inner.device_id()
    }

    pub fn pair(&self, link: String, device_name: String, kind: String) -> Promise {
        let inner = self.inner.clone();
        promise(async move { inner.pair(link, device_name, kind).await })
    }

    pub fn status(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.status(hub).await }))
    }

    pub fn unpair(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.unpair(hub).await }))
    }

    pub fn devices(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.devices(hub).await }))
    }

    pub fn push(
        &self,
        hub: JsValue,
        collection: String,
        records: JsValue,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let records = from_js::<Vec<SyncRecord>>(records)?;
        Ok(promise(async move {
            inner.push(hub, collection, records).await
        }))
    }

    pub fn pull(&self, hub: JsValue, collection: String, since: f64) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.pull(hub, collection, since as u64).await
        }))
    }

    pub fn watch(&self, hub: JsValue, since: f64) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.watch(hub, since as u64).await }))
    }

    #[wasm_bindgen(js_name = putGrant)]
    pub fn put_grant(&self, hub: JsValue, grant: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let grant = from_js::<McpGrant>(grant)?;
        Ok(promise(async move { inner.put_grant(hub, grant).await }))
    }

    #[wasm_bindgen(js_name = accessToken)]
    pub fn access_token(&self, hub: JsValue, server_id: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.access_token(hub, server_id).await },
        ))
    }

    #[wasm_bindgen(js_name = dropGrant)]
    pub fn drop_grant(&self, hub: JsValue, server_id: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.drop_grant(hub, server_id).await },
        ))
    }

    pub fn providers(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.providers(hub).await }))
    }

    //no local proxy in a browser
    pub fn forward(
        &self,
        hub: JsValue,
        provider: String,
        method: String,
        path: String,
        headers: JsValue,
        body: Vec<u8>,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let head = ForwardHead {
            provider,
            method,
            path,
            headers: from_js(headers)?,
        };
        Ok(future_to_promise(async move {
            match inner.forward(&hub, head, &body).await.map_err(hub_error)? {
                Forwarded::Response {
                    status,
                    headers,
                    body,
                } => Ok(WebResponse {
                    status,
                    headers,
                    body: Arc::new(Mutex::new(Body::Stream(body))),
                }
                .into()),
                Forwarded::Refused(message) => Ok(WebResponse {
                    status: 502,
                    headers: vec![("content-type".into(), "text/plain".into())],
                    body: Arc::new(Mutex::new(Body::Text(message.into_bytes()))),
                }
                .into()),
            }
        }))
    }
}

#[wasm_bindgen]
impl WebResponse {
    #[wasm_bindgen(getter)]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[wasm_bindgen(getter)]
    pub fn headers(&self) -> Result<JsValue, JsValue> {
        to_js(&self.headers)
    }

    //undefined once the body ended
    pub fn read(&self) -> Promise {
        let body = self.body.clone();
        future_to_promise(async move {
            let mut guard = body.lock().await;
            let stream = match &mut *guard {
                Body::Stream(stream) => stream,
                Body::Text(text) => {
                    let chunk = Uint8Array::from(text.as_slice());
                    *guard = Body::Done;
                    return Ok(chunk.into());
                }
                Body::Done => return Ok(JsValue::UNDEFINED),
            };
            let mut buf = vec![0u8; CHUNK_BYTES];
            let read = stream.read(&mut buf).await;
            match read {
                Ok(Some(n)) => Ok(Uint8Array::from(&buf[..n]).into()),
                Ok(None) => {
                    *guard = Body::Done;
                    Ok(JsValue::UNDEFINED)
                }
                Err(e) => {
                    *guard = Body::Done;
                    Err(hub_error(HubError::Unreachable(e.to_string())))
                }
            }
        })
    }

    //a closed reader stops the provider
    pub fn cancel(&self) {
        if let Ok(mut guard) = self.body.try_lock()
            && let Body::Stream(mut stream) = std::mem::replace(&mut *guard, Body::Done)
        {
            let _ = stream.stop(0u32.into());
        }
    }
}
