//! 通知发送模块 (ez_notify)
//!
//! 对应 Python reference 中的 `lib/ez_notify.py`

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 发送开播通知
pub async fn send(
    client: &reqwest::Client,
    id: i64,
    roomid: Option<i64>,
    body: Option<&str>,
    title: Option<&str>,
    icon: Option<&str>,
    url: Option<&str>,
) -> Result<serde_json::Value> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

    let mut send_data = serde_json::json!({
        "id": id,
        "timestamp": timestamp,
    });

    if let Some(v) = roomid {
        send_data["roomid"] = serde_json::json!(v);
    }
    if let Some(v) = body {
        send_data["body"] = serde_json::json!(v);
    }
    if let Some(v) = title {
        send_data["title"] = serde_json::json!(v);
    }
    if let Some(v) = icon {
        send_data["icon"] = serde_json::json!(v);
    }
    if let Some(v) = url {
        send_data["url"] = serde_json::json!(v);
    }

    let send_body = serde_json::to_vec(&send_data)?;

    let config = crate::env::get();

    let mut mac = HmacSha256::new_from_slice(config.notify_sign_key.as_bytes())?;
    mac.update(&send_body);
    let signature = hex::encode(mac.finalize().into_bytes());

    let resp: serde_json::Value = client
        .post(format!(
            "{}/post",
            config.notify_worker_url.trim_end_matches('/')
        ))
        .header("X-Signature", signature)
        .header("Content-Type", "application/json")
        .body(send_body)
        .send()
        .await?
        .json()
        .await?;

    Ok(resp)
}
