use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{error};
use web_push_native::{Auth, WebPushBuilder, jwt_simple::algorithms::ES256KeyPair, p256};

use crate::{cf, env, service::NotificationService};

#[derive(Default)]
struct WebPushTarget {
    pub key: String,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

struct WebPushDatabase {
    pub account_id: String,
    pub database_id: String,
    pub api_token: String,
}

impl WebPushDatabase {
    pub async fn get_subscriptions(&self, id: &str) -> Option<Vec<WebPushTarget>> {
        match cf::d1_query(
            &self.account_id,
            &self.database_id,
            &self.api_token,
            &format!(
                "\
        SELECT s.key, s.endpoint, s.p256dh, s.auth
        FROM notify_subscriptions s
        JOIN notify_configs c ON c.key = s.key
        AND c.revoked = 0
        WHERE EXISTS (
          SELECT 1 FROM json_each(c.subscriptions) WHERE json_each.key = \"{id}\"
        )
            "
            ),
        )
        .await
        {
            Ok(rows) => Some(
                rows.iter()
                    .filter_map(|row| {
                        Some(WebPushTarget {
                            key: row.get("key")?.as_str()?.to_owned(),
                            endpoint: row.get("endpoint")?.as_str()?.to_owned(),
                            p256dh: row.get("p256dh")?.as_str()?.to_owned(),
                            auth: row.get("auth")?.as_str()?.to_owned(),
                        })
                    })
                    .collect(),
            ),
            Err(x) => {
                error!("Get subscriptions error: {x}");
                None
            }
        }
    }

    pub async fn get_vapid_keys(&self) -> Result<(String, String)> {
        let rows = cf::d1_query(
            &self.account_id,
            &self.database_id,
            &self.api_token,
            "SELECT public_key, private_key FROM notify_keys WHERE id = 1",
        )
        .await
        .context("Failed to query VAPID keys from D1")?;

        if let Some(row) = rows.first() {
            let private_key = row
                .get("private_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing vapid_private_key in D1 row"))?
                .to_string();
            let public_key = row
                .get("public_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing vapid_public_key in D1 row"))?
                .to_string();
            Ok((private_key, public_key))
        } else {
            bail!("No VAPID keys found in D1 database");
        }
    }

    pub async fn revoke_key(&self, key: &str) -> Result<Vec<Value>> {
        cf::d1_query(
            &self.account_id,
            &self.database_id,
            &self.api_token,
            &format!("UPDATE notify_configs SET revoked = 1, updated_at = CURRENT_TIMESTAMP WHERE key = \"{key}\""),
        )
        .await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebPushRequest {
    pub key: String,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub url: Option<String>,
    pub icon: Option<String>,
}

struct PushResult {
    pub key: String,
    pub status_code: Option<u16>,
    // pub error: Option<String>,
}

struct WebPushClient {
    client: reqwest::Client,
    vapid_private_key: String,
    vapid_subject: String,
}

impl WebPushClient {
    pub async fn push_many(&self, requests: Vec<WebPushRequest>) -> Result<Vec<PushResult>> {
        Ok(
            futures::future::join_all(requests.into_iter().map(|request| self.send_one(request)))
                .await,
        )
    }

    async fn send_one(&self, request: WebPushRequest) -> PushResult {
        let result = (|| -> Result<http::Request<Vec<u8>>> {
            let endpoint = request.endpoint.parse()?;
            let p256dh = URL_SAFE_NO_PAD.decode(request.p256dh.as_bytes())?;
            let auth = URL_SAFE_NO_PAD.decode(request.auth.as_bytes())?;
            if auth.len() != 16 {
                bail!("Web Push auth secret must be 16 bytes");
            }
            let ua_public = p256::PublicKey::from_sec1_bytes(&p256dh)?;
            let ua_auth = Auth::clone_from_slice(&auth);
            let vapid_key = ES256KeyPair::from_der(
                &URL_SAFE_NO_PAD.decode(self.vapid_private_key.as_bytes())?,
            )?;
            let payload = serde_json::to_vec(&json!({
                "title": request.title,
                "body": request.body,
                "url": request.url,
                "icon": request.icon,
            }))?;
            Ok(WebPushBuilder::new(endpoint, ua_public, ua_auth)
                .with_vapid(&vapid_key, &self.vapid_subject)
                .build(payload)?)
        })();

        let response = match result {
            Ok(push_request) => {
                let mut request_builder = self.client.request(
                    push_request.method().clone(),
                    push_request.uri().to_string(),
                );
                for (name, value) in push_request.headers() {
                    request_builder = request_builder.header(name, value);
                }
                request_builder.body(push_request.into_body()).send().await
            }
            Err(_error) => {
                return PushResult {
                    key: request.key,
                    status_code: None,
                    // error: Some(error.to_string()),
                };
            }
        };

        match response {
            Ok(response) => PushResult {
                key: request.key,
                status_code: Some(response.status().as_u16()),
                // error: None,
            },
            Err(_error) => PushResult {
                key: request.key,
                status_code: None,
                // error: Some(error.to_string()),
            },
        }
    }
}

pub struct WebPushService {
    client: WebPushClient,
    database: WebPushDatabase,
}

impl NotificationService for WebPushService {
    fn send_notification(
        &self,
        request: super::NotificationRequest,
    ) -> super::NotificationFuture<'_> {
        Box::pin(async move {
            let targets = self
                .database
                .get_subscriptions(&request.id.to_string())
                .await;
            if let Some(targets) = targets {
                let mut failures = Vec::new();
                let push_requests: Vec<WebPushRequest> = targets
                    .into_iter()
                    .filter_map(|target| {
                        let endpoint = target.endpoint;
                        let p256dh = target.p256dh;
                        let auth = target.auth;
                        if endpoint.is_empty() || p256dh.is_empty() || auth.is_empty() {
                            failures.push(target.key);
                            return None;
                        }

                        Some(WebPushRequest {
                            key: target.key,
                            endpoint: endpoint,
                            p256dh: p256dh,
                            auth: auth,
                            title: request.title.clone(),
                            body: request.body.clone(),
                            url: request.url.clone(),
                            icon: request.icon.clone(),
                        })
                    })
                    .collect();
                let push_device_keys: Vec<String> = push_requests
                    .iter()
                    .map(|request| request.key.clone())
                    .collect();
                let results = match self.client.push_many(push_requests).await {
                    Ok(results) => results,
                    Err(error) => {
                        eprintln!("Failed to send Bark notifications: {error:#}");
                        failures.extend(push_device_keys);
                        Vec::new()
                    }
                };
                let success_count = results
                    .iter()
                    .filter(|r| {
                        r.status_code
                            .is_some_and(|status| (200..300).contains(&status))
                    })
                    .count() as u32;
                failures.extend(
                    results
                        .iter()
                        .filter(|result| matches!(result.status_code, Some(404 | 410)))
                        .map(|result| result.key.clone()),
                );
                let failure_count = failures.len() as u32;
                for failure in failures {
                    self.database.revoke_key(&failure).await.ok();
                }

                super::NotificationResponse {
                    success_count,
                    failure_count,
                }
            } else {
                super::NotificationResponse {
                    success_count: 0,
                    failure_count: 0,
                }
            }
        })
    }
}

impl WebPushService {
    pub async fn new(config: &env::AppConfig) -> Self {
        let database = WebPushDatabase {
            account_id: config.cloudflare_account_id.clone(),
            database_id: config.notify_d1_database_id.clone(),
            api_token: config.cloudflare_api_token.clone(),
        };
        let (vapid_private_key, _) = database
            .get_vapid_keys()
            .await
            .expect("Failed to get vapid key");
        let client = WebPushClient {
            client: reqwest::Client::new(),
            vapid_private_key,
            vapid_subject: config.web_push_vapid_subject.clone(),
        };
        Self { client, database }
    }
}
