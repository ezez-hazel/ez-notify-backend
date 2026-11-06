//! Bark/APNs high-concurrency push example.
//!
//! The caller must provide an APNs provider token key (`.p8`), team ID, key ID,
//! and the Bark app's bundle ID as the APNs topic.
//!
//! ```rust,ignore
//! # use ez_notify_backend::service::bark::{BarkClient, PushRequest};
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let client = BarkClient::from_token_file(
//!     "AuthKey_ABC123.p8",
//!     "ABC123",
//!     "TEAM123",
//!     "com.example.bark",
//!     64,
//! )?;
//! let results = client
//!     .push_many(vec![PushRequest {
//!         device_token: "device-token".to_owned(),
//!         title: "直播提醒".to_owned(),
//!         body: "主播正在直播".to_owned(),
//!     }])
//!     .await?;
//! # assert_eq!(results.len(), 1);
//! # Ok(())
//! # }
//! ```

use std::{io::Cursor, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use apns_h2::{
    Client, ClientConfig, CollapseId, DefaultNotificationBuilder, Endpoint,
    request::notification::{NotificationBuilder, NotificationOptions, PushType},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::error;

use crate::{cf, env, service::NotificationService};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum BarkLevel {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "timeSensitive")]
    TimeSensitive,
    #[serde(rename = "passive")]
    Passive,
    #[serde(rename = "critical")]
    Critical,
}

#[derive(Default)]
struct BarkTarget {
    pub device_key: String,
    pub device_token: Option<String>,
    pub config: Option<serde_json::Value>,
    pub level: Option<BarkLevel>,
    pub sound: Option<String>,
    pub volume: Option<i32>,
}

impl BarkTarget {
    pub fn extract_config(&mut self) {
        if let Some(config) = self.config.clone() {
            self.level = Some(
                if let Some(level) = config.get("level").cloned().and_then(|l| l.as_i64()) {
                    match level {
                        1 => BarkLevel::TimeSensitive,
                        2 => BarkLevel::Passive,
                        3 => BarkLevel::Critical,
                        _ => BarkLevel::Active,
                    }
                } else {
                    BarkLevel::Active
                },
            );
            self.sound = config
                .get("sound")
                .cloned()
                .and_then(|x| x.as_str().map(|s| s.to_string()));
            self.volume = Some(
                config
                    .get("volume")
                    .cloned()
                    .and_then(|x| x.as_i64().and_then(|n| n.try_into().ok()))
                    .unwrap_or(5),
            );
        };
    }
}

struct BarkDatabase {
    account_id: String,
    database_id: String,
    bark_database_id: String,
    api_token: String,
}

impl BarkDatabase {
    pub async fn get_subscriptions(&self, id: &str) -> Option<Vec<BarkTarget>> {
        match cf::d1_query(
            &self.account_id,
            &self.database_id,
            &self.api_token,
            &format!(
                "\
        SELECT key, subscriptions
        FROM notify_bark_configs
        WHERE revoked = 0
          AND EXISTS (
          SELECT 1 FROM json_each(subscriptions) WHERE json_each.key = \"{id}\"
        )
            "
            ),
        )
        .await
        {
            Ok(rows) => Some(
                rows.iter()
                    .filter_map(|row| {
                        Some(BarkTarget {
                            device_key: row.get("key")?.as_str()?.to_owned(),
                            config: serde_json::from_str::<serde_json::Value>(
                                &row.get("subscriptions")?.as_str()?,
                            )
                            .ok()?
                            .get(id)
                            .cloned(),
                            ..Default::default()
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

    pub async fn device_key_to_token(&self, key: &str) -> Option<String> {
        let rows = cf::d1_query(
            &self.account_id,
            &self.bark_database_id,
            &self.api_token,
            &format!("SELECT token FROM devices WHERE key = \"{key}\""),
        )
        .await
        .ok()?;

        if rows.len() > 0 {
            Some(rows.get(0)?.get("token")?.as_str()?.to_owned())
        } else {
            None
        }
    }

    pub async fn revoke_key(&self, key: &str) -> Result<Vec<Value>> {
        cf::d1_query(
            &self.account_id,
            &self.database_id,
            &self.api_token,
            &format!("UPDATE notify_bark_configs SET revoked = 1, updated_at = CURRENT_TIMESTAMP WHERE key = \"{key}\""),
        )
        .await
    }
}

/// A notification to be sent to one Bark device.
#[derive(Debug, Default, Clone)]
struct ApnsPushRequest {
    pub device_key: String,
    pub device_token: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub body: Option<String>,
    pub sound: Option<String>,
    pub group: Option<String>,
    pub call: Option<bool>,
    pub icon: Option<String>,
    pub level: Option<BarkLevel>,
    pub volume: Option<i32>,
    pub url: Option<String>,
    pub image: Option<String>,
    // pub copy: Option<String>,
    pub id: Option<String>,
    pub delete: Option<bool>,
    // pub markdown: Option<String>,
    pub ttl: Option<i64>,
}

/// The result of sending one notification.
#[derive(Debug)]
struct PushResult {
    pub device_key: String,
    // pub device_token: String,
    pub status_code: Option<u16>,
    // pub apns_id: Option<String>,
    // pub error: Option<String>,
}

/// A reusable APNs client with a bounded number of in-flight requests.
#[derive(Clone, Debug)]
struct ApnsClient {
    client: Client,
    topic: Option<String>,
    concurrency: usize,
}

impl ApnsClient {
    /// Create a client using an APNs provider token (`.p8`) and the production
    /// endpoint. `concurrency` limits in-flight pushes without creating extra
    /// HTTP/2 connections.
    pub fn from_token_str(
        key: impl Into<String>,
        key_id: impl Into<String>,
        team_id: impl Into<String>,
        topic: impl Into<String>,
        concurrency: usize,
    ) -> Result<Self> {
        if concurrency == 0 {
            bail!("push concurrency must be greater than zero");
        }

        let mut key_file = Cursor::new(key.into().clone().into_bytes());
        let client = Client::token(
            &mut key_file,
            key_id,
            team_id,
            ClientConfig {
                endpoint: Endpoint::Production,
                request_timeout: Some(Duration::from_secs(20)),
                pool_idle_timeout: Some(Duration::from_secs(60)),
                ..ClientConfig::default()
            },
        )?;

        Ok(Self {
            client,
            topic: Some(topic.into()),
            concurrency,
        })
    }

    /// Send all requests concurrently while preserving the input order in the
    /// returned result vector.
    pub async fn push_many(&self, requests: Vec<ApnsPushRequest>) -> Result<Vec<PushResult>> {
        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        let mut tasks = JoinSet::new();

        for (index, request) in requests.into_iter().enumerate() {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = self.client.clone();
            let topic = self.topic.clone().unwrap_or_default();

            tasks.spawn(async move {
                let result = send_one(&client, &topic, request).await;
                drop(permit);
                (index, result)
            });
        }

        let mut results = Vec::new();
        while let Some(task) = tasks.join_next().await {
            let (index, result) = task?;
            results.push((index, result));
        }
        results.sort_unstable_by_key(|(index, _)| *index);

        Ok(results.into_iter().map(|(_, result)| result).collect())
    }
}

async fn send_one(client: &Client, topic: &str, request: ApnsPushRequest) -> PushResult {
    let device_key = request.device_key.clone();
    let device_token = request.device_token.clone();
    let sound = if let Some(sound) = request.sound {
        if !sound.ends_with(".caf") {
            format!("{sound}.caf")
        } else {
            sound
        }
    } else {
        "1107".to_string()
    };

    let builder = if request.delete.unwrap_or(false) {
        DefaultNotificationBuilder::new()
            .content_available()
            .mutable_content()
    } else {
        let mut builder = DefaultNotificationBuilder::new()
            .mutable_content()
            .sound(sound)
            .category("myNotificationCategory");

        if let Some(title) = request.title.clone() {
            builder = builder.title(title);
        };

        if let Some(subtitle) = request.subtitle.clone() {
            builder = builder.subtitle(subtitle);
        };

        if let Some(body) = request.body.clone() {
            builder = builder.body(body);
        };

        if !request.title.is_some() && !request.subtitle.is_some() && !request.body.is_some() {
            builder = builder.body("Empty Message");
        }

        if let Some(group) = request.group.clone() {
            builder = builder.thread_id(group);
        }

        builder
    };

    let mut payload = builder.build(
        device_token,
        NotificationOptions {
            apns_topic: Some(topic),
            apns_collapse_id: request
                .id
                .as_deref()
                .and_then(|id| CollapseId::new(id).ok()),
            apns_push_type: if request.delete.unwrap_or(false) {
                Some(PushType::Background)
            } else {
                Some(PushType::Alert)
            },
            ..Default::default()
        },
    );

    request
        .group
        .and_then(|x| payload.add_custom_data("group", &x).ok());
    if request.call.unwrap_or(false) {
        payload.add_custom_data("call", &"1").ok();
    }
    request
        .icon
        .and_then(|x| payload.add_custom_data("icon", &x).ok());
    request
        .level
        .and_then(|x| payload.add_custom_data("level", &x).ok());
    request
        .volume
        .and_then(|x| payload.add_custom_data("volume", &format!("{x}")).ok());
    request
        .url
        .and_then(|x| payload.add_custom_data("url", &x).ok());
    request
        .image
        .and_then(|x| payload.add_custom_data("image", &x).ok());
    request
        .id
        .clone()
        .and_then(|x| payload.add_custom_data("id", &x).ok());
    if request.delete.unwrap_or(false) {
        payload.add_custom_data("delete", &"1").ok();
    }
    request
        .ttl
        .and_then(|x| payload.add_custom_data("ttl", &x).ok());

    match client.send(payload).await {
        Ok(response) => PushResult {
            device_key,
            // device_token,
            status_code: Some(response.code),
            // apns_id: response.apns_id,
            // error: response.error.map(|error| format!("{error:?}")),
        },
        Err(_error) => PushResult {
            device_key,
            // device_token,
            status_code: None,
            // apns_id: None,
            // error: Some(error.to_string()),
        },
    }
}

pub struct BarkService {
    client: ApnsClient,
    database: BarkDatabase,
}

impl NotificationService for BarkService {
    fn send_notification(
        &self,
        request: super::NotificationRequest,
    ) -> super::NotificationFuture<'_> {
        Box::pin(async move {
            let targets = self
                .database
                .get_subscriptions(&request.id.to_string())
                .await;
            if let Some(mut targets) = targets {
                for target in &mut targets {
                    target.device_token =
                        self.database.device_key_to_token(&target.device_key).await;
                    target.extract_config();
                }

                let mut failures = Vec::new();
                let push_requests: Vec<ApnsPushRequest> = targets
                    .into_iter()
                    .filter_map(|target| {
                        let device_token = target.device_token.unwrap_or_default();
                        if device_token.is_empty() || device_token == "deleted" {
                            failures.push(target.device_key);
                            return None;
                        }

                        Some(ApnsPushRequest {
                            device_key: target.device_key,
                            device_token,
                            title: request.title.clone(),
                            body: request.body.clone(),
                            sound: target.sound.clone(),
                            call: target
                                .level
                                .clone()
                                .map(|level| matches!(level, BarkLevel::Critical)),
                            icon: request.icon.clone(),
                            level: target.level.clone(),
                            volume: target.volume,
                            url: Some(format!(
                                "bilibili://live/{}",
                                request.room_id.unwrap_or_default()
                            )),
                            image: request.image.clone(),
                            ..Default::default()
                        })
                    })
                    .collect();

                let push_device_keys: Vec<String> = push_requests
                    .iter()
                    .map(|request| request.device_key.clone())
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
                    .filter(|r| r.status_code == Some(200))
                    .count() as u32;
                failures.extend(
                    results
                        .iter()
                        .filter(|result| result.status_code != Some(200))
                        .map(|result| result.device_key.clone()),
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

impl BarkService {
    pub fn new(config: &env::AppConfig) -> Self {
        let client = ApnsClient::from_token_str(
            "-----BEGIN PRIVATE KEY-----\nMIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQg4vtC3g5L5HgKGJ2+T1eA0tOivREvEAY2g+juRXJkYL2gCgYIKoZIzj0DAQehRANCAASmOs3JkSyoGEWZsUGxFs/4pw1rIlSV2IC19M8u3G5kq36upOwyFWj9Gi3Ejc9d3sC7+SHRqXrEAJow8/7tRpV+\n-----END PRIVATE KEY-----",
            "LH4T9V5U4R",
            "5U8LBRXG3A",
            "me.fin.bark",
            64,
        ).expect("Failed to create APNs client");
        let database = BarkDatabase {
            account_id: config.cloudflare_account_id.clone(),
            database_id: config.notify_d1_database_id.clone(),
            bark_database_id: config.bark_d1_database_id.clone(),
            api_token: config.cloudflare_api_token.clone(),
        };
        Self { client, database }
    }
}
