pub mod bark;
pub mod web_push;

use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use futures::future::join_all;

use crate::env;

#[derive(Default, Debug, Clone)]
pub struct NotificationRequest {
    pub id: i64,
    pub body: Option<String>,
    pub title: Option<String>,
    pub icon: Option<String>,
    pub url: Option<String>,
    pub room_id: Option<i64>,
    pub image: Option<String>,
}

#[derive(Debug, Default)]
pub struct NotificationResponse {
    pub success_count: u32,
    pub failure_count: u32,
}

impl Display for NotificationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Success: {}, failed: {}",
            self.success_count, self.failure_count
        )
    }
}

pub type NotificationFuture<'a> = Pin<Box<dyn Future<Output = NotificationResponse> + Send + 'a>>;

pub trait NotificationService {
    fn send_notification(&self, request: NotificationRequest) -> NotificationFuture<'_>;
}

static NOTIFICATION_SERVICES: OnceLock<
    Vec<Box<dyn crate::service::NotificationService + Send + Sync>>,
> = OnceLock::new();

pub async fn init() -> Result<(), String> {
    let config = env::get();
    let bark_service = bark::BarkService::new(config);
    let web_push_service = web_push::WebPushService::new(config).await;
    let services: Vec<Box<dyn crate::service::NotificationService + Send + Sync>> =
        vec![Box::new(bark_service), Box::new(web_push_service)];
    NOTIFICATION_SERVICES
        .set(services)
        .map_err(|_| "通知服务已经初始化".to_string())
}

pub async fn send_notification(
    request: NotificationRequest,
) -> Result<NotificationResponse, (NotificationResponse, String)> {
    // let tasks: Vec<_> = NOTIFICATION_SERVICES
    //     .get()?
    //     .into_iter()
    //     .map(|s| {
    //         let value = request.clone();
    //         tokio::task::spawn(async move { s.send_notification(value).await })
    //     })
    //     .collect();

    match NOTIFICATION_SERVICES.get() {
        Some(services) => {
            let tasks: Vec<_> = services
                .into_iter()
                .map(|s| {
                    let value = request.clone();
                    tokio::task::spawn(async move { s.send_notification(value).await })
                })
                .collect();
            let results: Vec<NotificationResponse> = join_all(tasks)
                .await
                .into_iter()
                .filter_map(|r| r.ok())
                .collect();
            let mut response = NotificationResponse {
                ..Default::default()
            };
            for result in results {
                response.success_count += result.success_count;
                response.failure_count += result.failure_count;
            }
            Ok(response)
        }
        None => Err((
            NotificationResponse {
                ..Default::default()
            },
            "No services available".to_string(),
        )),
    }
}
