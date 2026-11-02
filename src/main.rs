mod bili;
mod bili_wbi;
mod cf;
mod env;
mod ez_notify;
mod live_message_stream;
mod live_room;

use anyhow::Result;
use std::io::{self, IsTerminal};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    env::init().map_err(anyhow::Error::msg)?;

    let config = env::get();

    if io::stdout().is_terminal() {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_systemd::SystemdLayer::stdout()
                    .with_target(true)
                    .with_thread_ids(true),
            )
            .init();
    }

    // 从 Cloudflare D1 查询 UID 列表
    let rows = cf::d1_query(
        &config.cloudflare_account_id,
        &config.notify_d1_database_id,
        &config.cloudflare_api_token,
        "SELECT id FROM notify_items",
    )
    .await?;

    let uids: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_i64()))
        .filter(|&id| id != 0)
        .collect();

    info!("监听 UID 列表: {:?}", uids);

    if uids.is_empty() {
        info!("没有需要监听的 UID");
        return Ok(());
    }

    // 获取 Bilibili Cookie
    bili::get_cookies().await?;

    // 为每个 UID 启动一个监听任务，断线自动重连
    let mut handles = Vec::new();
    for uid in uids {
        let handle = tokio::spawn(async move {
            loop {
                match run_room(uid).await {
                    Ok(()) => {
                        info!("[{uid}] Room disconnected normally. Reconnecting...");
                    }
                    Err(e) => {
                        error!("[{uid}] Room disconnected due to {e}. Reconnecting...");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
        handles.push(handle);
    }

    // 等待所有任务（实际上它们会永远循环）
    futures::future::join_all(handles).await;

    Ok(())
}

async fn run_room(uid: i64) -> Result<()> {
    let mut room = live_room::LiveRoom::new_with_uid(uid);
    room.init_info().await?;
    info!(
        "主播: {}, 直播间标题: {}, 是否直播中: {} {}",
        room.user_name,
        room.title,
        room.is_live,
        room.live_time.unwrap_or_default().to_rfc3339()
    );
    // 移除默认的 DANMU_MSG 处理器
    room.handlers.remove("DANMU_MSG");
    // 添加 LIVE 事件钩子 —— 开播通知
    room.set_live_hook(true);
    room.init_stream();
    room.start_watching().await
}
