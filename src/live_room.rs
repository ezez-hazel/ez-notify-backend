//! 直播间模块
//!
//! 对应 Python reference 中的 `live_room.py`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use anyhow::Result;
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use tracing::{error, info, warn};

use crate::bili;
use crate::ez_notify;
use crate::live_message_stream::{LiveMessageStream, PayloadHandler};

/// 直播间共享上下文，供各个消息处理器访问与修改
#[derive(Clone)]
pub struct LiveRoomContext {
    pub uid: i64,
    pub room_id: Arc<AtomicI64>,
    pub title: Arc<RwLock<String>>,
    pub user_name: Arc<RwLock<String>>,
    pub face: Arc<RwLock<String>>,
    pub is_live: Arc<AtomicBool>,
    pub live_time: Arc<RwLock<Option<DateTime<Local>>>>,
    pub user_cover: Arc<RwLock<String>>,
    pub live_hook_enabled: Arc<AtomicBool>,
}

impl LiveRoomContext {
    pub fn new(uid: i64, room_id: i64) -> Self {
        Self {
            uid,
            room_id: Arc::new(AtomicI64::new(room_id)),
            title: Arc::new(RwLock::new(String::new())),
            user_name: Arc::new(RwLock::new(String::new())),
            face: Arc::new(RwLock::new(String::new())),
            is_live: Arc::new(AtomicBool::new(false)),
            live_time: Arc::new(RwLock::new(None)),
            user_cover: Arc::new(RwLock::new(String::new())),
            live_hook_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn get_title(&self) -> String {
        self.title.read().unwrap().clone()
    }

    pub fn get_user_name(&self) -> String {
        self.user_name.read().unwrap().clone()
    }

    pub fn get_face(&self) -> String {
        self.face.read().unwrap().clone()
    }

    pub fn get_live_time(&self) -> Option<DateTime<Local>> {
        self.live_time.read().unwrap().clone()
    }

    pub fn _get_user_cover(&self) -> String {
        self.user_cover.read().unwrap().clone()
    }

    #[allow(dead_code)]
    pub fn is_live(&self) -> bool {
        self.is_live.load(Ordering::Relaxed)
    }

    pub fn get_room_id(&self) -> i64 {
        self.room_id.load(Ordering::Relaxed)
    }
}

/// 消息处理器类型
pub type RoomHandler = Arc<dyn Fn(&LiveRoomContext, &serde_json::Value) + Send + Sync>;

/// 直播间
pub struct LiveRoom {
    pub uid: i64,
    pub user_name: String,
    pub face: String,
    pub room_id: i64,
    pub is_live: bool,
    pub title: String,
    pub user_cover: String,
    pub live_time: Option<DateTime<Local>>,

    pub handlers: HashMap<String, RoomHandler>,
    is_live_initialized: bool,
    live_hook_enabled: bool,

    message_stream: Option<LiveMessageStream>,
    pub ctx: LiveRoomContext,
}

impl LiveRoom {
    pub fn new_with_uid(uid: i64) -> Self {
        assert!(uid != 0, "uid 不能为 0");
        let ctx = LiveRoomContext::new(uid, 0);
        let handlers = Self::default_handlers();

        Self {
            uid,
            user_name: String::new(),
            face: String::new(),
            room_id: 0,
            is_live: false,
            title: String::new(),
            user_cover: String::new(),
            live_time: None,
            handlers,
            is_live_initialized: false,
            live_hook_enabled: false,
            message_stream: None,
            ctx,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_room_id(room_id: i64) -> Self {
        assert!(room_id != 0, "room_id 不能为 0");
        let ctx = LiveRoomContext::new(0, room_id);
        let handlers = Self::default_handlers();

        Self {
            uid: 0,
            user_name: String::new(),
            face: String::new(),
            room_id,
            is_live: false,
            title: String::new(),
            user_cover: String::new(),
            live_time: None,
            handlers,
            is_live_initialized: false,
            live_hook_enabled: false,
            message_stream: None,
            ctx,
        }
    }

    /// 默认注册的事件处理器
    pub fn default_handlers() -> HashMap<String, RoomHandler> {
        let mut handlers: HashMap<String, RoomHandler> = HashMap::new();
        handlers.insert(
            "ROOM_CHANGE".to_string(),
            Arc::new(Self::handle_room_change),
        );
        handlers.insert("LIVE".to_string(), Arc::new(Self::handle_live));
        handlers.insert("PREPARING".to_string(), Arc::new(Self::handle_preparing));
        handlers.insert("DANMU_MSG".to_string(), Arc::new(Self::handle_danmu_msg));
        handlers
    }

    /// 启用 LIVE 事件钩子（开播通知）
    pub fn set_live_hook(&mut self, enabled: bool) {
        self.live_hook_enabled = enabled;
        self.ctx.live_hook_enabled.store(enabled, Ordering::Relaxed);
    }

    /// 同步当前 LiveRoom 状态到 ctx
    fn sync_to_ctx(&self) {
        self.ctx.room_id.store(self.room_id, Ordering::Relaxed);
        self.ctx.is_live.store(self.is_live, Ordering::Relaxed);
        self.ctx
            .live_hook_enabled
            .store(self.live_hook_enabled, Ordering::Relaxed);
        if let Ok(mut t) = self.ctx.title.write() {
            *t = self.title.clone();
        }
        if let Ok(mut u) = self.ctx.user_name.write() {
            *u = self.user_name.clone();
        }
        if let Ok(mut f) = self.ctx.face.write() {
            *f = self.face.clone();
        }
        if let Ok(mut f) = self.ctx.user_cover.write() {
            *f = self.user_cover.clone();
        }
        if let Ok(mut f) = self.ctx.live_time.write() {
            *f = self.live_time.clone();
        }
    }

    /// 初始化直播间和主播信息
    pub async fn init_info(&mut self) -> Result<()> {
        let client = reqwest::Client::new();

        if self.room_id != 0 && self.uid != 0 {
            let (user_info, room_info) = tokio::try_join!(
                bili::master_info(&client, self.uid),
                bili::room_get_info(&client, self.room_id),
            )?;
            self.apply_user_info(&user_info);
            self.apply_room_info(&room_info);
        } else if self.room_id != 0 {
            let room_info = bili::room_get_info(&client, self.room_id).await?;
            self.uid = room_info["uid"].as_i64().unwrap_or(0);
            let user_info = bili::master_info(&client, self.uid).await?;
            self.apply_user_info(&user_info);
            self.apply_room_info(&room_info);
        } else if self.uid != 0 {
            let user_info = bili::master_info(&client, self.uid).await?;
            self.room_id = user_info["room_id"].as_i64().unwrap_or(0);
            let room_info = bili::room_get_info(&client, self.room_id).await?;
            self.apply_user_info(&user_info);
            self.apply_room_info(&room_info);
        }

        self.is_live_initialized = true;
        self.sync_to_ctx();
        Ok(())
    }

    fn apply_user_info(&mut self, user_info: &serde_json::Value) {
        self.user_name = user_info["info"]["uname"]
            .as_str()
            .unwrap_or("")
            .to_string();
        self.face = user_info["info"]["face"].as_str().unwrap_or("").to_string();
    }

    fn apply_room_info(&mut self, room_info: &serde_json::Value) {
        self.title = room_info["title"].as_str().unwrap_or("").to_string();
        self.user_cover = room_info["user_cover"].as_str().unwrap_or("").to_string();

        let was_live = self.is_live;
        self.is_live = room_info["live_status"].as_i64() == Some(1);

        if self.is_live {
            if let Some(live_time_str) = room_info["live_time"].as_str() {
                if let Ok(ndt) = NaiveDateTime::parse_from_str(live_time_str, "%Y-%m-%d %H:%M:%S") {
                    self.live_time =
                        Some(ndt.and_local_timezone(Local).single().unwrap_or_default());
                }
            }
        } else {
            self.live_time = None;
        }

        if self.is_live && self.is_live_initialized && !was_live {
            info!(
                "[{}] 直播开始（{}）",
                self.uid,
                self.live_time.unwrap_or_default().to_rfc3339()
            );
        }
    }

    // ==================== 单独事件处理器函数 ====================

    /// LIVE 事件处理器：开播状态切换与通知发送
    pub fn handle_live(ctx: &LiveRoomContext, payload_json: &serde_json::Value) {
        if let Some(live_time) = payload_json.get("live_time").and_then(|v| v.as_i64()) {
            let was_live = ctx.is_live.swap(true, Ordering::Relaxed);
            if let Ok(mut ctx_live_time) = ctx.live_time.write() {
                if let Some(dt) = DateTime::<Utc>::from_timestamp_secs(live_time) {
                    *ctx_live_time = Some(dt.with_timezone(&Local));
                }
                // *ctx_live_time = Some(DateTime::<Utc>::from_timestamp_secs(live_time).unwrap().with_timezone(&Local));
            }
            info!(
                "[{}] 直播开始 ({})",
                ctx.uid,
                ctx.get_live_time().unwrap().to_rfc3339()
            );

            // LIVE 钩子：发送开播通知
            if ctx.live_hook_enabled.load(Ordering::Relaxed) && !was_live {
                let title_val = ctx.get_title();
                let user_name = ctx.get_user_name();
                let face = ctx.get_face();
                let room_id = ctx.get_room_id();
                let uid = ctx.uid;
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    match ez_notify::send(
                        &client,
                        uid,
                        Some(room_id),
                        Some(&format!("正在直播：{title_val}")),
                        Some(&user_name),
                        Some(&face),
                        Some(&format!("https://live.bilibili.com/{room_id}")),
                    )
                    .await
                    {
                        Ok(resp) => info!("[{uid}] 通知发送成功: {resp}"),
                        Err(e) => error!("[{uid}] 通知发送失败: {e}"),
                    }
                });
            }
        }
    }

    /// PREPARING 事件处理器：下播状态切换
    pub fn handle_preparing(ctx: &LiveRoomContext, _payload_json: &serde_json::Value) {
        ctx.is_live.store(false, Ordering::Relaxed);
        info!("[{}] 直播结束", ctx.uid);
    }

    /// ROOM_CHANGE 事件处理器：直播间标题变更
    pub fn handle_room_change(ctx: &LiveRoomContext, payload_json: &serde_json::Value) {
        if let Some(new_title) = payload_json
            .get("data")
            .and_then(|d| d.get("title"))
            .and_then(|t| t.as_str())
        {
            if let Ok(mut title) = ctx.title.write() {
                *title = new_title.to_string();
            }
            info!("[{}] 直播间标题更改：{new_title}", ctx.uid);
        }
    }

    /// DANMU_MSG 事件处理器：弹幕打印
    pub fn handle_danmu_msg(ctx: &LiveRoomContext, payload_json: &serde_json::Value) {
        if let Some(info) = payload_json.get("info").and_then(|v| v.as_array()) {
            if info.len() > 2 {
                let content = info.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let sender = info
                    .get(2)
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.get(1))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let sender_uid = info
                    .get(2)
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.get(0))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                info!("[{}] {sender}({sender_uid}): {content}", ctx.uid);
            }
        }
    }

    /// 初始化消息流
    pub fn init_stream(&mut self) {
        let mut stream = LiveMessageStream::new(self.room_id);

        self.sync_to_ctx();
        let ctx = self.ctx.clone();
        let handlers = self.handlers.clone();

        let handler: PayloadHandler = Box::new(move |payload_json: &serde_json::Value| {
            let cmd = payload_json
                .get("cmd")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if cmd.is_empty() {
                return;
            }

            match handlers.get(cmd) {
                Some(h) => h(&ctx, payload_json),
                None => {}
            }
        });

        stream.set_handler(handler);
        self.message_stream = Some(stream);
    }

    /// 开始监听，断线自动重试
    pub async fn start_watching(&self) -> Result<()> {
        let stream = self
            .message_stream
            .as_ref()
            .expect("message_stream not initialized, call init_stream() first");

        let retry_delay = std::time::Duration::from_secs(5);
        loop {
            match stream.run().await {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "WebSocket disconnected for {e}, fallback to poll and retry connect in {} second(s)...",
                        retry_delay.as_secs()
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handlers_initialization_and_removal() {
        let mut room = LiveRoom::new_with_uid(12345);
        assert!(room.handlers.contains_key("ROOM_CHANGE"));
        assert!(room.handlers.contains_key("LIVE"));
        assert!(room.handlers.contains_key("PREPARING"));
        assert!(room.handlers.contains_key("DANMU_MSG"));

        // 测试移除 DANMU_MSG 处理器
        let removed = room.handlers.remove("DANMU_MSG");
        assert!(removed.is_some());
        assert!(!room.handlers.contains_key("DANMU_MSG"));
    }

    #[test]
    fn test_custom_handler_extensibility() {
        let mut room = LiveRoom::new_with_uid(12345);
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();

        room.handlers.insert(
            "CUSTOM_EVENT".to_string(),
            Arc::new(move |_ctx, payload| {
                if payload.get("cmd").and_then(|v| v.as_str()) == Some("CUSTOM_EVENT") {
                    called_clone.store(true, Ordering::Relaxed);
                }
            }),
        );

        let payload = serde_json::json!({ "cmd": "CUSTOM_EVENT" });
        if let Some(cmd) = payload.get("cmd").and_then(|v| v.as_str()) {
            match room.handlers.get(cmd) {
                Some(handler) => handler(&room.ctx, &payload),
                None => {}
            }
        }

        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn test_handle_room_change() {
        let room = LiveRoom::new_with_uid(12345);
        let payload = serde_json::json!({
            "cmd": "ROOM_CHANGE",
            "data": { "title": "新的测试直播间标题" }
        });

        match room.handlers.get("ROOM_CHANGE") {
            Some(handler) => handler(&room.ctx, &payload),
            None => panic!("ROOM_CHANGE handler missing"),
        }

        assert_eq!(room.ctx.get_title(), "新的测试直播间标题");
    }

    #[test]
    fn test_handle_live_and_preparing() {
        let room = LiveRoom::new_with_uid(12345);
        assert!(!room.ctx.is_live());

        let live_payload = serde_json::json!({
            "cmd": "LIVE",
            "live_time": 1700000000
        });
        match room.handlers.get("LIVE") {
            Some(handler) => handler(&room.ctx, &live_payload),
            None => panic!("LIVE handler missing"),
        }
        assert!(room.ctx.is_live());

        let preparing_payload = serde_json::json!({
            "cmd": "PREPARING"
        });
        match room.handlers.get("PREPARING") {
            Some(handler) => handler(&room.ctx, &preparing_payload),
            None => panic!("PREPARING handler missing"),
        }
        assert!(!room.ctx.is_live());
    }
}
