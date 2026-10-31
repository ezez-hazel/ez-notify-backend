//! Bilibili API 模块
//!
//! 对应 Python reference 中的 `bili.py`

use std::sync::LazyLock;
use std::vec;

use anyhow::{Result, bail};
use reqwest::Client;
use tokio::sync::RwLock;

use crate::bili_wbi;

pub const USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:156.0) Gecko/20100101 Firefox/156.0";

const COOKIE_URL: &str = "https://www.bilibili.com";

const URL_ROOM_GET_INFO: &str = "https://api.live.bilibili.com/room/v1/Room/get_info";
#[allow(dead_code)]
const URL_ROOM_GET_INFO_OLD: &str = "https://api.live.bilibili.com/room/v1/Room/getRoomInfoOld";
const URL_MASTER_INFO: &str = "https://api.live.bilibili.com/live_user/v1/Master/info";
const URL_DANMU_INFO: &str = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo";

/// 全局 Cookie 存储
pub static COOKIES: LazyLock<RwLock<CookieJar>> = LazyLock::new(|| RwLock::new(CookieJar::new()));

/// 简易 Cookie 容器
#[derive(Debug, Clone, Default)]
pub struct CookieJar {
    cookies: Vec<(String, String)>,
}

impl CookieJar {
    pub fn new() -> Self {
        Self {
            cookies: Vec::new(),
        }
    }

    /// 从 Set-Cookie header 加载
    pub fn load_from_set_cookie(&mut self, header_value: &str) {
        // 只取第一个 ";" 前面的 key=value
        if let Some(kv) = header_value.split(';').next() {
            let kv = kv.trim();
            if let Some((k, v)) = kv.split_once('=') {
                let key = k.trim().to_string();
                let value = v.trim().to_string();
                // 如果已存在同名 cookie，更新它
                if let Some(existing) = self.cookies.iter_mut().find(|(ek, _)| *ek == key) {
                    existing.1 = value;
                } else {
                    self.cookies.push((key, value));
                }
            }
        }
    }

    /// 输出为 Cookie header 值
    pub fn output(&self) -> String {
        self.cookies
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// 获取某个 Cookie 值
    pub fn get(&self, key: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// 获取 DedeUserID (mid)
    pub fn get_mid(&self) -> Option<i64> {
        self.get("DedeUserID").and_then(|v| v.parse::<i64>().ok())
    }
}

/// 获取 Bilibili Cookie（匿名访问）
pub async fn get_cookies() -> Result<()> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let resp = client
        .head(COOKIE_URL)
        .header("User-Agent", USER_AGENT)
        .send()
        .await?;

    let mut jar = COOKIES.write().await;
    for value in resp.headers().get_all("set-cookie") {
        if let Ok(s) = value.to_str() {
            jar.load_from_set_cookie(s);
        }
    }

    Ok(())
}

/// 发起带 WBI 签名的请求
pub async fn wbi_request(
    client: &Client,
    url: &str,
    params: Vec<(&str, String)>,
    referer: &str,
) -> Result<serde_json::Value> {
    let (img_key, sub_key) = bili_wbi::get_wbi_keys().await?;
    let query = bili_wbi::encode_wbi(params, (img_key, sub_key));

    let cookie_str = COOKIES.read().await.output();
    let resp: serde_json::Value = client
        .get(format!("{url}?{query}"))
        .header("User-Agent", USER_AGENT)
        .header("Referer", referer)
        .header("Cookie", cookie_str)
        .send()
        .await?
        .json()
        .await?;

    Ok(resp)
}

/// 获取直播间信息
pub async fn room_get_info(client: &Client, room_id: i64) -> Result<serde_json::Value> {
    let cookie_str = COOKIES.read().await.output();
    let resp: serde_json::Value = client
        .get(URL_ROOM_GET_INFO)
        .query(&[("room_id", room_id.to_string())])
        .header("User-Agent", USER_AGENT)
        .header("Referer", format!("https://live.bilibili.com/{room_id}"))
        .header("Cookie", cookie_str)
        .send()
        .await?
        .json()
        .await?;

    let code = resp["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = resp["message"].as_str().unwrap_or("unknown error");
        bail!("获取直播间信息失败: {msg}");
    }

    Ok(resp["data"].clone())
}

/// 获取用户对应的直播间状态 (getRoomInfoOld)
#[allow(dead_code)]
pub async fn room_get_info_old(client: &Client, mid: i64) -> Result<serde_json::Value> {
    let cookie_str = COOKIES.read().await.output();
    let resp: serde_json::Value = client
        .get(URL_ROOM_GET_INFO_OLD)
        .query(&[("mid", mid.to_string())])
        .header("User-Agent", USER_AGENT)
        .header("Referer", format!("https://space.bilibili.com/{mid}"))
        .header("Cookie", cookie_str)
        .send()
        .await?
        .json()
        .await?;

    let code = resp["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = resp["message"].as_str().unwrap_or("unknown error");
        bail!("获取直播间信息失败: {msg}");
    }

    Ok(resp["data"].clone())
}

/// 获取主播信息
pub async fn master_info(client: &Client, uid: i64) -> Result<serde_json::Value> {
    let cookie_str = COOKIES.read().await.output();
    let resp: serde_json::Value = client
        .get(URL_MASTER_INFO)
        .query(&[("uid", uid.to_string())])
        .header("User-Agent", USER_AGENT)
        .header("Referer", format!("https://space.bilibili.com/{uid}"))
        .header("Cookie", cookie_str)
        .send()
        .await?
        .json()
        .await?;

    let code = resp["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = resp["message"].as_str().unwrap_or("unknown error");
        bail!("获取主播信息失败: {msg}");
    }

    Ok(resp["data"].clone())
}

/// 获取弹幕连接信息 (getDanmuInfo)
pub async fn get_danmu_info(client: &Client, room_id: i64) -> Result<serde_json::Value> {
    let params = vec![("id", room_id.to_string())];

    let resp = wbi_request(client, URL_DANMU_INFO, params, "https://www.bilibili.com").await?;

    let code = resp["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = resp["message"].as_str().unwrap_or("unknown error");
        bail!("获取弹幕信息失败: {msg}");
    }

    Ok(resp["data"].clone())
}
