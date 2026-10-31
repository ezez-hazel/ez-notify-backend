//! 直播弹幕 WebSocket 消息流模块
//!
//! 对应 Python reference 中的 `live_message_stream.py`
//! 使用 fastwebsockets 作为 WebSocket 客户端

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use fastwebsockets::{FragmentCollector, Frame, OpCode};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tracing::{debug, info, warn};
use url::Url;

use crate::bili;

/// 协议头大小（固定 16 字节）
const HEADER_SIZE: usize = 16;

/// 心跳间隔（秒）
const HEARTBEAT_INTERVAL: u64 = 30;

/// 消息处理回调类型
pub type PayloadHandler = Box<dyn Fn(&serde_json::Value) + Send + Sync>;

/// Bilibili 直播 WebSocket 消息流
pub struct LiveMessageStream {
    room_id: i64,
    handler: Option<PayloadHandler>,
    waiting_for_pong: Arc<AtomicBool>,
}

impl LiveMessageStream {
    pub fn new(room_id: i64) -> Self {
        Self {
            room_id,
            handler: None,
            waiting_for_pong: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_handler(&mut self, handler: PayloadHandler) {
        self.handler = Some(handler);
    }

    /// 获取弹幕 WebSocket 地址和 token
    async fn get_auth_info(&self) -> Result<(Vec<String>, String)> {
        let client = reqwest::Client::new();
        let data = bili::get_danmu_info(&client, self.room_id).await?;

        let token = data["token"]
            .as_str()
            .context("token not found")?
            .to_string();

        let host_list = data["host_list"]
            .as_array()
            .context("host_list not found")?;

        let mut websocket_urls = Vec::new();
        for host in host_list {
            let host_url = host["host"].as_str().unwrap_or("");
            let wss_port = host["wss_port"].as_u64();
            let ws_port = host["ws_port"].as_u64();

            let (scheme, port) = if let Some(p) = wss_port {
                ("wss", p)
            } else if let Some(p) = ws_port {
                ("ws", p)
            } else {
                warn!("弹幕服务器 {host_url} 未提供可用端口");
                continue;
            };

            websocket_urls.push(format!("{scheme}://{host_url}:{port}/sub"));
        }

        if websocket_urls.is_empty() {
            bail!("弹幕服务器列表为空");
        }

        Ok((websocket_urls, token))
    }

    /// 构造 Bilibili 直播协议数据包
    fn make_packet(payload: &[u8], operation: u32, version: u16) -> Vec<u8> {
        let packet_length = (HEADER_SIZE + payload.len()) as u32;
        let header_size = HEADER_SIZE as u16;
        let sequence: u32 = 1;

        let mut packet = Vec::with_capacity(HEADER_SIZE + payload.len());
        packet.extend_from_slice(&packet_length.to_be_bytes());
        packet.extend_from_slice(&header_size.to_be_bytes());
        packet.extend_from_slice(&version.to_be_bytes());
        packet.extend_from_slice(&operation.to_be_bytes());
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    /// 构造心跳包
    fn make_heartbeat_packet() -> Vec<u8> {
        Self::make_packet(b"[object Object]", 2, 1)
    }

    /// 构造认证包
    fn make_auth_packet(
        room_id: i64,
        token: &str,
        mid: Option<i64>,
        buvid: Option<&str>,
    ) -> Vec<u8> {
        let mut auth_info = serde_json::json!({
            "uid": mid.unwrap_or(0),
            "roomid": room_id,
            "protover": 3,
            "support_ack": true,
            "scene": "room",
            "platform": "web",
            "type": 2,
            "key": token,
        });

        if let Some(mid_val) = mid {
            auth_info["mid"] = serde_json::json!(mid_val);
        }

        if let Some(buvid_val) = buvid {
            auth_info["buvid"] = serde_json::json!(buvid_val);
        }

        let payload = serde_json::to_vec(&auth_info).unwrap();
        info!("auth_info: {}", serde_json::to_string(&auth_info).unwrap());
        Self::make_packet(&payload, 7, 1)
    }

    /// 解析协议数据包（递归处理 zlib/brotli 压缩嵌套子包）
    fn parse_packets(data: &[u8], handler: &Option<PayloadHandler>) -> Vec<serde_json::Value> {
        let mut records = Vec::new();
        let mut offset = 0;

        while offset + HEADER_SIZE <= data.len() {
            let packet_length =
                u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            let header_length =
                u16::from_be_bytes(data[offset + 4..offset + 6].try_into().unwrap()) as usize;
            let version = u16::from_be_bytes(data[offset + 6..offset + 8].try_into().unwrap());
            let operation = u32::from_be_bytes(data[offset + 8..offset + 12].try_into().unwrap());
            let sequence = u32::from_be_bytes(data[offset + 12..offset + 16].try_into().unwrap());

            if packet_length < header_length
                || header_length < HEADER_SIZE
                || offset + packet_length > data.len()
            {
                break;
            }

            let payload = &data[offset + header_length..offset + packet_length];

            let mut record = serde_json::json!({
                "packet_length": packet_length,
                "header_length": header_length,
                "version": version,
                "operation": operation,
                "sequence": sequence,
            });

            // 尝试解码 payload
            if let Ok(text) = std::str::from_utf8(payload) {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                    // 调用 handler
                    if let Some(h) = handler {
                        h(&json_val);
                    }
                    record["payload_json"] = json_val;
                } else {
                    record["payload_text"] = serde_json::json!(text);
                }
            } else {
                record["payload_base64"] = serde_json::json!(BASE64.encode(payload));
            }

            // 处理压缩子包
            match version {
                2 => {
                    // zlib 解压
                    let mut decoder = flate2::read::ZlibDecoder::new(payload);
                    let mut decompressed = Vec::new();
                    match decoder.read_to_end(&mut decompressed) {
                        Ok(_) => {
                            let children = Self::parse_packets(&decompressed, handler);
                            record["children"] = serde_json::json!(children);
                            // 移除 payload_base64
                            if let Some(obj) = record.as_object_mut() {
                                obj.remove("payload_base64");
                            }
                        }
                        Err(e) => {
                            record["decompression_error"] = serde_json::json!(format!("zlib: {e}"));
                        }
                    }
                }
                3 => {
                    // brotli 解压
                    let mut decompressed = Vec::new();
                    let mut reader = brotli::Decompressor::new(payload, 4096);
                    match reader.read_to_end(&mut decompressed) {
                        Ok(_) => {
                            let children = Self::parse_packets(&decompressed, handler);
                            record["children"] = serde_json::json!(children);
                            if let Some(obj) = record.as_object_mut() {
                                obj.remove("payload_base64");
                            }
                        }
                        Err(e) => {
                            record["decompression_error"] =
                                serde_json::json!(format!("brotli: {e}"));
                        }
                    }
                }
                _ => {}
            }

            records.push(record);
            offset += packet_length;
        }

        records
    }

    /// TCP 延迟探测
    async fn probe_tcp_latency(websocket_url: &str) -> Result<(String, std::time::Duration)> {
        let url = Url::parse(websocket_url)?;
        let host = url.host_str().context("no host")?;
        let port = url.port_or_known_default().context("no port")?;
        let is_tls = url.scheme() == "wss";

        let start = std::time::Instant::now();

        let tcp_stream = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            TcpStream::connect(format!("{host}:{port}")),
        )
        .await??;

        if is_tls {
            let config = ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::from_iter(
                    webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
                ))
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(config));
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())?;
            let _ = connector.connect(server_name, tcp_stream).await?;
        }

        let latency = start.elapsed();
        Ok((websocket_url.to_string(), latency))
    }

    /// 选择最快的 WebSocket 服务器
    async fn select_fastest_url(websocket_urls: &[String]) -> Result<String> {
        if websocket_urls.is_empty() {
            bail!("WebSocket 地址列表为空");
        }

        // let mut handles = Vec::new();
        let mut handles = JoinSet::new();
        for url in websocket_urls {
            let url = url.clone();
            // handles.push(tokio::spawn(async move {
            //     Self::probe_tcp_latency(&url).await
            // }));
            handles.spawn(async move { Self::probe_tcp_latency(&url).await });
        }

        let mut best_url = None;
        // let mut best_latency = std::time::Duration::MAX;

        // for handle in handles {
        //     if let Ok(Ok((url, latency))) = handle.await {
        //         if latency < best_latency {
        //             best_latency = latency;
        //             best_url = Some(url);
        //         }
        //     }
        // }

        if let Some(result) = handles.join_next().await {
            match result {
                Ok(Ok((addr, _duration))) => {
                    best_url = Some(addr);
                    // best_latency = duration;
                    // println!("🎉 最快响应！目标: {}, 耗时: {:?}", addr, latency);
                }
                Ok(Err(e)) => {
                    println!("{}", e);
                }
                Err(e) => {
                    println!("{}", e);
                    // best_url.context("所有 WebSocket 地址 TCP 探测均失败");
                }
            }

            handles.abort_all();
        }

        // best_url
        best_url.context("所有 WebSocket 地址 TCP 探测均失败")
    }

    /// 通过 fastwebsockets 升级连接并返回 WebSocket
    async fn ws_upgrade(
        websocket_url: &str,
    ) -> Result<FragmentCollector<TokioIo<hyper::upgrade::Upgraded>>> {
        let url = Url::parse(websocket_url)?;
        let host = url.host_str().context("no host")?.to_string();
        let port = url.port_or_known_default().context("no port")?;
        let path = url.path().to_string();
        let is_tls = url.scheme() == "wss";

        let tcp_stream = TcpStream::connect(format!("{host}:{port}")).await?;

        if !is_tls {
            bail!("仅支持 wss 连接");
        }

        let config = ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::from_iter(
                webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
            ))
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let server_name = rustls::pki_types::ServerName::try_from(host.clone())?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;

        let req = Request::builder()
            .method("GET")
            .uri(&path)
            .header("Host", format!("{host}:{port}"))
            .header("User-Agent", bili::USER_AGENT)
            .header("Origin", "https://live.bilibili.com")
            .header("Cookie", bili::COOKIES.read().await.output())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .body(http_body_util::Empty::<Bytes>::new())?;

        let (ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await?;

        Ok(FragmentCollector::new(ws))
    }

    /// 接收消息主循环
    async fn receive_messages(&self, websocket_url: &str, token: &str) -> Result<()> {
        let cookies = bili::COOKIES.read().await;
        let mid = cookies.get_mid();
        let buvid = cookies
            .get("buvid3")
            .or_else(|| cookies.get("buvid4"))
            .map(|s| s.to_string());
        drop(cookies);

        let mut ws = Self::ws_upgrade(websocket_url).await?;

        // 发送认证包
        let auth_packet = Self::make_auth_packet(self.room_id, token, mid, buvid.as_deref());
        ws.write_frame(Frame::binary(fastwebsockets::Payload::Owned(auth_packet)))
            .await?;

        let waiting_for_pong = self.waiting_for_pong.clone();

        // 心跳任务
        let heartbeat_waiting = waiting_for_pong.clone();

        // 使用 tokio::select! 同时处理心跳和消息接收
        let mut heartbeat_interval =
            tokio::time::interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL));
        // 跳过第一个 tick（立即触发的那个）
        heartbeat_interval.tick().await;

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    if heartbeat_waiting.load(Ordering::Relaxed) {
                        bail!("Timeout after Ping send.");
                    }
                    let hb = Self::make_heartbeat_packet();
                    ws.write_frame(Frame::binary(fastwebsockets::Payload::Owned(hb))).await?;
                    debug!("[Room: {}] Send heartbeat.", self.room_id);
                    heartbeat_waiting.store(true, Ordering::Relaxed);
                }
                frame = ws.read_frame() => {
                    let frame = frame?;
                    match frame.opcode {
                        OpCode::Binary | OpCode::Text => {
                            waiting_for_pong.store(false, Ordering::Relaxed);
                            let data = frame.payload.to_vec();
                            let records = Self::parse_packets(&data, &self.handler);
                            debug!("收到 {} 条记录", records.len());
                        }
                        OpCode::Close => {
                            info!("WebSocket 连接关闭");
                            break;
                        }
                        OpCode::Ping => {
                            ws.write_frame(Frame::pong(frame.payload)).await?;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    /// 运行消息流
    pub async fn run(&self) -> Result<()> {
        if self.room_id <= 0 {
            bail!("ROOM_ID 必须是正整数");
        }

        let (websocket_urls, token) = self.get_auth_info().await?;
        let websocket_url = Self::select_fastest_url(&websocket_urls).await?;
        info!("选择最快的 WebSocket 服务器: {websocket_url}");

        self.receive_messages(&websocket_url, &token).await
    }
}

/// fastwebsockets 需要的 hyper executor
struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}
