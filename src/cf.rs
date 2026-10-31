//! Cloudflare D1 查询模块
//!
//! 对应 Python reference 中的 `lib/cf.py`

use anyhow::{Result, bail};
use serde_json::Value;

/// 查询 Cloudflare D1 数据库
pub async fn d1_query(
    account_id: &str,
    database_id: &str,
    api_token: &str,
    sql: &str,
) -> Result<Vec<Value>> {
    let query_url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    );

    let payload = serde_json::json!({
        "sql": sql,
    });

    let client = reqwest::Client::new();
    let resp: Value = client
        .post(&query_url)
        .header("Authorization", format!("Bearer {api_token}"))
        .header("Content-Type", "application/json")
        .json(&payload)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?
        .json()
        .await?;

    if resp.get("success").and_then(|v| v.as_bool()) != Some(true) {
        let errors = &resp["errors"];
        bail!("D1 query failed: {errors}");
    }

    let rows = resp
        .get("result")
        .and_then(|r| r.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("results"))
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();

    Ok(rows)
}
