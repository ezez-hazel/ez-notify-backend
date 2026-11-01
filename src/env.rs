use std::env;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct AppConfig {
    pub notify_worker_url: String,
    pub notify_sign_key: String,
    pub cloudflare_account_id: String,
    pub notify_d1_database_id: String,
    pub cloudflare_api_token: String,
    pub is_journal: bool,
}

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

impl AppConfig {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            notify_worker_url: required_env("NOTIFY_WORKER_URL")?,
            notify_sign_key: required_env("NOTIFY_SIGN_KEY")?,
            cloudflare_account_id: required_env("CLOUDFLARE_ACCOUNT_ID")?,
            notify_d1_database_id: required_env("NOTIFY_D1_DATABASE_ID")?,
            cloudflare_api_token: required_env("CLOUDFLARE_API_TOKEN")?,
            is_journal: existed_env("JOURNAL_STREAM"),
        })
    }
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("缺少必需的环境变量: {name}"))
}

fn existed_env(name: &str) -> bool {
    env::var_os(name).is_some()
}

pub fn init() -> Result<(), String> {
    CONFIG
        .set(AppConfig::from_env()?)
        .map_err(|_| "应用配置已经初始化".to_string())
}

pub fn get() -> &'static AppConfig {
    CONFIG
        .get()
        .expect("应用配置尚未初始化，请先调用 env::init()")
}
