use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    /// Email/SMTP + digest scheduling. Optional: absent → digests are logged,
    /// not sent, and the loop runs on the default cadence.
    #[serde(default)]
    pub email: EmailSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EmailSettings {
    /// SMTP relay host. Empty → mailer disabled (digests are logged only).
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    /// From address, e.g. "CROSV <noreply@crosv.example>".
    pub from: String,
    /// Plaintext SMTP (no TLS) — for a local dev relay like MailHog. Prod uses
    /// STARTTLS on 587 (the default when false).
    #[serde(default)]
    pub smtp_insecure: bool,
    /// How often the background digest loop wakes (seconds).
    pub digest_interval_secs: u64,
}

impl Default for EmailSettings {
    fn default() -> Self {
        Self {
            smtp_host: String::new(),
            smtp_port: 587,
            smtp_username: String::new(),
            smtp_password: String::new(),
            from: "CROSV <noreply@crosv.local>".to_string(),
            smtp_insecure: false,
            digest_interval_secs: 3600,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub host: String,
    pub allowed_origins: Vec<String>,
    pub api_token: String
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            port: 3000,
            host: "127.0.0.1".to_string(),
            allowed_origins: [
                "https://example.com".parse().unwrap(),
                "https://api.example.com".parse().unwrap(),
            ]
            .to_vec(),
            api_token: "mysecret".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub filename: String,
    pub init_sql_path: Option<String>,
    pub update_sql_path: Option<String>,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            filename: ":memory:".to_string(),
            init_sql_path: None,
            update_sql_path: None
            
        }
    }
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    // Create the default instances
    let default_server = ServerSettings::default();
    let default_db = DatabaseSettings::default();

    let settings = config::Config::builder()
        // 1. Inject the hardcoded defaults first
        .set_default("server.port", default_server.port)?
        .set_default("server.host", default_server.host)?
        .set_default("server.allowed_origins", default_server.allowed_origins)?
        .set_default("server.api_token", default_server.api_token)?
        .set_default("database.filename", default_db.filename)?
        // Email defaults so a partially-set [email] (e.g. only SMTP_HOST via env)
        // still deserializes — every field needs a base value.
        .set_default("email.smtp_host", "")?
        .set_default("email.smtp_port", 587)?
        .set_default("email.smtp_username", "")?
        .set_default("email.smtp_password", "")?
        .set_default("email.from", "CROSV <noreply@crosv.local>")?
        .set_default("email.smtp_insecure", false)?
        .set_default("email.digest_interval_secs", 3600)?
        // 2. Load the optional file (overwrites defaults)
        .add_source(config::File::with_name("config/default").required(false))
        // 3. Load environment variables (overwrites everything)
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}
