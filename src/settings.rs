use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub host: String,
    pub allowed_origins: Vec<String>,
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
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub filename: String,
    pub init_sql_path: Option<String>,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            filename: ":memory:".to_string(),
            init_sql_path: None,
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
        .set_default("database.filename", default_db.filename)?
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
