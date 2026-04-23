use axum::{
    Router, http::{HeaderValue, Method, header}, routing::{get, post}
};

use duckdb_backend::{
    dbinit::db_init,
    health::health_check,
    routes::{get_logs, get_logs_streaming, trigger_write},
    settings::get_configuration,
    state::AppState,
};

use tower::ServiceBuilder;
use tower_http::{
    cors::{CorsLayer},
    trace::TraceLayer,
};

use std::{sync::Arc, time::Instant};

#[tokio::main]
async fn main() {
    
    // Load config
    let settings = get_configuration().expect("Failed to load config");

    // initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Initialize DB using the database-specific settings
    let pool = db_init(&settings.database);

    // Initialize State
    let state = Arc::new(AppState {
        pool,
        start_time: Instant::now(),
        settings: settings.clone()
    });

    let allowed_origins: Vec<HeaderValue> = settings.server.allowed_origins
    .iter()
    .map(|s| s.parse().expect("Invalid character in allowed_origins"))
    .collect();

    // initialize cors layer, dev only!
    // 2. Define the CORS layer
    let cors_layer = CorsLayer::new()
        // Allow specific origins
        .allow_origin(allowed_origins)
        // Allow specific methods
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
        // Allow specific headers
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
        // Allow cookies/auth headers
        .allow_credentials(true);

    // Create App
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/logs", get(get_logs))
        .route("/logstream", get(get_logs_streaming))
        .route("/write", post(trigger_write)) // The new POST route
        .layer(ServiceBuilder::new().layer(cors_layer))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Run the server
    let addr = format!("{}:{}", settings.server.host, settings.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    tracing::debug!("🚀 Server started successfully");
    tracing::debug!("Available endpoints:");
    tracing::debug!("  GET /health                 - Health check");
    tracing::debug!("  GET /logs                   - get logs");
    tracing::debug!("  GET /logstream              - get arrow stream ndjson");
    tracing::debug!("  POST /write                 - Trigger write to database");
    tracing::debug!("listening on http://{}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}
