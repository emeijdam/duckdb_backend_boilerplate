# DuckDB Axum Backend

A high-performance boilerplate for a Rust backend using **Axum**, featuring **DuckDB** for analytical workloads, **Apache Arrow** for efficient data streaming, and **NDJSON** (Line-Delimited JSON) output.

## Features

- **Axum 0.8 Framework**: Modern, ergonomic, and high-performance web framework.
- **DuckDB Integration**: Embedded analytical database with a connection pool (`r2d2`).
- **Arrow Streaming**: Stream query results directly to clients using Apache Arrow and NDJSON to minimize memory overhead.
- **Dynamic Configuration**: Tiered configuration system (Defaults -> `config/default.toml` -> Environment Variables).
- **CORS & Tracing**: Pre-configured with `tower-http` for CORS and `tracing` for structured logging.

## Project Structure

- `src/main.rs`: Application entry point and route registration.
- `src/routes.rs`: API endpoint handlers (Write, Read, Stream).
- `src/dbinit.rs`: Database initialization and schema migrations.
- `src/settings.rs`: Configuration management.
- `src/state.rs`: Shared application state (DB pool, uptime).

## Setup & Running

### Prerequisites

- Rust (latest stable version)
- DuckDB (bundled automatically by the crate)

### Configuration

The app loads configuration from `config/default.toml` by default. You can override any setting using environment variables with the `APP_` prefix and `__` separator.

**Example: Overriding port and database path via Env**
```bash
APP_SERVER__PORT=3000 APP_SERVER__HOST=127.0.0.1 APP_SERVER__API_TOKEN=your-very-secure-static-token APP_DATABASE__FILENAME=./KANWEG.db APP_DATABASE__INIT_SQL_PATH=config/init.sql APP_DATABASE__REFRESH_SQL_PATH=config/refresh.sql cargo run
```

### Installation

1. Clone the repository.
2. Build the project:
   ```bash
   cargo build
   ```
3. Run the server:
   ```bash
   cargo run
   ```
   The server will start at `http://127.0.0.1:3000` (by default).

## API Endpoints

All data endpoints support two response formats via the `format` query parameter:
- `format=json` (default): Returns a JSON object with data and paging metadata. **Supports `limit` and `offset`**.
- `format=ndjson`: Returns a high-performance NDJSON (Line-Delimited JSON) stream of **all matching records**. Paging parameters are ignored.

### 1. Health Check
`GET /health`  
Returns the server status and uptime.

### 2. Get Log Count
`GET /logs`  
Returns a simple count of entries in the `logs` table.

### 3. Get Logs
`GET /logstream`  
Returns system logs.
- **Query Params**:
  - `level` (optional): Filter by log level.
  - `format` (optional): `json` or `ndjson`.
  - `limit` (optional): Limit results (default: 100).
  - `offset` (optional): Skip results (default: 0).

### 4. Get R Packages
`GET /packages`  
Returns R package data with safety status.
- **Query Params**:
  - `package` (optional): Search query across multiple fields.
  - `osv_safety_status` (optional): Filter by safety (`SAFE`, `VULNERABLE`, `FIXED`).
  - `format` (optional): `json` or `ndjson`.
  - `limit` (optional): Results per page (default: 100).
  - `offset` (optional): Page offset (default: 0).
  - `sort_by` (optional): Field to sort by (`package`, `version`, `published`, `title`, `status`, `osv_id`, `osv_safety_status`).
  - `sort_order` (optional): `asc` or `desc`.

### 5. Get R Release History
`GET /release`  
Returns history of R versions.
- **Query Params**:
  - `format` (optional): `json` or `ndjson`.
  - `limit` (optional): Results per page (default: 100).
  - `offset` (optional): Page offset (default: 0).

### 6. Get Data Refresh Log
`GET /refreshlog`  
Returns logs of data refresh operations.
- **Query Params**:
  - `format` (optional): `json` or `ndjson`.
  - `limit` (optional): Results per page (default: 100).
  - `offset` (optional): Page offset (default: 0).

### 7. Refresh Data (Write)
`POST /refresh`  
Executes the `refresh_sql_path` script and logs the operation.
```bash
curl -X POST http://localhost:3000/refresh \
     -H "Authorization: Bearer your-very-secure-static-token" \
     -H "Content-Type: application/json" \
     -d '{"message": "Manual refresh trigger"}'
```

## JSON Paging Response Example
When using `format=json`, the response structure is:
```json
{
  "data": [...],
  "paging": {
    "limit": 100,
    "offset": 0,
    "total": 1234
  }
}
```

## Development
- **Database Initialization**: On startup, the app runs `config/init.sql` (if it exists) and ensures the `logs` table is created.
- **CORS**: Configurable via `config/default.toml` under `server.allowed_origins`.

```bash
APP_SERVER__PORT=3000 APP_SERVER__HOST=127.0.0.1 APP_SERVER__API_TOKEN=your-very-secure-static-token APP_DATABASE__FILENAME=./KANWEG.db APP_DATABASE__INIT_SQL_PATH=config/init.sql APP_DATABASE__REFRESH_SQL_PATH=config/refresh.sql cargo watch run
```


curl -X POST https://backend.dasc.nl/refresh \
     -H "Authorization: Bearer your-very-secure-static-token" \
     -H "Content-Type: application/json" \
     -d '{"message": "System check successful"}'