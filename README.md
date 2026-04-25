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

### 1. Health Check
`GET /health`  
Returns the server status and uptime.

### 2. Get Log Count
`GET /logs`  
Returns a simple count of entries in the `logs` table.

### 3. Stream Logs (NDJSON/Arrow)
`GET /logstream?level=error&limit=100`  
Streams database results as Line-Delimited JSON (NDJSON).
- **Query Params**:
  - `level` (optional): Filter by log level.
  - `limit` (optional): Limit results (default: 1000).

### 4. Get Packages (NDJSON/Arrow)
`GET /packages`  
Streams R package data from `stage_cran_current` as NDJSON.
- **Query Params**:
  - `package` (optional): Case-insensitive search for package name.
  - `maintainer` (optional): Case-insensitive search for maintainer.
  - `license` (optional): Case-insensitive search for license.
  - `limit` (optional): Number of results to return (default: 1000).
  - `offset` (optional): Number of results to skip (default: 0).
  - `sort_by` (optional): Field to sort by (`package`, `version`, `license`, `maintainer`, `published`, `title`).
  - `sort_order` (optional): `asc` or `desc` (default: `asc`).

**Example: Filter by package and sort**
```bash
curl "http://localhost:3000/packages?package=ggplot&sort_by=published&sort_order=desc"
```

**Example: Filter by maintainer with pagination**
```bash
curl "http://localhost:3000/packages?maintainer=Hadley&limit=10&offset=0"
```

### 5. Write Log
`POST /write`  
Inserts a new message into the database.
```bash
curl -X POST http://localhost:3000/write \
     -H "Content-Type: application/json" \
     -d '{"message": "System check successful"}'
```

```bash
curl -H "Authorization: Bearer your-very-secure-static-token" \
     "http://localhost:3000/logs?limit=50"
```

```bash
curl -X POST http://localhost:3000/refresh \
     -H "Authorization: Bearer your-very-secure-static-token" \
     -H "Content-Type: application/json" \
     -d '{"message": "System check successful"}'
```

## Development
- **Database Initialization**: On startup, the app runs `config/init.sql` (if it exists) and ensures the `logs` table is created.
- **CORS**: Configurable via `config/default.toml` under `server.allowed_origins`.

```bash
APP_SERVER__PORT=3000 APP_SERVER__HOST=127.0.0.1 APP_SERVER__API_TOKEN=your-very-secure-static-token APP_DATABASE__FILENAME=./KANWEG.db APP_DATABASE__INIT_SQL_PATH=config/init.sql APP_DATABASE__REFRESH_SQL_PATH=config/refresh.sql cargo watch run
```
