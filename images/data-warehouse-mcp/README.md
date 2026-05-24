# data-warehouse-mcp

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that connects to a [Trino](https://trino.io) data warehouse, providing AI agents with tools to query data, run analysis scripts in isolated sandboxes, and generate visualizations.

## MCP Tools

| Tool | Description |
|---|---|
| `search_tables` | Search tables by name or description |
| `get_table_schema` | Get column details for a table |
| `create_session` | Create an isolated sandbox session |
| `execute_query` | Run SQL queries (inline results or save to file) |
| `workspace_bash` | Execute commands in the sandbox (bash, python3, jq, etc.) |
| `workspace_edit` | Create/edit files in the sandbox |
| `get_image_url` | Publish a sandbox image and return a public URL |

## HTTP Endpoints

| Path | Method | Purpose |
|---|---|---|
| `/mcp` | `POST` | MCP Streamable HTTP endpoint |
| `/healthz` | `GET` | Liveness probe |
| `/readyz` | `GET` | Readiness probe (checks Trino + session manager) |
| `/metrics` | `GET` | Prometheus metrics |
| `/images/{filename}` | `GET` | Serve published images |

## Configuration

All configuration is via environment variables.

### Required

| Variable | Description |
|---|---|
| `TRINO_HOST` | Trino server hostname |
| `TRINO_CATALOG` | Default Trino catalog |
| `TRINO_SCHEMA` | Default Trino schema |
| `IMAGE_BASE_URL` | Base URL for published images (e.g. `https://example.com`) |

### Optional

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `TRINO_PORT` | `8080` | Trino server port |
| `TRINO_SCHEME` | `http` | Trino protocol (`http` or `https`) |
| `TRINO_SOURCE` | `data-warehouse-mcp` | Trino query source identifier |
| `SESSION_TIMEOUT` | `1h` | Session idle timeout |
| `SESSION_SWEEP_INTERVAL` | `5m` | Interval to sweep expired sessions |
| `MAX_SESSIONS` | `10` | Maximum concurrent sessions |
| `MAX_INLINE_ROWS` | `100` | Max rows for inline query results |
| `MAX_QUERY_FILE_BYTES` | `104857600` | Max query output file size (bytes) |
| `MAX_BASH_OUTPUT_BYTES` | `1048576` | Max sandbox command output (bytes) |
| `DEFAULT_BASH_TIMEOUT` | `300s` | Default sandbox command timeout |
| `MAX_BASH_TIMEOUT` | `900s` | Maximum allowed sandbox command timeout |
| `DEFAULT_QUERY_TIMEOUT` | `120s` | Default SQL query timeout |
| `MAX_QUERY_TIMEOUT` | `900s` | Maximum allowed SQL query timeout |
| `MAX_EDIT_BYTES` | `1048576` | Max file edit size (bytes) |
| `MAX_IMAGE_BYTES` | `10485760` | Max published image size (bytes) |
| `SESSIONS_ROOT` | `/sessions` | Session data directory |
| `SESSION_ROOTFS_ARCHIVE` | `/opt/session-rootfs.tar` | Sandbox rootfs archive path |
| `IMAGES_ROOT` | `/images` | Published images directory |

## Building

```bash
docker build -t data-warehouse-mcp .
```

## Running

```bash
docker run -e TRINO_HOST=trino.example.com \
  -e TRINO_CATALOG=osquery \
  -e TRINO_SCHEMA=public \
  -e IMAGE_BASE_URL=https://example.com \
  -p 8080:8080 \
  data-warehouse-mcp
```
