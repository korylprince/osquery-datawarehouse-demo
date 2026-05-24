# osquery-extension

A demo osquery extension that exposes AWS billing data as a queryable table. Run this demo and keep track of how much it's costing you!

Results are cached in-memory for 12 hours to minimize Cost Explorer API calls.

## Table Schema

| Column       | Type   | Description                              |
| ------------ | ------ | ---------------------------------------- |
| `month`      | TEXT   | Billing month (`YYYY-MM`)                |
| `amount`     | TEXT   | Blended cost amount                      |
| `currency`   | TEXT   | Currency code (e.g. `USD`)               |
| `start_date` | TEXT   | Period start date                        |
| `end_date`   | TEXT   | Period end date                          |
| `estimated`  | TEXT   | Whether the result is estimated (`true`/`false`) |

## Usage

```sql
-- Current month (default)
SELECT * FROM aws_billing;

-- Specific month
SELECT * FROM aws_billing WHERE month = '2025-01';
```

## Getting Started

```bash
# 1. Build the extension
go build -o osquery-extension .

# 2. Authenticate with AWS
aws login

# 3. Query billing data
osqueryi --extensions_require aws_billing_extension --extension ./osquery-extension 'SELECT * FROM aws_billing;'
```

## Configuration

### AWS Credentials (required)

The extension supports any common AWS credential configuration, including `aws login`.

### Caching

Results are cached in-memory for 12 hours after the first query. While osqueryd may poll the table every 60 seconds, only the first call within each 12-hour window hits the Cost Explorer API. The cache resets when the extension process restarts.
