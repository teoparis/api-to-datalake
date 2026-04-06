# api-to-datalake

Config-driven ingestion framework for fintech/banking data.  
Pull data from **REST APIs** and **JDBC sources**, land it in **Azure Data Lake Storage Gen2** as partitioned **Parquet** files.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES                                      │
│                                                                     │
│  ┌──────────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  OpenExchangeRates│   │  SQL Server DB   │   │  Account Events│  │
│  │  (REST / API key) │   │  (JDBC / pyodbc) │   │  API (Bearer)  │  │
│  └────────┬─────────┘   └────────┬─────────┘   └───────┬────────┘  │
│           │                      │                      │           │
└───────────┼──────────────────────┼──────────────────────┼───────────┘
            │                      │                      │
            ▼                      ▼                      ▼
┌───────────────────────────────────────────────────────────────────┐
│                    EXTRACTOR LAYER  (src/extractors/)             │
│                                                                   │
│   RESTExtractor                      JDBCExtractor                │
│   ─────────────                      ─────────────                │
│   • api_key / Bearer / Basic auth    • pyodbc connection string   │
│   • offset / cursor / next_url       • chunked SELECT with        │
│     pagination                         watermark WHERE clause     │
│   • Retry-After rate-limit handling  • SQL→pandas type coercion   │
│   • incremental via watermark field  • incremental via watermark  │
│                                                                   │
│            Both implement BaseExtractor (abstract)                │
└───────────────────────┬───────────────────────────────────────────┘
                        │  pd.DataFrame + _ingested_at column
                        ▼
┌───────────────────────────────────────────────────────────────────┐
│               PARQUET SERIALIZER  (src/loaders/parquet_loader.py) │
│                                                                   │
│   • pyarrow Engine, snappy compression                            │
│   • Hive-style partitioning: year= / month= / day=               │
│   • Checksum verification after upload                            │
│   • Returns metadata: path, size_bytes, row_count                 │
└───────────────────────┬───────────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────────┐
│          ADLS Gen2 LOADER  (azure-storage-file-datalake)          │
│                                                                   │
│   Container: raw   Filesystem: datalake                           │
│   Path: {target_path}/year=YYYY/month=MM/day=DD/                  │
│          {source}_{timestamp}.parquet                             │
│                                                                   │
│   Auth: DefaultAzureCredential (Service Principal via env vars)   │
└───────────────────────────────────────────────────────────────────┘
```

## Config-Driven Approach

Every ingestion source is described as a YAML block in `config/sources.yaml`.  
**Adding a new source requires zero code changes**, only a new YAML entry.

### Anatomy of a source config

```yaml
sources:
  my_new_source:                     # unique source identifier
    type: rest                       # rest | jdbc
    url: https://api.example.com/v1/data
    auth:
      type: bearer                   # api_key | bearer | basic
      token_env: MY_API_TOKEN        # env var that holds the secret
    pagination:
      type: cursor                   # offset | cursor | next_url | none
      cursor_field: next_cursor
      page_size: 200
    incremental:
      enabled: true
      watermark_field: updated_at    # field used to filter new records
    schedule: daily
    target_path: raw/my_new_source
```

Save the file and run:

```bash
python cli.py run --source my_new_source
```

That's it. No Python changes needed.

## Project Layout

```
api-to-datalake/
├── config/
│   └── sources.yaml          # all source definitions
├── src/
│   ├── extractors/
│   │   ├── base_extractor.py
│   │   ├── rest_extractor.py
│   │   └── jdbc_extractor.py
│   ├── loaders/
│   │   └── parquet_loader.py
│   ├── utils/
│   │   ├── logger.py
│   │   ├── watermark.py
│   │   └── retry.py
│   └── pipeline.py
├── tests/
│   ├── test_rest_extractor.py
│   └── test_parquet_loader.py
├── cli.py
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

### Prerequisites

- Python 3.11+
- ODBC Driver 18 for SQL Server (for JDBC sources)
- Azure credentials (Service Principal or Managed Identity)

### Installation

```bash
git clone https://github.com/teoparis/api-to-datalake.git
cd api-to-datalake

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# fill in your secrets in .env
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | Yes | ADLS Gen2 storage account name |
| `AZURE_CLIENT_ID` | Yes | Service Principal application ID |
| `AZURE_CLIENT_SECRET` | Yes | Service Principal secret |
| `AZURE_TENANT_ID` | Yes | Azure AD tenant ID |
| `JDBC_HOST` | Yes (JDBC) | SQL Server hostname |
| `JDBC_PORT` | No | SQL Server port (default 1433) |
| `JDBC_DATABASE` | Yes (JDBC) | Database name |
| `JDBC_USER` | Yes (JDBC) | DB username |
| `JDBC_PASSWORD` | Yes (JDBC) | DB password |
| `API_KEY_OPENEXCHANGERATES` | Yes (exchange_rates) | OpenExchangeRates App ID |
| `WATERMARK_STORE_PATH` | No | Local path for watermarks (default `.watermarks/`) |
| `LOG_LEVEL` | No | Logging level (default `INFO`) |

## CLI Usage

```bash
# Run all configured sources
python cli.py run --source all

# Run a specific source
python cli.py run --source exchange_rates

# Dry-run: extract but do not upload
python cli.py run --source bank_transactions --dry-run

# Override the run date (useful for backfill)
python cli.py run --source account_events --date 2024-01-15

# Verbose logging
python cli.py run --source all --verbose

# List all configured sources with last watermark
python cli.py list-sources

# Reset watermark for a source (forces full reload on next run)
python cli.py reset-watermark --source exchange_rates
```

### Example output

```
$ python cli.py run --source all

2024-01-16 08:00:01 | INFO | pipeline | Starting ingestion run | sources=3
2024-01-16 08:00:02 | INFO | rest_extractor | Fetching exchange_rates | url=https://openexchangerates.org/api/latest.json
2024-01-16 08:00:03 | INFO | parquet_loader | Uploaded | path=raw/exchange_rates/year=2024/month=01/day=16/exchange_rates_20240116T080003.parquet rows=170 size_bytes=18432
2024-01-16 08:00:04 | INFO | jdbc_extractor | Fetching bank_transactions | watermark=2024-01-15T23:59:00
2024-01-16 08:00:09 | INFO | parquet_loader | Uploaded | path=raw/transactions/year=2024/month=01/day=16/bank_transactions_20240116T080009.parquet rows=4821 size_bytes=2097152
2024-01-16 08:00:10 | INFO | rest_extractor | Fetching account_events (page 1)
2024-01-16 08:00:11 | INFO | rest_extractor | Fetching account_events (page 2)
2024-01-16 08:00:12 | INFO | parquet_loader | Uploaded | path=raw/account_events/year=2024/month=01/day=16/account_events_20240116T080012.parquet rows=312 size_bytes=65536

┌─────────────────┬─────────┬──────┬──────────────────────────────────────────────────────────────────────────────┬─────────────┐
│ source          │ status  │ rows │ path                                                                         │ duration_ms │
├─────────────────┼─────────┼──────┼──────────────────────────────────────────────────────────────────────────────┼─────────────┤
│ exchange_rates  │ success │  170 │ raw/exchange_rates/year=2024/month=01/day=16/exchange_rates_20240116T...     │         912 │
│ bank_transact…  │ success │ 4821 │ raw/transactions/year=2024/month=01/day=16/bank_transactions_20240116T…      │        5124 │
│ account_events  │ success │  312 │ raw/account_events/year=2024/month=01/day=16/account_events_20240116T…       │        2341 │
└─────────────────┴─────────┴──────┴──────────────────────────────────────────────────────────────────────────────┴─────────────┘
```

## Running Tests

```bash
pytest tests/ -v
```

## Contributing

1. Fork the repository
2. Add your source config to `config/sources.yaml`
3. If the source requires a new extractor type, subclass `BaseExtractor`
4. Add tests in `tests/`
5. Open a pull request
