# Azure SQL Batch Output

This Azure Function app reads records from `dbo.FinalSuggestPO`, sorted by `PreVendor` ascending, and writes each batch payload to function output logs.

## Features
- Timer-triggered ingestion
- HTTP-triggered ingestion for manual runs
- Reads records from `dbo.FinalSuggestPO`
- Sorts records by `PreVendor` ascending
- Batches records by `PreVendor` groups and `BATCH_SIZE`
- Writes records in batches to output logs

## Files
- `function_app.py` - Main ingestion function
- `requirements.txt` - Python dependencies
- `host.json` - Function host config
- `local.settings.sample.json` - Sample local settings

## Settings
Set these in `local.settings.json` for local runs and in Azure Function App Configuration for production:

- `CSV_INGEST_SCHEDULE` (example: `0 */5 * * * *`)
- `SQL_CONNECTION_STRING`
- `BATCH_SIZE`
- `SQL_TIMEOUT_SEC`

## Quick start (local)
1. Install dependencies:
   - `pip install -r requirements.txt`
2. Copy settings:
   - `copy local.settings.sample.json local.settings.json`
3. Set `SQL_CONNECTION_STRING` to your SQL Server connection string.
4. Run locally:
   - `func start`

## Deploy to Azure
1. Create a Python Azure Function App.
2. Add all app settings listed above.
3. Deploy from this folder:
   - `func azure functionapp publish <your-function-app-name>`

## Manual trigger endpoint
After deployment, you can trigger ingestion manually via HTTP:

- Method: `GET` or `POST`
- Route: `/api/ingest`
- Auth: Function key required (`auth_level=FUNCTION`)

Example:
- `POST https://<your-function-app>.azurewebsites.net/api/ingest?code=<function-key>`
