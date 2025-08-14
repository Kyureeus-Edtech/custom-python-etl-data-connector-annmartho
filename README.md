```
# AlienVault OTX → MongoDB ETL Connector

ETL script to pull pulses from [AlienVault OTX](https://otx.alienvault.com/) into MongoDB with optional incremental updates.

## Features
- Auth via API key from `.env`
- Extract from `/pulses/subscribed` with optional `modified_since`
- Transform keys for MongoDB + add ingestion metadata
- Load via upsert into one collection (`otx_pulses_raw`)
- Supports pagination and watermark resume

## Setup

1. **Create `.env`**:
```

OTX_API_KEY=your_api_key_here
MONGO_URI=mongodb://localhost:27017
MONGO_DB=threatintel
MONGO_COLLECTION=otx_pulses_raw
OTX_PAGE_LIMIT=50
OTX_MODIFIED_SINCE=2025-01-01T00:00:00+00:00
OTX_WATERMARK_FILE=.otx_watermark.json
REQUEST_TIMEOUT=30
MAX_RETRIES=5
BACKOFF_SECONDS=2

````

2. **Install deps**
```bash
pip install -r requirements.txt
````

3. **Run**

```bash
python etl_connector.py --since "YYYY-MM-DDTHH:MM:SS+00:00"
# or to resume fetching from watermark
python etl_connector.py
```

## Notes

- API docs: [https://otx.alienvault.com/api](https://otx.alienvault.com/api)
- Handles bad keys, retries on errors, and empty results.
- Watermark file is optional (for incremental loads).

```

```
