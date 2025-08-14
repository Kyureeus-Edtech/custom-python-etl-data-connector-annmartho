"""
ETL Connector: AlienVault OTX → MongoDB
Author: Ann Maria Thomas - 3122 22 5001 014
Kyureeus EdTech — SSN CSE | Software Architecture Assignment

What it does
------------
- Validates your API key with /api/v1/users/me
- Extracts subscribed pulses (with indicators) from OTX using /api/v1/pulses/subscribed
- Handles pagination via the "next" URL in the response
- Supports incremental ingestion via a modified_since watermark (ENV or state file)
- Transforms data to be MongoDB-safe (no '.' or '$' in keys)
- Loads raw JSON documents into a MongoDB collection: connectorname_raw (default: otx_pulses_raw)
- Adds ingestion metadata (ingested_at, source, run_id, page_no) per document

Usage
-----
    pip install -r requirements.txt
    python etl_connector.py --since "2025-01-01T00:00:00+00:00"
    # or python etl_connector.py to resume from last watermark
"""

import os
import sys
import json
import time
import uuid
import argparse
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv
from pymongo import MongoClient, errors as pymongo_errors

OTX_BASE = "https://otx.alienvault.com"
OTX_PULSES_SUBSCRIBED = "/api/v1/pulses/subscribed"
OTX_USER_ME = "/api/v1/users/me"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def safe_key(key: str) -> str:
    # MongoDB does not allow keys with '.' and keys starting with '$'
    return key.replace('.', '_').lstrip('$')

def make_mongo_safe(obj):
    if isinstance(obj, dict):
        return {safe_key(k): make_mongo_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_mongo_safe(i) for i in obj]
    else:
        return obj

def load_watermark(path: str) -> str | None:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('modified_since')
    except Exception:
        return None

def save_watermark(path: str, iso_ts: str):
    if not path:
        return
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({'modified_since': iso_ts}, f, indent=2)
    except Exception as e:
        print(f"[warn] could not persist watermark: {e}", file=sys.stderr)

def make_session(timeout: int, max_retries: int, backoff: float) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.request = _wrap_timeout(sess.request, timeout)
    return sess

def _wrap_timeout(func, timeout):
    def inner(method, url, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = timeout
        return func(method, url, **kwargs)
    return inner

def validate_api_key(session: requests.Session, api_key: str) -> dict:
    url = urljoin(OTX_BASE, OTX_USER_ME)
    headers = {"X-OTX-API-KEY": api_key}
    r = session.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"OTX API key validation failed: {r.status_code} {r.text[:200]}")
    return r.json()

def fetch_pulses(session: requests.Session, api_key: str, limit: int, modified_since: str | None):
    headers = {"X-OTX-API-KEY": api_key}
    params = {"limit": limit}
    if modified_since:
        params["modified_since"] = modified_since

    url = urljoin(OTX_BASE, OTX_PULSES_SUBSCRIBED)
    while url:
        r = session.get(url, headers=headers, params=params if '?' not in url else None)
        if r.status_code != 200:
            raise RuntimeError(f"Fetch error: {r.status_code} {r.text[:200]}")
        data = r.json()
        yield data
        # pagination via 'next'
        url = data.get("next")
        params = None  # next already includes query

def connect_mongo(uri: str, db: str, coll: str):
    client = MongoClient(uri)
    collection = client[db][coll]
    # optional: ensure an index on pulse id for upserts
    try:
        collection.create_index("id")
        collection.create_index("ingested_at")
    except pymongo_errors.PyMongoError as e:
        print(f"[warn] index creation failed: {e}", file=sys.stderr)
    return collection

def upsert_pulse(collection, doc: dict):
    # Upsert by pulse 'id' and 'revision' (if present) to avoid duplicates across updates
    key = {"id": doc.get("id"), "revision": doc.get("revision")}
    if key["id"] is None:
        key = {"_hash": hash(json.dumps(doc, sort_keys=True))}
    collection.update_one(key, {"$set": doc}, upsert=True)

def run():
    load_dotenv(override=False)

    api_key = os.getenv("OTX_API_KEY")
    if not api_key:
        raise SystemExit("Missing OTX_API_KEY in environment or .env file")

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "threatintel")
    mongo_collection = os.getenv("MONGO_COLLECTION", "otx_pulses_raw")

    page_limit = int(os.getenv("OTX_PAGE_LIMIT", "50"))
    default_since = os.getenv("OTX_MODIFIED_SINCE", "")
    watermark_file = os.getenv("OTX_WATERMARK_FILE", ".otx_watermark.json")

    timeout = int(os.getenv("REQUEST_TIMEOUT", "30"))
    max_retries = int(os.getenv("MAX_RETRIES", "5"))
    backoff = float(os.getenv("BACKOFF_SECONDS", "2"))

    parser = argparse.ArgumentParser(description="AlienVault OTX → MongoDB ETL")
    parser.add_argument("--since", help="ISO8601 timestamp for incremental ingestion", default=None)
    parser.add_argument("--no-watermark", action="store_true", help="Ignore persisted watermark file")
    parser.add_argument("--dry-run", action="store_true", help="Extract+Transform only (skip Mongo load)")
    args = parser.parse_args()

    since_cli = args.since
    since_env = default_since or None
    since_file = None if args.no_watermark else load_watermark(watermark_file)
    modified_since = since_cli or since_file or since_env

    session = make_session(timeout, max_retries, backoff)

    # 1) Validate key
    me = validate_api_key(session, api_key)
    print(f"[ok] Authenticated as {me.get('username')} (pulses={me.get('pulse_count')}, indicators={me.get('indicator_count')})")

    run_id = str(uuid.uuid4())
    collection = None if args.dry_run else connect_mongo(mongo_uri, mongo_db, mongo_collection)

    latest_modified_seen = modified_since
    page_no = 0
    docs_ingested = 0

    for page in fetch_pulses(session, api_key, page_limit, modified_since):
        page_no += 1
        results = page.get("results", [])
        for p in results:
            # Add metadata & transform for Mongo
            doc = make_mongo_safe(p)
            doc["_source"] = "otx_pulses_subscribed"
            doc["ingested_at"] = iso_now()
            doc["run_id"] = run_id
            doc["page_no"] = page_no

            # Track latest modified for watermark
            try:
                mod = p.get("modified") or p.get("created")
                if mod:
                    if (latest_modified_seen is None) or (mod > latest_modified_seen):
                        latest_modified_seen = mod
            except Exception:
                pass

            if collection is not None:
                upsert_pulse(collection, doc)
            docs_ingested += 1

        print(f"[page {page_no}] fetched={len(results)} total={docs_ingested} next={bool(page.get('next'))}")

        # be polite if large pulls
        time.sleep(0.1)

    if not args.dry_run:
        print(f"[done] Ingested/updated {docs_ingested} pulse docs into {mongo_db}.{mongo_collection}")
    else:
        print(f"[dry-run] Would ingest {docs_ingested} pulse docs")

    # Persist watermark to the last seen 'modified' (if any pages found)
    if latest_modified_seen:
        save_watermark(watermark_file, latest_modified_seen)
        print(f"[watermark] saved modified_since={latest_modified_seen} -> {watermark_file}")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"[fatal] {e}", file=sys.stderr)
        sys.exit(1)
