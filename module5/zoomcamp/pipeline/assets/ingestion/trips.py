"""@bruin
name: ingestion.trips
type: python
connection: duckdb-default
materialization:
  type: table
  strategy: append
@bruin"""

"""
Minimal ingestion asset for NYC Taxi trip data.

Behavior:
- Reads BRUIN_START_DATE / BRUIN_END_DATE (or datetimes) from environment.
- Reads pipeline vars from BRUIN_VARS JSON; expects `taxi_types` list (defaults to ["yellow"]).
- Downloads monthly parquet files from public TLC CDN, concatenates into a single pandas.DataFrame,
  and returns it. Adds `taxi_type` and `extracted_at` columns.

Dependencies: `pandas`, `requests`, `pyarrow`, `python-dateutil`
Place them in the pipeline `requirements.txt` next to `pipeline.yml`.
"""

import os
import json
import logging
import tempfile
from datetime import datetime

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

logger = logging.getLogger("ingestion.trips")


def _parse_date(s: str) -> datetime:
  # Accept ISO date or datetime; raise on failure
  return datetime.fromisoformat(s)


def _months_between(start_dt: datetime, end_dt: datetime):
  cur = datetime(start_dt.year, start_dt.month, 1)
  last = datetime(end_dt.year, end_dt.month, 1)
  while cur <= last:
    yield cur.year, cur.month
    cur = cur + relativedelta(months=1)


def materialize():
  """Fetch monthly parquet files and return a concatenated DataFrame."""

  # Read date window
  start_s = os.environ.get("BRUIN_START_DATE") or os.environ.get("BRUIN_START_DATETIME")
  end_s = os.environ.get("BRUIN_END_DATE") or os.environ.get("BRUIN_END_DATETIME")
  if not start_s or not end_s:
    raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set for ingestion.trips")

  start_dt = _parse_date(start_s)
  end_dt = _parse_date(end_s)

  # Pipeline variables
  vars_json = os.environ.get("BRUIN_VARS", "{}")
  try:
    vars_obj = json.loads(vars_json)
  except Exception:
    vars_obj = {}
  taxi_types = vars_obj.get("taxi_types", ["yellow"]) or ["yellow"]

  # Map taxi_type to filename prefix used by public TLC dataset
  prefix_map = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "for-hire": "fhv_tripdata",
    "for_hire": "fhv_tripdata",
  }

  base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/{prefix}_{yyyymm}.parquet"

  frames = []
  now = datetime.utcnow().isoformat()

  for taxi in taxi_types:
    prefix = prefix_map.get(taxi, taxi)
    for y, m in _months_between(start_dt, end_dt):
      yyyymm = f"{y:04d}-{m:02d}"
      url = base_url.format(prefix=prefix, yyyymm=yyyymm)
      logger.info("Fetching %s", url)
      try:
        with requests.get(url, stream=True, timeout=60) as r:
          if r.status_code != 200:
            logger.warning("Skipping %s: HTTP %s", url, r.status_code)
            continue
          with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            for chunk in r.iter_content(chunk_size=8192):
              if chunk:
                tmp.write(chunk)
            tmp_name = tmp.name
        df = pd.read_parquet(tmp_name)
        try:
          os.unlink(tmp_name)
        except Exception:
          pass
        if df is None or len(df) == 0:
          continue
        df["taxi_type"] = taxi
        df["extracted_at"] = now
        frames.append(df)
      except Exception as e:
        logger.warning("Failed to fetch/read %s: %s", url, e)
        continue

  if not frames:
    return pd.DataFrame()

  result = pd.concat(frames, ignore_index=True)
  return result


