"""
Gold Price ETL Pipeline
=======================
Extracts gold futures (GC=F) from Yahoo Finance and VIX/DGS10 from FRED,
transforms and aligns the data, then loads it as partitioned Parquet files
into a local data lake.
"""

from __future__ import annotations

import argparse
import glob
import importlib
import io
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from time import sleep
import click  # type: ignore
import duckdb  # type: ignore[import]
import pandas as pd  # type: ignore[import]
import requests
import yfinance as yf  # type: ignore[import]
try:
    load_dotenv = importlib.import_module("dotenv").load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs) -> None:
        return None
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as UrllibRetry
try:
    tenacity = importlib.import_module("tenacity")
    retry = tenacity.retry
    stop_after_attempt = tenacity.stop_after_attempt
    wait_exponential = tenacity.wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator

    def stop_after_attempt(_):
        return None

    def wait_exponential(*args, **kwargs):
        return None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FRED_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

YF_TICKER = "GC=F"
FRED_TICKERS = ["VIXCLS", "DGS10"]

COLUMN_MAP: dict[str, str] = {
    "GC=F": "gold_close",
    "VIXCLS": "vix_close",
    "DGS10": "dgs10_close",
}

PARQUET_GLOB = "data_lake/**/*.parquet"
PARTITION_COL = "Year"

_DATE_COLS = ", ".join(
    c for c in ("Date", "gold_close", "vix_close", "dgs10_close", "_etl_loaded_at", PARTITION_COL)
)

# Batch size for API requests (not directly used by yfinance but a safety ceiling)
_MAX_RETRIES = 3
_REQUESTS_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Shared HTTP session (connection reuse + retries)
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    """Return a requests.Session with retry and connection-pooling baked in."""
    s = requests.Session()
    retries = UrllibRetry(
        total=_MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
    )
    adapter = HTTPAdapter(max_retries=retries,
                          pool_connections=5, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """Lightweight holder for source identifiers and date range."""

    start: pd.Timestamp
    end: pd.Timestamp
    yf_ticker: str = YF_TICKER
    fred_tickers: tuple[str, ...] = field(
        default_factory=lambda: tuple(FRED_TICKERS))


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(_MAX_RETRIES),
    reraise=True,
)
def extract_yfinance(cfg: SourceConfig) -> pd.DataFrame:
    """Download OHLCV data from Yahoo Finance for the configured ticker."""
    log.info("Downloading %s from Yahoo Finance …", cfg.yf_ticker)
    df = yf.download(
        cfg.yf_ticker,
        start=cfg.start,
        end=cfg.end,
        progress=False,
        auto_adjust=False,
    )
    if df is None or df.empty:
        log.info("YFinance returned empty data (Weekend/Holiday).")
        return pd.DataFrame(columns=["Close"])
    return df


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(_MAX_RETRIES),
    reraise=True,
)
def fetch_fred_csv(ticker: str, cfg: SourceConfig) -> pd.DataFrame:
    """Fetch a single FRED series as a DataFrame indexed by observation_date."""
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY not found in .env file")

    params: dict[str, str | None] = {
        "id": ticker,
        "api_key": api_key,
        "cosd": cfg.start.strftime("%Y-%m-%d"),
        "coed": cfg.end.strftime("%Y-%m-%d"),
    }
    log.info("Fetching FRED series %s …", ticker)
    resp = _session().get(FRED_BASE_URL, params=params, timeout=_REQUESTS_TIMEOUT)
    resp.raise_for_status()

    # A simple integrity check: the CSV must contain the expected date column
    sample = resp.text[:512]
    if "observation_date" not in sample:
        raise RuntimeError(
            f"FRED response for {ticker} does not contain 'observation_date'. "
            "Check FRED_API_KEY validity."
        )

    df = pd.read_csv(
        io.BytesIO(resp.content),
        parse_dates=["observation_date"],
        index_col="observation_date",
    )
    return df


def extract_fred(cfg: SourceConfig) -> pd.DataFrame:
    """Download all configured FRED series and merge on their date index."""
    frames = [fetch_fred_csv(t, cfg) for t in cfg.fred_tickers]
    merged = pd.concat(frames, axis=1, sort=False)
    return merged


def extract(cfg: SourceConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Orchestrate extraction from both sources."""
    yf_raw = extract_yfinance(cfg)
    fred_raw = extract_fred(cfg)
    return yf_raw, fred_raw


# ---------------------------------------------------------------------------
# Transformer
# ---------------------------------------------------------------------------

def _flatten_yfinance(yf_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise the yfinance DataFrame to a single-level column with a
    predictable 'gold_close' column.
    """
    if isinstance(yf_raw.columns, pd.MultiIndex):
        # Newer yfinance returns multi-index columns
        return yf_raw["Close"][YF_TICKER].to_frame(name=COLUMN_MAP[YF_TICKER])
    # Older / simpler response
    return yf_raw[["Close"]].rename(columns={"Close": COLUMN_MAP[YF_TICKER]})


def _rename_fred(fred_raw: pd.DataFrame) -> pd.DataFrame:
    return fred_raw.rename(columns=COLUMN_MAP)


def transform(yf_raw: pd.DataFrame, fred_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean, align, and merge the two source DataFrames.
    """
    log.info("Transforming data …")

    yf_close = _flatten_yfinance(yf_raw)
    fred = _rename_fred(fred_raw)

    # Ghost-data circuit breaker
    if yf_close.empty or yf_close["gold_close"].isna().all():
        log.info("[INFO] YFinance returned empty data (Weekend/Holiday). Exiting gracefully.")
        return pd.DataFrame(columns=["gold_close", "vix_close", "dgs10_close"])

    # Merge on the datetime index — pandas handles this natively
    df = yf_close.join(fred, how="outer")

    # Market-first rules: drop rows without gold data, then forward-fill
    df = df.dropna(subset=['gold_close'])

    # THE GRACEFUL EXIT: If the market was closed (weekend/holiday), exit cleanly.
    if df.empty:
        print("[INFO] No new market data found (likely a weekend/holiday). Pipeline finished gracefully.")
        # Return an empty DataFrame with the expected schema instead of None
        return df.reset_index()

    log.info("Transformed %d rows × %d columns", len(df), len(df.columns))

    # Provenance & partitioning
    df["_etl_loaded_at"] = pd.Timestamp.now(tz=timezone.utc)
    # Ensure we access .year on a DatetimeIndex for static type checkers
    df[PARTITION_COL] = pd.DatetimeIndex(df.index).year

    return df.reset_index()  # Date → column for DuckDB


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def _existing_parquet_count() -> int:
    return len(glob.glob(PARQUET_GLOB, recursive=True))


def _dedup_and_merge(con: duckdb.DuckDBPyConnection, new_df: pd.DataFrame) -> None:
    """
    Merge new data into the existing Parquet data lake, deduplicating on Date
    (keeping the latest _etl_loaded_at per date).
    """
    con.register("new_data", new_df)

    # COALESCE guards against parquet files where _etl_loaded_at is NULL
    con.execute(f"""
        CREATE OR REPLACE TABLE merged_data AS
        SELECT {_DATE_COLS} FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY Date ORDER BY _etl_loaded_at DESC
                   ) AS rn
            FROM (
                SELECT {_DATE_COLS.replace("_etl_loaded_at",
                                           "COALESCE(_etl_loaded_at, TIMESTAMP '1970-01-01 00:00:00') AS _etl_loaded_at")}
                FROM read_parquet('{PARQUET_GLOB}')
                UNION ALL
                SELECT {_DATE_COLS} FROM new_data
            )
        ) WHERE rn = 1
    """)

    con.execute(f"""
        COPY (
    SELECT * FROM merged_data
) TO 'data_lake/' 
(FORMAT PARQUET, PARTITION_BY ({PARTITION_COL}), OVERWRITE_OR_IGNORE)
    """)


def _fresh_load(con: duckdb.DuckDBPyConnection, new_df: pd.DataFrame) -> None:
    """Write the DataFrame as a brand-new partitioned data lake."""
    con.register("new_data", new_df)
    con.execute(f"""
        COPY (SELECT * FROM new_data) TO 'data_lake/'
        (FORMAT PARQUET, PARTITION_BY ({PARTITION_COL}), OVERWRITE_OR_IGNORE)
    """)


def load(df: pd.DataFrame, overwrite: bool) -> None:
    """Persist the transformed DataFrame as partitioned Parquet files."""
    if df.empty:
        raise ValueError("CRITICAL: DataFrame is empty — nothing to load.")

    log.info("Saving %d rows to data lake …", len(df))

    con = duckdb.connect()
    try:
        has_existing = _existing_parquet_count() > 0

        if has_existing and not overwrite:
            _dedup_and_merge(con, df)
            log.info("Merged new data with existing Parquet files.")
        else:
            _fresh_load(con, df)
            log.info("Fresh load complete.")

        file_count = _existing_parquet_count()
        log.info("Data lake now contains %d Parquet file(s).", file_count)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


@click.command()
@click.option("--start", default="2015-01-01", type=click.DateTime(), help="Start date (inclusive)")
@click.option("--end", default=None, type=click.DateTime(), help="End date (inclusive, defaults to today)")
@click.option("--overwrite", is_flag=True, help="Delete existing data lake and rebuild from scratch")
def run_pipeline(start: datetime, end: datetime | None, overwrite: bool) -> None:
    """Run the full gold-price ETL pipeline."""
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end) if end else pd.Timestamp.today()
    cfg = SourceConfig(start=start_ts, end=end_ts)

    log.info("Pipeline started — range: %s → %s",
             start_ts.date(), end_ts.date())

    # 1. Extract
    yf_raw, fred_raw = extract(cfg)

    # 2. Transform
    df = transform(yf_raw, fred_raw)

    # THE FINAL GRACEFUL EXIT: If the DataFrame is empty after transform, stop here.
    if df.empty:
        print("[INFO] No data to load. Exiting gracefully.")
        return
    
    # 3. Load
    load(df, overwrite)

    log.info("Pipeline finished successfully.")


if __name__ == "__main__":
    load_dotenv()
    run_pipeline()
