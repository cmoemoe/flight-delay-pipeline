import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

KEEP_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "OriginCityName", "OriginState",
    "Dest", "DestCityName", "DestState",
    "CRSDepTime", "DepTime", "DepDelay", "DepDelayMinutes",
    "CRSArrTime", "ArrTime", "ArrDelay", "ArrDelayMinutes",
    "Cancelled", "CancellationCode", "Diverted",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "AirTime", "Distance",
]


def download_month(year: int, month: int, output_dir: str = "/tmp") -> Path:
    """Download and extract BTS CSV for a single month. Returns path to CSV."""
    url = BTS_URL.format(year=year, month=month)
    logger.info(f"Downloading BTS data: {url}")

    response = requests.get(url, timeout=180)
    if response.status_code != 200:
        raise RuntimeError(f"BTS download failed ({response.status_code}): {url}")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV found inside BTS zip archive")
        output_path = Path(output_dir) / f"bts_{year}_{month:02d}.csv"
        with zf.open(csv_names[0]) as src, open(output_path, "wb") as dst:
            dst.write(src.read())

    logger.info(f"Extracted to {output_path}")
    return output_path


def validate_file(csv_path: Path) -> dict:
    """Validate file exists, columns are correct, row count is reasonable."""
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    df_sample = pd.read_csv(csv_path, nrows=5)
    missing = [c for c in KEEP_COLUMNS if c not in df_sample.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    row_count = sum(1 for _ in open(csv_path)) - 1  # exclude header
    if row_count < 1000:
        raise ValueError(f"Suspiciously low row count: {row_count:,}")

    logger.info(f"Validation passed: {row_count:,} rows — {csv_path.name}")
    return {"row_count": row_count, "path": str(csv_path)}


if __name__ == "__main__":
    import sys
    year, month = int(sys.argv[1]), int(sys.argv[2])
    path = download_month(year, month)
    validate_file(path)