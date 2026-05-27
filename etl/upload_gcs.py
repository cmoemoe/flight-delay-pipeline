import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]


def upload_to_gcs(local_path: Path, year: int, month: int) -> str:
    """Upload CSV to GCS under raw/year=YYYY/month=MM/. Returns GCS URI."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob_name = f"raw/year={year}/month={month:02d}/{local_path.name}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    logger.info(f"Uploaded to {gcs_uri}")
    return gcs_uri


if __name__ == "__main__":
    import sys
    path, year, month = Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    upload_to_gcs(path, year, month)