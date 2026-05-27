import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = os.environ.get("BIGQUERY_DATASET", "flights_raw")
TABLE      = "flights"


def load_gcs_to_bq(gcs_uri: str, year: int, month: int) -> int:
    """Load CSV from GCS into BigQuery. Returns rows loaded."""
    client    = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()

    rows_loaded = load_job.output_rows
    logger.info(f"Loaded {rows_loaded:,} rows into {table_ref}")
    return rows_loaded


if __name__ == "__main__":
    import sys
    gcs_uri, year, month = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    load_gcs_to_bq(gcs_uri, year, month)
