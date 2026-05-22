from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL", "")],
}


def _get_period(**context):
    """Derive year/month to load — always the month prior to execution date."""
    exec_date = context["data_interval_start"]
    if exec_date.month == 1:
        year, month = exec_date.year - 1, 12
    else:
        year, month = exec_date.year, exec_date.month - 1
    logger.info(f"Loading period: {year}-{month:02d}")
    context["ti"].xcom_push(key="year",  value=year)
    context["ti"].xcom_push(key="month", value=month)


def _download(**context):
    from etl.download import download_month
    ti    = context["ti"]
    year  = ti.xcom_pull(key="year",  task_ids="get_period")
    month = ti.xcom_pull(key="month", task_ids="get_period")
    path  = download_month(year, month, output_dir="/tmp")
    ti.xcom_push(key="csv_path", value=str(path))


def _validate(**context):
    from etl.download import validate_file
    ti       = context["ti"]
    csv_path = Path(ti.xcom_pull(key="csv_path", task_ids="download"))
    result   = validate_file(csv_path)
    ti.xcom_push(key="row_count", value=result["row_count"])


def _upload_gcs(**context):
    from etl.upload_gcs import upload_to_gcs
    ti       = context["ti"]
    csv_path = Path(ti.xcom_pull(key="csv_path", task_ids="download"))
    year     = ti.xcom_pull(key="year",  task_ids="get_period")
    month    = ti.xcom_pull(key="month", task_ids="get_period")
    gcs_uri  = upload_to_gcs(csv_path, year, month)
    ti.xcom_push(key="gcs_uri", value=gcs_uri)


def _load_bigquery(**context):
    from etl.load_bigquery import load_gcs_to_bq
    ti      = context["ti"]
    gcs_uri = ti.xcom_pull(key="gcs_uri", task_ids="upload_gcs")
    year    = ti.xcom_pull(key="year",  task_ids="get_period")
    month   = ti.xcom_pull(key="month", task_ids="get_period")
    rows    = load_gcs_to_bq(gcs_uri, year, month)
    ti.xcom_push(key="rows_loaded", value=rows)


def _run_dbt(**context):
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt"],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")


def _test_dbt(**context):
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt"],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")


with DAG(
    dag_id="flight_delay_pipeline",
    default_args=default_args,
    description="Monthly BTS flight delay ETL: download -> GCS -> BigQuery -> dbt",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "flights", "bts"],
) as dag:

    get_period = PythonOperator(task_id="get_period",    python_callable=_get_period)
    download   = PythonOperator(task_id="download",      python_callable=_download)
    validate   = PythonOperator(task_id="validate",      python_callable=_validate)
    upload_gcs = PythonOperator(task_id="upload_gcs",    python_callable=_upload_gcs)
    load_bq    = PythonOperator(task_id="load_bigquery", python_callable=_load_bigquery)
    run_dbt    = PythonOperator(task_id="dbt_run",       python_callable=_run_dbt)
    test_dbt   = PythonOperator(task_id="dbt_test",      python_callable=_test_dbt)

    get_period >> download >> validate >> upload_gcs >> load_bq >> run_dbt >> test_dbt