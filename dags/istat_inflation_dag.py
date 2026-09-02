"""
DatIstat - DAG Apache Airflow per l'Orchestrazione Automatica Mensile.

Schedula ed esegue la pipeline end-to-end con monitoraggio dei task:
1. Health Check API ISTAT / SDMX
2. Ingestion nel Bronze Layer (JSON/CSV con metadati)
3. Trasformazione e calcolo metriche nel Silver Layer (Parquet)
4. Modellazione analitica Star Schema nel Gold Layer (DuckDB)
5. Validazione bloccante Data Quality (Asserzioni e integrità)
6. Addestramento continuo e inferenza modelli di Machine Learning (Forecasting)
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Inclusione della root del progetto nel PYTHONPATH
DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.ingestion import check_api_health, fetch_raw_data
from src.pipeline.transform import transform_bronze_to_silver
from src.pipeline.database import build_star_schema
from src.pipeline.data_quality import run_data_quality_suite
from src.models.forecasting import train_and_evaluate_models


default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _task_health_check():
    health = check_api_health()
    if not (health.get("istat_primary") or health.get("eurostat_fallback")):
        raise ConnectionError("Nessun endpoint SDMX (ISTAT o Eurostat) è raggiungibile.")
    return health


def _task_ingest_bronze():
    bronze_path = fetch_raw_data(force_refresh=True)
    return str(bronze_path)


def _task_transform_silver():
    df_silver = transform_bronze_to_silver()
    return f"Silver generato con {len(df_silver)} righe."


def _task_build_gold_schema():
    build_star_schema()
    return "Star Schema Gold popolato con successo su DuckDB."


def _task_data_quality():
    report = run_data_quality_suite()
    if not report["all_passed"]:
        failed_tests = [t["test_name"] for t in report["tests"] if not t["passed"]]
        raise AssertionError(f"Pipeline interrotta: test di Data Quality falliti: {failed_tests}")
    return f"Tutti i {report['total_tests']} test superati con successo."


def _task_forecasting_models():
    summary, df_pred = train_and_evaluate_models()
    return f"Previsioni generate con successo con {summary['best_model']} (Orizzonte: {summary['forecast_horizon']} mesi)."


with DAG(
    dag_id="istat_monthly_inflation_pipeline",
    default_args=default_args,
    description="Pipeline mensile di Ingestion, Data Quality, DuckDB Star Schema e Machine Learning per dati ISTAT",
    schedule_interval="0 10 16 * *",  # Esegue ogni 16 del mese alle 10:00 (calendario ISTAT)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["istat", "data_engineering", "data_science", "duckdb", "streamlit"],
) as dag:

    t1_check_api = PythonOperator(
        task_id="t1_check_api_health",
        python_callable=_task_health_check,
    )

    t2_ingest_bronze = PythonOperator(
        task_id="t2_ingest_bronze",
        python_callable=_task_ingest_bronze,
    )

    t3_transform_silver = PythonOperator(
        task_id="t3_transform_silver",
        python_callable=_task_transform_silver,
    )

    t4_build_gold = PythonOperator(
        task_id="t4_build_gold_schema",
        python_callable=_task_build_gold_schema,
    )

    t5_data_quality = PythonOperator(
        task_id="t5_run_data_quality_suite",
        python_callable=_task_data_quality,
    )

    t6_forecasting = PythonOperator(
        task_id="t6_run_forecasting_models",
        python_callable=_task_forecasting_models,
    )

    # Definizione delle dipendenze sequenziali (DAG)
    t1_check_api >> t2_ingest_bronze >> t3_transform_silver >> t4_build_gold >> t5_data_quality >> t6_forecasting
