"""
DatIstat - End-to-End Pipeline Runner.

Esegue l'intero flusso dati in sequenza controllata:
1. Health Check API
2. Ingestion (Bronze Layer - JSON/CSV grezzo con metadati di audit)
3. Trasformazione e Normalizzazione (Silver Layer - Parquet compresso)
4. Modellazione Star Schema (Gold Layer - DuckDB)
5. Suite di validazione Data Quality (Asserzioni e regole d'integrità)
6. Addestramento e Rollout Modelli Machine Learning (Gold Previsioni)
"""
import logging
import sys
import time

from src.pipeline.ingestion import check_api_health, fetch_raw_data
from src.pipeline.transform import transform_bronze_to_silver
from src.pipeline.database import build_star_schema
from src.pipeline.data_quality import run_data_quality_suite
from src.models.forecasting import train_and_evaluate_models

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DatIstatPipeline")


def run_pipeline(force_refresh: bool = False) -> bool:
    """Esegue l'intera pipeline di dati end-to-end."""
    start_total = time.time()
    logger.info("=" * 65)
    logger.info("   AVVIO PIPELINE END-TO-END DATISTAT (DE + DS + DA)")
    logger.info("=" * 65)

    # Step 1: Health Check
    logger.info("[STEP 1/6] Verifica connettività API...")
    check_api_health()

    # Step 2: Ingestion Bronze
    logger.info("[STEP 2/6] Ingestion dati nel Bronze Layer...")
    bronze_path = fetch_raw_data(force_refresh=force_refresh)
    logger.info(f"Bronze Layer pronto: {bronze_path.name}")

    # Step 3: Transformation Silver
    logger.info("[STEP 3/6] Normalizzazione e calcolo metriche (Silver Parquet)...")
    df_silver = transform_bronze_to_silver()
    logger.info(f"Silver Layer creato con {len(df_silver)} record.")

    # Step 4: Data Modeling Gold
    logger.info("[STEP 4/6] Popolamento Star Schema analitico in DuckDB (Gold)...")
    build_star_schema()

    # Step 5: Data Quality Suite
    logger.info("[STEP 5/6] Esecuzione suite di test Data Quality...")
    dq_report = run_data_quality_suite()
    if not dq_report["all_passed"]:
        logger.error("FALLITA VALIDAZIONE DATA QUALITY! Interruzione pipeline.")
        return False
    logger.info(f"Tutti i {dq_report['total_tests']} test di integrità sono stati SUPERATI.")

    # Step 6: Data Science Forecasting
    logger.info("[STEP 6/6] Esecuzione modelli di Machine Learning e Time Series...")
    ds_summary, df_pred = train_and_evaluate_models()
    logger.info(
        f"Modello migliore: {ds_summary['best_model']} "
        f"(RMSE: {ds_summary['metrics'][ds_summary['best_model']]['rmse']:.3f})"
    )
    logger.info(f"Generate {len(df_pred)} previsioni per il periodo {ds_summary['forecast_period_start']} -> {ds_summary['forecast_period_end']}")

    elapsed = time.time() - start_total
    logger.info("=" * 65)
    logger.info(f"   PIPELINE COMPLETATA CON SUCCESSO IN {elapsed:.2f} SECONDI!")
    logger.info("=" * 65)
    return True


if __name__ == "__main__":
    force = "--force" in sys.argv
    success = run_pipeline(force_refresh=force)
    sys.exit(0 if success else 1)
