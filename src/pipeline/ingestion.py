"""
DatIstat - Modulo di Ingestion (Data Engineering - Bronze Layer).

Esegue l'estrazione da API SDMX REST (con failover automatico su canale SDMX
del Sistema Statistico Europeo qualora i server IstatData applichino rate limiting/firewall)
e persiste il payload originale non manipolato nel Bronze Layer con metadati di lineage.
"""
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import requests

from src.config import (
    ISTAT_DATA_URL,
    EUROSTAT_SDMX_URL,
    SDMX_HEADERS,
    API_TIMEOUT_SECONDS,
    MIN_REQUEST_INTERVAL_SECONDS,
    BRONZE_RAW_FILE,
    BRONZE_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_last_request_time: float = 0.0


def _enforce_rate_limit(min_interval: float = MIN_REQUEST_INTERVAL_SECONDS) -> None:
    """Garantisce una pausa prudenziale tra le chiamate di ingestion."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < min_interval:
        sleep_needed = min_interval - elapsed
        logger.info(f"Rate limiting attivo: attesa di {sleep_needed:.1f} secondi...")
        time.sleep(sleep_needed)
    _last_request_time = time.time()


def check_api_health() -> Dict[str, Any]:
    """Verifica lo stato dei server ISTAT e del canale di backup SDMX."""
    health_status = {"istat_primary": False, "eurostat_fallback": False}
    
    # Check Istat
    try:
        r = requests.head(ISTAT_DATA_URL, headers=SDMX_HEADERS, timeout=5)
        health_status["istat_primary"] = r.status_code in [200, 302, 307]
    except Exception:
        health_status["istat_primary"] = False

    # Check Eurostat SDMX
    try:
        r = requests.head(EUROSTAT_SDMX_URL, headers=SDMX_HEADERS, timeout=5)
        health_status["eurostat_fallback"] = r.status_code in [200, 302, 307]
    except Exception:
        health_status["eurostat_fallback"] = False

    logger.info(f"API Health check: {health_status}")
    return health_status


def fetch_raw_data(force_refresh: bool = False) -> Path:
    """
    Scarica i dati dei prezzi al consumo NIC mensili da ISTAT / SDMX.
    Salva il risultato grezzo nel Bronze Layer con metadati di audit.
    """
    if not force_refresh and BRONZE_RAW_FILE.exists():
        file_age_hours = (time.time() - BRONZE_RAW_FILE.stat().st_mtime) / 3600
        if file_age_hours < 24:
            logger.info(f"Utilizzo file Bronze esistente (età: {file_age_hours:.1f}h): {BRONZE_RAW_FILE}")
            return BRONZE_RAW_FILE

    _enforce_rate_limit()
    
    # 1. Tentativo primario su ISTAT SDMX REST
    logger.info(f"Tentativo ingestion da endpoint primario ISTAT: {ISTAT_DATA_URL}")
    try:
        t0 = time.time()
        resp = requests.get(ISTAT_DATA_URL, headers=SDMX_HEADERS, timeout=10)
        if resp.status_code == 200:
            duration = time.time() - t0
            payload = resp.json()
            bronze_payload = {
                "_metadata": {
                    "source": "ISTAT_SDMX_PRIMARY",
                    "url": ISTAT_DATA_URL,
                    "format": "SDMX-JSON",
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "response_time_sec": round(duration, 3),
                },
                "data": payload,
            }
            with open(BRONZE_RAW_FILE, "w", encoding="utf-8") as f:
                json.dump(bronze_payload, f, ensure_ascii=False)
            logger.info(f"Dati acquisiti da ISTAT con successo in {duration:.2f}s!")
            return BRONZE_RAW_FILE
    except Exception as e:
        logger.warning(f"Endpoint primario IstatData non ha risposto ({e}). Attivazione failover...")

    # 2. Canale failover su Eurostat SDMX (Fonte Istat per Italia IT)
    logger.info(f"Attivazione estrazione su canale SDMX Sistema Statistico Europeo (Eurostat Fonte ISTAT): {EUROSTAT_SDMX_URL}")
    t0 = time.time()
    resp = requests.get(EUROSTAT_SDMX_URL, headers=SDMX_HEADERS, timeout=API_TIMEOUT_SECONDS)
    resp.raise_for_status()
    duration = time.time() - t0
    
    csv_text = resp.text
    raw_csv_file = BRONZE_DIR / "nic_monthly_raw.csv"
    with open(raw_csv_file, "w", encoding="utf-8") as f:
        f.write(csv_text)

    bronze_payload = {
        "_metadata": {
            "source": "EUROSTAT_SDMX_SOURCE_ISTAT_IT",
            "url": EUROSTAT_SDMX_URL,
            "format": "SDMX-CSV",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "response_time_sec": round(duration, 3),
            "line_count": len(csv_text.strip().split("\n")),
        },
        "csv_path": str(raw_csv_file),
    }
    with open(BRONZE_RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(bronze_payload, f, ensure_ascii=False)

    logger.info(f"Bronze Layer salvato con successo: {BRONZE_RAW_FILE} e {raw_csv_file} ({duration:.2f}s, {bronze_payload['_metadata']['line_count']} righe)")
    return BRONZE_RAW_FILE


if __name__ == "__main__":
    fetch_raw_data(force_refresh=True)
