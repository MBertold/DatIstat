"""
DatIstat - Modulo Database & Modellazione Star Schema (Data Engineering - Gold Layer).

Utilizza DuckDB per creare e popolare lo schema analitico a stella (Kimball),
garantendo massime prestazioni per query analitiche e dashboard BI.
"""
import logging
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from src.config import (
    SILVER_PARQUET_FILE,
    GOLD_DUCKDB_FILE,
    GOLD_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MESI_ITALIANI = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
    5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
    9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
}


def get_db_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Restituisce una connessione al database DuckDB del Gold Layer."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(GOLD_DUCKDB_FILE), read_only=read_only)


def build_star_schema() -> None:
    """
    Legge il file Silver Parquet e popola lo Star Schema in DuckDB:
    - dim_tempo
    - dim_categoria_spesa
    - dim_territorio
    - fact_indice_prezzi
    - fact_previsioni (struttura pronta per modulo DS)
    """
    if not SILVER_PARQUET_FILE.exists():
        raise FileNotFoundError(f"File Silver non trovato: {SILVER_PARQUET_FILE}. Eseguire prima la trasformazione.")

    logger.info(f"Costruzione Star Schema Gold in: {GOLD_DUCKDB_FILE}")
    conn = get_db_connection(read_only=False)

    try:
        # Carica Silver Parquet come view temporanea
        conn.execute(f"CREATE OR REPLACE VIEW v_silver AS SELECT * FROM read_parquet('{SILVER_PARQUET_FILE.as_posix()}')")

        # 1. Tabella di Dimensione: TEMPO
        conn.execute("""
            CREATE OR REPLACE TABLE dim_tempo AS
            SELECT DISTINCT
                periodo AS id_tempo,
                anno,
                mese,
                trimestre,
                CAST(data_riferimento AS DATE) AS data_riferimento,
                CASE mese
                    WHEN 1 THEN 'Gennaio' WHEN 2 THEN 'Febbraio' WHEN 3 THEN 'Marzo'
                    WHEN 4 THEN 'Aprile' WHEN 5 THEN 'Maggio' WHEN 6 THEN 'Giugno'
                    WHEN 7 THEN 'Luglio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Settembre'
                    WHEN 10 THEN 'Ottobre' WHEN 11 THEN 'Novembre' WHEN 12 THEN 'Dicembre'
                END AS nome_mese,
                printf('%02d/%04d', mese, anno) AS mese_anno_label
            FROM v_silver
            ORDER BY id_tempo ASC;
        """)

        # 2. Tabella di Dimensione: CATEGORIA SPESA (ECOICOP)
        conn.execute("""
            CREATE OR REPLACE TABLE dim_categoria_spesa AS
            SELECT DISTINCT
                codice_ecoicop,
                nome_categoria,
                nome_categoria_en,
                is_generale
            FROM v_silver
            ORDER BY codice_ecoicop ASC;
        """)

        # 3. Tabella di Dimensione: TERRITORIO
        conn.execute("""
            CREATE OR REPLACE TABLE dim_territorio AS
            SELECT DISTINCT
                codice_territorio,
                nome_territorio
            FROM v_silver;
        """)

        # 4. Tabella dei Fatti: INDICE PREZZI
        conn.execute("""
            CREATE OR REPLACE TABLE fact_indice_prezzi AS
            SELECT
                periodo AS id_tempo,
                codice_ecoicop,
                codice_territorio,
                indice,
                var_mensile_pct,
                inflazione_annua_pct
            FROM v_silver
            ORDER BY id_tempo ASC, codice_ecoicop ASC;
        """)

        # 5. Tabella dei Fatti: PREVISIONI INFLAZIONE (predisposta per Data Science)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_previsioni (
                id_tempo VARCHAR,
                data_riferimento DATE,
                orizzonte_mesi INTEGER,
                codice_ecoicop VARCHAR,
                inflazione_prevista DOUBLE,
                intervallo_inf DOUBLE,
                intervallo_sup DOUBLE,
                modello VARCHAR,
                data_generazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Statistiche di verifica
        count_facts = conn.execute("SELECT COUNT(*) FROM fact_indice_prezzi").fetchone()[0]
        count_tempo = conn.execute("SELECT COUNT(*) FROM dim_tempo").fetchone()[0]
        count_cat = conn.execute("SELECT COUNT(*) FROM dim_categoria_spesa").fetchone()[0]

        logger.info(
            f"Star Schema popolato con successo! "
            f"Fatti: {count_facts} righe | Tempi: {count_tempo} | Categorie: {count_cat}"
        )
    finally:
        conn.close()


def query_kpis() -> dict:
    """Restituisce i KPI esecutivi più recenti per la dashboard."""
    conn = get_db_connection(read_only=True)
    try:
        query = """
            SELECT 
                f.id_tempo,
                f.indice,
                f.var_mensile_pct,
                f.inflazione_annua_pct
            FROM fact_indice_prezzi f
            JOIN dim_categoria_spesa c ON f.codice_ecoicop = c.codice_ecoicop
            WHERE c.is_generale = TRUE
            ORDER BY f.id_tempo DESC
            LIMIT 2;
        """
        rows = conn.execute(query).fetchall()
        if not rows:
            return {}

        current = rows[0]
        prev = rows[1] if len(rows) > 1 else None

        return {
            "periodo_corrente": current[0],
            "indice_corrente": current[1],
            "var_mensile": current[2],
            "inflazione_annua": current[3],
            "indice_precedente": prev[1] if prev else None,
            "inflazione_precedente": prev[3] if prev else None,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    build_star_schema()
    kpis = query_kpis()
    print("KPIs estratti:", kpis)
