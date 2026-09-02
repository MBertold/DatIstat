"""
DatIstat - Modulo di Trasformazione (Data Engineering - Silver Layer).

Esegue il parsing dei dati SDMX grezzi (JSON o CSV), la normalizzazione
delle dimensioni, il calcolo delle metriche derivate (inflazione congiunturale e tendenziale)
e persiste il dataset pulito e tipizzato nel formato colonnare compresso Parquet.
"""
import io
import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.config import (
    BRONZE_RAW_FILE,
    SILVER_PARQUET_FILE,
    ECOICOP_MAPPING,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def transform_bronze_to_silver() -> pd.DataFrame:
    """
    Legge i dati dal Bronze Layer, applica le trasformazioni di normalizzazione
    e scrive il Silver Layer in formato Parquet.
    """
    if not BRONZE_RAW_FILE.exists():
        raise FileNotFoundError(f"Bronze file non trovato in: {BRONZE_RAW_FILE}. Eseguire prima l'ingestion.")

    with open(BRONZE_RAW_FILE, "r", encoding="utf-8") as f:
        meta_payload = json.load(f)

    meta = meta_payload.get("_metadata", {})
    fmt = meta.get("format", "SDMX-CSV")
    logger.info(f"Avvio trasformazione Silver da sorgente: {meta.get('source')} (Formato: {fmt})")

    df_raw: pd.DataFrame
    if fmt == "SDMX-CSV":
        csv_path = Path(meta_payload["csv_path"])
        df_raw = pd.read_csv(csv_path)
    elif fmt == "SDMX-JSON":
        # Parsing payload SDMX-JSON 2.0 ISTAT
        data_block = meta_payload["data"]["data"]
        dataset = data_block["dataSets"][0]
        structure = data_block["structures"][0]["dimensions"]
        
        series_dims = structure["series"]
        obs_dims = structure["observation"]
        
        time_vals = [v["id"] for v in obs_dims[0]["values"]]
        coicop_dim_idx = next(i for i, d in enumerate(series_dims) if d["id"] == "E_COICOP_REV_ISTAT")
        coicop_vals = [v["id"] for v in series_dims[coicop_dim_idx]["values"]]
        
        records = []
        for s_key, s_data in dataset.get("series", {}).items():
            key_indices = [int(k) for k in s_key.split(":")]
            coicop_code = coicop_vals[key_indices[coicop_dim_idx]]
            
            for o_idx_str, o_vals in s_data.get("observations", {}).items():
                t_idx = int(o_idx_str)
                period = time_vals[t_idx]
                val = float(o_vals[0]) if o_vals[0] is not None else np.nan
                records.append({
                    "TIME_PERIOD": period,
                    "coicop": f"CP{coicop_code}" if not coicop_code.startswith("CP") else coicop_code,
                    "geo": "IT",
                    "OBS_VALUE": val,
                })
        df_raw = pd.DataFrame(records)
    else:
        raise ValueError(f"Formato non supportato: {fmt}")

    # Pulizia e tipizzazione
    logger.info(f"Righe grezze lette: {len(df_raw)}")
    df = df_raw.dropna(subset=["OBS_VALUE"]).copy()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["OBS_VALUE"]).copy()

    # Normalizzazione codici COICOP
    df["coicop"] = df["coicop"].astype(str).str.strip()

    # Mappatura dimensioni
    def get_ecoicop_info(c_code: str):
        info = ECOICOP_MAPPING.get(c_code)
        if not info:
            alt_code = f"CP{c_code}" if not c_code.startswith("CP") else c_code[2:]
            info = ECOICOP_MAPPING.get(alt_code, {
                "codice": c_code.replace("CP", ""),
                "nome_it": f"Categoria {c_code}",
                "nome_en": f"Category {c_code}",
                "is_generale": False,
            })
        return info

    df["codice_ecoicop"] = df["coicop"].apply(lambda c: get_ecoicop_info(c)["codice"])
    df["nome_categoria"] = df["coicop"].apply(lambda c: get_ecoicop_info(c)["nome_it"])
    df["nome_categoria_en"] = df["coicop"].apply(lambda c: get_ecoicop_info(c)["nome_en"])
    df["is_generale"] = df["coicop"].apply(lambda c: get_ecoicop_info(c)["is_generale"])

    # Normalizzazione temporale
    df["periodo"] = df["TIME_PERIOD"].astype(str)
    # Filtra solo periodi mensili validi YYYY-MM
    df = df[df["periodo"].str.contains(r"^\d{4}-\d{2}$")].copy()
    
    df["anno"] = df["periodo"].apply(lambda p: int(p.split("-")[0]))
    df["mese"] = df["periodo"].apply(lambda p: int(p.split("-")[1]))
    df["trimestre"] = df["mese"].apply(lambda m: (m - 1) // 3 + 1)
    df["data_riferimento"] = pd.to_datetime(df["periodo"] + "-01")

    # Territorio
    df["codice_territorio"] = df["geo"].astype(str)
    df["nome_territorio"] = df["codice_territorio"].map({"IT": "Italia"}).fillna("Italia")

    # Indice numerico
    df["indice"] = df["OBS_VALUE"].astype(float)

    # Ordinamento deterministico per calcolo metriche derivate
    df = df.sort_values(by=["codice_ecoicop", "data_riferimento"]).reset_index(drop=True)

    # Calcolo metriche derivate:
    # 1. Variazione congiunturale (mensile): (I_t / I_{t-1} - 1) * 100
    df["var_mensile_pct"] = df.groupby("codice_ecoicop")["indice"].pct_change(1) * 100
    df["var_mensile_pct"] = df["var_mensile_pct"].round(2)

    # 2. Variazione tendenziale (inflazione annua %): (I_t / I_{t-12} - 1) * 100
    df["inflazione_annua_pct"] = df.groupby("codice_ecoicop")["indice"].pct_change(12) * 100
    df["inflazione_annua_pct"] = df["inflazione_annua_pct"].round(2)

    # Selezione colonne per il Silver Layer
    cols_silver = [
        "periodo",
        "data_riferimento",
        "anno",
        "mese",
        "trimestre",
        "codice_territorio",
        "nome_territorio",
        "codice_ecoicop",
        "nome_categoria",
        "nome_categoria_en",
        "is_generale",
        "indice",
        "var_mensile_pct",
        "inflazione_annua_pct",
    ]
    df_silver = df[cols_silver].copy()

    # Salvataggio Silver Layer in Parquet
    SILVER_PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_silver.to_parquet(SILVER_PARQUET_FILE, index=False, engine="pyarrow", compression="snappy")
    
    file_size_kb = SILVER_PARQUET_FILE.stat().st_size / 1024
    logger.info(
        f"Silver Layer creato con successo in: {SILVER_PARQUET_FILE} "
        f"({len(df_silver)} righe, {file_size_kb:.1f} KB, "
        f"Periodo: {df_silver['periodo'].min()} -> {df_silver['periodo'].max()})"
    )
    return df_silver


if __name__ == "__main__":
    transform_bronze_to_silver()
