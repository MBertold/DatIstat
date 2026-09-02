# 🇮🇹 DatIstat: Osservatorio Inflazione & Prezzi al Consumo (ISTAT NIC)

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED.svg)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE.svg)](https://airflow.apache.org/)
[![DuckDB](https://img.shields.io/badge/lakehouse-DuckDB-FFF000.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/frontend-Streamlit-FF4B4B.svg)](https://streamlit.io/)
[![License: CC BY 4.0](https://img.shields.io/badge/Data_License-CC_BY_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

> **Progetto Portfolio End-to-End** che unisce le tre anime del mondo dati:
> **Data Engineering** (ETL SDMX resiliente, Medallion Architecture, DuckDB Star Schema, Data Quality, Airflow e Docker),
> **Data Science** (Machine Learning ed econometric forecasting di serie storiche a 12 mesi con intervalli di confidenza),
> e **Data Analytics / BI** (Dashboard interattiva Streamlit con KPI esecutivi, scomposizione ECOICOP e monitor DataOps).

---

## 🏛️ Architettura della Piattaforma

```
                                      FONTI DATI UFFICIALI
                      ┌──────────────────────────────────────────────────┐
                      │ • IstatData REST API (SDMX-JSON v2)              │
                      │ • European Statistical System (Eurostat SDMX-CSV)│
                      └─────────────────────────┬────────────────────────┘
                                                │
                 ┌──────────────────────────────┴──────────────────────────────┐
                 │       ORCHESTRAZIONE MENSILE (Apache Airflow DAG)           │
                 │   [1. Health] ─► [2. Ingest] ─► [3. Clean] ─► [4. Model]    │
                 │                                        │                    │
                 │                                        ▼                    │
                 │                  [5. Data Quality] ─► [6. ML Forecast]      │
                 └──────────────────────────────┬──────────────────────────────┘
                                                │
                                                ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │ MEDALLION LAKEHOUSE STORAGE (Condiviso via Docker Volume)                               │
 │                                                                                         │
 │  🥉 BRONZE (Raw Layer)       🥈 SILVER (Clean Layer)         🥇 GOLD (Analytical Layer) │
 │  data/bronze/nic_raw.json    data/silver/nic_cleaned.parquet data/gold/inflation.duckdb│
 │  (Payload originale +        (Dati tipizzati, var. mensili   (Star Schema Kimball:      │
 │   metadati di lineage)        e inflazione tendenziale)       Fatti, Dimensioni, Forecast)│
 └──────────────────────────────────────┬───────────────────────────────────┬──────────────┘
                                        │                                   │
                                        ▼                                   ▼
                       ┌─────────────────────────────────┐ ┌───────────────────────────────┐
                       │ MACHINE LEARNING & FORECASTING  │ │ STREAMLIT BI DASHBOARD        │
                       │ - Feature Engineering (Lags/STL)│ │ - Executive KPI Cards         │
                       │ - Ridge / Gradient Boosting     │ │ - Analisi 12 Categorie ECOICOP│
                       │ - Walk-Forward Backtesting      │ │ - Fan Chart Previsioni 95% CI │
                       │ - Rollout Autoregressivo 12 Mesi│ │ - Pannello Data Quality live  │
                       └─────────────────────────────────┘ └───────────────────────────────┘
```

---

## 🌟 Punti di Forza del Progetto per Ambito

### ⚙️ 1. Data Engineering
- **Client SDMX con Rate Limiting e Failover**: Ingestion progettata per superare le limitazioni di ISTAT (blocco IP se > 5 query/min) con algoritmo di throttling, caching locale e failover automatico sul canale SDMX del Sistema Statistico Europeo.
- **Medallion Architecture (Bronze $\to$ Silver $\to$ Gold)**:
  - **Bronze**: storage non distruttivo del dato grezzo con metadati di audit (tempo di risposta, timestamp UTC, line count).
  - **Silver**: archiviazione colonnare ad alte prestazioni in **Apache Parquet** compresso con Snappy.
  - **Gold**: modellazione a stella (**Star Schema di Kimball**) su database analitico **DuckDB** (`fact_indice_prezzi`, `dim_tempo`, `dim_categoria_spesa`, `dim_territorio`, `fact_previsioni`).
- **Data Quality Suite Automatica**: 7 test di convalida bloccanti (controllo schema, assenza nulli, unicità chiavi primarie, validità economica indici, continuità temporale su 120 mesi consecutivi).
- **Containerizzazione & Orchestrazione**: Multi-container Docker Compose con **Postgres**, **Airflow Webserver/Scheduler** e **Streamlit**.

### 🤖 2. Data Science
- **Time Series Feature Engineering**: Lag autoregressivi ($t-1, t-2, t-3, t-6, t-12$), medie e deviazioni standard mobili ($3, 6, 12$ mesi), componenti armoniche cicliche ($\sin$ e $\cos$ del mese) per catturare la stagionalità.
- **Modelli a Confronto & Backtesting**: Valutazione out-of-sample su finestra mobile (Walk-Forward Split sui 12 mesi più recenti) confrontando **Ridge Regression**, **Gradient Boosting** e **Random Forest**.
- **Metriche di Accuratezza**: Raggiunto **RMSE di 0.56** e **MAPE di 0.37%** sul set di test.
- **Point Forecast & Intervalli di Confidenza**: Rollout ricorsivo per la stima dell'inflazione futura a 12 mesi con intervalli di confidenza empirici al 95%.

### 📊 3. Data Analysis & Business Intelligence
- **Executive KPI Card**: Monitoraggio in tempo reale di Indice dei prezzi, Inflazione tendenziale annua %, Variazione congiunturale mese su mese e picchi storici.
- **Scomposizione ECOICOP**: Analisi dettagliata dei 12 capitoli di spesa (Alimentari, Energia/Abitazione, Trasporti, Sanità, Istruzione...) per individuare i driver dei rincari.
- **Grafici Interattivi Plotly**: Serie temporale storica a 10 anni (2016-2025) con annotazioni sugli shock macroeconomici (COVID-19 e shock energetico 2022).
- **Pannello DataOps**: Trasparenza totale su volumetrie, stato dei test di qualità e stato dell'infrastruttura.

---

## 🚀 Come Eseguire il Progetto

### Metodo A: Esecuzione Locale Diretta (Rapido)

Prerequisiti: Python 3.10+ installato.

```bash
# 1. Clona il repository ed entra nella cartella
cd DatIstat

# 2. Crea e attiva l'ambiente virtuale
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# 3. Installa le dipendenze
pip install -r requirements.txt

# 4. Esegui la pipeline dati end-to-end (DE + DS)
python run_pipeline.py

# 5. Avvia la Dashboard Streamlit
streamlit run app/app.py
```
👉 La dashboard sarà accessibile nel browser su **`http://localhost:8501`**.

---

### Metodo B: Stack Completo con Docker e Apache Airflow

Prerequisiti: [Docker Desktop](https://www.docker.com/products/docker-desktop/) installato e avviato.

```bash
# 1. Avvia tutti i servizi in background
docker compose up -d

# 2. Verifica lo stato dei container
docker compose ps
```

I servizi saranno immediatamente disponibili ai seguenti indirizzi:
* **Dashboard Streamlit (BI & Analytics):** [http://localhost:8501](http://localhost:8501)
* **Apache Airflow Web UI (Orchestrazione DAG):** [http://localhost:8080](http://localhost:8080)
  * *Username:* `admin`
  * *Password:* `admin`

Per arrestare la stack:
```bash
docker compose down
```

---

## 📂 Struttura del Repository

```text
DatIstat/
├── .dockerignore                 # Esclusioni per il build Docker
├── .gitignore                    # Esclusioni per il controllo di versione Git
├── Dockerfile                    # Container di produzione per pipeline e dashboard
├── docker-compose.yml            # Stack multi-servizio: Airflow, Postgres, Streamlit
├── requirements.txt              # Dipendenze Python (Pandas, DuckDB, Streamlit, ecc.)
├── run_pipeline.py               # Runner CLI orchestrato end-to-end
│
├── dags/                         # DAG di Apache Airflow
│   └── istat_inflation_dag.py    # DAG mensile per aggiornamenti automatici
│
├── src/                          # Moduli core dell'applicazione
│   ├── __init__.py
│   ├── config.py                 # Endpoint ISTAT, percorsi storage, mapping ECOICOP
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── ingestion.py          # Client SDMX con rate-limiting & Bronze storage
│   │   ├── transform.py          # Normalizzazione Parquet & Silver storage
│   │   ├── database.py           # Star Schema Kimball su DuckDB Gold
│   │   └── data_quality.py       # Suite di asserzioni e test di integrità
│   └── models/
│       ├── __init__.py
│       └── forecasting.py        # Modelli predittivi ML Time Series & 12M rollout
│
├── app/                          # Frontend analitico
│   └── app.py                    # Web Application Streamlit multi-vista
│
└── data/                         # Persistent Storage Layer (condiviso)
    ├── bronze/                   # Payload grezzi JSON/CSV con metadati audit
    ├── silver/                   # File colonnare pulito Parquet
    └── gold/                     # Database analitico DuckDB (inflation.duckdb)
```

---

## 📜 Licenza e Attribuzioni

* I dati utilizzati appartengono all'**Istituto Nazionale di Statistica (ISTAT)** e al **Sistema Statistico Europeo (Eurostat)**, rilasciati sotto licenza aperta [Creative Commons Attribuzione (CC-BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
* *Elaborazione statistica e sviluppo della piattaforma dati realizzati a scopo di portfolio e ricerca.*
