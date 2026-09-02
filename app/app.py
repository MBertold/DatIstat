"""
DatIstat - BI Dashboard & Analytical Platform (Streamlit).

Applicazione interattiva per Data Analysis, Data Engineering monitoring
e visualizzazione dei modelli di Time Series Machine Learning.
"""
import sys
from pathlib import Path

# Assicura importazione corretta dei moduli src
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.config import GOLD_DUCKDB_FILE, SILVER_PARQUET_FILE, BRONZE_RAW_FILE
from src.pipeline.database import get_db_connection, query_kpis
from src.pipeline.data_quality import run_data_quality_suite

# Configurazione Pagina Streamlit
st.set_page_config(
    page_title="Osservatorio Inflazione ISTAT | Data Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling CSS
st.markdown("""
    <style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        border-radius: 8px;
        padding: 16px;
        border-left: 5px solid #2563EB;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=600)
def load_all_data():
    """Carica i dati dallo Star Schema di DuckDB."""
    if not GOLD_DUCKDB_FILE.exists():
        return None, None, None, None
    
    conn = get_db_connection(read_only=True)
    try:
        df_facts = conn.execute("""
            SELECT 
                f.id_tempo,
                t.data_riferimento,
                t.anno,
                t.mese,
                t.trimestre,
                c.codice_ecoicop,
                c.nome_categoria,
                c.is_generale,
                f.indice,
                f.var_mensile_pct,
                f.inflazione_annua_pct
            FROM fact_indice_prezzi f
            JOIN dim_tempo t ON f.id_tempo = t.id_tempo
            JOIN dim_categoria_spesa c ON f.codice_ecoicop = c.codice_ecoicop
            ORDER BY t.data_riferimento ASC, c.codice_ecoicop ASC;
        """).df()

        df_categories = conn.execute("SELECT * FROM dim_categoria_spesa ORDER BY codice_ecoicop ASC").df()
        
        df_forecast = conn.execute("""
            SELECT 
                id_tempo,
                data_riferimento,
                orizzonte_mesi,
                inflazione_prevista,
                intervallo_inf,
                intervallo_sup,
                modello
            FROM fact_previsioni
            ORDER BY data_riferimento ASC;
        """).df()

        kpi_dict = query_kpis()

        return df_facts, df_categories, df_forecast, kpi_dict
    finally:
        conn.close()


# Caricamento Dati
df_facts, df_categories, df_forecast, kpi_dict = load_all_data()

# Controllo se il database è inizializzato
if df_facts is None or len(df_facts) == 0:
    st.error("⚠️ Il database analitico DuckDB non è ancora popolato! Esegui prima: `python run_pipeline.py`")
    st.stop()

# Sidebar di Navigazione
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/7/7b/ISTAT_logo.svg/320px-ISTAT_logo.svg.png", width=180)
st.sidebar.title("Navigazione Piattaforma")
menu_option = st.sidebar.radio(
    "Seleziona la vista:",
    [
        "📊 Executive KPI & Overview",
        "🛒 Scomposizione Spesa (ECOICOP)",
        "🤖 Previsioni & Machine Learning",
        "⚙️ Data Engineering & DataOps",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ Info Progetto")
st.sidebar.info(
    "**DatIstat Portfolio**\n\n"
    "• **Data Source:** ISTAT / SDMX\n\n"
    "• **Architettura:** Bronze/Silver/Gold\n\n"
    "• **Engine:** DuckDB + Parquet\n\n"
    "• **ML:** Ridge / Gradient Boosting\n\n"
    "• **Orchestrazione:** Airflow + Docker"
)

# HEADER COMUNE
st.markdown('<div class="main-title">🇮🇹 Osservatorio Inflazione & Prezzi al Consumo (ISTAT NIC)</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">Piattaforma End-to-End: Data Pipeline SDMX, Modellazione Star Schema, '
    'Machine Learning Forecasting e Business Intelligence.</div>',
    unsafe_allow_html=True,
)

# -------------------------------------------------------------
# VISTA 1: EXECUTIVE KPI & OVERVIEW
# -------------------------------------------------------------
if menu_option == "📊 Executive KPI & Overview":
    st.subheader("Quadro Macroeconomico dell'Inflazione")

    col1, col2, col3, col4 = st.columns(4)

    curr_idx = kpi_dict.get("indice_corrente", 0.0)
    prev_idx = kpi_dict.get("indice_precedente", curr_idx)
    delta_idx = round(curr_idx - prev_idx, 2)

    curr_infl = kpi_dict.get("inflazione_annua", 0.0)
    prev_infl = kpi_dict.get("inflazione_precedente", curr_infl)
    delta_infl = round(curr_infl - prev_infl, 2)

    curr_var_m = kpi_dict.get("var_mensile", 0.0)

    # Calcolo picco storico
    df_gen = df_facts[df_facts["is_generale"] == True]
    max_infl_row = df_gen.loc[df_gen["inflazione_annua_pct"].idxmax()]

    col1.metric("Indice NIC (Base 2015=100)", f"{curr_idx:.1f}", f"{delta_idx:+.2f} su mese prec.")
    col2.metric("Inflazione Annua (Tendenziale)", f"{curr_infl:.2f}%", f"{delta_infl:+.2f}% vs mese prec.")
    col3.metric("Variazione Congiunturale (Mese)", f"{curr_var_m:.2f}%", "Variazione su M-1")
    col4.metric("Picco Storico Inflazione", f"{max_infl_row['inflazione_annua_pct']:.1f}%", f"{max_infl_row['id_tempo']}")

    st.markdown("---")

    # Grafico Serie Storica Plotly
    st.markdown("#### Evoluzione Storica dell'Inflazione e dell'Indice Prezzi (2016 - Oggi)")

    fig = go.Figure()

    # Linea Indice Prezzi
    fig.add_trace(go.Scatter(
        x=df_gen["data_riferimento"],
        y=df_gen["indice"],
        name="Indice dei Prezzi (Base 2015=100)",
        line=dict(color="#2563EB", width=2.5),
        yaxis="y1",
    ))

    # Linea Inflazione %
    fig.add_trace(go.Scatter(
        x=df_gen["data_riferimento"],
        y=df_gen["inflazione_annua_pct"],
        name="Inflazione Annua (%)",
        line=dict(color="#DC2626", width=2, dash="dot"),
        yaxis="y2",
    ))

    # Eventi Economici Chiave
    fig.add_vrect(
        x0="2020-03-01", x1="2020-12-01",
        fillcolor="gray", opacity=0.15, line_width=0,
        annotation_text="Lockdown COVID-19", annotation_position="top left"
    )
    fig.add_vrect(
        x0="2022-02-01", x1="2022-12-01",
        fillcolor="red", opacity=0.1, line_width=0,
        annotation_text="Shock Energetico / Guerra", annotation_position="top left"
    )

    fig.update_layout(
        height=500,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(title="Anno e Mese", showgrid=True),
        yaxis=dict(title="Indice dei Prezzi", side="left", showgrid=True),
        yaxis2=dict(title="Inflazione Annua %", side="right", overlaying="y", showgrid=False, ticksuffix="%"),
        hovermode="x unified",
    )

    st.plotly_chart(fig)

    st.markdown("""
    > **💡 Key Takeaway per il Decisore:**
    > Dopo la fiammata inflattiva eccezionale del 2022 (con picco a fine anno sopra il 12%, trainato dal comparto energetico), 
    > la dinamica dei prezzi si è progressivamente normalizzata tornando attorno al target europeo del 2% nel corso del 2024-2025.
    """)

# -------------------------------------------------------------
# VISTA 2: SCOMPOSIZIONE DELLA SPESA (ECOICOP)
# -------------------------------------------------------------
elif menu_option == "🛒 Scomposizione Spesa (ECOICOP)":
    st.subheader("Chi spinge l'inflazione? Analisi per Categoria di Spesa")
    st.markdown("Confronto dell'andamento dei prezzi tra i 12 capitoli di spesa del paniere ufficiale ECOICOP.")

    # Selettore periodo
    periodi_disponibili = sorted(df_facts["id_tempo"].unique(), reverse=True)
    selected_periodo = st.selectbox("Seleziona Periodo di Riferimento:", periodi_disponibili, index=0)

    df_filtered_p = df_facts[
        (df_facts["id_tempo"] == selected_periodo) & (df_facts["is_generale"] == False)
    ].sort_values("inflazione_annua_pct", ascending=True)

    # Bar chart orizzontale
    fig_bar = px.bar(
        df_filtered_p,
        x="inflazione_annua_pct",
        y="nome_categoria",
        orientation="h",
        color="inflazione_annua_pct",
        color_continuous_scale="RdYlGn_r",
        labels={"inflazione_annua_pct": "Inflazione Annua (%)", "nome_categoria": "Capitolo di Spesa"},
        text="inflazione_annua_pct",
        title=f"Tasso di Inflazione Annua per Categoria ({selected_periodo})",
    )
    fig_bar.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig_bar.update_layout(height=520, margin=dict(l=20, r=30, t=40, b=20), yaxis=dict(title=""))
    st.plotly_chart(fig_bar)

    st.markdown("---")
    st.markdown("#### Confronto Temporale Multi-Categoria")
    
    cat_options = [c for c in df_categories["nome_categoria"].tolist() if c != "Indice Generale"]
    default_cats = [
        "Prodotti alimentari e bevande analcoliche",
        "Abitazione, acqua, elettricità e combustibili",
        "Trasporti"
    ]
    selected_cats = st.multiselect("Seleziona Categorie da Confrontare:", cat_options, default=default_cats)

    if selected_cats:
        df_multi = df_facts[df_facts["nome_categoria"].isin(selected_cats)]
        fig_multi = px.line(
            df_multi,
            x="data_riferimento",
            y="inflazione_annua_pct",
            color="nome_categoria",
            labels={"inflazione_annua_pct": "Inflazione Annua (%)", "data_riferimento": "Data"},
            title="Dinamica Inflattiva a Confronto nel Tempo",
        )
        fig_multi.update_layout(height=450, hovermode="x unified", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_multi)

# -------------------------------------------------------------
# VISTA 3: PREVISIONI & MACHINE LEARNING
# -------------------------------------------------------------
elif menu_option == "🤖 Previsioni & Machine Learning":
    st.subheader("Data Science: Time Series Forecasting a 12 Mesi")
    st.markdown("""
    I modelli di serie storiche (Ridge Regression con lag autoregressivi, medie mobili e componenti armoniche)
    vengono addestrati sui dati storici ISTAT con validazione walk-forward.
    """)

    if df_forecast is not None and len(df_forecast) > 0:
        # Metriche del modello
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Modello Selezionato", df_forecast["modello"].iloc[0])
        col_m2.metric("Orizzonte Previsivo", f"{len(df_forecast)} Mesi")
        col_m3.metric("Copertura Temporale", f"{df_forecast['id_tempo'].iloc[0]} → {df_forecast['id_tempo'].iloc[-1]}")

        # Fan Chart con Intervalli di Confidenza
        df_gen = df_facts[df_facts["is_generale"] == True].tail(36)

        fig_fc = go.Figure()

        # Storico Reale recente
        fig_fc.add_trace(go.Scatter(
            x=df_gen["data_riferimento"],
            y=df_gen["inflazione_annua_pct"],
            name="Storico Reale ISTAT",
            line=dict(color="#1E3A8A", width=2.5),
        ))

        # Previsione Point Forecast
        fig_fc.add_trace(go.Scatter(
            x=df_forecast["data_riferimento"],
            y=df_forecast["inflazione_prevista"],
            name="Previsione ML (Point Forecast)",
            line=dict(color="#E11D48", width=2.5, dash="dash"),
        ))

        # Intervallo Superiore
        fig_fc.add_trace(go.Scatter(
            x=df_forecast["data_riferimento"],
            y=df_forecast["intervallo_sup"],
            name="Limite Sup (Confidenza 95%)",
            line=dict(width=0),
            showlegend=False,
        ))

        # Intervallo Inferiore (con riempimento)
        fig_fc.add_trace(go.Scatter(
            x=df_forecast["data_riferimento"],
            y=df_forecast["intervallo_inf"],
            name="Intervallo di Confidenza (95%)",
            fill="tonexty",
            fillcolor="rgba(225, 29, 72, 0.15)",
            line=dict(width=0),
        ))

        fig_fc.update_layout(
            height=500,
            title="Fan Chart dell'Inflazione: Storico vs Previsione Futura con Intervalli di Confidenza",
            xaxis=dict(title="Orizzonte Temporale"),
            yaxis=dict(title="Inflazione Annua (%)", ticksuffix="%"),
            hovermode="x unified",
        )
        st.plotly_chart(fig_fc)

        st.markdown("#### Tabella di Dettaglio Previsioni a 12 Mesi")
        st.dataframe(
            df_forecast[[
                "id_tempo", "orizzonte_mesi", "inflazione_prevista",
                "intervallo_inf", "intervallo_sup", "modello"
            ]].rename(columns={
                "id_tempo": "Mese",
                "orizzonte_mesi": "Passo (+M)",
                "inflazione_prevista": "Inflazione Prevista (%)",
                "intervallo_inf": "Limite Inf 95% (%)",
                "intervallo_sup": "Limite Sup 95% (%)",
                "modello": "Algoritmo"
            }),
            hide_index=True,
        )
    else:
        st.warning("Nessuna previsione presente in DuckDB. Esegui la pipeline con il modulo DS.")

# -------------------------------------------------------------
# VISTA 4: DATA ENGINEERING & DATAOPS
# -------------------------------------------------------------
elif menu_option == "⚙️ Data Engineering & DataOps":
    st.subheader("Stato della Piattaforma Dati & DataOps")
    st.markdown("Monitoraggio dell'architettura Medallion, integrità dei dati e metriche di pipeline.")

    # Esecuzione live dei test di qualità
    dq_report = run_data_quality_suite()

    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric("Stato Data Quality", "SUPERATO ✅" if dq_report["all_passed"] else "FALLITO ❌")
    col_s2.metric("Test Eseguiti", f"{dq_report['passed_tests']} / {dq_report['total_tests']}")
    col_s3.metric("Storage Engine", "DuckDB (Gold) + Parquet (Silver)")

    st.markdown("---")
    st.markdown("#### Checklist Asserzioni Data Quality")

    for t in dq_report["tests"]:
        icon = "✅" if t["passed"] else "❌"
        st.markdown(f"**{icon} {t['test_name']}** — *{t['details']}*")

    st.markdown("---")
    st.markdown("#### Architettura a Medaglione Implementata")
    
    col_b, col_s, col_g = st.columns(3)
    with col_b:
        st.markdown("##### 🥉 Bronze Layer (Raw)")
        st.caption("JSON/CSV originale estratto da endpoint SDMX")
        if BRONZE_RAW_FILE.exists():
            st.success(f"File: `{BRONZE_RAW_FILE.name}`\nDimensione: {BRONZE_RAW_FILE.stat().st_size / 1024:.1f} KB")
    
    with col_s:
        st.markdown("##### 🥈 Silver Layer (Cleaned)")
        st.caption("Dati tipizzati e compressi in formato Parquet colonnare")
        if SILVER_PARQUET_FILE.exists():
            st.success(f"File: `{SILVER_PARQUET_FILE.name}`\nDimensione: {SILVER_PARQUET_FILE.stat().st_size / 1024:.1f} KB")

    with col_g:
        st.markdown("##### 🥇 Gold Layer (Star Schema)")
        st.caption("DuckDB modellato a stella per query ad alta velocità")
        if GOLD_DUCKDB_FILE.exists():
            st.success(f"File: `{GOLD_DUCKDB_FILE.name}`\nDimensione: {GOLD_DUCKDB_FILE.stat().st_size / 1024:.1f} KB")
