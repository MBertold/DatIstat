"""
DatIstat - Modulo di Data Science & Time Series Forecasting.

Esegue l'addestramento e il confronto di modelli predittivi sull'indice generale
dei prezzi e genera le previsioni a 12 mesi con intervalli di confidenza empirici,
popolando la tabella fact_previsioni nel database Gold (DuckDB).
"""
import logging
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, mean_absolute_error

from src.pipeline.database import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def prepare_features(df_series: pd.DataFrame) -> pd.DataFrame:
    """
    Costruisce le feature temporali ed econometriche:
    - Lag storici (t-1, t-2, t-3, t-6, t-12)
    - Medie mobili a 3, 6, 12 mesi
    - Componenti armoniche stagionali (seno e coseno del mese)
    - Trend lineare
    """
    df = df_series.copy().sort_values("data_riferimento").reset_index(drop=True)
    df["t"] = np.arange(len(df))

    # Trasformazioni cicliche stagionali
    df["sin_mese"] = np.sin(2 * np.pi * df["mese"] / 12)
    df["cos_mese"] = np.cos(2 * np.pi * df["mese"] / 12)

    # Lag features sull'indice
    for lag in [1, 2, 3, 6, 12]:
        df[f"lag_{lag}"] = df["indice"].shift(lag)

    # Medie e deviazioni standard mobili
    df["rolling_mean_3"] = df["indice"].shift(1).rolling(window=3).mean()
    df["rolling_mean_6"] = df["indice"].shift(1).rolling(window=6).mean()
    df["rolling_mean_12"] = df["indice"].shift(1).rolling(window=12).mean()
    df["rolling_std_6"] = df["indice"].shift(1).rolling(window=6).std()

    return df


def train_and_evaluate_models(codice_ecoicop: str = "00") -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Addestra modelli predittivi, esegue backtesting sull'ultimo anno (12 mesi)
    e produce la previsione per i successivi 12 mesi.
    """
    conn = get_db_connection(read_only=True)
    try:
        query = f"""
            SELECT 
                f.id_tempo,
                t.data_riferimento,
                t.anno,
                t.mese,
                f.indice,
                f.inflazione_annua_pct
            FROM fact_indice_prezzi f
            JOIN dim_tempo t ON f.id_tempo = t.id_tempo
            WHERE f.codice_ecoicop = '{codice_ecoicop}'
            ORDER BY t.data_riferimento ASC;
        """
        df_raw = conn.execute(query).df()
    finally:
        conn.close()

    if len(df_raw) < 36:
        raise ValueError("Serie storica troppo corta per addestramento (< 36 mesi).")

    df_feats = prepare_features(df_raw)
    # Rimuove righe iniziali con NaN dovuti al lag 12
    df_clean = df_feats.dropna().reset_index(drop=True)

    feature_cols = [
        "t", "sin_mese", "cos_mese",
        "lag_1", "lag_2", "lag_3", "lag_6", "lag_12",
        "rolling_mean_3", "rolling_mean_6", "rolling_mean_12", "rolling_std_6"
    ]

    # Split temporale per Backtesting (ultimo anno = test set)
    test_horizon = 12
    train_df = df_clean.iloc[:-test_horizon]
    test_df = df_clean.iloc[-test_horizon:]

    X_train, y_train = train_df[feature_cols], train_df["indice"]
    X_test, y_test = test_df[feature_cols], test_df["indice"]

    models = {
        "Ridge_Regression": Ridge(alpha=1.0),
        "Gradient_Boosting": GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42),
        "Random_Forest": RandomForestRegressor(n_estimators=100, max_depth=4, random_state=42),
    }

    metrics = {}
    best_model_name = "Gradient_Boosting"
    best_rmse = float("inf")
    trained_models = {}

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae = float(mean_absolute_error(y_test, y_pred))
        mape = float(np.mean(np.abs((y_test - y_pred) / y_test)) * 100)
        
        metrics[name] = {"rmse": round(rmse, 3), "mae": round(mae, 3), "mape_pct": round(mape, 3)}
        trained_models[name] = model
        logger.info(f"Modello {name} -> RMSE: {rmse:.3f} | MAE: {mae:.3f} | MAPE: {mape:.2f}%")

        if rmse < best_rmse:
            best_rmse = rmse
            best_model_name = name

    logger.info(f"Modello selezionato per il deployment: {best_model_name} (RMSE: {best_rmse:.3f})")

    # Re-train del modello migliore su TUTTI i dati disponibili
    final_model = models[best_model_name]
    X_full, y_full = df_clean[feature_cols], df_clean["indice"]
    final_model.fit(X_full, y_full)

    # Calcolo deviazione residua per intervalli di confidenza
    residuals = y_full - final_model.predict(X_full)
    residual_sigma = np.std(residuals)

    # Generazione previsioni a 12 mesi (Rollout autoregressivo)
    forecast_records = []
    current_history = df_raw["indice"].tolist()
    last_date = df_raw["data_riferimento"].max()
    last_year = last_date.year
    last_month = last_date.month

    for step in range(1, 13):
        next_month = (last_month + step - 1) % 12 + 1
        next_year = last_year + (last_month + step - 1) // 12
        next_period = f"{next_year:04d}-{next_month:02d}"
        next_date = pd.to_datetime(f"{next_period}-01")

        # Costruzione dinamica riga di feature
        t_val = len(current_history)
        sin_val = np.sin(2 * np.pi * next_month / 12)
        cos_val = np.cos(2 * np.pi * next_month / 12)
        l1 = current_history[-1]
        l2 = current_history[-2]
        l3 = current_history[-3]
        l6 = current_history[-6]
        l12 = current_history[-12]
        rm3 = np.mean(current_history[-3:])
        rm6 = np.mean(current_history[-6:])
        rm12 = np.mean(current_history[-12:])
        rstd6 = np.std(current_history[-6:])

        x_future = pd.DataFrame([{
            "t": t_val,
            "sin_mese": sin_val,
            "cos_mese": cos_val,
            "lag_1": l1,
            "lag_2": l2,
            "lag_3": l3,
            "lag_6": l6,
            "lag_12": l12,
            "rolling_mean_3": rm3,
            "rolling_mean_6": rm6,
            "rolling_mean_12": rm12,
            "rolling_std_6": rstd6
        }])

        pred_idx = float(final_model.predict(x_future)[0])
        current_history.append(pred_idx)

        # Calcolo inflazione annua implicita prevista rispetto a 12 mesi prima
        val_12m_ago = current_history[-13]
        pred_inflation = round(((pred_idx / val_12m_ago) - 1) * 100, 2)

        # Intervalli di confidenza empirici (crescono con l'orizzonte temporale)
        uncertainty = 1.96 * residual_sigma * np.sqrt(step / 2.0)
        ci_lower = round(pred_inflation - (uncertainty / pred_idx) * 100, 2)
        ci_upper = round(pred_inflation + (uncertainty / pred_idx) * 100, 2)

        forecast_records.append({
            "id_tempo": next_period,
            "data_riferimento": next_date.date(),
            "orizzonte_mesi": step,
            "codice_ecoicop": codice_ecoicop,
            "inflazione_prevista": pred_inflation,
            "intervallo_inf": ci_lower,
            "intervallo_sup": ci_upper,
            "modello": best_model_name,
        })

    df_forecast = pd.DataFrame(forecast_records)

    # Salvataggio nel database Gold (DuckDB)
    conn = get_db_connection(read_only=False)
    try:
        conn.execute("DELETE FROM fact_previsioni WHERE codice_ecoicop = ?", [codice_ecoicop])
        conn.register("df_forecast_view", df_forecast)
        conn.execute("""
            INSERT INTO fact_previsioni (
                id_tempo, data_riferimento, orizzonte_mesi, codice_ecoicop,
                inflazione_prevista, intervallo_inf, intervallo_sup, modello
            )
            SELECT 
                id_tempo, data_riferimento, orizzonte_mesi, codice_ecoicop,
                inflazione_prevista, intervallo_inf, intervallo_sup, modello
            FROM df_forecast_view;
        """)
        logger.info(f"Salvate {len(df_forecast)} previsioni future in fact_previsioni (DuckDB).")
    finally:
        conn.close()

    summary = {
        "best_model": best_model_name,
        "metrics": metrics,
        "forecast_horizon": test_horizon,
        "forecast_period_start": df_forecast["id_tempo"].iloc[0],
        "forecast_period_end": df_forecast["id_tempo"].iloc[-1],
    }

    return summary, df_forecast


if __name__ == "__main__":
    summary, df_pred = train_and_evaluate_models()
    print("Riepilogo Data Science:", summary)
    print("Prime 3 previsioni:\n", df_pred.head(3))
