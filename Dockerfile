# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Evita la scrittura di file .pyc e abilita log non bufferizzati
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Installazione dipendenze di sistema minime
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installazione dipendenze Python
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia del codice sorgente
COPY src/ /app/src/
COPY app/ /app/app/
COPY run_pipeline.py /app/run_pipeline.py

# Esposizione porta per la Dashboard Streamlit
EXPOSE 8501

# Healthcheck per container Streamlit
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Comando predefinito: avvio della Dashboard BI Streamlit
CMD ["streamlit", "run", "app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
