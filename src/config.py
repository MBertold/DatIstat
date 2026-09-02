"""
DatIstat - Modulo principale di configurazione e costanti.
"""
import sys
from pathlib import Path

# Percorsi di Base
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Percorsi dei file di output (Medallion Architecture)
BRONZE_RAW_FILE = BRONZE_DIR / "nic_monthly_raw.json"
SILVER_PARQUET_FILE = SILVER_DIR / "nic_cleaned.parquet"
GOLD_DUCKDB_FILE = GOLD_DIR / "inflation.duckdb"

# Endpoints SDMX
# 1. Endpoint primario IstatData (SDMX REST)
ISTAT_BASE_URL = "https://esploradati.istat.it/SDMXWS/rest"
NIC_DATAFLOW_ID = "167_744_DF_DCSP_NIC1B2015_1"
ISTAT_DATA_URL = f"{ISTAT_BASE_URL}/data/{NIC_DATAFLOW_ID}?startPeriod=2016-01"

# 2. Endpoint failover ufficiale Sistema Statistico Europeo (Eurostat - Fonte ISTAT Italia IT)
EUROSTAT_SDMX_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/PRC_HICP_MIDX/"
    "M.I15.CP00+CP01+CP02+CP03+CP04+CP05+CP06+CP07+CP08+CP09+CP10+CP11+CP12.IT"
    "?startPeriod=2016-01&format=SDMX-CSV"
)

# Headers standard per richieste SDMX
SDMX_HEADERS = {
    "User-Agent": "DatIstat-Research-Pipeline/1.0 (Educational Portfolio Project; https://github.com/DatIstat)",
    "Accept": "application/vnd.sdmx.data+json, text/csv, */*"
}

# Policy di Rate Limiting
MIN_REQUEST_INTERVAL_SECONDS = 12.0
API_TIMEOUT_SECONDS = 15

# Mappatura delle categorie ECOICOP per traduzione, raggruppamento e spiegazione
ECOICOP_MAPPING = {
    "CP00": {"codice": "00", "nome_it": "Indice Generale", "nome_en": "All items", "is_generale": True},
    "CP01": {"codice": "01", "nome_it": "Prodotti alimentari e bevande analcoliche", "nome_en": "Food and non-alcoholic beverages", "is_generale": False},
    "CP02": {"codice": "02", "nome_it": "Bevande alcoliche e tabacchi", "nome_en": "Alcoholic beverages and tobacco", "is_generale": False},
    "CP03": {"codice": "03", "nome_it": "Abbigliamento e calzature", "nome_en": "Clothing and footwear", "is_generale": False},
    "CP04": {"codice": "04", "nome_it": "Abitazione, acqua, elettricità e combustibili", "nome_en": "Housing, water, electricity, gas and other fuels", "is_generale": False},
    "CP05": {"codice": "05", "nome_it": "Mobili, articoli e servizi per la casa", "nome_en": "Furnishings, household equipment and routine maintenance", "is_generale": False},
    "CP06": {"codice": "06", "nome_it": "Servizi sanitari e spese per la salute", "nome_en": "Health", "is_generale": False},
    "CP07": {"codice": "07", "nome_it": "Trasporti", "nome_en": "Transport", "is_generale": False},
    "CP08": {"codice": "08", "nome_it": "Comunicazioni", "nome_en": "Communication", "is_generale": False},
    "CP09": {"codice": "09", "nome_it": "Ricreazione, spettacoli e cultura", "nome_en": "Recreation and culture", "is_generale": False},
    "CP10": {"codice": "10", "nome_it": "Istruzione", "nome_en": "Education", "is_generale": False},
    "CP11": {"codice": "11", "nome_it": "Servizi ricettivi e di ristorazione", "nome_en": "Restaurants and hotels", "is_generale": False},
    "CP12": {"codice": "12", "nome_it": "Altri beni e servizi", "nome_en": "Miscellaneous goods and services", "is_generale": False},
}

# Assicura la presenza delle cartelle di storage
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)
