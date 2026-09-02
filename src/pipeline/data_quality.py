"""
DatIstat - Modulo Data Quality Suite (Data Engineering).

Esegue asserzioni e test d'integrità automatizzati sul Gold Layer (DuckDB):
1. Completezza dello schema relazionale
2. Assenza di valori nulli critici
3. Unicità delle chiavi primarie (nessun duplicato)
4. Range di validità economica degli indici (indice > 0)
5. Continuità temporale mensile (nessun buco storico)
6. Integrità referenziale tra fatti e dimensioni
"""
import logging
from typing import Dict, List, Any

from src.pipeline.database import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_data_quality_suite() -> Dict[str, Any]:
    """
    Esegue tutti i test di qualità dati sul database Gold.
    Restituisce un dizionario dettagliato con esito e dettagli di ciascun test.
    """
    conn = get_db_connection(read_only=True)
    results: List[Dict[str, Any]] = []

    try:
        # TEST 1: Esistenza tabelle attese nello Star Schema
        expected_tables = ["dim_tempo", "dim_categoria_spesa", "dim_territorio", "fact_indice_prezzi"]
        existing_tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        missing = [t for t in expected_tables if t not in existing_tables]
        results.append({
            "test_name": "Integrità Struttura Tabelle",
            "passed": len(missing) == 0,
            "details": "Tutte le tabelle presenti" if not missing else f"Tabelle mancanti: {missing}",
        })

        # TEST 2: Volume minimo record
        row_count = conn.execute("SELECT COUNT(*) FROM fact_indice_prezzi").fetchone()[0]
        results.append({
            "test_name": "Volume Record dei Fatti",
            "passed": row_count > 500,
            "details": f"Presenti {row_count} osservazioni dei fatti",
        })

        # TEST 3: Assenza di valori nulli nelle chiavi e misure
        null_count = conn.execute("""
            SELECT COUNT(*) FROM fact_indice_prezzi 
            WHERE id_tempo IS NULL OR codice_ecoicop IS NULL OR indice IS NULL
        """).fetchone()[0]
        results.append({
            "test_name": "Assenza Valori Nulli Critici",
            "passed": null_count == 0,
            "details": f"Valori nulli riscontrati: {null_count}",
        })

        # TEST 4: Unicità della chiave primaria composita
        dup_count = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT id_tempo, codice_ecoicop, codice_territorio, COUNT(*) 
                FROM fact_indice_prezzi 
                GROUP BY id_tempo, codice_ecoicop, codice_territorio 
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        results.append({
            "test_name": "Unicità Chiavi Primarie (No Duplicati)",
            "passed": dup_count == 0,
            "details": f"Chiavi duplicate: {dup_count}",
        })

        # TEST 5: Validità economica dell'indice (strettamente positivo)
        invalid_indices = conn.execute("""
            SELECT COUNT(*) FROM fact_indice_prezzi WHERE indice <= 0 OR indice > 500
        """).fetchone()[0]
        results.append({
            "test_name": "Range Valori Indici (0 < Indice < 500)",
            "passed": invalid_indices == 0,
            "details": f"Osservazioni anomale/fuori scala: {invalid_indices}",
        })

        # TEST 6: Continuità temporale mensile (nessun mese mancante)
        months_count = conn.execute("SELECT COUNT(DISTINCT id_tempo) FROM dim_tempo").fetchone()[0]
        min_date, max_date = conn.execute("SELECT MIN(data_riferimento), MAX(data_riferimento) FROM dim_tempo").fetchone()
        
        # Calcolo mesi teorici attesi tra min e max
        expected_months = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
        has_temporal_gap = (months_count != expected_months)
        results.append({
            "test_name": "Continuità Temporale Serie Storica",
            "passed": not has_temporal_gap,
            "details": f"{months_count} mesi effettivi su {expected_months} attesi ({min_date} -> {max_date})",
        })

        # TEST 7: Integrità referenziale (Foreign Keys orfane)
        orphan_dim = conn.execute("""
            SELECT COUNT(*) FROM fact_indice_prezzi f
            LEFT JOIN dim_categoria_spesa c ON f.codice_ecoicop = c.codice_ecoicop
            WHERE c.codice_ecoicop IS NULL
        """).fetchone()[0]
        results.append({
            "test_name": "Integrità Referenziale Categorie",
            "passed": orphan_dim == 0,
            "details": f"Chiavi orfane: {orphan_dim}",
        })

        all_passed = all(r["passed"] for r in results)
        logger.info(f"Suite Data Quality completata: {'SUPERATA' if all_passed else 'FALLITA'}")
        for r in results:
            status_icon = "PASS" if r["passed"] else "FAIL"
            logger.info(f" [{status_icon}] {r['test_name']}: {r['details']}")

        return {
            "all_passed": all_passed,
            "total_tests": len(results),
            "passed_tests": sum(1 for r in results if r["passed"]),
            "tests": results,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    report = run_data_quality_suite()
    print("Report Data Quality:", report["all_passed"])
