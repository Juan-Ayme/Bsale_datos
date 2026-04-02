"""
Capa de base de datos para el Harvester.

Provee:
  - Pool de conexiones
  - Helper para executemany (batch upsert)
  - Sync log (inicio/fin de cada entidad)
  - Data quality issue logger
"""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.pool
import psycopg2.extras

from harvester.config import DB_CONFIG

logger = logging.getLogger("harvester.db")

# Pool de conexiones (min=1, max=4)
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool():
    """Inicializa el pool de conexiones. Llamar una vez al inicio."""
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=4, **DB_CONFIG
        )
        logger.info("Pool de conexiones inicializado (%s:%s/%s)",
                     DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])


def close_pool():
    """Cierra todas las conexiones del pool."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("Pool de conexiones cerrado")


@contextmanager
def get_conn():
    """Context manager que obtiene y devuelve una conexion al pool."""
    if _pool is None:
        raise RuntimeError("Pool no inicializado. Llama a init_pool() primero.")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def execute_batch(sql: str, rows: list[tuple], page_size: int = 100) -> int:
    """
    Ejecuta un batch de inserts/upserts eficientemente.

    Args:
        sql: Query SQL con %s placeholders
        rows: Lista de tuplas con los valores
        page_size: Tamanio del batch interno de psycopg2

    Returns:
        Cantidad de filas procesadas
    """
    if not rows:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=page_size)
    return len(rows)


# --- Sync Log ---

def sync_start(entity: str, params: dict | None = None) -> int:
    """Registra inicio de sync. Retorna el ID del log."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sync_log (entity, status, params)
                   VALUES (%s, 'RUNNING', %s) RETURNING id""",
                (entity, psycopg2.extras.Json(params)),
            )
            row = cur.fetchone()
            log_id = row[0]
    logger.info("Sync iniciada: %s (log_id=%d)", entity, log_id)
    return log_id


def sync_finish(
    log_id: int,
    *,
    status: str = "SUCCESS",
    fetched: int = 0,
    inserted: int = 0,
    updated: int = 0,
    skipped: int = 0,
    error: str | None = None,
):
    """Registra fin de sync con metricas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE sync_log
                   SET finished_at = NOW(), status = %s,
                       records_fetched = %s, records_inserted = %s,
                       records_updated = %s, records_skipped = %s,
                       error_message = %s
                   WHERE id = %s""",
                (status, fetched, inserted, updated, skipped, error, log_id),
            )
    level = logging.INFO if status == "SUCCESS" else logging.ERROR
    logger.log(
        level,
        "Sync finalizada (log_id=%d): %s | fetched=%d inserted=%d updated=%d skipped=%d",
        log_id, status, fetched, inserted, updated, skipped,
    )


# --- Data Quality ---

def log_quality_issue(
    entity: str,
    bsale_id: int | None,
    field: str,
    issue_type: str,
    description: str,
    raw_value: str | None = None,
):
    """Registra un problema de calidad de datos sin detener el proceso."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO data_quality_issues
                       (entity, bsale_id, field, issue_type, description, raw_value)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (entity, bsale_id, field, issue_type, description, raw_value),
                )
    except Exception as exc:
        logger.error("No se pudo registrar issue de calidad: %s", exc)
