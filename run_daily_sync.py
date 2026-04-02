"""
Kawii Daily Sync - Actualizacion incremental diaria
====================================================

Sincroniza los ultimos N dias de datos desde BSale -> PostgreSQL.
Diseñado para ejecutarse diariamente via Task Scheduler de Windows.

Uso:
    python run_daily_sync.py              # Sync ultimos 6 dias (default)
    python run_daily_sync.py --days 3     # Sync ultimos 3 dias
    python run_daily_sync.py --days 1     # Solo hoy/ayer

Que sincroniza:
    1. Masters (offices, categorias, tipos doc, variantes) - siempre full
    2. Stock levels + costos - siempre full (refleja estado actual)
    3. Recepciones - ultimos N dias
    4. Documentos (TURBO) - ultimos N dias

El UPSERT (ON CONFLICT DO UPDATE) garantiza idempotencia:
    ejecutar multiples veces NO duplica datos.
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from harvester import db
from harvester.sync_masters import (
    sync_offices,
    sync_product_types,
    sync_document_types,
    sync_variants,
    sync_variant_costs,
    sync_stock_levels,
    snapshot_stock_history,
)
from harvester.sync_transactions import sync_documents, sync_receptions


def setup_logging():
    fmt = "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("daily_sync.log", encoding="utf-8"),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Kawii Daily Sync (incremental)")
    parser.add_argument(
        "--days", type=int, default=6,
        help="Cuantos dias hacia atras sincronizar (default: 6)",
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("daily_sync")

    # Calcular fecha limite: N dias atras a las 00:00 UTC
    since_dt = datetime.now(timezone.utc) - timedelta(days=args.days)
    since_dt = since_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    since_unix = int(since_dt.timestamp())

    logger.info("=" * 60)
    logger.info("  KAWII DAILY SYNC - Incremental %d dias", args.days)
    logger.info("  Desde: %s (unix: %d)", since_dt.strftime("%Y-%m-%d"), since_unix)
    logger.info("  Inicio: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    db.init_pool()
    t_total = time.time()
    results = {}

    try:
        # --- 1. Masters (siempre full, son pocos registros) ---
        logger.info(">>> FASE 1: Masters")
        for name, func in [
            ("Sucursales", sync_offices),
            ("Categorias", sync_product_types),
            ("Tipos Doc", sync_document_types),
            ("Variantes", sync_variants),
        ]:
            t0 = time.time()
            r = func()
            logger.info("  %s: %.1fs | %s", name, time.time() - t0, r)
            results[name] = r

        # --- 2. Stock actual + costos ---
        logger.info(">>> FASE 2: Stock y Costos")
        t0 = time.time()
        results["Stock"] = sync_stock_levels()
        logger.info("  Stock: %.1fs", time.time() - t0)

        t0 = time.time()
        results["Costos"] = sync_variant_costs()
        logger.info("  Costos: %.1fs", time.time() - t0)

        # --- 2b. Snapshot de stock del dia ---
        logger.info(">>> FASE 2b: Snapshot Stock History")
        t0 = time.time()
        results["Stock History"] = snapshot_stock_history()
        logger.info("  Stock History: %.1fs | %s", time.time() - t0,
                     results["Stock History"])

        # --- 3. Recepciones recientes ---
        logger.info(">>> FASE 3: Recepciones")
        t0 = time.time()
        results["Recepciones"] = sync_receptions()
        logger.info("  Recepciones: %.1fs", time.time() - t0)

        # --- 4. Documentos recientes (TURBO) ---
        logger.info(">>> FASE 4: Documentos (TURBO, desde %s)",
                     since_dt.strftime("%Y-%m-%d"))
        t0 = time.time()
        results["Documentos"] = sync_documents(since_unix=since_unix)
        logger.info("  Documentos: %.1fs | %s", time.time() - t0,
                     results["Documentos"])

        # --- Resumen ---
        elapsed = time.time() - t_total
        logger.info("=" * 60)
        logger.info("  DAILY SYNC COMPLETADO en %.1fs (%.1f min)", elapsed, elapsed / 60)
        for k, v in results.items():
            logger.info("    %s: %s", k, v)
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Sync interrumpida por el usuario")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Error fatal en daily sync: %s", exc)
        sys.exit(1)
    finally:
        db.close_pool()


if __name__ == "__main__":
    main()
