"""
GRUPO HUDEC Harvester - Orquestador Principal
=======================================

Ejecuta la sincronizacion de datos BSale -> PostgreSQL.

Uso:
    python run_harvest.py              # Sync completa (todo)
    python run_harvest.py --only masters   # Solo entidades maestras
    python run_harvest.py --only docs      # Solo documentos
    python run_harvest.py --only stock     # Solo stock + costos
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from harvester import db
from harvester.sync_masters import (
    sync_offices,
    sync_product_types,
    sync_document_types,
    sync_variants,
    sync_variant_costs,
    sync_stock_levels,
    snapshot_stock_history,
    sync_product_type_attributes,
    sync_variant_attribute_values,
)
from harvester.sync_transactions import sync_documents, sync_receptions


# ============================================================
# Logging
# ============================================================

def setup_logging():
    fmt = "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("harvest.log", encoding="utf-8"),
        ],
    )
    # Silenciar logs verbosos de urllib3
    logging.getLogger("urllib3").setLevel(logging.WARNING)


# ============================================================
# Orquestacion
# ============================================================

def run_masters():
    """Sincroniza entidades maestras en orden de dependencias."""
    logger = logging.getLogger("harvester.main")

    steps = [
        ("Sucursales", sync_offices),
        ("Categorias", sync_product_types),
        ("Tipos de Documento", sync_document_types),
        ("Productos y Variantes", sync_variants),
        ("Atributos de Categoria", sync_product_type_attributes),
        ("Valores de Atributo por Variante", sync_variant_attribute_values),
    ]

    for name, func in steps:
        logger.info("=" * 50)
        logger.info("SYNC: %s", name)
        logger.info("=" * 50)
        t0 = time.time()
        result = func()
        elapsed = time.time() - t0
        logger.info("  -> %s en %.1fs | %s", name, elapsed, result)


def run_stock():
    """Sincroniza stock y costos (depende de maestros)."""
    logger = logging.getLogger("harvester.main")

    logger.info("=" * 50)
    logger.info("SYNC: Stock Levels")
    logger.info("=" * 50)
    t0 = time.time()
    result = sync_stock_levels()
    logger.info("  -> Stock en %.1fs | %s", time.time() - t0, result)

    logger.info("=" * 50)
    logger.info("SYNC: Costos de Variantes (puede tardar ~7 min)")
    logger.info("=" * 50)
    t0 = time.time()
    result = sync_variant_costs()
    logger.info("  -> Costos en %.1fs | %s", time.time() - t0, result)

    logger.info("=" * 50)
    logger.info("SYNC: Stock History (snapshot del dia)")
    logger.info("=" * 50)
    t0 = time.time()
    result = snapshot_stock_history()
    logger.info("  -> Stock History en %.1fs | %s", time.time() - t0, result)


def run_transactions():
    """Sincroniza documentos y recepciones."""
    logger = logging.getLogger("harvester.main")

    logger.info("=" * 50)
    logger.info("SYNC: Recepciones de Stock")
    logger.info("=" * 50)
    t0 = time.time()
    result = sync_receptions()
    logger.info("  -> Recepciones en %.1fs | %s", time.time() - t0, result)

    logger.info("=" * 50)
    logger.info("SYNC: Documentos de Venta (~102K, puede tardar ~5 min)")
    logger.info("=" * 50)
    t0 = time.time()
    result = sync_documents()
    logger.info("  -> Documentos en %.1fs | %s", time.time() - t0, result)


def run_full():
    """Sync completa en orden correcto de dependencias."""
    run_masters()
    run_stock()
    run_transactions()


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="GRUPO HUDEC BSale Harvester")
    parser.add_argument(
        "--only",
        choices=["masters", "stock", "docs", "costs", "attributes"],
        help="Ejecutar solo una fase especifica",
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("harvester.main")

    logger.info("=" * 60)
    logger.info("  GRUPO HUDEC HARVESTER - BSale -> PostgreSQL")
    logger.info("  Inicio: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # Inicializar pool de conexiones
    db.init_pool()

    t_total = time.time()

    try:
        if args.only == "masters":
            run_masters()
        elif args.only == "stock":
            run_stock()
        elif args.only == "docs":
            run_transactions()
        elif args.only == "costs":
            logger.info("SYNC: Solo costos")
            sync_variant_costs()
        elif args.only == "attributes":
            logger.info("SYNC: Solo atributos")
            sync_product_type_attributes()
            sync_variant_attribute_values()
        else:
            run_full()

        elapsed = time.time() - t_total
        logger.info("=" * 60)
        logger.info("  HARVESTER COMPLETADO en %.1f segundos (%.1f min)", elapsed, elapsed / 60)
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Sync interrumpida por el usuario")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Error fatal en harvester: %s", exc)
        sys.exit(1)
    finally:
        db.close_pool()


if __name__ == "__main__":
    main()
