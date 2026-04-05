"""
Sincronizadores de entidades maestras (cambian poco).

  - offices
  - product_types
  - document_types
  - products + variants (juntos, del mismo endpoint)
  - variant_costs
  - stock_levels
  - product_type_attributes  (atributos definidos por categoria)
  - variant_attribute_values (valores de atributo por variante)
"""

import logging
import concurrent.futures
from typing import Any

from harvester.bsale_client import paginate, fetch, fetch_subresource
from harvester.config import BSALE_BASE_URL, BSALE_HEADERS, BSALE_MAX_WORKERS
from harvester import db

logger = logging.getLogger("harvester.sync_masters")


# ============================================================
# Helpers de limpieza (auditoria)
# ============================================================

def _safe_int(val: Any, default: int = 0) -> int:
    """Convierte a int de forma segura. BSale a veces retorna string IDs."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _clean_str(val: Any, default: str = "") -> str:
    """Limpia strings: strip, manejo de None."""
    if val is None:
        return default
    return str(val).strip()


def _bsale_state_active(state: Any) -> bool:
    """BSale: state=0 es activo."""
    return _safe_int(state) == 0


# ============================================================
# OFFICES
# ============================================================

def sync_offices() -> dict:
    log_id = db.sync_start("offices")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        items = paginate("/offices.json")
        stats["fetched"] = len(items)

        rows = []
        for item in items:
            oid = _safe_int(item.get("id"))
            if oid == 0:
                db.log_quality_issue("offices", None, "id", "INVALID_TYPE",
                                     "Office sin ID valido", str(item.get("id")))
                stats["skipped"] += 1
                continue

            rows.append((
                oid,
                _clean_str(item.get("name"), "SIN NOMBRE"),
                _clean_str(item.get("address")),
                _clean_str(item.get("district")),
                _clean_str(item.get("city")),
                _clean_str(item.get("country"), "Peru"),
                item.get("isVirtual") == 1,
                _bsale_state_active(item.get("state")),
            ))

        sql = """
            INSERT INTO offices (bsale_office_id, name, address, district, city,
                                 country, is_virtual, is_active, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_office_id) DO UPDATE SET
                name = EXCLUDED.name, address = EXCLUDED.address,
                district = EXCLUDED.district, city = EXCLUDED.city,
                is_virtual = EXCLUDED.is_virtual, is_active = EXCLUDED.is_active,
                synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql, rows)
        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# PRODUCT_TYPES (Categorias)
# ============================================================

def sync_product_types() -> dict:
    log_id = db.sync_start("product_types")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        items = paginate("/product_types.json")
        stats["fetched"] = len(items)

        rows = []
        for item in items:
            ptid = _safe_int(item.get("id"))
            if ptid == 0:
                stats["skipped"] += 1
                continue

            rows.append((
                ptid,
                _clean_str(item.get("name"), "SIN CATEGORIA"),
                _bsale_state_active(item.get("state")),
            ))

        sql = """
            INSERT INTO product_types (bsale_product_type_id, name, is_active, synced_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (bsale_product_type_id) DO UPDATE SET
                name = EXCLUDED.name, is_active = EXCLUDED.is_active, synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql, rows)
        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# DOCUMENT_TYPES
# ============================================================

def sync_document_types() -> dict:
    log_id = db.sync_start("document_types")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        items = paginate("/document_types.json")  # Todas, incluso inactivas (docs historicos las referencian)
        stats["fetched"] = len(items)

        rows = []
        for item in items:
            dtid = _safe_int(item.get("id"))
            if dtid == 0:
                stats["skipped"] += 1
                continue

            rows.append((
                dtid,
                _clean_str(item.get("name"), "SIN TIPO"),
                _clean_str(item.get("code")),
                item.get("isCreditNote") == 1,
                item.get("isSalesNote") == 1,
                item.get("isElectronicDocument") == 1,
                _bsale_state_active(item.get("state")),
            ))

        sql = """
            INSERT INTO document_types (bsale_document_type_id, name, code,
                                        is_credit_note, is_sales_note, is_electronic,
                                        is_active, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_document_type_id) DO UPDATE SET
                name = EXCLUDED.name, code = EXCLUDED.code,
                is_credit_note = EXCLUDED.is_credit_note,
                is_sales_note = EXCLUDED.is_sales_note,
                is_electronic = EXCLUDED.is_electronic,
                is_active = EXCLUDED.is_active, synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql, rows)
        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# PRODUCTS + VARIANTS (del mismo endpoint)
# ============================================================

def sync_variants() -> dict:
    """
    Sincroniza productos y variantes desde /variants.json?expand=[product].
    Primero inserta products, luego variants (por FK).
    """
    log_id = db.sync_start("variants")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        items = paginate("/variants.json", "&state=0&expand=%5Bproduct%5D")
        stats["fetched"] = len(items)

        # Obtener product_types que ya existen en DB
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_product_type_id FROM product_types")
                known_pt_ids = {row[0] for row in cur.fetchall()}

        # Separar productos unicos y variantes
        products_seen: dict[int, tuple] = {}
        variant_rows: list[tuple] = []
        orphan_pt_ids: set[int] = set()  # categorias que no existen en DB

        for item in items:
            vid = _safe_int(item.get("id"))
            if vid == 0:
                db.log_quality_issue("variants", None, "id", "INVALID_TYPE",
                                     "Variante sin ID", str(item.get("id")))
                stats["skipped"] += 1
                continue

            # --- Producto (familia) ---
            prod = item.get("product") or {}
            pid = _safe_int(prod.get("id"))

            if pid == 0:
                db.log_quality_issue("variants", vid, "product.id", "NULL_REQUIRED",
                                     "Variante sin producto padre")
                stats["skipped"] += 1
                continue

            if pid not in products_seen:
                pt_id = _safe_int((prod.get("product_type") or {}).get("id"))
                # Detectar categorias huerfanas (eliminadas en BSale pero referenciadas)
                if pt_id > 0 and pt_id not in known_pt_ids:
                    orphan_pt_ids.add(pt_id)
                products_seen[pid] = (
                    pid,
                    _clean_str(prod.get("name"), f"SIN NOMBRE [{vid}]"),
                    _clean_str(prod.get("description")) or None,
                    pt_id if pt_id > 0 else None,
                    prod.get("stockControl") == 1,
                    prod.get("allowDecimal") == 1,
                    _bsale_state_active(prod.get("state")),
                )

            # --- Variante (SKU) ---
            code = _clean_str(item.get("code"))
            bar_code = _clean_str(item.get("barCode"))
            display_code = code or bar_code or f"V-{vid}"

            if not code and not bar_code:
                db.log_quality_issue("variants", vid, "code,barCode", "NULL_REQUIRED",
                                     f"Variante sin codigo, usando fallback V-{vid}")

            variant_rows.append((
                vid,
                pid,
                code or None,
                bar_code or None,
                display_code,
                _clean_str(item.get("description")) or None,
                _clean_str(item.get("unit")) or None,
                item.get("allowNegativeStock") == 1,
                _bsale_state_active(item.get("state")),
            ))

        # Resolver categorias huerfanas: fetch individual y crear en DB
        if orphan_pt_ids:
            logger.warning("Encontradas %d categorias huerfanas (eliminadas en BSale): %s",
                           len(orphan_pt_ids), orphan_pt_ids)
            orphan_rows = []
            for pt_id in orphan_pt_ids:
                pt_data = fetch(f"{BSALE_BASE_URL}/product_types/{pt_id}.json")
                name = _clean_str(pt_data.get("name"), f"ELIMINADA [{pt_id}]") if pt_data else f"ELIMINADA [{pt_id}]"
                is_active = _bsale_state_active(pt_data.get("state")) if pt_data else False
                orphan_rows.append((pt_id, name, is_active))
                db.log_quality_issue("product_types", pt_id, "state", "ORPHAN_FK",
                                     f"Categoria eliminada en BSale (state={pt_data.get('state') if pt_data else '?'}) pero referenciada por productos",
                                     str(pt_data.get("state") if pt_data else None))

            sql_orphan = """
                INSERT INTO product_types (bsale_product_type_id, name, is_active, synced_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (bsale_product_type_id) DO UPDATE SET
                    name = EXCLUDED.name, is_active = EXCLUDED.is_active, synced_at = NOW()
            """
            db.execute_batch(sql_orphan, orphan_rows)
            logger.info("Categorias huerfanas resueltas: %d", len(orphan_rows))

        # Insertar productos primero (FK)
        sql_prod = """
            INSERT INTO products (bsale_product_id, name, description,
                                  bsale_product_type_id, stock_control,
                                  allow_decimal, is_active, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_product_id) DO UPDATE SET
                name = EXCLUDED.name, description = EXCLUDED.description,
                bsale_product_type_id = EXCLUDED.bsale_product_type_id,
                stock_control = EXCLUDED.stock_control,
                allow_decimal = EXCLUDED.allow_decimal,
                is_active = EXCLUDED.is_active, synced_at = NOW()
        """
        db.execute_batch(sql_prod, list(products_seen.values()))
        logger.info("Products upserted: %d", len(products_seen))

        # Insertar variantes
        sql_var = """
            INSERT INTO variants (bsale_variant_id, bsale_product_id, code, bar_code,
                                  display_code, description, unit,
                                  allow_negative_stock, is_active, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_variant_id) DO UPDATE SET
                bsale_product_id = EXCLUDED.bsale_product_id,
                code = EXCLUDED.code, bar_code = EXCLUDED.bar_code,
                display_code = EXCLUDED.display_code,
                description = EXCLUDED.description, unit = EXCLUDED.unit,
                allow_negative_stock = EXCLUDED.allow_negative_stock,
                is_active = EXCLUDED.is_active, synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql_var, variant_rows)

        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# VARIANT COSTS (1 llamada por variante, paralelizado)
# ============================================================

def sync_variant_costs() -> dict:
    """Sincroniza costos. Usa multihilo porque es 1 call por variante."""
    log_id = db.sync_start("variant_costs")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        # Obtener lista de variant IDs de nuestra DB
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_variant_id FROM variants WHERE is_active = TRUE")
                variant_ids = [row[0] for row in cur.fetchall()]

        stats["fetched"] = len(variant_ids)
        logger.info("Sincronizando costos para %d variantes...", len(variant_ids))

        def _fetch_cost(vid: int) -> tuple:
            url = f"{BSALE_BASE_URL}/variants/{vid}/costs.json"
            data = fetch(url)

            avg_cost = 0.0
            latest_cost = 0.0
            source = "NONE"

            if data:
                avg_raw = data.get("averageCost")
                if avg_raw is not None:
                    try:
                        avg_cost = float(avg_raw)
                    except (ValueError, TypeError):
                        pass

                history = data.get("history") or []
                if history:
                    try:
                        latest_cost = float(history[0].get("cost", 0) or 0)
                    except (ValueError, TypeError):
                        pass

            # Determinar source y effective
            if avg_cost > 0:
                source = "AVERAGE"
                effective = avg_cost
            elif latest_cost > 0:
                source = "HISTORY"
                effective = latest_cost
            else:
                source = "NONE"
                effective = 0.0

            return (vid, avg_cost, latest_cost, source, effective)

        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=BSALE_MAX_WORKERS) as exe:
            futures = {exe.submit(_fetch_cost, vid): vid for vid in variant_ids}
            done_count = 0
            for future in concurrent.futures.as_completed(futures):
                rows.append(future.result())
                done_count += 1
                if done_count % 200 == 0:
                    logger.info("Costos: %d/%d procesados", done_count, len(variant_ids))

        sql = """
            INSERT INTO variant_costs (bsale_variant_id, average_cost, latest_cost,
                                       cost_source, effective_cost, synced_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_variant_id) DO UPDATE SET
                average_cost = EXCLUDED.average_cost,
                latest_cost = EXCLUDED.latest_cost,
                cost_source = EXCLUDED.cost_source,
                effective_cost = EXCLUDED.effective_cost,
                synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql, rows)

        # Log variantes sin costo
        no_cost = sum(1 for r in rows if r[4] == 0.0)
        if no_cost > 0:
            logger.warning("%d variantes sin costo registrado (effective_cost=0)", no_cost)

        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# STOCK LEVELS
# ============================================================

def sync_stock_levels() -> dict:
    """Sincroniza inventario de todas las sucursales."""
    log_id = db.sync_start("stock_levels")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        # Obtener offices de nuestra DB
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_office_id FROM offices WHERE is_active = TRUE")
                office_ids = [row[0] for row in cur.fetchall()]

        all_rows = []
        for oid in office_ids:
            items = paginate("/stocks.json", f"&officeid={oid}")
            logger.info("Stock office %d: %d registros", oid, len(items))

            for item in items:
                sid = _safe_int(item.get("id"))
                vid = _safe_int((item.get("variant") or {}).get("id"))
                office_id = _safe_int((item.get("office") or {}).get("id"))

                if sid == 0 or vid == 0:
                    stats["skipped"] += 1
                    continue

                qty = float(item.get("quantity", 0) or 0)
                qty_res = float(item.get("quantityReserved", 0) or 0)
                qty_avail = float(item.get("quantityAvailable", 0) or 0)

                all_rows.append((sid, vid, office_id, qty, qty_res, qty_avail))

            stats["fetched"] += len(items)

        sql = """
            INSERT INTO stock_levels (bsale_stock_id, bsale_variant_id, bsale_office_id,
                                      quantity, quantity_reserved, quantity_available, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bsale_stock_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                quantity_reserved = EXCLUDED.quantity_reserved,
                quantity_available = EXCLUDED.quantity_available,
                synced_at = NOW()
        """
        stats["inserted"] = db.execute_batch(sql, all_rows)

        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# STOCK HISTORY (snapshot diario)
# ============================================================

def snapshot_stock_history() -> dict:
    """
    Toma una foto del stock actual y la guarda en stock_history.

    Copia los datos de stock_levels (que ya fueron sincronizados)
    a stock_history con la fecha de hoy. El UNIQUE constraint
    (snapshot_date, variant, office) garantiza una sola foto por dia;
    si se ejecuta mas de una vez al dia, actualiza los valores.
    """
    from datetime import date

    today = date.today()
    log_id = db.sync_start("stock_history", {"snapshot_date": str(today)})
    stats = {"inserted": 0}

    try:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_history
                        (snapshot_date, bsale_variant_id, bsale_office_id,
                         quantity, quantity_reserved, quantity_available)
                    SELECT
                        %s,
                        bsale_variant_id, bsale_office_id,
                        quantity, quantity_reserved, quantity_available
                    FROM stock_levels
                    ON CONFLICT (snapshot_date, bsale_variant_id, bsale_office_id)
                    DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        quantity_reserved = EXCLUDED.quantity_reserved,
                        quantity_available = EXCLUDED.quantity_available,
                        created_at = NOW()
                """, (today,))
                stats["inserted"] = cur.rowcount

        logger.info("Stock history: %d registros para %s", stats["inserted"], today)
        db.sync_finish(log_id, inserted=stats["inserted"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc))
        raise

    return stats


# ============================================================
# PRODUCT TYPE ATTRIBUTES (atributos definidos por categoria)
# ============================================================

def sync_product_type_attributes() -> dict:
    """
    Sincroniza los tipos de atributo de cada categoria.

    Por cada product_type en nuestra DB llama a:
        GET /v1/product_types/{id}/attributes.json
    e inserta los resultados en product_type_attributes.

    Paralelizado con ThreadPoolExecutor (~246 llamadas, rapido).
    """
    log_id = db.sync_start("product_type_attributes")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        # Obtener todos los product_type_ids de nuestra DB
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_product_type_id FROM product_types")
                pt_ids = [row[0] for row in cur.fetchall()]

        logger.info("Sincronizando atributos para %d categorias...", len(pt_ids))

        def _fetch_attributes(pt_id: int) -> list[tuple]:
            """Descarga los atributos de una categoria. Retorna lista de rows."""
            url = f"{BSALE_BASE_URL}/product_types/{pt_id}/attributes.json"
            data = fetch(url)
            rows = []
            if not data:
                return rows
            for item in data.get("items") or []:
                aid = _safe_int(item.get("id"))
                if aid == 0:
                    return rows
                name = _clean_str(item.get("name"), f"SIN NOMBRE [{aid}]")
                rows.append((aid, pt_id, name))
            return rows

        all_rows: list[tuple] = []
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=BSALE_MAX_WORKERS) as exe:
            futures = {exe.submit(_fetch_attributes, pt_id): pt_id for pt_id in pt_ids}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                all_rows.extend(result)
                done += 1
                if done % 50 == 0:
                    logger.info("Atributos: %d/%d categorias procesadas", done, len(pt_ids))

        stats["fetched"] = len(all_rows)

        if all_rows:
            sql = """
                INSERT INTO product_type_attributes
                    (bsale_attribute_id, bsale_product_type_id, name, synced_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (bsale_attribute_id) DO UPDATE SET
                    name      = EXCLUDED.name,
                    synced_at = NOW()
            """
            stats["inserted"] = db.execute_batch(sql, all_rows)

        # Categorias sin ningun atributo definido (normal: la mayoria no los tiene)
        empty = len(pt_ids) - sum(1 for r in all_rows)
        if empty > 0:
            logger.info("%d categorias sin atributos (es normal)", empty)

        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


# ============================================================
# VARIANT ATTRIBUTE VALUES (valores concretos por variante)
# ============================================================

def sync_variant_attribute_values() -> dict:
    """
    Sincroniza los valores de atributo de cada variante activa.

    Por cada variant_id activo en nuestra DB llama a:
        GET /v1/variants/{id}/attribute_values.json
    e inserta en variant_attribute_values.

    Solo inserta variantes cuyos atributos ya existen en
    product_type_attributes (para respetar la FK).

    Paralelizado con ThreadPoolExecutor (~3,375 llamadas, ~7-10 min).
    """
    log_id = db.sync_start("variant_attribute_values")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0}

    try:
        # Obtener variantes activas y atributos conocidos
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_variant_id FROM variants WHERE is_active = TRUE")
                variant_ids = [row[0] for row in cur.fetchall()]

                cur.execute("SELECT bsale_attribute_id FROM product_type_attributes")
                known_attr_ids = {row[0] for row in cur.fetchall()}

        logger.info("Sincronizando attribute_values para %d variantes activas...",
                    len(variant_ids))

        if not known_attr_ids:
            logger.warning("No hay atributos en product_type_attributes. "
                           "Ejecuta sync_product_type_attributes() primero.")
            db.sync_finish(log_id, status="FAILED",
                           error="Sin atributos en product_type_attributes")
            return stats

        def _fetch_av(vid: int) -> list[tuple]:
            """Descarga attribute_values de una variante. Retorna lista de rows."""
            url = f"{BSALE_BASE_URL}/variants/{vid}/attribute_values.json"
            items = fetch_subresource(url)
            rows = []
            for item in items:
                av_id   = _safe_int(item.get("id"))
                av_desc = _clean_str(item.get("description"))
                attr_id = _safe_int((item.get("attribute") or {}).get("id"))

                if av_id == 0 or not av_desc:
                    continue

                # Solo insertar si el atributo padre ya existe en nuestra DB
                if attr_id not in known_attr_ids:
                    db.log_quality_issue(
                        "variant_attribute_values", vid,
                        "bsale_attribute_id", "ORPHAN_FK",
                        f"Atributo {attr_id} no existe en product_type_attributes",
                        str(attr_id),
                    )
                    continue

                rows.append((av_id, vid, attr_id, av_desc))
            return rows

        all_rows: list[tuple] = []
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=BSALE_MAX_WORKERS) as exe:
            futures = {exe.submit(_fetch_av, vid): vid for vid in variant_ids}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                all_rows.extend(result)
                done += 1
                if done % 500 == 0:
                    logger.info("AttributeValues: %d/%d variantes procesadas",
                                done, len(variant_ids))

        stats["fetched"] = len(all_rows)

        # Variantes sin ningun atributo (la gran mayoria)
        with_attrs = sum(1 for r in all_rows)
        logger.info("%d variantes tienen al menos un atributo (de %d totales)",
                    with_attrs, len(variant_ids))

        if all_rows:
            sql = """
                INSERT INTO variant_attribute_values
                    (bsale_av_id, bsale_variant_id, bsale_attribute_id,
                     description, synced_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (bsale_variant_id, bsale_attribute_id) DO UPDATE SET
                    description = EXCLUDED.description,
                    synced_at   = NOW()
            """
            stats["inserted"] = db.execute_batch(sql, all_rows)

        db.sync_finish(log_id, fetched=stats["fetched"], inserted=stats["inserted"],
                        skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats
