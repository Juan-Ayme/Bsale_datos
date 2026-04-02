"""
Sincronizadores de entidades transaccionales (alto volumen).

  - documents + document_details  (102K+ registros)
  - receptions + reception_details
"""

import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from typing import Any

from harvester.bsale_client import paginate, fetch, fetch_subresource
from harvester.config import (
    BSALE_BASE_URL, BSALE_PAGE_SIZE, BSALE_MAX_WORKERS, BSALE_HEADERS,
)
from harvester import db

logger = logging.getLogger("harvester.sync_transactions")


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _unix_to_ts(unix_ts: int) -> datetime | None:
    """Convierte unix timestamp BSale a datetime UTC. Valida rango razonable."""
    if not unix_ts or unix_ts < 946684800:  # antes de 2000-01-01
        return None
    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    except (OSError, ValueError):
        return None


# ============================================================
# DOCUMENTS (Boletas, Facturas, Notas de Credito)
# ============================================================

def _process_doc_page(url: str) -> dict:
    """
    Procesa UNA pagina de documentos (TURBO worker).
    Retorna dict con doc_rows, det_rows, fetched, skipped.
    Se ejecuta en un hilo del ThreadPoolExecutor.
    """
    result = {"doc_rows": [], "det_rows": [], "fetched": 0, "skipped": 0}

    data = fetch(url)
    if not data or "items" not in data:
        return result

    items = data["items"]
    if not items:
        return result

    result["fetched"] = len(items)

    for doc in items:
        doc_id = _safe_int(doc.get("id"))
        if doc_id == 0:
            result["skipped"] += 1
            continue

        if doc.get("state") != 0:
            result["skipped"] += 1
            continue

        # Tipo de documento
        dt = doc.get("document_type") or {}
        dt_id = _safe_int(dt.get("id"))
        if dt_id == 0:
            db.log_quality_issue("documents", doc_id, "document_type.id",
                                 "NULL_REQUIRED", "Documento sin tipo")
            result["skipped"] += 1
            continue

        is_credit_note = dt.get("isCreditNote") == 1

        # Fechas
        emission_ts = _unix_to_ts(_safe_int(doc.get("emissionDate")))
        if emission_ts is None:
            db.log_quality_issue("documents", doc_id, "emissionDate",
                                 "INVALID_TYPE", "Fecha emision invalida",
                                 str(doc.get("emissionDate")))
            result["skipped"] += 1
            continue

        generation_ts = _unix_to_ts(_safe_int(doc.get("generationDate")))
        office_id = _safe_int((doc.get("office") or {}).get("id"))
        user_id = _safe_int((doc.get("user") or {}).get("id")) or None

        result["doc_rows"].append((
            doc_id,
            dt_id,
            office_id,
            emission_ts,
            generation_ts,
            doc.get("serialNumber") or None,
            _safe_int(doc.get("number")) or None,
            _safe_float(doc.get("totalAmount")),
            _safe_float(doc.get("netAmount")),
            _safe_float(doc.get("taxAmount")),
            _safe_float(doc.get("exemptAmount")),
            is_credit_note,
            True,  # is_active (ya filtramos state=0)
            user_id,
            doc.get("token") or None,
        ))

        # --- Detalles (Motor de Excavación) ---
        details_container = doc.get("details") or {}
        detail_items = details_container.get("items") or []
        detail_count = _safe_int(details_container.get("count"))

        # Trampa de los 25: BSale inline limit. Si >= 25, paginar directo.
        needs_deep_fetch = (
            len(detail_items) >= 25
            or (detail_count > len(detail_items))
        )
        if needs_deep_fetch:
            deep_url = f"{BSALE_BASE_URL}/documents/{doc_id}/details.json"
            detail_items = fetch_subresource(deep_url, page_size=50)

        for det in detail_items:
            det_id = _safe_int(det.get("id"))
            if det_id == 0:
                continue

            variant_info = det.get("variant") or {}
            variant_id = _safe_int(variant_info.get("id"))
            if variant_id == 0:
                db.log_quality_issue("document_details", det_id, "variant.id",
                                     "NULL_REQUIRED",
                                     f"Detalle sin variante (doc={doc_id})")
                continue

            result["det_rows"].append((
                det_id,
                doc_id,
                variant_id,
                _safe_float(det.get("quantity")),
                _safe_float(det.get("netUnitValue")),
                _safe_float(det.get("netUnitValueRaw")),
                _safe_float(det.get("totalUnitValue")),
                _safe_float(det.get("netAmount")),
                _safe_float(det.get("taxAmount")),
                _safe_float(det.get("totalAmount")),
                _safe_float(det.get("discountPercentage")),
                _safe_float(det.get("netDiscount")),
                det.get("gratuity") == 1,
            ))

    return result


def sync_documents(since_unix: int | None = None) -> dict:
    """
    Sincroniza documentos de venta en modo TURBO (paralelo).

    Usa ThreadPoolExecutor para procesar múltiples páginas de la API
    simultáneamente, reduciendo drásticamente el tiempo de sync.

    Args:
        since_unix: Si se provee, solo sincroniza documentos desde esa fecha
                    (para sync incremental). Si None, sincroniza todo.
    """
    params_info = {"since_unix": since_unix}
    log_id = db.sync_start("documents", params_info)
    stats = {"fetched": 0, "inserted": 0, "skipped": 0, "details_inserted": 0}

    try:
        extra = "&state=0&expand=%5Bdetails%2Cdocument_type%5D"
        if since_unix:
            import time as _time
            now_unix = int(_time.time())
            extra += f"&emissiondaterange=[{since_unix},{now_unix}]"

        # Primero contar el total para pre-computar URLs
        count_url = f"{BSALE_BASE_URL}/documents.json?limit=1{extra}"
        meta = fetch(count_url)
        total_expected = meta.get("count", 0)
        logger.info("Documentos a sincronizar: %d", total_expected)

        if total_expected == 0:
            db.sync_finish(log_id, fetched=0, inserted=0)
            return stats

        # === TURBO: Pre-computar TODAS las URLs de paginas ===
        urls = [
            f"{BSALE_BASE_URL}/documents.json"
            f"?limit={BSALE_PAGE_SIZE}&offset={o}{extra}"
            for o in range(0, total_expected, BSALE_PAGE_SIZE)
        ]
        total_pages = len(urls)
        logger.info("TURBO: %d paginas pre-computadas, %d workers",
                     total_pages, BSALE_MAX_WORKERS)

        # === TURBO: Procesar todas las paginas en paralelo ===
        batch_doc_rows = []
        batch_det_rows = []
        pages_done = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=BSALE_MAX_WORKERS
        ) as exe:
            for page_result in exe.map(_process_doc_page, urls):
                batch_doc_rows.extend(page_result["doc_rows"])
                batch_det_rows.extend(page_result["det_rows"])
                stats["fetched"] += page_result["fetched"]
                stats["skipped"] += page_result["skipped"]
                pages_done += 1

                # Flush batch cada 2000 docs para no acumular demasiado en RAM
                if len(batch_doc_rows) >= 2000:
                    _flush_documents(batch_doc_rows, batch_det_rows)
                    stats["inserted"] += len(batch_doc_rows)
                    stats["details_inserted"] += len(batch_det_rows)
                    batch_doc_rows = []
                    batch_det_rows = []

                # Progreso cada 20 paginas
                if pages_done % 20 == 0:
                    logger.info("TURBO: %d/%d paginas | %d/%d docs procesados",
                                pages_done, total_pages,
                                stats["fetched"], total_expected)

        # Flush final
        if batch_doc_rows:
            _flush_documents(batch_doc_rows, batch_det_rows)
            stats["inserted"] += len(batch_doc_rows)
            stats["details_inserted"] += len(batch_det_rows)

        logger.info("TURBO completado: %d headers, %d detalles insertados",
                     stats["inserted"], stats["details_inserted"])

        db.sync_finish(log_id, fetched=stats["fetched"],
                        inserted=stats["inserted"], skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats


def _flush_documents(doc_rows: list[tuple], det_rows: list[tuple]):
    """Inserta batch de documentos y sus detalles."""
    sql_doc = """
        INSERT INTO documents (bsale_document_id, bsale_document_type_id,
                               bsale_office_id, emission_date, generation_date,
                               serial_number, doc_number, total_amount, net_amount,
                               tax_amount, exempt_amount, is_credit_note,
                               is_active, bsale_user_id, token, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bsale_document_id) DO UPDATE SET
            total_amount = EXCLUDED.total_amount,
            net_amount = EXCLUDED.net_amount,
            tax_amount = EXCLUDED.tax_amount,
            is_active = EXCLUDED.is_active,
            synced_at = NOW()
    """
    db.execute_batch(sql_doc, doc_rows)

    sql_det = """
        INSERT INTO document_details (bsale_detail_id, bsale_document_id,
                                      bsale_variant_id, quantity, net_unit_value,
                                      net_unit_value_raw, total_unit_value,
                                      net_amount, tax_amount, total_amount,
                                      discount_percentage, net_discount,
                                      is_gratuity, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bsale_detail_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            net_unit_value = EXCLUDED.net_unit_value,
            total_amount = EXCLUDED.total_amount,
            discount_percentage = EXCLUDED.discount_percentage,
            synced_at = NOW()
    """
    db.execute_batch(sql_det, det_rows)


# ============================================================
# RECEPTIONS + RECEPTION DETAILS
# ============================================================

def sync_receptions() -> dict:
    """Sincroniza recepciones de stock de todas las sucursales."""
    log_id = db.sync_start("receptions")
    stats = {"fetched": 0, "inserted": 0, "skipped": 0, "details_inserted": 0}

    try:
        # Obtener offices
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bsale_office_id FROM offices WHERE is_active = TRUE")
                office_ids = [row[0] for row in cur.fetchall()]

        for oid in office_ids:
            items = paginate("/stocks/receptions.json",
                             f"&officeid={oid}&expand=%5Bdetails%2Cdocument%5D")
            logger.info("Recepciones office %d: %d registros", oid, len(items))
            stats["fetched"] += len(items)

            rec_rows = []
            det_rows = []

            for rec in items:
                rec_id = _safe_int(rec.get("id"))
                if rec_id == 0:
                    stats["skipped"] += 1
                    continue

                # Fecha: prioridad documentDate > admissionDate (ambos pueden existir)
                admission_unix = _safe_int(
                    rec.get("documentDate") or rec.get("admissionDate") or 0
                )
                admission_ts = _unix_to_ts(admission_unix)
                if admission_ts is None:
                    db.log_quality_issue("receptions", rec_id, "admissionDate",
                                         "INVALID_TYPE", "Fecha invalida",
                                         str(rec.get("admissionDate")))
                    stats["skipped"] += 1
                    continue

                # Detectar traslado
                note = rec.get("note") or ""
                is_dispatch = _safe_int(rec.get("internalDispatchId")) > 0
                is_transfer = is_dispatch or "TRASLADO" in note.upper()

                raw_date = rec.get("rawAdmissionDate") or None
                office_id = _safe_int((rec.get("office") or {}).get("id"))
                user_id = _safe_int((rec.get("user") or {}).get("id")) or None

                rec_rows.append((
                    rec_id,
                    office_id,
                    admission_ts,
                    raw_date,
                    rec.get("document") if rec.get("document") != "Sin Documento" else None,
                    rec.get("documentNumber") or None,
                    note or None,
                    is_dispatch,
                    is_transfer,
                    user_id,
                ))

                # Detalles — "Motor de Excavación"
                # BSale limita detalles inline a 25 items. Si hay >= 25,
                # SIEMPRE paginamos el endpoint de detalles para no perder items.
                details_container = rec.get("details") or {}
                detail_items = details_container.get("items") or []
                detail_count = _safe_int(details_container.get("count"))

                # Trampa de los 25: si vinieron 25+ items, excavamos el endpoint real
                needs_deep_fetch = (
                    len(detail_items) >= 25
                    or (detail_count > len(detail_items))
                )
                if needs_deep_fetch:
                    deep_url = f"{BSALE_BASE_URL}/stocks/receptions/{rec_id}/details.json"
                    detail_items = fetch_subresource(deep_url, page_size=50)

                for det in detail_items:
                    det_id = _safe_int(det.get("id"))
                    variant_id = _safe_int((det.get("variant") or {}).get("id"))
                    if det_id == 0 or variant_id == 0:
                        continue

                    det_rows.append((
                        det_id,
                        rec_id,
                        variant_id,
                        _safe_float(det.get("quantity")),
                        _safe_float(det.get("cost")),
                    ))

            # Flush por sucursal
            sql_rec = """
                INSERT INTO receptions (bsale_reception_id, bsale_office_id,
                                        admission_date, admission_date_raw,
                                        document_ref, document_number, note,
                                        is_internal_dispatch, is_transfer,
                                        bsale_user_id, synced_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (bsale_reception_id) DO UPDATE SET
                    note = EXCLUDED.note,
                    is_transfer = EXCLUDED.is_transfer,
                    synced_at = NOW()
            """
            db.execute_batch(sql_rec, rec_rows)
            stats["inserted"] += len(rec_rows)

            sql_det = """
                INSERT INTO reception_details (bsale_reception_detail_id,
                                               bsale_reception_id, bsale_variant_id,
                                               quantity, cost, synced_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (bsale_reception_detail_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    cost = EXCLUDED.cost,
                    synced_at = NOW()
            """
            db.execute_batch(sql_det, det_rows)
            stats["details_inserted"] += len(det_rows)

        logger.info("Recepciones: %d headers, %d detalles",
                     stats["inserted"], stats["details_inserted"])

        db.sync_finish(log_id, fetched=stats["fetched"],
                        inserted=stats["inserted"], skipped=stats["skipped"])
    except Exception as exc:
        db.sync_finish(log_id, status="FAILED", error=str(exc),
                        fetched=stats["fetched"])
        raise

    return stats
