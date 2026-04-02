"""
KPIs de Desempeno Comercial
============================

Objetivo: Monitoreo diario de metas y comparacion entre tiendas
para detectar problemas operativos.

Formulas:
  Ticket Promedio = Venta Total / Numero de Transacciones
  % Cumplimiento = (Venta Actual / Meta Establecida) * 100

Fuentes de datos:
  - documents            (transacciones de venta)
  - document_details     (detalle de cada venta)
  - offices              (sucursales)
"""

import logging
from datetime import date
from harvester import db

logger = logging.getLogger("analytics.comercial")


def ticket_promedio(
    fecha_inicio: date,
    fecha_fin: date,
    office_id: int | None = None,
) -> list[dict]:
    """
    Calcula el ticket promedio por sucursal y dia.

    Ticket Promedio = Venta Total / Numero de Transacciones

    Returns:
        Lista de dicts con: fecha, office_id, office_name,
        num_transacciones, venta_total, ticket_promedio
    """
    filtro_office = "AND d.bsale_office_id = %s" if office_id else ""
    params: list = [fecha_inicio, fecha_fin]
    if office_id:
        params.append(office_id)

    sql = f"""
        SELECT
            d.emission_date::date          AS fecha,
            d.bsale_office_id              AS office_id,
            o.name                         AS office_name,
            COUNT(DISTINCT d.bsale_document_id) AS num_transacciones,
            SUM(d.total_amount)            AS venta_total,
            ROUND(
                SUM(d.total_amount) /
                NULLIF(COUNT(DISTINCT d.bsale_document_id), 0), 2
            ) AS ticket_promedio
        FROM documents d
        JOIN offices o ON o.bsale_office_id = d.bsale_office_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
          {filtro_office}
        GROUP BY d.emission_date::date, d.bsale_office_id, o.name
        ORDER BY fecha DESC, office_id
    """

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def cumplimiento_meta(
    fecha_inicio: date,
    fecha_fin: date,
    metas: dict[int, float] | None = None,
) -> list[dict]:
    """
    Calcula el porcentaje de cumplimiento de meta por sucursal.

    % Cumplimiento = (Venta Actual / Meta) * 100

    Args:
        metas: Dict {office_id: meta_soles}. Si None, no calcula %.
               Ejemplo: {1: 50000, 3: 45000}

    Returns:
        Lista de dicts con: office_id, office_name, venta_total,
        meta, cumplimiento_pct, estado
    """
    sql = """
        SELECT
            d.bsale_office_id              AS office_id,
            o.name                         AS office_name,
            SUM(d.total_amount)            AS venta_total,
            COUNT(DISTINCT d.bsale_document_id) AS num_transacciones,
            COUNT(DISTINCT d.emission_date::date) AS dias_activos
        FROM documents d
        JOIN offices o ON o.bsale_office_id = d.bsale_office_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
          AND o.bsale_office_id IN (1, 3)
        GROUP BY d.bsale_office_id, o.name
        ORDER BY venta_total DESC
    """

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (fecha_inicio, fecha_fin))
            cols = [c.name for c in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for r in rows:
        oid = r["office_id"]
        venta = float(r["venta_total"] or 0)

        if metas and oid in metas:
            meta = metas[oid]
            r["meta"] = meta
            r["cumplimiento_pct"] = round(venta / meta * 100, 1) if meta > 0 else 0

            if r["cumplimiento_pct"] >= 100:
                r["estado"] = "META CUMPLIDA"
            elif r["cumplimiento_pct"] >= 80:
                r["estado"] = "EN CAMINO"
            else:
                r["estado"] = "POR DEBAJO"
        else:
            r["meta"] = None
            r["cumplimiento_pct"] = None
            r["estado"] = "SIN META"

    return rows


def comparar_tiendas(
    fecha_inicio: date,
    fecha_fin: date,
) -> dict:
    """
    Compara el rendimiento entre tiendas para detectar problemas.

    Escenario: "Comparacion de Tiendas"
    Identifica si el problema es ticket promedio, rotacion o mix de categorias.

    Returns:
        Dict con:
          - resumen: lista de metricas por tienda
          - categorias: venta por categoria por tienda
          - diagnostico: texto explicando diferencias
    """
    # 1. Resumen por tienda
    sql_resumen = """
        SELECT
            d.bsale_office_id              AS office_id,
            o.name                         AS office_name,
            COUNT(DISTINCT d.bsale_document_id) AS num_transacciones,
            SUM(d.total_amount)            AS venta_total,
            ROUND(
                SUM(d.total_amount) /
                NULLIF(COUNT(DISTINCT d.bsale_document_id), 0), 2
            ) AS ticket_promedio,
            COUNT(DISTINCT dd.bsale_variant_id) AS productos_vendidos,
            SUM(dd.quantity)               AS unidades_vendidas
        FROM documents d
        JOIN offices o ON o.bsale_office_id = d.bsale_office_id
        LEFT JOIN document_details dd ON dd.bsale_document_id = d.bsale_document_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
          AND o.bsale_office_id IN (1, 3)
        GROUP BY d.bsale_office_id, o.name
        ORDER BY venta_total DESC
    """

    # 2. Mix de categorias por tienda
    sql_categorias = """
        SELECT
            d.bsale_office_id              AS office_id,
            COALESCE(pt.name, 'SIN CATEGORIA') AS category,
            SUM(dd.total_amount)           AS venta_categoria,
            SUM(dd.quantity)               AS unidades
        FROM document_details dd
        JOIN documents d ON d.bsale_document_id = dd.bsale_document_id
        JOIN variants v  ON v.bsale_variant_id = dd.bsale_variant_id
        LEFT JOIN products p ON p.bsale_product_id = v.bsale_product_id
        LEFT JOIN product_types pt ON pt.bsale_product_type_id = p.bsale_product_type_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
        GROUP BY d.bsale_office_id, pt.name
        ORDER BY d.bsale_office_id, venta_categoria DESC
    """

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_resumen, (fecha_inicio, fecha_fin))
            cols = [c.name for c in cur.description]
            resumen = [dict(zip(cols, row)) for row in cur.fetchall()]

            cur.execute(sql_categorias, (fecha_inicio, fecha_fin))
            cols2 = [c.name for c in cur.description]
            cat_rows = [dict(zip(cols2, row)) for row in cur.fetchall()]

    # Agrupar categorias por tienda
    categorias_por_tienda: dict[int, list] = {}
    for r in cat_rows:
        oid = r["office_id"]
        if oid not in categorias_por_tienda:
            categorias_por_tienda[oid] = []
        categorias_por_tienda[oid].append(r)

    # Generar diagnostico
    diagnostico = _diagnosticar_tiendas(resumen, categorias_por_tienda)

    return {
        "resumen": resumen,
        "categorias": categorias_por_tienda,
        "diagnostico": diagnostico,
    }


def _diagnosticar_tiendas(
    resumen: list[dict],
    categorias: dict[int, list],
) -> list[str]:
    """Genera diagnostico textual comparando tiendas."""
    if len(resumen) < 2:
        return ["Solo hay una tienda activa, no se puede comparar."]

    diagnosticos = []

    # Ordenar por venta
    mejor = resumen[0]
    for tienda in resumen[1:]:
        nombre_mejor = mejor["office_name"]
        nombre_actual = tienda["office_name"]
        venta_mejor = float(mejor["venta_total"] or 0)
        venta_actual = float(tienda["venta_total"] or 0)

        if venta_mejor == 0:
            continue

        diff_pct = round((venta_mejor - venta_actual) / venta_mejor * 100, 1)

        # Comparar ticket promedio
        ticket_mejor = float(mejor["ticket_promedio"] or 0)
        ticket_actual = float(tienda["ticket_promedio"] or 0)

        # Comparar transacciones
        tx_mejor = int(mejor["num_transacciones"] or 0)
        tx_actual = int(tienda["num_transacciones"] or 0)

        diagnosticos.append(
            f"{nombre_actual} vende {diff_pct}% menos que {nombre_mejor}."
        )

        if ticket_actual < ticket_mejor * 0.85:
            diagnosticos.append(
                f"  -> Problema de TICKET PROMEDIO: {nombre_actual} "
                f"(S/{ticket_actual:.0f}) vs {nombre_mejor} (S/{ticket_mejor:.0f}). "
                f"Revisar estrategia de venta cruzada / upselling."
            )

        if tx_actual < tx_mejor * 0.85:
            diagnosticos.append(
                f"  -> Problema de TRAFICO: {nombre_actual} "
                f"({tx_actual} txns) vs {nombre_mejor} ({tx_mejor} txns). "
                f"Revisar ubicacion, horarios o marketing local."
            )

        # Comparar mix de categorias
        cats_mejor = {c["category"]: float(c["venta_categoria"])
                      for c in categorias.get(mejor["office_id"], [])}
        cats_actual = {c["category"]: float(c["venta_categoria"])
                       for c in categorias.get(tienda["office_id"], [])}

        for cat, venta_cat_mejor in sorted(cats_mejor.items(),
                                            key=lambda x: x[1], reverse=True)[:5]:
            venta_cat_actual = cats_actual.get(cat, 0)
            if venta_cat_mejor > 0 and venta_cat_actual < venta_cat_mejor * 0.5:
                diagnosticos.append(
                    f"  -> Categoria debil en {nombre_actual}: '{cat}' "
                    f"(S/{venta_cat_actual:,.0f} vs S/{venta_cat_mejor:,.0f}). "
                    f"Revisar surtido o exhibicion."
                )

    return diagnosticos
