"""
KPIs de Rentabilidad y Margen
=============================

Objetivo: Detectar que productos generan valor real y cuales
son un lastre para el negocio.

Formulas:
  Margen Bruto = (Ventas - Costo de Ventas) / Ventas * 100
  Utilidad Bruta = Ventas - Costo de Ventas

Fuentes de datos:
  - documents + document_details  (ventas)
  - variant_costs                 (costos)
  - product_types                 (categorias)
"""

import logging
from datetime import date
from harvester import db

logger = logging.getLogger("analytics.rentabilidad")


def margen_bruto(
    fecha_inicio: date,
    fecha_fin: date,
    office_id: int | None = None,
) -> list[dict]:
    """
    Calcula el margen bruto por variante en un periodo.

    Margen Bruto (%) = (Venta - Costo) / Venta * 100

    Args:
        fecha_inicio: Inicio del periodo
        fecha_fin: Fin del periodo
        office_id: Filtrar por sucursal (None = todas)

    Returns:
        Lista de dicts con: variant_id, sku, description, category,
        venta_total, costo_total, utilidad, margen_pct
    """
    filtro_office = "AND d.bsale_office_id = %s" if office_id else ""
    params: list = [fecha_inicio, fecha_fin]
    if office_id:
        params.append(office_id)

    sql = f"""
        SELECT
            v.bsale_variant_id,
            v.code AS sku,
            v.description AS description,
            pt.name AS category,
            SUM(dd.total_amount)                          AS venta_total,
            SUM(dd.quantity * COALESCE(vc.average_cost, 0)) AS costo_total,
            SUM(dd.total_amount) -
                SUM(dd.quantity * COALESCE(vc.average_cost, 0)) AS utilidad,
            CASE
                WHEN SUM(dd.total_amount) > 0
                THEN ROUND(
                    (SUM(dd.total_amount) -
                     SUM(dd.quantity * COALESCE(vc.average_cost, 0)))
                    / SUM(dd.total_amount) * 100, 2
                )
                ELSE 0
            END AS margen_pct
        FROM document_details dd
        JOIN documents d ON d.bsale_document_id = dd.bsale_document_id
        JOIN variants v  ON v.bsale_variant_id = dd.bsale_variant_id
        LEFT JOIN variant_costs vc ON vc.bsale_variant_id = dd.bsale_variant_id
        LEFT JOIN products p ON p.bsale_product_id = v.bsale_product_id
        LEFT JOIN product_types pt ON pt.bsale_product_type_id = p.bsale_product_type_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
          {filtro_office}
        GROUP BY v.bsale_variant_id, v.code, v.description, pt.name
        ORDER BY utilidad DESC
    """

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def utilidad_por_categoria(
    fecha_inicio: date,
    fecha_fin: date,
    office_id: int | None = None,
) -> list[dict]:
    """
    Utilidad y margen agrupados por categoria (product_type).

    Returns:
        Lista de dicts con: category, venta_total, costo_total,
        utilidad, margen_pct, num_productos
    """
    filtro_office = "AND d.bsale_office_id = %s" if office_id else ""
    params: list = [fecha_inicio, fecha_fin]
    if office_id:
        params.append(office_id)

    sql = f"""
        SELECT
            COALESCE(pt.name, 'SIN CATEGORIA') AS category,
            SUM(dd.total_amount)                          AS venta_total,
            SUM(dd.quantity * COALESCE(vc.average_cost, 0)) AS costo_total,
            SUM(dd.total_amount) -
                SUM(dd.quantity * COALESCE(vc.average_cost, 0)) AS utilidad,
            CASE
                WHEN SUM(dd.total_amount) > 0
                THEN ROUND(
                    (SUM(dd.total_amount) -
                     SUM(dd.quantity * COALESCE(vc.average_cost, 0)))
                    / SUM(dd.total_amount) * 100, 2
                )
                ELSE 0
            END AS margen_pct,
            COUNT(DISTINCT v.bsale_variant_id) AS num_productos
        FROM document_details dd
        JOIN documents d ON d.bsale_document_id = dd.bsale_document_id
        JOIN variants v  ON v.bsale_variant_id = dd.bsale_variant_id
        LEFT JOIN variant_costs vc ON vc.bsale_variant_id = dd.bsale_variant_id
        LEFT JOIN products p ON p.bsale_product_id = v.bsale_product_id
        LEFT JOIN product_types pt ON pt.bsale_product_type_id = p.bsale_product_type_id
        WHERE d.emission_date >= %s
          AND d.emission_date < %s + INTERVAL '1 day'
          AND d.is_credit_note = FALSE
          AND d.is_active = TRUE
          {filtro_office}
        GROUP BY pt.name
        ORDER BY utilidad DESC
    """

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def productos_venta_alta_margen_bajo(
    fecha_inicio: date,
    fecha_fin: date,
    umbral_margen: float = 15.0,
    min_unidades: int = 5,
    office_id: int | None = None,
) -> list[dict]:
    """
    Detecta productos que venden mucho pero generan poca utilidad.

    Escenario: "Venta alta, Margen bajo"
    Accion sugerida: Ajustar precio o cambiar proveedor.

    Args:
        umbral_margen: Margen (%) por debajo del cual es alerta (default 15%)
        min_unidades: Minimo de unidades vendidas para considerar "alta venta"

    Returns:
        Lista de productos con margen < umbral ordenados por venta desc
    """
    resultados = margen_bruto(fecha_inicio, fecha_fin, office_id)

    alertas = []
    for r in resultados:
        venta = float(r["venta_total"] or 0)
        margen = float(r["margen_pct"] or 0)

        if venta > 0 and margen < umbral_margen:
            r["alerta"] = (
                f"MARGEN BAJO ({margen:.1f}%). "
                f"Proponer ajuste de precio o cambio de proveedor."
            )
            alertas.append(r)

    # Ordenar por venta descendente (los que mas venden con mal margen primero)
    alertas.sort(key=lambda x: float(x["venta_total"] or 0), reverse=True)
    return alertas
