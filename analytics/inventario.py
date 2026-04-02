"""
KPIs de Gestion de Inventario
==============================

Objetivo: Evitar dinero estancado en sobrestock y detectar
productos con baja rotacion para tomar accion.

Formulas:
  Rotacion = Costo de Ventas (Periodo) / Inventario Promedio
  Dias de Inventario = 365 / Rotacion

Fuentes de datos:
  - stock_history        (inventario diario historico)
  - stock_levels         (inventario actual)
  - document_details     (ventas para calcular rotacion)
  - variant_costs        (costo promedio)
"""

import logging
from datetime import date
from harvester import db

logger = logging.getLogger("analytics.inventario")


def rotacion_inventario(
    fecha_inicio: date,
    fecha_fin: date,
    office_id: int | None = None,
) -> list[dict]:
    """
    Calcula la rotacion de inventario por variante.

    Rotacion = Costo de Ventas / Inventario Promedio
    Dias de Inventario = dias_periodo / Rotacion

    El inventario promedio se calcula desde stock_history
    (promedio de quantity entre fecha_inicio y fecha_fin).

    Returns:
        Lista de dicts con: variant_id, sku, description, category,
        costo_ventas, inv_promedio, rotacion, dias_inventario
    """
    filtro_office_ventas = "AND d.bsale_office_id = %s" if office_id else ""
    filtro_office_stock = "AND sh.bsale_office_id = %s" if office_id else ""
    params_ventas: list = [fecha_inicio, fecha_fin]
    params_stock: list = [fecha_inicio, fecha_fin]
    if office_id:
        params_ventas.append(office_id)
        params_stock.append(office_id)

    # Calcular dias del periodo
    dias_periodo = (fecha_fin - fecha_inicio).days + 1

    sql = f"""
        WITH ventas AS (
            SELECT
                dd.bsale_variant_id,
                SUM(dd.quantity * COALESCE(vc.average_cost, 0)) AS costo_ventas,
                SUM(dd.quantity) AS unidades_vendidas
            FROM document_details dd
            JOIN documents d ON d.bsale_document_id = dd.bsale_document_id
            LEFT JOIN variant_costs vc ON vc.bsale_variant_id = dd.bsale_variant_id
            WHERE d.emission_date >= %s
              AND d.emission_date < %s + INTERVAL '1 day'
              AND d.is_credit_note = FALSE
              AND d.is_active = TRUE
              {filtro_office_ventas}
            GROUP BY dd.bsale_variant_id
        ),
        inv_prom AS (
            SELECT
                sh.bsale_variant_id,
                AVG(sh.quantity) AS inv_promedio
            FROM stock_history sh
            WHERE sh.snapshot_date >= %s
              AND sh.snapshot_date <= %s
              {filtro_office_stock}
            GROUP BY sh.bsale_variant_id
        )
        SELECT
            v.bsale_variant_id AS variant_id,
            v.code AS sku,
            v.description,
            COALESCE(pt.name, 'SIN CATEGORIA') AS category,
            COALESCE(ve.costo_ventas, 0)    AS costo_ventas,
            COALESCE(ve.unidades_vendidas, 0) AS unidades_vendidas,
            COALESCE(ip.inv_promedio, 0)    AS inv_promedio,
            CASE
                WHEN COALESCE(ip.inv_promedio, 0) > 0
                THEN ROUND(COALESCE(ve.costo_ventas, 0) / ip.inv_promedio, 2)
                ELSE 0
            END AS rotacion,
            CASE
                WHEN COALESCE(ve.costo_ventas, 0) > 0
                     AND COALESCE(ip.inv_promedio, 0) > 0
                THEN ROUND(
                    {dias_periodo}::NUMERIC /
                    (COALESCE(ve.costo_ventas, 0) / ip.inv_promedio), 1
                )
                ELSE 999
            END AS dias_inventario
        FROM variants v
        LEFT JOIN ventas ve  ON ve.bsale_variant_id = v.bsale_variant_id
        LEFT JOIN inv_prom ip ON ip.bsale_variant_id = v.bsale_variant_id
        LEFT JOIN products p ON p.bsale_product_id = v.bsale_product_id
        LEFT JOIN product_types pt ON pt.bsale_product_type_id = p.bsale_product_type_id
        WHERE v.is_active = TRUE
        ORDER BY rotacion DESC
    """

    all_params = params_ventas + params_stock

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, all_params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def dias_inventario(
    fecha_inicio: date,
    fecha_fin: date,
    office_id: int | None = None,
) -> list[dict]:
    """
    Wrapper: retorna rotacion_inventario() ordenado por dias_inventario DESC
    (los mas lentos primero).
    """
    data = rotacion_inventario(fecha_inicio, fecha_fin, office_id)
    data.sort(key=lambda x: float(x["dias_inventario"]), reverse=True)
    return data


def detectar_sobrestock(
    fecha_inicio: date,
    fecha_fin: date,
    umbral_dias: int = 90,
    office_id: int | None = None,
) -> list[dict]:
    """
    Detecta productos con mas de N dias de inventario (dinero estancado).

    Args:
        umbral_dias: Dias por encima del cual se considera sobrestock (default 90)

    Returns:
        Productos con dias_inventario > umbral, con alerta de accion.
    """
    data = rotacion_inventario(fecha_inicio, fecha_fin, office_id)

    alertas = []
    for r in data:
        dias = float(r["dias_inventario"])
        inv = float(r["inv_promedio"])

        if dias > umbral_dias and inv > 0:
            if dias > 180:
                r["alerta"] = (
                    f"SOBRESTOCK CRITICO ({dias:.0f} dias). "
                    f"Evaluar liquidacion o eliminacion del catalogo."
                )
                r["severidad"] = "CRITICO"
            else:
                r["alerta"] = (
                    f"SOBRESTOCK ({dias:.0f} dias). "
                    f"Considerar promocion o reubicacion entre tiendas."
                )
                r["severidad"] = "ALERTA"
            alertas.append(r)

    alertas.sort(key=lambda x: float(x["dias_inventario"]), reverse=True)
    return alertas


def detectar_baja_rotacion(
    fecha_inicio: date,
    fecha_fin: date,
    umbral_rotacion: float = 1.0,
    office_id: int | None = None,
) -> list[dict]:
    """
    Detecta categorias con baja rotacion.

    Escenario: "Baja rotacion en Categoria A"
    Accion: Evaluar impacto en margen y proponer reemplazo o eliminacion.

    Args:
        umbral_rotacion: Rotacion por debajo del cual es alerta (default 1.0)

    Returns:
        Productos agrupados por categoria con rotacion baja.
    """
    data = rotacion_inventario(fecha_inicio, fecha_fin, office_id)

    # Agrupar por categoria
    por_categoria: dict[str, dict] = {}
    for r in data:
        cat = r["category"]
        if cat not in por_categoria:
            por_categoria[cat] = {
                "category": cat,
                "num_productos": 0,
                "costo_ventas_total": 0,
                "inv_promedio_total": 0,
                "unidades_vendidas": 0,
            }
        c = por_categoria[cat]
        c["num_productos"] += 1
        c["costo_ventas_total"] += float(r["costo_ventas"])
        c["inv_promedio_total"] += float(r["inv_promedio"])
        c["unidades_vendidas"] += float(r["unidades_vendidas"])

    # Calcular rotacion por categoria
    alertas = []
    for cat, c in por_categoria.items():
        if c["inv_promedio_total"] > 0:
            rot = c["costo_ventas_total"] / c["inv_promedio_total"]
        else:
            rot = 0

        if rot < umbral_rotacion and c["inv_promedio_total"] > 0:
            c["rotacion"] = round(rot, 2)
            c["alerta"] = (
                f"BAJA ROTACION en {cat} (rotacion={rot:.2f}). "
                f"Evaluar impacto en margen y proponer reemplazo o eliminacion."
            )
            alertas.append(c)

    alertas.sort(key=lambda x: x["rotacion"])
    return alertas
