"""
Reglas de Oro - Validaciones Automaticas
=========================================

Cualquier reporte debe cumplir estas validaciones antes de
mostrar resultados:

1. No mostrar un dato sin comparacion de crecimiento
2. Si un KPI esta en rojo, sugerir accion concreta
3. No generar reportes si faltan datos de una tienda o categoria

Fuentes de datos:
  - sync_log             (verificar que sync corrio OK)
  - offices              (verificar cobertura de tiendas)
  - stock_history        (verificar dias con snapshot)
"""

import logging
from datetime import date, timedelta
from harvester import db

logger = logging.getLogger("analytics.validaciones")


def validar_datos_completos(
    fecha_inicio: date,
    fecha_fin: date,
) -> dict:
    """
    Verifica que hay datos completos para generar un reporte confiable.

    Regla de Integridad: No permitir generacion de reportes si faltan
    datos de una tienda o categoria.

    Returns:
        Dict con:
          - es_valido: bool
          - errores: lista de problemas encontrados
          - advertencias: lista de advertencias no bloqueantes
          - cobertura: detalle de datos disponibles
    """
    errores = []
    advertencias = []
    cobertura = {}

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Verificar que el sync corrio exitosamente
            cur.execute("""
                SELECT entity, MAX(finished_at) AS ultimo_sync, status
                FROM sync_log
                WHERE status = 'SUCCESS'
                GROUP BY entity, status
                ORDER BY entity
            """)
            syncs = {row[0]: row[1] for row in cur.fetchall()}
            cobertura["ultimo_sync"] = {k: str(v) for k, v in syncs.items()}

            entidades_requeridas = [
                "offices", "stock_levels", "documents", "variant_costs"
            ]
            for ent in entidades_requeridas:
                if ent not in syncs:
                    errores.append(
                        f"Nunca se sincronizo '{ent}'. Ejecutar run_daily_sync.py primero."
                    )
                elif syncs[ent].date() < date.today() - timedelta(days=2):
                    advertencias.append(
                        f"'{ent}' no se sincroniza desde {syncs[ent].date()}. "
                        f"Datos pueden estar desactualizados."
                    )

            # 2. Verificar cobertura de tiendas en el periodo
            cur.execute("""
                SELECT
                    o.bsale_office_id, o.name,
                    COUNT(DISTINCT d.bsale_document_id) AS docs
                FROM offices o
                LEFT JOIN documents d
                    ON d.bsale_office_id = o.bsale_office_id
                   AND d.emission_date >= %s
                   AND d.emission_date < %s + INTERVAL '1 day'
                   AND d.is_active = TRUE
                WHERE o.is_active = TRUE AND o.bsale_office_id IN (1, 3)
                GROUP BY o.bsale_office_id, o.name
            """, (fecha_inicio, fecha_fin))
            tiendas = cur.fetchall()
            cobertura["tiendas"] = []

            for oid, nombre, docs in tiendas:
                cobertura["tiendas"].append({
                    "office_id": oid, "name": nombre, "documentos": docs
                })
                if docs == 0:
                    errores.append(
                        f"Tienda '{nombre}' (id={oid}) no tiene documentos "
                        f"en el periodo {fecha_inicio} a {fecha_fin}. "
                        f"Verificar si estuvo cerrada o si falta sincronizar."
                    )

            # 3. Verificar stock_history tiene datos para el periodo
            cur.execute("""
                SELECT COUNT(DISTINCT snapshot_date)
                FROM stock_history
                WHERE snapshot_date >= %s AND snapshot_date <= %s
            """, (fecha_inicio, fecha_fin))
            dias_stock = cur.fetchone()[0]
            dias_periodo = (fecha_fin - fecha_inicio).days + 1
            cobertura["dias_stock_history"] = dias_stock
            cobertura["dias_periodo"] = dias_periodo

            if dias_stock == 0:
                advertencias.append(
                    f"No hay snapshots de stock_history en el periodo. "
                    f"Los KPIs de rotacion usaran solo datos actuales. "
                    f"El historial se construye dia a dia con el daily sync."
                )
            elif dias_stock < dias_periodo * 0.7:
                advertencias.append(
                    f"Solo {dias_stock}/{dias_periodo} dias tienen snapshot de stock. "
                    f"Promedios de inventario pueden ser imprecisos."
                )

            # 4. Verificar que hay costos cargados
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE average_cost > 0) AS con_costo
                FROM variant_costs
            """)
            total_vc, con_costo = cur.fetchone()
            cobertura["variantes_con_costo"] = con_costo
            cobertura["variantes_total"] = total_vc

            if con_costo == 0:
                errores.append(
                    "No hay costos cargados. Los margenes seran 0%."
                )
            elif con_costo < total_vc * 0.3:
                advertencias.append(
                    f"Solo {con_costo}/{total_vc} variantes tienen costo. "
                    f"Margenes pueden estar inflados."
                )

    return {
        "es_valido": len(errores) == 0,
        "errores": errores,
        "advertencias": advertencias,
        "cobertura": cobertura,
    }


def validar_con_comparacion(
    valor_actual: float,
    valor_anterior: float,
    nombre_kpi: str,
) -> dict:
    """
    Regla: No mostrar un dato sin comparacion de crecimiento.

    Toma el valor actual y el del periodo anterior, y retorna
    el dato enriquecido con variacion y tendencia.

    Returns:
        Dict con: valor, valor_anterior, variacion_pct, tendencia (UP/DOWN/STABLE)
    """
    if valor_anterior > 0:
        variacion = round((valor_actual - valor_anterior) / valor_anterior * 100, 1)
    elif valor_actual > 0:
        variacion = 100.0
    else:
        variacion = 0.0

    if variacion > 2:
        tendencia = "UP"
    elif variacion < -2:
        tendencia = "DOWN"
    else:
        tendencia = "STABLE"

    return {
        "kpi": nombre_kpi,
        "valor": valor_actual,
        "valor_anterior": valor_anterior,
        "variacion_pct": variacion,
        "tendencia": tendencia,
    }


def alerta_accion(kpi_nombre: str, valor: float, umbrales: dict) -> dict | None:
    """
    Regla: Si un KPI esta en rojo, sugerir accion concreta.

    Args:
        kpi_nombre: Nombre del KPI
        valor: Valor actual
        umbrales: Dict con 'verde', 'amarillo', 'rojo' y 'accion_rojo'
            Ejemplo: {
                "verde": 20,     # margen > 20% = OK
                "amarillo": 10,  # margen 10-20% = cuidado
                "rojo": 10,      # margen < 10% = alerta
                "accion_rojo": "Revisar precio o cambiar proveedor",
                "accion_amarillo": "Monitorear y evaluar ajuste"
            }

    Returns:
        None si esta en verde. Dict con nivel, mensaje, accion si amarillo/rojo.
    """
    verde = umbrales.get("verde", float("inf"))
    rojo = umbrales.get("rojo", 0)

    if valor >= verde:
        return None  # Todo OK, no alertar

    if valor < rojo:
        return {
            "kpi": kpi_nombre,
            "nivel": "ROJO",
            "valor": valor,
            "mensaje": f"{kpi_nombre} en nivel critico: {valor}",
            "accion": umbrales.get("accion_rojo", "Revisar urgente"),
        }

    return {
        "kpi": kpi_nombre,
        "nivel": "AMARILLO",
        "valor": valor,
        "mensaje": f"{kpi_nombre} requiere atencion: {valor}",
        "accion": umbrales.get("accion_amarillo", "Monitorear"),
    }
