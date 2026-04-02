"""
Orquestador de Reportes
========================

Genera reportes completos combinando los KPIs de:
  - rentabilidad
  - inventario
  - comercial

Cada reporte pasa por las validaciones antes de generarse
y enriquece los datos con comparaciones de periodo anterior.
"""

import logging
from datetime import date, timedelta
from harvester import db
from analytics import (
    margen_bruto,
    utilidad_por_categoria,
    productos_venta_alta_margen_bajo,
    ticket_promedio,
    cumplimiento_meta,
    comparar_tiendas,
    rotacion_inventario,
    detectar_sobrestock,
    detectar_baja_rotacion,
)
from analytics.validaciones import (
    validar_datos_completos,
    validar_con_comparacion,
    alerta_accion,
)

logger = logging.getLogger("analytics.reportes")


# Umbrales por defecto para alertas
UMBRALES = {
    "margen_bruto": {
        "verde": 20, "rojo": 10,
        "accion_rojo": "Ajustar precio de venta o negociar con proveedor",
        "accion_amarillo": "Monitorear y evaluar ajuste en siguiente compra",
    },
    "rotacion": {
        "verde": 4, "rojo": 1,
        "accion_rojo": "Revisar eliminacion del catalogo o liquidacion",
        "accion_amarillo": "Considerar promocion o reubicacion entre tiendas",
    },
    "cumplimiento": {
        "verde": 100, "rojo": 70,
        "accion_rojo": "Reforzar estrategia comercial urgente",
        "accion_amarillo": "Intensificar esfuerzo de venta",
    },
}


def reporte_diario(
    fecha: date,
    metas: dict[int, float] | None = None,
) -> dict:
    """
    Reporte diario de desempeno comercial.

    Incluye:
      - Venta por tienda + ticket promedio
      - Cumplimiento de meta (si se proveen metas)
      - Top 10 productos mas vendidos
      - Alertas activas

    Args:
        fecha: Dia del reporte
        metas: Dict {office_id: meta_diaria_soles}
    """
    logger.info("Generando reporte diario para %s", fecha)

    # Validar datos
    validacion = validar_datos_completos(fecha, fecha)
    if not validacion["es_valido"]:
        logger.warning("Datos incompletos: %s", validacion["errores"])

    # Comparar con dia anterior
    ayer = fecha - timedelta(days=1)

    # Ticket promedio hoy vs ayer
    tickets_hoy = ticket_promedio(fecha, fecha)
    tickets_ayer = ticket_promedio(ayer, ayer)

    tickets_enriquecidos = []
    map_ayer = {(t["office_id"],): t for t in tickets_ayer}
    for t in tickets_hoy:
        key = (t["office_id"],)
        ant = map_ayer.get(key, {})
        comp = validar_con_comparacion(
            float(t["ticket_promedio"] or 0),
            float(ant.get("ticket_promedio", 0) or 0),
            f"Ticket {t['office_name']}",
        )
        t["comparacion"] = comp
        tickets_enriquecidos.append(t)

    # Cumplimiento
    meta_result = cumplimiento_meta(fecha, fecha, metas) if metas else []

    # Alertas
    alertas = []
    for m in meta_result:
        if m.get("cumplimiento_pct") is not None:
            a = alerta_accion(
                f"Cumplimiento {m['office_name']}",
                m["cumplimiento_pct"],
                UMBRALES["cumplimiento"],
            )
            if a:
                alertas.append(a)

    return {
        "tipo": "DIARIO",
        "fecha": str(fecha),
        "validacion": validacion,
        "tickets": tickets_enriquecidos,
        "cumplimiento": meta_result,
        "alertas": alertas,
    }


def reporte_semanal(
    fecha_fin: date,
    metas_semanales: dict[int, float] | None = None,
) -> dict:
    """
    Reporte semanal: 7 dias terminando en fecha_fin.

    Incluye:
      - Rentabilidad por categoria
      - Comparacion de tiendas
      - Productos con margen bajo
      - Comparacion vs semana anterior
    """
    fecha_inicio = fecha_fin - timedelta(days=6)
    sem_anterior_fin = fecha_inicio - timedelta(days=1)
    sem_anterior_ini = sem_anterior_fin - timedelta(days=6)

    logger.info("Reporte semanal: %s a %s", fecha_inicio, fecha_fin)

    validacion = validar_datos_completos(fecha_inicio, fecha_fin)

    # Rentabilidad por categoria (esta semana vs anterior)
    cats_actual = utilidad_por_categoria(fecha_inicio, fecha_fin)
    cats_anterior = utilidad_por_categoria(sem_anterior_ini, sem_anterior_fin)

    map_ant = {c["category"]: c for c in cats_anterior}
    for c in cats_actual:
        ant = map_ant.get(c["category"], {})
        c["comparacion_venta"] = validar_con_comparacion(
            float(c["venta_total"] or 0),
            float(ant.get("venta_total", 0) or 0),
            f"Venta {c['category']}",
        )
        c["comparacion_margen"] = validar_con_comparacion(
            float(c["margen_pct"] or 0),
            float(ant.get("margen_pct", 0) or 0),
            f"Margen {c['category']}",
        )

    # Comparacion de tiendas
    comp_tiendas = comparar_tiendas(fecha_inicio, fecha_fin)

    # Productos venta alta, margen bajo
    margen_bajo = productos_venta_alta_margen_bajo(fecha_inicio, fecha_fin)

    # Alertas
    alertas = []
    for c in cats_actual:
        a = alerta_accion(
            f"Margen {c['category']}",
            float(c["margen_pct"] or 0),
            UMBRALES["margen_bruto"],
        )
        if a:
            alertas.append(a)

    return {
        "tipo": "SEMANAL",
        "periodo": f"{fecha_inicio} a {fecha_fin}",
        "validacion": validacion,
        "rentabilidad_categorias": cats_actual,
        "comparacion_tiendas": comp_tiendas,
        "productos_margen_bajo": margen_bajo[:20],
        "alertas": alertas,
    }


def reporte_mensual(
    anio: int,
    mes: int,
    metas_mensuales: dict[int, float] | None = None,
) -> dict:
    """
    Reporte mensual completo.

    Incluye todo lo del semanal MAS:
      - Rotacion de inventario
      - Sobrestock
      - Categorias con baja rotacion
      - Cumplimiento mensual
    """
    from calendar import monthrange

    ultimo_dia = monthrange(anio, mes)[1]
    fecha_inicio = date(anio, mes, 1)
    fecha_fin = date(anio, mes, ultimo_dia)

    # Mes anterior para comparacion
    if mes == 1:
        mes_ant, anio_ant = 12, anio - 1
    else:
        mes_ant, anio_ant = mes - 1, anio
    ultimo_dia_ant = monthrange(anio_ant, mes_ant)[1]
    ini_ant = date(anio_ant, mes_ant, 1)
    fin_ant = date(anio_ant, mes_ant, ultimo_dia_ant)

    logger.info("Reporte mensual: %s-%02d", anio, mes)

    validacion = validar_datos_completos(fecha_inicio, fecha_fin)

    # Rentabilidad
    cats_actual = utilidad_por_categoria(fecha_inicio, fecha_fin)
    cats_anterior = utilidad_por_categoria(ini_ant, fin_ant)
    map_ant = {c["category"]: c for c in cats_anterior}
    for c in cats_actual:
        ant = map_ant.get(c["category"], {})
        c["comparacion_venta"] = validar_con_comparacion(
            float(c["venta_total"] or 0),
            float(ant.get("venta_total", 0) or 0),
            f"Venta {c['category']}",
        )

    # Inventario
    sobrestock = detectar_sobrestock(fecha_inicio, fecha_fin)
    baja_rotacion = detectar_baja_rotacion(fecha_inicio, fecha_fin)

    # Comparacion tiendas
    comp_tiendas = comparar_tiendas(fecha_inicio, fecha_fin)

    # Cumplimiento
    meta_result = cumplimiento_meta(
        fecha_inicio, fecha_fin, metas_mensuales
    ) if metas_mensuales else []

    # Margen bajo
    margen_bajo = productos_venta_alta_margen_bajo(fecha_inicio, fecha_fin)

    # Alertas consolidadas
    alertas = []
    for s in sobrestock[:10]:
        alertas.append({
            "kpi": "Sobrestock",
            "nivel": s.get("severidad", "ALERTA"),
            "valor": f"{s['sku']} - {s['description']}",
            "accion": s["alerta"],
        })
    for b in baja_rotacion[:5]:
        alertas.append({
            "kpi": "Baja Rotacion",
            "nivel": "AMARILLO",
            "valor": b["category"],
            "accion": b["alerta"],
        })
    for m in margen_bajo[:10]:
        alertas.append({
            "kpi": "Margen Bajo",
            "nivel": "ROJO",
            "valor": f"{m['sku']} - {m['description']}",
            "accion": m["alerta"],
        })

    return {
        "tipo": "MENSUAL",
        "periodo": f"{anio}-{mes:02d}",
        "validacion": validacion,
        "rentabilidad_categorias": cats_actual,
        "comparacion_tiendas": comp_tiendas,
        "sobrestock": sobrestock[:20],
        "baja_rotacion": baja_rotacion,
        "productos_margen_bajo": margen_bajo[:20],
        "cumplimiento": meta_result,
        "alertas": alertas,
    }
