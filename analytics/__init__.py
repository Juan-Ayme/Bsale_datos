"""
Kawii Analytics - Modulo de KPIs y Reportes
============================================

Funciones de analisis para Kawii Pluss:
  - rentabilidad: Margen bruto, utilidad por producto/categoria
  - inventario: Rotacion, dias de inventario, sobrestock
  - comercial: Ticket promedio, cumplimiento de meta, comparacion de tiendas
  - validaciones: Reglas de integridad antes de generar reportes
  - reportes: Orquestador de reportes diarios/semanales/mensuales
"""

from analytics.rentabilidad import margen_bruto, utilidad_por_categoria, productos_venta_alta_margen_bajo
from analytics.inventario import rotacion_inventario, dias_inventario, detectar_sobrestock, detectar_baja_rotacion
from analytics.comercial import ticket_promedio, cumplimiento_meta, comparar_tiendas
from analytics.validaciones import validar_datos_completos, validar_con_comparacion, alerta_accion
