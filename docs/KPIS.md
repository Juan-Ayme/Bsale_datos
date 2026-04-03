# GRUPO HUDEC - KPIs y Funciones de Analisis

## Escenarios de Analisis

| Escenario | Tarea del Analista | Nivel Esperado |
|-----------|-------------------|----------------|
| Baja rotacion en Categoria | Analizar por que no se vende | Evaluar impacto en margen y proponer reemplazo o eliminacion con sustento |
| Venta alta, Margen bajo | Identificar productos que venden pero no generan utilidad | Proponer ajuste de precios o cambio de proveedor |
| Comparacion de Tiendas | Evaluar por que la Tienda X rinde menos que Y | Identificar si el problema es ticket promedio, rotacion o mix de categorias |

---

## A. Rentabilidad y Margen

> **Objetivo**: Detectar que productos generan valor real.

### Margen Bruto (%)

```
Margen Bruto = (Ventas - Costo de Ventas) / Ventas x 100
```

**Funcion**: `analytics.rentabilidad.margen_bruto(fecha_inicio, fecha_fin, office_id=None)`

- Calcula por variante individual
- Usa `variant_costs.average_cost` como costo unitario
- Excluye notas de credito automaticamente

**Umbrales**:
- VERDE: >= 20%
- AMARILLO: 10% - 20%
- ROJO: < 10% -> **Accion**: Ajustar precio o cambiar proveedor

### Utilidad por Categoria

**Funcion**: `analytics.rentabilidad.utilidad_por_categoria(fecha_inicio, fecha_fin)`

- Agrupa margen por `product_type`
- Permite detectar categorias que no generan valor

### Productos Venta Alta, Margen Bajo

**Funcion**: `analytics.rentabilidad.productos_venta_alta_margen_bajo(fecha_inicio, fecha_fin, umbral_margen=15.0)`

- Detecta productos que venden mucho pero con margen < umbral
- Retorna alerta con accion sugerida

**Ejemplo de uso**:
```python
from analytics import productos_venta_alta_margen_bajo
from datetime import date

alertas = productos_venta_alta_margen_bajo(
    date(2026, 3, 1), date(2026, 3, 31),
    umbral_margen=15.0
)
for a in alertas[:5]:
    print(f"{a['sku']} | Margen: {a['margen_pct']}% | {a['alerta']}")
```

---

## B. Gestion de Inventario

> **Objetivo**: Evitar dinero estancado en sobrestock.

### Rotacion de Inventario

```
Rotacion = Costo de Ventas (Periodo) / Inventario Promedio
```

**Funcion**: `analytics.inventario.rotacion_inventario(fecha_inicio, fecha_fin, office_id=None)`

- Inventario promedio se calcula de `stock_history` (snapshots diarios)
- Cuanto mayor la rotacion, mejor (se vende rapido)

**Umbrales**:
- VERDE: >= 4 (rota cada ~3 meses)
- AMARILLO: 1 - 4
- ROJO: < 1 -> **Accion**: Revisar eliminacion o liquidacion

### Dias de Inventario

```
Dias de Inventario = Dias del Periodo / Rotacion
```

**Funcion**: `analytics.inventario.dias_inventario(fecha_inicio, fecha_fin)`

- Cuantos dias tardaria en venderse el stock actual al ritmo actual
- Menor es mejor

### Deteccion de Sobrestock

**Funcion**: `analytics.inventario.detectar_sobrestock(fecha_inicio, fecha_fin, umbral_dias=90)`

- Alerta productos con > 90 dias de inventario
- Severidad CRITICO si > 180 dias
- Sugiere: liquidacion, promocion o eliminacion

### Baja Rotacion por Categoria

**Funcion**: `analytics.inventario.detectar_baja_rotacion(fecha_inicio, fecha_fin, umbral_rotacion=1.0)`

- Agrupa por categoria y detecta las que no rotan
- **Accion**: Evaluar impacto en margen y proponer reemplazo

---

## C. Desempeno Comercial

> **Objetivo**: Monitoreo diario de metas.

### Ticket Promedio

```
Ticket Promedio = Venta Total / Numero de Transacciones
```

**Funcion**: `analytics.comercial.ticket_promedio(fecha_inicio, fecha_fin, office_id=None)`

- Desglosado por dia y sucursal
- Util para detectar cambios en patron de compra

### Cumplimiento de Meta

```
% Cumplimiento = (Venta Actual / Meta Establecida) x 100
```

**Funcion**: `analytics.comercial.cumplimiento_meta(fecha_inicio, fecha_fin, metas={1: 50000, 3: 45000})`

- Recibe metas por sucursal como parametro
- Estados: META CUMPLIDA (>=100%), EN CAMINO (>=80%), POR DEBAJO (<80%)

### Comparacion de Tiendas

**Funcion**: `analytics.comercial.comparar_tiendas(fecha_inicio, fecha_fin)`

Retorna diagnostico automatico que identifica:
1. **Problema de TICKET**: tienda con ticket bajo -> revisar upselling
2. **Problema de TRAFICO**: pocas transacciones -> revisar ubicacion/marketing
3. **Categorias debiles**: categorias que venden mucho menos que en otra tienda

**Ejemplo de uso**:
```python
from analytics import comparar_tiendas
from datetime import date

result = comparar_tiendas(date(2026, 3, 1), date(2026, 3, 31))

# Resumen por tienda
for t in result["resumen"]:
    print(f"{t['office_name']}: S/{t['venta_total']:,.0f} | "
          f"Ticket: S/{t['ticket_promedio']:.0f} | "
          f"Txns: {t['num_transacciones']}")

# Diagnostico automatico
for d in result["diagnostico"]:
    print(d)
```

---

## Reglas de Oro (Validaciones)

Toda salida de reporte cumple estas reglas automaticamente:

### 1. Validacion con Comparacion
> No mostrar un dato sin comparacion de crecimiento.

**Funcion**: `validar_con_comparacion(valor_actual, valor_anterior, nombre_kpi)`

Retorna: valor, valor_anterior, variacion_pct, tendencia (UP/DOWN/STABLE)

### 2. Alerta de Accion
> Si un KPI esta en rojo, el sistema sugiere accion concreta.

**Funcion**: `alerta_accion(kpi_nombre, valor, umbrales)`

Ejemplo:
```python
from analytics.validaciones import alerta_accion

alerta = alerta_accion("Margen COLLARES", 8.5, {
    "verde": 20, "rojo": 10,
    "accion_rojo": "Negociar con proveedor o subir precio",
})
# -> {'nivel': 'ROJO', 'accion': 'Negociar con proveedor...'}
```

### 3. Integridad de Datos
> No generar reportes si faltan datos de una tienda o categoria.

**Funcion**: `validar_datos_completos(fecha_inicio, fecha_fin)`

Verifica:
- Sync exitoso reciente de todas las entidades
- Todas las tiendas tienen documentos en el periodo
- Stock history tiene snapshots suficientes
- Costos estan cargados

---

## Reportes Pre-armados

### Reporte Diario
```python
from analytics.reportes import reporte_diario
r = reporte_diario(date(2026, 3, 31), metas={1: 2000, 3: 1800})
```

### Reporte Semanal
```python
from analytics.reportes import reporte_semanal
r = reporte_semanal(date(2026, 3, 31))  # ultimos 7 dias
```

### Reporte Mensual
```python
from analytics.reportes import reporte_mensual
r = reporte_mensual(2026, 3, metas_mensuales={1: 50000, 3: 45000})
```

---

## Tabla stock_history

Tabla clave para los KPIs de inventario. Se llena automaticamente
con el daily sync:

```sql
-- Tendencia de un producto en los ultimos 30 dias
SELECT snapshot_date, quantity_available
FROM stock_history
WHERE bsale_variant_id = 123 AND bsale_office_id = 1
ORDER BY snapshot_date;

-- Stock total diario por sucursal
SELECT snapshot_date, bsale_office_id, SUM(quantity_available)
FROM stock_history
GROUP BY snapshot_date, bsale_office_id
ORDER BY snapshot_date;

-- Inventario promedio del mes
SELECT bsale_variant_id, AVG(quantity) AS inv_promedio
FROM stock_history
WHERE snapshot_date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY bsale_variant_id;
```
