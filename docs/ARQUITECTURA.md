# GRUPO HUDEC - Arquitectura del Sistema BI

## Vision General

Sistema de Business Intelligence para **GRUPO HUDEC** (2 tiendas + 1 almacen en Huamanga, Peru).
Extrae datos de BSale (POS/ERP) y los carga en PostgreSQL local para analisis.

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  BSale API  │────>│  Harvester   │────>│  PostgreSQL   │────>│  Analytics   │
│  (bsale.pe) │     │  (Python)    │     │  (local)      │     │  (KPIs)      │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
     REST API         Rate limited        11 tablas +          Rentabilidad
     9 req/s max      TURBO paralelo      stock_history        Inventario
     ~102K docs       UPSERT idempt.      historial diario     Comercial
```

## Estructura del Proyecto

```
Proyecto_Grupo_Hudec/
├── harvester/                 # Extractor de datos BSale -> PostgreSQL
│   ├── __init__.py
│   ├── config.py              # Variables de entorno, constantes
│   ├── bsale_client.py        # Cliente HTTP: rate limiter, retry, paginacion
│   ├── db.py                  # Pool de conexiones, batch upsert, sync log
│   ├── sync_masters.py        # Sync: offices, categorias, variantes, stock, costos
│   └── sync_transactions.py   # Sync: documentos (TURBO), recepciones
│
├── analytics/                 # Modulo de KPIs y reportes
│   ├── __init__.py            # Exports principales
│   ├── rentabilidad.py        # Margen bruto, utilidad por categoria
│   ├── inventario.py          # Rotacion, dias inventario, sobrestock
│   ├── comercial.py           # Ticket promedio, metas, comparar tiendas
│   ├── validaciones.py        # Reglas de integridad para reportes
│   └── reportes.py            # Orquestador: diario, semanal, mensual
│
├── docs/                      # Documentacion
│   ├── ARQUITECTURA.md        # Este archivo
│   ├── BSALE_API_AUDIT.md     # Auditoria completa de endpoints BSale
│   ├── schema.sql             # DDL de todas las tablas PostgreSQL
│   └── KPIS.md                # Documentacion de KPIs y formulas
│
├── run_harvest.py             # CLI: sync completa o por fases
├── run_daily_sync.py          # Sync incremental diaria (ultimos N dias)
├── daily_sync.bat             # Wrapper para Windows Task Scheduler
├── requirements.txt           # Dependencias Python
├── .env                       # Credenciales (NO en git)
└── .gitignore
```

## Flujo de Datos

### 1. Harvester (Extraccion)

```
BSale API ──> bsale_client.py ──> sync_*.py ──> db.py ──> PostgreSQL
              (rate limit 9/s)    (logica)      (upsert)
              (retry + backoff)   (excavacion)  (batch)
```

**Orden de dependencias:**
1. `offices` (sucursales)
2. `product_types` (categorias) + orphan resolution
3. `document_types` (boleta, factura, nota credito)
4. `variants` + `products` (catalogo)
5. `stock_levels` (inventario actual)
6. `variant_costs` (costo promedio)
7. `stock_history` (snapshot diario)
8. `receptions` + `reception_details` (ingresos de mercaderia)
9. `documents` + `document_details` (ventas - TURBO paralelo)

### 2. Problemas Resueltos

| Problema | Solucion |
|----------|----------|
| BSale inline limit 25 items | "Motor de Excavacion": detecta y pagina sub-recursos |
| Categorias eliminadas (state=99) | Orphan resolution: fetch individual de FK faltantes |
| Rate limit 9 req/s | Token-bucket thread-safe + retry con backoff |
| 102K docs (lento secuencial) | TURBO: ThreadPoolExecutor, 6 workers paralelos |
| NUMERIC overflow en recepciones | Campos ampliados a NUMERIC(20,2) y (20,4) |
| Variantes sin FK en detalles | FK removido (datos historicos referencian items eliminados) |

### 3. Analytics (Analisis)

```
PostgreSQL ──> analytics/*.py ──> validaciones ──> reportes
               (queries SQL)      (integridad)     (diario/semanal/mensual)
```

## Base de Datos

**PostgreSQL 18** en localhost (user: postgres, db: grupo_hudec_data)

### Tablas Maestras (sync completo)
- `offices` - 4 sucursales (2 tiendas + 1 almacen + 1 inactiva)
- `product_types` - 246 categorias
- `document_types` - 11 tipos (boleta, factura, NC, etc)
- `products` - productos padre
- `variants` - 3,382 SKUs
- `variant_costs` - costo promedio por variante
- `stock_levels` - inventario actual (se sobreescribe cada sync)

### Tablas Transaccionales (append/upsert)
- `documents` - 102K+ documentos de venta
- `document_details` - 309K+ lineas de detalle
- `receptions` - 8,373 ingresos de mercaderia
- `reception_details` - 14,735 lineas de ingreso

### Tablas de Historial
- `stock_history` - snapshot diario de inventario (~10K rows/dia)

### Tablas de Control
- `sync_log` - registro de cada ejecucion del harvester
- `data_quality_issues` - problemas de datos detectados

## Ejecucion
### Instalación de paquetes (primera vez)
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

### Sync Completa (primera vez)
```bash
python run_harvest.py
```

### Sync Diaria (incremental)
```bash
python run_daily_sync.py --days 6
```

### Sync solo atributos
```bash
python run_harvest.py --only attributes
```

### Programar en Windows Task Scheduler
- Programa: `daily_sync.bat`
- Trigger: Diario 06:00 AM
- Inicio en: directorio del proyecto

## Conexion a la Base de Datos

```python
# Desde cualquier notebook o script
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="grupo_hudec_data", user="postgres", password="root"
)
```
