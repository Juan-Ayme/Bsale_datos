# Auditoria API BSale - GRUPO HUDEC
> Fecha de auditoria: 2026-03-30
> Cuenta BSale: 108837 (bsale.com.pe)
> Volumenes: ~3,375 variantes | ~102,072 documentos | 4 sucursales | ~5,379 recepciones

---

## 1. OFFICES (Sucursales)
**Endpoint:** `GET /v1/offices.json`
**Total registros:** 4

| Campo BSale        | Tipo Real    | Nullable | Ejemplo                        | Uso en Harvester |
|--------------------|-------------|----------|--------------------------------|------------------|
| `id`               | int         | NO       | `4`                            | PK               |
| `name`             | string      | NO       | `"ALMACEN CENTRAL "`           | nombre (TRIM!)   |
| `description`      | string      | SI       | `""`                           | ignorar          |
| `address`          | string      | SI       | `"Jr. Jose Maria Eguren 370"`  | referencia       |
| `isVirtual`        | int (0/1)   | NO       | `0`                            | filtro           |
| `country`          | string      | SI       | `"Peru"`                       | referencia       |
| `district`         | string      | SI       | `"Jesus Nazareno"`             | referencia       |
| `city`             | string      | SI       | `"Huamanga"`                   | referencia       |
| `state`            | int (0/1)   | NO       | `0` (activo)                   | filtro           |
| `store`            | int         | NO       | `1`                            | ignorar          |
| `defaultPriceList` | int         | NO       | `4`                            | referencia       |

**Hallazgos de auditoria:**
- `name` tiene espacios al final (`"ALMACEN CENTRAL "`) --> TRIM obligatorio
- `state=0` significa activo (no intuitivo, es convension BSale)
- Solo 4 sucursales, 3 relevantes para el negocio: Magdalena (1), Asamblea (3), Almacen (4)

---

## 2. PRODUCT_TYPES (Categorias)
**Endpoint:** `GET /v1/product_types.json`
**Total registros:** 246

| Campo BSale                | Tipo Real  | Nullable | Ejemplo                | Uso en Harvester |
|----------------------------|-----------|----------|------------------------|------------------|
| `id`                       | int       | NO       | `37`                   | PK               |
| `name`                     | string    | NO       | `"ACCESORIO BANO"`     | nombre           |
| `isEditable`               | int (0/1) | NO       | `1`                    | ignorar          |
| `state`                    | int (0/1) | NO       | `0`                    | filtro           |
| `imagestionCategoryId`     | int       | NO       | `0`                    | ignorar          |
| `prestashopCategoryId`     | int       | NO       | `1`                    | ignorar          |

**Hallazgos de auditoria:**
- 246 categorias, algunas pueden estar inactivas (state!=0)
- Nombres tienen encoding UTF-8 con problemas de acentos (ej: `"BA\u00d1O"` vs `"BANO"`)
- Campos `imagestion*` y `prestashop*` son de integraciones externas, irrelevantes

---

## 3. VARIANTS (Productos/Variantes) + PRODUCT expandido
**Endpoint:** `GET /v1/variants.json?state=0&expand=[product]`
**Total registros:** 3,375

### Variante (nivel SKU)
| Campo BSale          | Tipo Real  | Nullable | Ejemplo              | Uso en Harvester    |
|----------------------|-----------|----------|----------------------|---------------------|
| `id`                 | int       | NO       | `21`                 | PK                  |
| `description`        | string    | SI       | `""`                 | variante desc       |
| `barCode`            | string    | SI       | `"7751324679139"`    | codigo barras       |
| `code`               | string    | SI       | `"21401"`            | codigo interno      |
| `tributaryCode`      | string    | SI       | `"01010101"`         | ignorar             |
| `unlimitedStock`     | int (0/1) | NO       | `0`                  | filtro              |
| `allowNegativeStock` | int (0/1) | NO       | `0`                  | auditoria           |
| `state`              | int (0/1) | NO       | `0`                  | filtro              |
| `serialNumber`       | int (0/1) | NO       | `0`                  | ignorar             |
| `isLot`              | int (0/1) | NO       | `0`                  | ignorar             |
| `unit`               | string    | SI       | `""`                 | unidad medida       |

### Producto expandido (nivel familia)
| Campo BSale          | Tipo Real  | Nullable | Ejemplo                              | Uso en Harvester |
|----------------------|-----------|----------|--------------------------------------|------------------|
| `product.id`         | int       | NO       | `12`                                 | FK producto      |
| `product.name`       | string    | SI       | `"AQUA ROSAS JABON DE GLICERINA 100G"` | nombre         |
| `product.description`| string    | SI       | `null`                               | descripcion      |
| `product.classification` | int   | NO       | `0`                                  | ignorar          |
| `product.stockControl`   | int (0/1) | NO  | `1`                                  | filtro           |
| `product.allowDecimal`   | int (0/1) | NO  | `0`                                  | auditoria        |
| `product.state`      | int (0/1) | NO       | `0`                                  | filtro           |
| `product.product_type.id` | string(!) | NO  | `"3"`                               | FK categoria     |
| `product.brand.id`   | string(!) | SI       | `"1"`                                | FK marca         |

**Hallazgos de auditoria:**
- `product.name` puede ser `null` --> marcar como "SIN NOMBRE"
- `product.product_type.id` viene como STRING `"3"`, no como INT --> cast obligatorio
- `product.brand.id` viene como STRING --> cast obligatorio
- `code` y `barCode` pueden ser ambos vacios --> necesitamos fallback: code -> barCode -> str(variant.id)
- `description` del producto puede ser `null` (diferente a string vacio)
- 3,375 variantes es manejable pero el endpoint de costos es 1 llamada POR variante (3,375 llamadas!)

---

## 4. VARIANT COSTS (Costos por variante)
**Endpoint:** `GET /v1/variants/{id}/costs.json`
**Llamadas necesarias:** 1 por variante (~3,375)

| Campo BSale                   | Tipo Real | Nullable | Ejemplo    | Uso en Harvester |
|-------------------------------|----------|----------|------------|------------------|
| `averageCost`                 | float    | NO       | `0.0`      | costo promedio   |
| `totalCost`                   | float    | NO       | `0.0`      | costo total      |
| `history`                     | array    | NO       | `[...]`    | historial        |
| `history[].cost`              | float    | NO       | `0.0`      | costo unitario   |
| `history[].admissionDate`     | int(unix)| NO       | `1772668800` | fecha recepcion |
| `history[].availableFifo`     | float    | NO       | `11.0`     | stock FIFO       |

**Hallazgos de auditoria:**
- `averageCost` puede ser `0.0` incluso cuando hay historial con costos > 0 --> usar fallback al historial
- `history` puede estar vacio `[]` --> costo = 0.0 (producto sin costo registrado)
- Este es el endpoint MAS LENTO del sistema (3,375 llamadas individuales)
- **Estrategia:** cachear costos en nuestra DB, solo re-sincronizar cuando cambie el stock

---

## 5. STOCKS (Inventario por sucursal)
**Endpoint:** `GET /v1/stocks.json?officeid={id}`
**Total registros:** ~3,369 por sucursal

| Campo BSale           | Tipo Real  | Nullable | Ejemplo | Uso en Harvester |
|-----------------------|-----------|----------|---------|------------------|
| `id`                  | int       | NO       | `2`     | PK               |
| `quantity`            | float     | NO       | `0.0`   | stock fisico     |
| `quantityReserved`    | float     | NO       | `0.0`   | reservado        |
| `quantityAvailable`   | float     | NO       | `0.0`   | disponible       |
| `variant.id`          | string(!) | NO       | `"21"`  | FK variante      |
| `office.id`           | string(!) | NO       | `"1"`   | FK sucursal      |

**Hallazgos de auditoria:**
- `variant.id` y `office.id` vienen como STRING --> cast a INT obligatorio
- `quantityAvailable` puede ser negativo si `allowNegativeStock=1` en la variante
- `quantity` vs `quantityAvailable`: usar `quantityAvailable` (descuenta reservas)
- Stock 0.0 es valido, no es null

---

## 6. STOCK RECEPTIONS (Recepciones/Ingresos)
**Endpoint:** `GET /v1/stocks/receptions.json?officeid={id}&expand=[details,document]`
**Total registros:** 5,379

### Cabecera de recepcion
| Campo BSale              | Tipo Real  | Nullable | Ejemplo                                      | Uso en Harvester |
|--------------------------|-----------|----------|----------------------------------------------|------------------|
| `id`                     | int       | NO       | `2`                                          | PK               |
| `admissionDate`          | int(unix) | NO       | `1749081600`                                 | fecha ingreso    |
| `rawAdmissionDate`       | string    | SI       | `"2025-06-05"`                               | fecha legible    |
| `document`               | string    | SI       | `"Sin Documento"`                            | ref documento    |
| `documentNumber`         | string    | SI       | `""`                                         | num documento    |
| `note`                   | string    | SI       | `"Importar Stock: LUIS ALBERTO HUAMAN..."`   | nota/motivo      |
| `internalDispatchId`     | int       | NO       | `0`                                          | traslado interno |
| `office.id`              | string(!) | NO       | `"1"`                                        | FK sucursal      |
| `user.id`                | string(!) | SI       | `"2"`                                        | FK usuario       |

### Detalle de recepcion
| Campo BSale              | Tipo Real  | Nullable | Ejemplo | Uso en Harvester |
|--------------------------|-----------|----------|---------|------------------|
| `details.items[].id`     | int       | NO       | `2`     | PK detalle       |
| `details.items[].quantity` | float   | NO       | `48.0`  | cantidad         |
| `details.items[].cost`   | float     | NO       | `1.6`   | costo unitario   |
| `details.items[].variant.id` | string(!) | NO  | `"22"`  | FK variante      |
| `details.items[].serialNumber` | string | SI   | `null`  | ignorar          |

**Hallazgos de auditoria:**
- `note` contiene pistas de traslados: buscar "TRASLADO" en el texto
- `internalDispatchId > 0` tambien indica traslado interno
- `document` puede ser `"Sin Documento"` (string, NO null)
- `admissionDate` siempre presente como unix timestamp
- `details` viene paginado internamente (limit=25) --> si una recepcion tiene >25 items, necesitamos paginar los detalles
- `cost` en detalle puede ser `0.0` (producto sin costo al momento del ingreso)

---

## 7. DOCUMENTS (Boletas/Facturas/NC)
**Endpoint:** `GET /v1/documents.json?state=0&expand=[details,document_type]`
**Total registros:** 102,072 (!)

### Cabecera del documento
| Campo BSale              | Tipo Real    | Nullable | Ejemplo                    | Uso en Harvester   |
|--------------------------|-------------|----------|----------------------------|--------------------|
| `id`                     | int         | NO       | `122`                      | PK                 |
| `emissionDate`           | int(unix)   | NO       | `1749254400`               | fecha emision      |
| `expirationDate`         | int(unix)   | SI       | `1749254400`               | fecha vencimiento  |
| `generationDate`         | int(unix)   | NO       | `1749338930`               | fecha generacion   |
| `number`                 | int         | NO       | `1`                        | numero correlativo |
| `serialNumber`           | string      | SI       | `"B001-1"`                 | serie-numero       |
| `totalAmount`            | float       | NO       | `17.3`                     | total con IGV      |
| `netAmount`              | float       | NO       | `14.66`                    | neto sin IGV       |
| `taxAmount`              | float       | NO       | `2.64`                     | IGV                |
| `exemptAmount`           | float       | NO       | `0.0`                      | exonerado          |
| `state`                  | int (0/1)   | NO       | `0`                        | filtro             |
| `commercialState`        | int         | NO       | `0`                        | estado comercial   |
| `token`                  | string      | NO       | `"5e41aebaa7dd"`           | token unico        |
| `urlPdf`                 | string      | SI       | `"https://...pdf"`         | link PDF           |
| `document_type.id`       | int         | NO       | `1`                        | FK tipo doc        |
| `document_type.name`     | string      | NO       | `"BOLETA - T"`             | nombre tipo        |
| `document_type.isCreditNote` | int(0/1) | NO    | `0`                        | es nota credito?   |
| `document_type.isSalesNote`  | int(0/1) | NO    | `0`                        | es nota venta?     |
| `document_type.code`     | string      | NO       | `"03"`                     | codigo SUNAT       |
| `office.id`              | string(!)   | NO       | `"1"`                      | FK sucursal        |
| `user.id`                | string(!)   | SI       | `"2"`                      | FK usuario/cajero  |

### Detalle del documento (lineas de venta)
| Campo BSale              | Tipo Real  | Nullable | Ejemplo    | Uso en Harvester     |
|--------------------------|-----------|----------|------------|----------------------|
| `details.items[].id`     | int       | NO       | `345`      | PK detalle           |
| `details.items[].quantity` | float   | NO       | `1.0`      | cantidad vendida     |
| `details.items[].netUnitValue` | float | NO     | `6.69`     | precio neto unitario |
| `details.items[].totalUnitValue` | float | NO   | `7.9`      | precio con IGV       |
| `details.items[].netAmount` | float  | NO       | `6.69`     | subtotal neto        |
| `details.items[].taxAmount` | float  | NO       | `1.21`     | IGV linea            |
| `details.items[].totalAmount` | float | NO      | `7.9`      | total linea          |
| `details.items[].netDiscount` | float | NO      | `0.0`      | descuento neto       |
| `details.items[].totalDiscount` | float | NO    | `0.0`      | descuento total      |
| `details.items[].discountPercentage` | float | NO | `0.0`    | % descuento          |
| `details.items[].variant.id` | int   | NO       | `162`      | FK variante          |
| `details.items[].variant.code` | string | SI    | `"P0087"`  | codigo variante      |
| `details.items[].note`   | string    | SI       | `""`       | nota de linea        |
| `details.items[].gratuity` | int(0/1) | NO     | `0`        | es gratuito?         |

**Hallazgos de auditoria:**
- 102,072 documentos es MUCHO. Paginar de 50 en 50 = 2,042 llamadas API
- `details` viene paginado (limit=25). Documentos con >25 lineas necesitan paginacion extra
- `variant.id` en detalles de documento viene como INT (no string como en stocks!)
- `emissionDate` es unix timestamp, NO puede ser 0 en documentos validos
- `document_type.isCreditNote=1` indica Nota de Credito (restar ventas)
- `document_type.isSalesNote=1` indica Nota de Venta (no fiscal, evaluar si incluir)
- `gratuity=1` son productos regalados (cantidad vendida pero ingreso=0)
- `netUnitValue` vs `netUnitValueRaw`: usar `netUnitValueRaw` para precision (mas decimales)
- `state=0` activo, otros valores = anulado
- `commercialState` puede indicar pagos pendientes
- El campo `discount` del script Colab NO existe en el JSON real --> el descuento esta en `discountPercentage` y `netDiscount`

---

## PROBLEMAS CRITICOS ENCONTRADOS

### 1. Inconsistencia de tipos en IDs
BSale retorna IDs como STRING en algunos endpoints y como INT en otros:
- `stocks.variant.id` = `"21"` (string)
- `document.details.variant.id` = `162` (int)
- `receptions.office.id` = `"1"` (string)
- `offices.id` = `4` (int)

**Regla:** SIEMPRE castear a INT al insertar en PostgreSQL.

### 2. Campo `discount` inexistente
El script Colab original usa `det.get("discount", 0)` pero el JSON real tiene:
- `discountPercentage` (float, porcentaje)
- `netDiscount` (float, monto)
- `totalDiscount` (float, monto con IGV)

**Regla:** Usar `discountPercentage` para calculos porcentuales, `netDiscount` para montos.

### 3. Paginacion interna de `details`
Tanto `documents` como `receptions` paginan sus `details` con limit=25.
Si un documento tiene >25 lineas, se pierden items.

**Regla:** Verificar `details.count` vs `len(details.items)`. Si difieren, paginar.

### 4. Encoding de caracteres
Nombres vienen con encoding incorrecto: `"BA\u00c3\u2018O"` en lugar de `"BANO"`.
Esto es doble encoding UTF-8.

**Regla:** Normalizar strings con `.encode('latin1').decode('utf-8')` como fallback.

### 5. Token expuesto en codigo
El token BSale esta hardcodeado en `main.py` y potencialmente en el historial de git.

**Regla:** Mover a `.env`, agregar `.env` a `.gitignore`, rotar el token.

---

## REGLAS DE AUDITORIA PARA EL HARVESTER

### Integridad de Tipos
| Campo               | Tipo esperado | Validacion                                    |
|---------------------|--------------|-----------------------------------------------|
| Todos los `*.id`    | INT          | `int(val)`, si falla -> log error + skip      |
| `*Date` (unix)      | INT > 0      | Debe ser > 946684800 (2000-01-01)             |
| `quantity`, `cost`  | FLOAT >= 0   | `float(val)`, negativos solo en NC            |
| `name`, `code`      | STRING       | `.strip()`, si vacio -> fallback              |
| `state`             | INT          | Solo procesar `state=0`                       |

### Manejo de Nulos
| Situacion                        | Accion                                  |
|----------------------------------|-----------------------------------------|
| `product.name` es null           | Usar `"SIN NOMBRE [variant_id]"`        |
| `variant.code` y `barCode` vacios | Usar `"V-{variant_id}"`               |
| `averageCost` es 0               | Buscar en `history[0].cost`             |
| `history` vacio                  | Costo = 0.0, marcar `cost_source='NONE'`|
| `document_type` no expandido     | Rechazar documento, log warning         |

### Deduplicacion
| Entidad     | Clave unica                        | Estrategia              |
|-------------|------------------------------------|-------------------------|
| Sucursal    | `bsale_office_id`                  | UPSERT (ON CONFLICT)    |
| Categoria   | `bsale_product_type_id`            | UPSERT                  |
| Variante    | `bsale_variant_id`                 | UPSERT                  |
| Stock       | `(bsale_variant_id, bsale_office_id)` | REPLACE en cada sync |
| Recepcion   | `bsale_reception_id`              | UPSERT                  |
| Documento   | `bsale_document_id`               | UPSERT                  |
| Doc Detalle | `bsale_detail_id`                 | UPSERT                  |

---

## VOLUMENES Y RATE LIMITING

| Endpoint          | Registros  | Paginacion (50/page) | Llamadas estimadas |
|-------------------|-----------|----------------------|--------------------|
| offices           | 4         | 1 pagina             | 1                  |
| product_types     | 246       | 5 paginas            | 5                  |
| variants          | 3,375     | 68 paginas           | 68                 |
| variant costs     | 3,375     | 1 por variante       | 3,375 (!)          |
| stocks (x3 suc)   | ~10,107   | ~203 paginas         | 203                |
| receptions        | 5,379     | ~108 paginas         | 108                |
| documents         | 102,072   | ~2,042 paginas       | 2,042              |
| **TOTAL**         |           |                      | **~5,802**         |

**Rate limit BSale:** ~9 req/segundo
**Tiempo estimado sync completa:** ~11 minutos
**Tiempo estimado sync incremental (solo docs nuevos):** ~2-3 minutos
