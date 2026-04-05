-- ============================================================
-- GRUPO HUDEC - Schema PostgreSQL
-- Diseñado desde auditoria real de API BSale (2026-03-30)
-- ============================================================

-- Convenciones:
--   bsale_*_id  = ID original de BSale (nunca generamos nosotros)
--   synced_at   = timestamp de ultima sincronizacion
--   raw_json    = JSON crudo de BSale para debug (opcional, activar si necesario)

-- ============================================================
-- TABLAS MAESTRAS (cambian poco, sync diaria)
-- ============================================================

CREATE TABLE IF NOT EXISTS offices (
    bsale_office_id   INTEGER PRIMARY KEY,
    name              VARCHAR(200) NOT NULL,
    address           VARCHAR(500),
    district          VARCHAR(100),
    city              VARCHAR(100),
    country           VARCHAR(50) DEFAULT 'Peru',
    is_virtual        BOOLEAN NOT NULL DEFAULT FALSE,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE offices IS 'Sucursales/almacenes de GRUPO HUDEC. Source: GET /v1/offices.json';
COMMENT ON COLUMN offices.is_active IS 'BSale state=0 -> activo=true';

-- ============================================================

CREATE TABLE IF NOT EXISTS product_types (
    bsale_product_type_id  INTEGER PRIMARY KEY,
    name                   VARCHAR(200) NOT NULL,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE product_types IS 'Categorias de producto. Source: GET /v1/product_types.json';

-- ============================================================

CREATE TABLE IF NOT EXISTS document_types (
    bsale_document_type_id  INTEGER PRIMARY KEY,
    name                    VARCHAR(200) NOT NULL,
    code                    VARCHAR(10),          -- codigo SUNAT: "03"=boleta, "01"=factura
    is_credit_note          BOOLEAN NOT NULL DEFAULT FALSE,
    is_sales_note           BOOLEAN NOT NULL DEFAULT FALSE,
    is_electronic           BOOLEAN NOT NULL DEFAULT FALSE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE document_types IS 'Tipos de documento fiscal. Source: GET /v1/document_types.json';
COMMENT ON COLUMN document_types.is_credit_note IS 'Si true, las cantidades restan del inventario vendido';

-- ============================================================

CREATE TABLE IF NOT EXISTS products (
    bsale_product_id       INTEGER PRIMARY KEY,
    name                   VARCHAR(500) NOT NULL DEFAULT 'SIN NOMBRE',
    description            TEXT,
    bsale_product_type_id  INTEGER REFERENCES product_types(bsale_product_type_id),
    stock_control          BOOLEAN NOT NULL DEFAULT TRUE,
    allow_decimal          BOOLEAN NOT NULL DEFAULT FALSE,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE products IS 'Productos (familia). Source: expand de /v1/variants.json';

-- ============================================================

CREATE TABLE IF NOT EXISTS variants (
    bsale_variant_id    INTEGER PRIMARY KEY,
    bsale_product_id    INTEGER NOT NULL REFERENCES products(bsale_product_id),
    code                VARCHAR(100),       -- codigo interno BSale
    bar_code            VARCHAR(100),       -- codigo de barras EAN
    display_code        VARCHAR(100) NOT NULL, -- code -> bar_code -> 'V-{id}' (fallback)
    description         VARCHAR(500),
    unit                VARCHAR(50),
    allow_negative_stock BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE variants IS 'Variantes (SKU). Source: GET /v1/variants.json?expand=[product]';
COMMENT ON COLUMN variants.display_code IS 'Codigo para mostrar: prioridad code > bar_code > V-{id}';

CREATE INDEX IF NOT EXISTS idx_variants_product ON variants(bsale_product_id);
CREATE INDEX IF NOT EXISTS idx_variants_display_code ON variants(display_code);

-- ============================================================
-- TABLAS DE ESTADO (cambian frecuentemente, sync cada hora o diaria)
-- ============================================================

CREATE TABLE IF NOT EXISTS variant_costs (
    bsale_variant_id    INTEGER PRIMARY KEY REFERENCES variants(bsale_variant_id),
    average_cost        NUMERIC(12,4) NOT NULL DEFAULT 0,
    latest_cost         NUMERIC(12,4) NOT NULL DEFAULT 0,   -- del history[0]
    cost_source         VARCHAR(20) NOT NULL DEFAULT 'NONE', -- 'AVERAGE', 'HISTORY', 'NONE'
    effective_cost      NUMERIC(12,4) NOT NULL DEFAULT 0,    -- el que usamos: average > latest > 0
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE variant_costs IS 'Costos por variante. Source: GET /v1/variants/{id}/costs.json';
COMMENT ON COLUMN variant_costs.cost_source IS 'De donde salio effective_cost: AVERAGE si averageCost>0, HISTORY si solo history tiene dato, NONE si todo es 0';

-- ============================================================

CREATE TABLE IF NOT EXISTS stock_levels (
    bsale_stock_id      INTEGER PRIMARY KEY,
    bsale_variant_id    INTEGER NOT NULL REFERENCES variants(bsale_variant_id),
    bsale_office_id     INTEGER NOT NULL REFERENCES offices(bsale_office_id),
    quantity             NUMERIC(12,2) NOT NULL DEFAULT 0,
    quantity_reserved    NUMERIC(12,2) NOT NULL DEFAULT 0,
    quantity_available   NUMERIC(12,2) NOT NULL DEFAULT 0,
    synced_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(bsale_variant_id, bsale_office_id)
);

COMMENT ON TABLE stock_levels IS 'Snapshot de inventario. Source: GET /v1/stocks.json?officeid={id}';
COMMENT ON COLUMN stock_levels.quantity_available IS 'quantity - quantity_reserved. Puede ser negativo si allowNegativeStock=true';

CREATE INDEX IF NOT EXISTS idx_stock_office ON stock_levels(bsale_office_id);
CREATE INDEX IF NOT EXISTS idx_stock_variant ON stock_levels(bsale_variant_id);

-- ============================================================
-- TABLAS TRANSACCIONALES (append-mostly, sync incremental)
-- ============================================================

CREATE TABLE IF NOT EXISTS receptions (
    bsale_reception_id   INTEGER PRIMARY KEY,
    bsale_office_id      INTEGER NOT NULL REFERENCES offices(bsale_office_id),
    admission_date       TIMESTAMPTZ NOT NULL,         -- from unix timestamp
    admission_date_raw   DATE,                         -- rawAdmissionDate parsed
    document_ref         VARCHAR(200),                 -- "Sin Documento" o referencia
    document_number      VARCHAR(100),
    note                 TEXT,
    is_internal_dispatch BOOLEAN NOT NULL DEFAULT FALSE, -- internalDispatchId > 0
    is_transfer          BOOLEAN NOT NULL DEFAULT FALSE, -- "TRASLADO" en note
    bsale_user_id        INTEGER,
    synced_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE receptions IS 'Recepciones de stock. Source: GET /v1/stocks/receptions.json';
COMMENT ON COLUMN receptions.is_transfer IS 'Detectado por "TRASLADO" en note O internalDispatchId>0';

CREATE INDEX IF NOT EXISTS idx_receptions_office ON receptions(bsale_office_id);
CREATE INDEX IF NOT EXISTS idx_receptions_date ON receptions(admission_date);

-- ============================================================

CREATE TABLE IF NOT EXISTS reception_details (
    bsale_reception_detail_id  INTEGER PRIMARY KEY,
    bsale_reception_id         INTEGER NOT NULL REFERENCES receptions(bsale_reception_id),
    bsale_variant_id           INTEGER NOT NULL,  -- sin FK: puede referenciar variantes eliminadas
    quantity                   NUMERIC(20,2) NOT NULL,
    cost                       NUMERIC(20,4) NOT NULL DEFAULT 0,
    synced_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE reception_details IS 'Lineas de recepcion. Source: details dentro de receptions';
COMMENT ON COLUMN reception_details.bsale_variant_id IS 'Sin FK: recepciones historicas pueden referenciar variantes ya eliminadas';

CREATE INDEX IF NOT EXISTS idx_recdet_reception ON reception_details(bsale_reception_id);
CREATE INDEX IF NOT EXISTS idx_recdet_variant ON reception_details(bsale_variant_id);

-- ============================================================

CREATE TABLE IF NOT EXISTS documents (
    bsale_document_id       INTEGER PRIMARY KEY,
    bsale_document_type_id  INTEGER NOT NULL REFERENCES document_types(bsale_document_type_id),
    bsale_office_id         INTEGER NOT NULL REFERENCES offices(bsale_office_id),
    emission_date           TIMESTAMPTZ NOT NULL,
    generation_date         TIMESTAMPTZ,
    serial_number           VARCHAR(50),           -- "B001-1"
    doc_number              INTEGER,               -- correlativo
    total_amount            NUMERIC(12,2) NOT NULL DEFAULT 0,
    net_amount              NUMERIC(12,2) NOT NULL DEFAULT 0,
    tax_amount              NUMERIC(12,2) NOT NULL DEFAULT 0,
    exempt_amount           NUMERIC(12,2) NOT NULL DEFAULT 0,
    is_credit_note          BOOLEAN NOT NULL DEFAULT FALSE, -- denormalizado para queries rapidos
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,  -- state=0
    bsale_user_id           INTEGER,
    token                   VARCHAR(50),
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE documents IS 'Documentos de venta (boletas, facturas, NC). Source: GET /v1/documents.json';
COMMENT ON COLUMN documents.is_credit_note IS 'Denormalizado de document_types.is_credit_note para facilitar queries';

CREATE INDEX IF NOT EXISTS idx_docs_type ON documents(bsale_document_type_id);
CREATE INDEX IF NOT EXISTS idx_docs_office ON documents(bsale_office_id);
CREATE INDEX IF NOT EXISTS idx_docs_emission ON documents(emission_date);
CREATE INDEX IF NOT EXISTS idx_docs_credit ON documents(is_credit_note) WHERE is_credit_note = TRUE;

-- ============================================================

CREATE TABLE IF NOT EXISTS document_details (
    bsale_detail_id       INTEGER PRIMARY KEY,
    bsale_document_id     INTEGER NOT NULL REFERENCES documents(bsale_document_id),
    bsale_variant_id      INTEGER NOT NULL,  -- sin FK para no bloquear si variante no existe aun
    quantity              NUMERIC(12,2) NOT NULL,
    net_unit_value        NUMERIC(12,4) NOT NULL DEFAULT 0,  -- precio neto unitario
    net_unit_value_raw    NUMERIC(16,8),                     -- precision completa
    total_unit_value      NUMERIC(12,4) NOT NULL DEFAULT 0,  -- precio con IGV
    net_amount            NUMERIC(12,2) NOT NULL DEFAULT 0,
    tax_amount            NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_amount          NUMERIC(12,2) NOT NULL DEFAULT 0,
    discount_percentage   NUMERIC(5,2) NOT NULL DEFAULT 0,
    net_discount          NUMERIC(12,2) NOT NULL DEFAULT 0,
    is_gratuity           BOOLEAN NOT NULL DEFAULT FALSE,
    synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE document_details IS 'Lineas de venta. Source: details dentro de documents';
COMMENT ON COLUMN document_details.bsale_variant_id IS 'Sin FK explícita: puede llegar un variant_id que aun no sincronizamos';
COMMENT ON COLUMN document_details.is_gratuity IS 'gratuity=1 en BSale: producto regalado, no genera ingreso';

CREATE INDEX IF NOT EXISTS idx_docdet_document ON document_details(bsale_document_id);
CREATE INDEX IF NOT EXISTS idx_docdet_variant ON document_details(bsale_variant_id);

-- ============================================================
-- HISTORIAL DIARIO DE STOCK (una foto por dia)
-- ============================================================

CREATE TABLE IF NOT EXISTS stock_history (
    id                  SERIAL PRIMARY KEY,
    snapshot_date       DATE NOT NULL,
    bsale_variant_id    INTEGER NOT NULL REFERENCES variants(bsale_variant_id),
    bsale_office_id     INTEGER NOT NULL REFERENCES offices(bsale_office_id),
    quantity            NUMERIC(12,2) NOT NULL DEFAULT 0,
    quantity_reserved   NUMERIC(12,2) NOT NULL DEFAULT 0,
    quantity_available  NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(snapshot_date, bsale_variant_id, bsale_office_id)
);

COMMENT ON TABLE stock_history IS 'Historial diario de stock. Una foto por variante/sucursal/dia. Permite analisis de tendencias de inventario.';

CREATE INDEX IF NOT EXISTS idx_sh_date ON stock_history(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_sh_variant ON stock_history(bsale_variant_id);
CREATE INDEX IF NOT EXISTS idx_sh_office ON stock_history(bsale_office_id);
CREATE INDEX IF NOT EXISTS idx_sh_date_office ON stock_history(snapshot_date, bsale_office_id);

-- ============================================================
-- TABLA DE CONTROL DE SINCRONIZACION
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_log (
    id                SERIAL PRIMARY KEY,
    entity            VARCHAR(50) NOT NULL,   -- 'offices', 'variants', 'documents', etc.
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING, SUCCESS, FAILED
    records_fetched   INTEGER DEFAULT 0,
    records_inserted  INTEGER DEFAULT 0,
    records_updated   INTEGER DEFAULT 0,
    records_skipped   INTEGER DEFAULT 0,
    error_message     TEXT,
    params            JSONB                   -- parametros usados (ej: {"since_date": "2026-03-29"})
);

COMMENT ON TABLE sync_log IS 'Registro de cada ejecucion del Harvester para trazabilidad';

-- ============================================================
-- TABLA DE ERRORES DE DATOS
-- ============================================================

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id                SERIAL PRIMARY KEY,
    entity            VARCHAR(50) NOT NULL,
    bsale_id          INTEGER,
    field             VARCHAR(100),
    issue_type        VARCHAR(50) NOT NULL,   -- 'NULL_REQUIRED', 'INVALID_TYPE', 'ORPHAN_FK', 'DUPLICATE'
    description       TEXT,
    raw_value         TEXT,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved          BOOLEAN NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE data_quality_issues IS 'Registro de problemas de calidad detectados durante sync';

CREATE INDEX IF NOT EXISTS idx_dqi_entity ON data_quality_issues(entity, resolved);

-- ============================================================
-- ATRIBUTOS DE CATEGORIA (definicion de tipos de etiqueta)
-- ============================================================

CREATE TABLE IF NOT EXISTS product_type_attributes (
    bsale_attribute_id      INTEGER PRIMARY KEY,
    bsale_product_type_id   INTEGER NOT NULL REFERENCES product_types(bsale_product_type_id),
    name                    VARCHAR(200) NOT NULL DEFAULT 'SIN NOMBRE',
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE product_type_attributes IS 'Atributos definidos por cada tipo de producto/categoria. Source: GET /v1/product_types/{id}/attributes.json';
COMMENT ON COLUMN product_type_attributes.bsale_attribute_id IS 'ID del atributo en BSale (ej: 3 = "Personaje" para product_type 251)';

CREATE INDEX IF NOT EXISTS idx_pta_product_type ON product_type_attributes(bsale_product_type_id);

-- ============================================================
-- VALORES DE ATRIBUTO POR VARIANTE (las etiquetas concretas)
-- ============================================================

CREATE TABLE IF NOT EXISTS variant_attribute_values (
    bsale_av_id             INTEGER PRIMARY KEY,
    bsale_variant_id        INTEGER NOT NULL REFERENCES variants(bsale_variant_id),
    bsale_attribute_id      INTEGER NOT NULL REFERENCES product_type_attributes(bsale_attribute_id),
    description             VARCHAR(500) NOT NULL,   -- el valor textual (ej: "DISNEY")
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(bsale_variant_id, bsale_attribute_id)
);

COMMENT ON TABLE variant_attribute_values IS 'Valores de atributo por variante. Source: GET /v1/variants/{id}/attribute_values.json';
COMMENT ON COLUMN variant_attribute_values.description IS 'Valor textual del atributo (ej: "DISNEY", "FROZEN", "MARVEL")';

CREATE INDEX IF NOT EXISTS idx_vav_variant ON variant_attribute_values(bsale_variant_id);
CREATE INDEX IF NOT EXISTS idx_vav_attribute ON variant_attribute_values(bsale_attribute_id);
