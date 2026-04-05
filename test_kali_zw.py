"""
Script de prueba: captura attribute_values del SKU KALI-ZW (variante 3450).

Pasos:
  1. Pregunta a BSale por los atributos de la variante 3450.
  2. Llama al endpoint de product_types/251/attributes para obtener el nombre
     del atributo (ej: "Personaje").
  3. Crea las tablas si no existen (aplica el DDL de schema.sql sobre la DB).
  4. Inserta los datos.
  5. Muestra el resultado con un SELECT final.

Uso:
    source .venv/Scripts/activate
    python test_kali_zw.py
"""

import sys
import logging

# ── Logging basico para ver lo que pasa ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_kali_zw")

# ── Bootstrap: pool de conexiones y config ─────────────────────────────────────
from harvester import db
from harvester.config import BSALE_BASE_URL, BSALE_HEADERS
from harvester.bsale_client import fetch, fetch_subresource

db.init_pool()

# ── Constantes de la prueba ────────────────────────────────────────────────────
VARIANT_ID      = 3450
SKU             = "KALI-ZW"
PRODUCT_TYPE_ID = 251   # "PRUEBA DISNEY" — viene del JSON


# ══════════════════════════════════════════════════════════════════════════════
# PASO 1: Crear tablas nuevas si no existen
# ══════════════════════════════════════════════════════════════════════════════

DDL = """
CREATE TABLE IF NOT EXISTS product_type_attributes (
    bsale_attribute_id      INTEGER PRIMARY KEY,
    bsale_product_type_id   INTEGER NOT NULL REFERENCES product_types(bsale_product_type_id),
    name                    VARCHAR(200) NOT NULL DEFAULT 'SIN NOMBRE',
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS variant_attribute_values (
    bsale_av_id             INTEGER PRIMARY KEY,
    bsale_variant_id        INTEGER NOT NULL REFERENCES variants(bsale_variant_id),
    bsale_attribute_id      INTEGER NOT NULL REFERENCES product_type_attributes(bsale_attribute_id),
    description             VARCHAR(500) NOT NULL,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(bsale_variant_id, bsale_attribute_id)
);
"""

logger.info("Creando tablas si no existen...")
with db.get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(DDL)
logger.info("OK — tablas listas.")


# ══════════════════════════════════════════════════════════════════════════════
# PASO 2: Obtener el nombre del atributo desde BSale
#         GET /v1/product_types/251/attributes.json
# ══════════════════════════════════════════════════════════════════════════════

logger.info("Llamando a /product_types/%d/attributes.json ...", PRODUCT_TYPE_ID)
attr_url = f"{BSALE_BASE_URL}/product_types/{PRODUCT_TYPE_ID}/attributes.json"
attr_data = fetch(attr_url)

if not attr_data:
    logger.error("No se pudo obtener atributos del product_type %d. Abortando.", PRODUCT_TYPE_ID)
    sys.exit(1)

attr_items = attr_data.get("items", [])
logger.info("Atributos encontrados en product_type %d: %d", PRODUCT_TYPE_ID, len(attr_items))

# Mostrar lo que vino
for a in attr_items:
    logger.info("  → id=%s  name=%s", a.get("id"), a.get("name"))

# Insertar atributos en product_type_attributes
attr_rows = []
for a in attr_items:
    aid = int(a.get("id", 0))
    if aid == 0:
        continue
    attr_rows.append((aid, PRODUCT_TYPE_ID, str(a.get("name", "SIN NOMBRE")).strip()))

if attr_rows:
    sql_attr = """
        INSERT INTO product_type_attributes
            (bsale_attribute_id, bsale_product_type_id, name, synced_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (bsale_attribute_id) DO UPDATE SET
            name = EXCLUDED.name,
            synced_at = NOW()
    """
    db.execute_batch(sql_attr, attr_rows)
    logger.info("Insertados/actualizados %d atributos.", len(attr_rows))
else:
    logger.warning("No se encontraron atributos para insertar.")


# ══════════════════════════════════════════════════════════════════════════════
# PASO 3: Obtener attribute_values de la variante 3450
#         GET /v1/variants/3450/attribute_values.json
# ══════════════════════════════════════════════════════════════════════════════

logger.info("Llamando a /variants/%d/attribute_values.json ...", VARIANT_ID)
av_base_url = f"{BSALE_BASE_URL}/variants/{VARIANT_ID}/attribute_values.json"

# Usamos fetch_subresource para manejar paginacion (aunque sea 1 item ahora)
av_items = fetch_subresource(av_base_url)

logger.info("Attribute values encontrados para variante %d: %d", VARIANT_ID, len(av_items))

if not av_items:
    logger.warning("La variante %d no tiene attribute_values. Nada que insertar.", VARIANT_ID)
    sys.exit(0)

for av in av_items:
    logger.info("  → av_id=%s  desc=%s  attr_id=%s",
                av.get("id"),
                av.get("description"),
                (av.get("attribute") or {}).get("id"))

# Insertar en variant_attribute_values
av_rows = []
for av in av_items:
    av_id   = int(av.get("id", 0))
    av_desc = str(av.get("description", "")).strip()
    av_attr = int((av.get("attribute") or {}).get("id", 0))

    if av_id == 0 or av_attr == 0 or not av_desc:
        logger.warning("Skipping av incompleto: %s", av)
        continue

    av_rows.append((av_id, VARIANT_ID, av_attr, av_desc))

if av_rows:
    sql_av = """
        INSERT INTO variant_attribute_values
            (bsale_av_id, bsale_variant_id, bsale_attribute_id, description, synced_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (bsale_variant_id, bsale_attribute_id) DO UPDATE SET
            description = EXCLUDED.description,
            synced_at   = NOW()
    """
    db.execute_batch(sql_av, av_rows)
    logger.info("Insertados/actualizados %d attribute_values.", len(av_rows))


# ══════════════════════════════════════════════════════════════════════════════
# PASO 4: SELECT final — verificar que todo quedó bien
# ══════════════════════════════════════════════════════════════════════════════

logger.info("\n%s", "=" * 60)
logger.info("RESULTADO FINAL — SELECT de verificacion")
logger.info("%s", "=" * 60)

query = """
    SELECT
        v.display_code              AS sku,
        v.description               AS nombre_variante,
        pta.name                    AS tipo_atributo,
        vav.description             AS valor_atributo
    FROM variant_attribute_values vav
    JOIN variants                v   ON vav.bsale_variant_id  = v.bsale_variant_id
    JOIN product_type_attributes pta ON vav.bsale_attribute_id = pta.bsale_attribute_id
    WHERE v.display_code = %s
    ORDER BY pta.name
"""

with db.get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(query, (SKU,))
        rows = cur.fetchall()

if not rows:
    logger.warning("No se encontraron resultados para SKU=%s. "
                   "Verifica que la variante existe en la tabla variants.", SKU)
else:
    print(f"\n{'SKU':<12} {'VARIANTE':<20} {'TIPO ATRIBUTO':<20} {'VALOR'}")
    print("-" * 65)
    for sku, nombre, tipo, valor in rows:
        print(f"{sku:<12} {(nombre or ''):<20} {(tipo or ''):<20} {valor}")
    print()

db.close_pool()
logger.info("Prueba completada.")
