"""
Configuracion centralizada del Harvester.
Lee variables de entorno desde .env y expone constantes.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raiz del proyecto
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_path)

# --- BSale API ---
BSALE_TOKEN: str = os.environ["BSALE_TOKEN"]
BSALE_BASE_URL: str = "https://api.bsale.io/v1"
BSALE_HEADERS: dict = {"access_token": BSALE_TOKEN, "Accept": "application/json"}

# Rate limiting
BSALE_MAX_RPS: int = 9          # requests por segundo (limite BSale)
BSALE_PAGE_SIZE: int = 50       # maximo permitido por BSale
BSALE_TIMEOUT: int = 25         # segundos
BSALE_MAX_RETRIES: int = 3
BSALE_MAX_WORKERS: int = 6      # hilos concurrentes

# --- PostgreSQL ---
DB_CONFIG: dict = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "coya_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "root"),
}

# --- Sucursales conocidas (para referencia rapida) ---
OFFICES_TIENDA: list[int] = [1, 3]       # Magdalena, Asamblea (venta)
OFFICE_ALMACEN: int = 4                   # Almacen Central (no venta)
