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
DB_NAME = str = os.environ["DB_NAME"]
DB_HOST = str = os.environ["DB_HOST"]
DB_PORT = str = os.environ["DB_PORT"]
DB_USER = str = os.environ["DB_USER"]
DB_PASSWORD = str = os.environ["DB_PASSWORD"]
DB_CONFIG: dict = {
    "host": os.getenv("DB_HOST", DB_HOST),
    "port": int(os.getenv("DB_PORT", DB_PORT)),
    "dbname": os.getenv("DB_NAME", DB_NAME),
    "user": os.getenv("DB_USER", DB_USER),
    "password": os.getenv("DB_PASSWORD", DB_PASSWORD),
}

# --- Sucursales conocidas (para referencia rapida) ---
OFFICES_TIENDA: list[int] = [1, 3]       # Magdalena, Asamblea (venta)
OFFICE_ALMACEN: int = 4                   # Almacen Central (no venta)
