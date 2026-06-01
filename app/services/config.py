import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urlparse

backend_dir = Path(__file__).parent.parent.parent
env_local_path = backend_dir / ".env.local"
env_path = backend_dir / ".env"

if env_path.exists():
    load_dotenv(env_path, override=False)

if env_local_path.exists():
    load_dotenv(env_local_path, override=True)

# Render inyecta DATABASE_URL al vincular una BD al servicio web.
# Si las variables individuales no están, las parseamos de DATABASE_URL.
_database_url = os.getenv("DATABASE_URL", "")
if _database_url and not os.getenv("DB_HOST"):
    _parsed = urlparse(_database_url)
    os.environ.setdefault("DB_HOST", _parsed.hostname or "")
    os.environ.setdefault("DB_PORT", str(_parsed.port or 5432))
    os.environ.setdefault("DB_NAME", (_parsed.path or "").lstrip("/"))
    os.environ.setdefault("DB_USER", _parsed.username or "")
    os.environ.setdefault("DB_PASS", _parsed.password or "")


class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "asistencia_vehicular")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "12345678")
    SECRET_KEY = os.getenv("SECRET_KEY", "TU_FIRMA_JWT_SECRET_SUPER_SEGURA")
    ALGORITHM = "HS256"

    CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME", "")
    CLOUDINARY_API_KEY    = os.getenv("CLOUDINARY_API_KEY", "")
    CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET", "")

    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
