import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno con prioridad: .env.local > .env > valores por defecto
backend_dir = Path(__file__).parent.parent.parent
env_local_path = backend_dir / ".env.local"
env_path = backend_dir / ".env"

# Primero cargar .env (defaults p/ producción)
if env_path.exists():
    load_dotenv(env_path, override=False)

# Luego cargar .env.local si existe (sobreescribe .env para desarrollo local)
if env_local_path.exists():
    load_dotenv(env_local_path, override=True)

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
    