import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "asistencia_vehicular")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASS", "12345678")

    SECRET_KEY = os.getenv("SECRET_KEY", "TU_FIRMA_JWT_SECRET_SUPER_SEGURA")
    ALGORITHM = "HS256"
    