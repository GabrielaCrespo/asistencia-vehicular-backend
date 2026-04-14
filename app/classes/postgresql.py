import psycopg2
from fastapi import HTTPException
from app.services.config import Config

class Database:
    @staticmethod
    def get_db():
        try:
            conn = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASS,
                sslmode="require",
                options="-c search_path=public"
            )
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"No se pudo conectar a la base de datos: {str(e)}")
        try:
            yield conn
        finally:
            conn.close()