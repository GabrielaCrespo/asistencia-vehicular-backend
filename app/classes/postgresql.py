import psycopg2
from app.services.config import Config

class Database:
    @staticmethod
    def get_db():
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASS,
            sslmode="require",
            options="-c search_path=public"
        )
        try:
            yield conn
        finally:
            conn.close()