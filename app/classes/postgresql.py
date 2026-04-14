import psycopg2
from fastapi import HTTPException
from ..services.config import Config
import logging

logger = logging.getLogger(__name__)

class Database:
    @staticmethod
    def get_db():
        try:
            # Log de la configuración (sin password)
            logger.info(f"Intentando conectar a BD: {Config.DB_USER}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
            
            conn = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASS,
                sslmode="require",
                options="-c search_path=public",
                connect_timeout=10
            )
            logger.info("✅ Conexión a BD exitosa")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Error operacional BD: {str(e)}")
            raise HTTPException(
                status_code=503, 
                detail=f"No se pudo conectar a la base de datos. Verifica las variables de ambiente DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS"
            )
        except Exception as e:
            logger.error(f"❌ Error inesperado en BD: {str(e)}")
            raise HTTPException(
                status_code=503, 
                detail=f"Error en base de datos: {type(e).__name__}"
            )
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error al cerrar conexión: {str(e)}")