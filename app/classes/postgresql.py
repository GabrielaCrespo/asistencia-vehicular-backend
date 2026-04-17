import psycopg2
import psycopg2.pool
import traceback
from fastapi import HTTPException
from ..services.config import Config
import logging

logger = logging.getLogger(__name__)

# Pool global — se inicializa una sola vez al arrancar el servidor.
# Las solicitudes posteriores reutilizan conexiones existentes (~<100ms)
# en lugar de abrir una nueva cada vez (~3-5s a Render/Oregon).
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        logger.info("Inicializando pool de conexiones a BD...")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=Config.DB_HOST,
            port=int(Config.DB_PORT),
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASS,
            sslmode="prefer",
            connect_timeout=15,
            keepalives_count=5,
        )
        logger.info("✅ Pool de BD listo (min=1, max=10)")
    return _pool


class Database:
    @staticmethod
    def get_db():
        """
        FastAPI dependency que entrega una conexión del pool.
        Al finalizar la petición la devuelve al pool (no la cierra).
        Primera petición: ~3-5s (establece la conexión inicial).
        Peticiones siguientes: <200ms (reutiliza conexión abierta).
        """
        pool = None
        conn = None
        try:
            pool = _get_pool()
            conn = pool.getconn()
            yield conn
        except psycopg2.pool.PoolError as e:
            logger.error(f"Pool agotado: {e}")
            raise HTTPException(
                status_code=503,
                detail="Servidor ocupado, intenta nuevamente."
            )
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Error operacional BD: {e}")
            raise HTTPException(
                status_code=503,
                detail="No se pudo conectar a la base de datos."
            )
        except HTTPException:
                raise
        except Exception as e:
                    logger.error(f"❌ Error inesperado BD: {type(e).__name__}: {e}\n{traceback.format_exc()}")
                    raise HTTPException(
                        status_code=503,
                        detail=f"Error en base de datos: {type(e).__name__}"
                    )
        finally:
            if conn is not None and pool is not None:
                try:
                    if conn.closed:
                        # Conexión muerta — descartarla del pool
                        pool.putconn(conn, close=True)
                    else:
                        # Limpiar estado transaccional antes de devolver al pool
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        pool.putconn(conn)
                except Exception as e:
                    logger.warning(f"Error devolviendo conexión al pool: {e}")