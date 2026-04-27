import json
from typing import Optional, Any, Dict


def crear_notificacion(
    db,
    usuario_id: int,
    tipo: str,
    titulo: str,
    descripcion: str,
    datos_asociados: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Inserta una notificación dentro de la transacción activa del caller.
    Usa SAVEPOINT para que un fallo no corrompa la transacción principal.
    """
    cur = db.cursor()
    try:
        cur.execute("SAVEPOINT notif_sp")
        cur.execute(
            """
            INSERT INTO NOTIFICACION (usuario_id, tipo, titulo, descripcion, datos_asociados)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                usuario_id,
                tipo,
                titulo[:200],
                descripcion,
                json.dumps(datos_asociados) if datos_asociados else None,
            ),
        )
        cur.execute("RELEASE SAVEPOINT notif_sp")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT notif_sp")
        except Exception:
            pass
    finally:
        cur.close()
