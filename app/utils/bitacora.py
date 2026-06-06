"""
Helper compartido de auditoría — puede ser importado desde cualquier router.
"""
import json
from typing import Optional


def log_bitacora(
    cur,
    usuario_id: Optional[int],
    accion: str,
    tabla: str,
    id_ref: Optional[int] = None,
    descripcion: Optional[str] = None,
    datos: Optional[dict] = None,
    organizacion_id: Optional[int] = None,
) -> None:
    """Inserta un registro en BITACORA. Nunca lanza excepción."""
    try:
        cur.execute(
            """
            INSERT INTO bitacora
                (usuario_id, accion, tabla_afectada, id_referencia,
                 descripcion, datos_cambio, organizacion_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                usuario_id,
                accion,
                tabla,
                id_ref,
                descripcion,
                json.dumps(datos) if datos else None,
                organizacion_id,
            ),
        )
    except Exception:
        pass
