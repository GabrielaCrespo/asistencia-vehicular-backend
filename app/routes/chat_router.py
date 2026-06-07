from fastapi import APIRouter, HTTPException, Depends, Header
from psycopg2.extras import RealDictCursor
from ..classes.postgresql import Database
from ..utils.tenant_deps import get_token_payload

router = APIRouter(prefix="/api/chat", tags=["Chat"])

@router.get("/{incidente_id}/mensajes")
async def obtener_mensajes(
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Obtiene el historial de mensajes de chat de un incidente."""
    payload = get_token_payload(authorization)
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                cm.mensaje_id,
                cm.incidente_id,
                cm.usuario_id,
                cm.rol,
                cm.mensaje,
                cm.leido,
                cm.fecha_creacion,
                u.nombre AS remitente_nombre
            FROM chat_mensaje cm
            JOIN usuario u ON cm.usuario_id = u.usuario_id
            WHERE cm.incidente_id = %s
            ORDER BY cm.fecha_creacion ASC
        """, (incidente_id,))
        mensajes = cur.fetchall()
        return {
            "success": True,
            "mensajes": [dict(m) | {"fecha_creacion": str(m["fecha_creacion"])} for m in mensajes]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()