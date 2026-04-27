import jwt
from fastapi import APIRouter, HTTPException, Depends, Header
from psycopg2.extras import RealDictCursor

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/notificaciones", tags=["Notificaciones"])


def _get_usuario_id(authorization: str = Header(None)) -> int:
    if not authorization:
        raise HTTPException(status_code=401, detail="Token no proporcionado")
    try:
        token = authorization.split(" ")[1]
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
        return int(payload.get("sub"))
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido")


@router.get("")
async def listar_notificaciones(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Devuelve todas las notificaciones del usuario autenticado, más recientes primero."""
    usuario_id = _get_usuario_id(authorization)
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT notificacion_id, tipo, titulo, descripcion,
                   datos_asociados, leida, fecha_creacion
            FROM NOTIFICACION
            WHERE usuario_id = %s
            ORDER BY fecha_creacion DESC
            """,
            (usuario_id,),
        )
        rows = cur.fetchall()
        notifs = []
        for r in rows:
            d = dict(r)
            d["fecha_creacion"] = str(d["fecha_creacion"]) if d["fecha_creacion"] else None
            notifs.append(d)
        no_leidas = sum(1 for n in notifs if not n["leida"])
        return {"success": True, "notificaciones": notifs, "no_leidas": no_leidas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.put("/leer-todas")
async def marcar_todas_leidas(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Marca todas las notificaciones del usuario como leídas."""
    usuario_id = _get_usuario_id(authorization)
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE NOTIFICACION SET leida = TRUE WHERE usuario_id = %s AND leida = FALSE",
            (usuario_id,),
        )
        db.commit()
        return {"success": True, "message": "Todas las notificaciones marcadas como leídas"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.put("/{notificacion_id}/leer")
async def marcar_leida(
    notificacion_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Marca una notificación específica como leída (solo si pertenece al usuario)."""
    usuario_id = _get_usuario_id(authorization)
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE NOTIFICACION SET leida = TRUE WHERE notificacion_id = %s AND usuario_id = %s",
            (notificacion_id, usuario_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Notificación no encontrada")
        db.commit()
        return {"success": True, "message": "Notificación marcada como leída"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
