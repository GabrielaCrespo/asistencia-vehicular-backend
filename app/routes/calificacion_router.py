"""
ROUTER DE CALIFICACIONES Y VALORACIONES

Permite a los clientes calificar un servicio finalizado:
  - Una sola valoración por incidente (inmutable).
  - Solo el cliente que solicitó el servicio puede calificarlo.
  - Solo se califican incidentes en estado finalizado.
  - Actualiza automáticamente calificacion_promedio y cantidad_resenas del taller.
"""

from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, validator
from psycopg2.extras import RealDictCursor
from typing import Optional, List

from ..classes.postgresql import Database
from ..utils.notificaciones import crear_notificacion
from ..utils.tenant_deps import get_token_payload

router = APIRouter(prefix="/api/calificacion", tags=["Calificaciones"])

# Estados que permiten calificar
_ESTADOS_FINALIZADOS = ('atendido', 'completado', 'cerrada', 'finalizado')


# ===================== MODELOS REQUEST =====================

class CalificacionCreate(BaseModel):
    incidente_id: int
    puntuacion: int          # 1–5 calificación del taller
    puntuacion_servicio: Optional[int] = None  # 1–5 calificación del servicio
    comentario: Optional[str] = None

    @validator("puntuacion")
    def val_puntuacion(cls, v):
        if not 1 <= v <= 5:
            raise ValueError("La calificación del taller debe ser entre 1 y 5")
        return v

    @validator("puntuacion_servicio")
    def val_puntuacion_servicio(cls, v):
        if v is not None and not 1 <= v <= 5:
            raise ValueError("La calificación del servicio debe ser entre 1 y 5")
        return v


# ===================== ENDPOINTS =====================

@router.post("")
async def crear_calificacion(
    data: CalificacionCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Registra la valoración de un cliente sobre un servicio finalizado.
    Reglas:
    - Solo el cliente propietario del incidente puede calificarlo.
    - Solo incidentes finalizados.
    - Una sola valoración por incidente (no modificable).
    """
    payload = get_token_payload(authorization)
    usuario_id = int(payload.get("sub", 0))

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Verificar incidente y obtener contexto
        cur.execute("""
            SELECT
                i.incidente_id,
                i.estado,
                i.usuario_id,
                i.descripcion,
                i.fecha_creacion,
                a.taller_id,
                t.razon_social  AS taller_nombre,
                t.usuario_id    AS taller_usuario_id
            FROM incidente i
            LEFT JOIN asignacion a ON a.incidente_id = i.incidente_id
            LEFT JOIN taller t     ON t.taller_id    = a.taller_id
            WHERE i.incidente_id = %s
        """, (data.incidente_id,))
        incidente = cur.fetchone()

        if not incidente:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")

        if incidente["usuario_id"] != usuario_id:
            raise HTTPException(
                status_code=403,
                detail="Solo el cliente del servicio puede calificarlo",
            )

        if incidente["estado"] not in _ESTADOS_FINALIZADOS:
            raise HTTPException(
                status_code=400,
                detail="Solo se pueden calificar servicios finalizados",
            )

        if not incidente["taller_id"]:
            raise HTTPException(
                status_code=400,
                detail="Este incidente no tiene taller asignado",
            )

        # 2. Verificar que no exista ya una calificación
        cur.execute(
            "SELECT calificacion_id FROM calificacion WHERE incidente_id = %s",
            (data.incidente_id,),
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=409,
                detail="Este servicio ya fue calificado",
            )

        taller_id = incidente["taller_id"]

        # 3. Insertar calificación
        cur.execute("""
            INSERT INTO calificacion
                (incidente_id, usuario_id, taller_id, puntuacion, puntuacion_servicio, comentario)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING calificacion_id, fecha_calificacion
        """, (
            data.incidente_id,
            usuario_id,
            taller_id,
            data.puntuacion,
            data.puntuacion_servicio,
            data.comentario,
        ))
        nueva = cur.fetchone()

        # 4. Recalcular KPIs del taller
        cur.execute("""
            UPDATE taller
            SET
                calificacion_promedio = (
                    SELECT COALESCE(ROUND(AVG(puntuacion)::NUMERIC, 2), 0)
                    FROM calificacion
                    WHERE taller_id = %s
                ),
                cantidad_resenas = (
                    SELECT COUNT(*) FROM calificacion WHERE taller_id = %s
                )
            WHERE taller_id = %s
        """, (taller_id, taller_id, taller_id))

        # 5. Notificar al taller
        if incidente.get("taller_usuario_id"):
            crear_notificacion(
                db,
                incidente["taller_usuario_id"],
                "nueva_calificacion",
                "Nueva valoración recibida",
                f"Tu taller recibió una valoración de {data.puntuacion}/5 estrellas",
                {
                    "incidente_id": data.incidente_id,
                    "puntuacion":   data.puntuacion,
                },
            )

        db.commit()
        return {
            "success":         True,
            "calificacion_id": nueva["calificacion_id"],
            "message":         "Valoración registrada exitosamente",
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error registrando calificación: {str(e)}")
    finally:
        cur.close()


@router.get("/detalle-incidente/{incidente_id}")
async def detalle_para_calificacion(
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Devuelve la información del incidente necesaria para el formulario de valoración,
    incluyendo si ya fue calificado y si puede calificarse.
    """
    payload = get_token_payload(authorization)
    usuario_id = int(payload.get("sub", 0))

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                i.incidente_id,
                i.descripcion,
                i.tipo_problema,
                i.estado,
                i.fecha_creacion,
                i.fecha_cierre,
                i.usuario_id,
                t.taller_id,
                t.razon_social        AS taller_nombre,
                t.direccion           AS taller_direccion,
                tec.nombre            AS tecnico_nombre,
                tec.especialidad      AS tecnico_especialidad,
                a.fecha_cierre_servicio
            FROM incidente i
            LEFT JOIN asignacion a ON a.incidente_id = i.incidente_id
            LEFT JOIN taller t     ON t.taller_id    = a.taller_id
            LEFT JOIN tecnico tec  ON tec.tecnico_id = a.tecnico_id
            WHERE i.incidente_id = %s
        """, (incidente_id,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")

        if row["usuario_id"] != usuario_id:
            raise HTTPException(status_code=403, detail="No autorizado")

        cur.execute(
            "SELECT calificacion_id FROM calificacion WHERE incidente_id = %s",
            (incidente_id,),
        )
        ya_calificado = cur.fetchone() is not None

        return {
            **dict(row),
            "ya_calificado":   ya_calificado,
            "puede_calificar": row["estado"] in _ESTADOS_FINALIZADOS and not ya_calificado,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.get("/incidente/{incidente_id}")
async def get_calificacion_incidente(
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Devuelve la calificación registrada para un incidente, si existe."""
    get_token_payload(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                c.calificacion_id,
                c.puntuacion,
                c.puntuacion_servicio,
                c.comentario,
                c.fecha_calificacion,
                t.razon_social AS taller_nombre,
                i.descripcion,
                i.fecha_creacion,
                i.estado
            FROM calificacion c
            JOIN taller   t ON t.taller_id   = c.taller_id
            JOIN incidente i ON i.incidente_id = c.incidente_id
            WHERE c.incidente_id = %s
        """, (incidente_id,))
        cal = cur.fetchone()

        if not cal:
            return {"exists": False, "calificacion": None}

        return {"exists": True, "calificacion": dict(cal)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.get("/taller/{taller_id}")
async def get_calificaciones_taller(
    taller_id: int,
    limit: int = 20,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista las calificaciones recibidas por un taller con estadísticas."""
    get_token_payload(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                c.calificacion_id,
                c.puntuacion,
                c.puntuacion_servicio,
                c.comentario,
                c.fecha_calificacion,
                u.nombre   AS cliente_nombre,
                i.tipo_problema,
                i.descripcion AS incidente_descripcion
            FROM calificacion c
            JOIN usuario   u ON u.usuario_id   = c.usuario_id
            JOIN incidente i ON i.incidente_id = c.incidente_id
            WHERE c.taller_id = %s
            ORDER BY c.fecha_calificacion DESC
            LIMIT %s OFFSET %s
        """, (taller_id, limit, offset))
        rows = cur.fetchall()

        cur.execute("""
            SELECT
                COUNT(*)                                              AS total,
                COALESCE(ROUND(AVG(puntuacion)::NUMERIC, 2), 0)      AS promedio,
                COALESCE(ROUND(AVG(puntuacion_servicio)::NUMERIC, 2), 0) AS promedio_servicio
            FROM calificacion
            WHERE taller_id = %s
        """, (taller_id,))
        stats = cur.fetchone()

        return {
            "taller_id":        taller_id,
            "total":            int(stats["total"] or 0),
            "promedio":         float(stats["promedio"] or 0),
            "promedio_servicio": float(stats["promedio_servicio"] or 0),
            "limit":            limit,
            "offset":           offset,
            "data":             [dict(r) for r in rows],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
