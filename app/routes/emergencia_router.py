from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import Optional

from ..classes.postgresql import Database

router = APIRouter(prefix="/api/emergencia", tags=["Emergencias"])

# ===================== MODELOS =====================

class EmergenciaCreate(BaseModel):
    usuario_id: int
    vehiculo_id: int
    descripcion: str
    latitud: float
    longitud: float
    tipo_problema: Optional[str] = None
    imagen_path: Optional[str] = None
    audio_path: Optional[str] = None

# ===================== ENDPOINTS =====================

@router.post("/registrar")
async def registrar_emergencia(data: EmergenciaCreate, db=Depends(Database.get_db)):
    """Registra una nueva emergencia vehicular"""
    cur = db.cursor()
    try:
        # Verificar que el vehículo pertenece al usuario
        cur.execute("""
            SELECT vehiculo_id FROM VEHICULO 
            WHERE vehiculo_id = %s AND usuario_id = %s
        """, (data.vehiculo_id, data.usuario_id))

        if not cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="El vehículo no pertenece al usuario"
            )

        # Registrar incidente
        cur.execute("""
            INSERT INTO INCIDENTE (
                usuario_id, vehiculo_id, descripcion,
                latitud, longitud, estado, prioridad,
                imagen_path, audio_path
            )
            VALUES (%s, %s, %s, %s, %s, 'pendiente', 'normal', %s, %s)
            RETURNING incidente_id
        """, (
            data.usuario_id,
            data.vehiculo_id,
            data.descripcion,
            data.latitud,
            data.longitud,
            data.imagen_path,
            data.audio_path
        ))

        incidente_id = cur.fetchone()[0]
        db.commit()

        return {
            "success": True,
            "message": "Emergencia registrada correctamente",
            "incidente_id": incidente_id
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.get("/listar/{usuario_id}")
async def listar_emergencias(usuario_id: int, db=Depends(Database.get_db)):
    """Lista las emergencias de un cliente"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                i.incidente_id,
                i.descripcion,
                i.estado,
                i.prioridad,
                i.fecha_creacion,
                v.marca,
                v.modelo,
                v.placa
            FROM INCIDENTE i
            LEFT JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            WHERE i.usuario_id = %s
            ORDER BY i.fecha_creacion DESC
        """, (usuario_id,))

        emergencias = cur.fetchall()
        return {"success": True, "emergencias": [dict(e) for e in emergencias]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()