from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import Optional

from ..classes.postgresql import Database

router = APIRouter(prefix="/api/vehiculo", tags=["Vehículos"])

# ===================== MODELOS =====================

class VehiculoCreate(BaseModel):
    usuario_id: int
    placa: str
    marca: str
    modelo: str
    anio: Optional[int] = None
    tipo: Optional[str] = None
    color: Optional[str] = None

# ===================== ENDPOINTS =====================

@router.post("/registrar")
async def registrar_vehiculo(data: VehiculoCreate, db=Depends(Database.get_db)):
    """Registra un nuevo vehículo"""
    cur = db.cursor()
    try:
        # Verificar placa única
        cur.execute(
            "SELECT vehiculo_id FROM VEHICULO WHERE placa = %s LIMIT 1",
            (data.placa.upper(),)
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="La placa ya está registrada")

        cur.execute("""
            INSERT INTO VEHICULO (usuario_id, placa, marca, modelo, anio, tipo, color)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING vehiculo_id
        """, (
            data.usuario_id,
            data.placa.upper(),
            data.marca.upper(),
            data.modelo.upper(),
            data.anio,
            data.tipo,
            data.color.upper() if data.color else None
        ))

        vehiculo_id = cur.fetchone()[0]
        db.commit()

        return {"success": True, "message": "Vehículo registrado", "vehiculo_id": vehiculo_id}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.get("/listar/{usuario_id}")
async def listar_vehiculos(usuario_id: int, db=Depends(Database.get_db)):
    """Lista los vehículos de un cliente"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT vehiculo_id, placa, marca, modelo, anio, tipo, color
            FROM VEHICULO
            WHERE usuario_id = %s
            ORDER BY creado_en DESC
        """, (usuario_id,))

        vehiculos = cur.fetchall()
        return {"success": True, "vehiculos": [dict(v) for v in vehiculos]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.delete("/eliminar/{vehiculo_id}")
async def eliminar_vehiculo(vehiculo_id: int, db=Depends(Database.get_db)):
    """Elimina un vehículo"""
    cur = db.cursor()
    try:
        cur.execute(
            "DELETE FROM VEHICULO WHERE vehiculo_id = %s",
            (vehiculo_id,)
        )

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")

        db.commit()
        return {"success": True, "message": "Vehículo eliminado"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()