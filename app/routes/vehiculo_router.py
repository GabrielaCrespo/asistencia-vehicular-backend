from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import Optional

from ..classes.postgresql import Database
from ..utils.tenant_deps import get_token_payload
from ..utils.bitacora import log_bitacora

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
async def registrar_vehiculo(data: VehiculoCreate, authorization: str = Header(None), db=Depends(Database.get_db)):
    """Registra un nuevo vehículo"""
    payload = get_token_payload(authorization)
    if int(payload.get("sub", 0)) != data.usuario_id:
        raise HTTPException(status_code=403, detail="No autorizado")
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
        log_bitacora(cur, data.usuario_id, 'REGISTRAR_VEHICULO', 'vehiculo',
                     vehiculo_id, f'Vehículo registrado: {data.placa} {data.marca} {data.modelo}')
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
async def listar_vehiculos(usuario_id: int, authorization: str = Header(None), db=Depends(Database.get_db)):
    """Lista los vehículos de un cliente"""
    payload = get_token_payload(authorization)
    if int(payload.get("sub", 0)) != usuario_id:
        raise HTTPException(status_code=403, detail="No autorizado")
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
async def eliminar_vehiculo(vehiculo_id: int, authorization: str = Header(None), db=Depends(Database.get_db)):
    """Elimina un vehículo"""
    payload = get_token_payload(authorization)
    cur = db.cursor()
    try:
        cur.execute("SELECT usuario_id FROM VEHICULO WHERE vehiculo_id = %s", (vehiculo_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")
        if row[0] != int(payload.get("sub", 0)):
            raise HTTPException(status_code=403, detail="No autorizado")

        cur.execute(
            "DELETE FROM VEHICULO WHERE vehiculo_id = %s",
            (vehiculo_id,)
        )

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Vehículo no encontrado")

        usuario_id = int(payload.get("sub", 0))
        log_bitacora(cur, usuario_id, 'ELIMINAR_VEHICULO', 'vehiculo',
                     vehiculo_id, f'Vehículo {vehiculo_id} eliminado')
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