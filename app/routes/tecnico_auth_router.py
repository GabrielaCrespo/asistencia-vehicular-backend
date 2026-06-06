from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
import jwt
from typing import Optional
from ..services.config import Config
from ..classes.postgresql import Database
from ..utils.tenant_deps import get_token_payload
from ..utils.bitacora import log_bitacora

router = APIRouter(prefix="/api/tecnico", tags=["Técnico Auth"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

class TecnicoLogin(BaseModel):
    email: str
    password: str

class TecnicoLoginResponse(BaseModel):
    success: bool
    access_token: str
    tecnico_id: int
    nombre: str
    taller_id: int
    taller_nombre: str

class ActualizarEstadoRequest(BaseModel):
    estado: str

@router.post("/login")
async def login_tecnico(data: TecnicoLogin, db=Depends(Database.get_db)):
    """Login del técnico con credenciales automáticas"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                u.usuario_id,
                u.contrasena_hash,
                u.estado,
                t.tecnico_id,
                t.nombre,
                t.taller_id,
                ta.razon_social as taller_nombre
            FROM USUARIO u
            JOIN TECNICO t ON u.usuario_id = t.usuario_id
            JOIN TALLER ta ON t.taller_id = ta.taller_id
            WHERE u.email = %s AND u.rol_id = 3
        """, (data.email.lower(),))

        tecnico = cur.fetchone()

        if not tecnico:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        if not verify_password(data.password, tecnico['contrasena_hash']):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        token_payload = {
            "sub": str(tecnico['usuario_id']),
            "tecnico_id": tecnico['tecnico_id'],
            "taller_id": tecnico['taller_id'],
            "rol": "tecnico",
            "exp": datetime.now(tz=timezone.utc) + timedelta(hours=24)
        }
        token = jwt.encode(token_payload, Config.SECRET_KEY, algorithm=Config.ALGORITHM)

        log_bitacora(cur, tecnico['usuario_id'], 'LOGIN_TECNICO', 'usuario',
                     tecnico['tecnico_id'], f'Login técnico: {data.email}')
        db.commit()

        return TecnicoLoginResponse(
            success=True,
            access_token=token,
            tecnico_id=tecnico['tecnico_id'],
            nombre=tecnico['nombre'],
            taller_id=tecnico['taller_id'],
            taller_nombre=tecnico['taller_nombre']
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

@router.get("/asignacion/{tecnico_id}")
async def get_asignacion_tecnico(tecnico_id: int, authorization: str = Header(None), db=Depends(Database.get_db)):
    """Obtiene la asignación activa del técnico"""
    payload = get_token_payload(authorization)
    if int(payload.get("tecnico_id", -1)) != tecnico_id:
        raise HTTPException(status_code=403, detail="No autorizado")
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                a.asignacion_id,
                a.estado,
                a.tiempo_estimado_minutos,
                i.incidente_id,
                i.descripcion,
                i.latitud,
                i.longitud,
                i.imagen_path,
                i.audio_path,
                i.prioridad,
                u.nombre as cliente_nombre,
                u.telefono as cliente_telefono,
                v.marca,
                v.modelo,
                v.placa
            FROM ASIGNACION a
            JOIN INCIDENTE i ON a.incidente_id = i.incidente_id
            JOIN USUARIO u ON i.usuario_id = u.usuario_id
            JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            WHERE a.tecnico_id = %s
            AND a.estado IN ('aceptada', 'en_camino', 'en_servicio')
            ORDER BY a.fecha_asignacion DESC
            LIMIT 1
        """, (tecnico_id,))

        asignacion = cur.fetchone()

        if not asignacion:
            return {"success": True, "asignacion": None}

        return {"success": True, "asignacion": dict(asignacion)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
class TecnicoDiagnosticoRequest(BaseModel):
    observaciones: str
    costo: float
    metodo_pago: Optional[str] = None

@router.put("/asignacion/{asignacion_id}/estado")
async def actualizar_estado_tecnico(
    asignacion_id: int,
    data: ActualizarEstadoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    payload = get_token_payload(authorization)
    ESTADOS_VALIDOS = ('en_camino', 'en_servicio', 'completada')
    if data.estado not in ESTADOS_VALIDOS:
        raise HTTPException(status_code=400, detail="Estado inválido.")

    cur = db.cursor()
    try:
        cur.execute(
            "SELECT asignacion_id, incidente_id, tecnico_id FROM ASIGNACION WHERE asignacion_id = %s",
            (asignacion_id,)
        )
        asignacion = cur.fetchone()
        if not asignacion:
            raise HTTPException(status_code=404, detail="Asignación no encontrada")
        if int(payload.get("tecnico_id", -1)) != asignacion[2]:
            raise HTTPException(status_code=403, detail="No autorizado")

        # Actualizar estado de la asignación
        cur.execute(
            "UPDATE ASIGNACION SET estado = %s WHERE asignacion_id = %s",
            (data.estado, asignacion_id)
        )

        # Actualizar estado del incidente según el estado del técnico
        if data.estado == 'en_camino':
            cur.execute(
                "UPDATE INCIDENTE SET estado = 'en_camino', fecha_actualizacion = CURRENT_TIMESTAMP WHERE incidente_id = %s",
                (asignacion[1],)
            )
        elif data.estado == 'en_servicio':
            cur.execute(
                "UPDATE INCIDENTE SET estado = 'en_servicio', fecha_actualizacion = CURRENT_TIMESTAMP WHERE incidente_id = %s",
                (asignacion[1],)
            )
        elif data.estado == 'completada':
            cur.execute(
                "UPDATE INCIDENTE SET estado = 'atendido', fecha_actualizacion = CURRENT_TIMESTAMP WHERE incidente_id = %s",
                (asignacion[1],)
            )
            if asignacion[2]:
                cur.execute(
                    "UPDATE TECNICO SET disponible = TRUE WHERE tecnico_id = %s",
                    (asignacion[2],)
                )

        db.commit()
        return {"success": True, "message": f"Estado actualizado a '{data.estado}'"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

@router.put("/asignacion/{asignacion_id}/finalizar")
async def finalizar_servicio_tecnico(
    asignacion_id: int,
    data: TecnicoDiagnosticoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Finaliza el servicio y registra el pago desde el técnico"""
    payload = get_token_payload(authorization)
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT asignacion_id, incidente_id, tecnico_id, taller_id FROM ASIGNACION WHERE asignacion_id = %s",
            (asignacion_id,)
        )
        asignacion = cur.fetchone()
        if not asignacion:
            raise HTTPException(status_code=404, detail="Asignación no encontrada")
        if int(payload.get("tecnico_id", -1)) != asignacion[2]:
            raise HTTPException(status_code=403, detail="No autorizado")

        comision = round(data.costo * 0.10, 2)
        monto_taller = round(data.costo * 0.90, 2)

        # Actualizar observaciones y marcar completada
        cur.execute("""
            UPDATE ASIGNACION
            SET observaciones = %s, estado = 'completada'
            WHERE asignacion_id = %s
        """, (data.observaciones, asignacion_id))

        # Cerrar el incidente
        cur.execute("""
            UPDATE INCIDENTE
            SET estado = 'atendido', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s
        """, (asignacion[1],))

        # Liberar técnico
        if asignacion[2]:
            cur.execute(
                "UPDATE TECNICO SET disponible = TRUE WHERE tecnico_id = %s",
                (asignacion[2],)
            )

        # Registrar pago en estado pendiente — el cliente elige y confirma el método de pago
        cur.execute("""
            INSERT INTO PAGO (
                incidente_id, asignacion_id, monto_total, monto_servicio,
                comision_plataforma, monto_taller, estado
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'pendiente')
            ON CONFLICT (incidente_id) DO UPDATE SET
                monto_total          = EXCLUDED.monto_total,
                monto_servicio       = EXCLUDED.monto_servicio,
                comision_plataforma  = EXCLUDED.comision_plataforma,
                monto_taller         = EXCLUDED.monto_taller,
                estado               = CASE WHEN PAGO.estado = 'completado' THEN 'completado' ELSE 'pendiente' END
        """, (
            asignacion[1], asignacion_id,
            data.costo, data.costo, comision, monto_taller,
        ))

        db.commit()
        return {"success": True, "message": "Servicio finalizado correctamente"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()       