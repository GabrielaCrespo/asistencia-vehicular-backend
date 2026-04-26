from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import jwt
from typing import List, Optional

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/pagos", tags=["Pagos e Ingresos"])


# ===================== MODELOS RESPONSE =====================

class PagoIngreso(BaseModel):
    pago_id: int
    incidente_id: int
    asignacion_id: Optional[int]
    monto_total: float
    monto_servicio: float
    comision_plataforma: float
    monto_taller: float
    metodo_pago: Optional[str]
    estado: str
    estado_comision: str
    fecha_pago: Optional[str]
    fecha_pago_comision: Optional[str]
    observaciones: Optional[str]
    creado_en: str
    cliente_nombre: Optional[str]
    descripcion_incidente: Optional[str]
    tipo_problema: Optional[str]


class ResumenIngresos(BaseModel):
    total_ingresos: float
    total_bruto: float
    total_comision_pendiente: float
    total_comision_pagada: float
    cantidad_servicios: int
    cantidad_comisiones_pendientes: int


class MessageResponse(BaseModel):
    success: bool
    message: str


# ===================== HELPERS =====================

def _get_token(authorization: str) -> dict:
    if not authorization:
        raise HTTPException(status_code=401, detail="Token no proporcionado")
    try:
        token = authorization.split(" ")[1]
    except IndexError:
        raise HTTPException(status_code=401, detail="Formato de token inválido")
    try:
        return jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


def _verify_taller(token_payload: dict, taller_id: int, db):
    usuario_id = int(token_payload.get("sub"))
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT usuario_id FROM TALLER WHERE taller_id = %s", (taller_id,))
        taller = cur.fetchone()
        if not taller or taller["usuario_id"] != usuario_id:
            raise HTTPException(status_code=403, detail="No tienes permiso para acceder a este taller")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error verificando acceso: {str(e)}")
    finally:
        cur.close()


def _row_to_pago(row: dict) -> PagoIngreso:
    return PagoIngreso(
        pago_id=row["pago_id"],
        incidente_id=row["incidente_id"],
        asignacion_id=row.get("asignacion_id"),
        monto_total=float(row["monto_total"]),
        monto_servicio=float(row["monto_servicio"]),
        comision_plataforma=float(row["comision_plataforma"]),
        monto_taller=float(row["monto_taller"]),
        metodo_pago=row.get("metodo_pago"),
        estado=row["estado"],
        estado_comision=row.get("estado_comision", "pendiente"),
        fecha_pago=str(row["fecha_pago"]) if row.get("fecha_pago") else None,
        fecha_pago_comision=str(row["fecha_pago_comision"]) if row.get("fecha_pago_comision") else None,
        observaciones=row.get("observaciones"),
        creado_en=str(row["creado_en"]),
        cliente_nombre=row.get("cliente_nombre"),
        descripcion_incidente=row.get("descripcion_incidente"),
        tipo_problema=row.get("tipo_problema"),
    )


# ===================== ENDPOINTS =====================

@router.get("/{taller_id}/resumen", response_model=ResumenIngresos)
async def resumen_ingresos(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Resumen financiero del taller: ingresos, comisiones pagadas y pendientes."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                COALESCE(SUM(p.monto_taller), 0)                                            AS total_ingresos,
                COALESCE(SUM(p.monto_total), 0)                                             AS total_bruto,
                COALESCE(SUM(CASE WHEN p.estado_comision = 'pendiente' THEN p.comision_plataforma ELSE 0 END), 0) AS total_comision_pendiente,
                COALESCE(SUM(CASE WHEN p.estado_comision = 'pagado'    THEN p.comision_plataforma ELSE 0 END), 0) AS total_comision_pagada,
                COUNT(p.pago_id)                                                             AS cantidad_servicios,
                COUNT(CASE WHEN p.estado_comision = 'pendiente' THEN 1 END)                 AS cantidad_comisiones_pendientes
            FROM PAGO p
            JOIN ASIGNACION a ON p.asignacion_id = a.asignacion_id
            WHERE a.taller_id = %s
              AND p.estado = 'completado'
        """, (taller_id,))
        row = cur.fetchone()
        return ResumenIngresos(
            total_ingresos=float(row["total_ingresos"]),
            total_bruto=float(row["total_bruto"]),
            total_comision_pendiente=float(row["total_comision_pendiente"]),
            total_comision_pagada=float(row["total_comision_pagada"]),
            cantidad_servicios=int(row["cantidad_servicios"]),
            cantidad_comisiones_pendientes=int(row["cantidad_comisiones_pendientes"]),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo resumen: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/ingresos", response_model=List[PagoIngreso])
async def listar_ingresos(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todos los ingresos generados por el taller (servicios completados y cobrados)."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                p.pago_id,
                p.incidente_id,
                p.asignacion_id,
                p.monto_total,
                p.monto_servicio,
                p.comision_plataforma,
                p.monto_taller,
                p.metodo_pago,
                p.estado,
                p.estado_comision,
                p.fecha_pago,
                p.fecha_pago_comision,
                p.observaciones,
                p.creado_en,
                u.nombre   AS cliente_nombre,
                i.descripcion AS descripcion_incidente,
                i.tipo_problema
            FROM PAGO p
            JOIN ASIGNACION a   ON p.asignacion_id = a.asignacion_id
            JOIN INCIDENTE i    ON p.incidente_id  = i.incidente_id
            JOIN USUARIO u      ON i.usuario_id    = u.usuario_id
            WHERE a.taller_id = %s
              AND p.estado = 'completado'
            ORDER BY p.fecha_pago DESC
        """, (taller_id,))
        rows = cur.fetchall()
        return [_row_to_pago(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando ingresos: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/comisiones", response_model=List[PagoIngreso])
async def historial_comisiones(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Historial completo de comisiones (pagadas y pendientes) del taller."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                p.pago_id,
                p.incidente_id,
                p.asignacion_id,
                p.monto_total,
                p.monto_servicio,
                p.comision_plataforma,
                p.monto_taller,
                p.metodo_pago,
                p.estado,
                p.estado_comision,
                p.fecha_pago,
                p.fecha_pago_comision,
                p.observaciones,
                p.creado_en,
                u.nombre   AS cliente_nombre,
                i.descripcion AS descripcion_incidente,
                i.tipo_problema
            FROM PAGO p
            JOIN ASIGNACION a   ON p.asignacion_id = a.asignacion_id
            JOIN INCIDENTE i    ON p.incidente_id  = i.incidente_id
            JOIN USUARIO u      ON i.usuario_id    = u.usuario_id
            WHERE a.taller_id = %s
              AND p.estado = 'completado'
            ORDER BY p.estado_comision ASC, p.fecha_pago DESC
        """, (taller_id,))
        rows = cur.fetchall()
        return [_row_to_pago(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando comisiones: {str(e)}")
    finally:
        cur.close()


@router.post("/{taller_id}/{pago_id}/pagar-comision", response_model=MessageResponse)
async def pagar_comision(
    taller_id: int,
    pago_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Marca la comisión del 10% de un pago como pagada a la plataforma."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el pago pertenece al taller
        cur.execute("""
            SELECT p.pago_id, p.estado_comision, p.comision_plataforma
            FROM PAGO p
            JOIN ASIGNACION a ON p.asignacion_id = a.asignacion_id
            WHERE p.pago_id = %s AND a.taller_id = %s AND p.estado = 'completado'
        """, (pago_id, taller_id))
        pago = cur.fetchone()
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado o no pertenece a este taller")
        if pago["estado_comision"] == "pagado":
            raise HTTPException(status_code=400, detail="La comisión de este pago ya fue registrada como pagada")

        cur.execute("""
            UPDATE PAGO
            SET estado_comision = 'pagado',
                fecha_pago_comision = CURRENT_TIMESTAMP
            WHERE pago_id = %s
        """, (pago_id,))
        db.commit()
        monto = float(pago["comision_plataforma"])
        return MessageResponse(
            success=True,
            message=f"Comisión de ${monto:,.2f} registrada como pagada a la plataforma"
        )
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error registrando pago de comisión: {str(e)}")
    finally:
        cur.close()
