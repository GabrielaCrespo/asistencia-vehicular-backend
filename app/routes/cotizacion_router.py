from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import asyncio
from typing import List, Optional

from ..classes.postgresql import Database
from ..utils.notificaciones import crear_notificacion
from ..utils.tenant_deps import get_token_payload, assert_taller_access
from ..managers.websocket_manager import manager

router = APIRouter(prefix="/api/cotizacion", tags=["Cotizaciones"])


# ===================== MODELOS REQUEST =====================

class RegistrarCotizacionRequest(BaseModel):
    incidente_id: int
    costo_estimado: float
    tiempo_estimado: int   # horas estimadas de reparación
    observaciones: Optional[str] = None


class ActualizarCotizacionRequest(BaseModel):
    costo_estimado: float
    tiempo_estimado: int
    observaciones: Optional[str] = None


class SeleccionarCotizacionRequest(BaseModel):
    cotizacion_id: int
    usuario_id: int


# ===================== MODELOS RESPONSE =====================

class CotizacionResponse(BaseModel):
    cotizacion_id: int
    incidente_id: int
    taller_id: int
    taller_nombre: str
    calificacion_promedio: float
    costo_estimado: float
    tiempo_estimado: int
    observaciones: Optional[str]
    estado: str
    fecha_creacion: str


class RegistrarResponse(BaseModel):
    success: bool
    message: str
    cotizacion_id: int


class SeleccionarResponse(BaseModel):
    success: bool
    message: str
    asignacion_id: int


class MessageResponse(BaseModel):
    success: bool
    message: str


# ===================== HELPERS =====================

def _row_to_cotizacion(row: dict) -> CotizacionResponse:
    return CotizacionResponse(
        cotizacion_id=row["cotizacion_id"],
        incidente_id=row["incidente_id"],
        taller_id=row["taller_id"],
        taller_nombre=row["taller_nombre"],
        calificacion_promedio=float(row.get("calificacion_promedio") or 0),
        costo_estimado=float(row["costo_estimado"]),
        tiempo_estimado=int(row["tiempo_estimado"]),
        observaciones=row.get("observaciones"),
        estado=row["estado"],
        fecha_creacion=str(row["fecha_creacion"]),
    )


# ===================== ENDPOINTS =====================

@router.post("/{taller_id}/registrar", response_model=RegistrarResponse, status_code=201)
async def registrar_cotizacion(
    taller_id: int,
    data: RegistrarCotizacionRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Taller registra una cotización para un incidente pendiente."""
    token_payload = get_token_payload(authorization)
    assert_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT incidente_id, estado, usuario_id FROM INCIDENTE WHERE incidente_id = %s",
            (data.incidente_id,),
        )
        incidente = cur.fetchone()
        if not incidente:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")
        if incidente["estado"] not in ("pendiente", "cotizacion"):
            raise HTTPException(
                status_code=400,
                detail=f"El incidente no está disponible para cotizar (estado: {incidente['estado']})",
            )

        if data.costo_estimado <= 0:
            raise HTTPException(status_code=400, detail="El costo estimado debe ser mayor a 0")
        if data.tiempo_estimado <= 0:
            raise HTTPException(status_code=400, detail="El tiempo estimado debe ser mayor a 0")

        cur.execute(
            "SELECT cotizacion_id FROM COTIZACION WHERE incidente_id = %s AND taller_id = %s",
            (data.incidente_id, taller_id),
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya enviaste una cotización para este incidente")

        cur.execute(
            """
            INSERT INTO COTIZACION (incidente_id, taller_id, costo_estimado, tiempo_estimado, observaciones)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING cotizacion_id
            """,
            (data.incidente_id, taller_id, data.costo_estimado, data.tiempo_estimado, data.observaciones),
        )
        cotizacion_id = cur.fetchone()["cotizacion_id"]

        # Marcar el incidente como 'cotizacion' para que el cliente sepa que hay propuestas
        cur.execute(
            """
            UPDATE INCIDENTE
            SET estado = 'cotizacion', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s AND estado = 'pendiente'
            """,
            (data.incidente_id,),
        )

        cur.execute("SELECT razon_social FROM TALLER WHERE taller_id = %s", (taller_id,))
        taller_row = cur.fetchone()
        razon_social = taller_row["razon_social"] if taller_row else "Un taller"

        cliente_uid = incidente["usuario_id"]
        crear_notificacion(
            db,
            cliente_uid,
            "nueva_cotizacion",
            "Nueva cotización recibida",
            f"{razon_social} envió una propuesta de Bs {data.costo_estimado:.2f} para tu emergencia.",
            {"incidente_id": data.incidente_id, "cotizacion_id": cotizacion_id, "taller_id": taller_id},
        )

        db.commit()

        asyncio.create_task(
            manager.send_to_user(
                cliente_uid,
                {
                    "tipo": "nueva_cotizacion",
                    "titulo": "Nueva cotización recibida",
                    "mensaje": f"{razon_social} envió una propuesta para tu emergencia.",
                    "incidente_id": data.incidente_id,
                    "cotizacion_id": cotizacion_id,
                },
            )
        )

        return RegistrarResponse(
            success=True,
            message="Cotización registrada correctamente",
            cotizacion_id=cotizacion_id,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error registrando cotización: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/mis-cotizaciones", response_model=List[CotizacionResponse])
async def listar_mis_cotizaciones(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todas las cotizaciones enviadas por el taller."""
    token_payload = get_token_payload(authorization)
    assert_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT
                c.cotizacion_id, c.incidente_id, c.taller_id,
                t.razon_social          AS taller_nombre,
                COALESCE(t.calificacion_promedio, 0) AS calificacion_promedio,
                c.costo_estimado, c.tiempo_estimado, c.observaciones,
                c.estado, c.fecha_creacion
            FROM COTIZACION c
            JOIN TALLER t ON c.taller_id = t.taller_id
            WHERE c.taller_id = %s
            ORDER BY c.fecha_creacion DESC
            """,
            (taller_id,),
        )
        return [_row_to_cotizacion(dict(r)) for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando cotizaciones: {str(e)}")
    finally:
        cur.close()


@router.get("/incidente/{incidente_id}", response_model=List[CotizacionResponse])
async def listar_cotizaciones_incidente(
    incidente_id: int,
    db=Depends(Database.get_db),
):
    """Lista todas las cotizaciones de un incidente para comparación del cliente."""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT
                c.cotizacion_id, c.incidente_id, c.taller_id,
                t.razon_social          AS taller_nombre,
                COALESCE(t.calificacion_promedio, 0) AS calificacion_promedio,
                c.costo_estimado, c.tiempo_estimado, c.observaciones,
                c.estado, c.fecha_creacion
            FROM COTIZACION c
            JOIN TALLER t ON c.taller_id = t.taller_id
            WHERE c.incidente_id = %s
            ORDER BY c.costo_estimado ASC
            """,
            (incidente_id,),
        )
        return [_row_to_cotizacion(dict(r)) for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando cotizaciones: {str(e)}")
    finally:
        cur.close()


@router.post("/seleccionar", response_model=SeleccionarResponse)
async def seleccionar_cotizacion(
    data: SeleccionarCotizacionRequest,
    db=Depends(Database.get_db),
):
    """
    Cliente selecciona una cotización.
    - Marca la cotización como 'aceptada'.
    - Marca el resto como 'no_seleccionada'.
    - Genera automáticamente la ASIGNACION al taller elegido.
    - Actualiza el incidente a 'asignada'.
    - Notifica al taller seleccionado.
    """
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT cotizacion_id, incidente_id, taller_id, estado,
                   tiempo_estimado, costo_estimado
            FROM COTIZACION
            WHERE cotizacion_id = %s
            """,
            (data.cotizacion_id,),
        )
        cotizacion = cur.fetchone()
        if not cotizacion:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")
        if cotizacion["estado"] != "pendiente":
            raise HTTPException(
                status_code=400,
                detail=f"La cotización ya no está disponible (estado: {cotizacion['estado']})",
            )

        incidente_id = cotizacion["incidente_id"]
        taller_id = cotizacion["taller_id"]

        cur.execute(
            "SELECT incidente_id, estado, usuario_id FROM INCIDENTE WHERE incidente_id = %s",
            (incidente_id,),
        )
        incidente = cur.fetchone()
        if not incidente:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")
        if str(incidente["usuario_id"]) != str(data.usuario_id):
            raise HTTPException(status_code=403, detail="No tienes permiso para seleccionar esta cotización")
        if incidente["estado"] not in ("pendiente", "cotizacion"):
            raise HTTPException(
                status_code=400,
                detail=f"El incidente ya fue procesado (estado: {incidente['estado']})",
            )

        cur.execute(
            "SELECT cotizacion_id FROM COTIZACION WHERE incidente_id = %s AND estado = 'aceptada'",
            (incidente_id,),
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya seleccionaste una cotización para este incidente")

        # Aceptar la cotización elegida
        cur.execute(
            """
            UPDATE COTIZACION
            SET estado = 'aceptada', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE cotizacion_id = %s
            """,
            (data.cotizacion_id,),
        )

        # Marcar las demás como no seleccionadas
        cur.execute(
            """
            UPDATE COTIZACION
            SET estado = 'no_seleccionada', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s AND cotizacion_id != %s AND estado = 'pendiente'
            """,
            (incidente_id, data.cotizacion_id),
        )

        # Crear ASIGNACION automáticamente (tiempo en minutos: horas * 60)
        cur.execute(
            """
            INSERT INTO ASIGNACION (
                incidente_id, taller_id, estado,
                tiempo_estimado_minutos, fecha_asignacion, fecha_aceptacion
            )
            VALUES (%s, %s, 'aceptada', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING asignacion_id
            """,
            (incidente_id, taller_id, cotizacion["tiempo_estimado"] * 60),
        )
        asignacion_id = cur.fetchone()["asignacion_id"]

        # Actualizar estado del incidente
        cur.execute(
            """
            UPDATE INCIDENTE
            SET estado = 'asignada', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s
            """,
            (incidente_id,),
        )

        # Notificar al taller seleccionado
        cur.execute(
            "SELECT usuario_id, razon_social FROM TALLER WHERE taller_id = %s",
            (taller_id,),
        )
        taller_row = cur.fetchone()
        taller_uid = taller_row["usuario_id"] if taller_row else None
        razon_social = taller_row["razon_social"] if taller_row else "Tu taller"

        if taller_uid:
            crear_notificacion(
                db,
                taller_uid,
                "cotizacion_aceptada",
                "¡Tu cotización fue seleccionada!",
                f"El cliente aceptó tu propuesta para la emergencia #{incidente_id}. Asignación #{asignacion_id} generada.",
                {
                    "incidente_id": incidente_id,
                    "cotizacion_id": data.cotizacion_id,
                    "asignacion_id": asignacion_id,
                },
            )

        crear_notificacion(
            db,
            data.usuario_id,
            "cotizacion_seleccionada",
            "Cotización aceptada",
            f"Seleccionaste la propuesta de {razon_social}. El taller está en camino.",
            {
                "incidente_id": incidente_id,
                "cotizacion_id": data.cotizacion_id,
                "asignacion_id": asignacion_id,
            },
        )

        db.commit()

        if taller_uid:
            asyncio.create_task(
                manager.send_to_user(
                    taller_uid,
                    {
                        "tipo": "cotizacion_aceptada",
                        "titulo": "¡Tu cotización fue seleccionada!",
                        "mensaje": f"El cliente aceptó tu propuesta. Asignación #{asignacion_id} generada.",
                        "incidente_id": incidente_id,
                        "asignacion_id": asignacion_id,
                    },
                )
            )

        asyncio.create_task(
            manager.send_to_user(
                data.usuario_id,
                {
                    "tipo": "cotizacion_seleccionada",
                    "titulo": "Cotización aceptada",
                    "mensaje": f"Seleccionaste la propuesta de {razon_social}.",
                    "incidente_id": incidente_id,
                    "asignacion_id": asignacion_id,
                },
            )
        )

        return SeleccionarResponse(
            success=True,
            message=f"Cotización aceptada. Asignación #{asignacion_id} creada automáticamente.",
            asignacion_id=asignacion_id,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error seleccionando cotización: {str(e)}")
    finally:
        cur.close()


@router.put("/{taller_id}/{cotizacion_id}", response_model=MessageResponse)
async def actualizar_cotizacion(
    taller_id: int,
    cotizacion_id: int,
    data: ActualizarCotizacionRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Actualiza una cotización pendiente del taller (costo, tiempo, observaciones)."""
    token_payload = get_token_payload(authorization)
    assert_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT cotizacion_id, estado FROM COTIZACION WHERE cotizacion_id = %s AND taller_id = %s",
            (cotizacion_id, taller_id),
        )
        cotizacion = cur.fetchone()
        if not cotizacion:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")
        if cotizacion["estado"] != "pendiente":
            raise HTTPException(
                status_code=400,
                detail="Solo se pueden editar cotizaciones en estado pendiente",
            )
        if data.costo_estimado <= 0:
            raise HTTPException(status_code=400, detail="El costo estimado debe ser mayor a 0")
        if data.tiempo_estimado <= 0:
            raise HTTPException(status_code=400, detail="El tiempo estimado debe ser mayor a 0")

        cur.execute(
            """
            UPDATE COTIZACION
            SET costo_estimado = %s, tiempo_estimado = %s, observaciones = %s,
                fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE cotizacion_id = %s
            """,
            (data.costo_estimado, data.tiempo_estimado, data.observaciones, cotizacion_id),
        )
        db.commit()
        return MessageResponse(success=True, message="Cotización actualizada correctamente")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error actualizando cotización: {str(e)}")
    finally:
        cur.close()


@router.delete("/{taller_id}/{cotizacion_id}", response_model=MessageResponse)
async def eliminar_cotizacion(
    taller_id: int,
    cotizacion_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Elimina una cotización pendiente del taller (antes de que sea seleccionada)."""
    token_payload = get_token_payload(authorization)
    assert_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT cotizacion_id, estado FROM COTIZACION WHERE cotizacion_id = %s AND taller_id = %s",
            (cotizacion_id, taller_id),
        )
        cotizacion = cur.fetchone()
        if not cotizacion:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")
        if cotizacion["estado"] != "pendiente":
            raise HTTPException(
                status_code=400,
                detail="Solo se pueden eliminar cotizaciones en estado pendiente",
            )

        cur.execute("DELETE FROM COTIZACION WHERE cotizacion_id = %s", (cotizacion_id,))
        db.commit()
        return MessageResponse(success=True, message="Cotización eliminada correctamente")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error eliminando cotización: {str(e)}")
    finally:
        cur.close()
