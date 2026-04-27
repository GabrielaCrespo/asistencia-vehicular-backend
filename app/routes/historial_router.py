from fastapi import APIRouter, HTTPException, Depends, Header, Query
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import jwt
from typing import List, Optional

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/historial", tags=["Historial del Taller"])


# ===================== MODELOS RESPONSE =====================

class ResumenHistorial(BaseModel):
    total_solicitudes: int
    solicitudes_completadas: int
    solicitudes_pendientes: int
    solicitudes_en_curso: int
    solicitudes_rechazadas: int
    total_ingresos: float
    calificacion_promedio: float
    total_calificaciones: int
    tecnico_mas_activo: Optional[str]
    servicio_mas_solicitado: Optional[str]


class SolicitudHistorialResponse(BaseModel):
    asignacion_id: int
    incidente_id: int
    estado: str
    tipo_problema: Optional[str]
    prioridad: str
    descripcion: Optional[str]
    cliente_nombre: str
    cliente_telefono: Optional[str]
    tecnico_nombre: Optional[str]
    vehiculo_marca: Optional[str]
    vehiculo_modelo: Optional[str]
    vehiculo_placa: Optional[str]
    fecha_solicitud: str
    fecha_aceptacion: Optional[str]
    fecha_inicio_servicio: Optional[str]
    fecha_cierre_servicio: Optional[str]
    duracion_minutos: Optional[int]
    monto_cobrado: Optional[float]
    calificacion: Optional[int]
    observaciones: Optional[str]


class ServicioRealizadoResponse(BaseModel):
    asignacion_id: int
    incidente_id: int
    fecha_servicio: str
    cliente_nombre: str
    vehiculo_marca: Optional[str]
    vehiculo_modelo: Optional[str]
    vehiculo_placa: Optional[str]
    tecnico_nombre: Optional[str]
    servicios_realizados: Optional[str]
    monto_total: Optional[float]
    monto_taller: Optional[float]
    calificacion: Optional[int]
    puntuacion_atencion: Optional[int]
    puntuacion_puntualidad: Optional[int]
    puntuacion_limpieza: Optional[int]


class TransaccionResponse(BaseModel):
    pago_id: int
    incidente_id: int
    asignacion_id: Optional[int]
    cliente_nombre: str
    tipo_problema: Optional[str]
    fecha_pago: Optional[str]
    monto_total: float
    monto_servicio: float
    monto_taller: float
    comision_plataforma: float
    metodo_pago: Optional[str]
    estado: str
    estado_comision: str
    creado_en: str


class DetalleSolicitudResponse(BaseModel):
    asignacion_id: int
    incidente_id: int
    estado: str
    tipo_problema: Optional[str]
    prioridad: str
    descripcion: Optional[str]
    observaciones: Optional[str]
    cliente_nombre: str
    cliente_telefono: Optional[str]
    cliente_email: Optional[str]
    vehiculo_marca: Optional[str]
    vehiculo_modelo: Optional[str]
    vehiculo_placa: Optional[str]
    vehiculo_anio: Optional[int]
    vehiculo_color: Optional[str]
    tecnico_nombre: Optional[str]
    tecnico_especialidad: Optional[str]
    fecha_solicitud: str
    fecha_aceptacion: Optional[str]
    fecha_inicio_servicio: Optional[str]
    fecha_cierre_servicio: Optional[str]
    tiempo_estimado_minutos: Optional[int]
    duracion_real_minutos: Optional[int]
    latitud: float
    longitud: float
    imagen_path: Optional[str]
    audio_path: Optional[str]
    servicios_realizados: Optional[str]
    ia_clasificacion: Optional[str]
    ia_resumen: Optional[str]
    ia_recomendaciones: Optional[str]
    monto_total: Optional[float]
    monto_taller: Optional[float]
    comision_plataforma: Optional[float]
    metodo_pago: Optional[str]
    estado_pago: Optional[str]
    calificacion: Optional[int]
    comentario_calificacion: Optional[str]


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


# ===================== ENDPOINTS =====================

@router.get("/{taller_id}/resumen", response_model=ResumenHistorial)
async def resumen_historial(
    taller_id: int,
    fecha_desde: Optional[str] = Query(None, description="Filtrar desde fecha (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Filtrar hasta fecha (YYYY-MM-DD)"),
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Dashboard general del taller: totales de solicitudes, ingresos y calificaciones."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        fecha_filtro = ""
        params_base = [taller_id]
        if fecha_desde:
            fecha_filtro += " AND a.fecha_asignacion >= %s"
            params_base.append(fecha_desde)
        if fecha_hasta:
            fecha_filtro += " AND a.fecha_asignacion <= %s::date + INTERVAL '1 day'"
            params_base.append(fecha_hasta)

        cur.execute(f"""
            SELECT
                COUNT(*)                                                                         AS total_solicitudes,
                COUNT(*) FILTER (WHERE a.estado = 'completada')                                  AS solicitudes_completadas,
                COUNT(*) FILTER (WHERE a.estado = 'pendiente')                                   AS solicitudes_pendientes,
                COUNT(*) FILTER (WHERE a.estado IN ('aceptada','en_camino','en_servicio'))        AS solicitudes_en_curso,
                COUNT(*) FILTER (WHERE a.estado = 'rechazada')                                   AS solicitudes_rechazadas
            FROM ASIGNACION a
            WHERE a.taller_id = %s {fecha_filtro}
        """, params_base)
        conteos = cur.fetchone()

        cur.execute(f"""
            SELECT
                COALESCE(SUM(p.monto_taller), 0)     AS total_ingresos,
                COALESCE(AVG(c.puntuacion), 0)        AS calificacion_promedio,
                COUNT(c.calificacion_id)              AS total_calificaciones
            FROM ASIGNACION a
            LEFT JOIN PAGO p        ON p.asignacion_id = a.asignacion_id AND p.estado = 'completado'
            LEFT JOIN CALIFICACION c ON c.incidente_id = a.incidente_id
            WHERE a.taller_id = %s {fecha_filtro}
        """, params_base)
        financiero = cur.fetchone()

        cur.execute(f"""
            SELECT t.nombre AS tecnico_nombre, COUNT(a.asignacion_id) AS total
            FROM ASIGNACION a
            JOIN TECNICO t ON t.tecnico_id = a.tecnico_id
            WHERE a.taller_id = %s AND a.estado = 'completada' {fecha_filtro}
            GROUP BY t.nombre
            ORDER BY total DESC
            LIMIT 1
        """, params_base)
        top_tecnico = cur.fetchone()

        cur.execute(f"""
            SELECT s.nombre AS servicio_nombre, COUNT(ins.incidente_servicio_id) AS total
            FROM ASIGNACION a
            JOIN INCIDENTE_SERVICIO ins ON ins.incidente_id = a.incidente_id
            JOIN SERVICIO s             ON s.servicio_id   = ins.servicio_id
            WHERE a.taller_id = %s AND a.estado = 'completada' {fecha_filtro}
            GROUP BY s.nombre
            ORDER BY total DESC
            LIMIT 1
        """, params_base)
        top_servicio = cur.fetchone()

        return ResumenHistorial(
            total_solicitudes=int(conteos["total_solicitudes"]),
            solicitudes_completadas=int(conteos["solicitudes_completadas"]),
            solicitudes_pendientes=int(conteos["solicitudes_pendientes"]),
            solicitudes_en_curso=int(conteos["solicitudes_en_curso"]),
            solicitudes_rechazadas=int(conteos["solicitudes_rechazadas"]),
            total_ingresos=float(financiero["total_ingresos"]),
            calificacion_promedio=round(float(financiero["calificacion_promedio"]), 2),
            total_calificaciones=int(financiero["total_calificaciones"]),
            tecnico_mas_activo=top_tecnico["tecnico_nombre"] if top_tecnico else None,
            servicio_mas_solicitado=top_servicio["servicio_nombre"] if top_servicio else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo resumen: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/solicitudes", response_model=List[SolicitudHistorialResponse])
async def historial_solicitudes(
    taller_id: int,
    fecha_desde: Optional[str] = Query(None, description="Filtrar desde fecha (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Filtrar hasta fecha (YYYY-MM-DD)"),
    estado: Optional[str] = Query(None, description="pendiente|aceptada|en_camino|en_servicio|completada|rechazada"),
    tipo_problema: Optional[str] = Query(None, description="Tipo de problema del incidente"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Historial completo de todas las solicitudes recibidas por el taller."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        filtros = ""
        params = [taller_id]
        if fecha_desde:
            filtros += " AND a.fecha_asignacion >= %s"
            params.append(fecha_desde)
        if fecha_hasta:
            filtros += " AND a.fecha_asignacion <= %s::date + INTERVAL '1 day'"
            params.append(fecha_hasta)
        if estado:
            filtros += " AND a.estado = %s"
            params.append(estado)
        if tipo_problema:
            filtros += " AND i.tipo_problema ILIKE %s"
            params.append(f"%{tipo_problema}%")

        params += [limit, offset]
        cur.execute(f"""
            SELECT
                a.asignacion_id,
                a.incidente_id,
                a.estado,
                a.observaciones,
                a.tiempo_estimado_minutos,
                a.fecha_asignacion,
                a.fecha_aceptacion,
                a.fecha_inicio_servicio,
                a.fecha_cierre_servicio,
                CASE
                    WHEN a.fecha_cierre_servicio IS NOT NULL AND a.fecha_inicio_servicio IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_cierre_servicio - a.fecha_inicio_servicio))::int / 60
                    ELSE NULL
                END                                         AS duracion_minutos,
                i.tipo_problema,
                i.prioridad,
                i.descripcion,
                u.nombre                                    AS cliente_nombre,
                u.telefono                                  AS cliente_telefono,
                t.nombre                                    AS tecnico_nombre,
                v.marca                                     AS vehiculo_marca,
                v.modelo                                    AS vehiculo_modelo,
                v.placa                                     AS vehiculo_placa,
                p.monto_taller                              AS monto_cobrado,
                c.puntuacion                                AS calificacion
            FROM ASIGNACION a
            JOIN INCIDENTE i         ON i.incidente_id  = a.incidente_id
            JOIN USUARIO u           ON u.usuario_id    = i.usuario_id
            LEFT JOIN TECNICO t      ON t.tecnico_id    = a.tecnico_id
            LEFT JOIN VEHICULO v     ON v.vehiculo_id   = i.vehiculo_id
            LEFT JOIN PAGO p         ON p.asignacion_id = a.asignacion_id AND p.estado = 'completado'
            LEFT JOIN CALIFICACION c ON c.incidente_id  = a.incidente_id
            WHERE a.taller_id = %s {filtros}
            ORDER BY a.fecha_asignacion DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall()

        return [
            SolicitudHistorialResponse(
                asignacion_id=r["asignacion_id"],
                incidente_id=r["incidente_id"],
                estado=r["estado"],
                tipo_problema=r.get("tipo_problema"),
                prioridad=r["prioridad"],
                descripcion=r.get("descripcion"),
                cliente_nombre=r["cliente_nombre"],
                cliente_telefono=r.get("cliente_telefono"),
                tecnico_nombre=r.get("tecnico_nombre"),
                vehiculo_marca=r.get("vehiculo_marca"),
                vehiculo_modelo=r.get("vehiculo_modelo"),
                vehiculo_placa=r.get("vehiculo_placa"),
                fecha_solicitud=str(r["fecha_asignacion"]),
                fecha_aceptacion=str(r["fecha_aceptacion"]) if r.get("fecha_aceptacion") else None,
                fecha_inicio_servicio=str(r["fecha_inicio_servicio"]) if r.get("fecha_inicio_servicio") else None,
                fecha_cierre_servicio=str(r["fecha_cierre_servicio"]) if r.get("fecha_cierre_servicio") else None,
                duracion_minutos=r.get("duracion_minutos"),
                monto_cobrado=float(r["monto_cobrado"]) if r.get("monto_cobrado") else None,
                calificacion=r.get("calificacion"),
                observaciones=r.get("observaciones"),
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo historial de solicitudes: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/servicios", response_model=List[ServicioRealizadoResponse])
async def historial_servicios(
    taller_id: int,
    fecha_desde: Optional[str] = Query(None, description="Filtrar desde fecha (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Filtrar hasta fecha (YYYY-MM-DD)"),
    tecnico_id: Optional[int] = Query(None, description="Filtrar por técnico"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Historial de servicios realizados (asignaciones completadas) con detalle de calificaciones."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        filtros = ""
        params = [taller_id]
        if fecha_desde:
            filtros += " AND a.fecha_cierre_servicio >= %s"
            params.append(fecha_desde)
        if fecha_hasta:
            filtros += " AND a.fecha_cierre_servicio <= %s::date + INTERVAL '1 day'"
            params.append(fecha_hasta)
        if tecnico_id:
            filtros += " AND a.tecnico_id = %s"
            params.append(tecnico_id)

        params += [limit, offset]
        cur.execute(f"""
            SELECT
                a.asignacion_id,
                a.incidente_id,
                COALESCE(a.fecha_cierre_servicio, a.fecha_asignacion) AS fecha_servicio,
                u.nombre                                    AS cliente_nombre,
                v.marca                                     AS vehiculo_marca,
                v.modelo                                    AS vehiculo_modelo,
                v.placa                                     AS vehiculo_placa,
                t.nombre                                    AS tecnico_nombre,
                STRING_AGG(DISTINCT s.nombre, ', ')         AS servicios_realizados,
                p.monto_total,
                p.monto_taller,
                c.puntuacion                                AS calificacion,
                c.aspecto_atencion                          AS puntuacion_atencion,
                c.aspecto_puntualidad                       AS puntuacion_puntualidad,
                c.aspecto_limpieza                          AS puntuacion_limpieza
            FROM ASIGNACION a
            JOIN INCIDENTE i              ON i.incidente_id  = a.incidente_id
            JOIN USUARIO u                ON u.usuario_id    = i.usuario_id
            LEFT JOIN TECNICO t           ON t.tecnico_id    = a.tecnico_id
            LEFT JOIN VEHICULO v          ON v.vehiculo_id   = i.vehiculo_id
            LEFT JOIN INCIDENTE_SERVICIO ins ON ins.incidente_id = a.incidente_id
            LEFT JOIN SERVICIO s          ON s.servicio_id   = ins.servicio_id
            LEFT JOIN PAGO p              ON p.asignacion_id = a.asignacion_id AND p.estado = 'completado'
            LEFT JOIN CALIFICACION c      ON c.incidente_id  = a.incidente_id
            WHERE a.taller_id = %s AND a.estado = 'completada' {filtros}
            GROUP BY
                a.asignacion_id, a.incidente_id, a.fecha_cierre_servicio, a.fecha_asignacion,
                u.nombre, v.marca, v.modelo, v.placa, t.nombre,
                p.monto_total, p.monto_taller,
                c.puntuacion, c.aspecto_atencion, c.aspecto_puntualidad, c.aspecto_limpieza
            ORDER BY fecha_servicio DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall()

        return [
            ServicioRealizadoResponse(
                asignacion_id=r["asignacion_id"],
                incidente_id=r["incidente_id"],
                fecha_servicio=str(r["fecha_servicio"]),
                cliente_nombre=r["cliente_nombre"],
                vehiculo_marca=r.get("vehiculo_marca"),
                vehiculo_modelo=r.get("vehiculo_modelo"),
                vehiculo_placa=r.get("vehiculo_placa"),
                tecnico_nombre=r.get("tecnico_nombre"),
                servicios_realizados=r.get("servicios_realizados"),
                monto_total=float(r["monto_total"]) if r.get("monto_total") else None,
                monto_taller=float(r["monto_taller"]) if r.get("monto_taller") else None,
                calificacion=r.get("calificacion"),
                puntuacion_atencion=r.get("puntuacion_atencion"),
                puntuacion_puntualidad=r.get("puntuacion_puntualidad"),
                puntuacion_limpieza=r.get("puntuacion_limpieza"),
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo historial de servicios: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/transacciones", response_model=List[TransaccionResponse])
async def historial_transacciones(
    taller_id: int,
    fecha_desde: Optional[str] = Query(None, description="Filtrar desde fecha (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Filtrar hasta fecha (YYYY-MM-DD)"),
    estado_comision: Optional[str] = Query(None, description="pendiente|pagado"),
    metodo_pago: Optional[str] = Query(None, description="efectivo|tarjeta|transferencia"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Historial de todas las transacciones económicas del taller con filtros."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        filtros = ""
        params = [taller_id]
        if fecha_desde:
            filtros += " AND p.fecha_pago >= %s"
            params.append(fecha_desde)
        if fecha_hasta:
            filtros += " AND p.fecha_pago <= %s::date + INTERVAL '1 day'"
            params.append(fecha_hasta)
        if estado_comision:
            filtros += " AND p.estado_comision = %s"
            params.append(estado_comision)
        if metodo_pago:
            filtros += " AND p.metodo_pago = %s"
            params.append(metodo_pago)

        params += [limit, offset]
        cur.execute(f"""
            SELECT
                p.pago_id,
                p.incidente_id,
                p.asignacion_id,
                p.monto_total,
                p.monto_servicio,
                p.monto_taller,
                p.comision_plataforma,
                p.metodo_pago,
                p.estado,
                p.estado_comision,
                p.fecha_pago,
                p.creado_en,
                u.nombre       AS cliente_nombre,
                i.tipo_problema
            FROM PAGO p
            JOIN ASIGNACION a   ON a.asignacion_id = p.asignacion_id
            JOIN INCIDENTE i    ON i.incidente_id  = p.incidente_id
            JOIN USUARIO u      ON u.usuario_id    = i.usuario_id
            WHERE a.taller_id = %s {filtros}
            ORDER BY p.creado_en DESC
            LIMIT %s OFFSET %s
        """, params)
        rows = cur.fetchall()

        return [
            TransaccionResponse(
                pago_id=r["pago_id"],
                incidente_id=r["incidente_id"],
                asignacion_id=r.get("asignacion_id"),
                cliente_nombre=r["cliente_nombre"],
                tipo_problema=r.get("tipo_problema"),
                fecha_pago=str(r["fecha_pago"]) if r.get("fecha_pago") else None,
                monto_total=float(r["monto_total"]),
                monto_servicio=float(r["monto_servicio"]),
                monto_taller=float(r["monto_taller"]),
                comision_plataforma=float(r["comision_plataforma"]),
                metodo_pago=r.get("metodo_pago"),
                estado=r["estado"],
                estado_comision=r["estado_comision"],
                creado_en=str(r["creado_en"]),
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo transacciones: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/solicitud/{incidente_id}", response_model=DetalleSolicitudResponse)
async def detalle_solicitud(
    taller_id: int,
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Detalle completo de una solicitud: incidente, vehiculo, servicios, pago, IA y calificación."""
    _verify_taller(_get_token(authorization), taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                a.asignacion_id,
                a.incidente_id,
                a.estado,
                a.observaciones,
                a.tiempo_estimado_minutos,
                a.fecha_asignacion,
                a.fecha_aceptacion,
                a.fecha_inicio_servicio,
                a.fecha_cierre_servicio,
                CASE
                    WHEN a.fecha_cierre_servicio IS NOT NULL AND a.fecha_inicio_servicio IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_cierre_servicio - a.fecha_inicio_servicio))::int / 60
                    ELSE NULL
                END                                     AS duracion_real_minutos,
                i.tipo_problema,
                i.prioridad,
                i.descripcion,
                i.latitud,
                i.longitud,
                i.imagen_path,
                i.audio_path,
                u.nombre                                AS cliente_nombre,
                u.telefono                              AS cliente_telefono,
                u.email                                 AS cliente_email,
                v.marca                                 AS vehiculo_marca,
                v.modelo                                AS vehiculo_modelo,
                v.placa                                 AS vehiculo_placa,
                v.anio                                  AS vehiculo_anio,
                v.color                                 AS vehiculo_color,
                t.nombre                                AS tecnico_nombre,
                t.especialidad                          AS tecnico_especialidad,
                STRING_AGG(DISTINCT s.nombre, ', ')     AS servicios_realizados,
                ia.clasificacion                        AS ia_clasificacion,
                ia.resumen_automatico                   AS ia_resumen,
                ia.recomendaciones                      AS ia_recomendaciones,
                p.monto_total,
                p.monto_taller,
                p.comision_plataforma,
                p.metodo_pago,
                p.estado                                AS estado_pago,
                c.puntuacion                            AS calificacion,
                c.comentario                            AS comentario_calificacion
            FROM ASIGNACION a
            JOIN INCIDENTE i              ON i.incidente_id  = a.incidente_id
            JOIN USUARIO u                ON u.usuario_id    = i.usuario_id
            LEFT JOIN TECNICO t           ON t.tecnico_id    = a.tecnico_id
            LEFT JOIN VEHICULO v          ON v.vehiculo_id   = i.vehiculo_id
            LEFT JOIN INCIDENTE_SERVICIO ins ON ins.incidente_id = a.incidente_id
            LEFT JOIN SERVICIO s          ON s.servicio_id   = ins.servicio_id
            LEFT JOIN IA_ANALISIS ia      ON ia.incidente_id = a.incidente_id
            LEFT JOIN PAGO p              ON p.asignacion_id = a.asignacion_id
            LEFT JOIN CALIFICACION c      ON c.incidente_id  = a.incidente_id
            WHERE a.taller_id = %s AND a.incidente_id = %s
            GROUP BY
                a.asignacion_id, a.incidente_id, a.estado, a.observaciones,
                a.tiempo_estimado_minutos, a.fecha_asignacion, a.fecha_aceptacion,
                a.fecha_inicio_servicio, a.fecha_cierre_servicio,
                i.tipo_problema, i.prioridad, i.descripcion, i.latitud, i.longitud,
                i.imagen_path, i.audio_path,
                u.nombre, u.telefono, u.email,
                v.marca, v.modelo, v.placa, v.anio, v.color,
                t.nombre, t.especialidad,
                ia.clasificacion, ia.resumen_automatico, ia.recomendaciones,
                p.monto_total, p.monto_taller, p.comision_plataforma, p.metodo_pago, p.estado,
                c.puntuacion, c.comentario
        """, (taller_id, incidente_id))

        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Solicitud no encontrada para este taller")

        return DetalleSolicitudResponse(
            asignacion_id=row["asignacion_id"],
            incidente_id=row["incidente_id"],
            estado=row["estado"],
            tipo_problema=row.get("tipo_problema"),
            prioridad=row["prioridad"],
            descripcion=row.get("descripcion"),
            observaciones=row.get("observaciones"),
            cliente_nombre=row["cliente_nombre"],
            cliente_telefono=row.get("cliente_telefono"),
            cliente_email=row.get("cliente_email"),
            vehiculo_marca=row.get("vehiculo_marca"),
            vehiculo_modelo=row.get("vehiculo_modelo"),
            vehiculo_placa=row.get("vehiculo_placa"),
            vehiculo_anio=row.get("vehiculo_anio"),
            vehiculo_color=row.get("vehiculo_color"),
            tecnico_nombre=row.get("tecnico_nombre"),
            tecnico_especialidad=row.get("tecnico_especialidad"),
            fecha_solicitud=str(row["fecha_asignacion"]),
            fecha_aceptacion=str(row["fecha_aceptacion"]) if row.get("fecha_aceptacion") else None,
            fecha_inicio_servicio=str(row["fecha_inicio_servicio"]) if row.get("fecha_inicio_servicio") else None,
            fecha_cierre_servicio=str(row["fecha_cierre_servicio"]) if row.get("fecha_cierre_servicio") else None,
            tiempo_estimado_minutos=row.get("tiempo_estimado_minutos"),
            duracion_real_minutos=row.get("duracion_real_minutos"),
            latitud=float(row["latitud"]),
            longitud=float(row["longitud"]),
            imagen_path=row.get("imagen_path"),
            audio_path=row.get("audio_path"),
            servicios_realizados=row.get("servicios_realizados"),
            ia_clasificacion=row.get("ia_clasificacion"),
            ia_resumen=row.get("ia_resumen"),
            ia_recomendaciones=row.get("ia_recomendaciones"),
            monto_total=float(row["monto_total"]) if row.get("monto_total") else None,
            monto_taller=float(row["monto_taller"]) if row.get("monto_taller") else None,
            comision_plataforma=float(row["comision_plataforma"]) if row.get("comision_plataforma") else None,
            metodo_pago=row.get("metodo_pago"),
            estado_pago=row.get("estado_pago"),
            calificacion=row.get("calificacion"),
            comentario_calificacion=row.get("comentario_calificacion"),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo detalle de solicitud: {str(e)}")
    finally:
        cur.close()
