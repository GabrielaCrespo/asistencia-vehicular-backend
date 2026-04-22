from fastapi import APIRouter, HTTPException, status, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import jwt
from typing import List, Optional

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/asignacion", tags=["Asignaciones"])


# ===================== MODELOS REQUEST =====================

class AceptarSolicitudRequest(BaseModel):
    incidente_id: int
    tecnico_id: Optional[int] = None
    tiempo_estimado_minutos: Optional[int] = None


class RechazarSolicitudRequest(BaseModel):
    incidente_id: int
    observaciones: Optional[str] = None


# ===================== MODELOS RESPONSE =====================

class SolicitudDisponibleResponse(BaseModel):
    incidente_id: int
    descripcion: str
    latitud: float
    longitud: float
    estado: str
    prioridad: str
    fecha_creacion: str
    imagen_path: Optional[str]
    audio_path: Optional[str]
    cliente_nombre: str
    cliente_telefono: str
    marca: str
    modelo: str
    placa: str
    vehiculo_tipo: Optional[str]


class SolicitudAsignadaResponse(BaseModel):
    asignacion_id: int
    incidente_id: int
    tecnico_id: Optional[int]
    tecnico_nombre: Optional[str]
    taller_id: int
    estado: str
    tiempo_estimado_minutos: Optional[int]
    fecha_asignacion: str
    observaciones: Optional[str]
    descripcion: str
    latitud: float
    longitud: float
    imagen_path: Optional[str]
    audio_path: Optional[str]
    prioridad: str
    cliente_nombre: str
    cliente_telefono: str
    marca: str
    modelo: str
    placa: str


class AsignarTecnicoRequest(BaseModel):
    tecnico_id: int


class ActualizarEstadoRequest(BaseModel):
    estado: str  # en_camino | en_servicio | completada


class DiagnosticoRequest(BaseModel):
    observaciones: str
    costo: float
    metodo_pago: Optional[str] = None


class IaAnalisisResponse(BaseModel):
    tipo_entrada: Optional[str]
    transcripcion_audio: Optional[str]
    clasificacion: Optional[str]
    nivel_confianza: Optional[float]
    resultado_imagen: Optional[str]
    resumen_automatico: Optional[str]
    recomendaciones: Optional[str]
    fecha_analisis: Optional[str]


class DetalleIncidenteResponse(BaseModel):
    incidente_id: int
    descripcion: str
    tipo_problema: Optional[str]
    latitud: float
    longitud: float
    estado: str
    prioridad: str
    fecha_creacion: str
    imagen_path: Optional[str]
    audio_path: Optional[str]
    cliente_nombre: str
    cliente_telefono: str
    cliente_email: str
    marca: str
    modelo: str
    placa: str
    vehiculo_tipo: Optional[str]
    anio: Optional[int]
    ia_analisis: Optional[IaAnalisisResponse]


class AsignacionResponse(BaseModel):
    success: bool
    message: str
    asignacion_id: int


class MessageResponse(BaseModel):
    success: bool
    message: str


# ===================== FUNCIONES AUXILIARES =====================

def get_token_from_header(authorization: str = Header(None)) -> dict:
    if not authorization:
        raise HTTPException(status_code=401, detail="Token no proporcionado")
    try:
        token = authorization.split(" ")[1]
    except IndexError:
        raise HTTPException(status_code=401, detail="Formato de token inválido")
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


def verify_taller_access(token_payload: dict, taller_id: int, db) -> bool:
    usuario_id = int(token_payload.get("sub"))
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT usuario_id FROM TALLER WHERE taller_id = %s", (taller_id,))
        taller = cur.fetchone()
        cur.close()
        if not taller or taller['usuario_id'] != usuario_id:
            raise HTTPException(status_code=403, detail="No tienes permiso para acceder a este taller")
        return True
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"Error verificando acceso: {str(e)}")


def _row_to_disponible(row: dict) -> SolicitudDisponibleResponse:
    return SolicitudDisponibleResponse(
        incidente_id=row['incidente_id'],
        descripcion=row['descripcion'],
        latitud=float(row['latitud']),
        longitud=float(row['longitud']),
        estado=row['estado'],
        prioridad=row['prioridad'],
        fecha_creacion=str(row['fecha_creacion']),
        imagen_path=row['imagen_path'],
        audio_path=row['audio_path'],
        cliente_nombre=row['cliente_nombre'],
        cliente_telefono=row['cliente_telefono'],
        marca=row['marca'],
        modelo=row['modelo'],
        placa=row['placa'],
        vehiculo_tipo=row.get('vehiculo_tipo'),
    )


def _row_to_asignada(row: dict) -> SolicitudAsignadaResponse:
    return SolicitudAsignadaResponse(
        asignacion_id=row['asignacion_id'],
        incidente_id=row['incidente_id'],
        tecnico_id=row.get('tecnico_id'),
        tecnico_nombre=row.get('tecnico_nombre'),
        taller_id=row['taller_id'],
        estado=row['estado'],
        tiempo_estimado_minutos=row.get('tiempo_estimado_minutos'),
        fecha_asignacion=str(row['fecha_asignacion']),
        observaciones=row.get('observaciones'),
        descripcion=row['descripcion'],
        latitud=float(row['latitud']),
        longitud=float(row['longitud']),
        imagen_path=row.get('imagen_path'),
        audio_path=row.get('audio_path'),
        prioridad=row['prioridad'],
        cliente_nombre=row['cliente_nombre'],
        cliente_telefono=row['cliente_telefono'],
        marca=row['marca'],
        modelo=row['modelo'],
        placa=row['placa'],
    )


# ===================== ENDPOINTS =====================

@router.get("/solicitudes/disponibles", response_model=List[SolicitudDisponibleResponse])
async def listar_solicitudes_disponibles(
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Lista todos los incidentes con estado='pendiente'.
    Cualquier taller autenticado puede verlos.
    """
    token_payload = get_token_from_header(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                i.incidente_id, i.descripcion, i.latitud, i.longitud,
                i.estado, i.prioridad, i.fecha_creacion,
                i.imagen_path, i.audio_path,
                u.nombre  AS cliente_nombre,
                u.telefono AS cliente_telefono,
                v.marca, v.modelo, v.placa,
                v.tipo    AS vehiculo_tipo
            FROM INCIDENTE i
            JOIN USUARIO  u ON i.usuario_id  = u.usuario_id
            JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            WHERE i.estado = 'pendiente'
            ORDER BY
                CASE WHEN i.prioridad = 'urgente' THEN 0 ELSE 1 END,
                i.fecha_creacion ASC
        """)
        rows = cur.fetchall()
        return [_row_to_disponible(dict(r)) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando solicitudes: {str(e)}")
    finally:
        cur.close()


@router.get("/incidente/{incidente_id}/detalle", response_model=DetalleIncidenteResponse)
async def detalle_incidente(
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Detalle completo de un incidente (cliente, vehículo, evidencias)."""
    get_token_from_header(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Datos del incidente + cliente + vehículo
        # Usamos LEFT JOIN en VEHICULO por si vehiculo_id es NULL
        cur.execute("""
            SELECT
                i.incidente_id, i.descripcion,
                i.latitud, i.longitud,
                i.estado, i.prioridad, i.fecha_creacion,
                i.imagen_path, i.audio_path,
                u.nombre   AS cliente_nombre,
                u.telefono AS cliente_telefono,
                u.email    AS cliente_email,
                v.marca, v.modelo, v.placa,
                v.tipo     AS vehiculo_tipo,
                v.anio
            FROM INCIDENTE i
            JOIN USUARIO  u ON i.usuario_id  = u.usuario_id
            LEFT JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            WHERE i.incidente_id = %s
        """, (incidente_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")

        r = dict(row)

        # tipo_problema: intentar leerlo si la columna ya existe en la BD
        tipo_problema = None
        try:
            cur.execute(
                "SELECT tipo_problema FROM INCIDENTE WHERE incidente_id = %s",
                (incidente_id,)
            )
            tp_row = cur.fetchone()
            if tp_row:
                tipo_problema = tp_row.get('tipo_problema')
        except Exception:
            db.rollback()  # limpiar estado de error en la conexión

        # Análisis IA más reciente para este incidente
        ia_row = None
        try:
            cur.execute("""
                SELECT
                    tipo_entrada, transcripcion_audio, clasificacion,
                    nivel_confianza, resultado_imagen,
                    resumen_automatico, recomendaciones, fecha_analisis
                FROM IA_ANALISIS
                WHERE incidente_id = %s
                ORDER BY fecha_analisis DESC
                LIMIT 1
            """, (incidente_id,))
            ia_row = cur.fetchone()
        except Exception:
            db.rollback()

        ia = dict(ia_row) if ia_row else None

        return DetalleIncidenteResponse(
            incidente_id=r['incidente_id'],
            descripcion=r['descripcion'],
            tipo_problema=tipo_problema,
            latitud=float(r['latitud']),
            longitud=float(r['longitud']),
            estado=r['estado'],
            prioridad=r['prioridad'],
            fecha_creacion=str(r['fecha_creacion']),
            imagen_path=r.get('imagen_path'),
            audio_path=r.get('audio_path'),
            cliente_nombre=r['cliente_nombre'],
            cliente_telefono=r['cliente_telefono'],
            cliente_email=r['cliente_email'],
            marca=r.get('marca', ''),
            modelo=r.get('modelo', ''),
            placa=r.get('placa', ''),
            vehiculo_tipo=r.get('vehiculo_tipo'),
            anio=r.get('anio'),
            ia_analisis=IaAnalisisResponse(
                tipo_entrada=ia.get('tipo_entrada'),
                transcripcion_audio=ia.get('transcripcion_audio'),
                clasificacion=ia.get('clasificacion'),
                nivel_confianza=float(ia['nivel_confianza']) if ia.get('nivel_confianza') is not None else None,
                resultado_imagen=ia.get('resultado_imagen'),
                resumen_automatico=ia.get('resumen_automatico'),
                recomendaciones=ia.get('recomendaciones'),
                fecha_analisis=str(ia['fecha_analisis']) if ia.get('fecha_analisis') else None,
            ) if ia else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo detalle: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/asignadas", response_model=List[SolicitudAsignadaResponse])
async def listar_asignadas(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Asignaciones activas (aceptada / en_camino / en_servicio) del taller."""
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                a.asignacion_id, a.incidente_id, a.tecnico_id, a.taller_id,
                a.estado, a.tiempo_estimado_minutos,
                a.fecha_asignacion, a.observaciones,
                i.descripcion, i.latitud, i.longitud,
                i.imagen_path, i.audio_path, i.prioridad,
                u.nombre   AS cliente_nombre,
                u.telefono AS cliente_telefono,
                v.marca, v.modelo, v.placa,
                t.nombre   AS tecnico_nombre
            FROM ASIGNACION a
            JOIN INCIDENTE i ON a.incidente_id = i.incidente_id
            JOIN USUARIO   u ON i.usuario_id   = u.usuario_id
            JOIN VEHICULO  v ON i.vehiculo_id  = v.vehiculo_id
            LEFT JOIN TECNICO t ON a.tecnico_id = t.tecnico_id
            WHERE a.taller_id = %s
              AND a.estado IN ('aceptada', 'en_camino', 'en_servicio')
            ORDER BY a.fecha_asignacion DESC
        """, (taller_id,))
        rows = cur.fetchall()
        return [_row_to_asignada(dict(r)) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando asignadas: {str(e)}")
    finally:
        cur.close()


@router.get("/{taller_id}/historial", response_model=List[SolicitudAsignadaResponse])
async def historial_asignaciones(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Historial completo de asignaciones del taller."""
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                a.asignacion_id, a.incidente_id, a.tecnico_id, a.taller_id,
                a.estado, a.tiempo_estimado_minutos,
                a.fecha_asignacion, a.observaciones,
                i.descripcion, i.latitud, i.longitud,
                i.imagen_path, i.audio_path, i.prioridad,
                u.nombre   AS cliente_nombre,
                u.telefono AS cliente_telefono,
                v.marca, v.modelo, v.placa,
                t.nombre   AS tecnico_nombre
            FROM ASIGNACION a
            JOIN INCIDENTE i ON a.incidente_id = i.incidente_id
            JOIN USUARIO   u ON i.usuario_id   = u.usuario_id
            JOIN VEHICULO  v ON i.vehiculo_id  = v.vehiculo_id
            LEFT JOIN TECNICO t ON a.tecnico_id = t.tecnico_id
            WHERE a.taller_id = %s
            ORDER BY a.fecha_asignacion DESC
        """, (taller_id,))
        rows = cur.fetchall()
        return [_row_to_asignada(dict(r)) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo historial: {str(e)}")
    finally:
        cur.close()


@router.post("/{taller_id}/aceptar", response_model=AsignacionResponse, status_code=201)
async def aceptar_solicitud(
    taller_id: int,
    data: AceptarSolicitudRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Acepta un incidente pendiente.
    - Crea ASIGNACION con estado='aceptada'.
    - Cambia INCIDENTE.estado a 'asignada'.
    """
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el incidente existe y sigue pendiente
        cur.execute(
            "SELECT incidente_id, estado FROM INCIDENTE WHERE incidente_id = %s",
            (data.incidente_id,)
        )
        incidente = cur.fetchone()
        if not incidente:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")
        if incidente['estado'] != 'pendiente':
            raise HTTPException(
                status_code=400,
                detail=f"El incidente ya no está disponible (estado: {incidente['estado']})"
            )

        # Verificar que no haya asignación activa de este taller para este incidente
        cur.execute("""
            SELECT asignacion_id FROM ASIGNACION
            WHERE incidente_id = %s AND taller_id = %s
              AND estado IN ('aceptada', 'en_servicio')
        """, (data.incidente_id, taller_id))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya tienes este incidente asignado")

        # Crear la asignación
        cur.execute("""
            INSERT INTO ASIGNACION (
                incidente_id, taller_id, tecnico_id, estado,
                tiempo_estimado_minutos, fecha_asignacion, fecha_aceptacion
            )
            VALUES (%s, %s, %s, 'aceptada', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING asignacion_id
        """, (
            data.incidente_id,
            taller_id,
            data.tecnico_id,
            data.tiempo_estimado_minutos,
        ))
        asignacion_id = cur.fetchone()['asignacion_id']

        # Actualizar estado del incidente
        cur.execute("""
            UPDATE INCIDENTE
            SET estado = 'asignada', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s
        """, (data.incidente_id,))

        db.commit()
        return AsignacionResponse(
            success=True,
            message="Solicitud aceptada correctamente",
            asignacion_id=asignacion_id,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error aceptando solicitud: {str(e)}")
    finally:
        cur.close()


@router.post("/{taller_id}/rechazar", response_model=MessageResponse)
async def rechazar_solicitud(
    taller_id: int,
    data: RechazarSolicitudRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Rechaza un incidente.
    - Registra ASIGNACION con estado='rechazada'.
    - El incidente queda 'pendiente' para que otros talleres puedan aceptarlo.
    """
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el incidente existe
        cur.execute(
            "SELECT incidente_id, estado FROM INCIDENTE WHERE incidente_id = %s",
            (data.incidente_id,)
        )
        incidente = cur.fetchone()
        if not incidente:
            raise HTTPException(status_code=404, detail="Incidente no encontrado")

        # Registrar rechazo (sin cambiar el estado del incidente)
        cur.execute("""
            INSERT INTO ASIGNACION (
                incidente_id, taller_id, estado, observaciones, fecha_asignacion
            )
            VALUES (%s, %s, 'rechazada', %s, CURRENT_TIMESTAMP)
        """, (data.incidente_id, taller_id, data.observaciones))

        db.commit()
        return MessageResponse(success=True, message="Solicitud rechazada")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error rechazando solicitud: {str(e)}")
    finally:
        cur.close()


@router.put("/{taller_id}/{asignacion_id}/asignar-tecnico", response_model=MessageResponse)
async def asignar_tecnico(
    taller_id: int,
    asignacion_id: int,
    data: AsignarTecnicoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Asigna un técnico disponible del taller a la asignación."""
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT asignacion_id, estado, tecnico_id FROM ASIGNACION WHERE asignacion_id = %s AND taller_id = %s",
            (asignacion_id, taller_id)
        )
        asignacion = cur.fetchone()
        if not asignacion:
            raise HTTPException(status_code=404, detail="Asignación no encontrada")
        if asignacion['estado'] not in ('aceptada', 'en_camino', 'en_servicio'):
            raise HTTPException(status_code=400, detail="No se puede asignar técnico en este estado")

        cur.execute(
            "SELECT tecnico_id FROM TECNICO WHERE tecnico_id = %s AND taller_id = %s AND disponible = TRUE",
            (data.tecnico_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Técnico no encontrado o no disponible")

        # Liberar técnico anterior si era distinto
        prev_tecnico_id = asignacion.get('tecnico_id')
        if prev_tecnico_id and prev_tecnico_id != data.tecnico_id:
            cur.execute(
                "UPDATE TECNICO SET disponible = TRUE WHERE tecnico_id = %s",
                (prev_tecnico_id,)
            )

        cur.execute(
            "UPDATE ASIGNACION SET tecnico_id = %s WHERE asignacion_id = %s",
            (data.tecnico_id, asignacion_id)
        )

        # Marcar al nuevo técnico como no disponible
        cur.execute(
            "UPDATE TECNICO SET disponible = FALSE WHERE tecnico_id = %s",
            (data.tecnico_id,)
        )

        db.commit()
        return MessageResponse(success=True, message="Técnico asignado correctamente")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error asignando técnico: {str(e)}")
    finally:
        cur.close()


@router.put("/{taller_id}/{asignacion_id}/estado", response_model=MessageResponse)
async def actualizar_estado(
    taller_id: int,
    asignacion_id: int,
    data: ActualizarEstadoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Actualiza el estado del servicio: en_camino → en_servicio → completada."""
    ESTADOS_VALIDOS = ('en_camino', 'en_servicio', 'completada')
    if data.estado not in ESTADOS_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Estado inválido. Opciones: {ESTADOS_VALIDOS}")

    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT asignacion_id, incidente_id, estado, tecnico_id FROM ASIGNACION WHERE asignacion_id = %s AND taller_id = %s",
            (asignacion_id, taller_id)
        )
        asignacion = cur.fetchone()
        if not asignacion:
            raise HTTPException(status_code=404, detail="Asignación no encontrada")
        if asignacion['estado'] == 'completada':
            raise HTTPException(status_code=400, detail="La asignación ya está completada")

        cur.execute(
            "UPDATE ASIGNACION SET estado = %s WHERE asignacion_id = %s",
            (data.estado, asignacion_id)
        )

        # Si se completa: cerrar incidente y liberar técnico
        if data.estado == 'completada':
            cur.execute(
                "UPDATE INCIDENTE SET estado = 'cerrada', fecha_actualizacion = CURRENT_TIMESTAMP WHERE incidente_id = %s",
                (asignacion['incidente_id'],)
            )
            if asignacion.get('tecnico_id'):
                cur.execute(
                    "UPDATE TECNICO SET disponible = TRUE WHERE tecnico_id = %s",
                    (asignacion['tecnico_id'],)
                )

        db.commit()
        return MessageResponse(success=True, message=f"Estado actualizado a '{data.estado}'")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error actualizando estado: {str(e)}")
    finally:
        cur.close()


@router.put("/{taller_id}/{asignacion_id}/diagnostico", response_model=MessageResponse)
async def registrar_diagnostico(
    taller_id: int,
    asignacion_id: int,
    data: DiagnosticoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """Registra diagnóstico, costo del servicio y cierra la asignación."""
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT asignacion_id, incidente_id, tecnico_id FROM ASIGNACION WHERE asignacion_id = %s AND taller_id = %s",
            (asignacion_id, taller_id)
        )
        asignacion = cur.fetchone()
        if not asignacion:
            raise HTTPException(status_code=404, detail="Asignación no encontrada")

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
            SET estado = 'cerrada', fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE incidente_id = %s
        """, (asignacion['incidente_id'],))

        # Liberar técnico asignado
        if asignacion.get('tecnico_id'):
            cur.execute(
                "UPDATE TECNICO SET disponible = TRUE WHERE tecnico_id = %s",
                (asignacion['tecnico_id'],)
            )

        # Registrar pago (INSERT OR UPDATE si ya existe)
        cur.execute("""
            INSERT INTO PAGO (
                incidente_id, asignacion_id, monto_total, monto_servicio,
                comision_plataforma, monto_taller, metodo_pago, estado, fecha_pago
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'completado', CURRENT_TIMESTAMP)
            ON CONFLICT (incidente_id) DO UPDATE SET
                monto_total = EXCLUDED.monto_total,
                monto_servicio = EXCLUDED.monto_servicio,
                comision_plataforma = EXCLUDED.comision_plataforma,
                monto_taller = EXCLUDED.monto_taller,
                metodo_pago = EXCLUDED.metodo_pago,
                estado = 'completado',
                fecha_pago = CURRENT_TIMESTAMP
        """, (
            asignacion['incidente_id'],
            asignacion_id,
            data.costo,
            data.costo,
            comision,
            monto_taller,
            data.metodo_pago,
        ))

        db.commit()
        return MessageResponse(success=True, message="Diagnóstico y costo registrados correctamente")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error registrando diagnóstico: {str(e)}")
    finally:
        cur.close()
