"""
ROUTER DE TÉCNICOS - Backend
Gestión CRUD de técnicos por taller

Cada taller solo puede ver y modificar sus propios técnicos
"""

from fastapi import APIRouter, HTTPException, status, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import jwt
from typing import List, Optional
from datetime import datetime

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/tecnicos", tags=["Técnicos"])


# ===================== MODELOS REQUEST =====================

class TecnicoCreate(BaseModel):
    """Modelo para crear nuevo técnico"""
    nombre: str
    especialidad: Optional[str] = None


class TecnicoUpdate(BaseModel):
    """Modelo para actualizar técnico"""
    nombre: Optional[str] = None
    especialidad: Optional[str] = None
    disponible: Optional[bool] = None


class TecnicoUbicacion(BaseModel):
    """Modelo para actualizar ubicación del técnico"""
    latitud: float
    longitud: float


# ===================== MODELOS RESPONSE =====================

class TecnicoResponse(BaseModel):
    """Modelo de respuesta de técnico"""
    tecnico_id: int
    taller_id: int
    nombre: str
    especialidad: Optional[str]
    latitud_actual: Optional[float]
    longitud_actual: Optional[float]
    disponible: bool
    fecha_ultima_ubicacion: Optional[str]
    creado_en: str


class TecnicoListaResponse(BaseModel):
    """Modelo de respuesta para listado de técnicos"""
    success: bool
    data: List[TecnicoResponse]
    total: int


class MessageResponse(BaseModel):
    """Respuesta genérica de mensaje"""
    success: bool
    message: str


# ===================== FUNCIONES AUXILIARES =====================

def get_token_from_header(authorization: str = Header(None)) -> dict:
    """
    Extrae y decodifica el JWT del header
    Retorna el payload del token
    Lanza excepción si no es válido
    """
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Token no proporcionado"
        )
    
    try:
        # El header viene como "Bearer <token>"
        token = authorization.split(" ")[1]
    except IndexError:
        raise HTTPException(
            status_code=401,
            detail="Formato de token inválido"
        )
    
    try:
        payload = jwt.decode(
            token,
            Config.SECRET_KEY,
            algorithms=[Config.ALGORITHM]
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Token expirado"
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token inválido"
        )


def verify_taller_access(token_payload: dict, taller_id: int, db) -> bool:
    """
    Verifica que el usuario del token es propietario del taller
    Retorna True si tiene acceso, lanza excepción si no
    """
    usuario_id = int(token_payload.get("sub"))
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT usuario_id FROM TALLER WHERE taller_id = %s",
            (taller_id,)
        )
        taller = cur.fetchone()
        cur.close()
        
        if not taller or taller['usuario_id'] != usuario_id:
            raise HTTPException(
                status_code=403,
                detail="No tienes permiso para acceder a este taller"
            )
        return True
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Error verificando acceso: {str(e)}"
        )


# ===================== ENDPOINTS =====================

@router.post("/{taller_id}", response_model=TecnicoResponse, status_code=201)
async def crear_tecnico(
    taller_id: int,
    data: TecnicoCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Crea un nuevo técnico para el taller
    Solo el propietario del taller puede crear técnicos
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Validar que el taller existe
        cur.execute(
            "SELECT taller_id FROM TALLER WHERE taller_id = %s",
            (taller_id,)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Taller no encontrado"
            )
        
        # Insertar técnico
        cur.execute("""
            INSERT INTO TECNICO (taller_id, nombre, especialidad, disponible)
            VALUES (%s, %s, %s, TRUE)
            RETURNING 
                tecnico_id, taller_id, nombre, especialidad, 
                latitud_actual, longitud_actual, disponible,
                fecha_ultima_ubicacion, creado_en
        """, (
            taller_id,
            data.nombre.upper(),
            data.especialidad.upper() if data.especialidad else None
        ))
        
        tecnico = cur.fetchone()
        db.commit()
        
        return TecnicoResponse(
            tecnico_id=tecnico['tecnico_id'],
            taller_id=tecnico['taller_id'],
            nombre=tecnico['nombre'],
            especialidad=tecnico['especialidad'],
            latitud_actual=tecnico['latitud_actual'],
            longitud_actual=tecnico['longitud_actual'],
            disponible=tecnico['disponible'],
            fecha_ultima_ubicacion=str(tecnico['fecha_ultima_ubicacion']) if tecnico['fecha_ultima_ubicacion'] else None,
            creado_en=str(tecnico['creado_en'])
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error creando técnico: {str(e)}"
        )
    finally:
        cur.close()


@router.get("/{taller_id}", response_model=TecnicoListaResponse)
async def listar_tecnicos(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Lista todos los técnicos del taller
    Solo el propietario del taller puede ver sus técnicos
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                tecnico_id, taller_id, nombre, especialidad,
                latitud_actual, longitud_actual, disponible,
                fecha_ultima_ubicacion, creado_en
            FROM TECNICO
            WHERE taller_id = %s
            ORDER BY creado_en DESC
        """, (taller_id,))
        
        tecnicos = cur.fetchall()
        
        tecnicos_list = [
            TecnicoResponse(
                tecnico_id=t['tecnico_id'],
                taller_id=t['taller_id'],
                nombre=t['nombre'],
                especialidad=t['especialidad'],
                latitud_actual=t['latitud_actual'],
                longitud_actual=t['longitud_actual'],
                disponible=t['disponible'],
                fecha_ultima_ubicacion=str(t['fecha_ultima_ubicacion']) if t['fecha_ultima_ubicacion'] else None,
                creado_en=str(t['creado_en'])
            )
            for t in tecnicos
        ]
        
        return TecnicoListaResponse(
            success=True,
            data=tecnicos_list,
            total=len(tecnicos_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listando técnicos: {str(e)}"
        )
    finally:
        cur.close()


@router.get("/{taller_id}/{tecnico_id}", response_model=TecnicoResponse)
async def obtener_tecnico(
    taller_id: int,
    tecnico_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Obtiene los detalles de un técnico específico
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                tecnico_id, taller_id, nombre, especialidad,
                latitud_actual, longitud_actual, disponible,
                fecha_ultima_ubicacion, creado_en
            FROM TECNICO
            WHERE tecnico_id = %s AND taller_id = %s
        """, (tecnico_id, taller_id))
        
        tecnico = cur.fetchone()
        
        if not tecnico:
            raise HTTPException(
                status_code=404,
                detail="Técnico no encontrado"
            )
        
        return TecnicoResponse(
            tecnico_id=tecnico['tecnico_id'],
            taller_id=tecnico['taller_id'],
            nombre=tecnico['nombre'],
            especialidad=tecnico['especialidad'],
            latitud_actual=tecnico['latitud_actual'],
            longitud_actual=tecnico['longitud_actual'],
            disponible=tecnico['disponible'],
            fecha_ultima_ubicacion=str(tecnico['fecha_ultima_ubicacion']) if tecnico['fecha_ultima_ubicacion'] else None,
            creado_en=str(tecnico['creado_en'])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo técnico: {str(e)}"
        )
    finally:
        cur.close()


@router.put("/{taller_id}/{tecnico_id}", response_model=TecnicoResponse)
async def actualizar_tecnico(
    taller_id: int,
    tecnico_id: int,
    data: TecnicoUpdate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Actualiza los datos de un técnico
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el técnico existe y pertenece al taller
        cur.execute(
            "SELECT tecnico_id FROM TECNICO WHERE tecnico_id = %s AND taller_id = %s",
            (tecnico_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Técnico no encontrado"
            )
        
        # Construir query dinámico
        updates = []
        params = []
        
        if data.nombre is not None:
            updates.append("nombre = %s")
            params.append(data.nombre.upper())
        
        if data.especialidad is not None:
            updates.append("especialidad = %s")
            params.append(data.especialidad.upper())
        
        if data.disponible is not None:
            updates.append("disponible = %s")
            params.append(data.disponible)
        
        if not updates:
            # Si no hay cambios, retornar el técnico sin cambios
            cur.execute("""
                SELECT 
                    tecnico_id, taller_id, nombre, especialidad,
                    latitud_actual, longitud_actual, disponible,
                    fecha_ultima_ubicacion, creado_en
                FROM TECNICO
                WHERE tecnico_id = %s AND taller_id = %s
            """, (tecnico_id, taller_id))
            tecnico = cur.fetchone()
        else:
            updates.append("actualizado_en = CURRENT_TIMESTAMP")
            params.extend([tecnico_id, taller_id])
            
            query = f"""
                UPDATE TECNICO
                SET {', '.join(updates)}
                WHERE tecnico_id = %s AND taller_id = %s
                RETURNING 
                    tecnico_id, taller_id, nombre, especialidad,
                    latitud_actual, longitud_actual, disponible,
                    fecha_ultima_ubicacion, creado_en
            """
            cur.execute(query, params)
            tecnico = cur.fetchone()
            db.commit()
        
        return TecnicoResponse(
            tecnico_id=tecnico['tecnico_id'],
            taller_id=tecnico['taller_id'],
            nombre=tecnico['nombre'],
            especialidad=tecnico['especialidad'],
            latitud_actual=tecnico['latitud_actual'],
            longitud_actual=tecnico['longitud_actual'],
            disponible=tecnico['disponible'],
            fecha_ultima_ubicacion=str(tecnico['fecha_ultima_ubicacion']) if tecnico['fecha_ultima_ubicacion'] else None,
            creado_en=str(tecnico['creado_en'])
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error actualizando técnico: {str(e)}"
        )
    finally:
        cur.close()


@router.delete("/{taller_id}/{tecnico_id}", response_model=MessageResponse)
async def eliminar_tecnico(
    taller_id: int,
    tecnico_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Elimina un técnico del taller
    Solo se puede eliminar si no tiene asignaciones activas
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el técnico existe
        cur.execute(
            "SELECT tecnico_id FROM TECNICO WHERE tecnico_id = %s AND taller_id = %s",
            (tecnico_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Técnico no encontrado"
            )
        
        # Verificar que no tiene asignaciones pendientes
        cur.execute(
            """SELECT COUNT(*) as count FROM ASIGNACION 
               WHERE tecnico_id = %s AND estado IN ('pendiente', 'aceptada')""",
            (tecnico_id,)
        )
        result = cur.fetchone()
        if result['count'] > 0:
            raise HTTPException(
                status_code=400,
                detail="No se puede eliminar un técnico con asignaciones activas"
            )
        
        # Eliminar técnico
        cur.execute(
            "DELETE FROM TECNICO WHERE tecnico_id = %s AND taller_id = %s",
            (tecnico_id, taller_id)
        )
        db.commit()
        
        return MessageResponse(
            success=True,
            message="Técnico eliminado exitosamente"
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error eliminando técnico: {str(e)}"
        )
    finally:
        cur.close()


@router.put("/{taller_id}/{tecnico_id}/ubicacion", response_model=TecnicoResponse)
async def actualizar_ubicacion_tecnico(
    taller_id: int,
    tecnico_id: int,
    data: TecnicoUbicacion,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Actualiza la ubicación actual del técnico
    Útil para tracking en tiempo real
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que el técnico existe
        cur.execute(
            "SELECT tecnico_id FROM TECNICO WHERE tecnico_id = %s AND taller_id = %s",
            (tecnico_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Técnico no encontrado"
            )
        
        # Actualizar ubicación
        cur.execute("""
            UPDATE TECNICO
            SET latitud_actual = %s, 
                longitud_actual = %s,
                fecha_ultima_ubicacion = CURRENT_TIMESTAMP,
                actualizado_en = CURRENT_TIMESTAMP
            WHERE tecnico_id = %s AND taller_id = %s
            RETURNING 
                tecnico_id, taller_id, nombre, especialidad,
                latitud_actual, longitud_actual, disponible,
                fecha_ultima_ubicacion, creado_en
        """, (data.latitud, data.longitud, tecnico_id, taller_id))
        
        tecnico = cur.fetchone()
        db.commit()
        
        return TecnicoResponse(
            tecnico_id=tecnico['tecnico_id'],
            taller_id=tecnico['taller_id'],
            nombre=tecnico['nombre'],
            especialidad=tecnico['especialidad'],
            latitud_actual=tecnico['latitud_actual'],
            longitud_actual=tecnico['longitud_actual'],
            disponible=tecnico['disponible'],
            fecha_ultima_ubicacion=str(tecnico['fecha_ultima_ubicacion']) if tecnico['fecha_ultima_ubicacion'] else None,
            creado_en=str(tecnico['creado_en'])
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error actualizando ubicación: {str(e)}"
        )
    finally:
        cur.close()