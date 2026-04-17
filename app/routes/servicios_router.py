"""
ROUTER DE SERVICIOS - Backend
Gestión de servicios disponibles en cada taller

Los servicios vienen de un catálogo global (SERVICIO)
Cada taller customiza qué servicios ofrece via TALLER_SERVICIO
con precios personalizados
"""

from fastapi import APIRouter, HTTPException, status, Depends, Header
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import jwt
from typing import List, Optional
from datetime import datetime

from ..services.config import Config
from ..classes.postgresql import Database

router = APIRouter(prefix="/api/servicios", tags=["Servicios"])


# ===================== MODELOS REQUEST =====================

class ServicioCatalogoCreate(BaseModel):
    """Modelo para crear un servicio en el catálogo global"""
    nombre: str
    descripcion: Optional[str] = None
    categoria: Optional[str] = None
    precio_base: float


class TallerServicioCreate(BaseModel):
    """Modelo para agregar un servicio existente a un taller"""
    servicio_id: int
    precio_personalizado: Optional[float] = None
    disponible: bool = True


class TallerServicioUpdate(BaseModel):
    """Modelo para actualizar servicio en un taller"""
    precio_personalizado: Optional[float] = None
    disponible: Optional[bool] = None


class TallerServicioDirectoCreate(BaseModel):
    """
    Crea un servicio directamente en el taller.
    Si el nombre ya existe en el catálogo global, lo reutiliza.
    Si no existe, lo crea automáticamente.
    No requiere conocer el servicio_id de antemano.
    """
    nombre: str
    descripcion: Optional[str] = None
    categoria: Optional[str] = None
    precio: float  # Precio que cobra este taller (precio_personalizado)
    disponible: bool = True


# ===================== MODELOS RESPONSE =====================

class ServicioResponse(BaseModel):
    """Respuesta de servicio del catálogo"""
    servicio_id: int
    nombre: str
    descripcion: Optional[str]
    categoria: Optional[str]
    precio_base: float
    creado_en: str


class TallerServicioResponse(BaseModel):
    """Respuesta de servicio asociado a un taller"""
    taller_servicio_id: int
    taller_id: int
    servicio_id: int
    nombre_servicio: str
    descripcion: Optional[str]
    categoria: Optional[str]
    precio_base: float
    precio_personalizado: Optional[float]
    disponible: bool
    creado_en: str


class TallerServicioListaResponse(BaseModel):
    """Modelo de respuesta para listado de servicios de un taller"""
    success: bool
    data: List[TallerServicioResponse]
    total: int


class ServicioCatalogoListaResponse(BaseModel):
    """Modelo de respuesta para listado de catálogo"""
    success: bool
    data: List[ServicioResponse]
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


# ===================== ENDPOINTS - CATÁLOGO GLOBAL =====================

@router.get("/catalogo/todos", response_model=ServicioCatalogoListaResponse)
async def listar_catalogo_servicios(
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Lista todos los servicios disponibles en el catálogo global
    Útil para agregar servicios a un taller
    """
    # Solo validar que hay token (cualquier usuario autenticado puede ver)
    token_payload = get_token_from_header(authorization)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                servicio_id, nombre, descripcion, 
                categoria, precio_base, creado_en
            FROM SERVICIO
            ORDER BY categoria, nombre
        """)
        
        servicios = cur.fetchall()
        
        servicios_list = [
            ServicioResponse(
                servicio_id=s['servicio_id'],
                nombre=s['nombre'],
                descripcion=s['descripcion'],
                categoria=s['categoria'],
                precio_base=float(s['precio_base']),
                creado_en=str(s['creado_en'])
            )
            for s in servicios
        ]
        
        return ServicioCatalogoListaResponse(
            success=True,
            data=servicios_list,
            total=len(servicios_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listando catálogo: {str(e)}"
        )
    finally:
        cur.close()


# ===================== ENDPOINTS - CATÁLOGO GLOBAL (ESCRITURA) =====================

@router.post("/catalogo", response_model=ServicioResponse, status_code=201)
async def crear_servicio_catalogo(
    data: ServicioCatalogoCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Crea un nuevo servicio en el catálogo global
    Cualquier usuario autenticado puede crear servicios en el catálogo
    """
    token_payload = get_token_from_header(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que no existe ya un servicio con ese nombre
        cur.execute(
            "SELECT servicio_id FROM SERVICIO WHERE UPPER(nombre) = UPPER(%s)",
            (data.nombre,)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Ya existe un servicio con ese nombre en el catálogo"
            )

        cur.execute("""
            INSERT INTO SERVICIO (nombre, descripcion, categoria, precio_base)
            VALUES (%s, %s, %s, %s)
            RETURNING servicio_id, nombre, descripcion, categoria, precio_base, creado_en
        """, (
            data.nombre.upper(),
            data.descripcion,
            data.categoria.upper() if data.categoria else None,
            data.precio_base
        ))

        servicio = cur.fetchone()
        db.commit()

        return ServicioResponse(
            servicio_id=servicio['servicio_id'],
            nombre=servicio['nombre'],
            descripcion=servicio['descripcion'],
            categoria=servicio['categoria'],
            precio_base=float(servicio['precio_base']),
            creado_en=str(servicio['creado_en'])
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error creando servicio: {str(e)}"
        )
    finally:
        cur.close()


# ===================== ENDPOINTS - SERVICIOS DEL TALLER =====================

@router.post("/{taller_id}/crear", response_model=TallerServicioResponse, status_code=201)
async def crear_servicio_directo_en_taller(
    taller_id: int,
    data: TallerServicioDirectoCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    NUEVO FLUJO: El taller crea un servicio en un solo paso.

    Lógica interna:
    1. Busca en el catálogo global si ya existe un servicio con ese nombre.
    2. Si no existe, lo crea automáticamente en SERVICIO (catálogo).
    3. Registra el servicio en TALLER_SERVICIO con el precio del taller.

    El taller nunca necesita conocer el catálogo global ni hacer doble paso.
    """
    token_payload = get_token_from_header(authorization)
    verify_taller_access(token_payload, taller_id, db)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        nombre_normalizado = data.nombre.strip().upper()
        categoria_normalizada = data.categoria.strip().upper() if data.categoria else None

        # Paso 1: Buscar si el servicio ya existe en el catálogo global
        cur.execute(
            "SELECT servicio_id, nombre, descripcion, categoria, precio_base FROM SERVICIO WHERE UPPER(nombre) = %s",
            (nombre_normalizado,)
        )
        servicio = cur.fetchone()

        if servicio:
            servicio_id = servicio['servicio_id']
            precio_base = float(servicio['precio_base'])
            nombre_final = servicio['nombre']
            descripcion_final = servicio['descripcion']
            categoria_final = servicio['categoria']
        else:
            # Paso 2: Crear automáticamente en el catálogo global
            # El precio_base del catálogo se toma del precio que define el taller
            cur.execute("""
                INSERT INTO SERVICIO (nombre, descripcion, categoria, precio_base)
                VALUES (%s, %s, %s, %s)
                RETURNING servicio_id, nombre, descripcion, categoria, precio_base
            """, (nombre_normalizado, data.descripcion, categoria_normalizada, data.precio))
            servicio = cur.fetchone()
            servicio_id = servicio['servicio_id']
            precio_base = float(servicio['precio_base'])
            nombre_final = servicio['nombre']
            descripcion_final = servicio['descripcion']
            categoria_final = servicio['categoria']

        # Paso 3: Verificar que el taller no tenga ya ese servicio
        cur.execute(
            "SELECT taller_servicio_id FROM TALLER_SERVICIO WHERE taller_id = %s AND servicio_id = %s",
            (taller_id, servicio_id)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail=f"El taller ya ofrece el servicio '{nombre_final}'"
            )

        # Paso 4: Registrar en TALLER_SERVICIO con el precio del taller
        cur.execute("""
            INSERT INTO TALLER_SERVICIO (taller_id, servicio_id, precio_personalizado, disponible)
            VALUES (%s, %s, %s, %s)
            RETURNING taller_servicio_id, creado_en
        """, (taller_id, servicio_id, data.precio, data.disponible))

        result = cur.fetchone()
        db.commit()

        return TallerServicioResponse(
            taller_servicio_id=result['taller_servicio_id'],
            taller_id=taller_id,
            servicio_id=servicio_id,
            nombre_servicio=nombre_final,
            descripcion=descripcion_final,
            categoria=categoria_final,
            precio_base=precio_base,
            precio_personalizado=data.precio,
            disponible=data.disponible,
            creado_en=str(result['creado_en'])
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error creando servicio: {str(e)}"
        )
    finally:
        cur.close()


@router.post("/{taller_id}", response_model=TallerServicioResponse, status_code=201)
async def agregar_servicio_a_taller(
    taller_id: int,
    data: TallerServicioCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Agrega un servicio del catálogo al taller
    Permite customizar el precio
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
        
        # Validar que el servicio existe en el catálogo
        cur.execute(
            "SELECT nombre, descripcion, categoria, precio_base FROM SERVICIO WHERE servicio_id = %s",
            (data.servicio_id,)
        )
        servicio = cur.fetchone()
        if not servicio:
            raise HTTPException(
                status_code=404,
                detail="Servicio no encontrado en el catálogo"
            )
        
        # Verificar que no existe ya
        cur.execute(
            """SELECT taller_servicio_id FROM TALLER_SERVICIO 
               WHERE taller_id = %s AND servicio_id = %s""",
            (taller_id, data.servicio_id)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Este servicio ya está agregado al taller"
            )
        
        # Insertar TALLER_SERVICIO
        cur.execute("""
            INSERT INTO TALLER_SERVICIO (
                taller_id, 
                servicio_id, 
                precio_personalizado, 
                disponible
            )
            VALUES (%s, %s, %s, %s)
            RETURNING taller_servicio_id, creado_en
        """, (
            taller_id,
            data.servicio_id,
            data.precio_personalizado,
            data.disponible
        ))
        
        result = cur.fetchone()
        db.commit()
        
        return TallerServicioResponse(
            taller_servicio_id=result['taller_servicio_id'],
            taller_id=taller_id,
            servicio_id=data.servicio_id,
            nombre_servicio=servicio['nombre'],
            descripcion=servicio['descripcion'],
            categoria=servicio['categoria'],
            precio_base=float(servicio['precio_base']),
            precio_personalizado=float(data.precio_personalizado) if data.precio_personalizado else None,
            disponible=data.disponible,
            creado_en=str(result['creado_en'])
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error agregando servicio: {str(e)}"
        )
    finally:
        cur.close()


@router.get("/{taller_id}", response_model=TallerServicioListaResponse)
async def listar_servicios_taller(
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Lista todos los servicios que ofrece un taller
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                ts.taller_servicio_id,
                ts.taller_id,
                s.servicio_id,
                s.nombre,
                s.descripcion,
                s.categoria,
                s.precio_base,
                ts.precio_personalizado,
                ts.disponible,
                ts.creado_en
            FROM TALLER_SERVICIO ts
            INNER JOIN SERVICIO s ON ts.servicio_id = s.servicio_id
            WHERE ts.taller_id = %s
            ORDER BY s.categoria, s.nombre
        """, (taller_id,))
        
        servicios = cur.fetchall()
        
        servicios_list = [
            TallerServicioResponse(
                taller_servicio_id=s['taller_servicio_id'],
                taller_id=s['taller_id'],
                servicio_id=s['servicio_id'],
                nombre_servicio=s['nombre'],
                descripcion=s['descripcion'],
                categoria=s['categoria'],
                precio_base=float(s['precio_base']),
                precio_personalizado=float(s['precio_personalizado']) if s['precio_personalizado'] else None,
                disponible=s['disponible'],
                creado_en=str(s['creado_en'])
            )
            for s in servicios
        ]
        
        return TallerServicioListaResponse(
            success=True,
            data=servicios_list,
            total=len(servicios_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listando servicios del taller: {str(e)}"
        )
    finally:
        cur.close()


@router.get("/{taller_id}/{taller_servicio_id}", response_model=TallerServicioResponse)
async def obtener_servicio_taller(
    taller_id: int,
    taller_servicio_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Obtiene los detalles de un servicio específico del taller
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                ts.taller_servicio_id,
                ts.taller_id,
                s.servicio_id,
                s.nombre,
                s.descripcion,
                s.categoria,
                s.precio_base,
                ts.precio_personalizado,
                ts.disponible,
                ts.creado_en
            FROM TALLER_SERVICIO ts
            INNER JOIN SERVICIO s ON ts.servicio_id = s.servicio_id
            WHERE ts.taller_servicio_id = %s AND ts.taller_id = %s
        """, (taller_servicio_id, taller_id))
        
        servicio = cur.fetchone()
        
        if not servicio:
            raise HTTPException(
                status_code=404,
                detail="Servicio no encontrado en este taller"
            )
        
        return TallerServicioResponse(
            taller_servicio_id=servicio['taller_servicio_id'],
            taller_id=servicio['taller_id'],
            servicio_id=servicio['servicio_id'],
            nombre_servicio=servicio['nombre'],
            descripcion=servicio['descripcion'],
            categoria=servicio['categoria'],
            precio_base=float(servicio['precio_base']),
            precio_personalizado=float(servicio['precio_personalizado']) if servicio['precio_personalizado'] else None,
            disponible=servicio['disponible'],
            creado_en=str(servicio['creado_en'])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo servicio: {str(e)}"
        )
    finally:
        cur.close()


@router.put("/{taller_id}/{taller_servicio_id}", response_model=TallerServicioResponse)
async def actualizar_servicio_taller(
    taller_id: int,
    taller_servicio_id: int,
    data: TallerServicioUpdate,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Actualiza precio_personalizado o disponibilidad de servicio en taller
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar existencia
        cur.execute(
            """SELECT 1 FROM TALLER_SERVICIO 
               WHERE taller_servicio_id = %s AND taller_id = %s""",
            (taller_servicio_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Servicio no encontrado en este taller"
            )
        
        # Construir query dinámico
        updates = []
        params = []
        
        if data.precio_personalizado is not None:
            updates.append("precio_personalizado = %s")
            params.append(data.precio_personalizado)
        
        if data.disponible is not None:
            updates.append("disponible = %s")
            params.append(data.disponible)
        
        if not updates:
            # Si no hay cambios, retornar sin actualizar
            cur.execute("""
                SELECT 
                    ts.taller_servicio_id,
                    ts.taller_id,
                    s.servicio_id,
                    s.nombre,
                    s.descripcion,
                    s.categoria,
                    s.precio_base,
                    ts.precio_personalizado,
                    ts.disponible,
                    ts.creado_en
                FROM TALLER_SERVICIO ts
                INNER JOIN SERVICIO s ON ts.servicio_id = s.servicio_id
                WHERE ts.taller_servicio_id = %s AND ts.taller_id = %s
            """, (taller_servicio_id, taller_id))
            servicio = cur.fetchone()
        else:
            params.extend([taller_servicio_id, taller_id])
            
            query = f"""
                UPDATE TALLER_SERVICIO
                SET {', '.join(updates)}
                WHERE taller_servicio_id = %s AND taller_id = %s
                RETURNING 
                    taller_servicio_id, taller_id, servicio_id,
                    precio_personalizado, disponible, creado_en
            """
            cur.execute(query, params)
            result = cur.fetchone()
            db.commit()
            
            # Obtener datos completos del servicio
            cur.execute("""
                SELECT 
                    ts.taller_servicio_id,
                    ts.taller_id,
                    s.servicio_id,
                    s.nombre,
                    s.descripcion,
                    s.categoria,
                    s.precio_base,
                    ts.precio_personalizado,
                    ts.disponible,
                    ts.creado_en
                FROM TALLER_SERVICIO ts
                INNER JOIN SERVICIO s ON ts.servicio_id = s.servicio_id
                WHERE ts.taller_servicio_id = %s AND ts.taller_id = %s
            """, (taller_servicio_id, taller_id))
            servicio = cur.fetchone()
        
        return TallerServicioResponse(
            taller_servicio_id=servicio['taller_servicio_id'],
            taller_id=servicio['taller_id'],
            servicio_id=servicio['servicio_id'],
            nombre_servicio=servicio['nombre'],
            descripcion=servicio['descripcion'],
            categoria=servicio['categoria'],
            precio_base=float(servicio['precio_base']),
            precio_personalizado=float(servicio['precio_personalizado']) if servicio['precio_personalizado'] else None,
            disponible=servicio['disponible'],
            creado_en=str(servicio['creado_en'])
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error actualizando servicio: {str(e)}"
        )
    finally:
        cur.close()


@router.delete("/{taller_id}/{taller_servicio_id}", response_model=MessageResponse)
async def remover_servicio_taller(
    taller_id: int,
    taller_servicio_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db)
):
    """
    Elimina un servicio de los ofrecidos por el taller
    """
    # Validar token
    token_payload = get_token_from_header(authorization)
    
    # Verificar acceso al taller
    verify_taller_access(token_payload, taller_id, db)
    
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar existencia
        cur.execute(
            """SELECT 1 FROM TALLER_SERVICIO 
               WHERE taller_servicio_id = %s AND taller_id = %s""",
            (taller_servicio_id, taller_id)
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail="Servicio no encontrado en este taller"
            )
        
        # Eliminar
        cur.execute(
            """DELETE FROM TALLER_SERVICIO 
               WHERE taller_servicio_id = %s AND taller_id = %s""",
            (taller_servicio_id, taller_id)
        )
        db.commit()
        
        return MessageResponse(
            success=True,
            message="Servicio removido del taller exitosamente"
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error removiendo servicio: {str(e)}"
        )
    finally:
        cur.close()