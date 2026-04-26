from fastapi import APIRouter, HTTPException, status, Depends, Header
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Optional
import bcrypt
import jwt

from ..services.config import Config
from ..classes.postgresql import Database

# Router para autenticación de talleres
router = APIRouter(prefix="/api/taller", tags=["Taller Authentication"])


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

# ===================== MODELOS REQUEST =====================

class TallerRegister(BaseModel):
    """Modelo para registro de nuevo taller"""
    nombre_contacto: str
    email: EmailStr
    telefono: str
    password: str
    documento_identidad: str
    razon_social: str
    direccion: str
    latitud: float
    longitud: float
    telefono_operativo: str
    horario_inicio: str
    horario_fin: str

class LoginRequest(BaseModel):
    """Modelo para login de taller"""
    email: str
    password: str

# ===================== MODELOS RESPONSE =====================

class UserResponse(BaseModel):
    """Respuesta con datos del usuario (sin passwordhash)"""
    usuario_id: int
    nombre: str
    email: str
    documento_identidad: str
    rol_id: int
    estado: str

class TallerUserResponse(BaseModel):
    """Respuesta completar del usuario con datos del taller"""
    usuario_id: int
    nombre: str
    email: str
    telefono: Optional[str] = None
    documento_identidad: str
    rol_id: int
    estado: str
    taller_id: int
    razon_social: str
    direccion: Optional[str] = None
    telefono_operativo: Optional[str] = None
    horario_inicio: Optional[str] = None
    horario_fin: Optional[str] = None

class LoginResponse(BaseModel):
    """Respuesta del endpoint login"""
    success: bool
    access_token: str
    user: TallerUserResponse

class RegisterResponse(BaseModel):
    """Respuesta del endpoint register"""
    success: bool
    message: str
    user_id: int = None

# ===================== ENDPOINTS =====================

@router.post("/register", response_model=RegisterResponse)
async def register_taller(data: TallerRegister, db=Depends(Database.get_db)):
    """
    Registra un nuevo taller en el sistema.
    
    Pasos:
    1. Valida que el email no exista
    2. Hash la contraseña
    3. Inserta usuario (rol_id = 2 para taller)
    4. Inserta taller asociado
    5. Retorna confirmación
    """
    cur = db.cursor()
    try:
        # Validar email único
        cur.execute(
            "SELECT usuario_id FROM USUARIO WHERE email = %s LIMIT 1",
            (data.email.lower(),)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="El correo ya está registrado."
            )

        # Hash de contraseña
        password_hash = hash_password(data.password)

        # 1. Insertar Usuario (rol_id=2 es TALLER)
        cur.execute("""
            INSERT INTO USUARIO (rol_id, nombre, email, telefono, contrasena_hash, documento_identidad, estado)
            VALUES (2, %s, %s, %s, %s, %s, 'activo')
            RETURNING usuario_id
        """, (
            data.nombre_contacto.upper(),
            data.email.lower(),
            data.telefono,
            password_hash,
            data.documento_identidad
        ))
        
        nuevo_usuario_id = cur.fetchone()[0]

        # 2. Insertar Taller
        cur.execute("""
            INSERT INTO TALLER (usuario_id, razon_social, direccion, latitud, longitud, telefono_operativo, horario_inicio, horario_fin, disponible)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
            RETURNING taller_id
        """, (
            nuevo_usuario_id,
            data.razon_social.upper(),
            data.direccion.upper(),
            data.latitud,
            data.longitud,
            data.telefono_operativo,
            data.horario_inicio,
            data.horario_fin
        ))
        nuevo_taller_id = cur.fetchone()[0]

        # 3. Vincular todos los servicios base con disponible=FALSE para que el taller los active
        cur.execute("""
            INSERT INTO TALLER_SERVICIO (taller_id, servicio_id, disponible)
            SELECT %s, s.servicio_id, FALSE
            FROM SERVICIO s
            WHERE s.categoria IN ('ELECTRICO', 'AUXILIO', 'MECANICA', 'GRUA', 'OTROS')
            ON CONFLICT (taller_id, servicio_id) DO NOTHING
        """, (nuevo_taller_id,))

        db.commit()
        return RegisterResponse(
            success=True,
            message="Taller registrado exitosamente",
            user_id=nuevo_usuario_id
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error en el registro: {str(e)}"
        )
    finally:
        cur.close()

@router.post("/login", response_model=LoginResponse)
async def login_taller(data: LoginRequest, db=Depends(Database.get_db)):
    """
    Autentica un taller y retorna JWT token.
    
    Query:
    - Busca usuario con rol_id=2 (TALLER)
    - Verifica contraseña
    - Genera JWT token con exp=24h
    - Retorna token + datos usuario (SIN contraseña)
    """
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Query que obtiene usuario y taller
        query = """
            SELECT
                u.usuario_id,
                u.contrasena_hash,
                u.nombre,
                u.email,
                u.telefono,
                u.estado,
                u.documento_identidad,
                u.rol_id,
                t.taller_id,
                t.razon_social,
                t.direccion,
                t.telefono_operativo,
                TO_CHAR(t.horario_inicio, 'HH24:MI') AS horario_inicio,
                TO_CHAR(t.horario_fin, 'HH24:MI') AS horario_fin
            FROM USUARIO u
            INNER JOIN TALLER t ON u.usuario_id = t.usuario_id
            WHERE u.email = %s AND u.rol_id = 2
            LIMIT 1
        """
        cur.execute(query, (data.email.lower(),))
        user = cur.fetchone()

        # Validar credenciales
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Credenciales inválidas"
            )
        
        if not verify_password(data.password, user['contrasena_hash']):
            raise HTTPException(
                status_code=401,
                detail="Credenciales inválidas"
            )

        # Generar JWT token
        token_payload = {
            "sub": str(user['usuario_id']),
            "taller_id": user['taller_id'],
            "email": user['email'],
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(
            token_payload,
            Config.SECRET_KEY,
            algorithm=Config.ALGORITHM
        )

        # Preparar respuesta SIN datos sensibles
        user_response = TallerUserResponse(
            usuario_id=user['usuario_id'],
            nombre=user['nombre'],
            email=user['email'],
            telefono=user['telefono'],
            documento_identidad=user['documento_identidad'],
            rol_id=user['rol_id'],
            estado=user['estado'],
            taller_id=user['taller_id'],
            razon_social=user['razon_social'],
            direccion=user['direccion'],
            telefono_operativo=user['telefono_operativo'],
            horario_inicio=user['horario_inicio'],
            horario_fin=user['horario_fin'],
        )

        return LoginResponse(
            success=True,
            access_token=token,
            user=user_response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error en login: {str(e)}"
        )
    finally:
        cur.close()


# ===================== MODELOS PERFIL =====================

class TallerProfileResponse(BaseModel):
    """Respuesta con perfil completo del taller"""
    usuario_id: int
    nombre: str
    email: str
    telefono: Optional[str] = None
    documento_identidad: Optional[str] = None
    rol_id: int
    estado: str
    taller_id: int
    razon_social: str
    direccion: Optional[str] = None
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    telefono_operativo: Optional[str] = None
    horario_inicio: Optional[str] = None
    horario_fin: Optional[str] = None
    disponible: bool
    calificacion_promedio: float = 0.0


class TallerProfileUpdate(BaseModel):
    """Campos actualizables del perfil del taller"""
    nombre_contacto: Optional[str] = None
    telefono: Optional[str] = None
    razon_social: Optional[str] = None
    direccion: Optional[str] = None
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    telefono_operativo: Optional[str] = None
    horario_inicio: Optional[str] = None
    horario_fin: Optional[str] = None


def _verify_token(authorization: str = Header(None)) -> dict:
    """Extrae y valida el JWT del header Authorization"""
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


# ===================== ENDPOINTS PERFIL =====================

@router.get("/profile/{taller_id}", response_model=TallerProfileResponse)
async def get_taller_profile(
    taller_id: int,
    payload: dict = Depends(_verify_token),
    db=Depends(Database.get_db)
):
    """
    Obtiene el perfil completo del taller (datos de usuario + taller).
    Solo el propio taller puede consultar su perfil.
    """
    if payload.get("taller_id") != taller_id:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                u.usuario_id, u.nombre, u.email, u.telefono,
                u.documento_identidad, u.rol_id, u.estado,
                t.taller_id, t.razon_social, t.direccion,
                CAST(t.latitud AS DOUBLE PRECISION) AS latitud,
                CAST(t.longitud AS DOUBLE PRECISION) AS longitud,
                t.telefono_operativo,
                COALESCE(TO_CHAR(t.horario_inicio, 'HH24:MI'), '') AS horario_inicio,
                COALESCE(TO_CHAR(t.horario_fin, 'HH24:MI'), '') AS horario_fin,
                t.disponible,
                CAST(COALESCE(t.calificacion_promedio, 0.0) AS DOUBLE PRECISION) AS calificacion_promedio
            FROM USUARIO u
            INNER JOIN TALLER t ON u.usuario_id = t.usuario_id
            WHERE t.taller_id = %s
        """, (taller_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Taller no encontrado")
        
        # Normalize and sanitize response data
        row['telefono'] = row.get('telefono') or ''
        row['documento_identidad'] = row.get('documento_identidad') or ''
        row['direccion'] = row.get('direccion') or ''
        row['latitud'] = row.get('latitud')
        row['longitud'] = row.get('longitud')
        row['telefono_operativo'] = row.get('telefono_operativo') or ''

        return TallerProfileResponse(**row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener perfil: {str(e)}")
    finally:
        cur.close()


@router.put("/profile/{taller_id}", response_model=TallerProfileResponse)
async def update_taller_profile(
    taller_id: int,
    data: TallerProfileUpdate,
    payload: dict = Depends(_verify_token),
    db=Depends(Database.get_db)
):
    """
    Actualiza el perfil del taller.
    Solo el propio taller puede editar su perfil.
    OPTIMIZACIÓN: Retorna respuesta sin query adicional.
    """
    print(f"[UPDATE TALLER] 📝 Recibida solicitud para taller_id={taller_id}")
    
    if payload.get("taller_id") != taller_id:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        usuario_id = int(payload.get("sub"))
        print(f"[UPDATE TALLER] 👤 Usuario ID: {usuario_id}")
        
        # Get current data ONCE at the beginning
        print(f"[UPDATE TALLER] 🔍 Fetching current data from DB...")
        cur.execute("""
            SELECT
                u.usuario_id, u.nombre, u.email, u.telefono, u.documento_identidad, u.rol_id, u.estado,
                t.taller_id, t.razon_social, t.direccion, t.latitud, t.longitud, t.telefono_operativo,
                t.horario_inicio, t.horario_fin, t.disponible, COALESCE(t.calificacion_promedio, 0.0) AS calificacion_promedio
            FROM USUARIO u
            INNER JOIN TALLER t ON u.usuario_id = t.usuario_id
            WHERE t.taller_id = %s
        """, (taller_id,))
        row = cur.fetchone()
        print(f"[UPDATE TALLER] ✅ Data fetched, row keys: {list(row.keys()) if row else 'None'}")
        
        if not row:
            raise HTTPException(status_code=404, detail="Taller no encontrado")
        
        # Apply updates in memory to the fetched row
        print(f"[UPDATE TALLER] 📋 Applying in-memory updates...")
        if data.nombre_contacto:
            row['nombre'] = data.nombre_contacto.upper()
        if data.telefono:
            row['telefono'] = data.telefono
        if data.razon_social:
            row['razon_social'] = data.razon_social.upper()
        if data.direccion:
            row['direccion'] = data.direccion.upper()
        if data.latitud is not None:
            row['latitud'] = float(data.latitud)
        if data.longitud is not None:
            row['longitud'] = float(data.longitud)
        if data.telefono_operativo:
            row['telefono_operativo'] = data.telefono_operativo
        if data.horario_inicio:
            row['horario_inicio'] = data.horario_inicio
        if data.horario_fin:
            row['horario_fin'] = data.horario_fin

        # Update USUARIO if needed
        print(f"[UPDATE TALLER] 💾 Updating USUARIO table...")
        if data.nombre_contacto or data.telefono:
            if data.nombre_contacto:
                cur.execute(
                    "UPDATE USUARIO SET nombre = %s WHERE usuario_id = %s",
                    (data.nombre_contacto.upper(), usuario_id)
                )
            if data.telefono:
                cur.execute(
                    "UPDATE USUARIO SET telefono = %s WHERE usuario_id = %s",
                    (data.telefono, usuario_id)
                )
        print(f"[UPDATE TALLER] ✓ USUARIO updated")

        # Update TALLER if needed
        print(f"[UPDATE TALLER] 💾 Updating TALLER table...")
        taller_updates = {}
        if data.razon_social:
            taller_updates['razon_social'] = data.razon_social.upper()
        if data.direccion:
            taller_updates['direccion'] = data.direccion.upper()
        if data.latitud is not None:
            taller_updates['latitud'] = data.latitud
        if data.longitud is not None:
            taller_updates['longitud'] = data.longitud
        if data.telefono_operativo:
            taller_updates['telefono_operativo'] = data.telefono_operativo
        if data.horario_inicio:
            taller_updates['horario_inicio'] = data.horario_inicio
        if data.horario_fin:
            taller_updates['horario_fin'] = data.horario_fin

        if taller_updates:
            set_clause = ", ".join(f"{k} = %s" for k in taller_updates.keys())
            values = list(taller_updates.values()) + [taller_id]
            cur.execute(f"UPDATE TALLER SET {set_clause} WHERE taller_id = %s", values)
        print(f"[UPDATE TALLER] ✓ TALLER updated")

        # Commit and return IMMEDIATELY with updated data from first query
        print(f"[UPDATE TALLER] 🔐 Committing transaction...")
        db.commit()
        print(f"[UPDATE TALLER] ✓ Transaction committed")
        
        # Normalize and sanitize response data
        print(f"[UPDATE TALLER] 🧹 Sanitizing response data...")
        
        # Ensure all optional fields have valid values (not None)
        row['telefono'] = row.get('telefono') or ''
        row['documento_identidad'] = row.get('documento_identidad') or ''
        row['direccion'] = row.get('direccion') or ''
        row['latitud'] = row.get('latitud')
        row['longitud'] = row.get('longitud')
        row['telefono_operativo'] = row.get('telefono_operativo') or ''

        # Convert time fields for response
        print(f"[UPDATE TALLER] 🕐 Converting time fields...")
        if row.get('horario_inicio'):
            try:
                row['horario_inicio'] = row['horario_inicio'].strftime('%H:%M') if hasattr(row['horario_inicio'], 'strftime') else str(row['horario_inicio'])
            except Exception as te:
                print(f"[UPDATE TALLER] ⚠️ Error converting horario_inicio: {te}")
                row['horario_inicio'] = ''
        else:
            row['horario_inicio'] = ''
            
        if row.get('horario_fin'):
            try:
                row['horario_fin'] = row['horario_fin'].strftime('%H:%M') if hasattr(row['horario_fin'], 'strftime') else str(row['horario_fin'])
            except Exception as te:
                print(f"[UPDATE TALLER] ⚠️ Error converting horario_fin: {te}")
                row['horario_fin'] = ''
        else:
            row['horario_fin'] = ''
        print(f"[UPDATE TALLER] ✓ Time fields converted")

        # Return response with ONE initial fetch + updates in memory
        print(f"[UPDATE TALLER] 📤 Building response object...")
        print(f"[UPDATE TALLER] Row data before response:")
        print(f"  usuario_id={row.get('usuario_id')} (type: {type(row.get('usuario_id'))})")
        print(f"  nombre={row.get('nombre')} (type: {type(row.get('nombre'))})")
        print(f"  latitud={row.get('latitud')} (type: {type(row.get('latitud'))})")
        print(f"  disponible={row.get('disponible')} (type: {type(row.get('disponible'))})")
        print(f"  calificacion_promedio={row.get('calificacion_promedio')} (type: {type(row.get('calificacion_promedio'))})")
        print(f"  horario_inicio={row.get('horario_inicio')} (type: {type(row.get('horario_inicio'))})")
        print(f"  horario_fin={row.get('horario_fin')} (type: {type(row.get('horario_fin'))})")
        
        try:
            # Necesitamos SANITIZAR disponible también
            row['disponible'] = row.get('disponible') if isinstance(row.get('disponible'), bool) else False
            
            response = TallerProfileResponse(**row)
            print(f"[UPDATE TALLER] Response object created successfully")
            response_json = response.model_dump_json()
            print(f"[UPDATE TALLER] Response JSON serialized OK - length: {len(response_json)} bytes")
            print(f"[UPDATE TALLER] Response JSON content: {response_json}")
            print(f"[UPDATE TALLER] ✅ ÉXITO - Retornando respuesta")
            return response
        except Exception as e:
            print(f"[UPDATE TALLER] ❌ ERROR building response: {str(e)}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error al serializar respuesta: {str(e)}")

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        print(f"[UPDATE TALLER] ❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al actualizar perfil: {str(e)}")
    finally:
        cur.close()