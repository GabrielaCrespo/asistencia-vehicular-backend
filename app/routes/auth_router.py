from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import bcrypt
import jwt

from app.services.config import Config
from app.classes.postgresql import Database

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
    documento_identidad: str
    rol_id: int
    estado: str
    taller_id: int
    razon_social: str

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
                u.estado,
                u.documento_identidad,
                u.rol_id,
                t.taller_id,
                t.razon_social
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
            documento_identidad=user['documento_identidad'],
            rol_id=user['rol_id'],
            estado=user['estado'],
            taller_id=user['taller_id'],
            razon_social=user['razon_social']
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