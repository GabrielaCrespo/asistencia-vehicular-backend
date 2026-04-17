from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import jwt

from ..services.config import Config
from ..classes.postgresql import Database
from passlib.context import CryptContext

router = APIRouter(prefix="/api/cliente", tags=["Cliente Authentication"])


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)

# ===================== MODELOS REQUEST =====================

class ClienteRegister(BaseModel):
    nombre: str
    email: EmailStr
    telefono: str
    password: str
    documento_identidad: str

class LoginRequest(BaseModel):
    email: str
    password: str

# ===================== MODELOS RESPONSE =====================

class ClienteUserResponse(BaseModel):
    usuario_id: int
    nombre: str
    email: str
    telefono: str
    documento_identidad: str
    rol_id: int
    estado: str

class LoginResponse(BaseModel):
    success: bool
    access_token: str
    user: ClienteUserResponse

class RegisterResponse(BaseModel):
    success: bool
    message: str
    user_id: int = None

# ===================== ENDPOINTS =====================

@router.post("/register", response_model=RegisterResponse)
async def register_cliente(data: ClienteRegister, db=Depends(Database.get_db)):
    """
    Registra un nuevo cliente en el sistema.
    """
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT usuario_id FROM USUARIO WHERE email = %s LIMIT 1",
            (data.email.lower(),)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="El correo ya está registrado."
            )

        password_hash = hash_password(data.password)

        cur.execute("""
            INSERT INTO USUARIO (rol_id, nombre, email, telefono, contrasena_hash, documento_identidad, estado)
            VALUES (1, %s, %s, %s, %s, %s, 'activo')
            RETURNING usuario_id
        """, (
            data.nombre.upper(),
            data.email.lower(),
            data.telefono,
            password_hash,
            data.documento_identidad
        ))

        nuevo_usuario_id = cur.fetchone()[0]
        db.commit()

        return RegisterResponse(
            success=True,
            message="Cliente registrado exitosamente",
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
async def login_cliente(data: LoginRequest, db=Depends(Database.get_db)):
    """
    Autentica un cliente y retorna JWT token.
    """
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                u.usuario_id,
                u.contrasena_hash,
                u.nombre,
                u.email,
                u.telefono,
                u.estado,
                u.documento_identidad,
                u.rol_id
            FROM USUARIO u
            WHERE u.email = %s AND u.rol_id = 1
            LIMIT 1
        """, (data.email.lower(),))

        user = cur.fetchone()

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

        token_payload = {
            "sub": str(user['usuario_id']),
            "email": user['email'],
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(
            token_payload,
            Config.SECRET_KEY,
            algorithm=Config.ALGORITHM
        )

        user_response = ClienteUserResponse(
            usuario_id=user['usuario_id'],
            nombre=user['nombre'],
            email=user['email'],
            telefono=user['telefono'] or "",
            documento_identidad=user['documento_identidad'] or "",
            rol_id=user['rol_id'],
            estado=user['estado']
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
 # ===================== PERFIL =====================

class ClienteUpdate(BaseModel):
    nombre: str
    telefono: str
    documento_identidad: str

@router.get("/profile/{usuario_id}")
async def get_perfil_cliente(usuario_id: int, db=Depends(Database.get_db)):
    """Obtiene el perfil del cliente"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT usuario_id, nombre, email, telefono, 
                   documento_identidad, estado, fecha_registro
            FROM USUARIO 
            WHERE usuario_id = %s AND rol_id = 1
        """, (usuario_id,))
        
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        return {"success": True, "user": dict(user)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.put("/profile/{usuario_id}")
async def update_perfil_cliente(usuario_id: int, data: ClienteUpdate, db=Depends(Database.get_db)):
    """Actualiza el perfil del cliente"""
    cur = db.cursor()
    try:
        cur.execute("""
            UPDATE USUARIO 
            SET nombre = %s, telefono = %s, documento_identidad = %s,
                actualizado_en = CURRENT_TIMESTAMP
            WHERE usuario_id = %s AND rol_id = 1
        """, (
            data.nombre.upper(),
            data.telefono,
            data.documento_identidad,
            usuario_id
        ))

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")

        db.commit()
        return {"success": True, "message": "Perfil actualizado correctamente"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()       