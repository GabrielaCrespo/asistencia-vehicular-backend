"""
Dependencias centralizadas para autenticación y aislamiento multi-tenant.

Jerarquía de roles:
  administrador  → acceso total a la plataforma
  tenant_admin   → acceso a toda su organización (todos sus talleres)
  taller         → acceso solo a su propio taller
  tecnico        → acceso a sus asignaciones
  cliente        → acceso a sus incidentes
"""

from fastapi import HTTPException, Header
from psycopg2.extras import RealDictCursor
import jwt

from ..services.config import Config


# ===================== TOKEN =====================

def get_token_payload(authorization: str = Header(None)) -> dict:
    """Decodifica el JWT del header Authorization y retorna el payload."""
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


# ===================== GUARDS POR ROL =====================

def require_tenant_admin(payload: dict) -> dict:
    """El caller pasa el payload ya decodificado; lanza 403 si no es admin de org."""
    if payload.get("rol") not in ("tenant_admin", "administrador"):
        raise HTTPException(
            status_code=403,
            detail="Se requiere rol de administrador de organización"
        )
    return payload


def require_taller_or_above(payload: dict) -> dict:
    """Permite taller, tenant_admin y administrador."""
    if payload.get("rol") not in ("taller", "tenant_admin", "administrador"):
        raise HTTPException(status_code=403, detail="Acceso no autorizado")
    return payload


# ===================== VERIFICACIÓN DE ACCESO A TALLER =====================

def assert_taller_access(payload: dict, taller_id: int, db) -> None:
    """
    Verifica que el usuario del token tiene acceso al taller indicado.

    - administrador  → siempre permitido
    - tenant_admin   → el taller debe pertenecer a su organización
    - taller         → el taller_id del token debe coincidir
    """
    rol = payload.get("rol")

    if rol == "administrador":
        return

    if rol == "taller":
        if payload.get("taller_id") != taller_id:
            raise HTTPException(status_code=403, detail="Acceso denegado a este taller")
        return

    if rol == "tenant_admin":
        org_id = payload.get("organizacion_id")
        cur = db.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                "SELECT organizacion_id FROM taller WHERE taller_id = %s",
                (taller_id,)
            )
            row = cur.fetchone()
        finally:
            cur.close()
        if not row or row["organizacion_id"] != org_id:
            raise HTTPException(
                status_code=403,
                detail="El taller no pertenece a tu organización"
            )
        return

    raise HTTPException(status_code=403, detail="Sin permisos suficientes")


# ===================== VERIFICACIÓN DE ACCESO A ORGANIZACIÓN =====================

def assert_org_access(payload: dict, org_id: int) -> None:
    """
    Verifica que el usuario del token tiene acceso a la organización indicada.

    - administrador  → siempre permitido
    - tenant_admin   → organizacion_id del token debe coincidir
    - otros          → denegado
    """
    rol = payload.get("rol")

    if rol == "administrador":
        return

    if rol == "tenant_admin":
        if payload.get("organizacion_id") != org_id:
            raise HTTPException(
                status_code=403,
                detail="No tienes acceso a esta organización"
            )
        return

    raise HTTPException(
        status_code=403,
        detail="Se requiere rol de administrador de organización"
    )


# ===================== HELPERS =====================

def get_org_id_default(db) -> int:
    """Retorna el organizacion_id de la organización principal (para talleres sin org explícita)."""
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE nombre = 'Organización Principal' LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=500,
                detail="Organización principal no encontrada. Ejecuta el script de migración."
            )
        return row[0]
    finally:
        cur.close()
