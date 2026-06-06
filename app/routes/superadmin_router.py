"""
ROUTER SUPER ADMINISTRACIÓN

Gestiona la plataforma completa como SuperAdministrador (rol: administrador).
Acceso exclusivo para usuarios con rol.nombre = 'administrador'.

Responsabilidades:
  - Login del SuperAdmin
  - Dashboard global de la plataforma
  - Gestión completa de organizaciones (tenants)
  - Gestión y asignación de talleres
  - Visualización de usuarios
  - KPIs globales
  - Bitácora de auditoría
"""

from fastapi import APIRouter, HTTPException, Header, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import bcrypt
import jwt
import json
import csv
import io

from ..services.config import Config
from ..classes.postgresql import Database
from ..utils.tenant_deps import get_token_payload
from ..utils.bitacora import log_bitacora


router = APIRouter(prefix="/api/superadmin", tags=["SuperAdministración"])


# ===================== HELPERS =====================

def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


def _require_superadmin(payload: dict) -> dict:
    """Lanza 403 si el caller no es SuperAdministrador."""
    if payload.get("rol") != "administrador":
        raise HTTPException(status_code=403, detail="Acceso exclusivo para SuperAdministrador")
    return payload


# Alias local para compatibilidad con el código existente
def _log_bitacora(cur, usuario_id, accion, tabla,
                  id_ref=None, descripcion=None, datos=None) -> None:
    log_bitacora(cur, usuario_id, accion, tabla, id_ref, descripcion, datos)


# ===================== MODELOS REQUEST =====================

class SuperAdminLogin(BaseModel):
    email: str
    password: str


class OrgCreate(BaseModel):
    nombre_organizacion: str
    descripcion: Optional[str] = None
    nit: Optional[str] = None
    email_contacto: Optional[str] = None
    telefono: Optional[str] = None
    plan: Optional[str] = "basico"


class OrgUpdate(BaseModel):
    nombre: Optional[str] = None
    descripcion: Optional[str] = None
    nit: Optional[str] = None
    email_contacto: Optional[str] = None
    telefono: Optional[str] = None
    plan: Optional[str] = None


class AsignarAdminRequest(BaseModel):
    nombre_admin: str
    email_admin: EmailStr
    password_admin: str
    telefono_admin: Optional[str] = None


class AsignarOrgRequest(BaseModel):
    organizacion_id: int


class SetEstadoTallerRequest(BaseModel):
    estado: str  # 'activo' | 'inactivo' | 'pendiente_asignacion'


# ===================== MODELOS RESPONSE =====================

class SuperAdminUser(BaseModel):
    usuario_id: int
    nombre: str
    email: str
    rol: str


class SuperAdminLoginResponse(BaseModel):
    success: bool
    access_token: str
    user: SuperAdminUser


# ===================== LOGIN =====================

@router.post("/login", response_model=SuperAdminLoginResponse)
async def login_superadmin(data: SuperAdminLogin, db=Depends(Database.get_db)):
    """Autentica al SuperAdministrador y retorna JWT."""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT u.usuario_id, u.contrasena_hash, u.nombre, u.email, u.estado,
                   r.nombre AS rol_nombre
            FROM usuario u
            INNER JOIN rol r ON u.rol_id = r.rol_id
            WHERE u.email = %s AND r.nombre = 'administrador'
            LIMIT 1
        """, (data.email.lower(),))
        user = cur.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        if not _verify_password(data.password, user["contrasena_hash"]):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        if user["estado"] != "activo":
            raise HTTPException(status_code=403, detail="Cuenta inactiva")

        cur.execute(
            "UPDATE usuario SET ultimo_acceso = CURRENT_TIMESTAMP WHERE usuario_id = %s",
            (user["usuario_id"],)
        )
        _log_bitacora(cur, user["usuario_id"], "LOGIN_SUPERADMIN", "usuario",
                      user["usuario_id"], f"Login superadmin: {user['email']}")
        db.commit()

        token_payload = {
            "sub": str(user["usuario_id"]),
            "organizacion_id": None,
            "taller_id": None,
            "rol": user["rol_nombre"],
            "email": user["email"],
            "exp": datetime.now(tz=timezone.utc) + timedelta(hours=24),
        }
        token = jwt.encode(token_payload, Config.SECRET_KEY, algorithm=Config.ALGORITHM)

        return SuperAdminLoginResponse(
            success=True,
            access_token=token,
            user=SuperAdminUser(
                usuario_id=user["usuario_id"],
                nombre=user["nombre"],
                email=user["email"],
                rol=user["rol_nombre"],
            ),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en login: {str(e)}")
    finally:
        cur.close()


# ===================== DASHBOARD GLOBAL =====================

@router.get("/dashboard")
async def dashboard_global(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """KPIs globales de toda la plataforma."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Totales básicos
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM organizacion)                                          AS total_organizaciones,
                (SELECT COUNT(*) FROM organizacion WHERE estado = 'activo')                  AS tenants_activos,
                (SELECT COUNT(*) FROM taller)                                                AS total_talleres,
                (SELECT COUNT(*) FROM taller WHERE disponible = TRUE)                        AS talleres_disponibles,
                (SELECT COUNT(*) FROM tecnico)                                               AS total_tecnicos,
                (SELECT COUNT(*) FROM usuario u JOIN rol r ON u.rol_id = r.rol_id
                 WHERE r.nombre = 'cliente')                                                 AS total_clientes,
                (SELECT COUNT(*) FROM incidente)                                             AS total_emergencias,
                (SELECT COUNT(*) FROM cotizacion)                                            AS total_cotizaciones,
                (SELECT COUNT(*) FROM pago WHERE estado = 'completado')                      AS total_pagos,
                (SELECT COALESCE(SUM(monto_total), 0) FROM pago WHERE estado = 'completado') AS ingresos_plataforma
        """)
        totales = cur.fetchone()

        # Talleres pendientes de asignación (columna estado puede no existir aún)
        talleres_pendientes = 0
        try:
            cur.execute("SELECT COUNT(*) AS p FROM taller WHERE estado = 'pendiente_asignacion'")
            talleres_pendientes = int(cur.fetchone()["p"] or 0)
        except Exception:
            pass

        # Top 5 talleres mejor calificados
        cur.execute("""
            SELECT t.taller_id, t.razon_social,
                   COALESCE(o.nombre, 'Sin organización') AS organizacion_nombre,
                   ROUND(AVG(c.puntuacion)::NUMERIC, 2)  AS calificacion_promedio,
                   COUNT(c.calificacion_id)              AS total_resenas
            FROM taller t
            LEFT JOIN calificacion c ON c.taller_id = t.taller_id
            LEFT JOIN organizacion o ON o.organizacion_id = t.organizacion_id
            GROUP BY t.taller_id, t.razon_social, o.nombre
            HAVING COUNT(c.calificacion_id) > 0
            ORDER BY calificacion_promedio DESC, total_resenas DESC
            LIMIT 5
        """)
        top_talleres = cur.fetchall()

        # Top 5 organizaciones más activas por incidentes
        cur.execute("""
            SELECT o.organizacion_id, o.nombre, o.plan, o.estado,
                   COUNT(DISTINCT a.incidente_id) AS total_incidentes,
                   COUNT(DISTINCT t.taller_id)    AS total_talleres
            FROM organizacion o
            LEFT JOIN taller t ON t.organizacion_id = o.organizacion_id
            LEFT JOIN asignacion a ON a.taller_id = t.taller_id
            GROUP BY o.organizacion_id, o.nombre, o.plan, o.estado
            ORDER BY total_incidentes DESC
            LIMIT 5
        """)
        top_orgs = cur.fetchall()

        # SLA global y tiempos promedio
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)                   AS sla_evaluados,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60 <= 15
                )                                                                         AS sla_cumplidos,
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60
                    END
                )::NUMERIC, 2)                                                            AS prom_asignacion_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_inicio_servicio IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_inicio_servicio - a.fecha_aceptacion)) / 60
                    END
                )::NUMERIC, 2)                                                            AS prom_llegada_min
            FROM asignacion a
            WHERE a.fecha_asignacion IS NOT NULL
        """)
        sla = cur.fetchone()

        sla_ev = int(sla["sla_evaluados"] or 0)
        sla_cu = int(sla["sla_cumplidos"] or 0)
        sla_pct = round(sla_cu * 100.0 / sla_ev, 1) if sla_ev > 0 else None

        return {
            "success": True,
            "totales": {
                "organizaciones":                int(totales["total_organizaciones"] or 0),
                "tenants_activos":               int(totales["tenants_activos"] or 0),
                "talleres":                      int(totales["total_talleres"] or 0),
                "talleres_disponibles":          int(totales["talleres_disponibles"] or 0),
                "talleres_pendientes_asignacion": talleres_pendientes,
                "tecnicos":                      int(totales["total_tecnicos"] or 0),
                "clientes":                      int(totales["total_clientes"] or 0),
                "emergencias":                   int(totales["total_emergencias"] or 0),
                "cotizaciones":                  int(totales["total_cotizaciones"] or 0),
                "pagos_completados":             int(totales["total_pagos"] or 0),
                "ingresos_plataforma":           float(totales["ingresos_plataforma"] or 0),
            },
            "sla": {
                "cumplimiento_pct":   sla_pct,
                "evaluados":          sla_ev,
                "cumplidos":          sla_cu,
                "prom_asignacion_min": float(sla["prom_asignacion_min"]) if sla["prom_asignacion_min"] else None,
                "prom_llegada_min":   float(sla["prom_llegada_min"])   if sla["prom_llegada_min"]   else None,
            },
            "top_talleres_calificados": [
                {
                    "taller_id":    r["taller_id"],
                    "razon_social": r["razon_social"],
                    "organizacion": r["organizacion_nombre"],
                    "calificacion": float(r["calificacion_promedio"] or 0),
                    "resenas":      int(r["total_resenas"] or 0),
                }
                for r in top_talleres
            ],
            "top_organizaciones_activas": [
                {
                    "organizacion_id": r["organizacion_id"],
                    "nombre":          r["nombre"],
                    "plan":            r["plan"],
                    "estado":          r["estado"],
                    "incidentes":      int(r["total_incidentes"] or 0),
                    "talleres":        int(r["total_talleres"] or 0),
                }
                for r in top_orgs
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en dashboard: {str(e)}")
    finally:
        cur.close()


# ===================== GESTIÓN DE ORGANIZACIONES =====================

@router.get("/organizaciones")
async def listar_organizaciones(
    estado: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todas las organizaciones de la plataforma."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        where_parts = []
        params: list = []
        if estado:
            where_parts.append("o.estado = %s")
            params.append(estado)

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        cur.execute(f"""
            SELECT
                o.organizacion_id, o.nombre, o.descripcion, o.nit,
                o.email_contacto, o.telefono, o.plan, o.estado,
                o.creado_en, o.actualizado_en,
                COUNT(DISTINCT t.taller_id)   AS total_talleres,
                COUNT(DISTINCT tec.tecnico_id) AS total_tecnicos,
                (SELECT u2.nombre FROM usuario u2 JOIN rol r2 ON u2.rol_id = r2.rol_id
                 WHERE u2.organizacion_id = o.organizacion_id AND r2.nombre = 'tenant_admin'
                 LIMIT 1)                     AS admin_nombre,
                (SELECT u2.email FROM usuario u2 JOIN rol r2 ON u2.rol_id = r2.rol_id
                 WHERE u2.organizacion_id = o.organizacion_id AND r2.nombre = 'tenant_admin'
                 LIMIT 1)                     AS admin_email
            FROM organizacion o
            LEFT JOIN taller t    ON t.organizacion_id = o.organizacion_id
            LEFT JOIN tecnico tec ON tec.taller_id = t.taller_id
            {where_clause}
            GROUP BY o.organizacion_id
            ORDER BY o.creado_en DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        count_query = f"SELECT COUNT(*) AS total FROM organizacion o {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()["total"]

        return {
            "success": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/organizaciones/{org_id}")
async def get_organizacion(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Detalle de una organización con KPIs y talleres."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM organizacion WHERE organizacion_id = %s", (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        cur.execute("SELECT * FROM vista_kpis_organizacion WHERE organizacion_id = %s", (org_id,))
        kpis = cur.fetchone()

        cur.execute("""
            SELECT u.usuario_id, u.nombre, u.email, u.telefono, u.estado, u.ultimo_acceso
            FROM usuario u JOIN rol r ON u.rol_id = r.rol_id
            WHERE u.organizacion_id = %s AND r.nombre = 'tenant_admin'
        """, (org_id,))
        admins = cur.fetchall()

        cur.execute("""
            SELECT t.taller_id, t.razon_social, t.disponible,
                   t.calificacion_promedio, COALESCE(t.estado, 'activo') AS estado
            FROM taller t WHERE t.organizacion_id = %s ORDER BY t.razon_social
        """, (org_id,))
        talleres = cur.fetchall()

        return {
            "success": True,
            "organizacion": dict(org),
            "kpis": dict(kpis) if kpis else {},
            "admins": [dict(a) for a in admins],
            "talleres": [dict(t) for t in talleres],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/organizaciones", status_code=201)
async def crear_organizacion(
    data: OrgCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea una nueva organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE nombre = %s LIMIT 1",
            (data.nombre_organizacion.upper(),)
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya existe una organización con ese nombre")

        cur.execute("""
            INSERT INTO organizacion (nombre, descripcion, nit, email_contacto, telefono, plan, estado)
            VALUES (%s, %s, %s, %s, %s, %s, 'activo')
            RETURNING organizacion_id
        """, (
            data.nombre_organizacion.upper(),
            data.descripcion,
            data.nit,
            data.email_contacto,
            data.telefono,
            data.plan or "basico",
        ))
        org_id = cur.fetchone()["organizacion_id"]

        _log_bitacora(cur, sa_id, "CREAR_ORGANIZACION", "organizacion", org_id,
                      f"Organización creada: {data.nombre_organizacion}")
        db.commit()
        return {"success": True, "message": "Organización creada exitosamente", "organizacion_id": org_id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.put("/organizaciones/{org_id}")
async def editar_organizacion(
    org_id: int,
    data: OrgUpdate,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Edita los datos de una organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM organizacion WHERE organizacion_id = %s", (org_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        updates: dict = {}
        if data.nombre is not None:
            updates["nombre"] = data.nombre.upper()
        if data.descripcion is not None:
            updates["descripcion"] = data.descripcion
        if data.nit is not None:
            updates["nit"] = data.nit
        if data.email_contacto is not None:
            updates["email_contacto"] = data.email_contacto
        if data.telefono is not None:
            updates["telefono"] = data.telefono
        if data.plan is not None:
            updates["plan"] = data.plan

        if updates:
            updates["actualizado_en"] = datetime.now(tz=timezone.utc)
            set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
            cur.execute(
                f"UPDATE organizacion SET {set_clause} WHERE organizacion_id = %s",
                list(updates.values()) + [org_id]
            )

        _log_bitacora(cur, sa_id, "EDITAR_ORGANIZACION", "organizacion", org_id,
                      f"Organización editada", updates)
        db.commit()
        return {"success": True, "message": "Organización actualizada"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.patch("/organizaciones/{org_id}/estado")
async def toggle_estado_organizacion(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Activa o desactiva una organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE organizacion
            SET estado = CASE WHEN estado = 'activo' THEN 'inactivo' ELSE 'activo' END,
                actualizado_en = CURRENT_TIMESTAMP
            WHERE organizacion_id = %s
            RETURNING organizacion_id, nombre, estado
        """, (org_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        _log_bitacora(cur, sa_id, f"CAMBIO_ESTADO_ORGANIZACION", "organizacion", org_id,
                      f"Estado cambiado a {row['estado']}")
        db.commit()
        return {"success": True, "organizacion_id": row["organizacion_id"],
                "nombre": row["nombre"], "estado": row["estado"]}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/organizaciones/{org_id}/asignar-admin", status_code=201)
async def asignar_admin_organizacion(
    org_id: int,
    data: AsignarAdminRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea y asigna un tenant_admin a la organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE organizacion_id = %s AND estado = 'activo'",
            (org_id,)
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Organización no encontrada o inactiva")

        cur.execute("SELECT usuario_id FROM usuario WHERE email = %s LIMIT 1", (data.email_admin.lower(),))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="El correo ya está registrado")

        cur.execute("SELECT rol_id FROM rol WHERE nombre = 'tenant_admin' LIMIT 1")
        rol_row = cur.fetchone()
        if not rol_row:
            raise HTTPException(status_code=500, detail="Rol tenant_admin no encontrado")

        password_hash = _hash_password(data.password_admin)
        cur.execute("""
            INSERT INTO usuario (rol_id, nombre, email, telefono, contrasena_hash, estado, organizacion_id)
            VALUES (%s, %s, %s, %s, %s, 'activo', %s)
            RETURNING usuario_id
        """, (
            rol_row["rol_id"],
            data.nombre_admin.upper(),
            data.email_admin.lower(),
            data.telefono_admin,
            password_hash,
            org_id,
        ))
        nuevo_id = cur.fetchone()["usuario_id"]

        _log_bitacora(cur, sa_id, "ASIGNAR_ADMIN_ORGANIZACION", "usuario", nuevo_id,
                      f"Tenant admin creado para org {org_id}: {data.email_admin}")
        db.commit()
        return {"success": True, "message": "Administrador de organización creado", "usuario_id": nuevo_id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== GESTIÓN DE TALLERES =====================

@router.get("/talleres")
async def listar_talleres(
    estado: Optional[str] = None,
    org_id: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todos los talleres de la plataforma."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        where_parts = []
        params: list = []

        if estado:
            where_parts.append("COALESCE(t.estado, 'activo') = %s")
            params.append(estado)
        if org_id:
            where_parts.append("t.organizacion_id = %s")
            params.append(org_id)

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        cur.execute(f"""
            SELECT
                t.taller_id, t.razon_social, t.direccion, t.disponible,
                t.organizacion_id, t.calificacion_promedio,
                TO_CHAR(t.horario_inicio, 'HH24:MI') AS horario_inicio,
                TO_CHAR(t.horario_fin, 'HH24:MI')   AS horario_fin,
                COALESCE(t.estado, 'activo')         AS estado,
                t.creado_en,
                COALESCE(o.nombre, 'Sin organización') AS organizacion_nombre,
                u.nombre  AS nombre_contacto,
                u.email,
                u.telefono,
                COUNT(DISTINCT tec.tecnico_id) AS total_tecnicos
            FROM taller t
            LEFT JOIN organizacion o  ON o.organizacion_id = t.organizacion_id
            LEFT JOIN usuario u       ON u.usuario_id = t.usuario_id
            LEFT JOIN tecnico tec     ON tec.taller_id = t.taller_id
            {where_clause}
            GROUP BY t.taller_id, t.razon_social, t.direccion, t.disponible,
                     t.organizacion_id, t.calificacion_promedio, t.horario_inicio,
                     t.horario_fin, t.estado, t.creado_en,
                     o.nombre, u.nombre, u.email, u.telefono
            ORDER BY t.creado_en DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) AS total FROM taller t {where_clause}", params)
        total = cur.fetchone()["total"]

        return {
            "success": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.patch("/talleres/{taller_id}/asignar-org")
async def asignar_taller_org(
    taller_id: int,
    data: AsignarOrgRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Asigna o reasigna un taller a una organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT taller_id, razon_social, organizacion_id FROM taller WHERE taller_id = %s",
            (taller_id,)
        )
        taller = cur.fetchone()
        if not taller:
            raise HTTPException(status_code=404, detail="Taller no encontrado")

        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE organizacion_id = %s AND estado = 'activo'",
            (data.organizacion_id,)
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Organización no encontrada o inactiva")

        org_anterior = taller["organizacion_id"]

        cur.execute("""
            UPDATE taller SET organizacion_id = %s, estado = 'activo'
            WHERE taller_id = %s
        """, (data.organizacion_id, taller_id))

        # Sincronizar organizacion_id del usuario del taller
        cur.execute("""
            UPDATE usuario SET organizacion_id = %s
            WHERE usuario_id = (SELECT usuario_id FROM taller WHERE taller_id = %s)
        """, (data.organizacion_id, taller_id))

        accion = "ASIGNAR_TALLER_ORG" if org_anterior is None else "REASIGNAR_TALLER_ORG"
        _log_bitacora(cur, sa_id, accion, "taller", taller_id,
                      f"Taller '{taller['razon_social']}' asignado a org {data.organizacion_id}",
                      {"org_anterior": org_anterior, "org_nueva": data.organizacion_id})
        db.commit()
        return {
            "success": True,
            "taller_id": taller_id,
            "organizacion_id": data.organizacion_id,
            "message": f"Taller '{taller['razon_social']}' asignado correctamente",
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.patch("/talleres/{taller_id}/estado")
async def set_estado_taller(
    taller_id: int,
    data: SetEstadoTallerRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Cambia el estado administrativo de un taller."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    estados_validos = ("activo", "inactivo", "pendiente_asignacion")
    if data.estado not in estados_validos:
        raise HTTPException(
            status_code=400,
            detail=f"Estado inválido. Valores permitidos: {', '.join(estados_validos)}"
        )

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE taller SET estado = %s WHERE taller_id = %s
            RETURNING taller_id, razon_social, estado
        """, (data.estado, taller_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Taller no encontrado")

        _log_bitacora(cur, sa_id, "CAMBIO_ESTADO_TALLER", "taller", taller_id,
                      f"Estado cambiado a {data.estado}")
        db.commit()
        return {"success": True, "taller_id": row["taller_id"],
                "razon_social": row["razon_social"], "estado": row["estado"]}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== GESTIÓN DE USUARIOS =====================

@router.get("/usuarios")
async def listar_usuarios(
    rol: Optional[str] = None,
    estado: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista usuarios de la plataforma filtrados por rol."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        where_parts = ["r.nombre != 'administrador'"]
        params: list = []

        if rol:
            where_parts.append("r.nombre = %s")
            params.append(rol)
        if estado:
            where_parts.append("u.estado = %s")
            params.append(estado)

        where_clause = "WHERE " + " AND ".join(where_parts)

        cur.execute(f"""
            SELECT
                u.usuario_id, u.nombre, u.email, u.telefono, u.estado,
                u.documento_identidad, u.fecha_registro, u.ultimo_acceso,
                u.organizacion_id,
                r.nombre AS rol,
                COALESCE(o.nombre, 'Sin organización') AS organizacion_nombre,
                CASE
                    WHEN r.nombre = 'taller'
                    THEN (SELECT t.razon_social FROM taller t WHERE t.usuario_id = u.usuario_id LIMIT 1)
                    WHEN r.nombre = 'tecnico'
                    THEN (SELECT tec.especialidad FROM tecnico tec WHERE tec.usuario_id = u.usuario_id LIMIT 1)
                    ELSE NULL
                END AS info_extra
            FROM usuario u
            INNER JOIN rol r ON u.rol_id = r.rol_id
            LEFT JOIN organizacion o ON o.organizacion_id = u.organizacion_id
            {where_clause}
            ORDER BY u.fecha_registro DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS total FROM usuario u
            INNER JOIN rol r ON u.rol_id = r.rol_id
            {where_clause}
        """, params)
        total = cur.fetchone()["total"]

        return {
            "success": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


class UserCreateRequest(BaseModel):
    nombre: str
    email: EmailStr
    password: str
    telefono: Optional[str] = None
    rol_nombre: str
    organizacion_id: Optional[int] = None
    documento_identidad: Optional[str] = None


class UserUpdateRequest(BaseModel):
    nombre: Optional[str] = None
    email: Optional[EmailStr] = None
    telefono: Optional[str] = None
    rol_nombre: Optional[str] = None
    organizacion_id: Optional[int] = None
    documento_identidad: Optional[str] = None


@router.get("/usuarios/{usuario_id}")
async def get_usuario_detalle(
    usuario_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Detalle completo de un usuario."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                u.usuario_id, u.nombre, u.email, u.telefono, u.estado,
                u.documento_identidad, u.fecha_registro, u.ultimo_acceso,
                u.organizacion_id,
                r.nombre AS rol, r.rol_id,
                COALESCE(o.nombre, 'Sin organización') AS organizacion_nombre,
                CASE
                    WHEN r.nombre = 'taller'
                    THEN (SELECT t.razon_social FROM taller t WHERE t.usuario_id = u.usuario_id LIMIT 1)
                    WHEN r.nombre = 'tecnico'
                    THEN (SELECT tec.especialidad FROM tecnico tec WHERE tec.usuario_id = u.usuario_id LIMIT 1)
                    ELSE NULL
                END AS info_extra
            FROM usuario u
            INNER JOIN rol r ON u.rol_id = r.rol_id
            LEFT JOIN organizacion o ON o.organizacion_id = u.organizacion_id
            WHERE u.usuario_id = %s AND r.nombre != 'administrador'
        """, (usuario_id,))
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return {"success": True, "data": dict(user)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/usuarios", status_code=201)
async def crear_usuario(
    data: UserCreateRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea un nuevo usuario en la plataforma."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    if data.rol_nombre == "administrador":
        raise HTTPException(status_code=400, detail="No se puede crear un usuario con rol administrador")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT usuario_id FROM usuario WHERE email = %s LIMIT 1", (data.email.lower(),))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="El correo ya está registrado")

        cur.execute("SELECT rol_id FROM rol WHERE nombre = %s LIMIT 1", (data.rol_nombre,))
        rol_row = cur.fetchone()
        if not rol_row:
            raise HTTPException(status_code=400, detail=f"Rol '{data.rol_nombre}' no encontrado")

        password_hash = _hash_password(data.password)
        cur.execute("""
            INSERT INTO usuario
                (rol_id, nombre, email, telefono, contrasena_hash, estado, organizacion_id, documento_identidad)
            VALUES (%s, %s, %s, %s, %s, 'activo', %s, %s)
            RETURNING usuario_id
        """, (
            rol_row["rol_id"], data.nombre.upper(), data.email.lower(),
            data.telefono, password_hash, data.organizacion_id, data.documento_identidad,
        ))
        nuevo_id = cur.fetchone()["usuario_id"]

        _log_bitacora(cur, sa_id, "CREAR_USUARIO", "usuario", nuevo_id,
                      f"Usuario creado: {data.email} con rol {data.rol_nombre}")
        db.commit()
        return {"success": True, "message": "Usuario creado exitosamente", "usuario_id": nuevo_id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.put("/usuarios/{usuario_id}")
async def editar_usuario(
    usuario_id: int,
    data: UserUpdateRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Edita los datos de un usuario."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT u.usuario_id FROM usuario u JOIN rol r ON u.rol_id = r.rol_id
            WHERE u.usuario_id = %s AND r.nombre != 'administrador'
        """, (usuario_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Usuario no encontrado")

        updates: dict = {}
        if data.nombre is not None:
            updates["nombre"] = data.nombre.upper()
        if data.email is not None:
            cur.execute(
                "SELECT usuario_id FROM usuario WHERE email = %s AND usuario_id != %s LIMIT 1",
                (data.email.lower(), usuario_id)
            )
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="El correo ya está en uso")
            updates["email"] = data.email.lower()
        if data.telefono is not None:
            updates["telefono"] = data.telefono
        if data.organizacion_id is not None:
            updates["organizacion_id"] = data.organizacion_id
        if data.documento_identidad is not None:
            updates["documento_identidad"] = data.documento_identidad
        if data.rol_nombre is not None:
            if data.rol_nombre == "administrador":
                raise HTTPException(status_code=400, detail="No se puede asignar ese rol")
            cur.execute("SELECT rol_id FROM rol WHERE nombre = %s LIMIT 1", (data.rol_nombre,))
            rol_row = cur.fetchone()
            if not rol_row:
                raise HTTPException(status_code=400, detail=f"Rol '{data.rol_nombre}' no encontrado")
            updates["rol_id"] = rol_row["rol_id"]

        if updates:
            set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
            cur.execute(
                f"UPDATE usuario SET {set_clause} WHERE usuario_id = %s",
                list(updates.values()) + [usuario_id]
            )

        _log_bitacora(cur, sa_id, "EDITAR_USUARIO", "usuario", usuario_id, "Usuario editado", updates)
        db.commit()
        return {"success": True, "message": "Usuario actualizado correctamente"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.patch("/usuarios/{usuario_id}/estado")
async def toggle_estado_usuario(
    usuario_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Activa o desactiva un usuario."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT u.usuario_id, r.nombre AS rol FROM usuario u
            JOIN rol r ON u.rol_id = r.rol_id
            WHERE u.usuario_id = %s
        """, (usuario_id,))
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        if user["rol"] == "administrador":
            raise HTTPException(status_code=403, detail="No se puede modificar el SuperAdministrador")

        cur.execute("""
            UPDATE usuario
            SET estado = CASE WHEN estado = 'activo' THEN 'inactivo' ELSE 'activo' END
            WHERE usuario_id = %s
            RETURNING usuario_id, nombre, estado
        """, (usuario_id,))
        row = cur.fetchone()

        _log_bitacora(cur, sa_id, "CAMBIO_ESTADO_USUARIO", "usuario", usuario_id,
                      f"Estado cambiado a {row['estado']}")
        db.commit()
        return {"success": True, "usuario_id": row["usuario_id"],
                "nombre": row["nombre"], "estado": row["estado"]}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== KPIs GLOBALES =====================

@router.get("/kpis/organizaciones")
async def kpis_organizaciones(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """KPIs detallados por organización."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM vista_kpis_organizacion ORDER BY total_incidentes DESC")
        rows = cur.fetchall()

        # Tiempos y SLA por organización
        cur.execute("""
            SELECT
                t.organizacion_id,
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60 END
                )::NUMERIC, 2)                                                           AS prom_asignacion_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_inicio_servicio IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_inicio_servicio - a.fecha_aceptacion)) / 60 END
                )::NUMERIC, 2)                                                           AS prom_llegada_min,
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)                   AS sla_evaluados,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60 <= 15
                )                                                                        AS sla_cumplidos,
                COUNT(DISTINCT CASE WHEN i.estado = 'cancelado' THEN i.incidente_id END) AS cancelados
            FROM asignacion a
            JOIN taller t    ON t.taller_id = a.taller_id
            JOIN incidente i ON i.incidente_id = a.incidente_id
            GROUP BY t.organizacion_id
        """)
        tiempos_map = {r["organizacion_id"]: r for r in cur.fetchall()}

        result = []
        for r in rows:
            org_id = r["organizacion_id"]
            t = tiempos_map.get(org_id, {})
            sla_ev = int(t.get("sla_evaluados") or 0) if t else 0
            sla_cu = int(t.get("sla_cumplidos") or 0) if t else 0
            entry = dict(r)
            entry["prom_asignacion_min"]  = float(t.get("prom_asignacion_min") or 0) if t else None
            entry["prom_llegada_min"]     = float(t.get("prom_llegada_min") or 0)    if t else None
            entry["sla_cumplimiento_pct"] = round(sla_cu * 100.0 / sla_ev, 1)       if sla_ev > 0 else None
            entry["casos_cancelados"]     = int(t.get("cancelados") or 0)            if t else 0
            result.append(entry)

        return {"success": True, "total": len(result), "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/kpis/talleres")
async def kpis_talleres(
    org_id: Optional[int] = None,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """KPIs detallados por taller."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        where_parts: list = []
        params: list = []
        if org_id:
            where_parts.append("t.organizacion_id = %s")
            params.append(org_id)
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        cur.execute(f"""
            SELECT
                t.taller_id, t.razon_social, t.organizacion_id,
                COALESCE(o.nombre, 'Sin organización') AS organizacion_nombre,
                COALESCE(t.estado, 'activo')           AS estado,
                COALESCE(AVG(c.puntuacion), 0)         AS calificacion_promedio,
                COUNT(DISTINCT tec.tecnico_id)         AS total_tecnicos,
                COUNT(DISTINCT CASE WHEN a.estado = 'completada' THEN a.asignacion_id END) AS servicios_completados,
                COUNT(DISTINCT a.incidente_id)         AS total_incidentes,
                COALESCE(SUM(CASE WHEN p.estado = 'completado' THEN p.monto_taller ELSE 0 END), 0) AS ingresos_totales,
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60 END
                )::NUMERIC, 2) AS prom_respuesta_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_cierre_servicio IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (
                        a.fecha_cierre_servicio - COALESCE(a.fecha_inicio_servicio, a.fecha_asignacion)
                    )) / 60 END
                )::NUMERIC, 2) AS prom_resolucion_min,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_aceptacion - a.fecha_asignacion)) / 60 <= 15
                )                                                                      AS sla_cumplidos,
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)                 AS sla_evaluados
            FROM taller t
            LEFT JOIN organizacion o  ON o.organizacion_id = t.organizacion_id
            LEFT JOIN tecnico tec     ON tec.taller_id = t.taller_id
            LEFT JOIN calificacion c  ON c.taller_id = t.taller_id
            LEFT JOIN asignacion a    ON a.taller_id = t.taller_id
            LEFT JOIN incidente i     ON i.incidente_id = a.incidente_id
            LEFT JOIN pago p          ON p.asignacion_id = a.asignacion_id
            {where_clause}
            GROUP BY t.taller_id, t.razon_social, t.organizacion_id, o.nombre, t.estado
            ORDER BY total_incidentes DESC NULLS LAST
        """, params)
        rows = cur.fetchall()

        result = []
        for r in rows:
            entry = dict(r)
            sla_ev = int(r["sla_evaluados"] or 0)
            sla_cu = int(r["sla_cumplidos"] or 0)
            entry["sla_cumplimiento_pct"] = round(sla_cu * 100.0 / sla_ev, 1) if sla_ev > 0 else None
            result.append(entry)

        return {"success": True, "total": len(result), "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== BITÁCORA =====================

@router.get("/bitacora")
async def listar_bitacora(
    accion: Optional[str] = None,
    tabla: Optional[str] = None,
    usuario_id: Optional[int] = None,
    fecha: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista la bitácora global de auditoría (solo SuperAdmin)."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        where_parts: list = []
        params: list = []

        if accion:
            where_parts.append("b.accion ILIKE %s")
            params.append(f"%{accion}%")
        if tabla:
            where_parts.append("b.tabla_afectada = %s")
            params.append(tabla)
        if usuario_id:
            where_parts.append("b.usuario_id = %s")
            params.append(usuario_id)
        if fecha:
            where_parts.append("b.fecha::date = %s")
            params.append(fecha)
        if fecha_desde:
            where_parts.append("b.fecha >= %s")
            params.append(fecha_desde)
        if fecha_hasta:
            where_parts.append("b.fecha <= %s")
            params.append(fecha_hasta)

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        cur.execute(f"""
            SELECT
                b.bitacora_id, b.usuario_id, b.accion, b.tabla_afectada,
                b.id_referencia, b.descripcion, b.datos_cambio, b.ip_origen, b.fecha,
                COALESCE(u.nombre, 'Sistema') AS usuario_nombre,
                u.email                       AS usuario_email,
                r.nombre                      AS usuario_rol
            FROM bitacora b
            LEFT JOIN usuario u ON u.usuario_id = b.usuario_id
            LEFT JOIN rol r     ON r.rol_id = u.rol_id
            {where_clause}
            ORDER BY b.fecha DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) AS total FROM bitacora b {where_clause}", params)
        total = cur.fetchone()["total"]

        return {
            "success": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== COPIAS DE SEGURIDAD =====================

def _ensure_backup_tables(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backup_config (
            config_id       SERIAL PRIMARY KEY,
            activo          BOOLEAN NOT NULL DEFAULT TRUE,
            frecuencia      VARCHAR(20) NOT NULL DEFAULT 'diario',
            hora            VARCHAR(5)  NOT NULL DEFAULT '02:00',
            retencion_dias  INTEGER     NOT NULL DEFAULT 30,
            actualizado_por INTEGER REFERENCES usuario(usuario_id),
            actualizado_en  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backup_historial (
            historial_id  SERIAL PRIMARY KEY,
            tipo          VARCHAR(20)  NOT NULL,
            estado        VARCHAR(30)  NOT NULL DEFAULT 'completado',
            usuario_id    INTEGER REFERENCES usuario(usuario_id),
            fecha         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            nombre_archivo VARCHAR(255),
            notas         TEXT
        )
    """)


class BackupConfigRequest(BaseModel):
    activo: bool = True
    frecuencia: str = "diario"
    hora: str = "02:00"
    retencion_dias: int = 30


@router.get("/backup/csv")
async def descargar_backup_csv(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Genera y descarga un CSV con los datos principales del sistema."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_backup_tables(cur)

        output = io.StringIO()
        writer = csv.writer(output)

        tables = [
            (
                "organizacion",
                "SELECT organizacion_id, nombre, descripcion, nit, email_contacto, telefono, plan, estado, creado_en "
                "FROM organizacion ORDER BY organizacion_id",
            ),
            (
                "taller",
                "SELECT taller_id, razon_social, direccion, disponible, calificacion_promedio, "
                "COALESCE(estado,'activo') AS estado, creado_en FROM taller ORDER BY taller_id",
            ),
            (
                "usuario",
                "SELECT u.usuario_id, u.nombre, u.email, u.telefono, u.estado, u.fecha_registro, r.nombre AS rol "
                "FROM usuario u JOIN rol r ON u.rol_id = r.rol_id "
                "WHERE r.nombre != 'administrador' ORDER BY u.usuario_id",
            ),
            (
                "tecnico",
                "SELECT tecnico_id, nombre, especialidad, disponible, creado_en "
                "FROM tecnico ORDER BY tecnico_id",
            ),
            (
                "incidente",
                "SELECT incidente_id, descripcion, tipo_problema, estado, prioridad, latitud, longitud "
                "FROM incidente ORDER BY incidente_id",
            ),
            (
                "asignacion",
                "SELECT asignacion_id, incidente_id, taller_id, tecnico_id, estado, "
                "fecha_asignacion, fecha_aceptacion, fecha_cierre_servicio "
                "FROM asignacion ORDER BY asignacion_id",
            ),
            (
                "cotizacion",
                "SELECT cotizacion_id, incidente_id, taller_id, costo_estimado, tiempo_estimado, observaciones, estado, fecha_creacion "
                "FROM cotizacion ORDER BY cotizacion_id",
            ),
            (
                "pago",
                "SELECT pago_id, asignacion_id, monto_total, monto_taller, estado, creado_en "
                "FROM pago ORDER BY pago_id",
            ),
        ]

        for table_name, query in tables:
            writer.writerow([f"=== TABLA: {table_name.upper()} ==="])
            try:
                cur.execute(query)
                rows = cur.fetchall()
                if rows:
                    writer.writerow(list(rows[0].keys()))
                    for row in rows:
                        writer.writerow([str(v) if v is not None else "" for v in row.values()])
                else:
                    writer.writerow(["(sin datos)"])
            except Exception as table_err:
                db.rollback()
                writer.writerow([f"ERROR al leer tabla: {str(table_err)}"])
            writer.writerow([])

        now_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"backup_{now_str}.csv"

        cur.execute(
            "INSERT INTO backup_historial (tipo, estado, usuario_id, nombre_archivo) VALUES ('manual', 'completado', %s, %s)",
            (sa_id, filename),
        )
        _log_bitacora(cur, sa_id, "BACKUP_MANUAL", "sistema", None,
                      f"Respaldo manual descargado: {filename}")
        db.commit()

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error generando backup: {str(e)}")
    finally:
        cur.close()


@router.get("/backup/config")
async def get_backup_config(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Obtiene la configuración actual de respaldo automático."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_backup_tables(cur)
        db.commit()
        cur.execute("SELECT * FROM backup_config ORDER BY config_id DESC LIMIT 1")
        config = cur.fetchone()
        return {"success": True, "config": dict(config) if config else None}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/backup/config")
async def save_backup_config(
    data: BackupConfigRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Guarda la configuración de respaldo automático."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    frecuencias_validas = ("diario", "semanal", "mensual")
    if data.frecuencia not in frecuencias_validas:
        raise HTTPException(status_code=400, detail="Frecuencia inválida")
    if data.retencion_dias not in (7, 15, 30):
        raise HTTPException(status_code=400, detail="Retención debe ser 7, 15 o 30 días")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_backup_tables(cur)
        cur.execute("DELETE FROM backup_config")
        cur.execute(
            "INSERT INTO backup_config (activo, frecuencia, hora, retencion_dias, actualizado_por, actualizado_en) "
            "VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING config_id",
            (data.activo, data.frecuencia, data.hora, data.retencion_dias, sa_id),
        )
        config_id = cur.fetchone()["config_id"]

        if data.activo:
            cur.execute(
                "INSERT INTO backup_historial (tipo, estado, usuario_id, notas) VALUES ('automatico', 'programado', %s, %s)",
                (sa_id, f"Programado: {data.frecuencia} a las {data.hora}, retención {data.retencion_dias} días"),
            )

        _log_bitacora(cur, sa_id, "CONFIGURAR_BACKUP", "sistema", None,
                      f"Backup automático configurado: {data.frecuencia} a las {data.hora}, retención {data.retencion_dias}d")
        db.commit()
        return {
            "success": True,
            "message": "Configuración de respaldo automático guardada correctamente.",
            "config_id": config_id,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/backup/historial")
async def get_backup_historial(
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista el historial de respaldos."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_backup_tables(cur)
        db.commit()
        cur.execute("""
            SELECT h.historial_id, h.tipo, h.estado, h.fecha, h.nombre_archivo, h.notas,
                   COALESCE(u.nombre, 'Sistema') AS usuario_nombre
            FROM backup_historial h
            LEFT JOIN usuario u ON u.usuario_id = h.usuario_id
            ORDER BY h.fecha DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        rows = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS total FROM backup_historial")
        total = cur.fetchone()["total"]

        return {"success": True, "total": total, "data": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


# ===================== ROLES Y PERMISOS =====================

def _ensure_roles_tables(cur) -> None:
    """Agrega columnas a rol si no existen y crea tablas de permisos."""
    cur.execute("ALTER TABLE rol ADD COLUMN IF NOT EXISTS descripcion TEXT")
    cur.execute("ALTER TABLE rol ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE")
    cur.execute("ALTER TABLE rol ADD COLUMN IF NOT EXISTS es_personalizado BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS permiso (
            permiso_id  SERIAL PRIMARY KEY,
            codigo      VARCHAR(100) NOT NULL UNIQUE,
            nombre      VARCHAR(200) NOT NULL,
            descripcion TEXT,
            modulo      VARCHAR(100),
            activo      BOOLEAN NOT NULL DEFAULT TRUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rol_permiso (
            rol_id     INTEGER NOT NULL REFERENCES rol(rol_id) ON DELETE CASCADE,
            permiso_id INTEGER NOT NULL REFERENCES permiso(permiso_id) ON DELETE CASCADE,
            PRIMARY KEY (rol_id, permiso_id)
        )
    """)
    permisos_default = [
        ("ver_dashboard",            "Ver Dashboard Global",           "Dashboard"),
        ("gestionar_organizaciones", "Gestionar Organizaciones",       "Organizaciones"),
        ("ver_organizaciones",       "Ver Organizaciones",             "Organizaciones"),
        ("gestionar_talleres",       "Gestionar Talleres",             "Talleres"),
        ("ver_talleres",             "Ver Talleres",                   "Talleres"),
        ("gestionar_usuarios",       "Gestionar Usuarios",             "Usuarios"),
        ("ver_usuarios",             "Ver Usuarios",                   "Usuarios"),
        ("ver_kpis",                 "Ver KPIs Globales",              "KPIs"),
        ("ver_bitacora",             "Ver Bitácora",                   "Auditoría"),
        ("gestionar_backup",         "Gestionar Copias de Seguridad",  "Backup"),
        ("ver_reportes",             "Ver Reportes",                   "Reportes"),
        ("ver_incidentes",           "Ver Incidentes",                 "Incidentes"),
        ("gestionar_incidentes",     "Gestionar Incidentes",           "Incidentes"),
        ("ver_cotizaciones",         "Ver Cotizaciones",               "Cotizaciones"),
        ("gestionar_cotizaciones",   "Gestionar Cotizaciones",         "Cotizaciones"),
        ("ver_pagos",                "Ver Pagos",                      "Pagos"),
        ("gestionar_pagos",          "Gestionar Pagos",                "Pagos"),
        ("gestionar_roles",          "Gestionar Roles y Permisos",     "Administración"),
    ]
    for codigo, nombre, modulo in permisos_default:
        cur.execute(
            "INSERT INTO permiso (codigo, nombre, modulo) VALUES (%s, %s, %s) ON CONFLICT (codigo) DO NOTHING",
            (codigo, nombre, modulo)
        )


class RolCreateRequest(BaseModel):
    nombre: str
    descripcion: Optional[str] = None


class RolUpdateRequest(BaseModel):
    nombre: Optional[str] = None
    descripcion: Optional[str] = None


class RolPermisosRequest(BaseModel):
    permiso_ids: List[int]


@router.get("/roles")
async def listar_roles(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todos los roles con conteo de usuarios."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute("""
            SELECT
                r.rol_id, r.nombre,
                COALESCE(r.descripcion, '')          AS descripcion,
                COALESCE(r.activo, TRUE)             AS activo,
                COALESCE(r.es_personalizado, FALSE)  AS es_personalizado,
                COUNT(u.usuario_id)                  AS total_usuarios
            FROM rol r
            LEFT JOIN usuario u ON u.rol_id = r.rol_id
            GROUP BY r.rol_id, r.nombre, r.descripcion, r.activo, r.es_personalizado
            ORDER BY r.rol_id
        """)
        rows = cur.fetchall()
        return {"success": True, "total": len(rows), "data": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/roles", status_code=201)
async def crear_rol(
    data: RolCreateRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea un rol personalizado."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    nombre_normalizado = data.nombre.lower().replace(" ", "_")
    if nombre_normalizado in {"cliente", "taller", "tecnico", "tenant_admin", "administrador"}:
        raise HTTPException(status_code=400, detail="No se puede usar un nombre de rol reservado")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        cur.execute("SELECT rol_id FROM rol WHERE nombre = %s LIMIT 1", (nombre_normalizado,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya existe un rol con ese nombre")

        cur.execute("""
            INSERT INTO rol (nombre, descripcion, activo, es_personalizado)
            VALUES (%s, %s, TRUE, TRUE) RETURNING rol_id
        """, (nombre_normalizado, data.descripcion))
        rol_id = cur.fetchone()["rol_id"]

        _log_bitacora(cur, sa_id, "CREAR_ROL", "rol", rol_id,
                      f"Rol personalizado creado: {nombre_normalizado}")
        db.commit()
        return {"success": True, "message": "Rol creado exitosamente", "rol_id": rol_id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.put("/roles/{rol_id}")
async def editar_rol(
    rol_id: int,
    data: RolUpdateRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Edita un rol personalizado."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute(
            "SELECT rol_id, nombre, COALESCE(es_personalizado, FALSE) AS es_personalizado FROM rol WHERE rol_id = %s",
            (rol_id,)
        )
        rol = cur.fetchone()
        if not rol:
            raise HTTPException(status_code=404, detail="Rol no encontrado")
        if not rol["es_personalizado"]:
            raise HTTPException(status_code=400, detail="Solo se pueden editar roles personalizados")

        updates: dict = {}
        if data.nombre is not None:
            updates["nombre"] = data.nombre.lower().replace(" ", "_")
        if data.descripcion is not None:
            updates["descripcion"] = data.descripcion

        if updates:
            set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
            cur.execute(f"UPDATE rol SET {set_clause} WHERE rol_id = %s",
                        list(updates.values()) + [rol_id])

        _log_bitacora(cur, sa_id, "EDITAR_ROL", "rol", rol_id, "Rol editado", updates)
        db.commit()
        return {"success": True, "message": "Rol actualizado correctamente"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.patch("/roles/{rol_id}/estado")
async def toggle_estado_rol(
    rol_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Activa o desactiva un rol personalizado."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute(
            "SELECT rol_id, nombre, COALESCE(es_personalizado, FALSE) AS es_personalizado FROM rol WHERE rol_id = %s",
            (rol_id,)
        )
        rol = cur.fetchone()
        if not rol:
            raise HTTPException(status_code=404, detail="Rol no encontrado")
        if not rol["es_personalizado"]:
            raise HTTPException(status_code=400, detail="Solo se pueden cambiar de estado roles personalizados")

        cur.execute("""
            UPDATE rol
            SET activo = CASE WHEN COALESCE(activo, TRUE) = TRUE THEN FALSE ELSE TRUE END
            WHERE rol_id = %s
            RETURNING rol_id, nombre, activo
        """, (rol_id,))
        row = cur.fetchone()

        _log_bitacora(cur, sa_id, "CAMBIO_ESTADO_ROL", "rol", rol_id,
                      f"Estado del rol '{row['nombre']}' cambiado a {row['activo']}")
        db.commit()
        return {"success": True, "rol_id": row["rol_id"],
                "nombre": row["nombre"], "activo": row["activo"]}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/roles/{rol_id}/permisos")
async def get_rol_permisos(
    rol_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Obtiene todos los permisos indicando cuáles están asignados al rol."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute("SELECT rol_id, nombre FROM rol WHERE rol_id = %s", (rol_id,))
        rol = cur.fetchone()
        if not rol:
            raise HTTPException(status_code=404, detail="Rol no encontrado")

        cur.execute("""
            SELECT p.permiso_id, p.codigo, p.nombre, p.modulo,
                   CASE WHEN rp.rol_id IS NOT NULL THEN TRUE ELSE FALSE END AS asignado
            FROM permiso p
            LEFT JOIN rol_permiso rp ON rp.permiso_id = p.permiso_id AND rp.rol_id = %s
            WHERE p.activo = TRUE
            ORDER BY p.modulo, p.nombre
        """, (rol_id,))
        permisos = cur.fetchall()

        return {"success": True, "rol": dict(rol), "permisos": [dict(p) for p in permisos]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.put("/roles/{rol_id}/permisos")
async def asignar_permisos_rol(
    rol_id: int,
    data: RolPermisosRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Reemplaza los permisos asignados a un rol."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)
    sa_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute("SELECT rol_id, nombre FROM rol WHERE rol_id = %s", (rol_id,))
        rol = cur.fetchone()
        if not rol:
            raise HTTPException(status_code=404, detail="Rol no encontrado")

        cur.execute("DELETE FROM rol_permiso WHERE rol_id = %s", (rol_id,))
        for permiso_id in data.permiso_ids:
            cur.execute(
                "INSERT INTO rol_permiso (rol_id, permiso_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (rol_id, permiso_id)
            )

        _log_bitacora(cur, sa_id, "ASIGNAR_PERMISOS_ROL", "rol", rol_id,
                      f"Permisos actualizados para rol '{rol['nombre']}'",
                      {"permiso_ids": data.permiso_ids})
        db.commit()
        return {"success": True,
                "message": f"Permisos del rol '{rol['nombre']}' actualizados correctamente"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/permisos")
async def listar_permisos(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Lista todos los permisos disponibles del sistema."""
    payload = get_token_payload(authorization)
    _require_superadmin(payload)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_roles_tables(cur)
        db.commit()
        cur.execute("SELECT * FROM permiso WHERE activo = TRUE ORDER BY modulo, nombre")
        rows = cur.fetchall()
        return {"success": True, "total": len(rows), "data": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()
