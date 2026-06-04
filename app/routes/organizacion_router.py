"""
ROUTER DE ORGANIZACIÓN (TENANT)

Gestiona el ciclo de vida de cada tenant:
  - Registro de nueva organización + usuario tenant_admin
  - Login del tenant_admin
  - Dashboard consolidado (KPIs de todos los talleres del tenant)
  - Gestión de talleres dentro de la organización
  - Vistas de técnicos e incidentes consolidados
"""

from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, EmailStr
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import bcrypt
import jwt

from ..services.config import Config
from ..classes.postgresql import Database
from ..utils.tenant_deps import (
    get_token_payload,
    require_tenant_admin,
    assert_org_access,
    get_org_id_default,
)

router = APIRouter(prefix="/api/organizacion", tags=["Organización Multi-Tenant"])


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


# ===================== MODELOS REQUEST =====================

class OrgRegister(BaseModel):
    """Crea la organización y su usuario tenant_admin en un solo paso."""
    # Datos de la organización
    nombre_organizacion: str
    descripcion: Optional[str] = None
    nit: Optional[str] = None
    email_contacto: EmailStr
    telefono_organizacion: Optional[str] = None
    plan: Optional[str] = "basico"
    # Datos del usuario administrador
    nombre_admin: str
    email_admin: EmailStr
    password_admin: str
    telefono_admin: Optional[str] = None


class OrgLogin(BaseModel):
    email: str
    password: str


class TallerEnOrgCreate(BaseModel):
    """Registra un nuevo taller dentro de la organización (lo hace el tenant_admin)."""
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


# ===================== MODELOS RESPONSE =====================

class OrgAdminUser(BaseModel):
    usuario_id: int
    nombre: str
    email: str
    rol: str
    organizacion_id: int
    organizacion_nombre: str


class OrgLoginResponse(BaseModel):
    success: bool
    access_token: str
    user: OrgAdminUser


class OrgRegisterResponse(BaseModel):
    success: bool
    message: str
    organizacion_id: int
    usuario_id: int


class TallerResumen(BaseModel):
    taller_id: int
    razon_social: str
    direccion: Optional[str]
    telefono_operativo: Optional[str]
    horario_inicio: Optional[str]
    horario_fin: Optional[str]
    disponible: bool
    calificacion_promedio: float
    total_tecnicos: int
    tecnicos_disponibles: int
    servicios_completados: int
    ingresos_totales: float
    latitud: Optional[float] = None
    longitud: Optional[float] = None


class OrgDashboard(BaseModel):
    organizacion_id: int
    organizacion_nombre: str
    plan: str
    total_talleres: int
    total_tecnicos: int
    total_incidentes: int
    incidentes_completados: int
    incidentes_pendientes: int
    incidentes_en_progreso: int
    ingresos_totales: float
    ingresos_talleres: float
    comisiones_plataforma: float
    calificacion_promedio: float


class TallerRegistradoResponse(BaseModel):
    success: bool
    message: str
    taller_id: int
    usuario_id: int


# ===================== ENDPOINTS AUTH =====================

@router.post("/register", response_model=OrgRegisterResponse, status_code=201)
async def registrar_organizacion(data: OrgRegister, db=Depends(Database.get_db)):
    """
    Crea una nueva organización (tenant) y su usuario administrador.
    Devuelve organizacion_id y usuario_id del admin creado.
    """
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Validar email único del admin
        cur.execute(
            "SELECT usuario_id FROM usuario WHERE email = %s LIMIT 1",
            (data.email_admin.lower(),)
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="El correo del administrador ya está registrado")

        # Validar nombre único de organización
        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE nombre = %s LIMIT 1",
            (data.nombre_organizacion.upper(),)
        )
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Ya existe una organización con ese nombre")

        # Obtener rol_id de tenant_admin
        cur.execute("SELECT rol_id FROM rol WHERE nombre = 'tenant_admin' LIMIT 1")
        rol_row = cur.fetchone()
        if not rol_row:
            raise HTTPException(status_code=500, detail="Rol 'tenant_admin' no encontrado. Ejecuta el script de migración.")
        rol_id = rol_row["rol_id"]

        # 1. Crear organización
        cur.execute("""
            INSERT INTO organizacion (nombre, descripcion, nit, email_contacto, telefono, plan, estado)
            VALUES (%s, %s, %s, %s, %s, %s, 'activo')
            RETURNING organizacion_id
        """, (
            data.nombre_organizacion.upper(),
            data.descripcion,
            data.nit,
            data.email_contacto.lower(),
            data.telefono_organizacion,
            data.plan or "basico",
        ))
        org_id = cur.fetchone()["organizacion_id"]

        # 2. Crear usuario tenant_admin
        password_hash = _hash_password(data.password_admin)
        cur.execute("""
            INSERT INTO usuario (rol_id, nombre, email, telefono, contrasena_hash, estado, organizacion_id)
            VALUES (%s, %s, %s, %s, %s, 'activo', %s)
            RETURNING usuario_id
        """, (
            rol_id,
            data.nombre_admin.upper(),
            data.email_admin.lower(),
            data.telefono_admin,
            password_hash,
            org_id,
        ))
        usuario_id = cur.fetchone()["usuario_id"]

        db.commit()
        return OrgRegisterResponse(
            success=True,
            message=f"Organización '{data.nombre_organizacion}' creada exitosamente",
            organizacion_id=org_id,
            usuario_id=usuario_id,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al registrar organización: {str(e)}")
    finally:
        cur.close()


@router.post("/login", response_model=OrgLoginResponse)
async def login_organizacion(data: OrgLogin, db=Depends(Database.get_db)):
    """
    Autentica al tenant_admin y retorna JWT con organizacion_id en el payload.
    """
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                u.usuario_id,
                u.contrasena_hash,
                u.nombre,
                u.email,
                u.estado,
                u.organizacion_id,
                r.nombre AS rol_nombre,
                o.nombre AS organizacion_nombre,
                o.plan
            FROM usuario u
            INNER JOIN rol r ON u.rol_id = r.rol_id
            INNER JOIN organizacion o ON u.organizacion_id = o.organizacion_id
            WHERE u.email = %s AND r.nombre = 'tenant_admin'
            LIMIT 1
        """, (data.email.lower(),))
        user = cur.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        if not _verify_password(data.password, user["contrasena_hash"]):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        if user["estado"] != "activo":
            raise HTTPException(status_code=403, detail="Cuenta inactiva")

        # Actualizar último acceso
        cur.execute(
            "UPDATE usuario SET ultimo_acceso = CURRENT_TIMESTAMP WHERE usuario_id = %s",
            (user["usuario_id"],)
        )
        db.commit()

        token_payload = {
            "sub": str(user["usuario_id"]),
            "organizacion_id": user["organizacion_id"],
            "taller_id": None,
            "rol": user["rol_nombre"],
            "email": user["email"],
            "exp": datetime.now(tz=timezone.utc) + timedelta(hours=24),
        }
        token = jwt.encode(token_payload, Config.SECRET_KEY, algorithm=Config.ALGORITHM)

        return OrgLoginResponse(
            success=True,
            access_token=token,
            user=OrgAdminUser(
                usuario_id=user["usuario_id"],
                nombre=user["nombre"],
                email=user["email"],
                rol=user["rol_nombre"],
                organizacion_id=user["organizacion_id"],
                organizacion_nombre=user["organizacion_nombre"],
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en login: {str(e)}")
    finally:
        cur.close()


# ===================== ENDPOINTS DE GESTIÓN =====================

@router.get("/{org_id}/dashboard", response_model=OrgDashboard)
async def get_dashboard(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    KPIs consolidados de toda la organización.
    Solo accesible para tenant_admin de esa org o administrador de plataforma.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM vista_kpis_organizacion WHERE organizacion_id = %s", (org_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        return OrgDashboard(
            organizacion_id=row["organizacion_id"],
            organizacion_nombre=row["organizacion_nombre"],
            plan=row["plan"] or "basico",
            total_talleres=int(row["total_talleres"] or 0),
            total_tecnicos=int(row["total_tecnicos"] or 0),
            total_incidentes=int(row["total_incidentes"] or 0),
            incidentes_completados=int(row["incidentes_completados"] or 0),
            incidentes_pendientes=int(row["incidentes_pendientes"] or 0),
            incidentes_en_progreso=int(row["incidentes_en_progreso"] or 0),
            ingresos_totales=float(row["ingresos_totales"] or 0),
            ingresos_talleres=float(row["ingresos_talleres"] or 0),
            comisiones_plataforma=float(row["comisiones_plataforma"] or 0),
            calificacion_promedio=float(row["calificacion_promedio"] or 0),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo dashboard: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/talleres", response_model=List[TallerResumen])
async def listar_talleres_org(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Lista todos los talleres de la organización con sus KPIs individuales.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                t.taller_id,
                t.razon_social,
                t.organizacion_id,
                t.disponible,
                t.latitud,
                t.longitud,
                t.direccion,
                t.telefono_operativo,
                t.horario_inicio::text AS horario_inicio,
                t.horario_fin::text   AS horario_fin,
                COALESCE(cal.puntuacion_promedio, 0)    AS puntuacion_promedio,
                COALESCE(tec.total_tecnicos, 0)         AS total_tecnicos,
                COALESCE(tec.tecnicos_disponibles, 0)   AS tecnicos_disponibles,
                COALESCE(svc.servicios_completados, 0)  AS servicios_completados,
                COALESCE(pag.ingresos_totales, 0)       AS ingresos_totales
            FROM taller t
            LEFT JOIN (
                SELECT taller_id,
                       ROUND(AVG(puntuacion)::numeric, 2) AS puntuacion_promedio
                FROM calificacion GROUP BY taller_id
            ) cal ON cal.taller_id = t.taller_id
            LEFT JOIN (
                SELECT taller_id,
                       COUNT(*)                                    AS total_tecnicos,
                       COUNT(CASE WHEN disponible THEN 1 END)      AS tecnicos_disponibles
                FROM tecnico GROUP BY taller_id
            ) tec ON tec.taller_id = t.taller_id
            LEFT JOIN (
                SELECT a.taller_id,
                       COUNT(DISTINCT CASE WHEN a.estado = 'completada'
                             THEN a.asignacion_id END)             AS servicios_completados
                FROM asignacion a GROUP BY a.taller_id
            ) svc ON svc.taller_id = t.taller_id
            LEFT JOIN (
                SELECT a.taller_id,
                       COALESCE(SUM(p.monto_taller), 0)            AS ingresos_totales
                FROM pago p
                JOIN asignacion a ON a.asignacion_id = p.asignacion_id
                WHERE p.estado = 'completado'
                GROUP BY a.taller_id
            ) pag ON pag.taller_id = t.taller_id
            WHERE t.organizacion_id = %s
            ORDER BY t.razon_social
        """, (org_id,))
        rows = cur.fetchall()

        return [
            TallerResumen(
                taller_id=r["taller_id"],
                razon_social=r["razon_social"],
                direccion=r.get("direccion"),
                telefono_operativo=r.get("telefono_operativo"),
                horario_inicio=r.get("horario_inicio"),
                horario_fin=r.get("horario_fin"),
                disponible=r["disponible"],
                calificacion_promedio=float(r["puntuacion_promedio"] or 0),
                total_tecnicos=int(r["total_tecnicos"] or 0),
                tecnicos_disponibles=int(r["tecnicos_disponibles"] or 0),
                servicios_completados=int(r["servicios_completados"] or 0),
                ingresos_totales=float(r["ingresos_totales"] or 0),
                latitud=float(r["latitud"]) if r.get("latitud") is not None else None,
                longitud=float(r["longitud"]) if r.get("longitud") is not None else None,
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando talleres: {str(e)}")
    finally:
        cur.close()


@router.post("/{org_id}/talleres", response_model=TallerRegistradoResponse, status_code=201)
async def registrar_taller_en_org(
    org_id: int,
    data: TallerEnOrgCreate,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    El tenant_admin registra un nuevo taller dentro de su organización.
    Crea el usuario (rol taller) + el registro de taller vinculado a la org.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Verificar que la organización existe y está activa
        cur.execute(
            "SELECT organizacion_id FROM organizacion WHERE organizacion_id = %s AND estado = 'activo'",
            (org_id,)
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Organización no encontrada o inactiva")

        # Validar email único
        cur.execute("SELECT usuario_id FROM usuario WHERE email = %s LIMIT 1", (data.email.lower(),))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="El correo ya está registrado")

        password_hash = _hash_password(data.password)

        # Crear usuario con rol taller (rol_id=2)
        cur.execute("""
            INSERT INTO usuario (rol_id, nombre, email, telefono, contrasena_hash,
                                 documento_identidad, estado, organizacion_id)
            VALUES (2, %s, %s, %s, %s, %s, 'activo', %s)
            RETURNING usuario_id
        """, (
            data.nombre_contacto.upper(),
            data.email.lower(),
            data.telefono,
            password_hash,
            data.documento_identidad,
            org_id,
        ))
        nuevo_usuario_id = cur.fetchone()["usuario_id"]

        # Crear taller vinculado a la organización
        cur.execute("""
            INSERT INTO taller (usuario_id, razon_social, direccion, latitud, longitud,
                                telefono_operativo, horario_inicio, horario_fin,
                                disponible, organizacion_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
            RETURNING taller_id
        """, (
            nuevo_usuario_id,
            data.razon_social.upper(),
            data.direccion.upper(),
            data.latitud,
            data.longitud,
            data.telefono_operativo,
            data.horario_inicio,
            data.horario_fin,
            org_id,
        ))
        nuevo_taller_id = cur.fetchone()["taller_id"]

        # Vincular servicios base al taller (desactivados por defecto)
        cur.execute("""
            INSERT INTO taller_servicio (taller_id, servicio_id, disponible)
            SELECT %s, s.servicio_id, FALSE
            FROM servicio s
            ON CONFLICT (taller_id, servicio_id) DO NOTHING
        """, (nuevo_taller_id,))

        db.commit()
        return TallerRegistradoResponse(
            success=True,
            message=f"Taller '{data.razon_social}' registrado en la organización",
            taller_id=nuevo_taller_id,
            usuario_id=nuevo_usuario_id,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error registrando taller: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/tecnicos")
async def listar_tecnicos_org(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Lista todos los técnicos de todos los talleres de la organización.
    Vista consolidada para el tenant_admin.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                tec.tecnico_id,
                tec.nombre AS tecnico_nombre,
                tec.especialidad,
                tec.disponible,
                tec.latitud_actual,
                tec.longitud_actual,
                tec.fecha_ultima_ubicacion,
                t.taller_id,
                t.razon_social AS taller_nombre
            FROM tecnico tec
            INNER JOIN taller t ON t.taller_id = tec.taller_id
            WHERE t.organizacion_id = %s
            ORDER BY t.razon_social, tec.nombre
        """, (org_id,))
        rows = cur.fetchall()

        return {
            "success": True,
            "organizacion_id": org_id,
            "total": len(rows),
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando técnicos: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/incidentes")
async def listar_incidentes_org(
    org_id: int,
    estado: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Lista todos los incidentes asignados a talleres de la organización.
    Soporta filtro por estado y paginación.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        base_query = """
            SELECT *
            FROM vista_incidentes_organizacion
            WHERE organizacion_id = %s
        """
        params: list = [org_id]

        if estado:
            base_query += " AND estado_incidente = %s"
            params.append(estado)

        # Conteo total
        count_query = f"SELECT COUNT(*) AS total FROM ({base_query}) sub"
        cur.execute(count_query, params)
        total = cur.fetchone()["total"]

        base_query += " ORDER BY fecha_creacion DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(base_query, params)
        rows = cur.fetchall()

        return {
            "success": True,
            "organizacion_id": org_id,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listando incidentes: {str(e)}")
    finally:
        cur.close()


@router.patch("/{org_id}/talleres/{taller_id}/disponibilidad")
async def toggle_disponibilidad_taller(
    org_id: int,
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Alterna el estado disponible/no disponible de un taller de la organización."""
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE taller SET disponible = NOT disponible
            WHERE taller_id = %s AND organizacion_id = %s
            RETURNING taller_id, disponible
        """, (taller_id, org_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Taller no encontrado en esta organización")
        db.commit()
        estado = "disponible" if row["disponible"] else "no disponible"
        return {
            "success": True,
            "taller_id": row["taller_id"],
            "disponible": row["disponible"],
            "message": f"Taller marcado como {estado}",
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/analitica")
async def analitica_global(
    org_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    KPIs operacionales avanzados del tenant:
    tiempos promedio, SLA, incidentes por tipo,
    zonas calientes y ranking de talleres.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Tiempos promedio y SLA
        # - Asignación  = fecha_asignacion - incidente.fecha_creacion  (respuesta al reporte)
        # - Llegada     = fecha_inicio_servicio - fecha_asignacion  (solo si se registró llegada)
        # - Resolución  = fecha_cierre_servicio - COALESCE(fecha_inicio, fecha_asignacion)
        #   (funciona con o sin registro de llegada; AVG ignora NULLs)
        # - SLA cumplido si respuesta ≤ 15 min
        cur.execute("""
            SELECT
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60
                    END
                )::NUMERIC, 2)  AS prom_asignacion_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_inicio_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_inicio_servicio - a.fecha_asignacion)) / 60
                    END
                )::NUMERIC, 2)  AS prom_llegada_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_cierre_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (
                             a.fecha_cierre_servicio
                             - COALESCE(a.fecha_inicio_servicio, a.fecha_asignacion)
                         )) / 60
                    END
                )::NUMERIC, 2)  AS prom_finalizacion_min,
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL) AS total_evaluados_sla,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60 <= 15
                ) AS sla_cumplidos
            FROM asignacion a
            JOIN taller t    ON t.taller_id    = a.taller_id
            JOIN incidente i ON i.incidente_id = a.incidente_id
            WHERE t.organizacion_id = %s
        """, (org_id,))
        tiempos = cur.fetchone()

        # 2. Total emergencias del tenant
        cur.execute("""
            SELECT COUNT(DISTINCT i.incidente_id) AS total
            FROM incidente i
            JOIN asignacion a ON a.incidente_id = i.incidente_id
            JOIN taller t ON t.taller_id = a.taller_id
            WHERE t.organizacion_id = %s
        """, (org_id,))
        total_row = cur.fetchone()

        # 3. Incidentes por tipo
        cur.execute("""
            SELECT
                COALESCE(i.tipo_problema, 'Sin clasificar') AS tipo,
                COUNT(DISTINCT i.incidente_id) AS cantidad
            FROM incidente i
            JOIN asignacion a ON a.incidente_id = i.incidente_id
            JOIN taller t ON t.taller_id = a.taller_id
            WHERE t.organizacion_id = %s
            GROUP BY i.tipo_problema
            ORDER BY cantidad DESC
        """, (org_id,))
        por_tipo = cur.fetchall()

        # 4. Casos cancelados
        cur.execute("""
            SELECT COUNT(DISTINCT i.incidente_id) AS cancelados
            FROM incidente i
            JOIN asignacion a ON a.incidente_id = i.incidente_id
            JOIN taller t ON t.taller_id = a.taller_id
            WHERE t.organizacion_id = %s
              AND i.estado = 'cancelado'
        """, (org_id,))
        cancelados_row = cur.fetchone()

        # 5. Zonas con más incidentes (top 5, lat/lng redondeados a 1 decimal ≈ 11 km)
        cur.execute("""
            SELECT
                ROUND(i.latitud::NUMERIC,  1) AS lat,
                ROUND(i.longitud::NUMERIC, 1) AS lng,
                COUNT(DISTINCT i.incidente_id) AS cantidad
            FROM incidente i
            JOIN asignacion a ON a.incidente_id = i.incidente_id
            JOIN taller t ON t.taller_id = a.taller_id
            WHERE t.organizacion_id = %s
              AND i.latitud  IS NOT NULL
              AND i.longitud IS NOT NULL
            GROUP BY ROUND(i.latitud::NUMERIC, 1), ROUND(i.longitud::NUMERIC, 1)
            ORDER BY cantidad DESC
            LIMIT 5
        """, (org_id,))
        zonas = cur.fetchall()

        # 6. Ranking de talleres por eficiencia
        # Score = 40% tasa_completados + 30% calificación/5 + 30% SLA_compliance
        cur.execute("""
            SELECT
                t.taller_id,
                t.razon_social,
                COUNT(DISTINCT i.incidente_id)  AS total,
                COUNT(DISTINCT CASE WHEN i.estado IN ('atendido','completado','cerrada')
                      THEN i.incidente_id END)  AS completados,
                COALESCE(ROUND(AVG(c.puntuacion)::NUMERIC, 2), 0) AS calificacion,
                COALESCE(ROUND(AVG(
                    EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60
                ) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)::NUMERIC, 2), 0) AS tiempo_prom_asignacion_min,
                ROUND((
                    0.4 * COALESCE(
                        COUNT(DISTINCT CASE WHEN i.estado IN ('atendido','completado','cerrada')
                              THEN i.incidente_id END)::float
                        / NULLIF(COUNT(DISTINCT i.incidente_id), 0), 0)
                    + 0.3 * COALESCE(AVG(c.puntuacion), 0) / 5.0
                    + 0.3 * COALESCE(
                        COUNT(*) FILTER (
                            WHERE a.fecha_aceptacion IS NOT NULL
                            AND EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60 <= 15
                        )::float
                        / NULLIF(COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL), 0), 0)
                )::NUMERIC * 100, 1) AS score
            FROM taller t
            LEFT JOIN asignacion a  ON a.taller_id    = t.taller_id
            LEFT JOIN incidente i   ON i.incidente_id = a.incidente_id
            LEFT JOIN calificacion c ON c.taller_id   = t.taller_id
            WHERE t.organizacion_id = %s
            GROUP BY t.taller_id, t.razon_social
            ORDER BY score DESC NULLS LAST
        """, (org_id,))
        ranking = cur.fetchall()

        sla_ev = int(tiempos["total_evaluados_sla"] or 0)
        sla_cu = int(tiempos["sla_cumplidos"] or 0)
        sla_pct = round(sla_cu * 100.0 / sla_ev, 1) if sla_ev > 0 else None

        return {
            "organizacion_id":      org_id,
            "total_emergencias":    int(total_row["total"] or 0),
            "tiempos": {
                "promedio_asignacion_min":   float(tiempos["prom_asignacion_min"])   if tiempos["prom_asignacion_min"]   is not None else None,
                "promedio_llegada_min":      float(tiempos["prom_llegada_min"])      if tiempos["prom_llegada_min"]      is not None else None,
                "promedio_finalizacion_min": float(tiempos["prom_finalizacion_min"]) if tiempos["prom_finalizacion_min"] is not None else None,
            },
            "casos_cancelados":     int(cancelados_row["cancelados"] or 0),
            "sla_cumplimiento_pct": sla_pct,
            "sla_total_evaluados":  sla_ev,
            "incidentes_por_tipo":  [{"tipo": r["tipo"], "cantidad": int(r["cantidad"])} for r in por_tipo],
            "zonas_top":            [{"lat": float(r["lat"]), "lng": float(r["lng"]), "cantidad": int(r["cantidad"])} for r in zonas],
            "ranking_talleres": [
                {
                    "taller_id":                  r["taller_id"],
                    "nombre":                     r["razon_social"],
                    "completados":                int(r["completados"] or 0),
                    "total":                      int(r["total"] or 0),
                    "calificacion":               float(r["calificacion"] or 0),
                    "tiempo_prom_asignacion_min": float(r["tiempo_prom_asignacion_min"] or 0),
                    "score":                      float(r["score"] or 0),
                }
                for r in ranking
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculando analítica: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/analitica/taller/{taller_id}")
async def analitica_taller(
    org_id: int,
    taller_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    KPIs operacionales del taller individual más comparación
    contra el promedio del tenant.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT taller_id, razon_social FROM taller WHERE taller_id = %s AND organizacion_id = %s",
            (taller_id, org_id)
        )
        taller = cur.fetchone()
        if not taller:
            raise HTTPException(status_code=404, detail="Taller no encontrado en esta organización")

        # KPIs propios del taller
        cur.execute("""
            SELECT
                COUNT(DISTINCT i.incidente_id)                                                                 AS total_emergencias,
                COUNT(DISTINCT CASE WHEN i.estado = 'cancelado' THEN i.incidente_id END)                      AS cancelados,
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_asignacion_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_inicio_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_inicio_servicio - a.fecha_asignacion)) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_llegada_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_cierre_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (
                             a.fecha_cierre_servicio
                             - COALESCE(a.fecha_inicio_servicio, a.fecha_asignacion)
                         )) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_finalizacion_min,
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)                                        AS sla_evaluados,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60 <= 15
                )                                                                                              AS sla_cumplidos
            FROM asignacion a
            JOIN incidente i ON i.incidente_id = a.incidente_id
            WHERE a.taller_id = %s
        """, (taller_id,))
        stats = cur.fetchone()

        # Incidentes por tipo del taller
        cur.execute("""
            SELECT COALESCE(i.tipo_problema, 'Sin clasificar') AS tipo, COUNT(*) AS cantidad
            FROM incidente i
            JOIN asignacion a ON a.incidente_id = i.incidente_id
            WHERE a.taller_id = %s
            GROUP BY i.tipo_problema ORDER BY cantidad DESC
        """, (taller_id,))
        por_tipo = cur.fetchall()

        # Rendimiento mensual – últimos 12 meses
        cur.execute("""
            SELECT
                EXTRACT(YEAR  FROM a.fecha_asignacion)::INT  AS anio,
                EXTRACT(MONTH FROM a.fecha_asignacion)::INT  AS mes,
                TO_CHAR(a.fecha_asignacion, 'Mon')           AS mes_nombre,
                COUNT(DISTINCT a.incidente_id)               AS total,
                COUNT(DISTINCT CASE WHEN i.estado IN ('atendido','completado')
                      THEN a.incidente_id END)               AS completados
            FROM asignacion a
            JOIN incidente i ON i.incidente_id = a.incidente_id
            WHERE a.taller_id = %s
              AND a.fecha_asignacion >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY EXTRACT(YEAR FROM a.fecha_asignacion),
                     EXTRACT(MONTH FROM a.fecha_asignacion),
                     TO_CHAR(a.fecha_asignacion, 'Mon')
            ORDER BY anio, mes
        """, (taller_id,))
        mensual = cur.fetchall()

        # Promedios del tenant para comparación
        cur.execute("""
            SELECT
                ROUND(AVG(
                    CASE WHEN a.fecha_aceptacion IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_asignacion_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_inicio_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (a.fecha_inicio_servicio - a.fecha_asignacion)) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_llegada_min,
                ROUND(AVG(
                    CASE WHEN a.fecha_cierre_servicio IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (
                             a.fecha_cierre_servicio
                             - COALESCE(a.fecha_inicio_servicio, a.fecha_asignacion)
                         )) / 60
                    END
                )::NUMERIC, 2)                                                                                 AS prom_finalizacion_min,
                COUNT(*) FILTER (WHERE a.fecha_aceptacion IS NOT NULL)                                        AS sla_evaluados,
                COUNT(*) FILTER (
                    WHERE a.fecha_aceptacion IS NOT NULL
                    AND EXTRACT(EPOCH FROM (a.fecha_asignacion - i.fecha_creacion)) / 60 <= 15
                )                                                                                              AS sla_cumplidos
            FROM asignacion a
            JOIN taller t    ON t.taller_id    = a.taller_id
            JOIN incidente i ON i.incidente_id = a.incidente_id
            WHERE t.organizacion_id = %s
        """, (org_id,))
        tenant_avg = cur.fetchone()

        def safe_f(val):
            return float(val) if val is not None else None

        sla_ev = int(stats["sla_evaluados"] or 0)
        sla_cu = int(stats["sla_cumplidos"] or 0)
        sla_pct = round(sla_cu * 100.0 / sla_ev, 1) if sla_ev > 0 else None

        t_sla_ev = int(tenant_avg["sla_evaluados"] or 0)
        t_sla_cu = int(tenant_avg["sla_cumplidos"] or 0)
        t_sla_pct = round(t_sla_cu * 100.0 / t_sla_ev, 1) if t_sla_ev > 0 else None

        return {
            "taller_id":        taller_id,
            "taller_nombre":    taller["razon_social"],
            "total_emergencias": int(stats["total_emergencias"] or 0),
            "casos_cancelados":  int(stats["cancelados"] or 0),
            "tiempos": {
                "promedio_asignacion_min":  safe_f(stats["prom_asignacion_min"]),
                "promedio_llegada_min":     safe_f(stats["prom_llegada_min"]),
                "promedio_resolucion_min":  safe_f(stats["prom_finalizacion_min"]),
            },
            "sla_cumplimiento_pct":  sla_pct,
            "sla_total_evaluados":   sla_ev,
            "incidentes_por_tipo":   [{"tipo": r["tipo"], "cantidad": int(r["cantidad"])} for r in por_tipo],
            "rendimiento_mensual": [
                {
                    "anio":       r["anio"],
                    "mes":        r["mes"],
                    "mes_nombre": r["mes_nombre"],
                    "total":      int(r["total"]),
                    "completados": int(r["completados"]),
                }
                for r in mensual
            ],
            "comparacion_tenant": {
                "promedio_asignacion_min":   safe_f(tenant_avg["prom_asignacion_min"]),
                "promedio_llegada_min":      safe_f(tenant_avg["prom_llegada_min"]),
                "promedio_finalizacion_min": safe_f(tenant_avg["prom_finalizacion_min"]),
                "sla_cumplimiento_pct":      t_sla_pct,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculando analítica del taller: {str(e)}")
    finally:
        cur.close()


@router.get("/{org_id}/reportes")
async def reportes_financieros_org(
    org_id: int,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Reporte financiero consolidado de la organización.
    Muestra ingresos, comisiones y pagos por taller.
    """
    payload = get_token_payload(authorization)
    assert_org_access(payload, org_id)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        params: list = []
        fecha_filter = ""

        if fecha_desde:
            fecha_filter += " AND p.creado_en >= %s"
            params.append(fecha_desde)
        if fecha_hasta:
            fecha_filter += " AND p.creado_en <= %s"
            params.append(fecha_hasta)

        cur.execute(f"""
            SELECT
                t.taller_id,
                t.razon_social,
                COUNT(DISTINCT CASE WHEN p.estado = 'completado' THEN p.pago_id END)
                                                                           AS total_transacciones,
                COALESCE(SUM(CASE WHEN p.estado = 'completado' THEN p.monto_total        ELSE 0 END), 0) AS ingresos_brutos,
                COALESCE(SUM(CASE WHEN p.estado = 'completado' THEN p.monto_taller       ELSE 0 END), 0) AS ingresos_talleres,
                COALESCE(SUM(CASE WHEN p.estado = 'completado' THEN p.comision_plataforma ELSE 0 END), 0) AS comisiones,
                COUNT(DISTINCT CASE WHEN p.estado = 'completado' THEN p.pago_id END)     AS pagos_completados,
                COUNT(DISTINCT CASE WHEN p.estado = 'pendiente'  THEN p.pago_id END)     AS pagos_pendientes,
                COALESCE(cal.puntuacion_promedio, 0)                                      AS calificacion_promedio
            FROM taller t
            LEFT JOIN asignacion a ON a.taller_id = t.taller_id
            LEFT JOIN pago p ON p.asignacion_id = a.asignacion_id {fecha_filter}
            LEFT JOIN (
                SELECT taller_id, ROUND(AVG(puntuacion)::numeric, 2) AS puntuacion_promedio
                FROM calificacion GROUP BY taller_id
            ) cal ON cal.taller_id = t.taller_id
            WHERE t.organizacion_id = %s
            GROUP BY t.taller_id, t.razon_social, cal.puntuacion_promedio
            ORDER BY ingresos_brutos DESC
        """, params + [org_id])
        rows = cur.fetchall()

        total_ingresos = sum(float(r["ingresos_brutos"] or 0) for r in rows)
        total_comisiones = sum(float(r["comisiones"] or 0) for r in rows)

        return {
            "success": True,
            "organizacion_id": org_id,
            "periodo": {"desde": fecha_desde, "hasta": fecha_hasta},
            "resumen": {
                "total_ingresos": total_ingresos,
                "total_comisiones": total_comisiones,
                "ingresos_netos": total_ingresos - total_comisiones,
            },
            "por_taller": [dict(r) for r in rows],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando reporte: {str(e)}")
    finally:
        cur.close()
