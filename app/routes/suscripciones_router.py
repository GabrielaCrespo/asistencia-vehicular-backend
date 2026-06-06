"""
ROUTER DE SUSCRIPCIONES SAAS

Gestión de planes y suscripciones de organizaciones (Multi-Tenant):
- Planes: Básico, Profesional, Empresarial
- Checkout con Stripe Test Mode (o modo demo si no hay clave)
- Portal de facturación Stripe
- Webhook para eventos de suscripción
- Vista SuperAdmin de todas las suscripciones
"""

import json
import stripe
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..services.config import Config
from ..classes.postgresql import Database
from ..utils.tenant_deps import get_token_payload, require_tenant_admin
from ..utils.bitacora import log_bitacora

stripe.api_key = Config.STRIPE_SECRET_KEY

router = APIRouter(prefix="/api/suscripciones", tags=["Suscripciones SaaS"])


# ===================== DEFINICIÓN DE PLANES =====================

PLANES = {
    "basico": {
        "codigo": "basico",
        "nombre": "Básico",
        "precio": 49.00,
        "max_talleres": 5,
        "max_tecnicos": 20,
        "tecnicos_ilimitados": False,
        "talleres_ilimitados": False,
        "features": [
            "Gestión de emergencias",
            "Seguimiento en tiempo real",
            "Cotizaciones",
        ],
        "color": "#3b82f6",
        "popular": False,
    },
    "profesional": {
        "codigo": "profesional",
        "nombre": "Profesional",
        "precio": 149.00,
        "max_talleres": 15,
        "max_tecnicos": None,
        "tecnicos_ilimitados": True,
        "talleres_ilimitados": False,
        "features": [
            "Dashboard KPI",
            "Reportes PDF y Excel",
            "Chat en tiempo real",
            "Mapa Inteligente de Riesgo",
        ],
        "color": "#7c3aed",
        "popular": True,
    },
    "empresarial": {
        "codigo": "empresarial",
        "nombre": "Empresarial",
        "precio": 349.00,
        "max_talleres": None,
        "max_tecnicos": None,
        "tecnicos_ilimitados": True,
        "talleres_ilimitados": True,
        "features": [
            "Multi-Tenant avanzado",
            "Analítica completa",
            "Reportes por voz",
            "Soporte prioritario",
        ],
        "color": "#059669",
        "popular": False,
    },
}


# ===================== HELPERS =====================

def _ensure_suscripcion_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS suscripcion (
            suscripcion_id           SERIAL PRIMARY KEY,
            organizacion_id          INTEGER NOT NULL
                                         REFERENCES organizacion(organizacion_id) ON DELETE CASCADE,
            plan_codigo              VARCHAR(50)  NOT NULL DEFAULT 'basico',
            stripe_customer_id       VARCHAR(100),
            stripe_subscription_id   VARCHAR(100),
            stripe_checkout_session_id VARCHAR(100),
            estado                   VARCHAR(30)  NOT NULL DEFAULT 'activa',
            fecha_inicio             TIMESTAMP,
            fecha_renovacion         TIMESTAMP,
            monto_mensual            NUMERIC(10,2),
            metodo_pago              VARCHAR(50)  DEFAULT 'stripe',
            CONSTRAINT suscripcion_org_unique UNIQUE(organizacion_id)
        )
    """)


# ===================== MODELOS =====================

class CheckoutRequest(BaseModel):
    plan_codigo: str
    success_url: Optional[str] = None
    cancel_url:  Optional[str] = None


class CancelarRequest(BaseModel):
    motivo: Optional[str] = None


# ===================== ENDPOINTS =====================

@router.get("/planes")
async def listar_planes():
    """Retorna los planes de suscripción disponibles (público)."""
    return {"success": True, "planes": list(PLANES.values())}


@router.get("/mi-suscripcion")
async def mi_suscripcion(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Retorna la suscripción activa de la organización del tenant_admin."""
    payload = get_token_payload(authorization)
    require_tenant_admin(payload)
    org_id = payload.get("organizacion_id")
    if not org_id:
        raise HTTPException(status_code=400, detail="No tienes organización asociada")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)
        db.commit()

        cur.execute("""
            SELECT o.organizacion_id, o.nombre, o.plan, o.email_contacto,
                   s.suscripcion_id,
                   COALESCE(s.plan_codigo, o.plan, 'basico') AS plan_codigo,
                   COALESCE(s.estado, 'activa')              AS estado_suscripcion,
                   s.stripe_customer_id,
                   s.stripe_subscription_id,
                   s.fecha_inicio,
                   s.fecha_renovacion,
                   s.monto_mensual,
                   s.metodo_pago
            FROM organizacion o
            LEFT JOIN suscripcion s ON s.organizacion_id = o.organizacion_id
            WHERE o.organizacion_id = %s
        """, (org_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        cur.execute(
            "SELECT COUNT(*) AS c FROM taller WHERE organizacion_id = %s", (org_id,)
        )
        uso_talleres = int(cur.fetchone()["c"] or 0)

        cur.execute("""
            SELECT COUNT(DISTINCT te.tecnico_id) AS c
            FROM tecnico te
            JOIN taller t ON t.taller_id = te.taller_id
            WHERE t.organizacion_id = %s
        """, (org_id,))
        uso_tecnicos = int(cur.fetchone()["c"] or 0)

        plan_codigo = row["plan_codigo"] or "basico"
        plan_info   = PLANES.get(plan_codigo, PLANES["basico"])

        return {
            "success": True,
            "organizacion": {"id": org_id, "nombre": row["nombre"]},
            "suscripcion": {
                "suscripcion_id":  row["suscripcion_id"],
                "plan_codigo":     plan_codigo,
                "plan_nombre":     plan_info["nombre"],
                "plan_precio":     plan_info["precio"],
                "plan_color":      plan_info["color"],
                "estado":          row["estado_suscripcion"],
                "fecha_inicio":    row["fecha_inicio"].isoformat() if row["fecha_inicio"] else None,
                "fecha_renovacion":row["fecha_renovacion"].isoformat() if row["fecha_renovacion"] else None,
                "monto_mensual":   float(row["monto_mensual"]) if row["monto_mensual"] else plan_info["precio"],
                "metodo_pago":     row["metodo_pago"] or "stripe",
                "tiene_stripe":    bool(row["stripe_subscription_id"]),
            },
            "uso": {
                "talleres_usados":      uso_talleres,
                "talleres_max":         plan_info["max_talleres"],
                "talleres_ilimitados":  plan_info["talleres_ilimitados"],
                "tecnicos_usados":      uso_tecnicos,
                "tecnicos_max":         plan_info["max_tecnicos"],
                "tecnicos_ilimitados":  plan_info["tecnicos_ilimitados"],
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/checkout")
async def crear_checkout(
    data: CheckoutRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea una Stripe Checkout Session para suscribirse o cambiar de plan."""
    payload = get_token_payload(authorization)
    require_tenant_admin(payload)
    org_id  = payload.get("organizacion_id")
    user_id = int(payload["sub"])

    if data.plan_codigo not in PLANES:
        raise HTTPException(status_code=400, detail="Plan no válido")

    plan_info = PLANES[data.plan_codigo]

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)
        db.commit()

        cur.execute("""
            SELECT o.organizacion_id, o.nombre, o.email_contacto,
                   s.stripe_customer_id, s.stripe_subscription_id, s.suscripcion_id
            FROM organizacion o
            LEFT JOIN suscripcion s ON s.organizacion_id = o.organizacion_id
            WHERE o.organizacion_id = %s
        """, (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organización no encontrada")

        # ── MODO DEMO (sin clave Stripe) ──────────────────────────────────────
        if not Config.STRIPE_SECRET_KEY:
            now = datetime.now(tz=timezone.utc)
            cur.execute("""
                INSERT INTO suscripcion
                    (organizacion_id, plan_codigo, estado, fecha_inicio,
                     fecha_renovacion, monto_mensual, metodo_pago)
                VALUES (%s, %s, 'activa', %s, %s, %s, 'demo')
                ON CONFLICT (organizacion_id) DO UPDATE SET
                    plan_codigo     = EXCLUDED.plan_codigo,
                    estado          = 'activa',
                    fecha_inicio    = EXCLUDED.fecha_inicio,
                    fecha_renovacion= EXCLUDED.fecha_renovacion,
                    monto_mensual   = EXCLUDED.monto_mensual,
                    metodo_pago     = 'demo'
            """, (org_id, data.plan_codigo, now, now + timedelta(days=30), plan_info["precio"]))
            cur.execute(
                "UPDATE organizacion SET plan = %s WHERE organizacion_id = %s",
                (data.plan_codigo, org_id)
            )
            log_bitacora(cur, user_id, "SUSCRIPCION_DEMO", "suscripcion", org_id,
                         f"Demo suscripción '{plan_info['nombre']}' activada para org {org_id}")
            db.commit()
            return {"success": True, "modo": "demo",
                    "message": f"Suscripción al Plan {plan_info['nombre']} activada en modo demo."}

        # ── STRIPE CHECKOUT ───────────────────────────────────────────────────
        customer_id = org.get("stripe_customer_id")
        if not customer_id:
            customer    = stripe.Customer.create(
                name=org["nombre"],
                email=org.get("email_contacto") or "",
                metadata={"organizacion_id": str(org_id)},
            )
            customer_id = customer.id
            cur.execute("""
                INSERT INTO suscripcion
                    (organizacion_id, plan_codigo, stripe_customer_id, estado, monto_mensual)
                VALUES (%s, %s, %s, 'pendiente', %s)
                ON CONFLICT (organizacion_id) DO UPDATE SET
                    stripe_customer_id = EXCLUDED.stripe_customer_id
            """, (org_id, data.plan_codigo, customer_id, plan_info["precio"]))
            db.commit()

        frontend_url = getattr(Config, "FRONTEND_URL", "http://localhost:4200")
        success_url  = (data.success_url
                        or f"{frontend_url}/organizacion/suscripcion"
                           "?success=1&session_id={CHECKOUT_SESSION_ID}")
        cancel_url   = data.cancel_url or f"{frontend_url}/organizacion/suscripcion?canceled=1"

        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"Plan {plan_info['nombre']} – Asistencia Vehicular",
                        "description": ", ".join(plan_info["features"]),
                    },
                    "unit_amount":  int(plan_info["precio"] * 100),
                    "recurring":    {"interval": "month"},
                },
                "quantity": 1,
            }],
            customer=customer_id,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={"organizacion_id": str(org_id), "plan_codigo": data.plan_codigo},
            subscription_data={
                "metadata": {"organizacion_id": str(org_id), "plan_codigo": data.plan_codigo},
            },
        )

        cur.execute("""
            UPDATE suscripcion
            SET stripe_checkout_session_id = %s, plan_codigo = %s
            WHERE organizacion_id = %s
        """, (session.id, data.plan_codigo, org_id))
        db.commit()

        return {"success": True, "checkout_url": session.url, "session_id": session.id}

    except HTTPException:
        db.rollback()
        raise
    except stripe.StripeError as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Error de Stripe: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/cancelar")
async def cancelar_suscripcion(
    data: CancelarRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Cancela la suscripción activa. Con Stripe: cancela al final del período."""
    payload = get_token_payload(authorization)
    require_tenant_admin(payload)
    org_id  = payload.get("organizacion_id")
    user_id = int(payload["sub"])

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)
        cur.execute("SELECT * FROM suscripcion WHERE organizacion_id = %s", (org_id,))
        suscripcion = cur.fetchone()
        if not suscripcion:
            raise HTTPException(status_code=404, detail="No hay suscripción registrada")
        if suscripcion["estado"] == "cancelada":
            raise HTTPException(status_code=400, detail="La suscripción ya está cancelada")

        if suscripcion.get("stripe_subscription_id") and Config.STRIPE_SECRET_KEY:
            try:
                stripe.Subscription.modify(
                    suscripcion["stripe_subscription_id"],
                    cancel_at_period_end=True,
                )
            except stripe.StripeError as e:
                raise HTTPException(status_code=502, detail=f"Error al cancelar en Stripe: {str(e)}")

        cur.execute(
            "UPDATE suscripcion SET estado = 'cancelada' WHERE organizacion_id = %s",
            (org_id,)
        )
        log_bitacora(cur, user_id, "CANCELAR_SUSCRIPCION", "suscripcion",
                     suscripcion["suscripcion_id"],
                     f"Suscripción cancelada. Motivo: {data.motivo or 'No especificado'}")
        db.commit()
        return {"success": True, "message": "Suscripción cancelada correctamente"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/portal")
async def portal_facturacion(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Crea sesión del Portal de Facturación de Stripe (gestión de método de pago, facturas)."""
    payload = get_token_payload(authorization)
    require_tenant_admin(payload)
    org_id = payload.get("organizacion_id")

    if not Config.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503,
                            detail="Portal de facturación no disponible en modo demo")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)
        cur.execute(
            "SELECT stripe_customer_id FROM suscripcion WHERE organizacion_id = %s", (org_id,)
        )
        row = cur.fetchone()
        if not row or not row.get("stripe_customer_id"):
            raise HTTPException(status_code=404,
                                detail="No hay cliente Stripe registrado para esta organización")

        frontend_url = getattr(Config, "FRONTEND_URL", "http://localhost:4200")
        portal = stripe.billing_portal.Session.create(
            customer=row["stripe_customer_id"],
            return_url=f"{frontend_url}/organizacion/suscripcion",
        )
        return {"success": True, "portal_url": portal.url}
    except HTTPException:
        raise
    except stripe.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Error de Stripe: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()


@router.post("/webhook")
async def suscripcion_webhook(request: Request, db=Depends(Database.get_db)):
    """Webhook de Stripe para eventos de suscripción."""
    body       = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if Config.STRIPE_WEBHOOK_SECRET:
        try:
            event = stripe.Webhook.construct_event(body, sig_header, Config.STRIPE_WEBHOOK_SECRET)
        except stripe.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Firma de webhook inválida")
    else:
        event = json.loads(body)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)

        if event["type"] == "checkout.session.completed":
            session = event["data"]["object"]
            if session.get("mode") != "subscription":
                return {"success": True}

            org_id          = session.get("metadata", {}).get("organizacion_id")
            plan_codigo     = session.get("metadata", {}).get("plan_codigo", "basico")
            subscription_id = session.get("subscription")
            customer_id     = session.get("customer")

            if org_id:
                now = datetime.now(tz=timezone.utc)
                cur.execute("""
                    INSERT INTO suscripcion
                        (organizacion_id, plan_codigo, stripe_customer_id,
                         stripe_subscription_id, estado, fecha_inicio,
                         fecha_renovacion, monto_mensual, metodo_pago)
                    VALUES (%s, %s, %s, %s, 'activa', %s, %s, %s, 'stripe')
                    ON CONFLICT (organizacion_id) DO UPDATE SET
                        plan_codigo            = EXCLUDED.plan_codigo,
                        stripe_customer_id     = EXCLUDED.stripe_customer_id,
                        stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                        estado                 = 'activa',
                        fecha_inicio           = EXCLUDED.fecha_inicio,
                        fecha_renovacion       = EXCLUDED.fecha_renovacion,
                        monto_mensual          = EXCLUDED.monto_mensual,
                        metodo_pago            = 'stripe'
                """, (org_id, plan_codigo, customer_id, subscription_id,
                      now, now + timedelta(days=30),
                      PLANES.get(plan_codigo, PLANES["basico"])["precio"]))
                cur.execute(
                    "UPDATE organizacion SET plan = %s WHERE organizacion_id = %s",
                    (plan_codigo, org_id)
                )
                db.commit()

        elif event["type"] == "customer.subscription.updated":
            sub    = event["data"]["object"]
            status = sub["status"]
            estado_local = (
                "activa"    if status == "active"   else
                "cancelada" if status == "canceled" else
                "vencida"
            )
            period_end       = sub.get("current_period_end")
            fecha_renovacion = (datetime.fromtimestamp(period_end, tz=timezone.utc)
                                if period_end else None)
            plan_codigo      = sub.get("metadata", {}).get("plan_codigo")

            fields = ["estado = %s", "fecha_renovacion = %s"]
            vals   = [estado_local, fecha_renovacion]
            if plan_codigo:
                fields.append("plan_codigo = %s")
                vals.append(plan_codigo)
            vals.append(sub["id"])
            cur.execute(
                f"UPDATE suscripcion SET {', '.join(fields)} WHERE stripe_subscription_id = %s",
                vals,
            )
            db.commit()

        elif event["type"] == "customer.subscription.deleted":
            cur.execute(
                "UPDATE suscripcion SET estado = 'cancelada' WHERE stripe_subscription_id = %s",
                (event["data"]["object"]["id"],),
            )
            db.commit()

        return {"success": True, "message": "Webhook procesado"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error en webhook: {str(e)}")
    finally:
        cur.close()


@router.get("/admin/todas")
async def todas_suscripciones(
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """SuperAdmin: lista todas las organizaciones con su estado de suscripción."""
    payload = get_token_payload(authorization)
    if payload.get("rol") != "administrador":
        raise HTTPException(status_code=403, detail="Acceso exclusivo para SuperAdministrador")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        _ensure_suscripcion_table(cur)
        db.commit()
        cur.execute("""
            SELECT
                o.organizacion_id,
                o.nombre,
                o.estado          AS estado_org,
                o.email_contacto,
                COALESCE(s.plan_codigo, o.plan, 'basico') AS plan_codigo,
                COALESCE(s.estado, 'activa')              AS estado_suscripcion,
                s.fecha_inicio,
                s.fecha_renovacion,
                COALESCE(s.monto_mensual, 0)              AS monto_mensual,
                s.metodo_pago,
                (SELECT COUNT(*) FROM taller t
                 WHERE t.organizacion_id = o.organizacion_id)           AS total_talleres,
                (SELECT COUNT(DISTINCT te.tecnico_id)
                 FROM tecnico te
                 JOIN taller ta ON ta.taller_id = te.taller_id
                 WHERE ta.organizacion_id = o.organizacion_id)          AS total_tecnicos
            FROM organizacion o
            LEFT JOIN suscripcion s ON s.organizacion_id = o.organizacion_id
            ORDER BY o.nombre
        """)
        rows = cur.fetchall()

        result = []
        for r in rows:
            plan_codigo = r["plan_codigo"] or "basico"
            plan_info   = PLANES.get(plan_codigo, PLANES["basico"])
            result.append({
                **dict(r),
                "plan_nombre":     plan_info["nombre"],
                "plan_precio":     plan_info["precio"],
                "plan_color":      plan_info["color"],
                "fecha_inicio":    r["fecha_inicio"].isoformat() if r["fecha_inicio"] else None,
                "fecha_renovacion":r["fecha_renovacion"].isoformat() if r["fecha_renovacion"] else None,
                "monto_mensual":   float(r["monto_mensual"] or 0),
            })

        return {"success": True, "total": len(result), "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        cur.close()
