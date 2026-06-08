import json
import stripe
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

from ..classes.postgresql import Database
from ..services.config import Config
from ..utils.tenant_deps import get_token_payload

stripe.api_key = Config.STRIPE_SECRET_KEY

router = APIRouter(prefix="/api/stripe", tags=["Stripe Pagos"])


# ===================== MODELOS =====================

class PaymentIntentRequest(BaseModel):
    pago_id: int


class PaymentIntentResponse(BaseModel):
    client_secret: str
    payment_intent_id: str
    amount: int
    currency: str


class StripeConfigResponse(BaseModel):
    publishable_key: str


class PagoIncidenteResponse(BaseModel):
    pago_id: int
    monto_total: float
    estado: str


class ConfirmarPagoEfectivoRequest(BaseModel):
    pago_id: int
    metodo_pago: str  # efectivo | qr | transferencia


class MessageResponse(BaseModel):
    success: bool
    message: str


class VerificarPagoRequest(BaseModel):
    pago_id: int
    payment_intent_id: str
    tipo: str = "servicio"  # "servicio" | "comision"


# ===================== ENDPOINTS =====================

@router.get("/config", response_model=StripeConfigResponse)
def stripe_config():
    """Retorna la publishable key para que el frontend inicialice Stripe."""
    if not Config.STRIPE_PUBLISHABLE_KEY:
        raise HTTPException(status_code=503, detail="Stripe no configurado")
    return StripeConfigResponse(publishable_key=Config.STRIPE_PUBLISHABLE_KEY)


@router.get("/pago-incidente/{incidente_id}", response_model=PagoIncidenteResponse)
def get_pago_for_incidente(
    incidente_id: int,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """Retorna el pago asociado a un incidente. Usado por la app móvil."""
    get_token_payload(authorization)
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT pago_id, monto_total, estado FROM PAGO WHERE incidente_id = %s",
            (incidente_id,),
        )
        pago = cur.fetchone()
        if not pago:
            raise HTTPException(status_code=404, detail="No se encontró un pago para este incidente")
        return PagoIncidenteResponse(
            pago_id=pago["pago_id"],
            monto_total=float(pago["monto_total"]),
            estado=pago["estado"],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.post("/payment-intent", response_model=PaymentIntentResponse)
def create_payment_intent(
    body: PaymentIntentRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Crea un PaymentIntent de Stripe para el pago de un servicio (cliente).
    El frontend usa el client_secret para completar el pago con Stripe.js / SDK móvil.
    """
    get_token_payload(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT p.pago_id, p.monto_total, p.estado, p.stripe_payment_intent_id
            FROM PAGO p
            WHERE p.pago_id = %s
            """,
            (body.pago_id,),
        )
        pago = cur.fetchone()
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado")

        if pago["estado"] == "completado":
            raise HTTPException(status_code=400, detail="Este pago ya fue completado")

        if pago.get("stripe_payment_intent_id"):
            try:
                existing = stripe.PaymentIntent.retrieve(pago["stripe_payment_intent_id"])
                if existing.status not in ("canceled", "succeeded"):
                    return PaymentIntentResponse(
                        client_secret=existing.client_secret,
                        payment_intent_id=existing.id,
                        amount=existing.amount,
                        currency=existing.currency,
                    )
            except stripe.StripeError:
                pass

        amount_cents = int(float(pago["monto_total"]) * 100)
        if amount_cents < 50:
            raise HTTPException(status_code=400, detail="El monto mínimo es 0.50")

        intent = stripe.PaymentIntent.create(
            amount=amount_cents,
            currency=Config.STRIPE_CURRENCY,
            metadata={"pago_id": str(body.pago_id), "tipo": "servicio"},
            automatic_payment_methods={"enabled": True},
        )

        cur.execute(
            "UPDATE PAGO SET stripe_payment_intent_id = %s WHERE pago_id = %s",
            (intent.id, body.pago_id),
        )
        db.commit()

        return PaymentIntentResponse(
            client_secret=intent.client_secret,
            payment_intent_id=intent.id,
            amount=intent.amount,
            currency=intent.currency,
        )
    except HTTPException:
        db.rollback()
        raise
    except stripe.StripeError as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Error de Stripe: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creando PaymentIntent: {str(e)}")
    finally:
        cur.close()


@router.post("/commission-payment-intent", response_model=PaymentIntentResponse)
def create_commission_payment_intent(
    body: PaymentIntentRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Crea un PaymentIntent de Stripe para el pago de la comisión de plataforma (taller).
    Usado desde el panel web del taller.
    """
    get_token_payload(authorization)

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT pago_id, comision_plataforma, estado_comision FROM PAGO WHERE pago_id = %s",
            (body.pago_id,),
        )
        pago = cur.fetchone()
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado")
        if pago["estado_comision"] == "pagado":
            raise HTTPException(status_code=400, detail="La comisión de este pago ya fue pagada")

        amount_cents = int(float(pago["comision_plataforma"]) * 100)
        if amount_cents < 50:
            raise HTTPException(status_code=400, detail="El monto mínimo es 0.50")

        intent = stripe.PaymentIntent.create(
            amount=amount_cents,
            currency=Config.STRIPE_CURRENCY,
            metadata={"pago_id": str(body.pago_id), "tipo": "comision"},
            automatic_payment_methods={"enabled": True},
        )

        return PaymentIntentResponse(
            client_secret=intent.client_secret,
            payment_intent_id=intent.id,
            amount=intent.amount,
            currency=intent.currency,
        )
    except HTTPException:
        raise
    except stripe.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Error de Stripe: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.post("/confirmar-pago-efectivo", response_model=MessageResponse)
def confirmar_pago_efectivo(
    body: ConfirmarPagoEfectivoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    El cliente confirma el pago presencial (efectivo, QR, transferencia).
    Marca el PAGO como completado desde la app móvil.
    """
    get_token_payload(authorization)

    metodos_validos = {"efectivo", "qr", "transferencia"}
    if body.metodo_pago not in metodos_validos:
        raise HTTPException(status_code=400, detail="Método de pago no válido")

    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT pago_id, estado FROM PAGO WHERE pago_id = %s",
            (body.pago_id,),
        )
        pago = cur.fetchone()
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado")
        if pago["estado"] == "completado":
            raise HTTPException(status_code=400, detail="Este pago ya fue completado")

        cur.execute(
            """
            UPDATE PAGO
            SET estado      = 'completado',
                metodo_pago = %s,
                fecha_pago  = CURRENT_TIMESTAMP
            WHERE pago_id = %s
            """,
            (body.metodo_pago, body.pago_id),
        )
        db.commit()
        return MessageResponse(success=True, message="Pago registrado correctamente")
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.post("/verificar-pago", response_model=MessageResponse)
def verificar_pago(
    body: VerificarPagoRequest,
    authorization: str = Header(None),
    db=Depends(Database.get_db),
):
    """
    Verifica con la API de Stripe que el PaymentIntent fue exitoso y actualiza la BD.
    Usado como fallback directo desde el frontend, sin depender del webhook.
    """
    get_token_payload(authorization)

    try:
        intent = stripe.PaymentIntent.retrieve(body.payment_intent_id)
    except stripe.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Error de Stripe: {str(e)}")

    if intent.status != "succeeded":
        raise HTTPException(
            status_code=400,
            detail=f"El pago no fue confirmado por Stripe (estado: {intent.status})"
        )

    cur = db.cursor()
    try:
        if body.tipo == "comision":
            cur.execute(
                """
                UPDATE PAGO
                SET estado_comision     = 'pagado',
                    fecha_pago_comision = CURRENT_TIMESTAMP
                WHERE pago_id = %s AND estado_comision != 'pagado'
                """,
                (body.pago_id,),
            )
        else:
            cur.execute(
                """
                UPDATE PAGO
                SET estado      = 'completado',
                    metodo_pago = 'stripe',
                    fecha_pago  = CURRENT_TIMESTAMP
                WHERE pago_id = %s AND estado != 'completado'
                """,
                (body.pago_id,),
            )
        db.commit()
        return MessageResponse(success=True, message="Pago verificado y registrado correctamente")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.post("/webhook", response_model=MessageResponse)
async def stripe_webhook(request: Request, db=Depends(Database.get_db)):
    """
    Webhook de Stripe. Confirma el pago y actualiza el estado en la BD.
    Configura la URL en el dashboard de Stripe: POST /api/stripe/webhook
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if Config.STRIPE_WEBHOOK_SECRET:
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, Config.STRIPE_WEBHOOK_SECRET
            )
        except stripe.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Firma de webhook inválida")
    else:
        event = json.loads(payload)

    if event["type"] == "payment_intent.succeeded":
        payment_intent = event["data"]["object"]
        metadata = payment_intent.get("metadata", {})
        pago_id = metadata.get("pago_id")
        tipo = metadata.get("tipo", "servicio")

        if pago_id:
            cur = db.cursor()
            try:
                if tipo == "comision":
                    cur.execute(
                        """
                        UPDATE PAGO
                        SET estado_comision      = 'pagado',
                            fecha_pago_comision  = CURRENT_TIMESTAMP
                        WHERE pago_id = %s AND estado_comision != 'pagado'
                        """,
                        (int(pago_id),),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE PAGO
                        SET estado      = 'completado',
                            metodo_pago = 'stripe',
                            fecha_pago  = CURRENT_TIMESTAMP
                        WHERE pago_id = %s AND estado != 'completado'
                        """,
                        (int(pago_id),),
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                cur.close()

    return MessageResponse(success=True, message="Webhook recibido")
