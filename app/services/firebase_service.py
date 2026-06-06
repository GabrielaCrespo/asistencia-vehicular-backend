import firebase_admin
from firebase_admin import credentials, messaging
import os
import json

# Inicializar Firebase Admin SDK
if not firebase_admin._apps:
    # Intentar desde variable de entorno (producción en Render)
    firebase_creds_json = os.getenv('FIREBASE_CREDENTIALS')
    if firebase_creds_json:
        cred_dict = json.loads(firebase_creds_json)
        cred = credentials.Certificate(cred_dict)
    else:
        # Fallback: archivo local (desarrollo)
        _cred_path = os.path.join(os.path.dirname(__file__), 'firebase_credentials.json')
        cred = credentials.Certificate(_cred_path)

    firebase_admin.initialize_app(cred)


def enviar_notificacion(fcm_token: str, titulo: str, cuerpo: str, datos: dict = None):
    """Envía una notificación push a un dispositivo específico."""
    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title=titulo,
                body=cuerpo,
            ),
            data=datos or {},
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority='high',
                notification=messaging.AndroidNotification(
                    sound='default',
                    priority='high',
                ),
            ),
        )
        response = messaging.send(message)
        print(f'[FCM] Notificación enviada: {response}')
        return True
    except Exception as e:
        print(f'[FCM] Error enviando notificación: {e}')
        return False


def enviar_notificacion_usuario(usuario_id: int, titulo: str, cuerpo: str, datos: dict = None, db=None):
    """Envía notificación push a todos los tokens FCM de un usuario."""
    if db is None:
        return
    try:
        cur = db.cursor()
        cur.execute(
            'SELECT token FROM fcm_token WHERE usuario_id = %s',
            (usuario_id,)
        )
        tokens = [r[0] for r in cur.fetchall()]
        cur.close()

        for token in tokens:
            enviar_notificacion(token, titulo, cuerpo, datos)
    except Exception as e:
        print(f'[FCM] Error obteniendo tokens: {e}')