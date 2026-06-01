import jwt
from fastapi import WebSocket, WebSocketDisconnect, Query
from typing import Dict, Set
from ..services.config import Config


class ConnectionManager:
    def __init__(self):
        self.active: Dict[int, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, usuario_id: int):
        await websocket.accept()
        if usuario_id not in self.active:
            self.active[usuario_id] = set()
        self.active[usuario_id].add(websocket)
        print(f"[WS] Usuario {usuario_id} conectado. Total conexiones: {len(self.active)}")

    def disconnect(self, websocket: WebSocket, usuario_id: int):
        if usuario_id in self.active:
            self.active[usuario_id].discard(websocket)
            if not self.active[usuario_id]:
                del self.active[usuario_id]
        print(f"[WS] Usuario {usuario_id} desconectado.")

    async def send_to_user(self, usuario_id: int, data: dict):
        """Envía un mensaje a todas las conexiones activas de un usuario."""
        if usuario_id in self.active:
            dead = set()
            for ws in self.active[usuario_id]:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                self.active[usuario_id].discard(ws)

    async def broadcast(self, data: dict):
        """Envía un mensaje a todos los usuarios conectados."""
        for usuario_id in list(self.active.keys()):
            await self.send_to_user(usuario_id, data)

    async def forward_to_incident_client(self, tecnico_usuario_id: int, data: dict, db):
        """Reenvía la ubicación del técnico al cliente Y al taller del incidente."""
        try:
            incidente_id = data.get("incidente_id")
            if not incidente_id:
                return
            from psycopg2.extras import RealDictCursor
            cur = db.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT i.usuario_id AS cliente_uid,
                       ta.usuario_id AS taller_uid
                FROM INCIDENTE i
                JOIN ASIGNACION a ON a.incidente_id = i.incidente_id
                JOIN TECNICO tc ON a.tecnico_id = tc.tecnico_id
                JOIN TALLER ta ON tc.taller_id = ta.taller_id
                WHERE i.incidente_id = %s
                  AND tc.usuario_id = %s
                  AND a.estado IN ('en_camino', 'en_servicio')
            """, (incidente_id, tecnico_usuario_id))
            row = cur.fetchone()
            cur.close()
            if row:
                # Reenviar al cliente
                await self.send_to_user(row["cliente_uid"], data)
                # Reenviar al taller
                await self.send_to_user(row["taller_uid"], data)
                print(f"[WS] Ubicación reenviada a cliente {row['cliente_uid']} y taller {row['taller_uid']}")
        except Exception as e:
            print(f"[WS] Error reenviando ubicación: {e}")


# Instancia global — se importa desde cualquier router
manager = ConnectionManager()