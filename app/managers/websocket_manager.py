import jwt
from fastapi import WebSocket, WebSocketDisconnect, Query
from typing import Dict, Set
from ..services.config import Config


class ConnectionManager:
    def __init__(self):
        # usuario_id -> set de websockets activos
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


# Instancia global — se importa desde cualquier router
manager = ConnectionManager()