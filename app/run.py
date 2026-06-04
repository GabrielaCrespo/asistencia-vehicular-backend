import os
import jwt
import uvicorn
import json
from fastapi import FastAPI, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from fastapi.staticfiles import StaticFiles
from .routes.auth_router import router as auth_router
from .routes.cliente_router import router as cliente_router
from .routes.vehiculo_router import router as vehiculo_router
from .routes.emergencia_router import router as emergencia_router
from .routes.asignacion_router import router as asignacion_router
from .routes.tecnicos_router import router as tecnicos_router
from .routes.servicios_router import router as servicios_router
from .routes.talleres_router import router as talleres_router
from .routes.pagos_router import router as pagos_router
from .routes.tecnico_auth_router import router as tecnico_auth_router
from .routes.notificaciones_router import router as notificaciones_router
from .routes.historial_router import router as historial_router
from .routes.organizacion_router import router as organizacion_router
from .routes.cotizacion_router import router as cotizacion_router
from .services.config import Config
from .classes.postgresql import Database
from .managers.websocket_manager import manager


app = FastAPI(
    title="Asistencia Vehicular API",
    description="API REST para plataforma de asistencia vehicular con IA",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        return await http_exception_handler(request, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Error interno del servidor"},
    )

# Servir archivos subidos
_img_dir = os.path.join(os.path.dirname(__file__), "..", "imagenes_incidentes")
os.makedirs(_img_dir, exist_ok=True)
app.mount("/imagenes", StaticFiles(directory=_img_dir), name="imagenes")

# Incluir routers
app.include_router(auth_router)
app.include_router(cliente_router)
app.include_router(vehiculo_router)
app.include_router(emergencia_router)
app.include_router(asignacion_router)
app.include_router(tecnicos_router)
app.include_router(servicios_router)
app.include_router(talleres_router)
app.include_router(pagos_router)
app.include_router(tecnico_auth_router)
app.include_router(notificaciones_router)
app.include_router(historial_router)
app.include_router(organizacion_router)
app.include_router(cotizacion_router)


# ===================== WEBSOCKET =====================
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...)
):
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
        usuario_id = int(payload.get("sub"))
    except Exception:
        await websocket.close(code=1008)
        return

    await manager.connect(websocket, usuario_id)
    try:
        await websocket.send_json({
            "tipo": "conexion_establecida",
            "mensaje": "Conectado al sistema de tiempo real",
            "usuario_id": usuario_id
        })
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"tipo": "pong"})
            else:
                try:
                    import json
                    msg = json.loads(data)
                    if msg.get("tipo") == "ubicacion_tecnico":
                        db = next(Database.get_db())
                        await manager.forward_to_incident_client(usuario_id, msg, db)
                except Exception:
                    pass
    except WebSocketDisconnect:
        manager.disconnect(websocket, usuario_id)


@app.get("/")
def index():
    return {
        "message": "Servidor de Asistencia Vehicular Activo",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
def health_check(db=Depends(Database.get_db)):
    try:
        cur = db.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return {
            "status": "healthy",
            "database": "connected",
            "version": "1.0.0"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }, 503

if __name__ == "__main__":
    uvicorn.run("app.run:app", host="0.0.0.0", port=8000, reload=True)