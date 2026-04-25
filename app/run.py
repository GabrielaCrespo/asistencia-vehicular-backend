import os
import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from .routes.auth_router import router as auth_router
from .routes.cliente_router import router as cliente_router
from .routes.vehiculo_router import router as vehiculo_router
from .routes.emergencia_router import router as emergencia_router
from .routes.asignacion_router import router as asignacion_router
from .routes.tecnicos_router import router as tecnicos_router
from .routes.servicios_router import router as servicios_router
from .routes.talleres_router import router as talleres_router
from .services.config import Config
from .classes.postgresql import Database


app = FastAPI(
    title="Asistencia Vehicular API",
    description="API REST para plataforma de asistencia vehicular con IA",
    version="1.0.0"
)

# Configurar CORS para permitir solicitudes del frontend
#_frontend_url = os.getenv("FRONTEND_URL", "https://asistencia-vehicular-frontend.onrender.com")
#_allowed_origins = list({
#   "http://localhost:4200",
#    "http://localhost:3000",
#    "http://127.0.0.1:4200",
#    "http://localhost:8080",
#    "http://localhost:60600",
#    "http://127.0.0.1:60600",
#    "http://localhost:*",
#    "https://asistencia-vehicular-frontend.onrender.com",
#    _frontend_url,
#})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Handler global: garantiza headers CORS en todos los errores.
# Re-delega HTTPException al handler nativo de FastAPI para que
# el status code y el detail originales lleguen al cliente.
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        return await http_exception_handler(request, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Error interno del servidor"},
    )

# Incluir routers
app.include_router(auth_router)
app.include_router(cliente_router)
app.include_router(vehiculo_router)
app.include_router(emergencia_router)
app.include_router(asignacion_router)
app.include_router(tecnicos_router)
app.include_router(servicios_router)
app.include_router(talleres_router)


@app.get("/")
def index():
    return {
        "message": "Servidor de Asistencia Vehicular Activo",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
def health_check(db=Depends(Database.get_db)):
    """
    Health check avanzado que verifica:
    - Servidor levantado
    - Conexión a BD
    - Base de datos accesible
    """
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