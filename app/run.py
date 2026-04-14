import os
import uvicorn
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.routes.auth_router import router as auth_router
from app.services.config import Config
from app.classes.postgresql import Database

app = FastAPI(
    title="Asistencia Vehicular API",
    description="API REST para plataforma de asistencia vehicular con IA",
    version="1.0.0"
)

# Configurar CORS para permitir solicitudes del frontend
_frontend_url = os.getenv("FRONTEND_URL", "https://asistencia-vehicular-frontend.onrender.com")
_allowed_origins = list({
    "http://localhost:4200",
    "http://localhost:3000",
    "http://127.0.0.1:4200",
    "https://asistencia-vehicular-frontend.onrender.com",
    _frontend_url,
})

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    expose_headers=["*"],
    allow_headers=["*"],
)

# Handler global: captura cualquier excepción no manejada
# Esto garantiza que los headers CORS estén presentes incluso en errores 500
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Error interno del servidor"},
    )

# Incluir routers
app.include_router(auth_router)

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