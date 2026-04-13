import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.auth_router import router as auth_router
from app.services.config import Config

app = FastAPI(
    title="Asistencia Vehicular API",
    description="API REST para plataforma de asistencia vehicular con IA",
    version="1.0.0"
)

# Configurar CORS para permitir solicitudes del frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",      # Frontend dev
        "http://localhost:3000",      # Alternativo
        "http://127.0.0.1:4200",
        "https://asistencia-vehicular-frontend.onrender.com",  # Producción
    ],
    allow_credentials=True,
    allow_methods=["*"],
    expose_headers=["*"],
    allow_headers=["*"],
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
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run("app.run:app", host="0.0.0.0", port=8000, reload=True)