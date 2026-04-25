from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from typing import Optional
import cloudinary
import cloudinary.uploader

from ..classes.postgresql import Database
from ..services.config import Config

router = APIRouter(prefix="/api/emergencia", tags=["Emergencias"])

cloudinary.config(
    cloud_name=Config.CLOUDINARY_CLOUD_NAME,
    api_key=Config.CLOUDINARY_API_KEY,
    api_secret=Config.CLOUDINARY_API_SECRET,
    secure=True,
)

# ===================== MODELOS =====================

class EmergenciaCreate(BaseModel):
    usuario_id: int
    vehiculo_id: int
    descripcion: str
    latitud: float
    longitud: float
    tipo_problema: Optional[str] = None
    imagen_path: Optional[str] = None
    audio_path: Optional[str] = None

# ===================== ENDPOINTS =====================
@router.post("/subir-imagen")
async def subir_imagen(imagen: UploadFile = File(...)):
    """Sube una imagen del incidente a Cloudinary"""
    try:
        contenido = await imagen.read()
        resultado = cloudinary.uploader.upload(
            contenido,
            folder="imagenes_incidentes",
            resource_type="image",
        )
        url = resultado["secure_url"]
        nombre = resultado["public_id"].split("/")[-1]
        return {
            "success": True,
            "imagen_path": url,
            "nombre": nombre,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/subir-audio")
async def subir_audio(audio: UploadFile = File(...)):
    """Sube un audio del incidente a Cloudinary"""
    try:
        contenido = await audio.read()
        resultado = cloudinary.uploader.upload(
            contenido,
            folder="audios_incidentes",
            resource_type="video",
        )
        url = resultado["secure_url"]
        nombre = resultado["public_id"].split("/")[-1]
        return {
            "success": True,
            "audio_path": url,
            "nombre": nombre,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/registrar")
async def registrar_emergencia(data: EmergenciaCreate, db=Depends(Database.get_db)):
    """Registra una nueva emergencia vehicular"""
    cur = db.cursor()
    try:
        # Verificar que el vehículo pertenece al usuario
        cur.execute("""
            SELECT vehiculo_id FROM VEHICULO 
            WHERE vehiculo_id = %s AND usuario_id = %s
        """, (data.vehiculo_id, data.usuario_id))

        if not cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="El vehículo no pertenece al usuario"
            )

        # Registrar incidente
        cur.execute("""
            INSERT INTO INCIDENTE (
                usuario_id, vehiculo_id, descripcion,
                latitud, longitud, estado, prioridad,
                imagen_path, audio_path
            )
            VALUES (%s, %s, %s, %s, %s, 'pendiente', 'normal', %s, %s)
            RETURNING incidente_id
        """, (
            data.usuario_id,
            data.vehiculo_id,
            data.descripcion,
            data.latitud,
            data.longitud,
            data.imagen_path,
            data.audio_path
        ))

        incidente_id = cur.fetchone()[0]

        # Vincular servicios requeridos según tipo_problema para filtrado posterior
        if data.tipo_problema:
            TIPO_MAP = {
                'batería': 'ELECTRICO', 'bateria': 'ELECTRICO',
                'llanta':  'AUXILIO',
                'motor':   'MECANICA',
                'choque':  'GRUA',
                'otros':   'OTROS',
            }
            categoria_match = TIPO_MAP.get(data.tipo_problema.strip().lower(), 'OTROS')
            cur.execute("""
                INSERT INTO INCIDENTE_SERVICIO (incidente_id, servicio_id, recomendado_por_ia)
                SELECT %s, s.servicio_id, TRUE FROM SERVICIO s
                WHERE UPPER(TRIM(s.categoria)) = %s
                ON CONFLICT (incidente_id, servicio_id) DO NOTHING
            """, (incidente_id, categoria_match))

        db.commit()

        return {
            "success": True,
            "message": "Emergencia registrada correctamente",
            "incidente_id": incidente_id
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()


@router.get("/listar/{usuario_id}")
async def listar_emergencias(usuario_id: int, db=Depends(Database.get_db)):
    """Lista las emergencias de un cliente"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                i.incidente_id,
                i.descripcion,
                i.estado,
                i.prioridad,
                i.fecha_creacion,
                v.marca,
                v.modelo,
                v.placa
            FROM INCIDENTE i
            LEFT JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            WHERE i.usuario_id = %s
            ORDER BY i.fecha_creacion DESC
        """, (usuario_id,))

        emergencias = cur.fetchall()
        return {"success": True, "emergencias": [dict(e) for e in emergencias]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
@router.get("/detalle/{incidente_id}")
async def detalle_emergencia(incidente_id: int, db=Depends(Database.get_db)):
    """Obtiene el detalle y estado de una emergencia"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                i.incidente_id,
                i.descripcion,
                i.estado,
                i.prioridad,
                i.fecha_creacion,
                i.imagen_path,
                i.audio_path,
                v.marca,
                v.modelo,
                v.placa,
                a.tiempo_estimado_minutos,
                t.razon_social as taller_nombre,
                t.direccion as taller_direccion,
                t.telefono_operativo as taller_telefono
            FROM INCIDENTE i
            LEFT JOIN VEHICULO v ON i.vehiculo_id = v.vehiculo_id
            LEFT JOIN ASIGNACION a ON i.incidente_id = a.incidente_id
            LEFT JOIN TALLER t ON a.taller_id = t.taller_id
            WHERE i.incidente_id = %s
        """, (incidente_id,))
        incidente = cur.fetchone()

        if not incidente:
            raise HTTPException(status_code=404, detail="Emergencia no encontrada")

        return {"success": True, "incidente": dict(incidente)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()       