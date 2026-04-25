from fastapi import APIRouter, HTTPException, Depends
from psycopg2.extras import RealDictCursor
import math

from ..classes.postgresql import Database

router = APIRouter(prefix="/api/talleres", tags=["Talleres"])

def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcula distancia en km entre dos coordenadas"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return round(R * c, 2)

@router.get("/candidatos")
async def obtener_talleres_candidatos(
    latitud: float,
    longitud: float,
    tipo_problema: str = None,
    db=Depends(Database.get_db)
):
    """Obtiene lista de talleres candidatos según ubicación y tipo de problema"""
    cur = db.cursor(cursor_factory=RealDictCursor)
    try:
        # Buscar talleres disponibles con sus servicios
        cur.execute("""
            SELECT DISTINCT
                t.taller_id,
                t.razon_social,
                t.direccion,
                t.latitud,
                t.longitud,
                t.telefono_operativo,
                t.horario_inicio,
                t.horario_fin,
                t.calificacion_promedio,
                t.disponible,
                array_agg(s.nombre) as servicios
            FROM TALLER t
            LEFT JOIN TALLER_SERVICIO ts ON t.taller_id = ts.taller_id AND ts.disponible = TRUE
            LEFT JOIN SERVICIO s ON ts.servicio_id = s.servicio_id
            WHERE t.disponible = TRUE
            AND t.latitud IS NOT NULL
            AND t.longitud IS NOT NULL
            GROUP BY t.taller_id
        """)

        talleres = cur.fetchall()
        talleres_lista = []

        # Mapear tipo de problema a categoría de servicio
        categoria_map = {
            'batería': 'electrica',
            'llanta': 'auxiliar',
            'motor': 'mecanica',
            'choque': 'mecanica',
            'otros': None
        }

        categoria_buscada = None
        if tipo_problema:
            categoria_buscada = categoria_map.get(tipo_problema.lower())

        for taller in talleres:
            taller_dict = dict(taller)
            
            # Calcular distancia
            if taller_dict['latitud'] and taller_dict['longitud']:
                distancia = calcular_distancia(
                    latitud, longitud,
                    float(taller_dict['latitud']),
                    float(taller_dict['longitud'])
                )
                taller_dict['distancia_km'] = distancia
                # Tiempo estimado (promedio 30km/h en ciudad)
                taller_dict['tiempo_minutos'] = round((distancia / 30) * 60)
            else:
                taller_dict['distancia_km'] = 999
                taller_dict['tiempo_minutos'] = 999

            talleres_lista.append(taller_dict)

        # Ordenar por distancia
        talleres_lista.sort(key=lambda x: x['distancia_km'])

        # Limitar a 10 talleres más cercanos
        talleres_lista = talleres_lista[:10]

        return {
            "success": True,
            "talleres": talleres_lista,
            "total": len(talleres_lista)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()