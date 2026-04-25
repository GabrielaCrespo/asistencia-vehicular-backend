import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS")
)

cur = conn.cursor()

# Actualizar coordenadas a Santa Cruz de la Sierra
cur.execute("""
    UPDATE TALLER 
    SET latitud = -17.7833, longitud = -63.1821
    WHERE taller_id = 1;
""")

conn.commit()
cur.close()
conn.close()

print("✅ Coordenadas actualizadas a Santa Cruz")