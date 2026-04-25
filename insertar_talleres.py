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

# Insertar usuarios para los talleres
cur.execute("""
    INSERT INTO USUARIO (rol_id, nombre, email, telefono, contrasena_hash, estado, documento_identidad)
    VALUES 
    (2, 'Taller Mecánico Rápido', 'taller.rapido@example.com', '77712345', '$2b$12$iJC5K8eXmN2qLpQ7rS9To.LwUzF0mKpL9vWxYzT3H5jGkL6FqP2K.', 'activo', '12345678'),
    (2, 'Taller Santa Cruz', 'taller.sc@example.com', '77798765', '$2b$12$iJC5K8eXmN2qLpQ7rS9To.LwUzF0mKpL9vWxYzT3H5jGkL6FqP2K.', 'activo', '87654321'),
    (2, 'Taller AutoExpert', 'taller.autoexpert@example.com', '77756789', '$2b$12$iJC5K8eXmN2qLpQ7rS9To.LwUzF0mKpL9vWxYzT3H5jGkL6FqP2K.', 'activo', '56789012')
    ON CONFLICT (email) DO NOTHING
    RETURNING usuario_id;
""")

usuarios = cur.fetchall()
print(f"Usuarios insertados: {usuarios}")

# Insertar talleres con coordenadas de Santa Cruz
cur.execute("""
    INSERT INTO TALLER (usuario_id, razon_social, direccion, latitud, longitud, telefono_operativo, horario_inicio, horario_fin, disponible)
    SELECT u.usuario_id, 
           CASE u.email 
               WHEN 'taller.rapido@example.com' THEN 'Taller Mecánico Rápido 24H'
               WHEN 'taller.sc@example.com' THEN 'Taller Santa Cruz'
               WHEN 'taller.autoexpert@example.com' THEN 'Taller AutoExpert'
           END,
           CASE u.email
               WHEN 'taller.rapido@example.com' THEN 'Av. Cañoto #123, Santa Cruz'
               WHEN 'taller.sc@example.com' THEN 'Av. Busch #456, Santa Cruz'
               WHEN 'taller.autoexpert@example.com' THEN 'Av. Cristo Redentor #789, Santa Cruz'
           END,
           CASE u.email
               WHEN 'taller.rapido@example.com' THEN -17.7800
               WHEN 'taller.sc@example.com' THEN -17.7900
               WHEN 'taller.autoexpert@example.com' THEN -17.7700
           END,
           CASE u.email
               WHEN 'taller.rapido@example.com' THEN -63.1800
               WHEN 'taller.sc@example.com' THEN -63.1900
               WHEN 'taller.autoexpert@example.com' THEN -63.1700
           END,
           u.telefono, '08:00:00', '20:00:00', TRUE
    FROM USUARIO u
    WHERE u.email IN ('taller.rapido@example.com', 'taller.sc@example.com', 'taller.autoexpert@example.com')
    ON CONFLICT (usuario_id) DO NOTHING;
""")

conn.commit()
cur.close()
conn.close()

print("✅ Talleres insertados correctamente")