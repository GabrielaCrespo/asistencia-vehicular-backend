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

cur.execute("""
    ALTER TABLE TECNICO 
    ADD COLUMN IF NOT EXISTS usuario_id INTEGER REFERENCES USUARIO(usuario_id);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Columna usuario_id agregada a TECNICO")