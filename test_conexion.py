"""Script de diagnostico - ejecutar desde la carpeta backend/"""
import sys
import os
import socket

# Datos de conexion
HOST = "dpg-d7e7jq9f9bms738hq3q0-a.oregon-postgres.render.com"
PORT = 5432
DB   = "asistencia_db_x7gq"
USER = "asistencia_db_x7gq_user"
PASS = "fKKjjSlVh1RWPY0Dfa2Ln6T1IPodS59x"

print(f"Python {sys.version}")
print(f"Sistema: {sys.platform}")
print()

# --- Test 1: TCP ---
print("TEST 1: Conexion TCP al host...")
try:
    s = socket.create_connection((HOST, PORT), timeout=10)
    s.close()
    print("  OK - Puerto 5432 accesible")
except Exception as e:
    print(f"  FALLO - No se puede llegar al host: {e}")
    sys.exit(1)

# --- Test 2: psycopg2 minimo ---
print()
print("TEST 2: psycopg2 sin parametros extra...")
import psycopg2
try:
    conn = psycopg2.connect(host=HOST, port=PORT, database=DB, user=USER, password=PASS)
    conn.close()
    print("  OK - Conexion basica funciona")
except Exception as e:
    print(f"  FALLO: {type(e).__name__}: {e}")

# --- Test 3: psycopg2 con URL string ---
print()
print("TEST 3: psycopg2 con URL string...")
try:
    import urllib.parse
    p = urllib.parse.quote(PASS, safe='')
    url = f"postgresql://{USER}:{p}@{HOST}:{PORT}/{DB}"
    conn = psycopg2.connect(url)
    conn.close()
    print("  OK - Conexion por URL funciona")
except Exception as e:
    print(f"  FALLO: {type(e).__name__}: {e}")

# --- Test 4: con PYTHONUTF8=1 ---
print()
print("TEST 4: forzando UTF-8 en Python...")
os.environ["PYTHONUTF8"] = "1"
os.environ["PGPASSFILE"] = "NUL"
os.environ["PGSYSCONFDIR"] = "C:\\"
try:
    conn = psycopg2.connect(host=HOST, port=PORT, database=DB, user=USER, password=PASS)
    conn.close()
    print("  OK - Con PYTHONUTF8=1 funciona")
except Exception as e:
    print(f"  FALLO: {type(e).__name__}: {e}")

print()
print("Diagnostico completo.")
