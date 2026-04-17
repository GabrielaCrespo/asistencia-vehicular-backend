#!/usr/bin/env python3
"""
Script para prueba rápida de conexión a BD Render
"""
import sys
sys.path.insert(0, str(__file__).rsplit('\\', 1)[0])

from app.services.config import Config
from app.classes.postgresql import _get_pool

print("🔍 Verificando conexión a BD Render...\n")
print(f"  Host: {Config.DB_HOST}")
print(f"  BD:   {Config.DB_NAME}")
print(f"  User: {Config.DB_USER}")

try:
    pool = _get_pool()
    conn = pool.getconn()
    
    # Test simple
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    cursor.close()
    
    pool.putconn(conn)
    
    print("\n✅ Conexión exitosa a Render")
    print(f"✅ Pool activo con {pool.getconn().__class__.__name__}")
    
except Exception as e:
    print(f"\n❌ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
