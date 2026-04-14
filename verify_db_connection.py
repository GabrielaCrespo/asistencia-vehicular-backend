#!/usr/bin/env python3
"""
SCRIPT DE VERIFICACIÓN DE CONEXIÓN A BD
=========================================

Ejecutar:
  cd backend
  python verify_db_connection.py

Este script verifica:
1. Que las variables de ambiente estén configuradas
2. Que la conexión a BD funciona
3. Que la estructura de tablas existe
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

# Colores para terminal
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(title):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}{title}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}\n")

def print_ok(msg):
    print(f"{Colors.GREEN}✅ {msg}{Colors.RESET}")

def print_error(msg):
    print(f"{Colors.RED}❌ {msg}{Colors.RESET}")

def print_warning(msg):
    print(f"{Colors.YELLOW}⚠️  {msg}{Colors.RESET}")

def print_info(msg):
    print(f"{Colors.BLUE}ℹ️  {msg}{Colors.RESET}")

# Cargar variables de ambiente
from dotenv import load_dotenv
load_dotenv()

from app.services.config import Config

# ============================================================
# PASO 1: Verificar variables de ambiente
# ============================================================
print_header("PASO 1: VERIFICACIÓN DE VARIABLES DE AMBIENTE")

env_vars = {
    'DB_HOST': Config.DB_HOST,
    'DB_PORT': Config.DB_PORT,
    'DB_NAME': Config.DB_NAME,
    'DB_USER': Config.DB_USER,
    'DB_PASS': '****' if Config.DB_PASS else 'NO CONFIGURADA',
    'SECRET_KEY': '****' if Config.SECRET_KEY else 'NO CONFIGURADA',
}

all_configured = True
for var_name, var_value in env_vars.items():
    if var_value and var_value != 'NO CONFIGURADA':
        display_value = var_value if 'PASS' not in var_name and 'KEY' not in var_name else '****'
        print_ok(f"{var_name} = {display_value}")
    else:
        print_error(f"{var_name} = NO CONFIGURADA")
        all_configured = False

if not all_configured:
    print_error("\n⚠️  ALGUNAS VARIABLES NO ESTÁN CONFIGURADAS")
    print_info("En desarrollo, usar archivo .env en la carpeta backend/")
    print_info("En Render, ir a Environment Variables en el dashboard")
    sys.exit(1)

# ============================================================
# PASO 2: Intentar conexión a BD
# ============================================================
print_header("PASO 2: INTENTO DE CONEXIÓN A BASE DE DATOS")

print_info(f"Conectando a: {Config.DB_USER}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")

try:
    conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=int(Config.DB_PORT),
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASS,
        sslmode="require",
        connect_timeout=10
    )
    print_ok("Connection exitosa a PostgreSQL")
    
except psycopg2.OperationalError as e:
    print_error(f"Error operacional: {str(e)}")
    print_error("\nPosibles causas:")
    print_error("  - Host incorrecto")
    print_error("  - Puerto incorrecto")
    print_error("  - User incorrecto")
    print_error("  - Password incorrecto")
    print_error("  - BD no existe")
    print_error("  - SSL/sslmode issue")
    sys.exit(1)
    
except Exception as e:
    print_error(f"Error inesperado: {type(e).__name__}: {str(e)}")
    sys.exit(1)

# ============================================================
# PASO 3: Verificar estructura de BD
# ============================================================
print_header("PASO 3: VERIFICACIÓN DE ESTRUCTURA DE BD")

try:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Listar tablas
    print_info("Buscando tablas en la BD...")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = cur.fetchall()
    if tables:
        print_ok(f"Se encontraron {len(tables)} tablas:")
        for table in tables:
            print(f"   • {table['table_name']}")
    else:
        print_warning("No hay tablas en la BD (podría estar vacía)")
    
    # Verificar tabla USUARIO
    print_info("\nVerificando tabla USUARIO...")
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'usuario'
        )
    """)
    
    if cur.fetchone()[0]:
        print_ok("Tabla USUARIO existe")
        
        # Contar usuarios
        cur.execute("SELECT COUNT(*) as total FROM usuario")
        count = cur.fetchone()['total']
        print_info(f"  Registros de usuarios: {count}")
    else:
        print_error("Tabla USUARIO NO existe (necesaria para login)")
    
    # Verificar tabla TALLER
    print_info("\nVerificando tabla TALLER...")
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'taller'
        )
    """)
    
    if cur.fetchone()[0]:
        print_ok("Tabla TALLER existe")
        
        # Contar talleres
        cur.execute("SELECT COUNT(*) as total FROM taller")
        count = cur.fetchone()['total']
        print_info(f"  Registros de talleres: {count}")
    else:
        print_warning("Tabla TALLER no existe (opcional)")
    
    cur.close()
    
except Exception as e:
    print_error(f"Error verificando estructura: {str(e)}")
    sys.exit(1)

# ============================================================
# PASO 4: Probar query de login
# ============================================================
print_header("PASO 4: PRUEBA DE QUERY DE LOGIN")

print_info("Intentando búsqueda de usuario para login...")

try:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Query del login (igual al backend)
    test_email = "test@example.com"
    
    query = """
        SELECT 
            u.usuario_id,
            u.contrasena_hash,
            u.nombre,
            u.email,
            u.estado,
            u.documento_identidad,
            u.rol_id,
            t.taller_id,
            t.razon_social
        FROM USUARIO u
        INNER JOIN TALLER t ON u.usuario_id = t.usuario_id
        WHERE u.email = %s AND u.rol_id = 2
        LIMIT 1
    """
    
    cur.execute(query, (test_email,))
    result = cur.fetchone()
    
    if result:
        print_ok(f"Usuario encontrado: {result['email']}")
        print(f"   ID: {result['usuario_id']}")
        print(f"   Nombre: {result['nombre']}")
        print(f"   Taller: {result['razon_social']}")
    else:
        print_warning(f"No hay usuario con email '{test_email}' en la BD")
    
    cur.close()
    
except Exception as e:
    print_error(f"Error en query de login: {str(e)}")
    sys.exit(1)

# ============================================================
# RESUMEN FINAL
# ============================================================
conn.close()

print_header("✅ VERIFICACIÓN COMPLETADA")
print_ok("Todo parece estar configurado correctamente")
print_info("Ahora puedes:")
print_info("  1. Ejecutar el backend: python -m uvicorn app.run:app --reload")
print_info("  2. O desplegar en Render con: git push")
print("\n")
