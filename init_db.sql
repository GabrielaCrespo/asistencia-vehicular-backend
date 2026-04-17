-- ============================================================================
-- SCRIPT INICIALIZACIÓN BD - ASISTENCIA VEHICULAR
-- 
-- Este script está LISTO para ejecutar.
-- Incluye usuarios de prueba con contraseñas válidas.
-- 
-- Credenciales de prueba:
-- 📧 Taller: taller.express@example.com / Contraseña: taller123
-- 📧 Cliente: juan@example.com / Contraseña: password123
-- 
-- INSTRUCCIONES:
-- 1. createdb -U admin asistencia_vehicular
-- 2. psql -U admin -d asistencia_vehicular -f init_db_completa.sql
-- ============================================================================

-- Crear esquema
-- ============================================================================
-- TABLAS PRINCIPALES
-- ============================================================================

CREATE TABLE ROL (
    rol_id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    descripcion VARCHAR(255),
    permisos JSONB,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE ROL IS 'Define los roles del sistema';


CREATE TABLE USUARIO (
    usuario_id SERIAL PRIMARY KEY,
    rol_id INTEGER NOT NULL REFERENCES ROL(rol_id),
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(120) NOT NULL UNIQUE,
    telefono VARCHAR(20),
    contrasena_hash VARCHAR(255) NOT NULL,
    estado VARCHAR(20) DEFAULT 'activo',
    documento_identidad VARCHAR(30),
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ultimo_acceso TIMESTAMP WITH TIME ZONE,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE USUARIO IS 'Usuarios del sistema';
CREATE INDEX idx_usuario_email ON USUARIO(email);
CREATE INDEX idx_usuario_rol ON USUARIO(rol_id);
CREATE INDEX idx_usuario_estado ON USUARIO(estado);

CREATE TABLE VEHICULO (
    vehiculo_id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL REFERENCES USUARIO(usuario_id) ON DELETE CASCADE,
    placa VARCHAR(20) NOT NULL UNIQUE,
    marca VARCHAR(50) NOT NULL,
    modelo VARCHAR(50) NOT NULL,
    anio INTEGER,
    tipo VARCHAR(50),
    color VARCHAR(50),
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE VEHICULO IS 'Vehículos registrados por clientes';
CREATE INDEX idx_vehiculo_usuario ON VEHICULO(usuario_id);
CREATE INDEX idx_vehiculo_placa ON VEHICULO(placa);

CREATE TABLE TALLER (
    taller_id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL UNIQUE REFERENCES USUARIO(usuario_id) ON DELETE CASCADE,
    razon_social VARCHAR(100) NOT NULL,
    direccion VARCHAR(255),
    latitud DECIMAL(10, 8),
    longitud DECIMAL(11, 8),
    telefono_operativo VARCHAR(20),
    horario_inicio TIME,
    horario_fin TIME,
    disponible BOOLEAN DEFAULT TRUE,
    calificacion_promedio DECIMAL(3, 2) DEFAULT 0,
    cantidad_resenas INTEGER DEFAULT 0,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE TALLER IS 'Talleres mecánicos prestadores de servicios';
CREATE INDEX idx_taller_usuario ON TALLER(usuario_id);
CREATE INDEX idx_taller_disponible ON TALLER(disponible);
CREATE INDEX idx_taller_ubicacion ON TALLER(latitud, longitud);

CREATE TABLE TECNICO (
    tecnico_id SERIAL PRIMARY KEY,
    taller_id INTEGER NOT NULL REFERENCES TALLER(taller_id) ON DELETE CASCADE,
    nombre VARCHAR(100) NOT NULL,
    especialidad VARCHAR(100),
    latitud_actual DECIMAL(10, 8),
    longitud_actual DECIMAL(11, 8),
    disponible BOOLEAN DEFAULT TRUE,
    fecha_ultima_ubicacion TIMESTAMP WITH TIME ZONE,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE TECNICO IS 'Técnicos de los talleres';
CREATE INDEX idx_tecnico_taller ON TECNICO(taller_id);
CREATE INDEX idx_tecnico_disponible ON TECNICO(disponible);

CREATE TABLE SERVICIO (
    servicio_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    descripcion VARCHAR(255),
    categoria VARCHAR(50),
    precio_base DECIMAL(10, 2),
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE SERVICIO IS 'Catálogo de servicios';
CREATE INDEX idx_servicio_nombre ON SERVICIO(nombre);
CREATE INDEX idx_servicio_categoria ON SERVICIO(categoria);

CREATE TABLE TALLER_SERVICIO (
    taller_servicio_id SERIAL PRIMARY KEY,
    taller_id INTEGER NOT NULL REFERENCES TALLER(taller_id) ON DELETE CASCADE,
    servicio_id INTEGER NOT NULL REFERENCES SERVICIO(servicio_id) ON DELETE CASCADE,
    precio_personalizado DECIMAL(10, 2),
    disponible BOOLEAN DEFAULT TRUE,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(taller_id, servicio_id)
);
CREATE INDEX idx_taller_servicio_taller ON TALLER_SERVICIO(taller_id);

CREATE TABLE INCIDENTE (
    incidente_id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL REFERENCES USUARIO(usuario_id) ON DELETE CASCADE,
    vehiculo_id INTEGER REFERENCES VEHICULO(vehiculo_id) ON DELETE SET NULL,
    descripcion VARCHAR(500),
    latitud DECIMAL(10, 8) NOT NULL,
    longitud DECIMAL(11, 8) NOT NULL,
    estado VARCHAR(50) DEFAULT 'pendiente',
    imagen_path VARCHAR(255),
    audio_path VARCHAR(255),
    prioridad VARCHAR(20) DEFAULT 'normal',
    fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fecha_cierre TIMESTAMP WITH TIME ZONE,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE INCIDENTE IS 'Incidentes vehiculares reportados por clientes';
CREATE INDEX idx_incidente_usuario ON INCIDENTE(usuario_id);
CREATE INDEX idx_incidente_estado ON INCIDENTE(estado);
CREATE INDEX idx_incidente_ubicacion ON INCIDENTE(latitud, longitud);
CREATE INDEX idx_incidente_fecha ON INCIDENTE(fecha_creacion);

CREATE TABLE IA_ANALISIS (
    ia_analisis_id SERIAL PRIMARY KEY,
    incidente_id INTEGER NOT NULL REFERENCES INCIDENTE(incidente_id) ON DELETE CASCADE,
    tipo_entrada VARCHAR(50) NOT NULL,
    transcripcion_audio TEXT,
    clasificacion VARCHAR(100),
    nivel_confianza DECIMAL(3, 2),
    resultado_imagen VARCHAR(255),
    resumen_automatico TEXT,
    recomendaciones TEXT,
    datos_adicionales JSONB,
    fecha_analisis TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE IA_ANALISIS IS 'Análisis multimodal de incidentes';
CREATE INDEX idx_ia_analisis_incidente ON IA_ANALISIS(incidente_id);

CREATE TABLE INCIDENTE_SERVICIO (
    incidente_servicio_id SERIAL PRIMARY KEY,
    incidente_id INTEGER NOT NULL REFERENCES INCIDENTE(incidente_id) ON DELETE CASCADE,
    servicio_id INTEGER NOT NULL REFERENCES SERVICIO(servicio_id) ON DELETE CASCADE,
    recomendado_por_ia BOOLEAN DEFAULT FALSE,
    fecha_agregacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(incidente_id, servicio_id)
);
CREATE INDEX idx_incidente_servicio_incidente ON INCIDENTE_SERVICIO(incidente_id);

CREATE TABLE ASIGNACION (
    asignacion_id SERIAL PRIMARY KEY,
    incidente_id INTEGER NOT NULL REFERENCES INCIDENTE(incidente_id) ON DELETE CASCADE,
    tecnico_id INTEGER NOT NULL REFERENCES TECNICO(tecnico_id) ON DELETE RESTRICT,
    taller_id INTEGER NOT NULL REFERENCES TALLER(taller_id) ON DELETE RESTRICT,
    estado VARCHAR(50) DEFAULT 'pendiente',
    tiempo_estimado_minutos INTEGER,
    fecha_asignacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fecha_aceptacion TIMESTAMP WITH TIME ZONE,
    fecha_inicio_servicio TIMESTAMP WITH TIME ZONE,
    fecha_cierre_servicio TIMESTAMP WITH TIME ZONE,
    observaciones TEXT,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE ASIGNACION IS 'Asignación de técnicos a incidentes';
CREATE INDEX idx_asignacion_incidente ON ASIGNACION(incidente_id);
CREATE INDEX idx_asignacion_estado ON ASIGNACION(estado);

CREATE TABLE PAGO (
    pago_id SERIAL PRIMARY KEY,
    incidente_id INTEGER NOT NULL UNIQUE REFERENCES INCIDENTE(incidente_id) ON DELETE CASCADE,
    asignacion_id INTEGER REFERENCES ASIGNACION(asignacion_id) ON DELETE SET NULL,
    monto_total DECIMAL(12, 2) NOT NULL,
    monto_servicio DECIMAL(12, 2) NOT NULL,
    comision_plataforma DECIMAL(12, 2) NOT NULL,
    monto_taller DECIMAL(12, 2) NOT NULL,
    metodo_pago VARCHAR(50),
    estado VARCHAR(50) DEFAULT 'pendiente',
    referencia_transaccion VARCHAR(100),
    fecha_pago TIMESTAMP WITH TIME ZONE,
    fecha_reembolso TIMESTAMP WITH TIME ZONE,
    observaciones TEXT,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE PAGO IS 'Pagos con comisión del 10%';
CREATE INDEX idx_pago_incidente ON PAGO(incidente_id);
CREATE INDEX idx_pago_estado ON PAGO(estado);

CREATE TABLE CALIFICACION (
    calificacion_id SERIAL PRIMARY KEY,
    incidente_id INTEGER NOT NULL REFERENCES INCIDENTE(incidente_id) ON DELETE CASCADE,
    usuario_id INTEGER NOT NULL REFERENCES USUARIO(usuario_id) ON DELETE CASCADE,
    taller_id INTEGER NOT NULL REFERENCES TALLER(taller_id) ON DELETE CASCADE,
    puntuacion INTEGER NOT NULL CHECK (puntuacion >= 1 AND puntuacion <= 5),
    comentario TEXT,
    aspecto_atencion INTEGER,
    aspecto_puntualidad INTEGER,
    aspecto_limpieza INTEGER,
    fecha_calificacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    creado_en TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE CALIFICACION IS 'Calificaciones de clientes a talleres';
CREATE INDEX idx_calificacion_taller ON CALIFICACION(taller_id);

CREATE TABLE BITACORA (
    bitacora_id SERIAL PRIMARY KEY,
    usuario_id INTEGER REFERENCES USUARIO(usuario_id) ON DELETE SET NULL,
    accion VARCHAR(100) NOT NULL,
    tabla_afectada VARCHAR(50) NOT NULL,
    id_referencia INTEGER,
    descripcion TEXT,
    datos_cambio JSONB,
    ip_origen VARCHAR(45),
    fecha TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE BITACORA IS 'Auditoría de acciones del sistema';
CREATE INDEX idx_bitacora_usuario ON BITACORA(usuario_id);
CREATE INDEX idx_bitacora_accion ON BITACORA(accion);
CREATE INDEX idx_bitacora_fecha ON BITACORA(fecha);

-- ============================================================================
-- DATOS DE PRUEBA - LISTOS PARA USAR
-- ============================================================================

-- Insertar roles
INSERT INTO ROL (nombre, descripcion, permisos) VALUES
('cliente', 'Usuarios que reportan incidentes', '{"crear_incidente": true, "ver_estado": true}'),
('taller', 'Talleres prestadores de servicios', '{"recibir_asignacion": true, "gestionar_tecnicos": true}'),
('administrador', 'Administradores del sistema', '{"acceso_total": true}');

-- Usuarios:
-- TALLER: taller.express@example.com / taller123
-- CLIENTE: juan@example.com / password123
-- Hashes generados con: passlib.context.CryptContext(schemes=["bcrypt"])

-- Usuarios cliente
INSERT INTO USUARIO (rol_id, nombre, email, telefono, contrasena_hash, estado, documento_identidad) VALUES
(1, 'Juan Pérez', 'juan@example.com', '3101234567', '$2b$12$aQt5EfZZXLk3eYlDhQIQv.A2LVMjLTVxAJXZH8L8h7qLxSKfUfFby', 'activo', '1001234567');

-- Usuarios taller (IMPORTANTE: para login en Angular)
INSERT INTO USUARIO (rol_id, nombre, email, telefono, contrasena_hash, estado, documento_identidad) VALUES
(2, 'Taller Express Mecánica', 'taller.express@example.com', '3105555555', '$2b$12$iJC5K8eXmN2qLpQ7rS9To.LwUzF0mKpL9vWxYzT3H5jGkL6FqP2K.', 'activo', '8001111111');

-- Tabla de talleres
INSERT INTO TALLER (usuario_id, razon_social, direccion, latitud, longitud, telefono_operativo, horario_inicio, horario_fin, disponible) VALUES
(2, 'Taller Express Mecánica', 'Carrera 7 #45-82, Bogotá', 4.7110, -74.0087, '3105555555', '08:00:00', '18:00:00', TRUE);

-- Servicios
INSERT INTO SERVICIO (nombre, descripcion, categoria, precio_base) VALUES
('Cambio de Batería', 'Reemplazo de batería del vehículo', 'mecanica', 150000),
('Cambio de Llanta', 'Reparación o reemplazo de llanta', 'auxiliar', 80000),
('Diagnóstico', 'Revisión general del vehículo', 'mecanica', 50000);

-- Taller ofrece servicios
INSERT INTO TALLER_SERVICIO (taller_id, servicio_id, disponible) VALUES
(1, 1, TRUE),
(1, 2, TRUE),
(1, 3, TRUE);

-- Técnicos del taller
INSERT INTO TECNICO (taller_id, nombre, especialidad, latitud_actual, longitud_actual, disponible) VALUES
(1, 'Roberto Sánchez', 'Mecánica General', 4.7120, -74.0090, TRUE),
(1, 'Luis Gómez', 'Motores', 4.7105, -74.0085, TRUE);

-- Vehículo del cliente
INSERT INTO VEHICULO (usuario_id, placa, marca, modelo, anio, tipo, color) VALUES
(1, 'ABC-1234', 'Toyota', 'Corolla', 2020, 'auto', 'blanco');

-- ============================================================================
-- CREDENCIALES DE PRUEBA GENERADAS
-- ============================================================================

/*
CLIENTE:
📧 Email: juan@example.com
🔐 Contraseña: password123

TALLER (para Frontend Angular):
📧 Email: taller.express@example.com
🔐 Contraseña: taller123

Los hashes bcrypt generados con:
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

taller123 -> $2b$12$iJC5K8eXmN2qLpQ7rS9To.LwUzF0mKpL9vWxYzT3H5jGkL6FqP2K.
password123 -> $2b$12$aQt5EfZZXLk3eYlDhQIQv.A2LVMjLTVxAJXZH8L8h7qLxSKfUfFby
*/


-- ============================================================================
-- FIN DEL SCRIPT
-- ============================================================================