-- ===============================================================================
-- ESQUEMA DDL PARA EL TALLER PRÁCTICO DE ETL EN POSTGRESQL / SQLITE
-- ===============================================================================
-- Este script define la tabla de destino 'admisiones_emergencia' para la carga
-- masiva de datos mediante el pipeline ETL desarrollado con generadores en Python.
-- ===============================================================================

-- Eliminación de la tabla si ya existe (para reejecutar pruebas limpias)
DROP TABLE IF EXISTS admisiones_emergencia CASCADE;

-- Creación de la tabla optimizada para PostgreSQL / SQLite
CREATE TABLE admisiones_emergencia (
    id_admision VARCHAR(36) PRIMARY KEY,
    fecha_ingreso TIMESTAMP NOT NULL,
    id_paciente VARCHAR(10) NOT NULL,
    cama_asignada VARCHAR(30) NOT NULL,
    diagnostico VARCHAR(100) NOT NULL,
    costo_consulta NUMERIC(10, 2) NOT NULL,
    comision_seguro NUMERIC(10, 2) NOT NULL,
    costo_neto NUMERIC(10, 2) NOT NULL,
    alerta_gravedad BOOLEAN DEFAULT FALSE,
    estado_paciente VARCHAR(20) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices recomendados para consultas analíticas de alto rendimiento
CREATE INDEX idx_admisiones_fecha ON admisiones_emergencia(fecha_ingreso);
CREATE INDEX idx_admisiones_paciente ON admisiones_emergencia(id_paciente);
CREATE INDEX idx_admisiones_diagnostico ON admisiones_emergencia(diagnostico);
