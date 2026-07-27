"""
===============================================================================
TALLER DE ETL CON GENERADORES (YIELD) - CLÍNICA SAN JOSÉ (COMPLETO)
===============================================================================
"""
import csv
import os
import time
import tracemalloc
import pymysql # Usamos pymysql para conectar con MySQL Workbench

# =============================================================================
# CONFIGURACIÓN DE CONEXIÓN MYSQL
# =============================================================================
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456" 
MYSQL_DATABASE = "clinica_san_jose_db"
MYSQL_TABLA = "admisiones_emergencia"

# =============================================================================
# FASE 1: EXTRACT - GENERADOR CON YIELD (STREAMING)
# =============================================================================
def extractor_lotes_csv(ruta_csv, tamano_lote=2000):
    if not os.path.exists(ruta_csv):
        raise FileNotFoundError(f"No existe el archivo: {ruta_csv}")

    with open(ruta_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        lote = []
        for fila in reader:
            lote.append(fila)
            if len(lote) >= tamano_lote:
                yield lote
                lote = []  # Reiniciamos la lista, el GC de Python liberará la memoria del lote anterior
        
        # Emitir el último lote si quedan registros remanentes
        if lote:
            yield lote

# =============================================================================
# FASE 2: TRANSFORM - REGLAS DE NEGOCIO Y LIMPIEZA
# =============================================================================
def transformar_lote(lote_raw):
    lote_transformado = []

    for reg in lote_raw:
        try:
            costo_str = reg.get("costo_consulta", "").strip()
            if not costo_str:
                continue  # Regla 1: Descartar vacíos

            costo = float(costo_str)
            if costo <= 0:
                continue  # Regla 1: Descartar menores o iguales a 0

            # Regla 2 y 3: Cálculos
            comision = round(costo * 0.05, 2)
            costo_neto = round(costo - comision, 2)
            
            # Regla 4: Alerta de gravedad
            estado = reg.get("estado_paciente", "Leve").strip()
            alerta_gravedad = 1 if (costo > 200.0 and estado in ["Critico", "Grave"]) else 0

            tupla_registro = (
                reg["id_admision"].strip(),
                reg["fecha_ingreso"].strip(),
                reg["id_paciente"].strip(),
                reg["cama_asignada"].strip(),
                reg["diagnostico"].strip(),
                costo,
                comision,
                costo_neto,
                alerta_gravedad,
                estado
            )
            lote_transformado.append(tupla_registro)

        except (ValueError, TypeError, KeyError):
            continue # Omitir registros corruptos o con formato inesperado

    return lote_transformado

# =============================================================================
# FASE 3: LOAD - CARGA EN LOTE (BATCH LOAD) EN MYSQL
# =============================================================================
def cargar_lote_mysql(conn, lote_transformado):
    if not lote_transformado:
        return
    
    sql = f"""
    INSERT INTO {MYSQL_TABLA} 
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico, 
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor = conn.cursor()
    cursor.executemany(sql, lote_transformado)
    conn.commit()
    cursor.close()

# =============================================================================
# EJECUCIÓN PRINCIPAL Y MONITOREO DE MEMORIA RAM
# =============================================================================
def ejecutar_pipeline():
    directorio_base = os.path.dirname(os.path.abspath(__file__))
    ruta_csv = os.path.join(directorio_base, "logs_admisiones_masivas.csv")

    print("=" * 70)
    print(" INICIANDO PIPELINE ETL CON GENERADORES - CLÍNICA SAN JOSÉ ")
    print("=" * 70)

    tracemalloc.start()
    tiempo_inicio = time.time()

    # 1. Conectar a MySQL y asegurar la tabla
    print(f"-> Conectando a MySQL: {MYSQL_DATABASE}...")
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_TABLA} (
                id_admision VARCHAR(50) PRIMARY KEY,
                fecha_ingreso DATETIME,
                id_paciente VARCHAR(20),
                cama_asignada VARCHAR(50),
                diagnostico VARCHAR(100),
                costo_consulta DECIMAL(10,2),
                comision_seguro DECIMAL(10,2),
                costo_neto DECIMAL(10,2),
                alerta_gravedad TINYINT(1),
                estado_paciente VARCHAR(20),
                INDEX idx_estado (estado_paciente),
                INDEX idx_costo (costo_consulta)
            );
        """)
        # Limpiar tabla previa para pruebas repetidas
        cursor.execute(f"TRUNCATE TABLE {MYSQL_TABLA};")
    conn.commit()

    total_procesados = 0
    total_lotes = 0
    tamano_lote = 5000 # Procesamos de 5000 en 5000

    print(f"-> Procesando archivo masivo en lotes de {tamano_lote} (Streaming via yield)...")

    # Bucle del ETL: Iteramos directamente sobre el GENERADOR
    for lote_raw in extractor_lotes_csv(ruta_csv, tamano_lote=tamano_lote):
        total_lotes += 1
        
        # 1. Transformar lote
        lote_listo = transformar_lote(lote_raw)

        # 2. Cargar lote
        cargar_lote_mysql(conn, lote_listo)

        total_procesados += len(lote_listo)

        # Reportar estado y RAM
        current_mem, peak_mem = tracemalloc.get_traced_memory()
        peak_ram_mb = peak_mem / (1024 * 1024)
        print(f"   [Lote #{total_lotes:02d}] Cargadas {len(lote_listo):,} filas válidas | "
              f"Acumulado: {total_procesados:,} | RAM Pico: {peak_ram_mb:.2f} MB")

    conn.close()
    duracion = time.time() - tiempo_inicio
    _, memoria_final_mb = tracemalloc.get_traced_memory()
    memoria_final_mb = memoria_final_mb / (1024 * 1024)
    tracemalloc.stop()

    print("\n-------------------------------------------------------------------")
    print(" SUMMARY DE RENDIMIENTO DEL PIPELINE:")
    print("-------------------------------------------------------------------")
    print(f" - Filas totales cargadas con éxito: {total_procesados:,}")
    print(f" - Lotes procesados:                 {total_lotes}")
    print(f" - Tiempo de ejecución:              {duracion:.2f} segundos")
    print(f" - Consumo máximo de RAM (Pico RAM): {memoria_final_mb:.2f} MB")
    
    if memoria_final_mb < 20.0:
        print(" [ÉXITO] ¡El consumo de RAM se mantuvo por debajo de 20 MB gracias a yield!")
    else:
        print(" [ADVERTENCIA] El consumo de RAM superó los 20 MB. Revisa el tamaño del lote.")
    print("===================================================================")

if __name__ == "__main__":
    ejecutar_pipeline()