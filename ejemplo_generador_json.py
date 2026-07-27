# ejemplo_generador_json.py
# Ejemplo didáctico 2: Lectura perezosa (Lazy Load) de un archivo JSONL (JSON Lines) en lotes de 10 en 10.
#
# CONCEPTO DE DATA WAREHOUSE:
# En ingeniería de datos, cargar un archivo JSON tradicional de gigabytes usando `json.load()`
# causa un error de Out-Of-Memory (OOM) porque intenta meter todo el archivo en la RAM.
# Para solucionarlo, la industria utiliza JSON Lines (.jsonl o .ndjson), donde cada línea es un 
# objeto JSON independiente. Esto nos permite leer el archivo línea por línea de forma perezosa.

import json
import os
import sys
import time

# Usaremos la extensión .jsonl (JSON Lines) para denotar que es streaming-friendly
RUTA_JSONL = "Taller ETL/datos_100.jsonl"

# =====================================================================
# 1. CREACIÓN DEL ARCHIVO EN FORMATO JSONL (Línea por Línea)
# =====================================================================
def crear_archivo_jsonl(ruta):
    """
    Crea un archivo JSON Lines (.jsonl). Cada línea es un diccionario JSON válido.
    Este es el formato estándar usado en Data Warehouses y Data Lakes (AWS Athena, BigQuery, Spark).
    """
    # Creamos el directorio padre si no existe
    dir_padre = os.path.dirname(ruta)
    if dir_padre and not os.path.exists(dir_padre):
        os.makedirs(dir_padre)

    with open(ruta, "w", encoding="utf-8") as f:
        for i in range(1, 101):
            registro = {
                "id": i,
                "nombre": f"Estudiante {i}",
                "nota": round(7.0 + (i % 4) * 0.75, 2),
                "materia": "Gestión de Datos"
            }
            # Escribimos el diccionario como una sola línea de texto JSON + salto de línea
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            
    print(f"[OK] Archivo '{ruta}' creado con 100 registros en formato JSONL.\n")


# =====================================================================
# 2. GENERADOR CON LAZY LOAD (STREAMING REAL SIN CARGAR TODO EL ARCHIVO)
# =====================================================================
def generador_lotes_jsonl(ruta, tamano_lote=10):
    """
    Lee un archivo JSONL línea por línea usando un iterador perezoso.
    Solo mantiene en memoria las líneas necesarias para completar un lote (lote_size).
    """
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")
    
    # Abrimos el archivo. Python NO carga el archivo en memoria aquí;
    # solo abre un puntero/descriptor de archivo.
    with open(ruta, "r", encoding="utf-8") as f:
        lote = []
        
        # Iterar sobre 'f' lee el archivo línea por línea física (streaming en disco)
        for linea in f:
            # Eliminamos espacios en blanco y saltos de línea
            linea_limpia = linea.strip()
            if not linea_limpia:
                continue
            
            # Decodificamos únicamente esta línea (un solo registro en RAM)
            registro = json.loads(linea_limpia)
            lote.append(registro)
            
            # Cuando completamos el lote, lo emitimos y limpiamos RAM
            if len(lote) == tamano_lote:
                yield lote
                lote = [] # Se libera la referencia de los objetos en RAM
                
        # Emitir registros restantes si quedan
        if lote:
            yield lote


# =====================================================================
# 3. EJECUCIÓN Y VERIFICACIÓN
# =====================================================================
if __name__ == "__main__":
    # 1. Crear el set de datos
    crear_archivo_jsonl(RUTA_JSONL)
    
    # 2. Medir el consumo inicial del generador
    objeto_generador = generador_lotes_jsonl(RUTA_JSONL, tamano_lote=10)
    print(f"-> Peso del objeto generador en RAM: {sys.getsizeof(objeto_generador)} bytes.")
    print("   (No carga los datos al instanciarse; solo se prepara para leer)\n")
    
    print("--- INICIANDO PROCESAMIENTO DE LOTES (LAZY LOAD) ---")
    
    # 3. Consumir perezosamente los lotes
    contador_lotes = 1
    for lote in objeto_generador:
        print(f"\n[LOTE #{contador_lotes}] - Cargando {len(lote)} registros a memoria temporal:")
        for registro in lote:
            print(f"  * ID: {registro['id']} | {registro['nombre']} | Nota: {registro['nota']}")
        
        # Al pasar al siguiente lote del bucle for, el recolector de basura de Python
        # liberará de la RAM el lote anterior ya procesado.
        contador_lotes += 1

        print(f" \n \n \n Esperando 3 segundos para el siguiente lote mientras \n se realiza un proceso muy riguroso de Transformacion \n \n \n")
        time.sleep(3)
        
    print("\n[OK] Procesamiento en streaming finalizado con éxito.")
