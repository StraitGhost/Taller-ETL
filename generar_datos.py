"""
SCRIPT AUXILIAR: GENERADOR DE DATASET MASIVO DE PRUEBA - CLÍNICA SAN JOSÉ
"""
import csv
import random
import uuid
import os
import time
from datetime import datetime, timedelta

def generar_dataset_masivo(nombre_archivo="logs_admisiones_masivas.csv", num_registros=100000):
    directorio_base = os.path.dirname(os.path.abspath(__file__))
    ruta_salida = os.path.join(directorio_base, nombre_archivo)

    print("=" * 70)
    print(f" GENERANDO DATASET MASIVO: '{nombre_archivo}' ({num_registros:,} filas)")
    print("=" * 70)

    inicio = time.time()
    diagnosticos = ["Dolor Toracico", "Trauma Craneal", "Crisis Hipertensiva", "Convulsion Febril", "Fractura", "Intoxicacion", "Infeccion Aguda"]
    camas = ["Cama_Emergencia_01", "Cama_Emergencia_02", "Cama_Emergencia_03", "Cama_Emergencia_04", "Cama_UCI_01", "Cama_UCI_02", "Sin_Cama"]
    estados = ["Leve", "Moderado", "Grave", "Critico"]
    fecha_inicio = datetime(2026, 1, 1)

    encabezados = ["id_admision", "fecha_ingreso", "id_paciente", "cama_asignada", "diagnostico", "costo_consulta", "estado_paciente"]

    with open(ruta_salida, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(encabezados)

        for i in range(1, num_registros + 1):
            adm_id = str(uuid.uuid4())
            fecha_adm = fecha_inicio + timedelta(seconds=random.randint(0, 15552000))
            fecha_str = fecha_adm.strftime("%Y-%m-%d %H:%M:%S")
            id_paciente = f"PAC{random.randint(1000, 9999)}"
            cama = random.choice(camas)
            diag = random.choice(diagnosticos)

            # Introducimos datos sucios (2% costo negativo, 2% costo vacío)
            rand_val = random.random()
            if rand_val < 0.02:
                costo = -round(random.uniform(50.0, 300.0), 2)
            elif rand_val < 0.04:
                costo = ""
            else:
                costo = round(random.uniform(20.0, 500.0), 2)

            estado = random.choice(estados)
            writer.writerow([adm_id, fecha_str, id_paciente, cama, diag, costo, estado])

            if i % 25000 == 0:
                print(f"   - Progresando: {i:,} / {num_registros:,} filas escritas...")

    duracion = time.time() - inicio
    tamano_mb = os.path.getsize(ruta_salida) / (1024 * 1024)
    print(f"\n[OK] Dataset generado en: '{ruta_salida}'")
    print(f" - Filas totales: {num_registros:,} | Tamaño: {tamano_mb:.2f} MB | Tiempo: {duracion:.2f}s")

if __name__ == "__main__":
    generar_dataset_masivo()