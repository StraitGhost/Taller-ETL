# Taller Práctico Avanzado: ETL Clínico de Alto Rendimiento con Generadores en Python

---

## 1. Información General

- **Asignatura:** Sistemas de Informacion
- **Unidad:** Parcial 3 - Procesamiento de Datos a Gran Escala (ETL & Data Streaming)
- **Tema:** Procesamiento por Streaming de Archivos Masivos utilizando Generadores (`yield`), Medición de Memoria y Carga Eficiente en SQLite/PostgreSQL.
- **Modalidad:** Práctica en Laboratorio.

---

## 2. Objetivo de la Práctica

Diseñar e implementar un pipeline **ETL (Extract, Transform, Load)** capaz de procesar **archivos masivos de registros clínicos (>100,000 admisiones)** bajo restricciones estrictas de memoria RAM. El estudiante aprenderá a evitar el problema de agotamiento de memoria (*Out-Of-Memory / OOM*) mediante el uso de **Generadores en Python (`yield`)**, evaluando perezosamente los datos (*Lazy Evaluation*) y realizando inserciones en lote (*Batch Loading*) en la base de datos de la **Clínica San José**.

---

## 3. Marco Teórico

### 1. El Problema de la Memoria en Big Data (OOM - Out Of Memory)

Cuando trabajamos con datasets grandes, el enfoque convencional en Pandas usando `pd.read_csv("archivo_gigante.csv")` intenta cargar **el archivo completo en la memoria RAM** de una sola vez.

```text
[ Enfoque Tradicional - Pandas ]
Archivo (10 GB) ------------> Carga Completa en RAM (10 GB) ------------> COLAPSO / OOM ERROR
```

Si el archivo es de varios gigabytes y el servidor de admisiones de la clínica tiene recursos limitados, el programa fallará de inmediato.

### 2. Solución: Data Streaming y Generadores en Python (`yield`)

Un **Generador** es una función especial en Python que produce una secuencia de valores sobre la cual se puede iterar, pero **un elemento o lote a la vez**, manteniendo un consumo de memoria insignificante y constante ($O(1)$).

```text
[ Enfoque con Generadores / Streaming ]
Archivo (10 GB) ---> Lote 1 (5 MB) en RAM ---> Transformar & Cargar DB ---> Liberar RAM
                ---> Lote 2 (5 MB) en RAM ---> Transformar & Cargar DB ---> Liberar RAM
                ...
                (RAM Máxima Usada: ~8 MB sin importar el tamaño del archivo)
```

### 3. Comparativa Teórica: `return` vs `yield`

| Característica | Función con `return` | Función Generadora con `yield` |
| :--- | :--- | :--- |
| **Ejecución** | Se ejecuta hasta el final y retorna todos los datos de golpe. | Congela su estado, devuelve un valor y se reanuda donde quedó. |
| **Uso de RAM** | Alto. Acumula todos los resultados en una lista en RAM. | Bajo y Constante. Solo retiene en RAM el elemento/lote actual. |
| **Evaluación** | Ansiosa (*Eager Evaluation*). | Perezosa (*Lazy Evaluation*). |
| **Ideal para** | Datasets pequeños a medianos. | Data Streaming, Logs en tiempo real, Datasets de Gigabytes. |

### 4. Ejemplos de Referencia Incluidos

Para facilitar la resolución de este taller, se han incluido dos ejemplos prácticos en la carpeta del proyecto. Úsalos como base y referencia antes de empezar a programar tu pipeline:

*   **[ejemplo_generador.py](Taller%20ETL/ejemplo_generador.py)**: Muestra el funcionamiento básico de `yield` y `next()`, comparando de forma directa el consumo de memoria RAM entre una lista convencional y una función generadora.
*   **[ejemplo_generador_json.py](Taller%20ETL/ejemplo_generador_json.py)**: Simula un escenario real de ETL en Data Warehouse leyendo un archivo en formato JSON Lines (`.jsonl`) de manera perezosa (*Lazy Load*) y procesándolo en lotes (*batches*) de 10 en 10 registros para no saturar la memoria RAM.

---

## 4. Desafío Práctico para los Estudiantes

### Contexto del Negocio
La **Clínica San José** requiere procesar un archivo de logs diarios `logs_admisiones_masivas.csv` que contiene **100,000+ admisiones médicas**. 
El servidor de integración tiene restringido el uso de memoria a un **máximo de 20 MB de RAM**.

### Pasos a Realizar:

#### Paso 1: Generar el Dataset Masivo de Prueba
Ejecuta el script auxiliar incluido para generar el archivo de prueba de 100,000 filas:
```bash
python generar_datos.py
```
Esto creará el archivo `logs_admisiones_masivas.csv`.

#### Paso 2: Crear la Base de Datos y Tabla Destino
Si utilizas **PostgreSQL**, ejecuta el script `schema.sql` en tu gestor de base de datos (pgAdmin, DBeaver o psql):
```bash
psql -U tu_usuario -d tu_base_datos -f schema.sql
```
*(Nota: Si no posees PostgreSQL instalado en tu equipo, la plantilla incluye soporte fallback a SQLite de forma transparente).*

#### Paso 3: Completar la Plantilla del Pipeline ETL (`plantilla_taller.py`)
Abre el archivo `plantilla_taller.py` y completa los bloques señalados con `# TODO:`:

1. **Extractor con `yield`:** Implementa la función generadora `extractor_lotes_csv(ruta_csv, tamano_lote=2000)` que lea el CSV fila a fila y emita lotes de datos usando `yield`.
2. **Transformador:** Implementa `transformar_lote(lote_raw)` aplicando las siguientes reglas de negocio:
   - **Limpieza:** Filtrar registros con montos de consulta vacíos o menores/iguales a `0`.
   - **Cálculo de Comisión:** Calcular la comisión administrativa de seguro del 5% (`costo_consulta * 0.05`).
   - **Costo Neto:** `costo_consulta - comision_seguro`.
   - **Alerta de Gravedad:** Marcar `alerta_gravedad = True` si el costo supera los `$200.00` y el estado del paciente es `'Grave'` o `'Critico'`.
3. **Cargador por Lotes:** Inserción masiva en SQLite/PostgreSQL usando `executemany` o `execute_values`.
4. **Medición de RAM:** Verifica que el pico máximo de memoria reportado por `tracemalloc` no supere los **15-20 MB**.

---

## 5. Estructura de Archivos del Taller

```text
Taller ETL/
├── README.md                # Guía teórica, instrucciones y rúbrica (Este archivo)
├── schema.sql               # Script DDL para PostgreSQL
├── generar_datos.py         # Script para generar dataset sintético de 100k filas de clínica
├── plantilla_taller.py      # Plantilla base en Python con TODOs para el estudiante
├── ejemplo_generador.py     # Ejemplo didáctico del funcionamiento básico de yield
└── ejemplo_generador_json.py# Ejemplo de Lazy Loading y procesamiento por lotes (JSONL)
```
