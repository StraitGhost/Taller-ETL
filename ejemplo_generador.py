# ejemplo_generador.py
# Ejemplo simple y didáctico sobre Generadores y el uso de yield en Python

import time
import sys

# =====================================================================
# 1. EL EJEMPLO MÁS BÁSICO: Generar números uno a uno
# =====================================================================

def generador_numeros(limite):
    """
    Función generadora. Al usar 'yield', no devuelve todos los valores
    de golpe ni termina la ejecución de la función. En cambio, "pausa"
    la ejecución y devuelve el valor actual, recordando su estado.
    """
    numero = 1
    while numero <= limite:
        print(f"[Generador] Produciendo el número: {numero}")
        yield numero  # <-- AQUÍ se pausa la función y se entrega el valor
        numero += 1
        # Cuando el código cliente solicita el siguiente valor,
        # la función se reanuda justo en la línea siguiente a este yield.

print("--- EXPLICACIÓN PASO A PASO CON next() ---")
# Cuando llamamos a la función generadora, NO se ejecuta el código interno inmediatamente.
# Solo nos devuelve un "objeto generador".
mi_generador = generador_numeros(3)
print(f"Tipo de objeto: {type(mi_generador)}")

# Consumimos los valores manualmente usando la función next()
print("\nLlamando a next() la primera vez:")
valor1 = next(mi_generador)
print(f"Recibido en el código principal: {valor1}")

print("\nLlamando a next() la segunda vez:")
valor2 = next(mi_generador)
print(f"Recibido en el código principal: {valor2}")

print("\nLlamando a next() la tercera vez:")
valor3 = next(mi_generador)
print(f"Recibido en el código principal: {valor3}")

# Si llamamos a next() una vez más, Python lanzará StopIteration porque el generador terminó.
try:
    print("\nLlamando a next() por cuarta vez:")
    next(mi_generador)
except StopIteration:
    print("¡StopIteration! El generador no tiene más elementos.")


# =====================================================================
# 2. CONSUMO TÍPICO CON UN BUCLE 'FOR'
# =====================================================================
print("\n--- CONSUMO AUTOMÁTICO CON BUCLE FOR ---")
# El bucle for maneja automáticamente el next() y detecta el StopIteration de forma invisible.
for numero in generador_numeros(4):
    print(f"[Bucle For] Recibido: {numero}")


# =====================================================================
# 3. ¿POR QUÉ IMPORTA EN ETL Y MEMORIA? (LISTA vs GENERADOR)
# =====================================================================
print("\n--- COMPARACIÓN DE USO DE MEMORIA (LISTA vs GENERADOR) ---")

# Caso A: Función normal que retorna una lista (Carga todo en RAM)
def obtener_lista_grande(n):
    resultado = []
    for i in range(n):
        resultado.append(i)
    return resultado

# Caso B: Generador que produce elementos bajo demanda (No ocupa RAM extra)
def generar_elementos_grandes(n):
    for i in range(n):
        yield i

N = 10_000_000  # 10 millones de números

print(f"Generando {N:,} elementos...")

# Medimos el tamaño en bytes de la lista (Cargada en memoria)
lista_completa = obtener_lista_grande(100_000) # solo 100 mil para no saturar la máquina del ejemplo
size_lista = sys.getsizeof(lista_completa)
print(f"- Una LISTA de 100,000 elementos ocupa en RAM: {size_lista:,} bytes")

# Medimos el tamaño en bytes del objeto generador (Incluso para 10 millones)
objeto_generador = generar_elementos_grandes(N)
size_generador = sys.getsizeof(objeto_generador)
print(f"- El GENERADOR para {N:,} elementos ocupa en RAM: {size_generador:,} bytes")
print("  (¡El generador pesa lo mismo sin importar si genera 10 o 10 millones de elementos,")
print("   porque procesa un elemento a la vez y no guarda el historial en memoria!)")
