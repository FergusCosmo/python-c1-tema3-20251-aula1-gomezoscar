"""
Enunciado:
En este ejercicio aprenderás a trabajar con bases de datos SQLite existentes.
Aprenderás a:
1. Conectar a una base de datos SQLite existente
2. Convertir datos de SQLite a formatos compatibles con JSON
3. Extraer datos de SQLite a pandas DataFrame

El archivo ventas_comerciales.db contiene datos de ventas con tablas relacionadas
que incluyen productos, vendedores, regiones y ventas. Debes analizar estos datos
usando diferentes técnicas.
"""

import sqlite3
import pandas as pd
import os
import json
from typing import List, Dict, Any, Optional, Tuple, Union

# Ruta a la base de datos SQLite
DB_PATH = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.db')

def conectar_bd() -> sqlite3.Connection:
    """
    Conecta a una base de datos SQLite existente

    Returns:
        sqlite3.Connection: Objeto de conexión a la base de datos SQLite
    """
    # Verifica que el archivo de base de datos existe
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"La base de datos '{DB_PATH}' no existe.")

    # Conecta a la base de datos
    conexion = sqlite3.connect(DB_PATH)

    # Configura la conexión para que devuelva las filas como diccionarios (opcional)
    conexion.row_factory = sqlite3.Row

    # Retorna la conexión
    return conexion

def convertir_a_json(conexion: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convierte los datos de la base de datos en un objeto compatible con JSON

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, List[Dict[str, Any]]]: Diccionario con todas las tablas y sus registros
        en formato JSON-serializable
    """
    cursor = conexion.cursor()

    # Crea un diccionario vacío para almacenar el resultado
    resultado = {}

    # Obtén la lista de tablas de la base de datos
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()

    for tabla in tablas:
        nombre_tabla = tabla[0]

        # Ejecuta una consulta SELECT * FROM tabla
        cursor.execute(f"SELECT * FROM {nombre_tabla}")
        filas = cursor.fetchall()

        # Obtén los nombres de las columnas
        columnas = [descripcion[0] for descripcion in cursor.description]

        # Convierte cada fila a un diccionario (clave: nombre columna, valor: valor celda)
        registros = []
        for fila in filas:
            registro = {}
            for i, valor in enumerate(fila):
                registro[columnas[i]] = valor
            registros.append(registro)

        # Añade el diccionario a una lista para esa tabla
        resultado[nombre_tabla] = registros

    # Retorna el diccionario completo con todas las tablas
    return resultado

def convertir_a_dataframes(conexion: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """
    Extrae los datos de la base de datos a DataFrames de pandas

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, pd.DataFrame]: Diccionario con DataFrames para cada tabla y para
        consultas combinadas relevantes
    """
    cursor = conexion.cursor()

    # Crea un diccionario vacío para los DataFrames
    dataframes = {}

    # Obtén la lista de tablas de la base de datos
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()

    # Para cada tabla, crea un DataFrame usando pd.read_sql_query
    for tabla in tablas:
        nombre_tabla = tabla[0]
        df = pd.read_sql_query(f"SELECT * FROM {nombre_tabla}", conexion)
        dataframes[nombre_tabla] = df

    # Añade consultas JOIN para relaciones importantes:
    # - Ventas con información de productos
    try:
        df_ventas_productos = pd.read_sql_query('''
            SELECT v.*, p.nombre as producto_nombre, p.precio
            FROM ventas v
            JOIN productos p ON v.producto_id = p.id
        ''', conexion)
        dataframes['ventas_con_productos'] = df_ventas_productos
    except:
        pass  # Ignorar si no existen las tablas

    # - Ventas con información de vendedores
    try:
        df_ventas_vendedores = pd.read_sql_query('''
            SELECT v.*, ven.nombre as vendedor_nombre, ven.region_id
            FROM ventas v
            JOIN vendedores ven ON v.vendedor_id = ven.id
        ''', conexion)
        dataframes['ventas_con_vendedores'] = df_ventas_vendedores
    except:
        pass  # Ignorar si no existen las tablas

    # - Vendedores con regiones
    try:
        df_vendedores_regiones = pd.read_sql_query('''
            SELECT ven.*, r.nombre as region_nombre
            FROM vendedores ven
            JOIN regiones r ON ven.region_id = r.id
        ''', conexion)
        dataframes['vendedores_con_regiones'] = df_vendedores_regiones
    except:
        pass  # Ignorar si no existen las tablas

    # Retorna el diccionario con todos los DataFrames
    return dataframes

if __name__ == "__main__":
    try:
        # Conectar a la base de datos existente
        print("Conectando a la base de datos...")
        conexion = conectar_bd()
        print("Conexión establecida correctamente.")

        # Verificar la conexión mostrando las tablas disponibles
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        print(f"\nTablas en la base de datos: {[t[0] for t in tablas]}")

        # Conversión a JSON
        print("\n--- Convertir datos a formato JSON ---")
        datos_json = convertir_a_json(conexion)
        print("Estructura JSON (ejemplo de una tabla):")
        if datos_json:
            # Muestra un ejemplo de la primera tabla encontrada
            primera_tabla = list(datos_json.keys())[0]
            print(f"Tabla: {primera_tabla}")
            if datos_json[primera_tabla]:
                print(f"Primer registro: {datos_json[primera_tabla][0]}")

            # Opcional: guardar los datos en un archivo JSON
            # ruta_json = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.json')
            # with open(ruta_json, 'w', encoding='utf-8') as f:
            #     json.dump(datos_json, f, ensure_ascii=False, indent=2)
            # print(f"Datos guardados en {ruta_json}")

        # Conversión a DataFrames de pandas
        print("\n--- Convertir datos a DataFrames de pandas ---")
        dataframes = convertir_a_dataframes(conexion)
        if dataframes:
            print(f"Se han creado {len(dataframes)} DataFrames:")
            for nombre, df in dataframes.items():
                print(f"- {nombre}: {len(df)} filas x {len(df.columns)} columnas")
                print(f"  Columnas: {', '.join(df.columns.tolist())}")
                print(f"  Vista previa:\n{df.head(2)}\n")

    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conexion' in locals() and conexion:
            conexion.close()
            print("\nConexión cerrada.")
