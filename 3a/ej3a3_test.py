"""
Tests para el ejercicio ej3a3.py que trabaja con bases de datos SQLite existentes
y realiza operaciones de conversión de datos a JSON y pandas DataFrames.
"""

import pytest
import sqlite3
import os
import json
import pandas as pd
from ej3a3 import conectar_bd, convertir_a_json, convertir_a_dataframes

# Path to database file
DB_PATH = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.db')

# Crear una base de datos de prueba si no existe
@pytest.fixture(scope="session", autouse=True)
def crear_db_prueba():
    """
    Fixture que crea una base de datos de prueba si no existe
    para permitir que los tests se ejecuten correctamente
    """
    # Si la base de datos no existe, la creamos con datos de prueba
    if not os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Crear tablas
        cursor.execute('''
            CREATE TABLE productos (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                categoria TEXT NOT NULL,
                precio_unitario REAL NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE regiones (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                pais TEXT NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE vendedores (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                region_id INTEGER NOT NULL,
                FOREIGN KEY (region_id) REFERENCES regiones(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE ventas (
                id INTEGER PRIMARY KEY,
                fecha TEXT NOT NULL,
                producto_id INTEGER NOT NULL,
                vendedor_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                FOREIGN KEY (producto_id) REFERENCES productos(id),
                FOREIGN KEY (vendedor_id) REFERENCES vendedores(id)
            )
        ''')

        # Insertar datos de prueba
        cursor.executemany(
            "INSERT INTO productos VALUES (?, ?, ?, ?)",
            [
                (1, "Laptop", "Electrónica", 1200.00),
                (2, "Smartphone", "Electrónica", 800.00),
                (3, "Escritorio", "Muebles", 350.00)
            ]
        )

        cursor.executemany(
            "INSERT INTO regiones VALUES (?, ?, ?)",
            [
                (1, "Norte", "España"),
                (2, "Centro", "España")
            ]
        )

        cursor.executemany(
            "INSERT INTO vendedores VALUES (?, ?, ?)",
            [
                (1, "Ana Martínez", 1),
                (2, "Carlos López", 2)
            ]
        )

        cursor.executemany(
            "INSERT INTO ventas VALUES (?, ?, ?, ?, ?)",
            [
                (1, "2024-03-15", 1, 1, 2),
                (2, "2024-03-18", 2, 1, 3),
                (3, "2024-03-20", 3, 2, 1),
                (4, "2024-04-01", 1, 2, 1)
            ]
        )

        conn.commit()
        conn.close()

@pytest.fixture
def conexion_bd():
    """
    Fixture que obtiene una conexión a la base de datos utilizando la función
    conectar_bd implementada por el estudiante
    """
    # Llamar a la función del estudiante que debe conectar a la BD
    conn = conectar_bd()

    yield conn
    
    # Cerrar la conexión después de las pruebas
    if conn:
        conn.close()

def test_conectar_bd():
    """
    Prueba la función conectar_bd
    Verifica que devuelve una conexión válida a la base de datos SQLite
    """
    # Llamar a la función que debe conectar a la BD
    conn = conectar_bd()

    try:
        # Verificar que retorna un objeto conexión
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)

        # Verificar que se puede ejecutar una consulta SQL
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()

        # Verificar que hay al menos algunas tablas en la BD
        assert len(tablas) > 0

    finally:
        # Cerrar la conexión
        if conn:
            conn.close()

def test_conectar_bd_data():
    """
    Prueba el acceso a los datos de la base de datos usando conectar_bd
    Verifica que la base contiene las tablas y relaciones esperadas
    """
    conn = conectar_bd()

    try:
        cursor = conn.cursor()

        # Verificar las tablas existentes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = [tabla[0] for tabla in cursor.fetchall()]

        # Verificar que existen las tablas principales que esperamos
        tablas_esperadas = ["productos", "vendedores", "ventas", "regiones"]
        for tabla in tablas_esperadas:
            assert tabla in tablas, f"No se encontró la tabla {tabla}"

        # Verificar estructura de tabla productos
        cursor.execute("PRAGMA table_info(productos);")
        columnas_productos = [col[1] for col in cursor.fetchall()]
        assert "id" in columnas_productos
        assert "nombre" in columnas_productos
        assert "categoria" in columnas_productos
        assert "precio_unitario" in columnas_productos

        # Verificar que hay datos en las tablas
        for tabla in tablas_esperadas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla};")
            count = cursor.fetchone()[0]
            assert count > 0, f"La tabla {tabla} no contiene registros"

        # Verificar relaciones entre tablas
        # Ventas debe estar relacionada con productos y vendedores
        cursor.execute("PRAGMA foreign_key_list(ventas);")
        relaciones = cursor.fetchall()
        tablas_referenciadas = [rel[2] for rel in relaciones]  # rel[2] es la tabla referenciada
        assert "productos" in tablas_referenciadas
        assert "vendedores" in tablas_referenciadas

    finally:
        # Cerrar la conexión
        if conn:
            conn.close()

def test_convertir_a_json(conexion_bd):
    """
    Prueba la función convertir_a_json
    Verifica que convierte correctamente los datos de la BD a formato JSON
    """
    # Llamar a la función que convierte los datos a JSON
    datos_json = convertir_a_json(conexion_bd)
    
    # Verificar que el resultado es un diccionario
    assert isinstance(datos_json, dict)
    
    # Verificar que el diccionario contiene al menos una tabla
    assert len(datos_json) > 0

    # Verificar que cada valor en el diccionario es una lista
    for tabla, registros in datos_json.items():
        assert isinstance(registros, list)

        # Si hay registros, verificar que son diccionarios
        if registros:
            assert isinstance(registros[0], dict)

    # Verificar que se puede serializar a JSON
    try:
        json_string = json.dumps(datos_json)
        assert isinstance(json_string, str)
    except Exception as e:
        pytest.fail(f"No se pudo convertir a JSON string: {e}")

def test_convertir_a_json_data(conexion_bd):
    """
    Prueba el contenido de los datos convertidos a JSON
    Verifica que los datos tienen la estructura y relaciones esperadas
    """
    # Obtener los datos convertidos a JSON
    datos_json = convertir_a_json(conexion_bd)

    # Verificar que contiene todas las tablas principales
    tablas_esperadas = ["productos", "vendedores", "ventas", "regiones"]
    for tabla in tablas_esperadas:
        assert tabla in datos_json, f"No se encontró la tabla {tabla} en los datos JSON"

    # Verificar estructura de registros en productos
    if datos_json["productos"]:
        primer_producto = datos_json["productos"][0]
        campos_producto = ["id", "nombre", "categoria", "precio_unitario"]
        for campo in campos_producto:
            assert campo in primer_producto, f"Falta el campo {campo} en el primer producto"

    # Verificar estructura de registros en ventas
    if datos_json["ventas"]:
        primera_venta = datos_json["ventas"][0]
        campos_venta = ["id", "fecha", "producto_id", "vendedor_id", "cantidad"]
        for campo in campos_venta:
            assert campo in primera_venta, f"Falta el campo {campo} en la primera venta"

    # Verificar integridad referencial
    if datos_json["ventas"] and datos_json["productos"]:
        # Obtener los IDs de productos
        producto_ids = [p["id"] for p in datos_json["productos"]]

        # Verificar que al menos una venta referencia a un producto existente
        venta_con_producto_valido = False
        for venta in datos_json["ventas"]:
            if venta["producto_id"] in producto_ids:
                venta_con_producto_valido = True
                break

        assert venta_con_producto_valido, "No se encontró ninguna venta con referencia a un producto válido"

def test_convertir_a_dataframes(conexion_bd):
    """
    Prueba la función convertir_a_dataframes
    Verifica que extrae correctamente los datos de la BD a DataFrames de pandas
    """
    # Llamar a la función que convierte los datos a DataFrames
    dataframes = convertir_a_dataframes(conexion_bd)
    
    # Verificar que el resultado es un diccionario
    assert isinstance(dataframes, dict)
    
    # Verificar que el diccionario contiene al menos un DataFrame
    assert len(dataframes) > 0

    # Verificar que cada valor en el diccionario es un DataFrame de pandas
    for nombre, df in dataframes.items():
        assert isinstance(df, pd.DataFrame)

    # Verificar que tiene al menos una consulta combinada (con '_' o 'join' en el nombre)
    assert any(key for key in dataframes if "_" in key or "join" in key.lower())

def test_convertir_a_dataframes_data(conexion_bd):
    """
    Prueba el contenido de los datos convertidos a DataFrames
    Verifica que los DataFrames contienen los datos y relaciones esperadas
    """
    # Obtener los DataFrames
    dataframes = convertir_a_dataframes(conexion_bd)

    # Verificar que contiene DataFrames para las tablas principales
    tablas_esperadas = ["productos", "vendedores", "ventas", "regiones"]
    for tabla in tablas_esperadas:
        assert tabla in dataframes, f"No se encontró DataFrame para la tabla {tabla}"

    # Verificar las columnas en el DataFrame de productos
    columnas_productos = ["id", "nombre", "categoria", "precio_unitario"]
    for columna in columnas_productos:
        assert columna in dataframes["productos"].columns, f"Falta la columna {columna} en el DataFrame de productos"

    # Verificar las columnas en el DataFrame de ventas
    columnas_ventas = ["id", "fecha", "producto_id", "vendedor_id", "cantidad"]
    for columna in columnas_ventas:
        assert columna in dataframes["ventas"].columns, f"Falta la columna {columna} en el DataFrame de ventas"

    # Verificar que hay al menos un DataFrame con un join
    dfs_join = [nombre for nombre, df in dataframes.items() if "_" in nombre or "join" in nombre.lower()]
    assert len(dfs_join) > 0, "No se encontró ningún DataFrame con join entre tablas"

    # Para un DataFrame con join, verificar que contiene columnas de ambas tablas
    for df_join_name in dfs_join:
        df_join = dataframes[df_join_name]
        # Un DataFrame con join debería tener más columnas que las tablas individuales
        assert len(df_join.columns) > len(dataframes["ventas"].columns), f"El DataFrame {df_join_name} no parece contener un join válido"
