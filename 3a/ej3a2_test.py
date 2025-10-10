"""
Tests para el ejercicio ej3a2.py que trabaja con la inicialización de bases de datos
SQLite a partir de un archivo SQL y realiza operaciones básicas de modificación de datos.
"""

import pytest
import sqlite3
import os
from ej3a2 import (crear_bd_desde_sql, obtener_libros, agregar_libro,
                 actualizar_libro, obtener_autores)

# Path to SQL script and database
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'test.sql')
DB_PATH = os.path.join(os.path.dirname(__file__), 'biblioteca.db')

@pytest.fixture
def conexion_bd():
    """
    Fixture que obtiene una conexión a la base de datos utilizando la función
    crear_bd_desde_sql implementada por el estudiante
    """
    # Llamar a la función del estudiante que debe crear la conexión
    conn = crear_bd_desde_sql()

    yield conn

    # Cerrar la conexión después de las pruebas
    if conn:
        conn.close()

    # Eliminar el archivo de BD después de las pruebas
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except:
            pass

def test_crear_bd_desde_sql():
    """
    Prueba la función crear_bd_desde_sql
    Verifica que devuelve una conexión válida a la base de datos SQLite
    """
    # Llamar a la función que debe crear la BD
    conn = crear_bd_desde_sql()

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

        # Eliminar la BD para limpieza
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except:
                pass

def test_crear_bd_desde_sql_data():
    """
    Prueba el contenido de datos de la base de datos creada con crear_bd_desde_sql
    Verifica que la base contiene las tablas y datos esperados de test.sql
    """
    conn = crear_bd_desde_sql()

    try:
        cursor = conn.cursor()

        # Verificar las tablas existentes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = [tabla[0] for tabla in cursor.fetchall()]

        # Verificar que existen las tablas principales que esperamos
        tablas_esperadas = ["autores", "libros"]
        for tabla in tablas_esperadas:
            assert tabla in tablas, f"No se encontró la tabla {tabla}"

        # Verificar estructura de tabla autores
        cursor.execute("PRAGMA table_info(autores);")
        columnas_autores = [col[1] for col in cursor.fetchall()]
        assert "id" in columnas_autores
        assert "nombre" in columnas_autores

        # Verificar estructura de tabla libros
        cursor.execute("PRAGMA table_info(libros);")
        columnas_libros = [col[1] for col in cursor.fetchall()]
        assert "id" in columnas_libros
        assert "titulo" in columnas_libros
        assert "anio" in columnas_libros
        assert "autor_id" in columnas_libros

        # Verificar que hay datos en las tablas
        cursor.execute("SELECT COUNT(*) FROM autores;")
        count_autores = cursor.fetchone()[0]
        assert count_autores == 3, f"Deberían haber 3 autores, pero hay {count_autores}"

        cursor.execute("SELECT COUNT(*) FROM libros;")
        count_libros = cursor.fetchone()[0]
        assert count_libros == 6, f"Deberían haber 6 libros, pero hay {count_libros}"

        # Verificar algunos datos específicos
        cursor.execute("SELECT nombre FROM autores WHERE id=1;")
        autor = cursor.fetchone()
        assert autor is not None
        assert "García Márquez" in autor[0], "El primer autor debería ser García Márquez"

        cursor.execute("SELECT titulo FROM libros WHERE autor_id=2 LIMIT 1;")
        libro = cursor.fetchone()
        assert libro is not None, "No se encontró un libro de Isabel Allende (autor_id=2)"

    finally:
        # Cerrar la conexión
        if conn:
            conn.close()

        # Eliminar la BD para limpieza
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except:
                pass

def test_obtener_autores(conexion_bd):
    """
    Prueba la función obtener_autores
    Verifica que devuelve la lista de autores correctamente
    """
    # Llamar a la función que obtiene los autores
    autores = obtener_autores(conexion_bd)

    # Verificar que el resultado es una lista
    assert isinstance(autores, list)

    # Verificar que la lista contiene tuplas
    for autor in autores:
        assert isinstance(autor, tuple)

    # Verificar que hay 3 autores
    assert len(autores) == 3, f"Deberían haber 3 autores, pero hay {len(autores)}"

    # Verificar estructura de las tuplas (id, nombre)
    for autor in autores:
        assert len(autor) == 2, "Cada tupla debe tener 2 elementos: (id, nombre)"

def test_obtener_libros(conexion_bd):
    """
    Prueba la función obtener_libros
    Verifica que devuelve la lista de libros con información de sus autores
    """
    # Llamar a la función que obtiene los libros
    libros = obtener_libros(conexion_bd)

    # Verificar que el resultado es una lista
    assert isinstance(libros, list)

    # Verificar que la lista contiene tuplas
    for libro in libros:
        assert isinstance(libro, tuple)

    # Verificar que hay 6 libros
    assert len(libros) == 6, f"Deberían haber 6 libros, pero hay {len(libros)}"

    # Verificar estructura de las tuplas (id, titulo, anio, autor)
    for libro in libros:
        assert len(libro) == 4, "Cada tupla debe tener 4 elementos: (id, titulo, anio, autor)"

def test_agregar_libro(conexion_bd):
    """
    Prueba la función agregar_libro
    Verifica que agrega un nuevo libro correctamente
    """
    # Datos para el nuevo libro
    titulo = "Libro de prueba"
    anio = 2022
    autor_id = 1

    # Llamar a la función que agrega el libro
    libro_id = agregar_libro(conexion_bd, titulo, anio, autor_id)

    # Verificar que devuelve un ID válido
    assert libro_id is not None
    assert isinstance(libro_id, int)
    assert libro_id > 0

    # Verificar que el libro fue agregado correctamente
    cursor = conexion_bd.cursor()
    cursor.execute("SELECT titulo, anio, autor_id FROM libros WHERE id = ?", (libro_id,))
    libro = cursor.fetchone()

    assert libro is not None, f"No se encontró el libro con ID {libro_id}"
    assert libro[0] == titulo, f"El título no coincide: {libro[0]} != {titulo}"
    assert libro[1] == anio, f"El año no coincide: {libro[1]} != {anio}"
    assert libro[2] == autor_id, f"El ID del autor no coincide: {libro[2]} != {autor_id}"

def test_actualizar_libro(conexion_bd):
    """
    Prueba la función actualizar_libro
    Verifica que actualiza un libro existente correctamente
    """
    # Primero agregamos un libro para luego actualizarlo
    titulo_original = "Libro para actualizar"
    anio_original = 2020
    autor_id_original = 2

    libro_id = agregar_libro(conexion_bd, titulo_original, anio_original, autor_id_original)

    # Datos para la actualización
    nuevo_titulo = "Título actualizado"
    nuevo_anio = 2023

    # Actualizar solo el título
    actualizado1 = actualizar_libro(conexion_bd, libro_id, nuevo_titulo=nuevo_titulo)
    assert actualizado1 is True, "La función debería devolver True cuando se actualiza un libro"

    # Verificar que se actualizó correctamente
    cursor = conexion_bd.cursor()
    cursor.execute("SELECT titulo, anio, autor_id FROM libros WHERE id = ?", (libro_id,))
    libro = cursor.fetchone()

    assert libro[0] == nuevo_titulo, f"El título no se actualizó correctamente: {libro[0]} != {nuevo_titulo}"
    assert libro[1] == anio_original, f"El año no debería haberse modificado"

    # Actualizar solo el año
    actualizado2 = actualizar_libro(conexion_bd, libro_id, nuevo_anio=nuevo_anio)
    assert actualizado2 is True

    # Verificar que se actualizó correctamente
    cursor.execute("SELECT titulo, anio FROM libros WHERE id = ?", (libro_id,))
    libro = cursor.fetchone()

    assert libro[0] == nuevo_titulo, "El título no debería haberse modificado"
    assert libro[1] == nuevo_anio, f"El año no se actualizó correctamente: {libro[1]} != {nuevo_anio}"

    # Intentar actualizar un libro que no existe
    libro_id_inexistente = 9999
    actualizado3 = actualizar_libro(conexion_bd, libro_id_inexistente, nuevo_titulo="No debería actualizarse")
    assert actualizado3 is False, "La función debería devolver False cuando el libro no existe"
