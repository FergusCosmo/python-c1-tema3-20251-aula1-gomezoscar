"""
Tests para el ejercicio ej3a4.py que utiliza la biblioteca PyMongo para
trabajar con bases de datos MongoDB.
"""

import pytest
import pymongo
import os
import shutil
from bson.objectid import ObjectId
from unittest.mock import patch, MagicMock

from ej3a4 import (verificar_mongodb_instalado, iniciar_mongodb_en_memoria,
                  crear_conexion, crear_colecciones, insertar_autores, insertar_libros,
                  consultar_libros, buscar_libros_por_autor, actualizar_libro,
                  eliminar_libro, ejemplo_transaccion, DB_NAME, MONGODB_PORT)

@pytest.fixture
def mongodb_proceso():
    """Fixture que inicia una instancia de MongoDB en memoria para las pruebas"""
    # Verificar si MongoDB está instalado
    if not verificar_mongodb_instalado():
        pytest.skip("MongoDB no está instalado, omitiendo pruebas")

    # Iniciar MongoDB en memoria
    proceso = iniciar_mongodb_en_memoria()
    if not proceso:
        pytest.skip("No se pudo iniciar MongoDB, omitiendo pruebas")

    yield proceso

    # Detener MongoDB
    if proceso:
        proceso.terminate()
        proceso.wait()

        # Eliminar directorio temporal
        temp_dir = os.path.join(os.path.dirname(__file__), "temp_mongodb")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

@pytest.fixture
def db(mongodb_proceso):
    """Fixture que proporciona una conexión a la base de datos MongoDB de prueba"""
    # Crear conexión
    db_conn = crear_conexion()

    # Limpiar cualquier dato previo
    db_conn.drop_collection("autores")
    db_conn.drop_collection("libros")

    yield db_conn

@pytest.fixture
def db_con_colecciones(db):
    """Fixture que proporciona una base de datos con las colecciones ya creadas"""
    crear_colecciones(db)
    return db

@pytest.fixture
def db_con_datos(db_con_colecciones):
    """Fixture que proporciona una base de datos con colecciones y datos de ejemplo"""
    # Insertar autores de ejemplo
    autores = [
        ("Gabriel García Márquez",),
        ("Isabel Allende",),
        ("Jorge Luis Borges",)
    ]
    autor_ids = insertar_autores(db_con_colecciones, autores)

    # Insertar libros de ejemplo
    libros = [
        ("Cien años de soledad", 1967, autor_ids[0]),
        ("El amor en los tiempos del cólera", 1985, autor_ids[0]),
        ("La casa de los espíritus", 1982, autor_ids[1]),
        ("Eva Luna", 1987, autor_ids[1]),
        ("Ficciones", 1944, autor_ids[2]),
        ("El Aleph", 1949, autor_ids[2])
    ]
    insertar_libros(db_con_colecciones, libros)

    return db_con_colecciones

@patch('ej3a4.verificar_mongodb_instalado')
def test_verificar_mongodb_instalado(mock_verificar):
    """Prueba la función verificar_mongodb_instalado"""
    # Configurar el mock para devolver True
    mock_verificar.return_value = True

    # Llamar a la función
    resultado = verificar_mongodb_instalado()

    # Verificar el resultado
    assert isinstance(resultado, bool)

@patch('subprocess.Popen')
def test_iniciar_mongodb_en_memoria(mock_popen):
    """Prueba la función iniciar_mongodb_en_memoria"""
    # Configurar el mock para simular un proceso que se inicia correctamente
    mock_proceso = MagicMock()
    mock_proceso.poll.return_value = None  # El proceso está corriendo
    mock_popen.return_value = mock_proceso

    # Llamar a la función real con un mock para time.sleep
    with patch('time.sleep'):  # Para no esperar realmente
        proceso = iniciar_mongodb_en_memoria()

    # Verificar el resultado
    assert proceso is not None

def test_crear_conexion(mongodb_proceso):
    """Prueba la función crear_conexion"""
    # Crear la conexión
    db = crear_conexion()

    # Verificar que la conexión es válida
    assert db is not None
    assert isinstance(db, pymongo.database.Database)

    # Verificar el nombre de la base de datos
    assert db.name == DB_NAME

def test_crear_colecciones(db_con_colecciones):
    """Prueba la función crear_colecciones"""
    # Verificar que las colecciones se han creado correctamente
    colecciones = db_con_colecciones.list_collection_names()

    assert "autores" in colecciones
    assert "libros" in colecciones

    # Verificar índices (opcional, dependerá de la implementación del estudiante)
    indices_autores = db_con_colecciones.autores.index_information()
    indices_libros = db_con_colecciones.libros.index_information()

    # Siempre debe existir al menos el índice _id
    assert "_id_" in indices_autores
    assert "_id_" in indices_libros

def test_insertar_autores(db_con_colecciones):
    """Prueba la función insertar_autores"""
    # Datos de prueba
    autores = [
        ("Gabriel García Márquez",),
        ("Isabel Allende",),
        ("Jorge Luis Borges",)
    ]

    # Insertar autores
    autor_ids = insertar_autores(db_con_colecciones, autores)

    # Verificar que se devolvieron IDs
    assert len(autor_ids) == 3

    # Verificar que los autores se insertaron correctamente
    autores_en_db = list(db_con_colecciones.autores.find().sort("_id", 1))

    assert len(autores_en_db) == 3
    assert autores_en_db[0]["nombre"] == "Gabriel García Márquez"
    assert autores_en_db[1]["nombre"] == "Isabel Allende"
    assert autores_en_db[2]["nombre"] == "Jorge Luis Borges"

    # Verificar que los IDs devueltos corresponden con los documentos insertados
    for i, autor_id in enumerate(autor_ids):
        autor = db_con_colecciones.autores.find_one({"_id": ObjectId(autor_id) if isinstance(autor_id, str) else autor_id})
        assert autor is not None
        assert autor["nombre"] == autores[i][0]

def test_insertar_libros(db_con_colecciones):
    """Prueba la función insertar_libros"""
    # Primero insertamos autores para tener las claves foráneas
    autores = [
        ("Gabriel García Márquez",),
        ("Isabel Allende",)
    ]
    autor_ids = insertar_autores(db_con_colecciones, autores)

    # Datos de prueba para libros
    libros = [
        ("Cien años de soledad", 1967, autor_ids[0]),
        ("La casa de los espíritus", 1982, autor_ids[1])
    ]

    # Insertar libros
    libro_ids = insertar_libros(db_con_colecciones, libros)

    # Verificar que se devolvieron IDs
    assert len(libro_ids) == 2

    # Verificar que los libros se insertaron correctamente
    libros_en_db = list(db_con_colecciones.libros.find().sort("_id", 1))

    assert len(libros_en_db) == 2
    assert libros_en_db[0]["titulo"] == "Cien años de soledad"
    assert libros_en_db[0]["anio"] == 1967
    assert libros_en_db[1]["titulo"] == "La casa de los espíritus"
    assert libros_en_db[1]["anio"] == 1982

    # Verificar las referencias a los autores
    assert str(libros_en_db[0]["autor_id"]) == str(autor_ids[0])
    assert str(libros_en_db[1]["autor_id"]) == str(autor_ids[1])

    # Verificar que los IDs devueltos corresponden con los documentos insertados
    for i, libro_id in enumerate(libro_ids):
        libro = db_con_colecciones.libros.find_one({"_id": ObjectId(libro_id) if isinstance(libro_id, str) else libro_id})
        assert libro is not None
        assert libro["titulo"] == libros[i][0]
        assert libro["anio"] == libros[i][1]

def test_consultar_libros(db_con_datos, capfd):
    """Prueba la función consultar_libros usando capfd para capturar la salida estándar"""
    consultar_libros(db_con_datos)

    # Capturar la salida estándar
    salida, _ = capfd.readouterr()

    # Verificar que la salida contiene información de los libros
    assert "Cien años de soledad" in salida
    assert "Gabriel García Márquez" in salida
    assert "La casa de los espíritus" in salida
    assert "Isabel Allende" in salida
    assert "Ficciones" in salida
    assert "Jorge Luis Borges" in salida

def test_buscar_libros_por_autor(db_con_datos):
    """Prueba la función buscar_libros_por_autor"""
    # Buscar libros de Gabriel García Márquez
    libros = buscar_libros_por_autor(db_con_datos, "Gabriel García Márquez")

    # Verificar los resultados
    assert len(libros) == 2
    titulos = [libro[0] for libro in libros]
    anios = [libro[1] for libro in libros]

    assert "Cien años de soledad" in titulos
    assert "El amor en los tiempos del cólera" in titulos
    assert 1967 in anios
    assert 1985 in anios

def test_actualizar_libro(db_con_datos):
    """Prueba la función actualizar_libro"""
    # Primero obtenemos el ID del primer libro
    primer_libro = db_con_datos.libros.find_one({"titulo": "Cien años de soledad"})
    libro_id = str(primer_libro["_id"])

    # Actualizar el título del libro
    actualizado = actualizar_libro(db_con_datos, libro_id,
                                  nuevo_titulo="Cien años de soledad (Edición especial)")

    # Verificar que la función devuelve True (éxito)
    assert actualizado is True

    # Verificar que el libro se actualizó correctamente
    libro_actualizado = db_con_datos.libros.find_one({"_id": ObjectId(libro_id)})

    assert libro_actualizado["titulo"] == "Cien años de soledad (Edición especial)"
    assert libro_actualizado["anio"] == 1967  # El año no debe cambiar

    # Actualizar sólo el año
    actualizado = actualizar_libro(db_con_datos, libro_id, nuevo_anio=2020)
    assert actualizado is True

    libro_actualizado = db_con_datos.libros.find_one({"_id": ObjectId(libro_id)})
    assert libro_actualizado["titulo"] == "Cien años de soledad (Edición especial)"
    assert libro_actualizado["anio"] == 2020

    # Actualizar ambos campos
    actualizado = actualizar_libro(db_con_datos, libro_id,
                                  nuevo_titulo="Título actualizado", nuevo_anio=2021)
    assert actualizado is True

    libro_actualizado = db_con_datos.libros.find_one({"_id": ObjectId(libro_id)})
    assert libro_actualizado["titulo"] == "Título actualizado"
    assert libro_actualizado["anio"] == 2021

def test_eliminar_libro(db_con_datos):
    """Prueba la función eliminar_libro"""
    # Primero obtenemos el ID del último libro
    ultimo_libro = db_con_datos.libros.find_one({"titulo": "El Aleph"})
    libro_id = str(ultimo_libro["_id"])

    # Verificar que el libro existe antes de eliminarlo
    assert db_con_datos.libros.count_documents({"_id": ObjectId(libro_id)}) == 1

    # Verificar el total de libros antes de eliminar
    total_libros_inicial = db_con_datos.libros.count_documents({})

    # Eliminar el libro
    eliminado = eliminar_libro(db_con_datos, libro_id)

    # Verificar que la función devuelve True (éxito)
    assert eliminado is True

    # Verificar que el libro fue eliminado
    assert db_con_datos.libros.count_documents({"_id": ObjectId(libro_id)}) == 0

    # Verificar que sólo se eliminó ese libro
    assert db_con_datos.libros.count_documents({}) == total_libros_inicial - 1

def test_ejemplo_transaccion(db_con_datos):
    """Prueba la función ejemplo_transaccion"""
    # Obtener el estado inicial de la base de datos
    libros_inicial = db_con_datos.libros.count_documents({})

    # Ejecutar la transacción
    resultado = ejemplo_transaccion(db_con_datos)

    # Verificar el resultado de la transacción
    assert isinstance(resultado, bool)

    # La implementación específica dependerá del estudiante,
    # pero comprobamos que la función no genera errores
    # y que potencialmente haya modificado algo en la base de datos
    libros_final = db_con_datos.libros.count_documents({})

    # Nota: No podemos asumir exactamente qué hace la transacción,
    # pero verificamos que la función existe y se ejecuta sin errores
    assert True
