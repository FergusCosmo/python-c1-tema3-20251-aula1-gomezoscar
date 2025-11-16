"""
Enunciado:
En este ejercicio aprenderás a utilizar MongoDB con Python para trabajar
con bases de datos NoSQL. MongoDB es una base de datos orientada a documentos que
almacena datos en formato similar a JSON (BSON).

Tareas:
1. Conectar a una base de datos MongoDB
2. Crear colecciones (equivalentes a tablas en SQL)
3. Insertar, actualizar, consultar y eliminar documentos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de MongoDB desde Python utilizando PyMongo.
"""

import pymongo
import subprocess
import time
import os
import sys
import shutil
from typing import List, Dict, Any, Optional, Tuple, Union

# Configuración de MongoDB
DB_NAME = "biblioteca"
MONGODB_PORT = 27017

def verificar_mongodb_instalado() -> bool:
    """
    Verifica si MongoDB está instalado en el sistema
    """
    try:
        # Intentamos ejecutar mongod --version para verificar que está instalado
        result = subprocess.run(["mongod", "--version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def iniciar_mongodb_en_memoria() -> Optional[subprocess.Popen]:
    """
    Inicia una instancia de MongoDB en memoria para pruebas
    """
    # Crear directorio temporal para MongoDB
    temp_dir = os.path.join(os.path.dirname(__file__), "temp_mongodb")
    os.makedirs(temp_dir, exist_ok=True)

    # Iniciar MongoDB con almacenamiento en memoria
    cmd = ["mongod", "--storageEngine", "inMemory", "--dbpath", temp_dir, "--port", str(MONGODB_PORT)]

    try:
        proceso = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Dar tiempo para que MongoDB se inicie
        time.sleep(2)

        # Verificar que MongoDB se ha iniciado correctamente
        if proceso.poll() is not None:
            raise Exception("No se pudo iniciar MongoDB")

        return proceso
    except Exception as e:
        print(f"Error al iniciar MongoDB: {e}")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return None

def crear_conexion() -> pymongo.database.Database:
    """
    Crea y devuelve una conexión a la base de datos MongoDB
    """
    # Conectar al servidor MongoDB
    cliente = pymongo.MongoClient(f"mongodb://localhost:{MONGODB_PORT}")
    # Acceder a la base de datos
    db = cliente[DB_NAME]
    return db

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # No es necesario crear colecciones explícitamente en MongoDB
    # Se crean automáticamente al insertar el primer documento
    # Aquí solo creamos índices para optimizar consultas
    db["autores"].create_index("nombre")
    db["libros"].create_index("titulo")
    db["libros"].create_index("autor_id")

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        autores: Lista de tuplas (nombre,)
    """
    autores_docs = [{"nombre": nombre} for nombre, in autores]
    resultado = db["autores"].insert_many(autores_docs)
    return resultado.inserted_ids

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        libros: Lista de tuplas (titulo, anio, autor_id)
    """
    libros_docs = [{"titulo": titulo, "anio": anio, "autor_id": autor_id} for titulo, anio, autor_id in libros]
    resultado = db["libros"].insert_many(libros_docs)
    return resultado.inserted_ids

def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # Hacer una consulta para obtener libros con sus autores
    for libro in db["libros"].find():
        autor = db["autores"].find_one({"_id": libro["autor_id"]})
        autor_nombre = autor["nombre"] if autor else "Desconocido"
        print(f"- {libro['titulo']} ({libro['anio']}) - {autor_nombre}")

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        nombre_autor: Nombre del autor a buscar

    Returns:
        Lista de tuplas (titulo, anio)
    """
    autor = db["autores"].find_one({"nombre": nombre_autor})
    if not autor:
        return []
    libros = db["libros"].find({"autor_id": autor["_id"]})
    return [(libro["titulo"], libro["anio"]) for libro in libros]

def actualizar_libro(
        db: pymongo.database.Database,
        id_libro: str,
        nuevo_titulo: Optional[str]=None, nuevo_anio: Optional[int]=None
) -> bool:
    """
    Actualiza la información de un libro existente
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        id_libro: ID del libro a actualizar
        nuevo_titulo: Nuevo título del libro (opcional)
        nuevo_anio: Nuevo año de publicación (opcional)

    Returns:
        `True` si se actualizó correctamente, `False` si no se encontró el libro.

    """
    update_doc = {}
    if nuevo_titulo is not None:
        update_doc["titulo"] = nuevo_titulo
    if nuevo_anio is not None:
        update_doc["anio"] = nuevo_anio

    if not update_doc:
        return True  # No hay campos para actualizar

    resultado = db["libros"].update_one({"_id": id_libro}, {"$set": update_doc})
    return resultado.modified_count > 0

def eliminar_libro(
        db: pymongo.database.Database,
        id_libro: str
) -> bool:
    """
    Elimina un libro por su ID
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        id_libro: ID del libro a eliminar

    Returns:
        `True` si se eliminó correctamente, `False` si no se encontró el libro.
    """
    resultado = db["libros"].delete_one({"_id": id_libro})
    return resultado.deleted_count > 0

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de transacciones para operaciones agrupadas
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB

    Returns:
        `True` si la transacción se completó correctamente, `False` en caso de error.
    """
    try:
        # Las transacciones requieren un clúster replicado o MongoDB 4.0+
        # Para este ejemplo, usamos una operación simple sin transacción
        # ya que MongoDB en memoria no soporta transacciones
        print("Agregando nuevo autor y libro...")
        nuevo_autor = {"nombre": "Nuevo Autor"}
        autor_result = db["autores"].insert_one(nuevo_autor)
        nuevo_libro = {"titulo": "Nuevo Libro", "anio": 2025, "autor_id": autor_result.inserted_id}
        libro_result = db["libros"].insert_one(nuevo_libro)
        print("Operaciones completadas sin transacción (no soportada en inMemory).")
        return True
    except Exception as e:
        print(f"Error en la transacción: {e}")
        return False

if __name__ == "__main__":
    mongodb_proceso = None
    db = None

    try:
        # Verificar si MongoDB está instalado
        if not verificar_mongodb_instalado():
            print("Error: MongoDB no está instalado o no está disponible en el PATH.")
            print("Por favor, instala MongoDB y asegúrate de que esté en tu PATH.")
            sys.exit(1)

        # Iniciar MongoDB en memoria para el ejercicio
        print("Iniciando MongoDB en memoria...")
        mongodb_proceso = iniciar_mongodb_en_memoria()
        if not mongodb_proceso:
            print("No se pudo iniciar MongoDB. Asegúrate de tener los permisos necesarios.")
            sys.exit(1)

        print("MongoDB iniciado correctamente.")

        # Crear una conexión
        print("Conectando a MongoDB...")
        db = crear_conexion()
        print("Conexión establecida correctamente.")

        # Crear colecciones e índices
        crear_colecciones(db)

        # Insertar autores
        autores = [
            ("Gabriel García Márquez",),
            ("Isabel Allende",),
            ("Jorge Luis Borges",)
        ]
        autores_ids = insertar_autores(db, autores)
        print("Autores insertados correctamente")

        # Insertar libros
        libros = [
            ("Cien años de soledad", 1967, autores_ids[0]),
            ("El amor en los tiempos del cólera", 1985, autores_ids[0]),
            ("La casa de los espíritus", 1982, autores_ids[1]),
            ("Paula", 1994, autores_ids[1]),
            ("Ficciones", 1944, autores_ids[2]),
            ("El Aleph", 1949, autores_ids[2])
        ]
        libros_ids = insertar_libros(db, libros)
        print("Libros insertados correctamente")

        print("\n--- Lista de todos los libros con sus autores ---")
        consultar_libros(db)

        print("\n--- Búsqueda de libros por autor ---")
        nombre_autor = "Gabriel García Márquez"
        libros_autor = buscar_libros_por_autor(db, nombre_autor)
        print(f"Libros de {nombre_autor}:")
        for titulo, anio in libros_autor:
            print(f"- {titulo} ({anio})")

        print("\n--- Actualización de un libro ---")
        id_libro_a_actualizar = libros_ids[0]
        actualizado = actualizar_libro(db, id_libro_a_actualizar, nuevo_titulo="Cien años de soledad (Edición especial)")
        if actualizado:
            print("Libro actualizado. Nueva información:")
            consultar_libros(db)
        else:
            print("No se pudo actualizar el libro.")

        print("\n--- Eliminación de un libro ---")
        id_libro_a_eliminar = libros_ids[-1]  # Elimina "El Aleph"
        eliminado = eliminar_libro(db, id_libro_a_eliminar)
        if eliminado:
            print("Libro eliminado. Lista actualizada:")
            consultar_libros(db)
        else:
            print("No se pudo eliminar el libro.")

        print("\n--- Demostración de transacción ---")
        ejemplo_transaccion(db)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            mongodb_proceso.terminate()
            mongodb_proceso.wait()

            # Eliminar directorio temporal
            temp_dir = os.path.join(os.path.dirname(__file__), "temp_mongodb")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            print("MongoDB detenido correctamente.")
