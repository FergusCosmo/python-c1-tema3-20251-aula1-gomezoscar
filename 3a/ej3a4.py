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
    # TODO: Implementar la conexión a MongoDB
    pass

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # TODO: Implementar la creación de colecciones e índices
    pass

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        autores: Lista de tuplas (nombre,)
    """
    # TODO: Implementar la inserción de autores
    pass

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        libros: Lista de tuplas (titulo, anio, autor_id)
    """
    # TODO: Implementar la inserción de libros
    pass

def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # TODO: Implementar la consulta de libros con sus autores
    pass

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB
        nombre_autor: Nombre del autor a buscar

    Returns:
        Lista de tuplas (titulo, anio)
    """
    # TODO: Implementar la búsqueda de libros por autor
    pass

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
    # TODO: Implementar la actualización de un libro
    pass

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
    # TODO: Implementar la eliminación de un libro
    pass

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de transacciones para operaciones agrupadas
    
    Args:
        db: Objeto de conexión a la base de datos MongoDB

    Returns:
        `True` si la transacción se completó correctamente, `False` en caso de error.
    """
    # TODO: Implementar un ejemplo de transacción
    pass

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

        # TODO: Implementar el código para probar las funciones

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
