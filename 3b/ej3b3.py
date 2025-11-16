"""
Enunciado:
En este ejercicio, crearás una API REST sencilla con validación de esquema JSON para los endpoints POST.
La validación de esquemas JSON asegura que los datos enviados por los clientes cumplan con la estructura
y requisitos esperados antes de procesarlos.

Tareas:
1. Implementar validación de esquema JSON para el endpoint POST /authors
   usando el archivo author_schema.json
2. Implementar validación de esquema JSON para el endpoint POST /books
   usando el archivo book_schema.json
3. Devolver errores apropiados cuando los datos enviados no cumplan con los esquemas

Requerimientos de error:
- Si los datos no cumplen con el esquema JSON, debes devolver un error HTTP 400 (Bad Request)
- Para el endpoint /books, si el autor_id no existe, debes devolver un error HTTP 404 (Not Found)
- Debes incluir mensajes claros que indiquen la naturaleza del error

Esta versión utiliza Flask-SQLAlchemy como ORM para persistir los datos en una base de datos SQLite
y la biblioteca jsonschema para la validación de los datos de entrada.
"""

import os
import json
from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy
from jsonschema import validate, ValidationError

# Configura la base de datos
db = SQLAlchemy()

class Author(db.Model):
    """
    Modelo de autor usando SQLAlchemy ORM
    Debe tener: id, name y una relación con los libros
    """
    # Define la tabla 'authors' con:
    # - __tablename__ para especificar el nombre de la tabla
    __tablename__ = 'authors'

    # - id: clave primaria autoincremental
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # - name: nombre del autor (obligatorio)
    name = db.Column(db.String, nullable=False)

    # - Una relación con los libros usando db.relationship
    books = db.relationship("Book", backref="author", lazy=True, cascade="all, delete-orphan")

    @classmethod
    def load_schema(cls):
        """Carga el esquema JSON para validar datos de autor"""
        # Implementa este método para cargar el esquema desde el archivo 'author_schema.json'.
        with open('author_schema.json', 'r', encoding='utf-8') as f:
            return json.load(f)

    @classmethod
    def check_schema(cls, data):
        """Valida los datos contra el esquema JSON de autor"""
        # Implementa este método para validar los datos usando jsonschema.validate()
        schema = cls.load_schema()
        validate(instance=data, schema=schema)

    def to_dict(self):
        """Convierte el autor a un diccionario para la respuesta JSON"""
        # Implementa este método para devolver id y name
        # No incluyas la lista de libros para evitar recursión infinita
        return {
            "id": self.id,
            "name": self.name
        }


class Book(db.Model):
    """
    Modelo de libro usando SQLAlchemy ORM
    Debe tener: id, title, year (opcional), author_id y relación con el autor
    """
    # Define la tabla 'books' con:
    # - __tablename__ para especificar el nombre de la tabla
    __tablename__ = 'books'

    # - id: clave primaria autoincremental
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # - title: título del libro (obligatorio)
    title = db.Column(db.String, nullable=False)

    # - year: año de publicación (opcional)
    year = db.Column(db.Integer, nullable=True)

    # - author_id: clave foránea que relaciona con la tabla 'authors'
    author_id = db.Column(db.Integer, db.ForeignKey('authors.id'), nullable=False)

    @classmethod
    def load_schema(cls):
        """ Carga el esquema JSON para validar datos de libro """
        # Implementa este método para cargar el esquema desde el archivo 'book_schema.json'
        with open('book_schema.json', 'r', encoding='utf-8') as f:
            return json.load(f)

    @classmethod
    def check_schema(cls, data):
        """Valida los datos contra el esquema JSON de libro"""
        # Implementa este método similar a Author.check_schema()
        schema = cls.load_schema()
        validate(instance=data, schema=schema)

    def to_dict(self):
        """Convierte el libro a un diccionario para la respuesta JSON"""
        # Implementa este método para devolver id, title, year y author_id
        return {
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "author_id": self.author_id
        }


def create_app():
    """
    Crea y configura la aplicación Flask con SQLAlchemy
    """
    app = Flask(__name__)
    
    # Configuración de la base de datos SQLite en memoria
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Inicializa la base de datos con la aplicación
    db.init_app(app)
    
    # Crea todas las tablas en la base de datos
    with app.app_context():
        db.create_all()
    
    @app.route('/authors', methods=['POST'])
    def add_author():
        """
        Agrega un nuevo autor con validación de esquema JSON
        
        Implementa este endpoint para:
        1. Obtener los datos JSON de la solicitud
        2. Validar los datos usando el método Author.check_schema()
        3. Si la validación falla, devolver un error 400 con el mensaje de error
        4. Si la validación pasa, crear un nuevo autor con el nombre proporcionado
        5. Guardar el autor en la base de datos
        6. Devolver el autor creado con código 201
        
        Ejemplo de uso:
            POST /authors
            Content-Type: application/json
            
            {
                "name": "Gabriel García Márquez"
            }
            
        Respuesta exitosa:
            Status: 201 Created
            {
                "id": 1,
                "name": "Gabriel García Márquez"
            }
            
        Respuesta de error (si los datos no son válidos):
            Status: 400 Bad Request
            {
                "error": "Mensaje descriptivo del error de validación"
            }
        """
        # Obtener los datos JSON de la solicitud
        data = request.get_json()

        # Validar los datos usando el método Author.check_schema()
        try:
            Author.check_schema(data)
        except ValidationError as e:
            # Si la validación falla, devolver un error 400 con el mensaje de error
            return jsonify({"error": e.message}), 400

        # Si la validación pasa, crear un nuevo autor con el nombre proporcionado
        new_author = Author(name=data['name'])

        # Guardar el autor en la base de datos
        db.session.add(new_author)
        db.session.commit()

        # Devolver el autor creado con código 201
        return jsonify(new_author.to_dict()), 201

    @app.route('/books', methods=['POST'])
    def add_book():
        """
        Agrega un nuevo libro con validación de esquema JSON
        
        Implementa este endpoint para:
        1. Obtener los datos JSON de la solicitud
        2. Validar los datos usando el método Book.check_schema()
        3. Si la validación falla, devolver un error 400 con el mensaje de error
        4. Si la validación pasa, verificar que el author_id existe:
           - Buscar el autor en la base de datos
           - Si no existe, devolver un error 404 con mensaje adecuado
        5. Crear un nuevo libro con título, autor_id y año (si está presente)
        6. Guardar el libro en la base de datos
        7. Devolver el libro creado con código 201
        
        Ejemplo de uso:
            POST /books
            Content-Type: application/json
            
            {
                "title": "Cien años de soledad",
                "author_id": 1,
                "year": 1967
            }
            
        Respuesta exitosa:
            Status: 201 Created
            {
                "id": 1,
                "title": "Cien años de soledad",
                "author_id": 1,
                "year": 1967
            }
            
        Respuesta de error (si los datos no son válidos):
            Status: 400 Bad Request
            {
                "error": "Mensaje descriptivo del error de validación"
            }
            
        Respuesta de error (si el autor no existe):
            Status: 404 Not Found
            {
                "error": "No existe un autor con el id proporcionado"
            }
        """
        # Obtener los datos JSON de la solicitud
        data = request.get_json()

        # Validar los datos usando el método Book.check_schema()
        try:
            Book.check_schema(data)
        except ValidationError as e:
            # Si la validación falla, devolver un error 400 con el mensaje de error
            return jsonify({"error": e.message}), 400

        # Si la validación pasa, verificar que el author_id existe
        author = Author.query.get(data['author_id'])
        if not author:
            # Si no existe, devolver un error 404 con mensaje adecuado
            return jsonify({"error": "No existe un autor con el id proporcionado"}), 404

        # Crear un nuevo libro con título, autor_id y año (si está presente)
        new_book = Book(
            title=data['title'],
            author_id=data['author_id'],
            year=data.get('year')
        )

        # Guardar el libro en la base de datos
        db.session.add(new_book)
        db.session.commit()

        # Devolver el libro creado con código 201
        return jsonify(new_book.to_dict()), 201

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
