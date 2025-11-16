"""
Enunciado:
Desarrolla una API REST utilizando Flask que permita realizar operaciones básicas sobre una biblioteca
con dos modelos relacionados: Autores y Libros.
La API debe exponer los siguientes endpoints:

Autores:
1. `GET /authors`: Devuelve la lista completa de autores.
2. `POST /authors`: Agrega un nuevo autor. El cuerpo de la solicitud debe incluir un JSON con el campo "name".
3. `GET /authors/<author_id>`: Obtiene los detalles de un autor específico y su lista de libros.

Libros:
1. `GET /books`: Devuelve la lista completa de libros.
2. `POST /books`: Agrega un nuevo libro. El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional).
3. `DELETE /books/<book_id>`: Elimina un libro específico por su ID.
4. `PUT /books/<book_id>`: Actualiza la información de un libro existente. El cuerpo puede incluir "title" y/o "year".

Esta versión utiliza Flask-SQLAlchemy como ORM para persistir los datos en una base de datos SQLite.
"""

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Define aquí tus modelos
# Usa los mismos modelos que en el ejercicio anterior: Author y Book

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
    
    # Endpoints de Autores
    @app.route('/authors', methods=['GET'])
    def get_authors():
        """
        Devuelve la lista completa de autores
        """
        # Consulta todos los autores
        authors = Author.query.all()
        # Convierte cada autor a diccionario usando to_dict()
        authors_list = [author.to_dict() for author in authors]
        # Devuelve la lista en formato JSON
        return jsonify(authors_list)

    @app.route('/authors', methods=['POST'])
    def add_author():
        """
        Agrega un nuevo autor
        El cuerpo de la solicitud debe incluir un JSON con el campo "name"
        """
        # Obtiene los datos JSON de la solicitud
        data = request.get_json()
        if not data or 'name' not in data:
            return jsonify({"error": "El campo 'name' es obligatorio"}), 400

        # Crea un nuevo autor con el nombre proporcionado
        new_author = Author(name=data['name'])

        # Lo guarda en la base de datos
        db.session.add(new_author)
        db.session.commit()

        # Devuelve el autor creado con código 201
        return jsonify(new_author.to_dict()), 201

    @app.route('/authors/<int:author_id>', methods=['GET'])
    def get_author(author_id):
        """
        Obtiene los detalles de un autor específico y su lista de libros
        """
        # Busca el autor por ID (usa get_or_404 para gestionar el error 404)
        author = Author.query.get_or_404(author_id)

        # Devuelve los detalles del autor y su lista de libros
        author_data = author.to_dict()
        author_data['books'] = [book.to_dict() for book in author.books]
        return jsonify(author_data)

    # Endpoints de Libros
    @app.route('/books', methods=['GET'])
    def get_books():
        """
        Devuelve la lista completa de libros
        """
        # Consulta todos los libros
        books = Book.query.all()
        # Convierte cada libro a diccionario
        books_list = [book.to_dict() for book in books]
        # Devuelve la lista en formato JSON
        return jsonify(books_list)

    @app.route('/books', methods=['POST'])
    def add_book():
        """
        Agrega un nuevo libro
        El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional)
        """
        # Obtiene los datos JSON de la solicitud
        data = request.get_json()
        if not data or 'title' not in data or 'author_id' not in data:
            return jsonify({"error": "Los campos 'title' y 'author_id' son obligatorios"}), 400

        # Crea un nuevo libro con título, autor_id y año (opcional)
        new_book = Book(
            title=data['title'],
            author_id=data['author_id'],
            year=data.get('year')  # Opcional
        )

        # Lo guarda en la base de datos
        db.session.add(new_book)
        db.session.commit()

        # Devuelve el libro creado con código 201
        return jsonify(new_book.to_dict()), 201

    @app.route('/books/<int:book_id>', methods=['GET'])
    def get_book(book_id):
        """
        Obtiene un libro específico por su ID
        """
        # Busca el libro por ID (usa get_or_404 para gestionar el error 404)
        book = Book.query.get_or_404(book_id)
        # Devuelve los detalles del libro
        return jsonify(book.to_dict())

    @app.route('/books/<int:book_id>', methods=['DELETE'])
    def delete_book(book_id):
        """
        Elimina un libro específico por su ID
        """
        # Busca el libro por ID (usa get_or_404)
        book = Book.query.get_or_404(book_id)
        # Elimina el libro de la base de datos
        db.session.delete(book)
        db.session.commit()
        # Devuelve respuesta vacía con código 204
        return '', 204

    @app.route('/books/<int:book_id>', methods=['PUT'])
    def update_book(book_id):
        """
        Actualiza la información de un libro existente
        El cuerpo puede incluir "title" y/o "year"
        """
        # Obtiene los datos JSON de la solicitud
        data = request.get_json()
        # Busca el libro por ID (usa get_or_404)
        book = Book.query.get_or_404(book_id)

        # Actualiza los campos proporcionados (título y/o año)
        if 'title' in data:
            book.title = data['title']
        if 'year' in data:
            book.year = data['year']

        # Guarda los cambios en la base de datos
        db.session.commit()

        # Devuelve el libro actualizado
        return jsonify(book.to_dict())

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
