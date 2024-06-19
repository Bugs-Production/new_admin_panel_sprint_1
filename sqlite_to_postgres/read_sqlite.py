import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class FilmWork:
    title: str
    type: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)
    description: Optional[str] = None
    creation_date: Optional[str] = None


@dataclass
class Genre:
    name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    description: Optional[str] = None


@dataclass
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: Optional[str] = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.connection.row_factory = sqlite3.Row

    def load_movies(self, batch_size: int = 10) -> List[FilmWork]:
        """Загружаем данные из таблицы фильмов."""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM film_work")
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]

    def load_genres(self, batch_size: int = 10) -> List[Genre]:
        """Загружаем данные из таблицы жанров."""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM genre")
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]

    def load_persons(self, batch_size: int = 10) -> List[Person]:
        """Загружаем данные из таблицы персон."""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM person")
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]

    def load_genre_film_work(self, batch_size: int = 10) -> List[GenreFilmWork]:
        """Загружаем данные из таблицы жанры и фильмы."""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM genre_film_work")
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]

    def load_person_film_work(self, batch_size: int = 10) -> List[PersonFilmWork]:
        """Загружаем данные из таблицы актеры и фильмы."""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM person_film_work")
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]
