import sqlite3
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import List, Optional, Union


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

    def load_data(self, table_name: str, schema: Optional[Union[FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork]], batch_size: int = 10) -> List:
        """Загружаем данные из таблиц SQLite."""
        with self.connection as connect:
            cursor = connect.cursor()  # по поводу менеджера для sqlite не поддерживается

            # Формируем строку полей для запроса SELECT
            if schema is not None:
                fields_from_sql = ", ".join(f.name for f in fields(schema))
                query = f"SELECT {fields_from_sql} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"

            cursor.execute(query)
            while True:
                data = cursor.fetchmany(batch_size)
                if not data:
                    break
                yield [dict(row) for row in data]
