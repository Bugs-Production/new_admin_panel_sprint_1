import sqlite3
import os

import psycopg
from psycopg import ClientCursor
from psycopg import connection as _connection
from psycopg.rows import dict_row
from read_sqlite import SQLiteLoader, Genre, FilmWork, Person, GenreFilmWork, PersonFilmWork
from save_postgres import PostgresSaver
from dotenv import load_dotenv

dotenv_path = "../movies_admin/config/.env"
load_dotenv(dotenv_path=dotenv_path)

table_name_and_fields = {
    "film_work": FilmWork,
    "genre": Genre,
    "person": Person,
    "genre_film_work": GenreFilmWork,
    "person_film_work": PersonFilmWork,
}


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    try:
        for table_name, schema in table_name_and_fields.items():
            sqlite_data = sqlite_loader.load_data(table_name, schema)
            for data in sqlite_data:
                postgres_saver.save_all_data(data, table_name)
    except Exception as e:
        print(f"Ошибка при переносе данных: {e}")


if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST", "127.0.0.1"),
        "port": os.environ.get("DB_PORT", 5432),
    }
    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
