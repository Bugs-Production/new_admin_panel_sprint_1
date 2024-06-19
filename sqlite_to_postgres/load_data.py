import sqlite3

import psycopg
from psycopg import ClientCursor
from psycopg import connection as _connection
from psycopg.rows import dict_row
from read_sqlite import SQLiteLoader
from save_postgres import PostgresSaver


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    try:
        for data in sqlite_loader.load_movies():
            postgres_saver.save_all_data(data, "film_work")

        for data in sqlite_loader.load_genres():
            postgres_saver.save_all_data(data, "genre")

        for data in sqlite_loader.load_persons():
            postgres_saver.save_all_data(data, "person")

        for data in sqlite_loader.load_genre_film_work():
            postgres_saver.save_all_data(data, "genre_film_work")

        for data in sqlite_loader.load_person_film_work():
            postgres_saver.save_all_data(data, "person_film_work")
    except Exception as e:
        print(f"Ошибка при переносе данных: {e}")


if __name__ == "__main__":
    dsl = {
        "dbname": "movies_database",
        "user": "app",
        "password": "123qwe",
        "host": "127.0.0.1",
        "port": 5432,
    }
    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
