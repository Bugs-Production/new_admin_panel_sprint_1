import datetime
import sqlite3

import psycopg
import pytest
from sqlite_to_postgres.read_sqlite import (FilmWork, Genre, GenreFilmWork,
                                            Person, PersonFilmWork,
                                            SQLiteLoader)

dsl = {
    "dbname": "movies_database",
    "user": "app",
    "password": "123qwe",
    "host": "127.0.0.1",
    "port": 5432,
}


def test_count_values_for_sqlite_and_postgres():
    """Проверяем совпадает ли кол-во записей в базах данных."""
    with sqlite3.connect("../db.sqlite") as sql_connect:
        sqlite_loader = SQLiteLoader(sql_connect)

    # получаем кол-во элементов в каждой таблице SQLite
    genres_count_sqlite = sum(
        len(item) for item in sqlite_loader.load_data("genre", Genre)
    )
    movies_count_sqlite = sum(
        len(item) for item in sqlite_loader.load_data("film_work", FilmWork)
    )
    persons_count_sqlite = sum(
        len(item) for item in sqlite_loader.load_data("person", Person)
    )
    genre_and_film_work_count_sqlite = sum(
        len(item) for item in sqlite_loader.load_data("genre_film_work", GenreFilmWork)
    )
    person_and_film_work_count_sqlite = sum(
        len(item)
        for item in sqlite_loader.load_data("person_film_work", PersonFilmWork)
    )

    # получаем кол-во элементов в каждой таблице PostgreSQL
    with psycopg.connect(**dsl) as pg_conn:
        cur = pg_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM content.genre")
        genres_count_postgresql = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM content.film_work")
        movies_count_postgresql = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM content.person")
        person_count_postgresql = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM content.genre_film_work")
        genre_and_film_work_count_postgresql = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM content.person_film_work")
        person_and_film_work_count_postgresql = cur.fetchone()[0]

    # проверяем одинаковое кол-во в базе данных
    assert genres_count_sqlite == genres_count_postgresql
    assert movies_count_sqlite == movies_count_postgresql
    assert persons_count_sqlite == person_count_postgresql
    assert genre_and_film_work_count_sqlite == genre_and_film_work_count_postgresql
    assert person_and_film_work_count_sqlite == person_and_film_work_count_postgresql


def test_data_consistency_between_sqlite_and_postgresql():
    """Проверка соответствия данных между SQLite и PostgreSQL."""
    with sqlite3.connect("../db.sqlite") as sql_connect:
        sqlite_loader = SQLiteLoader(sql_connect)

    # получаем данные из PostgreSQL
    with psycopg.connect(**dsl) as pg_conn:
        cur = pg_conn.cursor()
        cur.execute("SELECT * FROM content.genre")
        genres_postgresql = cur.fetchall()

        cur.execute("SELECT * FROM content.person")
        person_postgresql = cur.fetchall()

        cur.execute("SELECT * FROM content.genre_film_work")
        genre_film_work_postgresql = cur.fetchall()

        cur.execute("SELECT * FROM content.person_film_work")
        person_film_work_postgresql = cur.fetchall()

    # получаем данные из sqlite для проверки
    genres_sqlite = sqlite_loader.load_data("genre", Genre)
    movies_sqlite = sqlite_loader.load_data("film_work", FilmWork)
    persons_sqlite = sqlite_loader.load_data("person", Person)
    genre_and_film_work_sqlite = sqlite_loader.load_data(
        "genre_film_work", GenreFilmWork
    )
    person_and_film_work_sqlite = sqlite_loader.load_data(
        "person_film_work", PersonFilmWork
    )

    # делаем единый список для каждой таблицы, с элементами словарей
    genres_sqlite_extend = [item for genre in genres_sqlite for item in genre]
    persons_sqlite_extend = [item for person in persons_sqlite for item in person]
    genre_and_film_work_extend = [
        item for sublist in genre_and_film_work_sqlite for item in sublist
    ]
    person_and_film_work_extend = [
        item for sublist in person_and_film_work_sqlite for item in sublist
    ]

    # проверяем данные таблицы genre
    for i in range(len(genres_postgresql)):
        assert str(genres_postgresql[i][0]) == genres_sqlite_extend[i].get("id")
        assert genres_postgresql[i][1] == genres_sqlite_extend[i].get("name")
        assert genres_postgresql[i][2] == genres_sqlite_extend[i].get("description")

        # преобразовываем время в секунды для сравнения
        created_genres_psql = datetime.datetime.strptime(
            str(genres_postgresql[i][3])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        created_genres_sqlite = datetime.datetime.strptime(
            genres_sqlite_extend[i].get("created_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        updated_genres_psql = datetime.datetime.strptime(
            str(genres_postgresql[i][4])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        updated_genres_sqlite = datetime.datetime.strptime(
            genres_sqlite_extend[i].get("updated_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        assert created_genres_psql == created_genres_sqlite
        assert updated_genres_psql == updated_genres_sqlite

    # проверяем данные в person
    for i in range(len(person_postgresql)):
        assert str(person_postgresql[i][0]) == persons_sqlite_extend[i].get("id")
        assert person_postgresql[i][1] == persons_sqlite_extend[i].get("full_name")

        # преобразовываем время в секунды для сравнения
        created_persons_psql = datetime.datetime.strptime(
            str(person_postgresql[i][2])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        created_persons_sqlite = datetime.datetime.strptime(
            persons_sqlite_extend[i].get("created_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        updated_persons_psql = datetime.datetime.strptime(
            str(person_postgresql[i][3])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        updated_persons_sqlite = datetime.datetime.strptime(
            persons_sqlite_extend[i].get("updated_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        assert created_persons_psql == created_persons_sqlite
        assert updated_persons_psql == updated_persons_sqlite

    # проверяем данные в person_film_work
    for i in range(len(person_film_work_postgresql)):
        assert str(person_film_work_postgresql[i][0]) == person_and_film_work_extend[
            i
        ].get("id")
        assert person_film_work_postgresql[i][1] == person_and_film_work_extend[i].get(
            "role"
        )

        # преобразовываем время в секунды для сравнения
        created_psql = datetime.datetime.strptime(
            str(person_film_work_postgresql[i][2])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        created_sqlite = datetime.datetime.strptime(
            person_and_film_work_extend[i].get("created_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        assert created_psql == created_sqlite
        assert str(person_film_work_postgresql[i][3]) == person_and_film_work_extend[
            i
        ].get("film_work_id")
        assert str(person_film_work_postgresql[i][4]) == person_and_film_work_extend[
            i
        ].get("person_id")

    # проверяем данные в genre_film_work
    for i in range(len(genre_film_work_postgresql)):
        assert str(genre_film_work_postgresql[i][0]) == genre_and_film_work_extend[
            i
        ].get("id")

        # преобразовываем время в секунды для сравнения
        created_psql = datetime.datetime.strptime(
            str(genre_film_work_postgresql[i][1])[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        created_sqlite = datetime.datetime.strptime(
            genre_and_film_work_extend[i].get("created_at")[:19], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        assert created_psql == created_sqlite
        assert str(genre_film_work_postgresql[i][2]) == genre_and_film_work_extend[
            i
        ].get("film_work_id")
        assert str(genre_film_work_postgresql[i][3]) == genre_and_film_work_extend[
            i
        ].get("genre_id")
