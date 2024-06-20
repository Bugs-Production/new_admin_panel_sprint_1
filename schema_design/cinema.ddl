-- Создание базы данных
CREATE DATABASE movies_database;

-- Создание схемы
CREATE SCHEMA IF NOT EXISTS content;

-- Создание таблицы film_work
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT not null,
    created timestamp with time zone,
    modified timestamp with time zone
);

-- Создание таблицы genre
CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

-- Создание таблицы person
CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

-- Создание промежуточной таблицы для связи film_work и genre
CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid REFERENCES content.genre(id),
    film_work_id uuid REFERENCES content.film_work(id),
    created timestamp with time zone
);

-- Создание промежуточной таблицы для связи film_work и person
CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid REFERENCES content.person(id),
    film_work_id uuid REFERENCES content.film_work(id),
    role TEXT,
    created timestamp with time zone
);

-- Индексы для поиска и уникальности
CREATE INDEX idx_film_work_title ON content.film_work (title);
CREATE UNIQUE INDEX idx_film_work_id_type ON content.film_work (id, type);
CREATE INDEX idx_genre_name ON content.genre (name);
CREATE INDEX idx_person_full_name ON content.person (full_name);
CREATE UNIQUE INDEX unique_genre_film_work ON content.genre_film_work (genre_id, film_work_id);
CREATE UNIQUE INDEX unique_person_film_work ON content.person_film_work (person_id, film_work_id, role);
