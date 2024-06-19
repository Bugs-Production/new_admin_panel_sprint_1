from typing import Dict, List

import psycopg


class PostgresSaver:
    def __init__(self, connection: psycopg.Connection):
        self.connection = connection

    def save_all_data(self, data_gen: List, table: str):
        """Сохраняем данные в PostgreSQL."""
        cursor = self.connection.cursor()
        for data in data_gen:
            self._save_data(cursor, data, table)
        self.connection.commit()

    def _save_data(self, cursor: psycopg.ClientCursor, data: Dict, table: str):
        if not data:
            return

        columns = data.keys()
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        query = f"INSERT INTO content.{table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"

        try:
            cursor.execute(query, tuple(data.values()))
        except Exception as e:
            raise RuntimeError(
                f"Ошибка при выполнении запроса: {query} с параметрами {tuple(data.values())}: {e}"
            )
