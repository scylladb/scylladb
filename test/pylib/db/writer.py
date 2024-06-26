import sqlite3
from typing import Dict, List

METRICS_TABLE = 'test_metrics'
RESOURCE_TIMELINE_TABLE = 'resource_timeline'
DEFAULT_DB_NAME = 'sqlite.db'
DATE_TIME_TEMPLATE = '%Y-%m-%d %H:%M:%S.%f'

create_table = [
    f"""CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
                id INTEGER PRIMARY KEY, 
                test_name VARCHAR(255) NOT NULL, 
                architecture VARCHAR(15) NOT NULL, 
                mode VARCHAR(15) NOT NULL, 
                user_usec INTEGER, 
                system_usec INTEGER, 
                usage_usec INTEGER, 
                memory_peak INTEGER,
                time_taken INTEGER,
                time_start DATETIME,
                time_end DATETIME,
                success BOOLEAN,
                directory VARCHAR(255),
                run_id INTEGER
        );""",
    f"""CREATE TABLE IF NOT EXISTS {RESOURCE_TIMELINE_TABLE}
(
    id        INTEGER PRIMARY KEY,
    memory    INTEGER,
    cpu       INTEGER,
    timestamp DATETIME
);
        """
]


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SQLiteWriter(metaclass=SingletonMeta):
    __instance = None

    def __init__(self, database_path):
        """
        Initializes the SQLWriter object.

        Args:
            database_path: Path to the SQLite database file.
        """
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('PRAGMA foreign_keys=ON')
        for table in create_table:
            self.cursor.execute(table).connection.commit()
        SQLiteWriter.__instance = self

    def write_row(self, data: Dict, table_name: str) -> int:
        """
        Inserts a single row of data into the specified table.

        Args:
            data: A dictionary where keys are column names and values are data to insert.
            table_name: Name of the table where data will be written.
        """

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = tuple(data.values())

        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(sql_query, values)
        last_row_id = self.cursor.lastrowid
        self.conn.commit()
        return last_row_id

    def write_multiple_rows(self, data_list: List[Dict], table_name: str) -> None:
        """
        Inserts multiple rows of data into the specified table.

        Args:
            data_list: A list of dictionaries, each representing a row of data.
            table_name: Name of the table where data will be written.
        """
        for data in data_list:
            self.write_row(data, table_name)

    def __del__(self):
        """
        Closes the database connection when the object is deleted.
        """
        self.conn.close()
