#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import sqlite3
from typing import List

from attr import AttrsInstance, asdict

TESTS_TABLE = 'tests'
METRICS_TABLE = 'test_metrics'
SYSTEM_RESOURCE_METRICS_TABLE = 'system_resource_metrics'
CGROUP_MEMORY_METRICS_TABLE = 'cgroup_memory_metrics'
DEFAULT_DB_NAME = 'sqlite.db'
DATE_TIME_TEMPLATE = '%Y-%m-%d %H:%M:%S.%f'

create_table = [
    f'''
    CREATE TABLE IF NOT EXISTS {TESTS_TABLE} (
        id INTEGER PRIMARY KEY,
        architecture VARCHAR(15) NOT NULL,
        directory VARCHAR(255),
        mode VARCHAR(15) NOT NULL,
        run_id INTEGER,
        test_name VARCHAR(255) NOT NULL
    );
    ''',

    f'''
    CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
        id INTEGER PRIMARY KEY,
        test_id INT NOT NULL,
        user_sec REAL,
        system_sec REAL,
        usage_sec REAL,
        memory_peak INTEGER,
        time_taken REAL,
        time_start DATETIME,
        time_end DATETIME,
        success BOOLEAN,
        FOREIGN KEY(test_id) REFERENCES {TESTS_TABLE}(id)
    );
    ''',

    f'''
    CREATE TABLE IF NOT EXISTS {SYSTEM_RESOURCE_METRICS_TABLE} (
        id INTEGER PRIMARY KEY,
        memory REAL,
        cpu REAL,
        timestamp DATETIME
    );
    ''',

    f'''
    CREATE TABLE IF NOT EXISTS {CGROUP_MEMORY_METRICS_TABLE} (
        id INTEGER PRIMARY KEY,
        test_id INT NOT NULL,
        memory REAL,
        timestamp DATETIME,
        FOREIGN KEY(test_id) REFERENCES {TESTS_TABLE}(id)
    );
    '''
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
        self.lock = asyncio.Lock()
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('PRAGMA foreign_keys=ON')
        self.cursor.execute('PRAGMA sychronous=off')
        for table in create_table:
            self.cursor.execute(table).connection.commit()
        SQLiteWriter.__instance = self

    def write_row(self, model, table_name: str) -> int:
        """
        Inserts a single row of data into the specified table.

        Args:
            model: A AttrsInstance object with a data to insert.
            table_name: Name of the table where data is being written.

        Return:
            str: Returns the ID of the inserted record
        """
        data = asdict(model)
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = tuple(data.values())

        sql_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        self.cursor.execute(sql_query, values)
        last_row_id = self.cursor.lastrowid
        self.conn.commit()
        return last_row_id

    def write_multiple_rows(self, data_list: List[AttrsInstance], table_name: str) -> None:
        """
        Inserts multiple rows of data into the specified table.

        Args:
            data_list: A list of AttrsInstance objects, each representing a row of data.
            table_name: Name of the table where data is being written.
        """
        for model in data_list:
            self.write_row(model, table_name)

    def __del__(self):
        """
        Closes the database connection when the object is deleted.
        """
        self.conn.close()

    def write_row_if_not_exist(self, model, table_name: str):
        data = asdict(model)
        values = tuple(data.values())

        # Construct the SQL query to retrieve the ID if the record exists
        select_query = f"""
                SELECT id FROM {table_name} WHERE {
        ' AND '.join([f"{col} = ?" for col in data.keys()])
        }
        """

        # Execute the select query first
        cursor = self.conn.execute(select_query, values)
        existing_row = cursor.fetchone()

        if existing_row:
            # Record exists, return its ID
            return existing_row[0]
        else:
            return self.write_row(model, table_name)