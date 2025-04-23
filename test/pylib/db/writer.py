#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import datetime
import sqlite3
import os
from typing import List
from multiprocessing import Lock
from contextlib import contextmanager

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

def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()

sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)



class SQLiteWriter:
    _lock = Lock()

    def __init__(self, database_path):
        """
        Initializes the SQLWriter object.

        Args:
            database_path: Path to the SQLite database file.
        """
        self.database_path = database_path
        self.pid = os.getpid()
        self._connection = None
        self._cursor = None

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('PRAGMA foreign_keys=ON')
            cursor.execute('PRAGMA synchronous=off')
            for table in create_table:
                cursor.execute(table)
            conn.commit()

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a database connection.
        Ensures proper handling of connections per process and automatic closing.
        """
        current_pid = os.getpid()

        # If we're in a new process or don't have a connection, create one
        if self._connection is None or self.pid != current_pid:
            if self._connection is not None:
                self._connection.close()
            self._connection = sqlite3.connect(
                self.database_path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                timeout=30
            )
            self.pid = current_pid

        try:
            yield self._connection
        except Exception as e:
            self._connection.rollback()
            raise e

    @contextmanager
    def get_cursor(self):
        """
        Context manager for getting a database cursor.
        Ensures proper transaction handling and automatic commits.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

    def write_row(self, model, table_name: str) -> int:
        """
        Inserts a single row of data into the specified table.

        Args:
            model: A AttrsInstance object with a data to insert.
            table_name: Name of the table where data is being written.

        Return:
            int: Returns the ID of the inserted record
        """
        with self._lock:
            with self.get_cursor() as cursor:
                data = asdict(model)
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['?'] * len(data))
                values = tuple(data.values())
                sql_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
                cursor.execute(sql_query, values)
                return cursor.lastrowid

    def write_multiple_rows(self, data_list: List[AttrsInstance], table_name: str) -> None:
        """
        Inserts multiple rows of data into the specified table.

        Args:
            data_list: A list of AttrsInstance objects, each representing a row of data.
            table_name: Name of the table where data is being written.
        """
        for model in data_list:
            self.write_row(model, table_name)

    def write_row_if_not_exist(self, model, table_name: str) -> int:
        """
        Writes a row to the table if it doesn't exist, otherwise returns the existing row's ID.

        Args:
            model: A AttrsInstance object with a data to insert.
            table_name: Name of the table where data is being written.

        Return:
            int: Returns the ID of the existing or newly inserted record
        """
        with self._lock:
            with self.get_cursor() as cursor:
                data = asdict(model)
                values = tuple(data.values())

                # Construct the SQL query to retrieve the ID if the record exists
                select_query = f"""
                    SELECT id FROM {table_name} WHERE {
                    ' AND '.join([f"{col} = ?" for col in data.keys()])
                    }
                """

                cursor.execute(select_query, values)
                existing_row = cursor.fetchone()

        if existing_row:
            return existing_row[0]
        else:
            return self.write_row(model, table_name)
