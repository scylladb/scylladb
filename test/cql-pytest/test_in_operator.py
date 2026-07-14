# -*- coding: utf-8 -*-
# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
import random
import string
from typing import List, Any

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import Session, NoHostAvailable

from util import unique_name


@pytest.fixture(scope="module")
def table(cql: Session, test_keyspace: str) -> str:
    table = f"{test_keyspace}.{unique_name()}"

    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, a ascii, b boolean, d double,"
                f"mai map<ascii, int>, tup frozen<tuple<text, int>>, l list<text>, s set<text>)")
    yield table
    cql.execute("DROP TABLE " + table)


@pytest.fixture(scope="module")
def rows(cql: Session, table: str) -> List[Any]:
    rows = [
        [
            idx,
            str(random.randint(0, 255)),
            bool(random.randint(0, 1)),
            random.uniform(10, 20),
            {str(random.randint(0, 255)): random.randint(0, 1000)},
            ("".join(random.choices(string.printable, k=random.randint(10, 20))), random.randint(0, 255)),
            ["".join(random.choices(string.printable, k=random.randint(10, 20))) for _ in range(10)],
            {"".join(random.choices(string.printable, k=random.randint(10, 20))) for _ in range(10)},
        ]
        for idx in range(3)
    ]

    insert_query = cql.prepare(
        f"INSERT INTO {table} (p, a, b, d, mai, tup, l, s) VALUES ({', '.join(8 * '?')});")
    for row in rows:
        cql.execute(insert_query, row)
    return rows


@pytest.mark.parametrize("column,column_type", [
    ("mai", "map<ascii, int>"),
    ("l", "list<text>"),
    ("s", "set<text>"),
])
def test_in_query_with_invalid_column_type(cql: Session, table: str, column: str, column_type: str):
    pattern = r".*code=2200 \[Invalid query\].*IN predicates on non-primary-key columns|cannot be restricted by a" \
              r" 'IN' relation"
    with pytest.raises(InvalidRequest, match=pattern):
        cql.prepare(f"SELECT * FROM {table} where {column} IN (?, ?) ALLOW FILTERING;")


def test_in_query_without_allow_filtering(cql: Session, table: str):
    pattern = r".*code=2200 \[Invalid query\].*IN predicates on non-primary-key columns|use ALLOW FILTERING"
    with pytest.raises(InvalidRequest, match=pattern):
        cql.prepare(f"SELECT * FROM {table} where p IN (?, ?) AND a IN (?, ?);")


def test_in_query_with_valid_product_limit(cql: Session, rows: List[Any], table: str):
    # The maximums (one for partition keys and one for clustering keys) default to 100
    real_scylla_cartesian_product_limit = 100
    query = f"SELECT * FROM {table} WHERE p IN " \
            f"{tuple(idx for idx in range(real_scylla_cartesian_product_limit))} ALLOW FILTERING;"
    assert len(cql.execute(query).current_rows) == len(rows)


def test_in_query_with_invalid_product_limit(cql: Session, scylla_only, table: str):
    limit = 101
    with pytest.raises(NoHostAvailable, match=f"partition key cartesian product size {limit} is greater than"
                                              f" maximum 100"):
        cql.execute(f"SELECT * FROM {table} WHERE p IN {tuple(idx for idx in range(limit))} ALLOW FILTERING;")
