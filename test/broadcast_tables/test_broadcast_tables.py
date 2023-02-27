#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import random
import string
from typing import Dict, List, Optional, Tuple
import pytest
import enum
from cassandra.query import PreparedStatement


def random_string(size=10) -> str:
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(size))


class QueriesHandler:
    def __init__(self, cql):
        self.cql = cql
        self.prepared_select: PreparedStatement = cql.prepare(f"SELECT value FROM system.broadcast_kv_store WHERE key = ?;")
        self.prepared_update: PreparedStatement = cql.prepare(f"UPDATE system.broadcast_kv_store SET value = ? WHERE key = ?;")
        self.prepared_conditional_update: PreparedStatement = cql.prepare(f"UPDATE system.broadcast_kv_store SET value = ? WHERE key = ? IF value = ?;")

    def select(self, key: str) -> Optional[str]:
        result = list(self.cql.execute(self.prepared_select, parameters=[key]))

        if len(result) == 0:
            return None

        assert len(result) == 1
        return result[0].value

    def update(self, key: str, new_value: str) -> None:
        self.cql.execute(self.prepared_update, parameters=[new_value, key])

    def update_conditional(self, key: str, new_value: str, value_condition: Optional[str]) -> Tuple[bool, Optional[str]]:
        result = list(self.cql.execute(self.prepared_conditional_update, parameters=[new_value, key, value_condition]))

        assert len(result) == 1
        return (result[0].applied, result[0].value)


# For now, only the following types of statements can be compiled:
# * select value where key = CONST from system.broadcast_kv_store;
# * update system.broadcast_kv_store set value = CONST where key = CONST;
# * update system.broadcast_kv_store set value = CONST where key = CONST if value = CONST;
# where CONST is string literal.
class QueryType(enum.Enum):
    SELECT = enum.auto()
    UPDATE = enum.auto()
    CONDITIONAL_UPDATE = enum.auto()

class ConditionType(enum.Enum):
    EXISTING_KEY_PASS = enum.auto()
    EXISTING_KEY_FAIL = enum.auto()
    NEW_KEY = enum.auto()

def test_broadcast_kv_store(cql) -> None:
    seed = random.randint(0, 2 ** 64)
    print(f"Seed: {seed}")
    random.seed(seed)

    experimental_features = list(cql.execute("SELECT value FROM system.config WHERE name = 'experimental_features'"))[0].value
    assert "broadcast-tables" in experimental_features

    random_strings: List[str] = [random_string() for _ in range(42)]
    remaining_strings = random_strings.copy()
    kv_store: Dict[str, str] = {}

    queries_handler = QueriesHandler(cql)

    query_types = random.choices(
            [QueryType.SELECT, QueryType.UPDATE, QueryType.CONDITIONAL_UPDATE],
            weights=[0.3, 0.2, 0.5], k=3721
        )
    for query_type in query_types:
        key = random.choice(random_strings)

        if query_type == QueryType.SELECT:
            value = queries_handler.select(key)
            assert value == kv_store.get(key)
        else:
            new_value = random.choice(random_strings)

            if query_type == QueryType.UPDATE:
                queries_handler.update(key, new_value)
                kv_store[key] = new_value
            elif query_type == QueryType.CONDITIONAL_UPDATE:
                condition_type: ConditionType
                if len(kv_store) == 0:
                    condition_type = ConditionType.NEW_KEY
                elif len(remaining_strings) == 0:
                    condition_type = random.choice([ConditionType.EXISTING_KEY_PASS, ConditionType.EXISTING_KEY_FAIL])
                else:
                    condition_type = random.choices(
                            [ConditionType.NEW_KEY, ConditionType.EXISTING_KEY_PASS, ConditionType.EXISTING_KEY_FAIL],
                            weights=[0.2, 0.4, 0.4], k=1
                        )[0]

                value_condition: Optional[str]
                if condition_type == ConditionType.EXISTING_KEY_FAIL:
                    value_condition = random.choice(list[Optional[str]](random_strings) + [None])
                elif condition_type == ConditionType.EXISTING_KEY_PASS:
                    value_condition = kv_store.get(key)
                elif condition_type == ConditionType.NEW_KEY:
                    key = remaining_strings.pop()
                    value_condition = None

                result = queries_handler.update_conditional(key, new_value, value_condition)
                applied = False
                previous_value = kv_store.get(key)
                if previous_value == value_condition:
                    applied = True
                    kv_store[key] = new_value
                assert (applied, previous_value) == result
