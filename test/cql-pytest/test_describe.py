
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for server-side describe
###############################################################################

import random
from contextlib import contextmanager
from util import new_type, unique_name, new_test_table, new_test_keyspace

### =========================== UTILITY FUNCTIONS =============================

# Configuration parameters for randomly created elements (keyspaces/tables/types)

# If depth reaches 0, random type will return native type
max_depth = 2
# How many types can be in collection/tuple/UDT
max_types = 5
# How many rows can be in partition key
max_pk = 3
# How many rows can be in clustering key
max_ck = 5
# How many regular rows can be in table
max_regular = 4
# How many static rows can be in table
max_static = 3
# Chance of counter table instead of normal one
counter_table_chance = 0.1

# A utility function for obtaining random native type
def get_random_native_type():
    types = ["text", "bigint", "boolean", "decimal", "double", "float", "int"]
    return random.choice(types)

# A utility function for obtaining random collection type.
# `udts` argument represents user-defined types that can be used for
# creating the collection type. If `frozen` is true, then the function will
# return `frozen<collection_type>`
def get_random_collection_type(cql, keyspace, depth, udts=[], frozen=False):
    types = ["set", "list", "map"]
    collection_type = random.choice(types)

    if collection_type == "set":
        return_type = f"set<{get_random_type(cql, keyspace, depth-1, udts, True)}>"
    elif collection_type == "list":
        return_type = f"list<{get_random_type(cql, keyspace, depth-1, udts, True)}>"
    elif collection_type == "map":
        return_type = f"map<{get_random_type(cql, keyspace, depth-1, udts, True)}, {get_random_type(cql, keyspace, depth-1, udts, True)}>"
    return f"frozen<{return_type}>" if frozen else return_type

# A utility function for obtaining random tuple type.
# `udts` argument represents user-defined types that can be used for
# creating the tuple type. If `frozen` is true, then the function will
# return `frozen<tuple<some_fields>>`
def get_random_tuple_type(cql, keyspace, depth, udts=[], frozen=False):
    n = random.randrange(1, max_types+1)
    types = [get_random_type(cql, keyspace, depth-1, udts, False) for _ in range(n)]
    fields = ", ".join(types)
    return_type = f"tuple<{fields}>"

    return f"frozen<{return_type}>" if frozen else return_type

# A utility function for obtaining random type. The function can return
# native types, collections, tuples and UDTs. `udts` argument represents
# user-defined types that can be returned or used for creation of other types
# If `frozen` is true, then the function will return `frozen<some_type>`
def get_random_type(cql, keyspace, depth, udts=[], frozen=False):
    types = ["native", "collection", "tuple"]
    if len(udts) != 0:
        types.append("UDT")
    type_type = random.choice(types)

    if type_type == "native" or depth <= 0: # native type
        return get_random_native_type()
    elif type_type == "collection":
        return get_random_collection_type(cql, keyspace, depth, udts, frozen)
    elif type_type == "tuple":
        return get_random_tuple_type(cql, keyspace, depth, udts, frozen)
    elif type_type == "UDT":
        udt = random.choice(udts)
        return f"frozen<{udt}>" if frozen else udt

def new_random_keyspace(cql):
    strategies = ["SimpleStrategy", "NetworkTopologyStrategy"]
    writes = ["true", "false"]

    options = {}
    options["class"] = random.choice(strategies)
    options["replication_factor"] = random.randrange(1, 6)
    options_str = ", ".join([f"'{k}': '{v}'" for (k, v) in options.items()])

    write = random.choice(writes)
    return new_test_keyspace(cql, f"with replication = {{{options_str}}} and durable_writes = {write}")

# A utility function for creating random table. `udts` argument represents
# UDTs that can be used to create the table. The function uses `new_test_table`
# from util.py, so it can be used in a "with", as:
#   with new_random_table(cql, test_keyspace) as table:
def new_random_table(cql, keyspace, udts=[]):
    pk_n = random.randrange(1, max_pk)
    ck_n = random.randrange(max_ck)
    regular_n = random.randrange(1, max_regular)
    static_n = random.randrange(max_static if ck_n > 0 else 1)

    pk = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts, True)) for _ in range(pk_n)]
    ck = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts, True)) for _ in range(ck_n)]
    pk_fields = ", ".join([f"{n} {t}" for (n, t) in pk])
    ck_fields = ", ".join([f"{n} {t}" for (n, t) in ck])

    partition_key = ", ".join([n for (n, _) in pk])
    clustering_fields = ", ".join([n for (n, _) in ck])
    clustering_key = f", {clustering_fields}" if ck_n > 0 else ""
    primary_def = f"PRIMARY KEY(({partition_key}){clustering_key})"


    schema = ""
    extras = {}
    if random.random() <= counter_table_chance:
        counter_name = unique_name()
        schema = f"{pk_fields}, {ck_fields}, {counter_name} counter, {primary_def}"
    else:
        regular = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts)) for _ in range(regular_n)]
        static = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts)) for _ in range(static_n)]
        regular_fields = ", ".join([f"{n} {t}" for (n, t) in regular])
        static_fields = ", ".join([f"{n} {t} static" for (n, t) in static])
        schema = f"{pk_fields}, {ck_fields}, {regular_fields}, {static_fields}, {primary_def}"
        
        extras["default_time_to_live"] = random.randrange(1000000)


    caching_keys = ["ALL", "NONE"]
    caching_rows = ["ALL", "NONE", random.randrange(1000000)]
    caching_k = random.choice(caching_keys)
    caching_r = random.choice(caching_rows)
    extras["caching"] = f"{{'keys':'{caching_k}', 'rows_per_partition':'{caching_r}'}}"

    compactions = ["SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "LeveledCompactionStrategy"]
    extras["compaction"] = f"{{'class': '{random.choice(compactions)}'}}"

    speculative_retries = ["ALWAYS", "NONE", "99.0PERCENTILE", "50.00ms"]
    extras["speculative_retry"] = f"'{random.choice(speculative_retries)}'"

    compressions = ["LZ4Compressor", "SnappyCompressor", "DeflateCompressor"]
    extras["compression"] = f"{{'sstable_compression': '{random.choice(compressions)}'}}"

    extras["bloom_filter_fp_chance"] = round(random.random(), 5)
    extras["crc_check_chance"] = round(random.random(), 5)
    extras["gc_grace_seconds"] = random.randrange(100000, 1000000)
    min_idx_interval = random.randrange(100, 1000)
    extras["min_index_interval"] = min_idx_interval
    extras["max_index_interval"] = random.randrange(min_idx_interval, 10000)
    extras["memtable_flush_period_in_ms"] = random.randrange(0, 10000)

    extra_options = [f"{k} = {v}" for (k, v) in extras.items()]
    extra_str = " AND ".join(extra_options)
    extra = f" WITH {extra_str}"
    return new_test_table(cql, keyspace, schema, extra)

# A utility function for creating random UDT. `udts` argument represents
# other UDTs that can be used to create the new one. The function uses `new_type`
# from util.py, so it can be used in a "with", as:
#   with new_random_type(cql, test_keyspace) as udt:
def new_random_type(cql, keyspace, udts=[]):
    n = random.randrange(1, max_types+1)
    types = [get_random_type(cql, keyspace, max_depth, udts, True) for _ in range(n)]
    fields = ", ".join([f"{unique_name()} {t}" for t in types])
    
    return new_type(cql, keyspace, f"({fields})")

### ===========================================================================


