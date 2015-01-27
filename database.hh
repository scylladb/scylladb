/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "db_clock.hh"
#include <functional>
#include <boost/any.hpp>
#include <cstdint>
#include <boost/variant.hpp>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "core/future.hh"

struct row;
struct paritition;
struct column_family;

struct row {
    std::vector<bytes> cells;
};

struct partition {
    explicit partition(column_family& cf);
    row static_columns;
    // row key within partition -> row
    std::map<bytes, row, key_compare> rows;
};

struct column_definition {
    sstring name;
    shared_ptr<abstract_type> type;
    struct name_compare {
        bool operator()(const column_definition& cd1, const column_definition& cd2) const {
            return std::lexicographical_compare(
                    cd1.name.begin(), cd1.name.end(),
                    cd2.name.begin(), cd2.name.end(),
                    [] (char c1, char c2) { return uint8_t(c1) < uint8_t(c1); });
        }
    };
};

struct column_family {
    column_family(shared_ptr<abstract_type> partition_key_type, shared_ptr<abstract_type> clustering_key_type);
    // primary key = paritition key + clustering_key
    shared_ptr<abstract_type> partition_key_type;
    shared_ptr<abstract_type> clustering_key_type;
    std::vector<column_definition> partition_key;
    std::vector<column_definition> clustering_key;
    std::vector<column_definition> column_defs; // sorted by name
    partition& find_or_create_partition(const bytes& key);
    row& find_or_create_row(const bytes& partition_key, const bytes& clustering_key);
    partition* find_partition(const bytes& key);
    row* find_row(const bytes& partition_key, const bytes& clustering_key);
    // partition key -> partition
    std::map<bytes, partition, key_compare> partitions;
};

class keyspace {
public:
    std::unordered_map<sstring, column_family> column_families;
    static future<keyspace> populate(sstring datadir);
};

class database {
public:
    std::unordered_map<sstring, keyspace> keyspaces;
    static future<database> populate(sstring datadir);
};


#endif /* DATABASE_HH_ */
