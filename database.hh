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
#include "gc_clock.hh"
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
#include "tuple.hh"
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

using column_id = uint32_t;

struct column_definition final {
    enum column_kind { PARTITION, CLUSTERING, REGULAR, STATIC };
    sstring name;
    shared_ptr<abstract_type> type;
    column_id id; // unique within (kind, schema instance)
    column_kind kind;
    struct name_compare {
        bool operator()(const column_definition& cd1, const column_definition& cd2) const {
            return std::lexicographical_compare(
                    cd1.name.begin(), cd1.name.end(),
                    cd2.name.begin(), cd2.name.end(),
                    [] (char c1, char c2) { return uint8_t(c1) < uint8_t(c1); });
        }
    };
    bool is_static() const { return kind == column_kind::STATIC; }
};

struct thrift_schema {
    shared_ptr<abstract_type> partition_key_type;
};

/*
 * Keep this effectively immutable.
 */
class schema final {
private:
    std::unordered_map<bytes, column_definition*> _columns_by_name;
private:
    void annotate_columns(std::vector<column_definition>& columns, column_definition::column_kind kind);
public:
    gc_clock::duration default_time_to_live = gc_clock::duration::zero();
    const sstring ks_name;
    const sstring cf_name;
    const std::vector<column_definition> partition_key;
    const std::vector<column_definition> clustering_key;
    const std::vector<column_definition> regular_columns; // sorted by name
    shared_ptr<tuple_type<>> partition_key_type;
    shared_ptr<tuple_type<>> clustering_key_type;
    shared_ptr<tuple_prefix> clustering_key_prefix_type;
    thrift_schema thrift;
public:
    schema(sstring ks_name, sstring cf_name,
        std::vector<column_definition> partition_key,
        std::vector<column_definition> clustering_key,
        std::vector<column_definition> regular_columns);
    bool is_dense() const {
        return false;
    }
    bool is_counter() const {
        return false;
    }
    column_definition* get_column_definition(const bytes& name);
};

using schema_ptr = lw_shared_ptr<schema>;

struct column_family {
    column_family(schema_ptr schema);
    partition& find_or_create_partition(const bytes& key);
    row& find_or_create_row(const bytes& partition_key, const bytes& clustering_key);
    partition* find_partition(const bytes& key);
    row* find_row(const bytes& partition_key, const bytes& clustering_key);
    schema_ptr _schema;
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
