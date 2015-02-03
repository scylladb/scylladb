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

class column_definition final {
private:
    bytes _name;
public:
    enum column_kind { PARTITION, CLUSTERING, REGULAR, STATIC };
    column_definition(bytes name, data_type type, column_id id, column_kind kind);
    data_type type;
    column_id id; // unique within (kind, schema instance)
    column_kind kind;
    bool is_static() const { return kind == column_kind::STATIC; }
    const sstring& name_as_text() const;
    const bytes& name() const;
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
    std::map<bytes, column_definition*, serialized_compare> _regular_columns_by_name;
public:
    struct column {
        bytes name;
        data_type type;
        struct name_compare {
            shared_ptr<abstract_type> type;
            name_compare(shared_ptr<abstract_type> type) : type(type) {}
            bool operator()(const column& cd1, const column& cd2) const {
                return type->less(cd1.name, cd2.name);
            }
        };
    };
private:
    void build_columns(std::vector<column> columns, column_definition::column_kind kind, std::vector<column_definition>& dst);
    ::shared_ptr<cql3::column_specification> make_column_specification(column_definition& def);
public:
    gc_clock::duration default_time_to_live = gc_clock::duration::zero();
    const sstring ks_name;
    const sstring cf_name;
    std::vector<column_definition> partition_key;
    std::vector<column_definition> clustering_key;
    std::vector<column_definition> regular_columns; // sorted by name
    shared_ptr<tuple_type<>> partition_key_type;
    shared_ptr<tuple_type<>> clustering_key_type;
    shared_ptr<tuple_prefix> clustering_key_prefix_type;
    data_type regular_column_name_type;
    thrift_schema thrift;
public:
    schema(sstring ks_name, sstring cf_name,
        std::vector<column> partition_key,
        std::vector<column> clustering_key,
        std::vector<column> regular_columns,
        shared_ptr<abstract_type> regular_column_name_type);
    bool is_dense() const {
        return false;
    }
    bool is_counter() const {
        return false;
    }
    column_definition* get_column_definition(const bytes& name);
    auto regular_begin() {
        return regular_columns.begin();
    }
    auto regular_end() {
        return regular_columns.end();
    }
    auto regular_lower_bound(const bytes& name) {
        // TODO: use regular_columns and a version of std::lower_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.lower_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return regular_columns.begin() + i->second->id;
        }
    }
    auto regular_upper_bound(const bytes& name) {
        // TODO: use regular_columns and a version of std::upper_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.upper_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return regular_columns.begin() + i->second->id;
        }
    }
    column_id get_regular_columns_count() {
        return regular_columns.size();
    }
    data_type column_name_type(column_definition& def) {
        return def.kind == column_definition::REGULAR ? regular_column_name_type : utf8_type;
    }
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
    schema_ptr find_schema(sstring cf_name);
};

class database {
public:
    std::unordered_map<sstring, keyspace> keyspaces;
    static future<database> populate(sstring datadir);
    keyspace* find_keyspace(sstring name);
};


#endif /* DATABASE_HH_ */
