/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <unordered_map>
#include <boost/range/iterator_range.hpp>

#include "cql3/column_specification.hh"
#include "core/shared_ptr.hh"
#include "types.hh"
#include "tuple.hh"
#include "gc_clock.hh"

using column_id = uint32_t;

class column_definition final {
private:
    bytes _name;
public:
    enum class column_kind { PARTITION, CLUSTERING, REGULAR, STATIC };
    column_definition(bytes name, data_type type, column_id id, column_kind kind);
    data_type type;
    column_id id; // unique within (kind, schema instance)
    column_kind kind;
    ::shared_ptr<cql3::column_specification> column_specification;
    bool is_static() const { return kind == column_kind::STATIC; }
    bool is_partition_key() const { return kind == column_kind::PARTITION; }
    bool is_clustering_key() const { return kind == column_kind::CLUSTERING; }
    bool is_primary_key() const { return kind == column_kind::PARTITION || kind == column_kind::CLUSTERING; }
    bool is_atomic() const { return !type->is_multi_cell(); }
    bool is_compact_value() const;
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
    std::vector<column_definition> _partition_key;
    std::vector<column_definition> _clustering_key;
    std::vector<column_definition> _regular_columns; // sorted by name
    std::vector<column_definition> _static_columns; // sorted by name, present only when there's any clustering column
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
    void build_columns(const std::vector<column>& columns, column_definition::column_kind kind, std::vector<column_definition>& dst);
    ::shared_ptr<cql3::column_specification> make_column_specification(column_definition& def);
public:
    gc_clock::duration default_time_to_live = gc_clock::duration::zero();
    const sstring ks_name;
    const sstring cf_name;
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
        std::vector<column> static_columns,
        shared_ptr<abstract_type> regular_column_name_type);
    bool is_dense() const {
        return false;
    }
    bool is_counter() const {
        return false;
    }
    column_definition* get_column_definition(const bytes& name);
    auto regular_begin() {
        return _regular_columns.begin();
    }
    auto regular_end() {
        return _regular_columns.end();
    }
    auto regular_lower_bound(const bytes& name) {
        // TODO: use regular_columns and a version of std::lower_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.lower_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return _regular_columns.begin() + i->second->id;
        }
    }
    auto regular_upper_bound(const bytes& name) {
        // TODO: use regular_columns and a version of std::upper_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.upper_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return _regular_columns.begin() + i->second->id;
        }
    }
    data_type column_name_type(column_definition& def) {
        return def.kind == column_definition::column_kind::REGULAR ? regular_column_name_type : utf8_type;
    }
    column_definition& regular_column_at(column_id id) {
        return _regular_columns[id];
    }
    bool is_last_partition_key(column_definition& def) {
        return &_partition_key[_partition_key.size() - 1] == &def;
    }
    bool has_static_columns() {
        return !_static_columns.empty();
    }
    size_t partition_key_size() const { return _partition_key.size(); }
    size_t clustering_key_size() const { return _clustering_key.size(); }
    size_t static_columns_count() const { return _static_columns.size(); }
    size_t regular_columns_count() const { return _regular_columns.size(); }
    // Returns a range of column definitions
    auto partition_key_columns() const {
        return boost::make_iterator_range(_partition_key.begin(), _partition_key.end());
    }
    // Returns a range of column definitions
    auto clustering_key_columns() const {
        return boost::make_iterator_range(_clustering_key.begin(), _clustering_key.end());
    }
    // Returns a range of column definitions
    auto static_columns() const {
        return boost::make_iterator_range(_static_columns.begin(), _static_columns.end());
    }
    // Returns a range of column definitions
    auto regular_columns() const {
        return boost::make_iterator_range(_regular_columns.begin(), _regular_columns.end());
    }
};

using schema_ptr = lw_shared_ptr<schema>;
