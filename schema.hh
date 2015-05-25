/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <unordered_map>
#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>
#include <boost/range/algorithm/transform.hpp>

#include "cql3/column_specification.hh"
#include "core/shared_ptr.hh"
#include "types.hh"
#include "compound.hh"
#include "gc_clock.hh"
#include "unimplemented.hh"
#include "utils/UUID.hh"

using column_id = uint32_t;

enum class column_kind { partition_key, clustering_key, regular_column, static_column,  };

class column_definition final {
public:
    template<typename ColumnRange>
    static std::vector<const column_definition*> vectorize(ColumnRange&& columns) {
        std::vector<const column_definition*> r;
        boost::transform(std::forward<ColumnRange>(columns), std::back_inserter(r), [] (auto& def) { return &def; });
        return r;
    }
private:
    bytes _name;
public:
    column_definition(bytes name, data_type type, column_id id, column_kind kind);

    data_type type;

    // Unique within (kind, schema instance).
    // schema::position() and component_index() depend on the fact that for PK columns this is
    // equivalent to component index.
    column_id id;

    column_kind kind;
    ::shared_ptr<cql3::column_specification> column_specification;

    bool is_static() const { return kind == column_kind::static_column; }
    bool is_regular() const { return kind == column_kind::regular_column; }
    bool is_partition_key() const { return kind == column_kind::partition_key; }
    bool is_clustering_key() const { return kind == column_kind::clustering_key; }
    bool is_primary_key() const { return kind == column_kind::partition_key || kind == column_kind::clustering_key; }
    bool is_atomic() const { return !type->is_multi_cell(); }
    bool is_compact_value() const;
    const sstring& name_as_text() const;
    const bytes& name() const;
    friend std::ostream& operator<<(std::ostream& os, const column_definition& cd) {
        return os << cd.name_as_text();
    }
    uint32_t component_index() const {
        assert(is_primary_key());
        return id;
    }
};

/*
 * Effectively immutable.
 * Not safe to access across cores because of shared_ptr's.
 */
class schema final {
private:
    // More complex fields are derived from these inside rebuild().
    // Contains only fields which can be safely default-copied.
    struct raw_schema {
        raw_schema(utils::UUID id);
        utils::UUID _id;
        sstring _ks_name;
        sstring _cf_name;
        std::vector<column_definition> _partition_key;
        std::vector<column_definition> _clustering_key;
        std::vector<column_definition> _regular_columns; // sorted by name
        std::vector<column_definition> _static_columns; // sorted by name, present only when there's any clustering column
        sstring _comment;
        gc_clock::duration _default_time_to_live = gc_clock::duration::zero();
        data_type _regular_column_name_type;
        double _bloom_filter_fp_chance = 0.01;
    };
    raw_schema _raw;
private:
    std::unordered_map<bytes, const column_definition*> _columns_by_name;
    std::map<bytes, const column_definition*, serialized_compare> _regular_columns_by_name;
    lw_shared_ptr<compound_type<allow_prefixes::no>> _partition_key_type;
    lw_shared_ptr<compound_type<allow_prefixes::no>> _clustering_key_type;
    lw_shared_ptr<compound_type<allow_prefixes::yes>> _clustering_key_prefix_type;
public:
    struct column {
        bytes name;
        data_type type;
        struct name_compare {
            data_type type;
            name_compare(data_type type) : type(type) {}
            bool operator()(const column& cd1, const column& cd2) const {
                return type->less(cd1.name, cd2.name);
            }
        };
    };
private:
    void build_columns(const std::vector<column>& columns, column_kind kind, std::vector<column_definition>& dst);
    ::shared_ptr<cql3::column_specification> make_column_specification(const column_definition& def);
    void rebuild();
public:
    schema(std::experimental::optional<utils::UUID> id,
        sstring ks_name,
        sstring cf_name,
        std::vector<column> partition_key,
        std::vector<column> clustering_key,
        std::vector<column> regular_columns,
        std::vector<column> static_columns,
        data_type regular_column_name_type,
        sstring comment = {});
    schema(const schema&);
    double bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    schema& set_bloom_filter_fp_chance(double fp) {
        _raw._bloom_filter_fp_chance = fp;
        return *this;
    }

    const utils::UUID& id() const {
        return _raw._id;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    void set_comment(const sstring& comment) {
        _raw._comment = comment;
    }
    void set_id(utils::UUID new_id) {
        _raw._id = new_id;
    }
    bool is_dense() const {
        return false;
    }
    bool is_counter() const {
        return false;
    }
    const column_definition* get_column_definition(const bytes& name) const;
    auto regular_begin() const {
        return _raw._regular_columns.begin();
    }
    auto regular_end() const {
        return _raw._regular_columns.end();
    }
    auto regular_lower_bound(const bytes& name) const {
        // TODO: use regular_columns and a version of std::lower_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.lower_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return _raw._regular_columns.begin() + i->second->id;
        }
    }
    auto regular_upper_bound(const bytes& name) const {
        // TODO: use regular_columns and a version of std::upper_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.upper_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return _raw._regular_columns.begin() + i->second->id;
        }
    }
    data_type column_name_type(const column_definition& def) const {
        return def.kind == column_kind::regular_column ? _raw._regular_column_name_type : utf8_type;
    }
    const column_definition& regular_column_at(column_id id) const {
        return _raw._regular_columns.at(id);
    }
    const column_definition& static_column_at(column_id id) const {
        return _raw._static_columns.at(id);
    }
    bool is_last_partition_key(const column_definition& def) const {
        return &_raw._partition_key[_raw._partition_key.size() - 1] == &def;
    }
    bool has_collections() const ;
    bool has_static_columns() const {
        return !_raw._static_columns.empty();
    }
    size_t partition_key_size() const { return _raw._partition_key.size(); }
    size_t clustering_key_size() const { return _raw._clustering_key.size(); }
    size_t static_columns_count() const { return _raw._static_columns.size(); }
    size_t regular_columns_count() const { return _raw._regular_columns.size(); }
    // Returns a range of column definitions
    auto partition_key_columns() const {
        return boost::make_iterator_range(_raw._partition_key.begin(), _raw._partition_key.end());
    }
    // Returns a range of column definitions
    auto clustering_key_columns() const {
        return boost::make_iterator_range(_raw._clustering_key.begin(), _raw._clustering_key.end());
    }
    // Returns a range of column definitions
    auto static_columns() const {
        return boost::make_iterator_range(_raw._static_columns.begin(), _raw._static_columns.end());
    }
    // Returns a range of column definitions
    auto regular_columns() const {
        return boost::make_iterator_range(_raw._regular_columns.begin(), _raw._regular_columns.end());
    }
    // Returns a range of column definitions
    auto all_columns_in_select_order() const {
        return boost::range::join(partition_key_columns(),
            boost::range::join(clustering_key_columns(),
            boost::range::join(static_columns(), regular_columns())));
    }
    uint32_t position(const column_definition& column) const {
        if (column.is_primary_key()) {
            return column.id;
        }
        return _raw._clustering_key.size();
    }
    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }
    const sstring& ks_name() const {
        return _raw._ks_name;
    }
    const sstring& cf_name() const {
        return _raw._cf_name;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::no>>& partition_key_type() const {
        return _partition_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::no>>& clustering_key_type() const {
        return _clustering_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::yes>>& clustering_key_prefix_type() const {
        return _clustering_key_prefix_type;
    }
    const data_type& regular_column_name_type() const {
        return _raw._regular_column_name_type;
    }
};

using schema_ptr = lw_shared_ptr<const schema>;

utils::UUID generate_legacy_id(const sstring& ks_name, const sstring& cf_name);
