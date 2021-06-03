/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mutation.hh"
#include "cql3/cql3_type.hh"
#include "schema.hh"

namespace tests::data_model {

static constexpr api::timestamp_type previously_removed_column_timestamp = 100;
static constexpr api::timestamp_type data_timestamp = 200;
static constexpr api::timestamp_type column_removal_timestamp = 300;

class mutation_description {
public:
    struct expiry_info {
        gc_clock::duration ttl;
        gc_clock::time_point expiry_point;
    };
    struct row_marker {
        api::timestamp_type timestamp;
        std::optional<expiry_info> expiring;

        row_marker(api::timestamp_type timestamp = data_timestamp);
        row_marker(api::timestamp_type timestamp, gc_clock::duration ttl, gc_clock::time_point expiry_point);
    };
    using key = std::vector<bytes>;
    struct atomic_value {
        bytes value;
        api::timestamp_type timestamp;
        std::optional<expiry_info> expiring;

        atomic_value(bytes value, api::timestamp_type timestamp = data_timestamp);
        atomic_value(bytes value, api::timestamp_type timestamp, gc_clock::duration ttl, gc_clock::time_point expiry_point);
    };
    struct collection_element {
        bytes key;
        atomic_value value;
    };
    struct collection {
        tombstone tomb;
        std::vector<collection_element> elements;

        collection() = default;
        collection(std::initializer_list<collection_element> elements);
        collection(std::vector<collection_element> elements);
    };
    using value = std::variant<atomic_value, collection>;
    struct cell {
        sstring column_name;
        value data_value;
    };
    using row = std::vector<cell>;
    struct clustered_row {
        row_marker marker;
        row_tombstone tomb;
        row cells;
    };
    struct range_tombstone {
        nonwrapping_range<key> range;
        tombstone tomb;
    };

private:
    key _partition_key;
    tombstone _partition_tombstone;
    row _static_row;
    std::map<key, clustered_row> _clustered_rows;
    std::vector<range_tombstone> _range_tombstones;

private:
    static void remove_column(row& r, const sstring& name);

public:
    explicit mutation_description(key partition_key);

    void set_partition_tombstone(tombstone partition_tombstone);

    void add_static_cell(const sstring& column, value v);
    void add_clustered_cell(const key& ck, const sstring& column, value v);
    void add_clustered_row_marker(const key& ck, row_marker marker = row_marker(data_timestamp));
    void add_clustered_row_tombstone(const key& ck, row_tombstone tomb);

    void remove_static_column(const sstring& name);
    void remove_regular_column(const sstring& name);

    // Both overloads accept out-of-order ranges and will make sure the
    // range-tombstone is created with start <= end.
    void add_range_tombstone(const key& start, const key& end,
            tombstone tomb = tombstone(previously_removed_column_timestamp, gc_clock::time_point()));
    void add_range_tombstone(nonwrapping_range<key> range,
            tombstone tomb = tombstone(previously_removed_column_timestamp, gc_clock::time_point()));

    mutation build(schema_ptr s) const;
};

class table_description {
public:
    using column = std::tuple<sstring, data_type>;
    struct removed_column {
        sstring name;
        data_type type;
        api::timestamp_type removal_timestamp;
    };

private:
    std::vector<column> _partition_key;
    std::vector<column> _clustering_key;
    std::vector<column> _static_columns;
    std::vector<column> _regular_columns;

    std::vector<removed_column> _removed_columns;

    std::vector<mutation_description> _mutations;

    std::vector<sstring> _change_log;

private:
    static std::vector<column>::iterator find_column(std::vector<column>& columns, const sstring& name);
    static void add_column(std::vector<column>& columns, const sstring& name, data_type type);
    void add_old_column(const sstring& name, data_type type);
    void remove_column(std::vector<column>& columns, const sstring& name);
    static void alter_column_type(std::vector<column>& columns, const sstring& name, data_type new_type);

    schema_ptr build_schema() const;

    std::vector<mutation> build_mutations(schema_ptr s) const;
public:
    explicit table_description(std::vector<column> partition_key, std::vector<column> clustering_key);

    void add_static_column(const sstring& name, data_type type);
    void add_regular_column(const sstring& name, data_type type);

    void add_old_static_column(const sstring& name, data_type type);
    void add_old_regular_column(const sstring& name, data_type type);

    void remove_static_column(const sstring& name);
    void remove_regular_column(const sstring& name);

    void alter_partition_column_type(const sstring& name, data_type new_type);
    void alter_clustering_column_type(const sstring& name, data_type new_type);

    void alter_static_column_type(const sstring& name, data_type new_type);
    void alter_regular_column_type(const sstring& name, data_type new_type);

    void rename_partition_column(const sstring& from, const sstring& to);
    void rename_clustering_column(const sstring& from, const sstring& to);

    std::vector<mutation_description>& unordered_mutations() { return _mutations; }
    const std::vector<mutation_description>& unordered_mutations() const { return _mutations; }

    struct table {
        sstring schema_changes_log;
        schema_ptr schema;
        std::vector<mutation> mutations;
    };
    table build() const;
};

}
