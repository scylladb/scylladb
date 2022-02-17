/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/update_parameters.hh"
#include "query-result-reader.hh"
#include "types/map.hh"

namespace cql3 {

const std::vector<std::pair<data_value, data_value>> *
update_parameters::get_prefetched_list(const partition_key& pkey, const clustering_key& ckey, const column_definition& column) const {

    auto row = _prefetched.find_row(pkey, column.is_static() ? clustering_key::make_empty() : ckey);

    if (row == nullptr) {
        return nullptr;
    }

    auto j = row->cells.find(column.ordinal_id);
    if (j == row->cells.end()) {
        return nullptr;
    }
    const data_value& cell = j->second;
    // Ensured by collections_as_maps flag in read_command flags
    assert(cell.type()->is_map());
    const map_type_impl& map_type = static_cast<const map_type_impl&>(*cell.type());
    return &map_type.from_value(cell);
}

update_parameters::prefetch_data::prefetch_data(schema_ptr schema)
    : rows(key_less{*schema})
    , schema(schema)
{ }


const update_parameters::prefetch_data::row*
update_parameters::prefetch_data::find_row(const partition_key& pkey, const clustering_key& ckey) const {

    const auto it = rows.find({pkey, ckey});
    return it == rows.end() ? nullptr : &it->second;
}

// Implements ResultVisitor concept from query.hh
class prefetch_data_builder {
    update_parameters::prefetch_data& _data;
    const query::partition_slice& _ps;
    schema_ptr _schema;
    std::optional<partition_key> _pkey;

    // Add partition key columns to the current full row
    void add_partition_key(update_parameters::prefetch_data::row& cells, const partition_key& key)
    {
        auto i = key.begin(*_schema);
        for (auto&& col : _schema->partition_key_columns()) {
            cells.cells.emplace(col.ordinal_id, col.type->deserialize_value(*i));
            ++i;
        }
    }

    // Add clustering key columns to the current full row
    void add_clustering_key(update_parameters::prefetch_data::row& cells, const clustering_key& key)
    {
        auto i = key.begin(*_schema);
        for (auto&& col : _schema->clustering_key_columns()) {
            if (i == key.end(*_schema)) {
                break;
            }
            cells.cells.emplace(col.ordinal_id, col.type->deserialize_value(*i));
            ++i;
        }
    }

    // Add a prefetched cell to the current full row
    void add_cell(update_parameters::prefetch_data::row& cells, const column_definition& def,
            const std::optional<query::result_bytes_view>& cell) {

        if (cell == std::nullopt) {
            return;
        }
        auto type = def.type;
        // We use collections_as_maps flag, so set/list type is map, reconstruct the
        // data type used for serialization.
        if (type->is_listlike() && type->is_multi_cell()) {
            auto ctype = static_pointer_cast<const collection_type_impl>(type);
            type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        }
        cells.cells.emplace(def.ordinal_id, type->deserialize(*cell));
    };
public:
    prefetch_data_builder(schema_ptr s, update_parameters::prefetch_data& data, const query::partition_slice& ps)
        : _data(data)
        , _ps(ps)
        , _schema(std::move(s))
    { }

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _pkey = key;
    }

    void accept_new_partition(uint64_t row_count) {
        assert(0);
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
                    const query::result_row_view& row) {
        update_parameters::prefetch_data::row cells;

        add_partition_key(cells, *_pkey);
        add_clustering_key(cells, key);
        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }
        auto row_iterator = row.iterator();
        for (auto&& id : _ps.regular_columns) {
            add_cell(cells, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }

        _data.rows.emplace(std::make_pair(*_pkey, key), std::move(cells));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(0);
    }

    void accept_partition_end(const query::result_row_view& static_row) {

        if (!_schema->clustering_key_size() || !_schema->has_static_columns()) {
            // Do not add an (empty) static row if there are no
            // clustering key columns or not static cells, such
            // row will have no non-null cells in it, so will
            // be useless.
            return;
        }
        // When no clustering row matches WHERE condition of
        // UPSERT-like operation (INSERT, UPDATE)
        // the static row will be used to materialize the initial
        // clustering row.

        update_parameters::prefetch_data::row cells;
        add_partition_key(cells, *_pkey);

        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }
        // We end up here only if the table has a clustering key,
        // so no other row added for this partition thus has an
        // empty ckey.
        _data.rows.emplace(std::make_pair(*_pkey, clustering_key_prefix::make_empty()), std::move(cells));
    }
};

update_parameters::prefetch_data update_parameters::build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice) {

    update_parameters::prefetch_data rows(schema);
    query::result_view::consume(query_result, slice, prefetch_data_builder(schema, rows, slice));
    return rows;
}

} // end of namespace cql3
