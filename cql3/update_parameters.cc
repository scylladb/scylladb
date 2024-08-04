/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/update_parameters.hh"
#include "cql3/selection/selection.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "query-result-reader.hh"
#include "types/map.hh"

namespace cql3 {

std::optional<std::vector<std::pair<data_value, data_value>>>
update_parameters::get_prefetched_list(const partition_key& pkey, const clustering_key& ckey, const column_definition& column) const {

    auto row = _prefetched.find_row(pkey, column.is_static() ? clustering_key::make_empty() : ckey);

    if (row == nullptr) {
        return std::nullopt;
    }

    auto pkey_bytes = pkey.explode();
    auto ckey_bytes = ckey.explode();

    auto val = expr::extract_column_value(&column, expr::evaluation_inputs{
        .partition_key = pkey_bytes,
        .clustering_key = ckey_bytes,
        .static_and_regular_columns = row->cells,
        .selection = _prefetched.selection.get(),
    });

    if (!val) {
        return std::nullopt;
    }

    auto type = column.type;
    // We use collections_as_maps flag, so set/list type is map, reconstruct the
    // data type used for serialization.
    if (type->is_listlike() && type->is_multi_cell()) {
        auto ctype = static_pointer_cast<const collection_type_impl>(type);
        type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
    }

    // Ensured by collections_as_maps flag in read_command flags
    SCYLLA_ASSERT(type->is_map());

    auto cell = type->deserialize(managed_bytes_view(*val));
    const map_type_impl& map_type = static_cast<const map_type_impl&>(*cell.type());
    return map_type.from_value(cell);
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
    schema_ptr _schema;
    std::optional<partition_key> _pkey;

public:
    prefetch_data_builder(schema_ptr s, update_parameters::prefetch_data& data)
        : _data(data)
        , _schema(std::move(s))
    { }

    void update_has_static(update_parameters::prefetch_data::row& cells) {
        cells.has_static = false;
        size_t idx = 0;
        for (auto* cdef : _data.selection->get_columns()) {
            if (cdef->is_regular()) {
                // no more static columns
                break;
            }
            if (cdef->is_static() && cells.cells[idx]) {
                cells.has_static = true;
                break;
            }

            ++idx;
        }
    }

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _pkey = key;
    }

    void accept_new_partition(uint64_t row_count) {
        SCYLLA_ASSERT(0);
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
                    const query::result_row_view& row) {
        update_parameters::prefetch_data::row cells;

        cells.cells = expr::get_non_pk_values(*_data.selection, static_row, &row);
        update_has_static(cells);

        _data.rows.emplace(std::make_pair(*_pkey, key), std::move(cells));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        SCYLLA_ASSERT(0);
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
        cells.cells = expr::get_non_pk_values(*_data.selection, static_row, nullptr);
        update_has_static(cells);

        // We end up here only if the table has a clustering key,
        // so no other row added for this partition thus has an
        // empty ckey.
        _data.rows.emplace(std::make_pair(*_pkey, clustering_key_prefix::make_empty()), std::move(cells));
    }
};

update_parameters::prefetch_data update_parameters::build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice) {

    update_parameters::prefetch_data rows(schema);
    rows.selection = selection::selection_from_partition_slice(schema, slice);
    query::result_view::consume(query_result, slice, prefetch_data_builder(schema, rows));
    return rows;
}

} // end of namespace cql3
