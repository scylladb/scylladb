/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "mutation/mutation.hh"
#include "modification_statement.hh"
#include "cas_request.hh"
#include <seastar/core/sleep.hh>
#include "cql3/result_set.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "transport/messages/result_message.hh"
#include "types/map.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"

namespace cql3::statements {

using namespace std::chrono;

void cas_request::add_row_update(const modification_statement& stmt_arg,
        std::vector<query::clustering_range> ranges_arg,
        modification_statement::json_cache_opt json_cache_arg,
        const query_options& options_arg) {
    // TODO: reserve updates array for batches
    _updates.emplace_back(cas_row_update{
        .statement = stmt_arg,
        .ranges = std::move(ranges_arg),
        .json_cache = std::move(json_cache_arg),
        .options = options_arg});
}

std::optional<mutation> cas_request::apply_updates(api::timestamp_type ts) const {
    // We're working with a single partition, so there will be only one element
    // in the vector. A vector is used since this is a conventional format
    // to pass a mutation onward.
    std::optional<mutation> mutation_set;
    for (const cas_row_update& op: _updates) {
        update_parameters params(_schema, op.options, ts, op.statement.get_time_to_live(op.options), _rows);

        std::vector<mutation> statement_mutations = op.statement.apply_updates(_key, op.ranges, params, op.json_cache);
        // Append all mutations (in fact only one) to the consolidated one.
        for (mutation& m : statement_mutations) {
            if (mutation_set.has_value() == false) {
                mutation_set.emplace(std::move(m));
            } else {
                mutation_set->apply(std::move(m));
            }
        }
    }

    return mutation_set;
}

lw_shared_ptr<query::read_command> cas_request::read_command(query_processor& qp) const {

    column_set columns_to_read(_schema->all_columns_count());
    std::vector<query::clustering_range> ranges;

    for (const cas_row_update& op : _updates) {
        if (op.statement.has_conditions() == false && op.statement.requires_read() == false) {
            // No point in pre-fetching the old row if the statement doesn't check it in a CAS and
            // doesn't use it to apply updates.
            continue;
        }
        columns_to_read.union_with(op.statement.columns_to_read());
        if (op.statement.has_only_static_column_conditions() && !op.statement.requires_read()) {
            // If a statement has only static column conditions and doesn't have operations that
            // require read, it doesn't matter what clustering key range to query - any partition
            // row will do for the check.
            continue;
        }
        ranges.reserve(op.ranges.size());
        std::copy(op.ranges.begin(), op.ranges.end(), std::back_inserter(ranges));
    }
    uint64_t max_rows = query::partition_max_rows;
    if (ranges.empty()) {
        // With only a static condition, we still want to make the distinction between
        // a non-existing partition and one that exists (has some live data) but has not
        // static content. So we query the first live row of the partition.
        ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
        max_rows = 1;
    } else {
        ranges = query::clustering_range::deoverlap(std::move(ranges), clustering_key::tri_compare(*_schema));
    }
    auto options = update_parameters::options;
    options.set(query::partition_slice::option::always_return_static_content);
    query::partition_slice ps(std::move(ranges), *_schema, columns_to_read, options);
    ps.set_partition_row_limit(max_rows);
    return make_lw_shared<query::read_command>(_schema->id(), _schema->version(), std::move(ps), qp.proxy().get_max_result_size(ps),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()));
}

bool cas_request::applies_to() const {
    for (const cas_row_update& op: _updates) {
        if (!op.statement.has_conditions()) {
            continue;
        }
        // No need to check subsequent conditions as we have already failed the current one.
        auto old_row = find_old_row(op).row;
        if (!op.statement.applies_to(_rows.selection.get(), old_row, op.options)) {
            return false;
        }
    }
    return true;
}

std::optional<mutation> cas_request::apply(foreign_ptr<lw_shared_ptr<query::result>> qr,
        const query::partition_slice& slice, api::timestamp_type ts) {
    _rows = update_parameters::build_prefetch_data(_schema, *qr, slice);
    if (applies_to()) {
        return apply_updates(ts);
    } else {
        return {};
    }
}

cas_request::old_row cas_request::find_old_row(const cas_row_update& op) const {
    static const clustering_key empty_ckey = clustering_key::make_empty();
    if (_key.empty()) {
        throw exceptions::invalid_request_exception("Empty partition key range");
    }
    const partition_key& pkey = _key.front().start()->value().key().value();
    // We must ignore statement clustering column restriction when
    // choosing a row to check the conditions. If there is no
    // exact match, choose static row to check if the statement
    // applies.
    // For example, the following update must successfully apply (effectively
    // turn into INSERT), because, although the table doesn't have any regular rows matching the
    // statement clustering column restriction, the static row matches the statement condition:
    //   CREATE TABLE t(p int, c int, s int static, v int, PRIMARY KEY(p, c));
    //   INSERT INTO t(p, s) VALUES(1, 1);
    //   UPDATE t SET v=1 WHERE p=1 AND c=1 IF s=1;
    if (op.ranges.empty()) {
        throw exceptions::invalid_request_exception("Empty clustering range");
    }
    const clustering_key& ckey = op.ranges.front().start() ?  op.ranges.front().start()->value() : empty_ckey;
    auto row = _rows.find_row(pkey, ckey);
    auto ckey_ptr = &ckey;
    if (row == nullptr && !ckey.is_empty() &&
        !op.statement.has_if_exist_condition() && !op.statement.has_if_not_exist_condition()) {
        row = _rows.find_row(pkey, empty_ckey);
        ckey_ptr = &empty_ckey;
    }
    if (!row) {
        ckey_ptr = &empty_ckey;
    }
    return old_row{ckey_ptr, row};
}

seastar::shared_ptr<cql_transport::messages::result_message>
cas_request::build_cas_result_set(seastar::shared_ptr<cql3::metadata> metadata,
                                  const column_set& columns,
                                  bool is_applied) const {
    const partition_key& pkey = _key.front().start()->value().key().value();
    const clustering_key empty_ckey = clustering_key::make_empty();
    auto result_set = std::make_unique<cql3::result_set>(metadata);

    auto pkey_bytes = pkey.explode();

    for (const cas_row_update& op: _updates) {
        // Construct the result set row
        std::vector<bytes_opt> rs_row;
        rs_row.reserve(metadata->value_count());
        rs_row.emplace_back(boolean_type->decompose(is_applied));
        // Get old row from prefetched data for the row update
        auto old_row = find_old_row(op);
        if (!old_row.row) {
            // In case there is no old row, leave all other columns null
            // so that we can infer whether the update attempts to insert a
            // non-existing row.
            rs_row.resize(metadata->value_count());
            result_set->add_row(std::move(rs_row));
            continue;
        }
        auto ckey_bytes = old_row.ckey->explode();

        auto eval_inputs = expr::evaluation_inputs{
            .partition_key = pkey_bytes,
            .clustering_key = ckey_bytes,
            .static_and_regular_columns = old_row.row->cells,
            .selection = _rows.selection.get(),
        };

        // Fill in the cells from prefetch data (old row) into the result set row
        for (ordinal_column_id id = columns.find_first(); id != column_set::npos; id = columns.find_next(id)) {
            auto& cdef = _schema->column_at(id);
            auto val = expr::extract_column_value(&cdef, eval_inputs);
            if (!val) {
                rs_row.emplace_back(bytes_opt{});
                continue;
            }
            const abstract_type& column_type = *cdef.type;

            if (column_type.is_listlike() && column_type.is_multi_cell()) {
                // List/sets are fetched as maps, but need to be stored as sets.
                const listlike_collection_type_impl& list_type = static_cast<const listlike_collection_type_impl&>(column_type);
                auto map_type_holder = map_type_impl::get_instance(list_type.name_comparator(), list_type.value_comparator(), list_type.is_multi_cell());
                const map_type_impl& map_type = static_cast<const map_type_impl&>(*map_type_holder);
                auto cell = map_type.deserialize(managed_bytes_view(*val));
                rs_row.emplace_back(list_type.serialize_map(map_type, cell));
            } else {
                rs_row.emplace_back(to_bytes(*val));
            }
        }
        result_set->add_row(std::move(rs_row));
    }
    cql3::result result(std::move(result_set));
    return seastar::make_shared<cql_transport::messages::result_message::rows>(std::move(result));
}

} // end of namespace "cql3::statements"
