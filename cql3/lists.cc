/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lists.hh"
#include "update_parameters.hh"
#include "column_identifier.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include <boost/iterator/transform_iterator.hpp>
#include "types/list.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "mutation/mutation.hh"

namespace cql3 {

lw_shared_ptr<column_specification>
lists::index_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("idx({})", *column.name), true), int32_type);
}

lw_shared_ptr<column_specification>
lists::uuid_index_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("uuid_idx({})", *column.name), true), uuid_type);
}

void
lists::setter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    auto value = expr::evaluate(*_e, params._options);
    execute(m, prefix, params, column, std::move(value));
}

void
lists::setter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const cql3::raw_value& value) {
    if (column.type->is_multi_cell()) {
        // Delete all cells first, then append new ones
        collection_mutation_view_description mut;
        mut.tomb = params.make_tombstone_just_before();

        m.set_cell(prefix, column, mut.serialize(*column.type));
    }
    do_append(value, m, prefix, column, params);
}

bool
lists::setter_by_index::requires_read() const {
    return true;
}

void
lists::setter_by_index::fill_prepare_context(prepare_context& ctx) {
    operation::fill_prepare_context(ctx);
    expr::fill_prepare_context(_idx, ctx);
}

void
lists::setter_by_index::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    // we should not get here for frozen lists
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to set an individual element on a frozen list";

    auto index = expr::evaluate(_idx, params._options);
    if (index.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }
    auto value = expr::evaluate(*_e, params._options);

    auto idx = index.view().deserialize<int32_t>(*int32_type);
    auto&& existing_list_opt = params.get_prefetched_list(m.key(), prefix, column);
    if (!existing_list_opt) {
        throw exceptions::invalid_request_exception("Attempted to set an element on a list which is null");
    }
    auto&& existing_list = *existing_list_opt;
    // we verified that index is an int32_type
    if (idx < 0 || size_t(idx) >= existing_list.size()) {
        throw exceptions::invalid_request_exception(format("List index {:d} out of bound, list has size {:d}",
                idx, existing_list.size()));
    }

    auto ltype = static_cast<const list_type_impl*>(column.type.get());
    const data_value& eidx_dv = existing_list[idx].first;
    bytes eidx = eidx_dv.type()->decompose(eidx_dv);
    collection_mutation_description mut;
    mut.cells.reserve(1);
    if (value.is_null()) {
        mut.cells.emplace_back(std::move(eidx), params.make_dead_cell());
    } else {
        mut.cells.emplace_back(std::move(eidx),
                params.make_cell(*ltype->value_comparator(), value.view(), atomic_cell::collection_member::yes));
    }

    m.set_cell(prefix, column, mut.serialize(*ltype));
}

bool
lists::setter_by_uuid::requires_read() const {
    return false;
}

void
lists::setter_by_uuid::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    // we should not get here for frozen lists
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to set an individual element on a frozen list";

    auto index = expr::evaluate(_idx, params._options);
    auto value = expr::evaluate(*_e, params._options);

    if (index.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }

    auto ltype = static_cast<const list_type_impl*>(column.type.get());

    collection_mutation_description mut;
    mut.cells.reserve(1);

    if (value.is_null()) {
        mut.cells.emplace_back(std::move(index).to_bytes(), params.make_dead_cell());
    } else {
        mut.cells.emplace_back(
                    std::move(index).to_bytes(),
                    params.make_cell(*ltype->value_comparator(), value.view(), atomic_cell::collection_member::yes));
    }

    m.set_cell(prefix, column, mut.serialize(*ltype));
}

void
lists::appender::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    const cql3::raw_value value = expr::evaluate(*_e, params._options);
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to append to a frozen list";
    do_append(value, m, prefix, column, params);
}

void
lists::do_append(const cql3::raw_value& list_value,
        mutation& m,
        const clustering_key_prefix& prefix,
        const column_definition& column,
        const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        // If we append null, do nothing. Note that for Setter, we've
        // already removed the previous value so we're good here too
        if (list_value.is_null()) {
            return;
        }

        auto ltype = static_cast<const list_type_impl*>(column.type.get());

        auto&& to_add = expr::get_list_elements(list_value);
        collection_mutation_description appended;
        appended.cells.reserve(to_add.size());
        for (auto&& e : to_add) {
            try {
                auto uuid1 = utils::UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(
                    std::chrono::microseconds{params.timestamp()},
                    params._options.next_list_append_seq());
                auto uuid = bytes(reinterpret_cast<const int8_t*>(uuid1.data()), uuid1.size());
                if (!e) {
                    throw exceptions::invalid_request_exception("Invalid NULL element in list");
                }
                // FIXME: can e be empty?
                appended.cells.emplace_back(
                    std::move(uuid),
                    params.make_cell(*ltype->value_comparator(), *e, atomic_cell::collection_member::yes));
            } catch (utils::timeuuid_submicro_out_of_range&) {
                throw exceptions::invalid_request_exception("Too many list values per single CQL statement or batch");
            }
        }
        m.set_cell(prefix, column, appended.serialize(*ltype));
    } else {
        auto ltype = static_cast<const list_type_impl*>(column.type.get());
        // for frozen lists, we're overwriting the whole cell value
        if (list_value.is_null()) {
            m.set_cell(prefix, column, params.make_dead_cell());
        } else {
            list_value.view().with_value([&] (const FragmentedView auto& v) {
                ltype->validate_for_storage(v);
            });
            m.set_cell(prefix, column, params.make_cell(*column.type, list_value.view()));
        }
    }
}

void
lists::prepender::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to prepend to a frozen list";
    cql3::raw_value lvalue = expr::evaluate(*_e, params._options);
    if (lvalue.is_null()) {
        return;
    }

    // For prepend we need to be able to generate a unique but decreasing
    // timeuuid. We achieve that by by using a time in the past which
    // is 2x the distance between the original timestamp (it
    // would be the current timestamp, user supplied timestamp, or
    // unique monotonic LWT timestsamp, whatever is in query
    // options) and a reference time of Jan 1 2010 00:00:00.
    // E.g. if query timestamp is Jan 1 2020 00:00:00, the prepend
    // timestamp will be Jan 1, 2000, 00:00:00.

    // 2010-01-01T00:00:00+00:00 in api::timestamp_time format (microseconds)
    static constexpr int64_t REFERENCE_TIME_MICROS = 1262304000L * 1000 * 1000;

    int64_t micros = params.timestamp();
    if (micros > REFERENCE_TIME_MICROS) {
        micros = REFERENCE_TIME_MICROS - (micros - REFERENCE_TIME_MICROS);
    } else {
        // Scylla, unlike Cassandra, respects user-supplied timestamps
        // in prepend, but there is nothing useful it can do with
        // a timestamp less than Jan 1, 2010, 00:00:00.
        throw exceptions::invalid_request_exception("List prepend custom timestamp must be greater than Jan 1 2010 00:00:00");
    }

    collection_mutation_description mut;
    utils::chunked_vector<managed_bytes_opt> list_elements = expr::get_list_elements(lvalue);
    mut.cells.reserve(list_elements.size());

    auto ltype = static_cast<const list_type_impl*>(column.type.get());
    int clockseq = params._options.next_list_prepend_seq(list_elements.size(), utils::UUID_gen::SUBMICRO_LIMIT);
    for (auto&& v : list_elements) {
        try {
            auto uuid = utils::UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(std::chrono::microseconds{micros}, clockseq++);
            if (!v) {
                throw exceptions::invalid_request_exception("Invalid NULL element in list");
            }
            mut.cells.emplace_back(bytes(uuid.data(), uuid.size()), params.make_cell(*ltype->value_comparator(), *v, atomic_cell::collection_member::yes));
        } catch (utils::timeuuid_submicro_out_of_range&) {
            throw exceptions::invalid_request_exception("Too many list values per single CQL statement or batch");
        }
    }
    m.set_cell(prefix, column, mut.serialize(*ltype));
}

bool
lists::discarder::requires_read() const {
    return true;
}

void
lists::discarder::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to delete from a frozen list";

    auto&& existing_list = params.get_prefetched_list(m.key(), prefix, column);
    // We want to call bind before possibly returning to reject queries where the value provided is not a list.
    cql3::raw_value lvalue = expr::evaluate(*_e, params._options);

    if (!existing_list) {
        return;
    }

    auto&& elist = *existing_list;

    if (elist.empty()) {
        return;
    }

    if (lvalue.is_null()) {
        return;
    }

    auto ltype = static_cast<const list_type_impl*>(column.type.get());

    // Note: below, we will call 'contains' on this toDiscard list for each element of existingList.
    // Meaning that if toDiscard is big, converting it to a HashSet might be more efficient. However,
    // the read-before-write this operation requires limits its usefulness on big lists, so in practice
    // toDiscard will be small and keeping a list will be more efficient.
    auto&& to_discard = expr::get_list_elements(lvalue);
    collection_mutation_description mnew;
    auto ensure = [] (const managed_bytes_opt& v) {
        if (!v) {
            // Note: for discarder operation, we might just ignore NULLs
            throw exceptions::invalid_request_exception("Invalid NULL value in list");
        }
    };
    for (auto&& cell : elist) {
        auto has_value = [&] (bytes_view value) {
            return std::find_if(to_discard.begin(), to_discard.end(),
                                [ltype, value, &ensure] (auto&& v) { ensure(v); return ltype->get_elements_type()->equal(*v, value); })
                                         != to_discard.end();
        };
        bytes eidx = cell.first.type()->decompose(cell.first);
        bytes value = cell.second.type()->decompose(cell.second);
        if (has_value(value)) {
            mnew.cells.emplace_back(std::move(eidx), params.make_dead_cell());
        }
    }
    m.set_cell(prefix, column, mnew.serialize(*ltype));
}

bool
lists::discarder_by_index::requires_read() const {
    return true;
}

void
lists::discarder_by_index::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to delete an item by index from a frozen list";
    cql3::raw_value index = expr::evaluate(*_e, params._options);
    if (index.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }

    auto&& existing_list_opt = params.get_prefetched_list(m.key(), prefix, column);
    int32_t idx = index.view().deserialize<int32_t>(*int32_type);

    if (!existing_list_opt) {
        throw exceptions::invalid_request_exception("Attempted to delete an element from a list which is null");
    }
    auto&& existing_list = *existing_list_opt;
    if (idx < 0 || size_t(idx) >= existing_list.size()) {
        throw exceptions::invalid_request_exception(format("List index {:d} out of bound, list has size {:d}", idx, existing_list.size()));
    }
    collection_mutation_description mut;
    const data_value& eidx_dv = existing_list[idx].first;
    bytes eidx = eidx_dv.type()->decompose(eidx_dv);
    mut.cells.emplace_back(std::move(eidx), params.make_dead_cell());
    m.set_cell(prefix, column, mut.serialize(*column.type));
}

}
