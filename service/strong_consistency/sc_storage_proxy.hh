/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation/mutation.hh"
#include "query/query-result.hh"

namespace service {

class sc_groups_manager;

template <typename T = void>
class sc_operation_result {
    using stored_type = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    std::variant<locator::tablet_replica, stored_type> _val;

    sc_operation_result(std::variant<locator::tablet_replica, stored_type> val)
        : _val(std::move(val))
    {
    }
public:
    static sc_operation_result<T> redirect(locator::tablet_replica target) {
        return {target};
    }

    template <typename U = T>
    static std::enable_if_t<!std::is_void_v<U>, sc_operation_result<T>> result(stored_type val) {
        return {std::move(val)};
    }

    template <typename U = T>
    static std::enable_if_t<std::is_void_v<U>, sc_operation_result<T>> result() {
        return {std::monostate{}};
    }

    const locator::tablet_replica* get_if_redirect() const {
        return std::get_if<locator::tablet_replica>(&_val);
    }

    template <typename U = T>
    std::enable_if_t<!std::is_void_v<U>, U>&& extract_result() && {
        return std::move(std::get<U>(_val));
    }
};

class sc_storage_proxy: public peering_sharded_service<sc_storage_proxy> {
    sc_groups_manager& _sc_groups;
    replica::database& _db;
public:
    sc_storage_proxy(sc_groups_manager& sc_groups, replica::database& db);

    using mutatation_gen = noncopyable_function<mutation(api::timestamp_type)>;
    future<sc_operation_result<>> mutate(schema_ptr schema, 
        const dht::token& token,
        mutatation_gen&& mutatation_gen);

    using query_result_type = sc_operation_result<lw_shared_ptr<query::result>>;
    future<query_result_type> query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout);
};

}
