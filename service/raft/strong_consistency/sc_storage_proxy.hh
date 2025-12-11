/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/chunked_vector.hh"
#include "mutation/mutation.hh"

namespace service {

class raft_group_registry;

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
    raft_group_registry& _raft_groups;
    db::system_keyspace& _sys_ks;
public:
    sc_storage_proxy(raft_group_registry& raft_groups, db::system_keyspace& sys_ks);

    using mutatations_gen = noncopyable_function<utils::chunked_vector<mutation>(api::timestamp_type)>;
    future<sc_operation_result<>> mutate(const schema& schema, const dht::token& token, 
        mutatations_gen&& mutatations_gen);
};

}
