/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/adaptors.hpp>

#include <seastar/core/coroutine.hh>

#include "gms/endpoint_state_map.hh"
#include "utils/stall_free.hh"

namespace gms {

future<> endpoint_state_map::clear_gently() {
    co_await utils::clear_gently(_state_map);
}

std::vector<inet_address> endpoint_state_map::get_endpoints() const {
    return boost::copy_range<std::vector<inet_address>>(_state_map | boost::adaptors::map_keys);
}

endpoint_state_ptr endpoint_state_map::get_endpoint_state_ptr(const inet_address& addr) const noexcept {
    if (auto it = _state_map.find(addr); it != _state_map.end()) {
        return it->second;
    }
    return nullptr;
}

stop_iteration endpoint_state_map::for_each_endpoint_state_until(std::function<stop_iteration(const inet_address&, const endpoint_state&)> func) const {
    for (const auto& [node, eps] : _state_map) {
        if (func(node, *eps) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

bool endpoint_state_map::insert(const inet_address& addr, endpoint_state_ptr eps) {
    auto [it, inserted] = _state_map.try_emplace(addr, eps);
    if (!inserted) {
        it->second = std::move(eps);
    }
    return inserted;
}

bool endpoint_state_map::erase(const inet_address& addr) {
    return _state_map.erase(addr);
}

} // namespace gms
