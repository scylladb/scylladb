/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/adaptors.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/coroutine.hh>

#include "gms/endpoint_state_map.hh"
#include "gms/inet_address.hh"
#include "locator/host_id.hh"
#include "seastar/net/api.hh"
#include "utils/stall_free.hh"
#include "log.hh"

namespace gms {

logging::logger log("endpoint_state_map");

future<> endpoint_state_map::clear_gently() {
    co_await utils::clear_gently(_state_map);
}

bool endpoint_state_map::contains(const inet_address& addr) const noexcept {
    for (const auto& [ep, eps] : _state_map) {
        if (eps->get_address() == addr) {
            return true;
        }
    }
    return false;
}

inet_address endpoint_state_map::get_endpoint_address(const endpoint_type& endpoint) const noexcept {
    if (auto it = _state_map.find(endpoint); it != _state_map.end()) {
        return it->second->get_address();
    }
    return inet_address{};
}

endpoint_state_map::endpoint_type endpoint_state_map::get_endpoint_by_address(const inet_address& addr) const noexcept {
    for (auto it = _state_map.begin(); it != _state_map.end(); ++it) {
        if (it->second->get_address() == addr) {
            return it->first;
        }
    }
    return endpoint_type{};
}

std::vector<endpoint_state_map::endpoint_type> endpoint_state_map::get_endpoints() const {
    return boost::copy_range<std::vector<endpoint_type>>(_state_map | boost::adaptors::map_keys);
}

std::unordered_set<inet_address> endpoint_state_map::get_endpoint_addresses() const {
    return boost::copy_range<std::unordered_set<inet_address>>(_state_map | boost::adaptors::transformed([] (const auto& x) {
        return x.second->get_address();
    }));
}

endpoint_state_ptr endpoint_state_map::get_ptr(const endpoint_type& endpoint) const noexcept {
    if (auto it = _state_map.find(endpoint); it != _state_map.end()) {
        return it->second;
    }
    return nullptr;
}

endpoint_state_ptr endpoint_state_map::get_ptr(const inet_address& addr) const noexcept {
    for (auto it = _state_map.begin(); it != _state_map.end(); ++it) {
        if (it->second->get_address() == addr) {
            return it->second;
        }
    }
    return nullptr;
}

stop_iteration endpoint_state_map::for_each_until(std::function<stop_iteration(const locator::host_id&, const endpoint_state&)> func) const {
    for (const auto& [endpoint, eps] : _state_map) {
        if (func(endpoint, *eps) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

bool endpoint_state_map::insert(const endpoint_type& endpoint, endpoint_state_ptr eps) {
    if (!endpoint) {
        on_internal_error(log, fmt::format("Tried inserting node {} with null host_id", eps->get_address()));
    }
    if (eps->get_address() == inet_address{}) {
        on_internal_error(log, fmt::format("Tried inserting node {} with null address", endpoint));
    }
    auto [it, inserted] = _state_map.try_emplace(endpoint, eps);
    if (inserted) {
        return true;
    } else {
        it->second = eps;
        return false;
    }
}

bool endpoint_state_map::erase(const endpoint_type& endpoint) {
    return _state_map.erase(endpoint);
}

} // namespace gms
