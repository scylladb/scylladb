/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "seastarx.hh"
#include "gms/endpoint_state.hh"

namespace gms {

class gossiper;

class endpoint_state_map {
    std::unordered_map<inet_address, endpoint_state_ptr> _state_map;

    friend class gossiper;
public:
    auto size() const noexcept {
        return _state_map.size();
    }

    future<> clear_gently();

    bool contains(const inet_address& addr) const noexcept {
        return _state_map.contains(addr);
    }

    std::vector<inet_address> get_endpoints() const;

    endpoint_state_ptr get_endpoint_state_ptr(const inet_address& addr) const noexcept;

    // Calls func for each endpoint_state.
    // Called function must not yield
    void for_each_endpoint_state(std::function<void(const inet_address&, const endpoint_state&)> func) const {
        for_each_endpoint_state_until([func = std::move(func)] (const inet_address& node, const endpoint_state& eps) {
            func(node, eps);
            return stop_iteration::no;
        });
    }

    // Calls func for each endpoint_state until it returns stop_iteration::yes
    // Returns stop_iteration::yes iff `func` returns stop_iteration::yes.
    // Called function must not yield
    stop_iteration for_each_endpoint_state_until(std::function<stop_iteration(const inet_address&, const endpoint_state&)>) const;

    bool insert(const inet_address& addr, endpoint_state_ptr eps);

    bool erase(const inet_address& addr);
};

} // namespace gms
