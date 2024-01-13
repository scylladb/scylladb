/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "gms/application_state.hh"
#include "seastarx.hh"
#include "gms/endpoint_state.hh"

namespace gms {

class gossiper;

class endpoint_state_map {
public:
    using endpoint_type = locator::host_id;
    using map_type = std::unordered_map<endpoint_type, endpoint_state_ptr>;

private:
    map_type _state_map;

    friend class gossiper;
public:
    auto size() const noexcept {
        return _state_map.size();
    }

    future<> clear_gently();

    map_type get_endpoint_states() const {
        return _state_map;
    }

    bool contains(const endpoint_type& endpoint) const noexcept {
        return _state_map.contains(endpoint);
    }

    bool contains(const inet_address& addr) const noexcept;

    inet_address get_endpoint_address(const endpoint_type& endpoint) const noexcept;
    endpoint_type get_endpoint_by_address(const inet_address& addr) const noexcept;

    std::vector<endpoint_type> get_endpoints() const;
    std::unordered_set<inet_address> get_endpoint_addresses() const;

    endpoint_state_ptr get_ptr(const endpoint_type& endpoint) const noexcept;

    // Looks up the endpoint state by address
    // Note: this function is inefficient and requires O(n) comparisons
    endpoint_state_ptr get_ptr(const inet_address& addr) const noexcept;

    const versioned_value* get_application_state_ptr(const endpoint_type& endpoint, application_state key) const noexcept {
        if (auto eps = get_ptr(endpoint)) {
            return eps->get_application_state_ptr(key);
        }
        return nullptr;
    }

    sstring get_application_state_value(const endpoint_type& endpoint, application_state key) const {
        if (auto eps = get_ptr(endpoint)) {
            if (auto app_state = eps->get_application_state_ptr(key)) {
                return app_state->value();
            }
        }
        return "";
    }

    // Calls func for each endpoint_state.
    // Called function must not yield
    void for_each(std::function<void(const locator::host_id&, const endpoint_state&)> func) const {
        for_each_until([func = std::move(func)] (const locator::host_id& host_id, const endpoint_state& eps) {
            func(host_id, eps);
            return stop_iteration::no;
        });
    }

    // Calls func for each endpoint_state until it returns stop_iteration::yes
    // Returns stop_iteration::yes iff `func` returns stop_iteration::yes.
    // Called function must not yield
    stop_iteration for_each_until(std::function<stop_iteration(const locator::host_id&, const endpoint_state&)>) const;

    // Returns true if there was no such endpoint in the state map
    // Otherwise, the mapped endpoint_state_ptr is replaced with the given one and false is returned.
    bool insert(const endpoint_type& endpoint, endpoint_state_ptr eps);

    // Returns true iff the endpoint state was erased.
    bool erase(const endpoint_type& endpoint);

    // Returns null address if not found
};

} // namespace gms
