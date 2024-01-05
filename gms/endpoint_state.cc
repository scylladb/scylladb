/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/endpoint_state.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include <optional>
#include <ostream>
#include <boost/lexical_cast.hpp>

namespace gms {

static_assert(std::is_default_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_copy_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_move_constructible_v<heart_beat_state>);

static_assert(std::is_nothrow_default_constructible_v<application_state_map>);

// Note: although std::map::find is not guaranteed to be noexcept
// it depends on the comparator used and in this case comparing application_state
// is noexcept.  Therefore, we can safely mark this method noexcept.
const versioned_value* endpoint_state::get_application_state_ptr(application_state key) const noexcept {
    auto it = _application_state.find(key);
    if (it == _application_state.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::ostream& operator<<(std::ostream& os, const endpoint_state& x) {
    fmt::print(os, "HeartBeatState = {}, AppStateMap =", x._heart_beat_state);
    for (auto&entry : x._application_state) {
        const application_state& state = entry.first;
        const versioned_value& value = entry.second;
        os << " { " << state << " : " << value << " } ";
    }
    return os;
}

bool endpoint_state::is_cql_ready() const noexcept {
    auto* app_state = get_application_state_ptr(application_state::RPC_READY);
    if (!app_state) {
        return false;
    }
    try {
        return boost::lexical_cast<int>(app_state->value());
    } catch (...) {
        return false;
    }
}

future<> i_endpoint_state_change_subscriber::on_application_state_change(inet_address endpoint,
        const gms::application_state_map& states, application_state app_state, permit_id pid,
        std::function<future<>(inet_address, const gms::versioned_value&, permit_id)> func) {
    auto it = states.find(app_state);
    if (it != states.end()) {
        return func(endpoint, it->second, pid);
    }
    return make_ready_future<>();
}

}
