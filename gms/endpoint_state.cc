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
    fmt::print(os, "{}", x);
    return os;
}

bool endpoint_state::is_cql_ready() const noexcept {
    // Note:
    // - New scylla node always send application_state::RPC_READY = false when
    // the node boots and send application_state::RPC_READY = true when cql
    // server is up
    // - Old scylla node that does not support the application_state::RPC_READY
    // never has application_state::RPC_READY in the endpoint_state, we can
    // only think their cql server is up, so we return true here if
    // application_state::RPC_READY is not present
    auto* app_state = get_application_state_ptr(application_state::RPC_READY);
    if (!app_state) {
        return true;
    }
    try {
        return boost::lexical_cast<int>(app_state->value());
    } catch (...) {
        return false;
    }
}

future<> i_endpoint_state_change_subscriber::on_application_state_change(locator::host_id host_id, inet_address endpoint,
        const gms::application_state_map& states, application_state app_state, permit_id pid,
        std::function<future<>(locator::host_id, inet_address, const gms::versioned_value&, permit_id)> func) {
    auto it = states.find(app_state);
    if (it != states.end()) {
        return func(host_id, endpoint, it->second, pid);
    }
    return make_ready_future<>();
}

}

auto fmt::formatter<gms::endpoint_state>::format(const gms::endpoint_state& x,
                                                 fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "HeartBeatState = {}, AppStateMap =", x._heart_beat_state);
    for (auto& [state, value] : x._application_state) {
        out = fmt::format_to(out, " {{ {} : {} }} ", state, value);
    }
    return out;
}
