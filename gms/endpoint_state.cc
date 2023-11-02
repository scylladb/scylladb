/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/endpoint_state.hh"
#include <optional>
#include <ostream>
#include <boost/lexical_cast.hpp>

namespace gms {

static_assert(std::is_default_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_copy_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_move_constructible_v<heart_beat_state>);

static_assert(std::is_nothrow_default_constructible_v<std::map<application_state, versioned_value>>);

// Note: although std::map::find is not guaranteed to be noexcept
// it depends on the comperator used and in this case comparing application_state
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
    os << "HeartBeatState = " << x._heart_beat_state << ", AppStateMap =";
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

}
