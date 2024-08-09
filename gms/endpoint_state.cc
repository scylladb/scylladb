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
#include <ostream>
#include <boost/lexical_cast.hpp>
#include "log.hh"

namespace gms {

logging::logger logger("endpoint_state");

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

locator::host_id endpoint_state::get_host_id() const noexcept {
    locator::host_id host_id;
    if (auto app_state = get_application_state_ptr(application_state::HOST_ID)) {
        host_id = locator::host_id(utils::UUID(app_state->value()));
        if (!host_id) {
            on_internal_error_noexcept(logger, format("Node has null host_id"));
        }
    }
    return host_id;
}

std::optional<locator::endpoint_dc_rack> endpoint_state::get_dc_rack() const {
    if (const auto* dc_state = get_application_state_ptr(application_state::DC)) {
        const auto* rack_state = get_application_state_ptr(application_state::RACK);
        if (dc_state->value().empty() || !rack_state || rack_state->value().empty()) {
            on_internal_error_noexcept(logger, format("Node {} has empty dc={} or rack={}", get_host_id(), dc_state->value(), rack_state ? rack_state->value() : "(null)"));
        } else {
            return std::make_optional<locator::endpoint_dc_rack>(dc_state->value(), rack_state->value());
        }
    }
    return std::nullopt;
}

std::unordered_set<dht::token> endpoint_state::get_tokens() const {
    std::unordered_set<dht::token> ret;
    if (auto app_state = get_application_state_ptr(application_state::TOKENS)) {
        ret = versioned_value::tokens_from_string(app_state->value());
    }
    return ret;
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

auto fmt::formatter<gms::endpoint_state>::format(const gms::endpoint_state& x,
                                                 fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "HeartBeatState = {}, AppStateMap =", x._heart_beat_state);
    for (auto& [state, value] : x._application_state) {
        out = fmt::format_to(out, " {{ {} : {} }} ", state, value);
    }
    return out;
}
