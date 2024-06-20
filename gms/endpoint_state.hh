/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/heart_beat_state.hh"
#include "gms/application_state.hh"
#include "gms/versioned_value.hh"
#include "locator/host_id.hh"
#include "locator/types.hh"

namespace gms {

using application_state_map = std::unordered_map<application_state, versioned_value>;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */
class endpoint_state {
public:
    using clk = seastar::lowres_system_clock;
private:
    heart_beat_state _heart_beat_state;
    application_state_map _application_state;
    /* fields below do not get serialized */
    clk::time_point _update_timestamp;
    bool _is_normal = false;

public:
    bool operator==(const endpoint_state& other) const {
        return _heart_beat_state  == other._heart_beat_state &&
               _application_state == other._application_state &&
               _update_timestamp  == other._update_timestamp;
    }

    endpoint_state() noexcept
        : _heart_beat_state()
        , _update_timestamp(clk::now())
    {
        update_is_normal();
    }

    endpoint_state(heart_beat_state initial_hb_state) noexcept
        : _heart_beat_state(initial_hb_state)
        , _update_timestamp(clk::now())
    {
        update_is_normal();
    }

    endpoint_state(heart_beat_state&& initial_hb_state,
            const application_state_map& application_state)
        : _heart_beat_state(std::move(initial_hb_state))
        , _application_state(application_state)
        , _update_timestamp(clk::now())
    {
        update_is_normal();
    }

    // Valid only on shard 0
    heart_beat_state& get_heart_beat_state() noexcept {
        return _heart_beat_state;
    }

    // Valid only on shard 0
    const heart_beat_state& get_heart_beat_state() const noexcept {
        return _heart_beat_state;
    }

    void set_heart_beat_state_and_update_timestamp(heart_beat_state hbs) noexcept {
        update_timestamp();
        _heart_beat_state = hbs;
    }

    const versioned_value* get_application_state_ptr(application_state key) const noexcept;

    /**
     * TODO replace this with operations that don't expose private state
     */
    // @Deprecated
    application_state_map& get_application_state_map() noexcept {
        return _application_state;
    }

    const application_state_map& get_application_state_map() const noexcept {
        return _application_state;
    }

    void add_application_state(application_state key, versioned_value value) {
        _application_state[key] = std::move(value);
        update_is_normal();
    }

    void add_application_state(const endpoint_state& es) {
        _application_state = es._application_state;
        update_is_normal();
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     *
     * Valid only on shard 0.
     */
    clk::time_point get_update_timestamp() const noexcept {
        return _update_timestamp;
    }

    void update_timestamp() noexcept {
        _update_timestamp = clk::now();
    }

public:
    std::string_view get_status() const noexcept {
        constexpr std::string_view empty = "";
        auto* app_state = get_application_state_ptr(application_state::STATUS);
        if (!app_state) {
            return empty;
        }
        const auto& value = app_state->value();
        if (value.empty()) {
            return empty;
        }
        auto pos = value.find(',');
        if (pos == sstring::npos) {
            return std::string_view(value);
        }
        return std::string_view(value.c_str(), pos);
    }

    bool is_shutdown() const noexcept {
        return get_status() == versioned_value::SHUTDOWN;
    }

    bool is_normal() const noexcept {
        return _is_normal;
    }

    void update_is_normal() noexcept {
        _is_normal = get_status() == versioned_value::STATUS_NORMAL;
    }

    bool is_cql_ready() const noexcept;

    // Return the value of the HOST_ID application state
    // or a null host_id if the application state is not found.
    locator::host_id get_host_id() const noexcept;

    std::optional<locator::endpoint_dc_rack> get_dc_rack() const;

    // Return the value of the TOKENS application state
    // or an empty set if the application state is not found.
    std::unordered_set<dht::token> get_tokens() const;

    friend fmt::formatter<endpoint_state>;
};

using endpoint_state_ptr = lw_shared_ptr<const endpoint_state>;

inline endpoint_state_ptr make_endpoint_state_ptr(const endpoint_state& eps) {
    return make_lw_shared<endpoint_state>(eps);
}

inline endpoint_state_ptr make_endpoint_state_ptr(endpoint_state&& eps) {
    return make_lw_shared<endpoint_state>(std::move(eps));
}

// The endpoint state is protected with an endpoint lock
// acquired in the gossiper using gossiper::lock_endpoint.
//
// permit_id identifies the held endpoint lock
// and it is used by gossiper::lock_endpoint to prevent a deadlock
// when a notification function is called under the endpoint lock
// and calls back into the gossiper on a path that wants to acquire
// the endpoint_lock for the same endpoint.
using permit_id = utils::tagged_uuid<struct permit_id_tag>;
constexpr permit_id null_permit_id = permit_id::create_null_id();

} // gms

template <>
struct fmt::formatter<gms::endpoint_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const gms::endpoint_state&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
