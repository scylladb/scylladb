/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "gms/inet_address.hh"
#include "locator/host_id.hh"
#include "utils/frozen_sstring.hh"

using namespace seastar;

namespace locator {

using inet_address = gms::inet_address;

using dc_name = utils::basic_frozen_sstring<struct dc_name_tag, char, uint32_t, 15, true>;
using rack_name = utils::basic_frozen_sstring<struct rack_name_tag, char, uint32_t, 15, true>;

// Endpoint Data Center and Rack names
struct endpoint_dc_rack {
    dc_name dc;
    rack_name rack;

    endpoint_dc_rack() = default;

    endpoint_dc_rack(dc_name dc, rack_name rack) noexcept
        : dc(std::move(dc))
        , rack(std::move(rack))
    {}

    endpoint_dc_rack(sstring dc, sstring rack) noexcept
        : dc(std::move(dc))
        , rack(std::move(rack))
    {}

    static thread_local const endpoint_dc_rack default_location;

    bool operator==(const endpoint_dc_rack&) const = default;
};

using dc_rack_fn = seastar::noncopyable_function<std::optional<endpoint_dc_rack>(host_id)>;

} // namespace locator
