/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_set>

#include <boost/intrusive/list.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include "gms/inet_address.hh"
#include "locator/host_id.hh"

using namespace seastar;

namespace locator {

using inet_address = gms::inet_address;

class shared_token_metadata;

// Endpoint Data Center and Rack names
struct endpoint_dc_rack {
    sstring dc;
    sstring rack;

    static thread_local const endpoint_dc_rack default_location;

    bool operator==(const endpoint_dc_rack&) const = default;
    bool operator!=(const endpoint_dc_rack&) const = default;
};

using dc_rack_fn = seastar::noncopyable_function<endpoint_dc_rack(inet_address)>;

} // namespace locator
