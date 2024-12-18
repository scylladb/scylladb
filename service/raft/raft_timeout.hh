// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/source_location-compat.hh>

#include "seastarx.hh"

#include <optional>

namespace service {

struct raft_timeout {
    seastar::compat::source_location loc = seastar::compat::source_location::current();
    std::optional<lowres_clock::time_point> value;
};

}
