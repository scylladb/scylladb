/*
 * Copyright (C) 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <iosfwd>
#include <vector>
#include <fmt/ostream.h>
#include <seastar/core/sstring.hh>

namespace seastar {
struct unified_port_options;
}

namespace YAML {
class Node;
}

namespace db {

/// Startup configuration for the unified CQL listener.
struct native_transport_unified_port_config {
    uint16_t port = 0;
    seastar::sstring encryption;
    seastar::sstring default_routing = "balanced";
    seastar::sstring ppv2 = "disabled";
    std::vector<seastar::sstring> interface_subnets;
    std::vector<seastar::sstring> addresses;

    bool operator==(const native_transport_unified_port_config&) const = default;
    static native_transport_unified_port_config decode(const YAML::Node&);
    void validate() const;
    /// Snapshot interface subnets and build the connection policy at startup.
    seastar::unified_port_options make_listen_options() const;
};

std::istream& operator>>(std::istream&, native_transport_unified_port_config&);
std::ostream& operator<<(std::ostream&, const native_transport_unified_port_config&);

}

template <>
struct fmt::formatter<db::native_transport_unified_port_config> : fmt::ostream_formatter {};
