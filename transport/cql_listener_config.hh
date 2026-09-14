/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>

namespace cql_transport {

// The properties of a single listening socket that the connections accepted on
// it need to know about.
struct cql_listener_config {
    // The shard-aware listener that clients of this listener are to be handed
    // over to, and whether it is encrypted. Advertised in the SUPPORTED
    // message. Disengaged when this listener offers no shard-aware port.
    std::optional<uint16_t> shard_aware_port;
    bool shard_aware_port_tls = false;

    bool operator==(const cql_listener_config&) const = default;
};

}
