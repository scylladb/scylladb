/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/gossip_digest_ack.hh"
#include <ostream>

namespace gms {

std::ostream& operator<<(std::ostream& os, const gossip_digest_ack& ack) {
    os << "digests:{";
    for (auto& d : ack._digests) {
        os << d << " ";
    }
    os << "} ";
    os << "endpoint_state:{";
    for (auto& d : ack._map) {
        fmt::print(os, "[{}->{}]", d.first, d.second);
    }
    return os << "}";
}

} // namespace gms
