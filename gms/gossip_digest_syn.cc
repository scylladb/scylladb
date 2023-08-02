/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/gossip_digest_syn.hh"
#include <ostream>

namespace gms {

std::ostream& operator<<(std::ostream& os, const gossip_digest_syn& syn) {
    os << "cluster_id:" << syn._cluster_id << ",partioner:" << syn._partioner << ",group0_id:" << syn._group0_id << ",";
    os << "digests:{";
    for (auto& d : syn._digests) {
        os << d << " ";
    }
    return os << "}";
}

} // namespace gms
