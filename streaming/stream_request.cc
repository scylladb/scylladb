/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_request.hh"

namespace streaming {

std::ostream& operator<<(std::ostream& os, const stream_request& sr) {
    os << "[ ks = " << sr.keyspace << " cf =  ";
    for (auto& cf : sr.column_families) {
        os << cf << " ";
    }
    return os << "]";
}

} // namespace streaming;
