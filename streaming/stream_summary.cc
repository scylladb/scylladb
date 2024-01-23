/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_summary.hh"

namespace streaming {

std::ostream& operator<<(std::ostream& os, const stream_summary& x) {
    os << "[ cf_id=" << x.cf_id << " ]";
    return os;
}

} // namespace streaming
