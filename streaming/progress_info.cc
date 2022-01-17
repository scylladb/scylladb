/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/progress_info.hh"

namespace streaming {

std::ostream& operator<<(std::ostream& os, const progress_info& x) {
    sstring dir = x.dir == progress_info::direction::OUT ? "sent to " : "received from ";
    return os << format("{} {:d}/({:f}%) {} {}", x.file_name, x.current_bytes,
            x.current_bytes * 100 / x.total_bytes, dir, x.peer);
}

}
