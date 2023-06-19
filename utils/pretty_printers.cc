/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "pretty_printers.hh"

namespace utils {

std::ostream& operator<<(std::ostream& os, pretty_printed_data_size data) {
    static constexpr const char *suffixes[] = {" bytes", "kB", "MB", "GB", "TB", "PB"};

    unsigned exp = 0;
    while ((data._size >= 1000) && (exp < sizeof(suffixes))) {
        exp++;
        data._size /= 1000;
    }

    os << data._size << suffixes[exp];
    return os;
}

std::ostream& operator<<(std::ostream& os, pretty_printed_throughput tp) {
    uint64_t throughput = tp._duration.count() > 0 ? tp._size / tp._duration.count() : 0;
    os << pretty_printed_data_size(throughput) << "/s";
    return os;
}

}
