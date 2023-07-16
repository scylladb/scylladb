/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "pretty_printers.hh"

namespace utils {

std::ostream& operator<<(std::ostream& os, pretty_printed_data_size data) {
    static constexpr const char * suffixes[] = {" bytes", "kB", "MB", "GB", "TB", "PB"};

    const char* suffix = nullptr;
    uint64_t size = data._size;
    uint64_t next_size = size;
    for (auto s : suffixes) {
        suffix = s;
        size = next_size;
        next_size = size / 1000;
        if (next_size == 0) {
            break;
        }
    }
    return os << size << suffix;
}

std::ostream& operator<<(std::ostream& os, pretty_printed_throughput tp) {
    uint64_t throughput = tp._duration.count() > 0 ? tp._size / tp._duration.count() : 0;
    os << pretty_printed_data_size(throughput) << "/s";
    return os;
}

}
