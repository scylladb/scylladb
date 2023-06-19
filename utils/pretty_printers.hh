/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <ostream>

namespace utils {

class pretty_printed_data_size {
    uint64_t _size;
public:
    pretty_printed_data_size(uint64_t size) : _size(size) {}

    friend std::ostream& operator<<(std::ostream&, pretty_printed_data_size);
};

class pretty_printed_throughput {
    uint64_t _size;
    std::chrono::duration<float> _duration;
public:
    pretty_printed_throughput(uint64_t size, std::chrono::duration<float> dur) : _size(size), _duration(std::move(dur)) {}

    friend std::ostream& operator<<(std::ostream&, pretty_printed_throughput);
};

}
