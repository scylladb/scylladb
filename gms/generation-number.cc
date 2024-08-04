/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <cassert>
#include <chrono>
#include <utility>

#include <fmt/format.h>

#include "generation-number.hh"

namespace gms {

generation_type get_generation_number() {
    using namespace std::chrono;
    auto now = high_resolution_clock::now().time_since_epoch();
    int generation_number = duration_cast<seconds>(now).count();
    auto ret = generation_type(generation_number);
    // Make sure the clock didn't overflow the 32 bits value
    SCYLLA_ASSERT(ret.value() == generation_number);
    return ret;
}

void validate_gossip_generation(int64_t generation_number) {
    if (!std::in_range<gms::generation_type::value_type>(generation_number)) {
        throw std::out_of_range(fmt::format("gossip generation {} is out of range", generation_number));
    }
}

}
