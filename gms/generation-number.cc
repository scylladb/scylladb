/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cassert>
#include <chrono>
#include "generation-number.hh"

namespace gms {

generation_type get_generation_number() {
    using namespace std::chrono;
    auto now = high_resolution_clock::now().time_since_epoch();
    int generation_number = duration_cast<seconds>(now).count();
    auto ret = generation_type(generation_number);
    // Make sure the clock didn't overflow the 32 bits value
    assert(ret.value() == generation_number);
    return ret;
}

}
