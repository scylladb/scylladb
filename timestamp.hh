/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <cstdint>
#include <limits>

namespace api {

using timestamp_type = int64_t;
timestamp_type constexpr missing_timestamp = std::numeric_limits<timestamp_type>::min();
timestamp_type constexpr min_timestamp = std::numeric_limits<timestamp_type>::min() + 1;
timestamp_type constexpr max_timestamp = std::numeric_limits<timestamp_type>::max();

}



