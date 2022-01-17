/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace streaming {

enum class stream_mutation_fragments_cmd : uint8_t {
    error,
    mutation_fragment_data,
    end_of_stream,
};


}
