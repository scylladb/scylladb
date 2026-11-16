/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace streaming {

enum class stream_mutation_fragments_cmd : uint8_t {
    error,
    mutation_fragment_data,
    end_of_stream,
    abort,
};

// The status the receiver of STREAM_MUTATION_FRAGMENTS sends back to the sender.
// The values are part of the wire protocol.
enum class stream_mutation_fragments_status : int32_t {
    ok = 0,
    error = -1,
    // The receiver failed because the table was dropped. Told apart from
    // `error`, because some senders don't treat it as a failure.
    table_dropped = -2,
};

}
