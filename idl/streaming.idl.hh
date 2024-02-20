/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "idl/range.idl.hh"
#include "idl/token.idl.hh"
#include "idl/uuid.idl.hh"

#include "streaming/stream_fwd.hh"

namespace streaming {

class plan_id final {
    utils::UUID uuid();
};

class stream_request {
    sstring keyspace;
    // For compatibility with <= 1.5, we use wrapping ranges
    // (though we never send wraparounds; only allow receiving them)
    std::vector<wrapping_interval<dht::token>> ranges_compat();
    std::vector<sstring> column_families;
};

class stream_summary {
    table_id cf_id;
    int files;
    long total_size;
};


class prepare_message {
    std::vector<streaming::stream_request> requests;
    std::vector<streaming::stream_summary> summaries;
    uint32_t dst_cpu_id;
};

enum class stream_reason : uint8_t {
    unspecified,
    bootstrap,
    decommission,
    removenode,
    rebuild,
    repair,
    replace,
};

enum class stream_mutation_fragments_cmd : uint8_t {
    error,
    mutation_fragment_data,
    end_of_stream,
};

}
