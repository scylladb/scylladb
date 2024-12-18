/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/range.idl.hh"
#include "idl/token.idl.hh"
#include "idl/uuid.idl.hh"

#include "streaming/stream_fwd.hh"

namespace service {

// Before the mode of prepare_message verb to the IDL
// there was no serizlizer for session_id and one from
// raft_storage.idl.hh for tagged_id was erroneously
// used. It does not marked as `final`, so here we have
// to omit it as well for compatibility.
class session_id {
    utils::UUID uuid();
}

}

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

verb [[with_client_info]] prepare_message (streaming::prepare_message msg, streaming::plan_id plan_id, sstring description, streaming::stream_reason reason [[version 3.1.0]], service::session_id session [[version 6.0.0]]) -> streaming::prepare_message;
verb [[with_client_info]] prepare_done_message (streaming::plan_id plan_id, unsigned dst_cpu_id);
verb [[with_client_info]] stream_mutation_done (streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id);
verb [[with_client_info]] complete_message (streaming::plan_id plan_id, unsigned dst_cpu_id, bool failed [[version 2.1.0]]);
}
