/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "gms/inet_address_serializer.hh"
#include "gms/version_generator.hh"
#include "gms/generation-number.hh"

#include "idl/utils.idl.hh"

namespace gms {
enum class application_state:int {
        STATUS = 0,
        LOAD,
        SCHEMA,
        DC,
        RACK,
        RELEASE_VERSION,
        REMOVAL_COORDINATOR,
        INTERNAL_IP,
        RPC_ADDRESS,
        X_11_PADDING,
        SEVERITY,
        NET_VERSION,
        HOST_ID,
        TOKENS,
        SUPPORTED_FEATURES
};

class versioned_value {
    sstring value();
    gms::version_type version();
};

class heart_beat_state {
    gms::generation_type get_generation();
    gms::version_type get_heart_beat_version();
};

class endpoint_state {
    gms::heart_beat_state get_heart_beat_state();
    std::unordered_map<gms::application_state, gms::versioned_value> get_application_state_map();
};

class gossip_digest {
    gms::inet_address get_endpoint();
    gms::generation_type get_generation();
    gms::version_type get_max_version();
};

class gossip_digest_syn {
    sstring get_cluster_id();
    sstring get_partioner();
    utils::chunked_vector<gms::gossip_digest> get_gossip_digests();
    utils::UUID get_group0_id()[[version 5.4]];
};

class gossip_digest_ack {
    utils::chunked_vector<gms::gossip_digest> get_gossip_digest_list();
    std::map<gms::inet_address, gms::endpoint_state> get_endpoint_state_map();
};

class gossip_digest_ack2 {
    std::map<gms::inet_address, gms::endpoint_state> get_endpoint_state_map();
};

struct gossip_get_endpoint_states_request {
    std::unordered_set<gms::application_state> application_states;
};

struct gossip_get_endpoint_states_response {
    std::unordered_map<gms::inet_address, gms::endpoint_state> endpoint_state_map;
};

}
