/*
 * Copyright 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace service {

struct join_node_request_params {
    raft::server_id host_id;
    std::optional<raft::server_id> replaced_id;
    std::vector<sstring> ignore_nodes;
    sstring cluster_name;
    sstring snitch_name;
    sstring datacenter;
    sstring rack;
    sstring release_version;
    uint32_t num_tokens;
    uint32_t shard_count;
    uint32_t ignore_msb;
    std::vector<sstring> supported_features;
};

struct join_node_request_result {
    struct ok {};
    struct rejected {
        sstring reason;
    };

    std::variant<
        service::join_node_request_result::ok,
        service::join_node_request_result::rejected
    > result;
};

struct join_node_response_params {
    struct accepted {};

    struct rejected {
        sstring reason;
    };

    std::variant<
        service::join_node_response_params::accepted,
        service::join_node_response_params::rejected
    > response;
};

struct join_node_response_result {};

verb join_node_request (raft::server_id dst_id, service::join_node_request_params) -> service::join_node_request_result;
verb join_node_response (raft::server_id dst_id, service::join_node_response_params) -> service::join_node_response_result;

}
