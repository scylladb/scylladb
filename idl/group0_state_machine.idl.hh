/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace service {

struct schema_change {
    std::vector<canonical_mutation> mutations;
};

struct group0_command {
    std::variant<service::schema_change> change;
    canonical_mutation history_append;

    std::optional<utils::UUID> prev_state_id;
    utils::UUID new_state_id;

    gms::inet_address creator_addr;
    raft::server_id creator_id;
};

} // namespace service
