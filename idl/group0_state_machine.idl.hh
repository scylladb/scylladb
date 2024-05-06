/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "raft/raft.hh"
#include "gms/inet_address_serializer.hh"

#include "idl/frozen_schema.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/raft_storage.idl.hh"

namespace service {

struct schema_change {
    std::vector<canonical_mutation> mutations;
};

struct broadcast_table_query {
    service::broadcast_tables::query query;
};

struct topology_change {
    std::vector<canonical_mutation> mutations;
};

struct mixed_change {
    std::vector<canonical_mutation> mutations;
};

struct write_mutations {
    std::vector<canonical_mutation> mutations;
};

struct group0_command {
    std::variant<service::schema_change, service::broadcast_table_query, service::topology_change, service::write_mutations, service::mixed_change> change;
    canonical_mutation history_append;

    std::optional<utils::UUID> prev_state_id;
    utils::UUID new_state_id;

    gms::inet_address creator_addr;
    raft::server_id creator_id;
};

} // namespace service
