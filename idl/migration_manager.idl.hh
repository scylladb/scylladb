/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/chunked_vector.hh"

#include "idl/frozen_mutation.idl.hh"
#include "idl/frozen_schema.idl.hh"
#include "idl/messaging_service.idl.hh"

verb [[with_client_info, cancellable]] migration_request (netw::schema_pull_options options [[version 3.2.0]]) -> utils::chunked_vector<frozen_mutation>, utils::chunked_vector<canonical_mutation> [[version 3.2.0]]
verb get_schema_version (unsigned shard, table_schema_version version) -> frozen_schema
verb [[cancellable]] schema_check () -> table_schema_version
verb [[with_client_info, one_way]] definitions_update (utils::chunked_vector<frozen_mutation> fm, utils::chunked_vector<canonical_mutation> cm [[version 3.2.0]])
