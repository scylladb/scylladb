/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "types/types.hh"
#include "types/tuple.hh"
#include "types/list.hh"
#include "timestamp.hh"
#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "mutation/mutation.hh"
#include "mutation/canonical_mutation.hh"
#include "replica/database_fwd.hh"

#include <seastar/core/future.hh>

#include <vector>


namespace cql3 {

class query_processor;

}

namespace replica {

data_type get_replica_set_type();

data_type get_tablet_info_type();

schema_ptr make_tablets_schema();

std::vector<data_value> replicas_to_data_value(const locator::tablet_replica_set& replicas);

/// Converts information in tablet_map to mutations of system.tablets.
///
/// The mutations will delete any older tablet information for the same table.
/// The provided timestamp should be strictly monotonically increasing
/// between calls for the overriding to work correctly.
future<mutation> tablet_map_to_mutation(const locator::tablet_map&,
                                        table_id,
                                        const sstring& keyspace_name,
                                        const sstring& table_name,
                                        api::timestamp_type);

mutation make_drop_tablet_map_mutation(table_id, api::timestamp_type);

/// Stores a given tablet_metadata in system.tablets.
///
/// Overrides tablet maps for tables present in the given tablet metadata.
/// Does not delete tablet maps for tables which are absent in the given tablet metadata.
/// The provided timestamp should be strictly monotonically increasing
/// between calls for tablet map overriding to work correctly.
/// The timestamp must be greater than api::min_timestamp.
future<> save_tablet_metadata(replica::database&, const locator::tablet_metadata&, api::timestamp_type);

/// Extract a tablet metadata change hint from the tablet mutations.
///
/// Mutations which don't mutate the tablet table are ignored.
std::optional<locator::tablet_metadata_change_hint> get_tablet_metadata_change_hint(const std::vector<canonical_mutation>&);

/// Update the tablet metadata change hint, with the changes represented by the tablet mutation.
///
/// If the mutation belongs to another table, no updates are done.
void update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint&, const mutation&);

/// Reads the replica set from given cell value
locator::tablet_replica_set tablet_replica_set_from_cell(const data_value&);

/// Reads tablet metadata from system.tablets.
future<locator::tablet_metadata> read_tablet_metadata(cql3::query_processor&);

/// Reads the set of hosts referenced by tablet replicas.
future<std::unordered_set<locator::host_id>> read_required_hosts(cql3::query_processor&);

/// Update tablet metadata from system.tablets, based on the provided hint.
///
/// The hint is used to determine what has changed and only reload the changed
/// parts from disk, updating the passed-in metadata in-place accordingly.
future<> update_tablet_metadata(cql3::query_processor&, locator::tablet_metadata&, const locator::tablet_metadata_change_hint&);

/// Reads tablet metadata from system.tablets in the form of mutations.
future<std::vector<canonical_mutation>> read_tablet_mutations(seastar::sharded<database>&);

/// Reads tablet transition stage (if any)
future<std::optional<locator::tablet_transition_stage>> read_tablet_transition_stage(cql3::query_processor& qp, table_id tid, dht::token last_token);

} // namespace replica
