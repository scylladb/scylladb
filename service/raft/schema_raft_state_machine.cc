/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "service/raft/schema_raft_state_machine.hh"
#include <seastar/core/coroutine.hh>
#include "service/migration_manager.hh"
#include "message/messaging_service.hh"
#include "canonical_mutation.hh"
#include "schema_mutations.hh"
#include "frozen_schema.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/uuid.dist.hh"
#include "serializer_impl.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"
#include "service/migration_manager.hh"

namespace service {

static logging::logger slogger("schema_raft_sm");

future<> schema_raft_state_machine::apply(std::vector<raft::command_cref> command) {
    slogger.trace("apply() is called");
    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        std::vector<canonical_mutation> mutations =
                            ser::deserialize(is, boost::type<std::vector<canonical_mutation>>());

        slogger.trace("merging schema mutations");
        co_await _mm.merge_schema_from(netw::messaging_service::msg_addr(gms::inet_address{}), std::move(mutations));
    }
}

future<raft::snapshot_id> schema_raft_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void schema_raft_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> schema_raft_state_machine::load_snapshot(raft::snapshot_id id) {
    return make_ready_future<>();
}

future<> schema_raft_state_machine::transfer_snapshot(gms::inet_address from, raft::snapshot_id snp) {
    // Note that this may bring newer state than the schema state machine raft's
    // log, so some raft entries may be double applied, but since the state
    // machine idempotent it is not a problem.
    return _mm.submit_migration_task(from, false);
}

future<> schema_raft_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service
