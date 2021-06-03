/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "connection_notifier.hh"
#include "cql3/constants.hh"
#include "database.hh"
#include "service/storage_proxy.hh"

#include <stdexcept>

sstring to_string(client_type ct) {
    switch (ct) {
        case client_type::cql: return "cql";
        case client_type::thrift: return "thrift";
        case client_type::alternator: return "alternator";
    }
    throw std::runtime_error("Invalid client_type");
}

static sstring to_string(client_connection_stage ccs) {
    switch (ccs) {
        case client_connection_stage::established: return connection_stage_literal<client_connection_stage::established>;
        case client_connection_stage::authenticating: return connection_stage_literal<client_connection_stage::authenticating>;
        case client_connection_stage::ready: return connection_stage_literal<client_connection_stage::ready>;
    }
    throw std::runtime_error("Invalid client_connection_stage");
}

future<> notify_new_client(client_data cd) {
    // FIXME: consider prepared statement
    const static sstring req
            = format("INSERT INTO system.{} (address, port, client_type, connection_stage, shard_id, protocol_version, username) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?);", db::system_keyspace::CLIENTS);
    
    return db::qctx->execute_cql(req,
            std::move(cd.ip), cd.port, to_string(cd.ct), to_string(cd.connection_stage), cd.shard_id,
            cd.protocol_version.has_value() ? data_value(*cd.protocol_version) : data_value::make_null(int32_type),
            cd.username.value_or("anonymous")).discard_result();
}

future<> notify_disconnected_client(net::inet_address addr, int port, client_type ct) {
    // FIXME: consider prepared statement
    const static sstring req
            = format("DELETE FROM system.{} where address=? AND port=? AND client_type=?;",
                     db::system_keyspace::CLIENTS);
    return db::qctx->execute_cql(req, std::move(addr), port, to_string(ct)).discard_result();
}

future<> clear_clientlist() {
    auto& db_local = service::get_storage_proxy().local().get_db().local();
    return db_local.truncate(
            db_local.find_keyspace(db::system_keyspace_name()),
            db_local.find_column_family(db::system_keyspace_name(),
                    db::system_keyspace::CLIENTS),
            [] { return make_ready_future<db_clock::time_point>(db_clock::now()); },
            false /* with_snapshot */);
}
