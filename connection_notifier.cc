/*
 * Copyright (C) 2019 ScyllaDB
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
#include "db/query_context.hh"
#include "cql3/constants.hh"

#include <stdexcept>

namespace db::system_keyspace {
const char *const clients_cf_name();
}

static sstring to_string(client_type ct) {
    switch (ct) {
        case client_type::cql: return "cql";
        case client_type::thrift: return "thrift";
        case client_type::alternator: return "alternator";
        default: throw std::runtime_error("Invalid client_type");
    }
}

future<> notify_new_client(client_data cd) {
    const static sstring req
            = format("INSERT INTO system.{} (address, port, client_type, shard_id, protocol_version, username) "
                     "VALUES (?, ?, ?, ?, ?, ?);", db::system_keyspace::clients_cf_name());
    
    return db::execute_cql(req,
            std::move(cd.ip), cd.port, to_string(cd.ct), cd.shard_id,
            cd.protocol_version.has_value() ? data_value(*cd.protocol_version) : data_value::make_null(int32_type),
            cd.username.value_or("anonymous")).discard_result();
}

future<> notify_disconnected_client(gms::inet_address addr, client_type ct, int port) {
    const static sstring req
            = format("DELETE FROM system.{} where address=? AND port=? AND client_type=?;",
                     db::system_keyspace::clients_cf_name());
    return db::execute_cql(req, addr.addr(), port, to_string(ct)).discard_result();
}

future<> clear_clientlist() {
    const static sstring req
            = format("TRUNCATE system.{};", db::system_keyspace::clients_cf_name());
    return db::execute_cql(req).discard_result();
}
