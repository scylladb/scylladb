/*
 * Copyright (C) 2017 ScyllaDB
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

#include "auth/common.hh"

#include <seastar/core/shared_ptr.hh>

#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"

namespace auth {

namespace meta {

const sstring DEFAULT_SUPERUSER_NAME("cassandra");
const sstring AUTH_KS("system_auth");
const sstring USERS_CF("users");
const sstring AUTH_PACKAGE_NAME("org.apache.cassandra.auth.");

}

future<> create_metadata_table_if_missing(
        const sstring& table_name,
        cql3::query_processor& qp,
        const sstring& cql,
        ::service::migration_manager& mm) {
    auto& db = qp.db().local();

    if (db.has_schema(meta::AUTH_KS, table_name)) {
        return make_ready_future<>();
    }

    auto parsed_statement = static_pointer_cast<cql3::statements::raw::cf_statement>(
            cql3::query_processor::parse_statement(cql));

    parsed_statement->prepare_keyspace(meta::AUTH_KS);

    auto statement = static_pointer_cast<cql3::statements::create_table_statement>(
            parsed_statement->prepare(db, qp.get_cql_stats())->statement);

    const auto schema = statement->get_cf_meta_data();
    const auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    return mm.announce_new_column_family(b.build(), false);
}

}
