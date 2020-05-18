/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include "bytes.hh"
#include "schema_fwd.hh"
#include "service/migration_manager.hh"
#include "utils/UUID.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace cql3 {
class query_processor;
}

namespace cdc {
    class stream_id;
    class topology_description;
    class topology_description_version;
} // namespace cdc

namespace db {

class system_distributed_keyspace {
public:
    static constexpr auto NAME = "system_distributed";
    static constexpr auto VIEW_BUILD_STATUS = "view_build_status";

    /* Nodes use this table to communicate new CDC stream generations to other nodes. */
    static constexpr auto CDC_TOPOLOGY_DESCRIPTION = "cdc_topology_description";

    /* This table is used by CDC clients to learn about avaliable CDC streams. */
    static constexpr auto CDC_DESC = "cdc_description";

    /* Information required to modify/query some system_distributed tables, passed from the caller. */
    struct context {
        /* How many different token owners (endpoints) are there in the token ring? */
        size_t num_token_owners;
    };
private:
    cql3::query_processor& _qp;
    service::migration_manager& _mm;

public:
    system_distributed_keyspace(cql3::query_processor&, service::migration_manager&);

    future<> start();
    future<> stop();

    future<std::unordered_map<utils::UUID, sstring>> view_status(sstring ks_name, sstring view_name) const;
    future<> start_view_build(sstring ks_name, sstring view_name) const;
    future<> finish_view_build(sstring ks_name, sstring view_name) const;
    future<> remove_view(sstring ks_name, sstring view_name) const;

    future<> insert_cdc_topology_description(db_clock::time_point streams_ts, const cdc::topology_description&, context);
    future<std::optional<cdc::topology_description>> read_cdc_topology_description(db_clock::time_point streams_ts, context);
    future<> expire_cdc_topology_description(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);

    future<> create_cdc_desc(db_clock::time_point streams_ts, const std::vector<cdc::stream_id>&, context);
    future<> expire_cdc_desc(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);
    future<bool> cdc_desc_exists(db_clock::time_point streams_ts, context);

    future<std::vector<cdc::topology_description_version>> cdc_get_topology_descriptions(context);
};

}
