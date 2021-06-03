/*
 * Copyright (C) 2018-present ScyllaDB
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
#include "service/qos/qos_common.hh"
#include "utils/UUID.hh"
#include "cdc/generation_id.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace cql3 {
class query_processor;
}

namespace cdc {
    class stream_id;
    class topology_description;
    class streams_version;
} // namespace cdc

namespace service {
    class storage_proxy;
}

namespace db {

class system_distributed_keyspace {
public:
    static constexpr auto NAME = "system_distributed";
    static constexpr auto NAME_EVERYWHERE = "system_distributed_everywhere";

    static constexpr auto VIEW_BUILD_STATUS = "view_build_status";
    static constexpr auto SERVICE_LEVELS = "service_levels";

    /* Nodes use this table to communicate new CDC stream generations to other nodes. */
    static constexpr auto CDC_TOPOLOGY_DESCRIPTION = "cdc_generation_descriptions";

    /* Nodes use this table to communicate new CDC stream generations to other nodes.
     * Resides in system_distributed_everywhere. */
    static constexpr auto CDC_GENERATIONS_V2 = "cdc_generation_descriptions_v2";

    /* This table is used by CDC clients to learn about available CDC streams. */
    static constexpr auto CDC_DESC_V2 = "cdc_streams_descriptions_v2";

    /* Used by CDC clients to learn CDC generation timestamps. */
    static constexpr auto CDC_TIMESTAMPS = "cdc_generation_timestamps";

    /* Previous version of the "cdc_streams_descriptions_v2" table.
     * We use it in the upgrade procedure to ensure that CDC generations appearing
     * in the old table also appear in the new table, if necessary. */
    static constexpr auto CDC_DESC_V1 = "cdc_streams_descriptions";

    /* Information required to modify/query some system_distributed tables, passed from the caller. */
    struct context {
        /* How many different token owners (endpoints) are there in the token ring? */
        size_t num_token_owners;
    };
private:
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    service::storage_proxy& _sp;

    bool _started = false;
    bool _forced_cdc_timestamps_schema_sync = false;

public:
    /* Should writes to the given table always be synchronized by commitlog (flushed to disk)
     * before being acknowledged? */
    static bool is_extra_durable(const sstring& cf_name);

    system_distributed_keyspace(cql3::query_processor&, service::migration_manager&, service::storage_proxy&);

    future<> start();
    future<> stop();

    bool started() const { return _started; }

    future<std::unordered_map<utils::UUID, sstring>> view_status(sstring ks_name, sstring view_name) const;
    future<> start_view_build(sstring ks_name, sstring view_name) const;
    future<> finish_view_build(sstring ks_name, sstring view_name) const;
    future<> remove_view(sstring ks_name, sstring view_name) const;

    // Precondition: "system_distributed.cdc_generation_descriptions" exists and it was created by Scylla.
    // In practice this means that the table was created by a previous version from which the cluster was upgraded;
    // the precondition says that this function should not be called in clusters that were freshly created in a new version.
    future<> insert_cdc_topology_description(cdc::generation_id_v1, const cdc::topology_description&, context);
    // Precondition: same as above.
    future<std::optional<cdc::topology_description>> read_cdc_topology_description(cdc::generation_id_v1, context);

    future<> insert_cdc_generation(utils::UUID, const cdc::topology_description&, context);
    future<std::optional<cdc::topology_description>> read_cdc_generation(utils::UUID);

    future<> create_cdc_desc(db_clock::time_point, const cdc::topology_description&, context);
    future<bool> cdc_desc_exists(db_clock::time_point, context);

    /* Get all generation timestamps appearing in the "cdc_streams_descriptions" table
     * (the old CDC stream description table). */
    future<std::vector<db_clock::time_point>> get_cdc_desc_v1_timestamps(context);

    future<std::map<db_clock::time_point, cdc::streams_version>> cdc_get_versioned_streams(db_clock::time_point not_older_than, context);

    future<db_clock::time_point> cdc_current_generation_timestamp(context);

    future<qos::service_levels_info> get_service_levels() const;
    future<qos::service_levels_info> get_service_level(sstring service_level_name) const;
    future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const;
    future<> drop_service_level(sstring service_level_name) const;
};

}
