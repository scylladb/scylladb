/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "cdc/metadata.hh"
#include "cdc/generation_id.hh"

namespace db {
class system_keyspace;
}

namespace locator {
class tablet_map;
}

namespace cdc {

class generation_service : public peering_sharded_service<generation_service>
                         , public async_sharded_service<generation_service> {
public:
    struct config {
        std::chrono::milliseconds ring_delay;
    };

private:
    bool _stopped = false;

    config _cfg;
    sharded<db::system_keyspace>& _sys_ks;
    replica::database& _db;

    /* Maintains the set of known CDC generations used to pick streams for log writes (i.e., the partition keys of these log writes). */
    cdc::metadata _cdc_metadata;

public:
    generation_service(config cfg,
            sharded<db::system_keyspace>& sys_ks,
            replica::database& db);

    future<> stop();
    ~generation_service();

    cdc::metadata& get_cdc_metadata() {
        return _cdc_metadata;
    }

    /* Retrieve the CDC generation with the given ID from local tables
     * and start using it for CDC log writes if it's not obsolete.
     * Precondition: the generation was committed using group 0 and locally applied.
     */
    future<> handle_cdc_generation(cdc::generation_id_v2);

    future<> load_cdc_tablet_streams(std::optional<std::unordered_set<table_id>> changed_tables);

    future<> query_cdc_timestamps(table_id table, bool ascending, noncopyable_function<future<>(db_clock::time_point)> f);
    future<> query_cdc_streams(table_id table, noncopyable_function<future<>(db_clock::time_point, const utils::chunked_vector<cdc::stream_id>& current, cdc::cdc_stream_diff)> f);

    future<> generate_tablet_resize_update(utils::chunked_vector<canonical_mutation>& muts, table_id table, const locator::tablet_map& new_tablet_map, api::timestamp_type ts);

    future<utils::chunked_vector<mutation>> garbage_collect_cdc_streams_for_table(table_id table, std::optional<std::chrono::seconds> ttl, api::timestamp_type ts);
    future<> garbage_collect_cdc_streams(utils::chunked_vector<canonical_mutation>& muts, api::timestamp_type ts);

};

} // namespace cdc
