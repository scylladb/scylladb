/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "snapshot_types.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "db/consistency_level_type.hh"
#include "locator/host_id.hh"
#include "dht/token.hh"
#include "sstables/types.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <unordered_map>

namespace cql3 {
class query_processor;
}

namespace cdc {
    class topology_description;
    class streams_version;
} // namespace cdc

namespace service {
    class storage_proxy;
    class migration_manager;
}


namespace db {

class system_distributed_keyspace {
public:
    static constexpr auto NAME = "system_distributed";

    static constexpr auto VIEW_BUILD_STATUS = "view_build_status";

    /* This table is used by CDC clients to learn about available CDC streams. */
    static constexpr auto CDC_DESC_V2 = "cdc_streams_descriptions_v2";

    /* Used by CDC clients to learn CDC generation timestamps. */
    static constexpr auto CDC_TIMESTAMPS = "cdc_generation_timestamps";

    /* Previous version of the "cdc_streams_descriptions_v2" table.
     * We use it in the upgrade procedure to ensure that CDC generations appearing
     * in the old table also appear in the new table, if necessary. */
    static constexpr auto CDC_DESC_V1 = "cdc_streams_descriptions";

    /* This table is used by the backup and restore code to store per-sstable metadata.
     * The data the coordinator node puts in this table comes from the snapshot manifests. */
    static constexpr auto SNAPSHOT_SSTABLES = "snapshot_sstables";

    /* This table is used by the backup and restore code to store snapshot
     * remote location metadata per datacenter. */
    static constexpr auto SNAPSHOT_REMOTE_LOCATIONS = "snapshot_remote_locations";

    static constexpr uint64_t SNAPSHOT_SSTABLES_TTL_SECONDS = std::chrono::seconds(std::chrono::days(3)).count();

    /* Information required to modify/query some system_distributed tables, passed from the caller. */
    struct context {
        /* How many different token owners (endpoints) are there in the token ring? */
        size_t num_token_owners;
    };

    cql3::query_processor& qp() const {
        return _qp;
    }
private:
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    service::storage_proxy& _sp;

    bool _started = false;
    bool _forced_cdc_timestamps_schema_sync = false;

public:
    static std::vector<schema_ptr> all_distributed_tables();

    system_distributed_keyspace(cql3::query_processor&, service::migration_manager&, service::storage_proxy&);

    future<> start();
    future<> stop();

    bool started() const { return _started; }

    future<> create_cdc_desc(db_clock::time_point, const cdc::topology_description&, context);
    future<bool> cdc_desc_exists(db_clock::time_point, context);

    // Reads and builds generation map - a map from generation timestamps to vector of all stream ids for that generation.
    // Generations with timestamp >= `not_older_than` are returned, plus the one just before it (the straddling generation).
    // Returns empty map if there are no generations with timestamp >= `not_older_than`.
    // NOTE: there's a sibling `read_cdc_for_tablets_versioned_streams`, that reads the same data for tables backed by tablets. The data returned is the same.
    // NOTE: currently used only by alternator
    future<std::map<db_clock::time_point, cdc::streams_version>> cdc_get_versioned_streams(db_clock::time_point not_older_than, context);

    // Read current generation timestamp for the given table. Throws runtime_error (see `cql3::untyped_result_set::one()`) if table not found.
    // NOTE: there's a sibling `read_cdc_for_tablets_current_generation_timestamp` in `system_keyspace`, that does the same for tables backed up by tablets.
    // NOTE: currently used only by alternator
    future<db_clock::time_point> cdc_current_generation_timestamp(context);

private:
    future<> create_tables(std::vector<schema_ptr> tables);
};

class snapshot_table_helper {
    cql3::query_processor& _qp;
public:
    snapshot_table_helper(cql3::query_processor&);

    /* Inserts a single SSTable entry for a given snapshot, keyspace, table, datacenter,
     * and rack. The row is written with the specified TTL (in seconds). Uses consistency
     * level `EACH_QUORUM` by default.*/
    future<> insert_snapshot_sstable(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack
        , sstables::sstable_id sstable_id, dht::token first_token, dht::token last_token, sstring toc_name, sstring prefix
        , locator::host_id node, size_t tablet_id, snapshot_state, int64_t repaired_at = {}
        , int64_t data_size = 0, int64_t index_size = 0
        , db::consistency_level cl = db::consistency_level::EACH_QUORUM);

    /* Retrieves all SSTable entries for a given snapshot, keyspace, table, datacenter, and rack.
     * If `start_token` and `end_token` are provided, only entries whose `first_token` is in the range [`start_token`, `end_token`] will be returned.
     * Returns a vector of `snapshot_sstable_entry` structs containing `sstable_id`, `first_token`, `last_token`,
     * `toc_name`, and `prefix`. Uses consistency level `LOCAL_QUORUM` by default. */
    future<utils::chunked_vector<snapshot_sstable_entry>> get_snapshot_sstables(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM, std::optional<dht::token> start_token = std::nullopt, std::optional<dht::token> end_token = std::nullopt) const;

    future<> update_sstable_download_status(sstring snapshot_name,
                                            sstring ks,
                                            sstring table,
                                            sstring dc,
                                            sstring rack,
                                            sstables::sstable_id sstable_id,
                                            dht::token start_token,
                                            is_downloaded downloaded) const;

    future<> insert_snapshot_remote_location(sstring snapshot_name, sstring datacenter, sstring endpoint, sstring bucket, sstring prefix, db::consistency_level cl = db::consistency_level::EACH_QUORUM);
    future<snapshot_remote_location_entry> get_snapshot_remote_location(sstring snapshot_name, sstring datacenter, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM) const;
};

}
