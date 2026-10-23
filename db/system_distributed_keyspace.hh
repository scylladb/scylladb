/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "snapshot_types.hh"
#include "db_clock.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "db/consistency_level_type.hh"
#include "locator/host_id.hh"
#include "dht/token.hh"
#include "sstables/types.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <chrono>
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

namespace replica {
    class table;
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

    /* This table is used by the backup and restore code to store snapshot metadata. */
    static constexpr auto SNAPSHOTS = "snapshots";

    /* This table is used by the backup and restore code to store snapshot keyspaces. */
    static constexpr auto SNAPSHOT_KEYSPACES = "snapshot_keyspaces";

    /* This table is used by the backup and restore code to store snapshot tables. */
    static constexpr auto SNAPSHOT_TABLES = "snapshot_tables";

    /* This table is used by the backup and restore code to store snapshot tablets. */
    static constexpr auto SNAPSHOT_TABLETS = "snapshot_tablets";

    /* This table is used by the backup and restore code to store snapshot nodes. */
    static constexpr auto SNAPSHOT_NODES = "snapshot_nodes";

    /* This table is used by the backup and restore code to store per-sstable metadata.
     * The data the coordinator node puts in this table comes from the snapshot manifests. */
    static constexpr auto SNAPSHOT_SSTABLES = "snapshot_sstables";

    /* This table is used by the backup and restore code to store snapshot
     * remote location metadata per datacenter. */
    static constexpr auto SNAPSHOT_REMOTE_LOCATIONS = "snapshot_remote_locations";

    /* This table is used by the Alternator export to S3 code to store the metadata of each
     * export, keyed by its ARN. The node driving an export writes its progress here, and
     * DescribeExport/ListExports read from it. */
    static constexpr auto ALTERNATOR_EXPORT_TO_S3_EXPORTS = "alternator_export_to_s3_exports";

    /* This table is used by the Alternator export to S3 code to make ExportTableToPointInTime
     * idempotent: it maps the client token of a request to the export which that request
     * created. The node accepting a request writes the token here and later looks it up to
     * detect a repeated request. */
    static constexpr auto ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS = "alternator_export_to_s3_client_tokens";

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

/* Writes of the two Alternator export to S3 tables (see their schemas above). Alternator reads
 * them through storage_proxy, but the writes are conditional, which is one CQL statement each
 * and a fair amount of hand-written code otherwise, so they are made here, through the
 * query_processor system_distributed_keyspace already holds - as snapshot_table_helper below
 * does for the backup tables. The scan of the client token table is here too, next to the write
 * of that table. */
class alternator_export_table_helper {
    cql3::query_processor& _qp;
public:
    /* A row of alternator_export_to_s3_exports. `node_id` is kept as stored - its meaning is
     * Alternator's (see live_node_identifier in alternator/export.cc). */
    struct export_row {
        sstring export_arn;
        sstring client_token;
        sstring request;
        ::table_id table_id;
        std::optional<sstring> export_manifest;
        sstring export_status;
        std::optional<sstring> failure_code;
        std::optional<sstring> failure_message;
        std::optional<int64_t> item_count;
        std::optional<int64_t> billed_size_bytes;
        sstring export_id_token;
        db_clock::time_point export_time;
        db_clock::time_point accepted_at;
        std::optional<db_clock::time_point> completed_at;
        sstring node_id;
    };

    /* A row of alternator_export_to_s3_client_tokens. */
    struct client_token_row {
        sstring client_token;
        sstring export_arn;
        sstring request;
        sstring node_id;
    };

    explicit alternator_export_table_helper(cql3::query_processor&);

    /* Inserts an export row unless one with the same ARN already exists; returns whether it was
     * inserted. The row is written with `ttl`, which is what makes it expire: an UPDATE cannot
     * rewrite the row marker, so a row inserted without a TTL would outlive the TTL of every
     * update of it, as a row with nothing but its key in it. */
    future<bool> insert_export(const export_row&, std::chrono::seconds ttl);

    /* Rewrites every non-key column of an export row, but only if the row is still in state
     * `expected_export_status` and still owned by `expected_node_id`; returns whether it was.
     * `ttl`, when given, applies to the columns written - which is why all of them are. */
    future<bool> update_export(const export_row&, const sstring& expected_export_status, const sstring& expected_node_id,
            std::optional<std::chrono::seconds> ttl = std::nullopt);

    /* Inserts a client token row unless one with the same token already exists; returns whether
     * it was inserted. The table's default TTL applies. */
    future<bool> insert_client_token(const client_token_row&);

    /* Returns every client token row there is, in no particular order. `as`, when given, aborts
     * the scan - it reads the whole table, so a caller which can be stopped needs a way to cut
     * it short. */
    future<utils::chunked_vector<client_token_row>> get_all_client_tokens(const seastar::abort_source* as = nullptr) const;
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
     * `toc_name`, and `prefix`. Uses consistency level `QUORUM` by default. */
    future<utils::chunked_vector<snapshot_sstable_entry>> get_snapshot_sstables(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl = db::consistency_level::QUORUM, std::optional<dht::token> start_token = std::nullopt, std::optional<dht::token> end_token = std::nullopt) const;

    /* Retrieves download progress for a given snapshot, keyspace, table, datacenter, and rack */
    future<snapshot_sstables_progress> get_snapshot_sstables_progress(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl = db::consistency_level::QUORUM) const;

    future<> update_sstable_download_status(sstring snapshot_name,
                                            sstring ks,
                                            sstring table,
                                            sstring dc,
                                            sstring rack,
                                            sstables::sstable_id sstable_id,
                                            dht::token start_token,
                                            is_downloaded downloaded) const;

    future<> insert_snapshot_remote_location(sstring snapshot_name, sstring datacenter, sstring endpoint, sstring bucket, sstring prefix, snapshot_state, db::consistency_level cl = db::consistency_level::EACH_QUORUM);
    future<snapshot_remote_location_entry> get_snapshot_remote_location(sstring snapshot_name, sstring datacenter, db::consistency_level cl = db::consistency_level::QUORUM) const;

    /* Inserts multiple SSTable entries for a given snapshot, keyspace, table, datacenter,
     * and rack. 
     */
    future<> insert_snapshot_sstables(std::string_view snapshot_name, std::string_view ks, std::string_view table, std::string_view dc, std::string_view rack
        , std::span<const snapshot_sstable_entry> sstables
        , db::consistency_level cl = db::consistency_level::EACH_QUORUM
    );

    future<> insert_snapshot_sstables(std::string_view snapshot_name, std::string_view ks, std::string_view table, std::string_view dc, std::string_view rack
        , const utils::chunked_vector<snapshot_sstable_entry>& sstables
        , db::consistency_level cl = db::consistency_level::EACH_QUORUM
    );

    /**
     * Inserts a snapshot into system.dist table
     */
    future<> insert_snapshot(const snapshot_entry&, db::consistency_level cl = db::consistency_level::EACH_QUORUM);
    /**
     * Find a snapshot entry. 
     */
    future<std::optional<snapshot_entry>> get_snapshot(std::string_view snapshot_name, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM);

    /**
     * Add backup locations to a snapshot
     */
    future<> insert_snapshot_remote_locations(std::span<const snapshot_remote_location_entry> keyspaces, db::consistency_level cl = db::consistency_level::EACH_QUORUM);

    /**
     * Get all remote locations for snapshot
     */
    future<utils::chunked_vector<snapshot_remote_location_entry>> get_snapshot_remote_locations(std::string_view snapshot_name, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM);

    /**
     * Add keyspaces to a snapshot
     */
    future<> insert_snapshot_keyspaces(std::span<const snapshot_keyspace_entry> keyspaces, db::consistency_level cl = db::consistency_level::EACH_QUORUM);

    /**
     * Get all keyspaces in snapshot
     */
    future<utils::chunked_vector<snapshot_keyspace_entry>> get_snapshot_keyspaces(std::string_view snapshot_name, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM);

    /**
     * Add tables to a snapshot
     */
    future<> insert_snapshot_tables(std::span<const snapshot_table_entry> tables, db::consistency_level cl = db::consistency_level::EACH_QUORUM);

    /**
     * Helper to build a snapshot_table_entry describing the current schema of the given table
     */
    snapshot_table_entry make_snapshot_table_entry(std::string_view snapshot_name, const replica::table& table) const;

    /**
     * Get all tables in snapshot, optionally restricted by keyspace
     */
    future<utils::chunked_vector<snapshot_table_entry>> get_snapshot_tables(std::string_view snapshot_name, std::string_view keyspace = {}, std::string_view table = {}, db::consistency_level cl = db::consistency_level::LOCAL_QUORUM);

    /**
     * Add tablets to a snapshot
     */
    future<> insert_snapshot_tablets(std::string_view snapshot_name
        , std::string_view keyspace, std::string_view table, std::string_view datacenter
        , std::span<const snapshot_tablet_entry> tables
        , db::consistency_level cl = db::consistency_level::EACH_QUORUM
    );

    /**
     * Get all tablets in snapshot for a given keyspace + table in datacenter
     * TODO: query by range?
     */
    future<utils::chunked_vector<snapshot_tablet_entry>> get_snapshot_tablets(std::string_view snapshot_name
        , std::string_view keyspace, std::string_view table, std::string_view datacenter
        , db::consistency_level cl = db::consistency_level::LOCAL_QUORUM
    );

    future<> insert_snapshot_nodes(std::string_view snapshot_name
        , std::span<const snapshot_node_entry> nodes
        , db::consistency_level cl = db::consistency_level::EACH_QUORUM
    );

    /**
     * Get all nodes in snapshot for a given datacenter and optionally rack
     */
    future<utils::chunked_vector<snapshot_node_entry>> get_snapshot_nodes(std::string_view snapshot_name
        , std::string_view datacenter = {}
        , std::string_view rack = {}
        , db::consistency_level cl = db::consistency_level::LOCAL_QUORUM
    );

    /**
     * Helper to write a full snapshot_entries
     */
    future<> insert_snapshot_entries(std::string_view snapshot_name, std::string_view keyspace
        , std::string_view table, std::string_view datacenter, std::string_view rack, locator::host_id
        , const snapshot_entries& entries
        , db::consistency_level cl = db::consistency_level::LOCAL_QUORUM
    );

    /**
     * Helper to write snapshot base metadata
     */
    future<> insert_snapshot_info(std::string_view snapshot_name
        , db_clock::time_point created, db_clock::time_point expiry
        , std::span<const lw_shared_ptr<replica::table>> tables
        , db::consistency_level cl = db::consistency_level::LOCAL_QUORUM
    );

};

}
