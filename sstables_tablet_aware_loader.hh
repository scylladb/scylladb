/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "locator/abstract_replication_strategy.hh"
#include "sstables/generation_type.hh"
#include "sstables/shared_sstable.hh"

namespace db {
struct sstable_info;
}
class tablet_aware_loader;

class sstables_tablet_aware_loader {
    struct minimal_sst_info {
        shard_id shard;
        sstables::generation_type generation;
        sstables::sstable_version_types version;
        sstables::sstable_format_types format;
    };
    std::string _snapshot;
    std::string _data_center;
    std::string _rack;
    std::string _keyspace;
    std::string _table;

    std::string _endpoint;
    std::string _bucket;

    [[maybe_unused]]abort_source& _as;
    tablet_aware_loader& _loader;

    future<minimal_sst_info> download_sstable(sstables::shared_sstable sstable) const;
    future<utils::chunked_vector<db::sstable_info>> get_owned_sstables(utils::chunked_vector<db::sstable_info> sst_infos) const;
    future<std::vector<std::vector<minimal_sst_info>>> get_snapshot_sstables() const;
    future<> attach_sstable(shard_id from_shard, const minimal_sst_info& min_info) const;

public:
    sstables_tablet_aware_loader(tablet_aware_loader& loader,
                                 const std::string& snapshot,
                                 const std::string& data_center,
                                 const std::string& rack,
                                 const std::string& ks_name,
                                 const std::string& cf_name,
                                 const std::string& endpoint,
                                 const std::string& bucket,
                                 abort_source& as);

    future<> load_snapshot_sstables();
};
