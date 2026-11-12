/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables_loader_helpers.hh"

#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/storage.hh"
#include "dht/auto_refreshing_sharder.hh"

// Restores the backup sstable `sstable` into `table`. A table on object storage clones
// the components inside the object storage: they do not go through this node. A table
// on filesystem has nothing to clone there, so the components are streamed down.
static future<minimal_sst_info> download_sstable(replica::table& table, const sstables::shared_sstable& sstable, std::string_view snapshot_tag, logging::logger& logger) {
    // sstable::load() always sets the sstable_id: from Scylla.db, or from the
    // generation. So the backup sstable has an sstable_id here.
    auto source_sid = sstable->sstable_identifier();
    if (!source_sid) {
        on_internal_error(logger, fmt::format("Backup sstable {} has no sstable_id", sstable->get_filename()));
    }
    const auto& shards = sstable->get_shards_for_this_sstable();
    if (shards.size() != 1) {
        on_internal_error(logger, "Fully-contained sstable must belong to one shard only");
    }

    auto& sst_manager = table.get_sstables_manager();
    auto storage = sstables::make_storage(sst_manager, table.schema(), table.get_storage_options(), sstables::sstable_state::normal);
    auto gen = table.get_sstable_generation_generator()();
    auto desc = co_await storage->clone_from(*sstable, gen, sstables::clone_from_cfg{
        // Sharing the components of a backup sstable needs a snapshot to hold them.
        // Without a tag there is no snapshot, so there is nothing to share with.
        .may_use_reference_sharing = !snapshot_tag.empty() && sst_manager.get_features().sstable_reference_sharing,
        .snapshot_tag = snapshot_tag,
        .schema = table.schema(),
    });
    logger.debug("SSTable gen={} sid={} restored from sid={}: shard {}", gen, desc.sid, *source_sid, shards.front());
    co_return minimal_sst_info{shards.front(), gen, desc.version, desc.format, desc.sid, *source_sid};
}

future<std::vector<std::vector<minimal_sst_info>>> download_sstables(replica::table& table, const std::vector<sstables::shared_sstable>& sstables, std::string_view snapshot_tag, logging::logger& logger) {
    std::vector<std::vector<minimal_sst_info>> downloaded(this_smp_shard_count());
    // For now, tablet-aware restore doesn't need to mutate sstable level to 0
    // since we support only restoring to empty tables and so we can keep the sstable levels on backup.
    // Once we support restoring onto live tables we may want to mutate the ingested sstables' level to 0.
    co_await max_concurrent_for_each(sstables, 16, [&] (const sstables::shared_sstable& sstable) -> future<> {
        auto min_info = co_await download_sstable(table, sstable, snapshot_tag, logger);
        downloaded[min_info.shard].emplace_back(std::move(min_info));
    });
    co_return downloaded;
}

future<sstables::shared_sstable> attach_downloaded_sstable(replica::table& table, const minimal_sst_info& min_info, logging::logger& logger) {
    logger.debug("Adding downloaded SSTable gen={} to the table {}.{} on shard {}", min_info.generation, table.schema()->ks_name(), table.schema()->cf_name(), this_shard_id());
    auto& sst_manager = table.get_sstables_manager();
    auto sst = sst_manager.make_sstable(
        table.schema(), table.get_storage_options(), min_info.generation, min_info.sid, sstables::sstable_state::normal, min_info.version, min_info.format);
    sst->set_sstable_level(0);
    auto units = co_await sst_manager.dir_semaphore().get_units(1);
    // The sstable was downloaded for this shard, which is why it was submitted here,
    // so there is nothing to work out: declare this shard its owner.
    sstables::sstable_open_config cfg {
        .current_shard_as_sstable_owner = true,
        .unsealed_sstable = true,
    };
    // Unused given current_shard_as_sstable_owner, but load() wants a sharder.
    dht::auto_refreshing_sharder sharder(table.shared_from_this());
    co_await sst->load(sharder, cfg);
    // load() takes the sstable_id from Scylla.db, or from the generation, so the
    // sstable has one by now.
    if (!sst->sstable_identifier()) {
        on_internal_error(logger, fmt::format("Downloaded sstable {} has no sstable_id", sst->get_filename()));
    }
    co_await table.add_new_sstable_and_update_cache(sst, [&sst_manager, sst] (sstables::shared_sstable loading_sst) -> future<> {
        if (loading_sst == sst) {
            auto writer_cfg = sst_manager.configure_writer(loading_sst->get_origin());
            co_await loading_sst->seal_sstable(writer_cfg.backup);
        }
    });
    co_return sst;
}
