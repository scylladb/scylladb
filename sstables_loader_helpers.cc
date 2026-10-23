/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables_loader_helpers.hh"

#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/storage.hh"

// Restores the backup sstable `sstable` into `table`. A table on object storage clones
// the components inside the object storage: they do not go through this node. A table
// on filesystem has nothing to clone there, so the components are streamed down.
future<minimal_sst_info> download_sstable(replica::table& table, const sstables::shared_sstable& sstable, std::string_view snapshot_tag, logging::logger& logger) {
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
