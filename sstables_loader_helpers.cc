/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables_loader_helpers.hh"

#include <seastar/core/file.hh>
#include <seastar/core/units.hh>
#include <seastar/core/fstream.hh>
#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/storage.hh"
#include "utils/error_injection.hh"

// Restores a backup sstable into a table on object storage. The components do not
// go through this node: clone_from() copies them inside the object storage, or
// shares them with the backup sstable.
future<minimal_sst_info> clone_backup_sstable(replica::table& table, const sstables::shared_sstable& sstable, std::string_view snapshot_tag, logging::logger& logger) {
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
        .may_use_reference_sharing = sst_manager.get_features().sstable_reference_sharing,
        .snapshot_tag = snapshot_tag,
    });
    logger.debug("SSTable gen={} sid={} restored from sid={}: shard {}", gen, desc.sid, *source_sid, shards.front());
    co_return minimal_sst_info{shards.front(), gen, desc.version, desc.format, desc.sid, *source_sid};
}

future<sstables::entry_descriptor> stream_remote_sstable(sstables::sstable& remote, sstables::generation_type gen,
        schema_ptr dst_schema, data_dictionary::storage_options dst_opts, logging::logger& logger) {
    constexpr auto foptions = file_open_options{.extent_allocation_size_hint = 32_MiB, .sloppy_size = true};
    constexpr auto stream_options = file_output_stream_options{.buffer_size = 128_KiB, .write_behind = 10};
    auto components = remote.all_components();

    utils::get_local_injector().inject("fail_download_sstable", [] { throw std::runtime_error("Failing sstable download"); });
    co_await utils::get_local_injector().inject("pause_download_sstable", utils::wait_for_message(std::chrono::seconds(60)));

    // Move the TOC to the front to be processed first since `sstables::create_stream_sink` takes care
    // of creating behind the scene TemporaryTOC instead of usual one. This assures that in case of failure
    // this partially created SSTable will be cleaned up properly at some point.
    auto toc_it = std::ranges::find_if(components, [](const auto& component) { return component.first == component_type::TOC; });
    if (toc_it != components.begin()) {
        swap(*toc_it, components.front());
    }

    // Ensure the Scylla component is processed second.
    //
    // The sstable_sink->output() call for each component may invoke load_metadata()
    // and save_metadata(), but these functions only operate correctly if the Scylla
    // component file already exists on disk. If the Scylla component is written first,
    // load_metadata()/save_metadata() become no-ops, leaving the original Scylla
    // component (with outdated metadata) untouched.
    //
    // By placing the Scylla component second, we guarantee that:
    //   1) The first component (TOC) is written and the Scylla component file already
    //      exists on disk when subsequent output() calls happen.
    //   2) Later output() calls will overwrite the Scylla component with the correct,
    //      updated metadata.
    //
    // In short: Scylla must be written second so that all following output() calls
    // can properly update its metadata instead of silently skipping it.
    auto scylla_it = std::ranges::find_if(components, [](const auto& component) { return component.first == component_type::Scylla; });
    if (scylla_it != std::next(components.begin())) {
        swap(*scylla_it, *std::next(components.begin()));
    }

    auto files = co_await remote.readable_file_for_all_components();
    for (auto it = components.cbegin(); it != components.cend(); ++it) {
        try {
            auto descriptor = remote.get_descriptor(it->first);
            descriptor.generation = gen;
            auto sstable_sink =
                sstables::create_stream_sink(dst_schema,
                                             remote.manager(),
                                             dst_opts,
                                             sstables::sstable_state::normal,
                                             descriptor,
                                             sstables::sstable_stream_sink_cfg{.last_component = std::next(it) == components.cend(), .leave_unsealed = true});
            auto out = co_await sstable_sink->output(foptions, stream_options);

            // The components are copied as-is, including the Data component: a
            // compressed Data.db must stay compressed, so no decompressing or
            // checksumming stream is wanted, and make_source() gives the raw bytes.
            const auto fis_options = file_input_stream_options{.buffer_size = 128_KiB, .read_ahead = 2};
            input_stream<char> src(co_await remote.get_storage().make_source(remote, it->first, files.at(it->first), 0, std::numeric_limits<size_t>::max(), fis_options));

            std::exception_ptr eptr;
            try {
                co_await seastar::copy(src, out);
            } catch (...) {
                eptr = std::current_exception();
                logger.info("Error downloading SSTable component {}. Reason: {}", it->first, eptr);
            }
            co_await src.close();
            co_await out.close();
            if (eptr) {
                co_await sstable_sink->abort();
                std::rethrow_exception(eptr);
            }
            if (co_await sstable_sink->close()) {
                // The new sstable takes its sstable_id from the streamed Scylla.db, so
                // leave the descriptor's one unset.
                co_return sstables::entry_descriptor(gen, {}, descriptor.version, descriptor.format,
                        sstables::component_type::TOC, sstables::sstable_state::normal);
            }
        } catch (...) {
            logger.info("Error downloading SSTable component {}. Reason: {:t}", it->first, std::current_exception());
            throw;
        }
    }
    throw std::logic_error("SSTable must have at least one component");
}

// Restores a backup sstable into a table on filesystem.
future<minimal_sst_info> download_sstable(replica::table& table, sstables::shared_sstable sstable, logging::logger& logger) {
    auto source_sid = sstable->sstable_identifier();
    if (!source_sid) {
        on_internal_error(logger, fmt::format("Backup sstable {} has no sstable_id", sstable->get_filename()));
    }
    const auto& shards = sstable->get_shards_for_this_sstable();
    if (shards.size() != 1) {
        on_internal_error(logger, "Fully-contained sstable must belong to one shard only");
    }

    auto gen = table.get_sstable_generation_generator()();
    auto desc = co_await stream_remote_sstable(*sstable, gen, table.schema(), table.get_storage_options(), logger);
    logger.debug("SSTable gen={} sid={} downloaded from sid={}: shard {}", gen, desc.sid, *source_sid, shards.front());
    co_return minimal_sst_info{shards.front(), gen, desc.version, desc.format, desc.sid, *source_sid};
}
