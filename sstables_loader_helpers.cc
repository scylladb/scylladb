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

// A failure is logged and does not stop the removal of the rest, nor hide the error that caused it.
// Returns whether the component is gone: a missing one counts.
static future<bool> unlink_downloaded_component(replica::table& table, const sstables::shared_sstable& source, sstables::component_type type,
        sstables::generation_type gen, logging::logger& logger) {
    try {
        auto descriptor = source->get_descriptor(type);
        descriptor.generation = gen;
        co_return co_await sstables::create_stream_sink(table.schema(), table.get_sstables_manager(), table.get_storage_options(),
                sstables::sstable_state::normal, descriptor, sstables::sstable_stream_sink_cfg{.leave_unsealed = true})->abort();
    } catch (...) {
        logger.warn("Failed to remove component {} of a downloaded SSTable gen={}: {}", type, gen, std::current_exception());
        co_return false;
    }
}

future<> remove_downloaded_sstable(replica::table& table, const sstables::shared_sstable& source, sstables::generation_type gen, logging::logger& logger) {
    // The TemporaryTOC goes last, and stays if a component is left: the next boot refuses orphans but cleans up a TemporaryTOC.
    bool all_removed = true;
    for (const auto& [type, name] : source->all_components()) {
        if (type != sstables::component_type::TOC) {
            all_removed &= co_await unlink_downloaded_component(table, source, type, gen, logger);
        }
    }
    if (!all_removed) {
        logger.warn("Keeping the TemporaryTOC of a downloaded SSTable gen={}: a component could not be removed", gen);
        co_return;
    }
    co_await unlink_downloaded_component(table, source, sstables::component_type::TOC, gen, logger);
}

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
    auto desc = co_await storage->clone_from(*sstable, gen, sst_manager.get_features().sstable_reference_sharing, snapshot_tag);
    logger.debug("SSTable gen={} sid={} restored from sid={}: shard {}", gen, desc.sid, *source_sid, shards.front());
    co_return minimal_sst_info{shards.front(), gen, desc.version, desc.format, desc.sid, *source_sid};
}

future<minimal_sst_info> download_sstable(replica::table& table, sstables::shared_sstable sstable, logging::logger& logger, seastar::abort_source* as) {
    constexpr auto foptions = file_open_options{.extent_allocation_size_hint = 32_MiB, .sloppy_size = true};
    constexpr auto stream_options = file_output_stream_options{.buffer_size = 128_KiB, .write_behind = 10};
    auto components = sstable->all_components();

    co_await utils::get_local_injector().inject("pause_download_sstable", utils::wait_for_message(std::chrono::seconds(60)));
    utils::get_local_injector().inject("fail_download_sstable", [] { throw std::runtime_error("Failing sstable download"); });

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

    auto source_sid = sstable->sstable_identifier();
    if (!source_sid) {
        on_internal_error(logger, fmt::format("Backup sstable {} has no sstable_id", sstable->get_filename()));
    }

    auto gen = table.get_sstable_generation_generator()();
    auto files = co_await sstable->readable_file_for_all_components();
    for (auto it = components.cbegin(); it != components.cend(); ++it) {
        std::exception_ptr failure;
        // The component's file may exist, and the sink has not removed it yet: output() can fail after creating it.
        bool leftover = false;
        bool all_removed = true;
        try {
            if (as) {
                as->check();
            }
            if (it != components.cbegin()) {
                utils::get_local_injector().inject("fail_download_sstable_after_first_component", [] { throw std::runtime_error("Failing sstable download after its first component"); });
            }
            auto descriptor = sstable->get_descriptor(it->first);
            descriptor.generation = gen;
            auto sstable_sink =
                sstables::create_stream_sink(table.schema(),
                                             table.get_sstables_manager(),
                                             table.get_storage_options(),
                                             sstables::sstable_state::normal,
                                             descriptor,
                                             sstables::sstable_stream_sink_cfg{.last_component = std::next(it) == components.cend(), .leave_unsealed = true});
            leftover = true;
            auto out = co_await sstable_sink->output(foptions, stream_options);

            // The components are copied as-is, including the Data component: a
            // compressed Data.db must stay compressed, so no decompressing or
            // checksumming stream is wanted, and make_source() gives the raw bytes.
            const auto fis_options = file_input_stream_options{.buffer_size = 128_KiB, .read_ahead = 2};
            input_stream<char> src(co_await sstable->get_storage().make_source(*sstable, it->first, files.at(it->first), 0, std::numeric_limits<size_t>::max(), fis_options));

            std::exception_ptr eptr;
            try {
                // Not seastar::copy(), so that an abort of the restore stops the copy at the next buffer.
                for (;;) {
                    auto buf = co_await src.read();
                    if (buf.empty()) {
                        break;
                    }
                    if (as) {
                        as->check();
                    }
                    // Copied into the stream's own buffer, as seastar::copy() does: the file sink wants aligned writes.
                    co_await out.write(buf.get(), buf.size());
                }
            } catch (...) {
                eptr = std::current_exception();
                // The siblings of a failed download are aborted, and that is not worth a line each.
                if (!(as && as->abort_requested())) {
                    logger.info("Error downloading SSTable component {}. Reason: {}", it->first, eptr);
                }
            }
            co_await src.close();
            co_await out.close();
            if (eptr) {
                all_removed = co_await sstable_sink->abort();
                leftover = false;
                std::rethrow_exception(eptr);
            }
            if (auto sst = co_await sstable_sink->close()) {
                const auto& shards = sstable->get_shards_for_this_sstable();
                if (shards.size() != 1) {
                    on_internal_error(logger, "Fully-contained sstable must belong to one shard only");
                }
                logger.debug("SSTable gen={} sid={}: shards {}", gen, sst->sstable_identifier(), fmt::join(shards, ", "));
                co_return minimal_sst_info{shards.front(), gen, descriptor.version, descriptor.format, std::nullopt, *source_sid};
            }
        } catch (...) {
            if (as && as->abort_requested()) {
                logger.debug("SSTable component {} not downloaded, the download was aborted", it->first);
            } else {
                logger.info("Error downloading SSTable component {}. Reason: {:t}", it->first, std::current_exception());
            }
            failure = std::current_exception();
        }
        if (failure) {
            // Nothing partial stays behind: remove what was written, the TemporaryTOC, which is first, last.
            if (leftover) {
                all_removed &= co_await unlink_downloaded_component(table, sstable, it->first, gen, logger);
            }
            for (auto jt = it; jt != components.cbegin(); --jt) {
                const auto type = std::prev(jt)->first;
                // Without the TemporaryTOC a component left behind would be an orphan, which the next boot refuses.
                if (type == component_type::TOC && !all_removed) {
                    logger.warn("Keeping the TemporaryTOC of a downloaded SSTable gen={}: a component could not be removed", gen);
                    break;
                }
                all_removed &= co_await unlink_downloaded_component(table, sstable, type, gen, logger);
            }
            std::rethrow_exception(failure);
        }
    }
    throw std::logic_error("SSTable must have at least one component");
}
