/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstables_loader_helpers.hh"

#include <seastar/core/file.hh>
#include <seastar/core/units.hh>
#include <seastar/core/fstream.hh>
#include "replica/database.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"

future<minimal_sst_info> download_sstable(replica::database& db, replica::table& table, sstables::shared_sstable sstable, logging::logger& logger) {
    constexpr auto foptions = file_open_options{.extent_allocation_size_hint = 32_MiB, .sloppy_size = true};
    constexpr auto stream_options = file_output_stream_options{.buffer_size = 128_KiB, .write_behind = 10};
    auto components = sstable->all_components();

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

    auto gen = table.get_sstable_generation_generator()();
    auto files = co_await sstable->readable_file_for_all_components();
    for (auto it = components.cbegin(); it != components.cend(); ++it) {
        try {
            auto descriptor = sstable->get_descriptor(it->first);
            auto sstable_sink =
                sstables::create_stream_sink(table.schema(),
                                             table.get_sstables_manager(),
                                             table.get_storage_options(),
                                             sstables::sstable_state::normal,
                                             sstables::sstable::component_basename(
                                                 table.schema()->ks_name(), table.schema()->cf_name(), descriptor.version, gen, descriptor.format, it->first),
                                             sstables::sstable_stream_sink_cfg{.last_component = std::next(it) == components.cend()});
            auto out = co_await sstable_sink->output(foptions, stream_options);

            input_stream src(co_await [&it, sstable, f = files.at(it->first), &db, &table]() -> future<input_stream<char>> {
                const auto fis_options = file_input_stream_options{.buffer_size = 128_KiB, .read_ahead = 2};

                if (it->first != sstables::component_type::Data) {
                    co_return input_stream<char>(
                        co_await sstable->get_storage().make_source(*sstable, it->first, f, 0, std::numeric_limits<size_t>::max(), fis_options));
                }
                auto permit = co_await db.obtain_reader_permit(table, "download_fully_contained_sstables", db::no_timeout, {});
                co_return co_await (
                    sstable->get_compression()
                        ? sstable->data_stream(0, sstable->ondisk_data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::yes)
                        : sstable->data_stream(0, sstable->data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::no));
            }());

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
            if (auto sst = co_await sstable_sink->close()) {
                const auto& shards = sstable->get_shards_for_this_sstable();
                if (shards.size() != 1) {
                    on_internal_error(logger, "Fully-contained sstable must belong to one shard only");
                }
                logger.debug("SSTable shards {}", fmt::join(shards, ", "));
                co_return minimal_sst_info{shards.front(), gen, descriptor.version, descriptor.format};
            }
        } catch (...) {
            logger.info("Error downloading SSTable component {}. Reason: {}", it->first, std::current_exception());
            throw;
        }
    }
    throw std::logic_error("SSTable must have at least one component");
}
