/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/system_distributed_keyspace.hh"
#include "replica/database.hh"
#include "replica/distributed_loader.hh"
#include "replica/global_table_ptr.hh"
#include "service/storage_service.hh"
#include "sstables_loader.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/units.hh>

static logging::logger tall("sstables_tablet_aware_loader");

struct minimal_sst_info {
    shard_id shard;
    sstables::generation_type generation;
    sstables::sstable_version_types version;
    sstables::sstable_format_types format;
};

future<minimal_sst_info> sstables_loader::download_sstable(table_id tid, sstables::shared_sstable sstable) const {
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

    auto& db = _db.local();
    auto& table = db.find_column_family(tid);
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

            input_stream src(co_await [this, &it, sstable, f = files.at(it->first), &table]() -> future<input_stream<char>> {
                const auto fis_options = file_input_stream_options{.buffer_size = 128_KiB, .read_ahead = 2};

                if (it->first != sstables::component_type::Data) {
                    co_return input_stream<char>(
                        co_await sstable->get_storage().make_source(*sstable, it->first, f, 0, std::numeric_limits<size_t>::max(), fis_options));
                }
                auto permit = co_await _db.local().obtain_reader_permit(table, "download_fully_contained_sstables", db::no_timeout, {});
                co_return co_await (
                    sstable->get_compression()
                        ? sstable->data_stream(0, sstable->ondisk_data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::yes)
                        : sstable->data_stream(0, sstable->data_size(), std::move(permit), nullptr, nullptr, sstables::sstable::raw_stream::no));
            }());

            std::exception_ptr eptr;
            try {
                co_await copy(src, out);
            } catch (...) {
                eptr = std::current_exception();
                tall.info("Error downloading SSTable component {}. Reason: {}", it->first, eptr);
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
                    on_internal_error(tall, "Fully-contained sstable must belong to one shard only");
                }
                tall.debug("SSTable shards {} for {} (from {})", fmt::join(shards, ", "), sst->toc_filename(), sstable->toc_filename());
                co_return minimal_sst_info{shards.front(), gen, descriptor.version, descriptor.format};
            }
        } catch (...) {
            tall.info("Error downloading SSTable component {}. Reason: {}", it->first, std::current_exception());
            throw;
        }
    }
    throw std::logic_error("SSTable must have at least one component");
}

future<utils::chunked_vector<db::sstable_info>> sstables_loader::get_owned_sstables(locator::global_tablet_id tid, utils::chunked_vector<db::sstable_info> sst_infos) const {
    const auto& tablet_map = _db.local().get_token_metadata().tablets().get_tablet_map(tid.table);
    auto tablet_range = tablet_map.get_token_range(tid.tablet);

    tall.debug("Restoring tablet range {} for tablet {}", tablet_range, tid);
    utils::chunked_vector<db::sstable_info> ret_val;
    {
        for (const auto& sst : sst_infos) {
            tall.debug("Checking {} {}/{} sstable tokens", sst.sstable_id, sst.first_token, sst.last_token);

            // SSTable entirely after tablet -> no further SSTables (larger keys) can overlap
            if (tablet_range.after(sst.first_token, dht::token_comparator{})) {
                tall.debug("Sstable {} is after tablet range", sst.sstable_id);
                break;
            }
            // SSTable entirely before tablet -> skip and continue scanning later (larger keys)
            if (tablet_range.before(sst.last_token, dht::token_comparator{})) {
                tall.debug("Sstable {} is before tablet range", sst.sstable_id);
                continue;
            }

            if (tablet_range.contains(dht::token_range{sst.first_token, sst.last_token}, dht::token_comparator{})) {
                ret_val.push_back(sst);
            } else {
                tall.debug("Sstable {} is partially contained", sst.sstable_id);
                throw std::logic_error("sstables_partially_contained");
            }
            co_await coroutine::maybe_yield();
        }
    }
    co_return std::move(ret_val);
}

future<std::vector<std::vector<minimal_sst_info>>> sstables_loader::get_snapshot_sstables(locator::global_tablet_id tid, sstring snap_name, sstring endpoint, sstring bucket) const {
    const auto& topo = _db.local().get_token_metadata().get_topology();
    auto s = _db.local().find_schema(tid.table);
    auto sst_infos = co_await _sys_dist_ks.get_snapshot_sstables(snap_name, s->ks_name(), s->cf_name(), topo.get_datacenter(), topo.get_rack());
    tall.debug("{} SSTables found", sst_infos.size(), tid);
    if (sst_infos.empty()) {
        throw std::runtime_error(format("No SSTables found in system_distributed.snapshot_sstables for {}", snap_name));
    }

    auto owned_sstables = co_await get_owned_sstables(tid, sst_infos);
    tall.debug("{} SSTables owned by this node tablets", owned_sstables.size());
    if (owned_sstables.empty()) {
        // It can happen that a tablet exists and contains no data. Just skip it
        co_return std::vector<std::vector<minimal_sst_info>>(smp::count);
    }

    auto ep_type = _storage_manager.get_endpoint_type(endpoint);
    auto [_, sstables_on_shards] = co_await replica::distributed_loader::get_sstables_from_object_store(
        _db,
        s->ks_name(),
        s->cf_name(),
        owned_sstables | std::views::transform([](const auto& sst) { return sst.toc_name; }) | std::ranges::to<std::vector>(),
        endpoint,
        ep_type,
        bucket,
        owned_sstables.front().prefix,
        sstables::sstable_open_config{
            .load_bloom_filter = false,
        },
        [&] {
            // TODO
            return nullptr;
        });

    co_return co_await container().map_reduce0(
        [this, tid, &sstables_on_shards](auto&) -> future<std::vector<std::vector<minimal_sst_info>>> {
            auto sst_chunk = std::move(sstables_on_shards[this_shard_id()]);
            std::vector<std::vector<minimal_sst_info>> local_min_infos(smp::count);
            co_await max_concurrent_for_each(sst_chunk, 16, [this, tid, &local_min_infos](const auto& sst) -> future<> {
                auto min_info = co_await download_sstable(tid.table, sst);
                local_min_infos[min_info.shard].emplace_back(std::move(min_info));
            });
            co_return local_min_infos;
        },
        std::vector<std::vector<minimal_sst_info>>(smp::count),
        [](auto init, auto&& item) -> std::vector<std::vector<minimal_sst_info>> {
            for (std::size_t i = 0; i < item.size(); ++i) {
                init[i].append_range(std::move(item[i]));
            }
            return init;
        });
}

future<> sstables_loader::attach_sstable(table_id tid, const minimal_sst_info& min_info) const {
    auto& db = _db.local();
    auto& table = db.find_column_family(tid);
    tall.debug("Adding downloaded SSTables to the table {} on shard {}", table.schema()->cf_name(), this_shard_id());
    auto& sst_manager = table.get_sstables_manager();
    auto sst = sst_manager.make_sstable(
        table.schema(), table.get_storage_options(), min_info.generation, sstables::sstable_state::normal, min_info.version, min_info.format);
    sst->set_sstable_level(0);
    auto erm = table.get_effective_replication_map();
    co_await sst->load(erm->get_sharder(*table.schema()));
    co_await table.add_sstable_and_update_cache(sst);
    co_return;
}

future<> sstables_loader::load_snapshot_sstables(locator::global_tablet_id tid, sstring snap_name, sstring endpoint, sstring bucket) {
    try {
        auto downloaded_ssts = co_await get_snapshot_sstables(tid, snap_name, endpoint, bucket);
        co_await smp::invoke_on_all([this, tid, &downloaded_ssts] -> future<> {
            auto shard_ssts = std::move(downloaded_ssts[this_shard_id()]);
            co_await max_concurrent_for_each(shard_ssts, 16, [this, tid](const auto& min_info) -> future<> { co_await attach_sstable(tid.table, min_info); });
        });
    } catch (...) {
        tall.info("Error loading snapshot SSTables. Reason: {}", std::current_exception());
        throw;
    }
}

future<> sstables_loader::abort_loading_sstables(locator::global_tablet_id tid) {
    co_return; // FIXME -- implement
}
