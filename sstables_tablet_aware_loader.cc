/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstables_tablet_aware_loader.hh"
#include "db/system_distributed_keyspace.hh"
#include "replica/database.hh"
#include "replica/distributed_loader.hh"
#include "replica/global_table_ptr.hh"
#include "service/storage_service.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "tablet_aware_loader.hh"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/units.hh>

static logging::logger tall("sstables_tablet_aware_loader");


future<sstables_tablet_aware_loader::minimal_sst_info> sstables_tablet_aware_loader::download_sstable(sstables::shared_sstable sstable) const {
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

    auto& db = _loader.get_db().local();
    auto& table = db.find_column_family(_keyspace, _table);
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
                auto permit = co_await _loader.get_db().local().obtain_reader_permit(table, "download_fully_contained_sstables", db::no_timeout, {});
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
                tall.debug("SSTable shards {}", fmt::join(shards, ", "));
                co_return minimal_sst_info{shards.front(), gen, descriptor.version, descriptor.format};
            }
        } catch (...) {
            tall.info("Error downloading SSTable component {}. Reason: {}", it->first, std::current_exception());
            throw;
        }
    }
    throw std::logic_error("SSTable must have at least one component");
}

future<utils::chunked_vector<db::sstable_info>> sstables_tablet_aware_loader::get_owned_sstables(utils::chunked_vector<db::sstable_info> sst_infos) const {
    const auto global_table = co_await replica::get_table_on_all_shards(_loader.get_db(), _keyspace, _table);
    const auto table_id = global_table->schema()->id();

    auto erm = co_await ([this, &table_id] -> future<locator::effective_replication_map_ptr> {
        locator::effective_replication_map_ptr erm;
        while (true) {
            auto& t = _loader.get_db().local().find_column_family(table_id);
            erm = t.get_effective_replication_map();
            auto expected_topology_version = erm->get_token_metadata().get_version();
            auto& ss = _loader.get_storage_service().local();

            // The awaiting only works with raft enabled, and we only need it with tablets,
            // so let's bypass the awaiting when tablet is disabled.
            if (!t.uses_tablets()) {
                break;
            }
            // optimistically attempt to grab an erm on quiesced topology
            if (co_await ss.verify_topology_quiesced(expected_topology_version)) {
                break;
            }
            erm = nullptr;
            co_await ss.await_topology_quiesced();
        }

        co_return std::move(erm);
    }());

    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(table_id);
    auto tablet_in_node_scope = [&erm, &tablet_map](locator::tablet_id tid) {
        const auto& topo = erm->get_topology();
        return std::ranges::any_of(tablet_map.get_tablet_info(tid).replicas, [&topo](const auto& r) { return topo.is_me(r.host); });
    };
    auto tablets_in_scope = tablet_map.tablet_ids() | std::views::filter([&](auto tid) { return tablet_in_node_scope(tid); }) |
                            std::views::transform([&tablet_map](auto tid) { return tablet_map.get_token_range(tid); });

    utils::chunked_vector<db::sstable_info> ret_val;
    for (const auto& tablet_range : tablets_in_scope) {
        for (const auto& sst : sst_infos) {

            // SSTable entirely after tablet -> no further SSTables (larger keys) can overlap
            if (tablet_range.after(sst.first_token, dht::token_comparator{})) {
                break;
            }
            // SSTable entirely before tablet -> skip and continue scanning later (larger keys)
            if (tablet_range.before(sst.last_token, dht::token_comparator{})) {
                continue;
            }

            if (tablet_range.contains(dht::token_range{sst.first_token, sst.last_token}, dht::token_comparator{})) {
                ret_val.push_back(sst);
            } else {
                throw std::logic_error("sstables_partially_contained");
            }
            co_await coroutine::maybe_yield();
        }
    }
    co_return std::move(ret_val);
}

future<std::vector<std::vector<sstables_tablet_aware_loader::minimal_sst_info>>> sstables_tablet_aware_loader::get_snapshot_sstables() const {
    tall.debug("Getting snapshot '{}' SSTables for keyspace {}, table {}, data center {}, rack {}", _snapshot, _keyspace, _table, _data_center, _rack);
    auto sst_infos = co_await _loader.get_systesm_distributed_keyspace().local().get_snapshot_sstables(_snapshot, _keyspace, _table, _data_center, _rack);
    tall.debug("{} SSTables found for snapshot {}", sst_infos.size(), _snapshot);
    if (sst_infos.empty()) {
        on_internal_error(tall,
                          format("No SSTables found in system_distributed.snapshot_sstables for snapshot {}, keyspace {}, table {}, data center {}, rack {}",
                                 _snapshot,
                                 _keyspace,
                                 _table,
                                 _data_center,
                                 _rack));
    }

    auto owned_sstables = co_await get_owned_sstables(sst_infos);
    tall.debug("{} SSTables owned by this node tablets", owned_sstables.size());
    if (owned_sstables.empty()) {
        on_internal_error(tall,
                          format("No SSTables belonging to the node's tablets were found for snapshot: {}, keyspace: {}, table: {}, data center: {}, rack: {}",
                                 _snapshot,
                                 _keyspace,
                                 _table,
                                 _data_center,
                                 _rack));
    }

    auto ep_type = _loader.get_storage_manager().get_endpoint_type(_endpoint);
    auto [_, sstables_on_shards] = co_await replica::distributed_loader::get_sstables_from_object_store(
        _loader.get_db(),
        _keyspace,
        _table,
        owned_sstables | std::views::transform([](const auto& sst) { return sst.toc_name; }) | std::ranges::to<std::vector>(),
        _endpoint,
        ep_type,
        _bucket,
        owned_sstables.front().prefix,
        sstables::sstable_open_config{
            .load_bloom_filter = false,
        },
        [&] {
            // TODO
            return nullptr;
        });
    std::vector<std::vector<minimal_sst_info>> min_infos(smp::count);
    co_await smp::invoke_on_all([this, &sstables_on_shards, &min_infos] -> future<> {
        auto sst_chunk = std::move(sstables_on_shards[this_shard_id()]);
        co_await max_concurrent_for_each(sst_chunk, 16, [this, &min_infos](const auto& sst) -> future<> {
            auto min_info = co_await download_sstable(sst);
            min_infos[min_info.shard].emplace_back(std::move(min_info));
        });
    });

    co_return min_infos;
}

future<> sstables_tablet_aware_loader::attach_sstable(shard_id from_shard, const minimal_sst_info& min_info) const {
    tall.debug("Adding downloaded SSTables to the table {} on shard {}, submitted from shard {}", _table, this_shard_id(), from_shard);
    auto& db = _loader.get_db().local();
    auto& table = db.find_column_family(_keyspace, _table);
    auto& sst_manager = table.get_sstables_manager();
    auto sst = sst_manager.make_sstable(
        table.schema(), table.get_storage_options(), min_info.generation, sstables::sstable_state::normal, min_info.version, min_info.format);
    sst->set_sstable_level(0);
    co_await sst->load(table.get_effective_replication_map()->get_sharder(*table.schema()));
    co_await table.add_sstable_and_update_cache(sst);
}

sstables_tablet_aware_loader::sstables_tablet_aware_loader(tablet_aware_loader& loader,
                                                           const std::string& snapshot,
                                                           const std::string& data_center,
                                                           const std::string& rack,
                                                           const std::string& ks_name,
                                                           const std::string& cf_name,
                                                           const std::string& endpoint,
                                                           const std::string& bucket,
                                                           abort_source& as)
    : _snapshot(snapshot)
    , _data_center(data_center)
    , _rack(rack)
    , _keyspace(ks_name)
    , _table(cf_name)
    , _endpoint(endpoint)
    , _bucket(bucket)
    , _as(as)
    , _loader(loader) {
}

future<> sstables_tablet_aware_loader::load_snapshot_sstables() {
    try {
        auto downloaded_ssts = co_await get_snapshot_sstables();

        co_await smp::invoke_on_all([this, &downloaded_ssts, from = this_shard_id()] -> future<> {
            auto shard_ssts = std::move(downloaded_ssts[this_shard_id()]);
            co_await max_concurrent_for_each(shard_ssts, 16, [this, from](const auto& min_info) -> future<> { co_await attach_sstable(from, min_info); });
        });
    } catch (...) {
        tall.info("Error loading snapshot SSTables. Reason: {}", std::current_exception());
        throw;
    }
}
