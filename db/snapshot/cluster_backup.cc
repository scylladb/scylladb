/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <array>

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "cluster_backup.hh"
#include "manifest.hh"
#include "utils/lister.hh"
#include "replica/database.hh"
#include "replica/global_table_ptr.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/storage_proxy.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/storage.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/component_type.hh"
#include "utils/error_injection.hh"
#include "utils/upload_progress.hh"
#include "utils/memory_data_sink.hh"
#include "utils/overloaded_functor.hh"
#include "idl/snapshot_backup.dist.hh"

extern logging::logger snap_log;

static constexpr auto cluster_backup_task_type = "cluster backup";
static constexpr auto cluster_clear_snapshot_task_type = "cluster clear snapshot";

template<> 
struct std::hash<db::snapshot_dc_location> {
    size_t operator()(const db::snapshot_dc_location& a) const {
        return utils::tuple_hash{}(std::tie(a.endpoint, a.bucket, a.prefix));
    }
};

std::string db::snapshot::sstables_location(std::string_view prefix) {
    auto pp = prefix.empty() ? "" : "/";
    return fmt::format("{}{}{}",  prefix, pp, "sstables");
}

std::string db::snapshot::snapshot_meta_location(std::string_view prefix, const replica::table& t, std::string_view snapshot_name) {
    auto pp = prefix.empty() ? "" : "/";
    return fmt::format("{}{}{}/{}/{}/{}",  prefix, pp, "snapshots", t.schema()->ks_name(), t.schema()->cf_name(), snapshot_name);
}

std::string db::snapshot::snapshot_prefix_from_manifest(std::string_view manifest_path) {
    // manifest_path = <prefix>/snapshots/<ks>/<cf>/<snapshot>/manifest.json
    return std::filesystem::path(manifest_path)
        .parent_path() // <prefix>/snapshots/<ks>/<cf>/<snapshot>
        .parent_path() // <prefix>/snapshots/<ks>/<cf>
        .parent_path() // <prefix>/snapshots/<ks>
        .parent_path() // <prefix>/snapshots/
        .parent_path() // <prefix>
        .string();
}

static future<> do_cluster_backup(db::snapshot_ctl& snap_ctl, const std::string& snapshot_name, const std::unordered_multimap<sstring, sstring>& ks_tables, std::unordered_map<sstring, db::snapshot_dc_location>& dc_locations, bool remove_on_uploaded, tasks::task_manager::task::progress& total_progress, abort_source& as) {
    using namespace db::snapshot;

    snap_log.info("Begin processing {} {}", snapshot_name, ks_tables);

    db::snapshot_table_helper sth(snap_ctl.qp().local());
    auto snapshot = co_await sth.get_snapshot(snapshot_name);
    if (!snapshot) {
        snap_log.info("No snapshot named {} in snapshot table. Creating...", snapshot_name);
        co_await snap_ctl.sp().local().snapshot_keyspace(ks_tables, snapshot_name, {});
        snapshot = co_await sth.get_snapshot(snapshot_name);
        if (!snapshot) {
            snap_log.error("Could not create snapshot {}.", snapshot_name);
            co_return; // throw?
        }
    }

    auto locations = co_await sth.get_snapshot_remote_locations(snapshot_name);
    std::unordered_map<std::string, db::snapshot_state> state_filter;

    for (auto& dc : dc_locations | std::views::keys) {
        state_filter[dc] = db::snapshot_state::remote_and_local;
    }

    for (auto& loc : locations) {
        auto i = dc_locations.find(loc.datacenter);
        if (i == dc_locations.end()) {
            continue;
        }

        auto filter = db::snapshot_state::remote_and_local;

        // check if we are writing to a different destination. If so, we reset state
        if (loc.endpoint != i->second.endpoint || loc.bucket != i->second.bucket || loc.prefix != i->second.prefix) {
            loc.state = db::snapshot_state::local;
            filter = db::snapshot_state::remote;
        }
        if (loc.state >= db::snapshot_state::remote_and_local) {
            snap_log.warn("Snapshot {} for {} is already backed up: {}:{}", snapshot_name, loc.datacenter, loc.bucket, loc.prefix);
            dc_locations.erase(loc.datacenter);
        }

        state_filter[loc.datacenter] = filter;
    }

    if (dc_locations.empty()) {
        // Every requested location is already backed up. Abort early,
        // the per-table loop below dereferences dc_locations.begin() and would crash the node.
        snap_log.info("Snapshot {} is already backed up at every requested location", snapshot_name);
        co_return;
    }

    auto new_locations = dc_locations | std::views::transform([&](auto& p) {
        return db::snapshot_remote_location_entry {
            .snapshot_name = snapshot_name,
            .datacenter = p.first,
            .endpoint = p.second.endpoint,
            .bucket = p.second.bucket,
            .prefix = p.second.prefix,
            .state = db::snapshot_state::being_backed_up,
        };
    }) | std::ranges::to<std::vector<db::snapshot_remote_location_entry>>();

    // We are already on shard 0. Can use all objects fine. Just need to acquire snap locks.
    // Mainly for semantic consistency. Maybe all this should be a raft op.
    co_await snap_ctl.run_snapshot_modify_operation([&]() -> future<> {
        co_await sth.insert_snapshot_remote_locations(new_locations, db::consistency_level::QUORUM, 0); // update status
    });

    auto nodes = co_await sth.get_snapshot_nodes(snapshot_name);
    auto nodes_for_location = nodes | std::views::filter([&](auto& n) { return dc_locations.count(n.datacenter); });
    auto& db = snap_ctl.db().local();

    total_progress.total = (std::distance(nodes_for_location.begin(), nodes_for_location.end()) + dc_locations.size() /* manifests */) * ks_tables.size();

    co_await coroutine::parallel_for_each(ks_tables, [&](const std::pair<sstring, sstring>& pair) -> future<>{
        auto [keyspace, table] = pair;
        snap_log.info("Processing {}, {}:{}", snapshot_name, keyspace, table);

        auto& t = db.find_column_family(keyspace, table);
        auto schema = t.schema();
        auto tid = schema->id();

        // Recording the unified layout prefix as the destination makes the manifest land correctly at
        // {prefix}/snapshots/{ks}/{cf}/{tag}/manifest.json, which is where restore reads it from.
        const bool remote_table = t.get_storage_options().is_object_storage_type();
        auto effective_dst = [&] (const db::snapshot_dc_location& requested) -> db::snapshot_dc_location {
            return std::visit(overloaded_functor{
                [&] (const data_dictionary::storage_options::local&) {
                    return requested;
                },
                [] (const data_dictionary::storage_options::object_storage& os) {
                    return db::snapshot_dc_location {
                        .endpoint = os.endpoint,
                        .bucket = os.bucket,
                        .prefix = os.location ? std::string(*os.location) : std::string(sstables::object_storage_default_prefix),
                    };
                },
            }, t.get_storage_options().value);
        };

        struct dst_data {
            utils::chunked_vector<db::snapshot_sstable_entry> sstables;
            std::unordered_set<std::string> datacenters;
        };

        std::unordered_map<db::snapshot_dc_location, dst_data> dst_mapping;
        // redundant since we don't support per-dc tablets, but why not complicate things
        std::unordered_map<std::string, std::unordered_map<size_t, db::snapshot_tablet_entry>> dc_tablets;
        for (auto& dc : dc_locations | std::views::keys) {
            dc_tablets.emplace(dc, 
                (co_await sth.get_snapshot_tablets(snapshot_name, keyspace, table, dc))
                | std::views::transform([](auto& e) {
                    return std::make_pair(e.tablet_id, e);
                })
                | std::ranges::to<std::unordered_map>()
            );
        }

        struct dedup_state {
            locator::host_id owner;
            std::unordered_set<locator::host_id> others;
        };

        std::unordered_map<db::snapshot_dc_location, std::unordered_map<dht::token, dedup_state>> repair_masters;
        std::unordered_map<locator::host_id, std::tuple<size_t, size_t, std::string>> node_sstables_dc;

        for (const db::snapshot_node_entry& node : nodes_for_location) {
            if (auto e = as.abort_requested_exception_ptr(); e) {
                snap_log.warn("Abort {} requested when processing {}, {}:{}", snapshot_name, node.node, keyspace, table);
                std::rethrow_exception(e);
            }
            snap_log.info("Calculating sstable set for {} node {}, {}:{}", snapshot_name, node.node, keyspace, table);

            assert(state_filter.count(node.datacenter));
            assert(dc_locations.count(node.datacenter));

            auto dst = effective_dst(dc_locations.at(node.datacenter));
            auto& repair_master = repair_masters[dst];
            auto sstables = co_await sth.get_snapshot_sstables(snapshot_name, keyspace, table, node.datacenter, node.rack);
            auto& tablets = dc_tablets.at(node.datacenter);

            // eliminate all but one of the completed repair set.
            // for each tablet/dc, we include sstables that are either 
            // not repaired, or if they are, if we are the first node
            // to process the tablet.
            sstables = sstables | std::views::filter([&](auto& e) {
                if (e.node != node.node) {
                    return false; // cannot claim sstable not on my node.
                }
                auto& ti = tablets.at(e.tablet_id);

                if (e.repaired_at < ti.repaired_at || e.repaired_at == 0) {
                    return true; // must include
                }
                auto i = repair_master.find(ti.first_token);
                if (i == repair_master.end()) {
                    i = repair_master.emplace(ti.first_token, dedup_state{ .owner = node.node }).first;
                }
                auto include = i->second.owner == node.node;
                if (!include) {
                    snap_log.debug("De-duplicating sstable {} from {}:{} for {}", e.sstable_id, node.datacenter, node.rack, node.node);
                    i->second.others.emplace(node.node);
                }
                return include; // we claimed the token range
            }) | std::ranges::to<utils::chunked_vector<db::snapshot_sstable_entry>>();

            auto& dst_info = dst_mapping[dst];
            auto off = dst_info.sstables.size(), n = sstables.size();
            dst_info.sstables.insert(dst_info.sstables.end(), sstables.begin(), sstables.end());
            dst_info.datacenters.emplace(node.datacenter);

            node_sstables_dc.emplace(node.node, std::make_tuple(off, n, node.datacenter));
        }

        co_await coroutine::parallel_for_each(nodes_for_location, [&](const db::snapshot_node_entry& node) -> future<>{
            if (auto e = as.abort_requested_exception_ptr(); e) {
                // note the point at which we aborted
                snap_log.warn("Abort {} requested when processing {}, {}:{}", snapshot_name, node.node, keyspace, table);
                std::rethrow_exception(e);
            }

            snap_log.info("Processing {} node {}, {}:{}", snapshot_name, node.node, keyspace, table);

            assert(state_filter.count(node.datacenter));
            assert(dc_locations.count(node.datacenter));

            auto [off, n, _]= node_sstables_dc.at(node.node);
            auto filter = state_filter.at(node.datacenter);
            auto dst = effective_dst(dc_locations.at(node.datacenter));
            auto& dst_info = dst_mapping[dst];

            // filter out sstables already backed up
            auto sstables = std::ranges::subrange(dst_info.sstables.begin() + off, dst_info.sstables.begin() + off + n) | std::views::filter([&node, filter](auto& e) { 
                return e.node == node.node && e.state < filter;
            }) | std::ranges::to<utils::chunked_vector<db::snapshot_sstable_entry>>();

            if (sstables.empty()) {
                snap_log.info("All sstables for {} node {}, {}:{} already backed up", snapshot_name, node.node, keyspace, table);
                total_progress.completed += 1;
                co_return;
            }
            if (filter > db::snapshot_state::remote_and_local) {
                // changed location. rewrite states
                for (auto& sst : sstables) {
                    sst.state = db::snapshot_state::local;
                }
                co_await sth.insert_snapshot_sstables(snapshot_name, keyspace, table, node.datacenter, node.rack, sstables, db::consistency_level::QUORUM, 0);
            }

            // we don't really need/use this atm, but...
            auto first_token = std::accumulate(sstables.begin(), sstables.end(), dht::token::maximum(), [](dht::token t1, const db::snapshot_sstable_entry& e) {
                return std::min(t1, e.first_token);
            });
            auto last_token = std::accumulate(sstables.begin(), sstables.end(), dht::token::minimum(), [](dht::token t1, const db::snapshot_sstable_entry& e) {
                return std::max(t1, e.last_token);
            });

            auto sstable_ids = sstables | std::views::transform([](auto& e) { return e.sstable_id; }) | std::ranges::to<utils::chunked_vector<sstables::sstable_id>>();

            snap_log.info("Requesting backup of {}: {}", node.node, sstable_ids);

            try {
                // For an object-storage table dst.prefix already names the
                // data location
                auto prefix = remote_table ? dst.prefix : db::snapshot::sstables_location(dst.prefix);
                co_await ser::snapshot_backup_rpc_verbs::send_backup_snapshot_sstables(&snap_ctl.ms(), node.node, tid, snapshot_name, dst.endpoint, dst.bucket, prefix, first_token, last_token, std::move(sstable_ids), remove_on_uploaded);
                total_progress.completed += 1;
            } catch (...) {
                snap_log.error("Exception requesting backup of {}:{} from {}", snapshot_name, sstable_ids, node.node);
                throw; // fail the whole process already
            }
        });

        snap_log.info("Generate manifests for {} {}:{}", snapshot_name, keyspace, table);

        // Note: atm, tablet mapping is same across all dcs. If this changes, we need to get this per dc,
        // each of which can shared dest with others. Thus we also need to change manifest grammar to allow
        // tablet info per dc.
        auto tablets = co_await sth.get_snapshot_tablets(snapshot_name, keyspace, table, dc_locations.begin()->first);
        auto tables = co_await sth.get_snapshot_tables(snapshot_name, keyspace, table);

        auto& manager = db.get_sstables_manager(*schema);

        // Now generate a manifest
        co_await coroutine::parallel_for_each(dst_mapping, [&](const auto& pair) -> future<> {
            auto& [dst, info] = pair;
            auto& repair_master = repair_masters[dst];

            snap_log.info("Generate manifest for {} {}:{} ({}:{}/{})", snapshot_name, keyspace, table, dst.endpoint, dst.bucket, dst.prefix);

            manifest_json manifest;

            manifest_json::info minfo;
            minfo.version = "1.0";
            minfo.scope = "dc";
            manifest.manifest = std::move(minfo);

            manifest_json::snapshot_info snapshot_info;
            snapshot_info.name = snapshot_name;
            snapshot_info.created_at = decltype(snapshot->created_at)::clock::to_time_t(snapshot->created_at);
            snapshot_info.expires_at = decltype(snapshot->expires_at)::clock::to_time_t(snapshot->expires_at);
            manifest.snapshot = std::move(snapshot_info);

            manifest_json::table_info table_info;
            table_info.keyspace_name = keyspace;
            table_info.table_name = table;
            table_info.table_id = tid.to_sstring();
            table_info.tablets_type = tables.empty() ? "none" : tables.front().tablet_layout;
            table_info.tablet_count = tablets.size();
            manifest.table = std::move(table_info);

            for (auto& t : tablets) {
                manifest.tablets.push(t);
            }

            for (auto& s : info.sstables) {
                manifest_json::sstable_info sst(s);
                auto& tablet_map = dc_tablets.at(std::get<2>(node_sstables_dc.at(s.node)));
                auto& ti = tablet_map.at(s.tablet_id);
                // was this a de-duplicated sstable? If so, with mark additional nodes
                if (s.repaired_at >= ti.repaired_at && s.repaired_at != 0) {
                    auto i = repair_master.find(ti.first_token);
                    if (i != repair_master.end()) {
                        for (auto& n : i->second.others) {
                            snap_log.debug("Adding additional (deduplicated) node  {} for sstable {}", n, s.sstable_id);
                            sst.additional_nodes.push(n.to_sstring());
                        }
                    }
                }
                manifest.sstables.push(sst);
            }
            for (auto& n : nodes_for_location) {
                if (info.datacenters.contains(n.datacenter)) {
                    manifest.nodes.push(n);
                }
            }

            auto client = manager.get_endpoint_client(dst.endpoint);
            auto prefix = db::snapshot::snapshot_meta_location(dst.prefix, t, snapshot_name);
            output_stream<char> out(client->make_upload_sink(sstables::object_name(dst.bucket, prefix, "manifest.json"), sstables::object_storage_attributes{}, &as));
            auto streamer = json::stream_object(std::move(manifest));
            co_await streamer(std::move(out));
            total_progress.completed += 1;
        });

    });

    for (auto& loc : new_locations) {
        loc.state = remove_on_uploaded ? db::snapshot_state::remote : db::snapshot_state::remote_and_local;
    }

    // See above.
    co_await snap_ctl.run_snapshot_modify_operation([&]() -> future<> {
        co_await sth.insert_snapshot_remote_locations(new_locations, db::consistency_level::QUORUM, 0); // update status
    });
}


static future<> do_cluster_clear_snapshot(db::snapshot_ctl& snap_ctl, const std::string& snapshot_name, bool force, tasks::task_manager::task::progress& total_progress, abort_source& as) {
    using namespace db::snapshot;

    snap_log.info("Begin clearing snapshot {}", snapshot_name);

    db::snapshot_table_helper sth(snap_ctl.qp().local());
    auto snapshot = co_await sth.get_snapshot(snapshot_name, db::consistency_level::QUORUM);
    if (!snapshot && !force) {
        throw std::invalid_argument(fmt::format("No snapshot named {} in the snapshot catalog, use force to clear the leftovers of a failed attempt", snapshot_name));
    }

    auto tables = co_await sth.get_snapshot_tables(snapshot_name, {}, {}, db::consistency_level::QUORUM);
    auto nodes = co_await sth.get_snapshot_nodes(snapshot_name, {}, {}, db::consistency_level::QUORUM);
    if (!snapshot) {
        if (tables.empty() && nodes.empty()) {
            throw std::invalid_argument(fmt::format("No snapshot named {} in the snapshot catalog", snapshot_name));
        }
        snap_log.info("Snapshot {} has no commit marker, force-clearing the leftovers of a failed attempt", snapshot_name);
    }
    auto& db = snap_ctl.db().local();

    auto tmptr = snap_ctl.sp().local().shared_token_metadata().get();
    std::unordered_set<locator::host_id> departed;
    for (const auto& n : nodes) {
        if (!tmptr->get_topology().find_node(n.node)) {
            snap_log.warn("Snapshot {}: node {} is no longer a cluster member, its snapshot references cannot be released",
                    snapshot_name, n.node);
            departed.insert(n.node);
        }
    }

    total_progress.total = tables.size() * (nodes.size() + 1) + 1;

    for (const auto& t : tables) {
        if (auto e = as.abort_requested_exception_ptr(); e) {
            std::rethrow_exception(e);
        }
        auto tbl = db.get_tables_metadata().get_table_if_exists(t.table_id);
        if (!tbl) {
            // FIXME SCYLLADB-3488: The schema is gone and the storage options as well, so nothing
            // can name this table's refs in the bucket anymore.
            // The catalog rows below are still deleted.
            snap_log.warn("Snapshot {}: table {}.{} no longer exists, its snapshot references cannot be released",
                    snapshot_name, t.keyspace_name, t.table_name);
            total_progress.completed += nodes.size() + 1;
            continue;
        }

        co_await coroutine::parallel_for_each(nodes, [&] (const db::snapshot_node_entry& node) -> future<> {
            if (departed.contains(node.node)) {
                total_progress.completed += 1;
                co_return;
            }
            snap_log.info("Requesting clear of snapshot {} table {}.{} from {}", snapshot_name, t.keyspace_name, t.table_name, node.node);
            co_await ser::snapshot_backup_rpc_verbs::send_clear_snapshot_sstables(&snap_ctl.ms(), node.node, t.table_id, snapshot_name);
            total_progress.completed += 1;
        });

        // In cluster world, a backup is a "promoted" snapshot plus a manifest file.
        // If the backup is gone, we delete its manifest as well, if it exists, if not, it was
        // only a snapshot all along.
        const auto* table_os = std::get_if<data_dictionary::storage_options::object_storage>(&tbl->get_storage_options().value);
        if (table_os) {
            auto managed = table_os->location ? std::string(*table_os->location) : std::string(sstables::object_storage_default_prefix);
            auto client = snap_ctl.sstm().container().local().get_endpoint_client(table_os->endpoint);
            auto prefix = db::snapshot::snapshot_meta_location(managed, *tbl, snapshot_name);
            co_await client->delete_object(sstables::object_name(table_os->bucket, prefix, "manifest.json"));
        }
        total_progress.completed += 1;
    }

    std::unordered_set<std::string> datacenters;
    for (const auto& node : nodes) {
        datacenters.insert(node.datacenter);
    }
    for (const auto& t : tables) {
        for (const auto& node : nodes) {
            co_await sth.delete_snapshot_sstables_partition(snapshot_name, t.keyspace_name, t.table_name, node.datacenter, node.rack);
        }
        for (const auto& dc : datacenters) {
            co_await sth.delete_snapshot_tablets_partition(snapshot_name, t.keyspace_name, t.table_name, dc);
        }
    }
    co_await sth.delete_snapshot_metadata(snapshot_name);

    // The commit marker gets deleted last.
    co_await sth.delete_snapshot_entry(snapshot_name, db::consistency_level::QUORUM);
    total_progress.completed += 1;
    snap_log.info("Cleared snapshot {}", snapshot_name);
}

future<tasks::task_id> 
db::snapshot::start_global_backup(db::snapshot_ctl& ctl, tasks::task_manager::module_ptr tm, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, snapshot_dc_location> locations, bool move_files) {
    if (ks_tables.empty()) {
        throw std::invalid_argument("No tables provided for backup");
    }
    auto progress = make_lw_shared<tasks::task_manager::task::progress>();
    tasks::task_manager::task_builder task_builder{std::move(tm), cluster_backup_task_type};
    task_builder.set_scope("datacenter")
                .set_keyspace(ks_tables.begin()->first)
                .set_table(ks_tables.begin()->second)
                .set_is_abortable(tasks::is_abortable::yes)
                .set_is_internal(tasks::is_internal::no)
                .set_is_user_task(tasks::is_user_task::yes)
                .set_progress_fn([progress] (const tasks::task_manager::task::impl&) {
                    return make_ready_future<tasks::task_manager::task::progress>(*progress);
                });
    auto task = co_await std::move(task_builder).build([&ctl, snapshot_name = std::move(snapshot_name), ks_tables = std::move(ks_tables), locations = std::move(locations), move_files, progress] (tasks::task_manager::task::impl& self) mutable {
        // this is mainly to prevent a drain from stopping us while we are running.
        // however, it will not prevent the nodes doing actual sending from being
        // unalived, but...
        return ctl.run_snapshot_gate_operation([&ctl, &snapshot_name, &ks_tables, &locations, move_files, progress, &as = self.get_abort_source()] {
            return do_cluster_backup(ctl, snapshot_name, ks_tables, locations, move_files, *progress, as);
        });
    });
    co_return task->id();
}

// Marks an sstable as backedup in the snapshot_sstables catalog table.
static future<> mark_sstable_backed_up(db::snapshot_ctl& snap, const std::string& tag, const sstring& ksname, const sstring& cfname,
        const locator::endpoint_dc_rack& local, db::snapshot_sstable_entry& e, bool use_move) {
    db::snapshot_table_helper sth(snap.qp().local());
    e.state = use_move ? db::snapshot_state::remote : db::snapshot_state::remote_and_local;
    // Spell out the single-element array instead of passing a braced list,
    // which would be ambiguous between the two insert_snapshot_sstables()
    // overloads.
    std::array entry{e};
    co_await sth.insert_snapshot_sstables(tag, ksname, cfname, local.dc, local.rack, entry, db::consistency_level::QUORUM, 0);
}

future<tasks::task_id>
db::snapshot::start_cluster_clear_snapshot(db::snapshot_ctl& ctl, tasks::task_manager::module_ptr tm, std::string snapshot_name, bool force) {
    if (snapshot_name.empty()) {
        throw std::invalid_argument("You must supply a snapshot name.");
    }
    auto progress = make_lw_shared<tasks::task_manager::task::progress>();
    tasks::task_manager::task_builder task_builder{std::move(tm), cluster_clear_snapshot_task_type};
    task_builder.set_scope("cluster")
                .set_entity(snapshot_name)
                .set_is_abortable(tasks::is_abortable::yes)
                .set_is_internal(tasks::is_internal::no)
                .set_is_user_task(tasks::is_user_task::yes)
                .set_progress_fn([progress] (const tasks::task_manager::task::impl&) {
                    return make_ready_future<tasks::task_manager::task::progress>(*progress);
                });
    auto task = co_await std::move(task_builder).build([&ctl, snapshot_name = std::move(snapshot_name), force, progress] (tasks::task_manager::task::impl& self) mutable {
        return ctl.run_snapshot_gate_operation([&ctl, &snapshot_name, force, progress, &as = self.get_abort_source()] {
            return do_cluster_clear_snapshot(ctl, snapshot_name, force, *progress, as);
        });
    });
    co_return task->id();
}

future<>
db::snapshot::clear_sstables(db::snapshot_ctl& snap, table_id table_id, std::string tag) {
    snap_log.info("Got clear request for snapshot {}, table {}", tag, table_id);

    auto& db = snap.db().local();
    auto& cf = db.find_column_family(table_id);
    auto ksname = cf.schema()->ks_name();
    auto cfname = cf.schema()->cf_name();
    auto global_table = co_await get_table_on_all_shards(snap.db(), ksname, cfname);
    auto& storage_options = global_table->get_storage_options();

    if (storage_options.is_local_type()) {
        co_return co_await snap.clear_snapshot(tag, { ksname }, cfname);
    }

    // Delete the refs, and when it was the sstable's last claim, the
    // pinned components and the retained snapshot_owned registry entry gets cleared
    // as well.
    // A ref that is kept by another snapshot or by a live sstable, pins
    // the sstable components.
    const auto os = std::get<data_dictionary::storage_options::object_storage>(storage_options.value);
    auto location = snap.sp().local().shared_token_metadata().get()->get_topology().get_location();
    auto me = snap.sp().local().shared_token_metadata().get()->get_topology().my_host_id();
    db::snapshot_table_helper sth(snap.qp().local());
    auto rows = co_await sth.get_snapshot_sstables(tag, ksname, cfname, location.dc, location.rack, db::consistency_level::QUORUM);
    auto own = rows | std::views::filter([&] (const auto& e) { return e.node == me; })
            | std::ranges::to<utils::chunked_vector<db::snapshot_sstable_entry>>();

    auto client = snap.sstm().container().local().get_endpoint_client(os.endpoint);
    auto& manager = db.get_sstables_manager(*cf.schema());
    auto* registry = manager.has_sstables_registry() ? &manager.sstables_registry() : nullptr;

    co_await seastar::max_concurrent_for_each(own, 16, [&] (const db::snapshot_sstable_entry& e) -> future<> {
        auto desc = sstables::parse_path(std::filesystem::path(std::string_view(e.toc_name)), ksname, cfname);
        if (!desc) {
            snap_log.warn("Snapshot '{}' of {}.{}: cannot derive the reference of {} from toc_name '{}', its ref (if any) leaks until healed",
                    tag, ksname, cfname, e.sstable_id, e.toc_name);
            co_return;
        }
        desc->sid = e.sstable_id;
        co_await sstables::release_object_storage_snapshot_ref(*client, os, registry, cf.schema()->id(), me, tag, *desc);
        utils::get_local_injector().inject("cluster_clear_snapshot_release", [] {
            throw std::runtime_error("cluster_clear_snapshot_release: injected error");
        });
    });
}

future<>
db::snapshot::backup_sstables(db::snapshot_ctl& snap, table_id table_id, std::string tag, std::string endpoint, std::string bucket, std::string prefix, dht::token first_token, dht::token last_token, utils::chunked_vector<sstables::sstable_id> sstable_ids, bool use_move) {
    snap_log.info("Got backup request for snapshot {}, table {}, sstables {} ({}:{}) -> {}:{}:{}", tag, table_id, sstable_ids, first_token, last_token, endpoint, bucket, prefix);

    auto& db = snap.db().local();
    // This is not super efficient. We need to match files on disk, but we do want to use 
    // "proper" ids like sstable_id for designating tables to backup. We could open files
    // on scan to match ID:s, but it might be easier/faster to just re-use the meta table
    auto& cf = db.find_column_family(table_id);
    auto local = snap.sp().local().shared_token_metadata().get()->get_topology().get_location();
    auto ksname = cf.schema()->ks_name();
    auto cfname = cf.schema()->cf_name();
    db::snapshot_table_helper sth(snap.qp().local());
    auto sstables = co_await sth.get_snapshot_sstables(tag, ksname, cfname 
        , local.dc
        , local.rack
        , db::consistency_level::ONE // we are querying our own data
        , first_token
        , last_token
    );
    std::sort(sstable_ids.begin(), sstable_ids.end());
    std::ranges::sort(sstables, {}, &db::snapshot_sstable_entry::sstable_id);
    auto i = sstable_ids.begin(); 
    auto j = std::remove_if(sstables.begin(), sstables.end(), [&](const db::snapshot_sstable_entry& e) {
        while (i != sstable_ids.end() && e.sstable_id > *i) {
            ++i;
        }
        if (i == sstable_ids.end()) {
            return true;
        }
        if (e.sstable_id < *i) {
            return true;
        }
        return false;
    });
    sstables.erase(j, sstables.end());

    snap_log.debug("Found {} sstables not yet backed up", sstables.size());

    auto global_table = co_await get_table_on_all_shards(snap.db(), ksname, cfname);
    auto& storage_options = global_table->get_storage_options();
    if (storage_options.is_object_storage_type()) {
        co_await seastar::max_concurrent_for_each(sstables, 16, [&] (db::snapshot_sstable_entry& e) -> future<> {
            snap_log.debug("Marking {} as backed up in place", e.sstable_id);
            // the state change doesnt say anything meaningful for remote tables
            // we're just emulation the state change for consistency with local backups, but
            // there is nothing functional about it really.
            co_await mark_sstable_backed_up(snap, tag, ksname, cfname, local, e, use_move);
            utils::get_local_injector().inject("cluster_backup_object_storage_flip", [] {
                throw std::runtime_error("cluster_backup_object_storage_flip: injected error");
            });
        });
        co_return;
    }

    auto& local_storage_options = std::get<data_dictionary::storage_options::local>(storage_options.value);
    auto dir = (local_storage_options.dir / sstables::snapshots_dir / std::string_view(tag));

    struct gen_info {
        db::snapshot_sstable_entry& sstable;
        std::vector<std::string> filenames;
    };

    size_t num_components = 0;
    auto base_names = sstables | std::views::transform([&](auto& ss) {
        auto& toc_name = ss.toc_name;
        auto i = toc_name.find_last_of('-');
        return std::make_pair(std::string_view(toc_name).substr(0, i), gen_info{ ss });
    }) | std::ranges::to<std::unordered_map>();

    {
        auto directory = co_await io_check(open_directory, dir.native());
        auto snapshot_dir_lister = directory_lister(directory
            , dir
            , lister::dir_entry_types::of<directory_entry_type::regular>()
            , [&](const fs::path& path, const directory_entry& e) {
                if (auto i = e.name.find_last_of('-'); i != sstring::npos) {
                    std::string_view tmp(e.name.data(), i);
                    if (auto j = base_names.find(tmp); j != base_names.end()) {
                        j->second.filenames.emplace_back(e.name);
                        ++num_components;
                        return true;
                    }
                }
                return false;
            });

        while (co_await snapshot_dir_lister.get()) {
            // just iterate through
        }
        co_await snapshot_dir_lister.close();
    }

    std::erase_if(base_names, [](auto& p) {
        return p.second.filenames.empty();
    });

    snap_log.debug("backup_sstables: found {} SSTables consisting of {} component files", base_names.size(), num_components);

    auto chunks = base_names 
            | std::views::chunk(size_t(std::ceil(double(base_names.size())/this_smp_shard_count())))
            | std::ranges::to<std::vector>()
            ;
    co_await snap.db().invoke_on_all([&](auto& db) -> future<> {
        if (this_shard_id() >= chunks.size()) {
            co_return;
        }
        std::exception_ptr p;

        auto chunk = chunks[this_shard_id()];
        auto client = snap.sstm().container().local().get_endpoint_client(endpoint);
        auto& t = db.find_column_family(table_id);
        auto& manager = db.get_sstables_manager(*t.schema());

        co_await coroutine::parallel_for_each(chunk | std::views::values, [&](const gen_info& info) -> future<>{
            auto& id = info.sstable.sstable_id;
            auto table_prefix = fmt::format("{}/{}", prefix, id);

            auto gen_info = sstables::parse_path(std::filesystem::path(info.sstable.toc_name), ksname, cfname);
            if (!gen_info) {
                throw std::runtime_error(fmt::format("Could not parse sstable generation for {}", id));
            }

            auto gen = (*gen_info).generation;
            auto ref_name = sstables::object_name(bucket, table_prefix, sstables::object_storage_snapshot_ref_name(tag, gen));
            co_await client->put_object(ref_name, memory_data_sink_buffers{}, sstables::object_storage_attributes{}); // any exception here can just propagate

            bool any_failed = false;
            co_await coroutine::parallel_for_each(info.filenames, [&](std::string_view name) -> future<> {
                auto units = co_await manager.dir_semaphore().get_units(1);

                // Pre-upload break point. For testing abort in actual s3 client usage.
                co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

                auto component_name = dir / name;
                auto destination = sstables::object_name(bucket, table_prefix, name);

                snap_log.trace("Upload {} to {}", component_name.native(), destination);

                bool error = false;

                try {
                    auto exists = co_await client->object_exists(destination);

                    if (exists) {
                        snap_log.trace("Object {} already exists. Skipping...", destination);
                    } else {
                        utils::upload_progress dummy;
                        co_await client->upload_file(component_name, std::move(destination), dummy);
                    }
                } catch (...) {
                    error = true; // we might have written parts
                    snap_log.error("Error uploading {}: {:t}", component_name.native(), std::current_exception());
                    if (!p) {
                        p = std::current_exception();
                    }
                }
                if (error) {
                    any_failed = true;
                    co_return;
                }
                if (use_move) {
                    try {
                        co_await remove_file(component_name.native());
                    } catch (...) {
                        snap_log.warn("Failed to remove {}: {:t}", component_name, std::current_exception());
                    }
                }
                co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
            });

            if (any_failed) {
                try {
                    co_await client->delete_object(ref_name);
                } catch (...) {
                    // nothing to do here...
                }
                co_return; // don't update status.
            }

            try {
                snap_log.info("Marking {} as uploaded", id);
                co_await mark_sstable_backed_up(snap, tag, ksname, cfname, local, info.sstable, use_move);
            } catch (...) {
                snap_log.error("Error marking {} as uploaded: {:t}", id, std::current_exception());
            }
        });

        if (p) {
            co_await coroutine::return_exception_ptr(std::move(p));
        }
    });
}
