/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
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
#include "sstables/object_storage_client.hh"
#include "sstables/component_type.hh"
#include "utils/error_injection.hh"
#include "utils/upload_progress.hh"
#include "idl/snapshot_backup.dist.hh"

extern logging::logger snap_log;

template<> 
struct std::hash<db::snapshot_dc_location> {
    size_t operator()(const db::snapshot_dc_location& a) const {
        return utils::tuple_hash{}(std::tie(a.endpoint, a.bucket, a.prefix));
    }
};

class cluster_backup_task : public tasks::task_manager::task::impl {
    db::snapshot_ctl& _snap_ctl;
    std::string _snapshot;
    std::unordered_multimap<sstring, sstring> _ks_tables;
    std::unordered_map<sstring, db::snapshot_dc_location> _locations;
    bool _remove_on_uploaded;
    tasks::task_manager::task::progress _total_progress;

    future<> do_backup();
protected:
    future<> run() override;
public:
    cluster_backup_task(tasks::task_manager::module_ptr module
        , db::snapshot_ctl& ctl
        , std::string snapshot
        , std::unordered_multimap<sstring, sstring> ks_tables
        , std::unordered_map<sstring, db::snapshot_dc_location> locations
        , bool move_files) noexcept;

    std::string type() const override {
        return "cluster backup";
    }
    tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::no;
    }
    tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable::yes;
    }
    future<tasks::task_manager::task::progress> get_progress() const override {
        co_return _total_progress;
    }
    tasks::is_user_task is_user_task() const noexcept override {
        return tasks::is_user_task::yes;
    }
};

cluster_backup_task::cluster_backup_task(tasks::task_manager::module_ptr module
    , db::snapshot_ctl& ctl
    , std::string snapshot
    , std::unordered_multimap<sstring, sstring> ks_tables
    , std::unordered_map<sstring, db::snapshot_dc_location> locations
    , bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "datacenter", ks_tables.begin()->first, ks_tables.begin()->second, "", tasks::task_id::create_null_id())    
    , _snap_ctl(ctl)
    , _snapshot(std::move(snapshot))
    , _ks_tables(std::move(ks_tables))
    , _locations(std::move(locations))
    , _remove_on_uploaded(move_files)
{}


future<> cluster_backup_task::run() {
    // this is mainly to prevent a drain from stopping us while we are running.
    // however, it will not prevent the nodes doing actual sending from being
    // unalived, but...
    co_await _snap_ctl.run_snapshot_gate_operation([this] {
        return do_backup();
    });
}

static std::string format_snapshot_location(std::string_view prefix, std::string_view what, const replica::table&, std::string_view appendix = {}) {
    auto pp = prefix.empty() ? "" : "/";
    auto ap = appendix.empty() ? "" : "/";
    return fmt::format("{}{}{}{}{}",  prefix, pp, what, ap, appendix);
}

std::string db::snapshot::sstables_location(std::string_view prefix, const replica::table& t, std::string_view snapshot_name) {
    return format_snapshot_location(prefix, "sstables", t);
}

std::string db::snapshot::snapshot_meta_location(std::string_view prefix, const replica::table& t, std::string_view snapshot_name) {
    return format_snapshot_location(prefix, "snapshots", t, snapshot_name);
}

future<> cluster_backup_task::do_backup() {
    using namespace db::snapshot;

    snap_log.info("Begin processing {} {}", _snapshot, _ks_tables);

    db::snapshot_table_helper sth(_snap_ctl.qp().local());
    auto snapshot = co_await sth.get_snapshot(_snapshot);
    if (!snapshot) {
        snap_log.info("No snapshot named {} in snapshot table. Creating...", _snapshot);
        co_await _snap_ctl.sp().local().snapshot_keyspace(_ks_tables, _snapshot, {});
        snapshot = co_await sth.get_snapshot(_snapshot);
        if (!snapshot) {
            snap_log.error("Could not create snapshot {}.", _snapshot);
            co_return; // throw?
        }
    }

    auto locations = co_await sth.get_snapshot_remote_locations(_snapshot);
    std::unordered_map<std::string, db::snapshot_state> state_filter;

    for (auto& dc : _locations | std::views::keys) {
        state_filter[dc] = db::snapshot_state::remote_and_local;
    }

    for (auto& loc : locations) {
        auto i = _locations.find(loc.datacenter);
        if (i == _locations.end()) {
            continue;
        }

        auto filter = db::snapshot_state::remote_and_local;

        // check if we are writing to a different destination. If so, we reset state
        if (loc.endpoint != i->second.endpoint || loc.bucket != i->second.bucket || loc.prefix != i->second.prefix) {
            loc.state = db::snapshot_state::local;
            filter = db::snapshot_state::remote;
        }
        if (loc.state >= db::snapshot_state::remote_and_local) {
            snap_log.warn("Snapshot {} for {} is already backed up: {}:{}", _snapshot, loc.datacenter, loc.bucket, loc.prefix);
            _locations.erase(loc.datacenter);
        }

        state_filter[loc.datacenter] = filter;
    }

    auto new_locations = _locations | std::views::transform([&](auto& p) {
        return db::snapshot_remote_location_entry {
            .snapshot_name = _snapshot,
            .datacenter = p.first,
            .endpoint = p.second.endpoint,
            .bucket = p.second.bucket,
            .prefix = p.second.prefix,
            .state = db::snapshot_state::being_backed_up,
        };
    }) | std::ranges::to<std::vector<db::snapshot_remote_location_entry>>();

    // We are already on shard 0. Can use all objects fine. Just need to acquire snap locks.
    // Mainly for semantic consistency. Maybe all this should be a raft op.
    co_await _snap_ctl.run_snapshot_modify_operation([&]() -> future<> {
        co_await sth.insert_snapshot_remote_locations(new_locations); // update status
    });

    auto nodes = co_await sth.get_snapshot_nodes(_snapshot);
    auto nodes_for_location = nodes | std::views::filter([&](auto& n) { return _locations.count(n.datacenter); });
    auto& db = _snap_ctl.db().local();

    _total_progress.total = (std::distance(nodes_for_location.begin(), nodes_for_location.end()) + _locations.size() /* manifests */) * _ks_tables.size();

    co_await coroutine::parallel_for_each(_ks_tables, [&](const std::pair<sstring, sstring>& pair) -> future<>{
        auto [keyspace, table] = pair;
        snap_log.info("Processing {}, {}:{}", _snapshot, keyspace, table);

        auto& t = db.find_column_family(keyspace, table);
        auto schema = t.schema();
        auto tid = schema->id();

        struct dst_data {
            utils::chunked_vector<db::snapshot_sstable_entry> sstables;
            std::unordered_set<std::string> datacenters;
        };

        std::unordered_map<db::snapshot_dc_location, dst_data> dst_mapping;

        co_await coroutine::parallel_for_each(nodes_for_location, [&](const db::snapshot_node_entry& node) -> future<>{
            if (auto e = _as.abort_requested_exception_ptr(); e) {
                // note the point at which we aborted
                snap_log.warn("Abort {} requested when processing {}, {}:{}", _snapshot, node.node, keyspace, table);
                std::rethrow_exception(e);
            }

            snap_log.info("Processing {} node {}, {}:{}", _snapshot, node.node, keyspace, table);

            assert(state_filter.count(node.datacenter));
            assert(_locations.count(node.datacenter));

            auto sstables = co_await sth.get_snapshot_sstables(_snapshot, keyspace, table, node.datacenter, node.rack);
            auto filter = state_filter.at(node.datacenter);
            auto& dst = _locations.at(node.datacenter);

            auto& dst_info = dst_mapping[dst];
            dst_info.sstables.insert( dst_info.sstables.end(), sstables.begin(), sstables.end());
            dst_info.datacenters.emplace(node.datacenter);

            // TODO: here we would like to group sstables by tablet replica and
            // if possible eliminate some of the data to store.

            // filter out sstables already backed up
            sstables = sstables | std::views::filter([&node, filter](auto& e) { 
                return e.node == node.node && e.state < filter;
            }) | std::ranges::to<utils::chunked_vector<db::snapshot_sstable_entry>>();

            if (sstables.empty()) {
                snap_log.info("All sstables for {} node {}, {}:{} already backed up", _snapshot, node.node, keyspace, table);
                _total_progress.completed += 1;
                co_return;
            }
            if (filter > db::snapshot_state::remote_and_local) {
                // changed location. rewrite states
                for (auto& sst : sstables) {
                    sst.state = db::snapshot_state::local;
                }
                co_await sth.insert_snapshot_sstables(_snapshot, keyspace, table, node.datacenter, node.rack, sstables);
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
                auto prefix = db::snapshot::sstables_location(dst.prefix, t, _snapshot);
                co_await ser::snapshot_backup_rpc_verbs::send_backup_snapshot_sstables(&_snap_ctl.ms(), node.node, tid, _snapshot, dst.endpoint, dst.bucket, prefix, first_token, last_token, std::move(sstable_ids), _remove_on_uploaded);
                _total_progress.completed += 1;
            } catch (...) {
                snap_log.error("Exception requesting backup of {}:{} from {}", _snapshot, sstable_ids, node.node);
                throw; // fail the whole process already
            }
        });

        snap_log.info("Generate manifests for {} {}:{}", _snapshot, keyspace, table);

        // Note: atm, tablet mapping is same across all dcs. If this changes, we need to get this per dc,
        // each of which can shared dest with others. Thus we also need to change manifest grammar to allow
        // tablet info per dc.
        auto tablets = co_await sth.get_snapshot_tablets(_snapshot, keyspace, table, _locations.begin()->first);
        auto tables = co_await sth.get_snapshot_tables(_snapshot, keyspace, table);

        auto& manager = db.get_sstables_manager(*schema);

        // Now generate a manifest
        co_await coroutine::parallel_for_each(dst_mapping, [&](const auto& pair) -> future<> {
            auto& [dst, info] = pair;

            snap_log.info("Generate manifest for {} {}:{} ({}:{}/{})", _snapshot, keyspace, table, dst.endpoint, dst.bucket, dst.prefix);

            manifest_json manifest;

            manifest_json::info minfo;
            minfo.version = "1.0";
            minfo.scope = "dc";
            manifest.manifest = std::move(minfo);

            manifest_json::snapshot_info snapshot_info;
            snapshot_info.name = _snapshot;
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
                manifest.sstables.push(s);
            }
            for (auto& n : nodes_for_location) {
                manifest.nodes.push(n);
            }

            auto client = manager.get_endpoint_client(dst.endpoint);
            auto prefix = db::snapshot::snapshot_meta_location(dst.prefix, t, _snapshot);
            output_stream<char> out(client->make_upload_sink(sstables::object_name(dst.bucket, prefix, "manifest.json"), &_as));
            auto streamer = json::stream_object(std::move(manifest));
            co_await streamer(std::move(out));
            _total_progress.completed += 1;
        });

    });

    for (auto& loc : new_locations) {
        loc.state = _remove_on_uploaded ? db::snapshot_state::remote : db::snapshot_state::remote_and_local;
    }

    // See above.
    co_await _snap_ctl.run_snapshot_modify_operation([&]() -> future<> {
        co_await sth.insert_snapshot_remote_locations(new_locations); // update status
    });
}

future<tasks::task_id> 
db::snapshot::start_global_backup(db::snapshot_ctl& ctl, tasks::task_manager::module_ptr tm, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, snapshot_dc_location> locations, bool move_files) {
    if (ks_tables.empty()) {
        throw std::invalid_argument("No tables provided for backup");
    }
    auto task = co_await tm->make_and_start_task<cluster_backup_task>({}, ctl, std::move(snapshot_name), std::move(ks_tables), std::move(locations), move_files);
    co_return task->id();
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
        , db::consistency_level::LOCAL_ONE // we are querying our own data
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
    if (!storage_options.is_local_type()) {
        throw std::invalid_argument("not able to backup a non-local table");
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
                    snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
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
                        snap_log.warn("Failed to remove {}: {}", component_name, std::current_exception());
                    }
                }
                co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
            });

            if (any_failed) {
                co_return; // don't update status.
            }

            try {
                snap_log.info("Marking {} as uploaded", id);
                db::snapshot_table_helper sth(snap.qp().local());
                info.sstable.state = use_move ? db::snapshot_state::remote : db::snapshot_state::remote_and_local;
                co_await sth.insert_snapshot_sstables(tag, ksname, cfname, local.dc, local.rack, { info.sstable });
            } catch (...) {
                snap_log.error("Error marking {} as uploaded: {}", id, std::current_exception());
            }
        });

        if (p) {
            co_await coroutine::return_exception_ptr(std::move(p));
        }
    });
}
