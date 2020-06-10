/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "distributed_loader.hh"
#include "database.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/schema_tables.hh"
#include "lister.hh"
#include "sstables/compaction.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/remove.hh"
#include "sstables/sstable_directory.hh"
#include "service/priority_manager.hh"
#include "auth/common.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "db/view/view_update_checks.hh"
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>
#include "db/view/view_update_generator.hh"

extern logging::logger dblog;

static future<> execute_futures(std::vector<future<>>& futures);

static const std::unordered_set<sstring> system_keyspaces = {
                db::system_keyspace::NAME, db::schema_tables::NAME
};

// Not super nice. Adding statefulness to the file. 
static std::unordered_set<sstring> load_prio_keyspaces;
static bool population_started = false;

void distributed_loader::mark_keyspace_as_load_prio(const sstring& ks) {
    assert(!population_started);
    load_prio_keyspaces.insert(ks);
}

bool is_system_keyspace(const sstring& name) {
    return system_keyspaces.find(name) != system_keyspaces.end();
}

static const std::unordered_set<sstring> internal_keyspaces = {
        db::system_distributed_keyspace::NAME,
        db::system_keyspace::NAME,
        db::schema_tables::NAME,
        auth::meta::AUTH_KS,
        tracing::trace_keyspace_helper::KEYSPACE_NAME
};

bool is_internal_keyspace(const sstring& name) {
    return internal_keyspaces.find(name) != internal_keyspaces.end();
}

static io_error_handler error_handler_for_upload_dir() {
    return [] (std::exception_ptr eptr) {
        // do nothing about sstable exception and caller will just rethrow it.
    };
}

io_error_handler error_handler_gen_for_upload_dir(disk_error_signal_type& dummy) {
    return error_handler_for_upload_dir();
}

// TODO: possibly move it to seastar
template <typename Service, typename PtrType, typename Func>
static future<> invoke_shards_with_ptr(std::unordered_set<shard_id> shards, distributed<Service>& s, PtrType ptr, Func&& func) {
    return parallel_for_each(std::move(shards), [&s, &func, ptr] (shard_id id) {
        return s.invoke_on(id, [func, foreign = make_foreign(ptr)] (Service& s) mutable {
            return func(s, std::move(foreign));
        });
    });
}

// global_column_family_ptr provides a way to easily retrieve local instance of a given column family.
class global_column_family_ptr {
    distributed<database>& _db;
    utils::UUID _id;
private:
    column_family& get() const { return _db.local().find_column_family(_id); }
public:
    global_column_family_ptr(distributed<database>& db, sstring ks_name, sstring cf_name)
        : _db(db)
        , _id(_db.local().find_column_family(ks_name, cf_name).schema()->id()) {
    }

    column_family* operator->() const {
        return &get();
    }
    column_family& operator*() const {
        return get();
    }
};

// checks whether or not a given column family is worth resharding by checking if any of its
// sstables has more than one owner shard.
static future<bool> worth_resharding(distributed<database>& db, global_column_family_ptr cf) {
    auto has_shared_sstables = [cf] (database& db) {
        return cf->has_shared_sstables();
    };
    return db.map_reduce0(has_shared_sstables, bool(false), std::logical_or<bool>());
}

static future<std::vector<sstables::shared_sstable>>
load_sstables_with_open_info(std::vector<sstables::foreign_sstable_open_info> ssts_info, global_column_family_ptr cf, sstring dir,
        noncopyable_function<bool (const sstables::foreign_sstable_open_info&)> pred) {
    return do_with(std::vector<sstables::shared_sstable>(), [ssts_info = std::move(ssts_info), cf, dir, pred = std::move(pred)] (auto& ssts) mutable {
        return parallel_for_each(std::move(ssts_info), [&ssts, cf, dir, pred = std::move(pred)] (auto& info) mutable {
            if (!pred(info)) {
                return make_ready_future<>();
            }
            auto sst = cf->make_sstable(dir, info.generation, info.version, info.format);
            return sst->load(std::move(info)).then([&ssts, sst] {
                ssts.push_back(std::move(sst));
                return make_ready_future<>();
            });
        }).then([&ssts] () mutable {
            return std::move(ssts);
        });
    });
}

// make a set of sstables available at another shard.
static future<> forward_sstables_to(shard_id shard, sstring directory, std::vector<sstables::shared_sstable> sstables, global_column_family_ptr cf,
        std::function<future<> (std::vector<sstables::shared_sstable>)> func) {
    return seastar::async([sstables = std::move(sstables), directory, shard, cf, func = std::move(func)] () mutable {
        auto infos = boost::copy_range<std::vector<sstables::foreign_sstable_open_info>>(sstables
            | boost::adaptors::transformed([] (auto&& sst) { return sst->get_open_info().get0(); }));

        smp::submit_to(shard, [cf, func, infos = std::move(infos), directory] () mutable {
            return load_sstables_with_open_info(std::move(infos), cf, directory, [] (auto& p) {
                return true;
            }).then([func] (std::vector<sstables::shared_sstable> sstables) {
                return func(std::move(sstables));
            });
        }).get();
    });
}

// Return all sstables that need resharding in the system. Only one instance of a shared sstable is returned.
static future<std::vector<sstables::shared_sstable>> get_all_shared_sstables(distributed<database>& db, sstring sstdir, global_column_family_ptr cf) {
    class all_shared_sstables {
        sstring _dir;
        global_column_family_ptr _cf;
        std::unordered_map<int64_t, sstables::shared_sstable> _result;
    public:
        all_shared_sstables(const sstring& sstdir, global_column_family_ptr cf) : _dir(sstdir), _cf(cf) {}

        future<> operator()(std::vector<sstables::foreign_sstable_open_info> ssts_info) {
            return load_sstables_with_open_info(std::move(ssts_info), _cf, _dir, [this] (auto& info) {
                // skip loading of shared sstable that is already stored in _result.
                return !_result.count(info.generation);
            }).then([this] (std::vector<sstables::shared_sstable> sstables) {
                for (auto& sst : sstables) {
                    auto gen = sst->generation();
                    _result.emplace(gen, std::move(sst));
                }
                return make_ready_future<>();
            });
        }

        std::vector<sstables::shared_sstable> get() && {
            return boost::copy_range<std::vector<sstables::shared_sstable>>(std::move(_result) | boost::adaptors::map_values);
        }
    };

    return db.map_reduce(all_shared_sstables(sstdir, cf), [cf, sstdir] (database& db) mutable {
        return seastar::async([cf, sstdir] {
            return boost::copy_range<std::vector<sstables::foreign_sstable_open_info>>(cf->sstables_need_rewrite()
                | boost::adaptors::filtered([sstdir] (auto&& sst) { return sst->get_dir() == sstdir; })
                | boost::adaptors::transformed([] (auto&& sst) { return sst->get_open_info().get0(); }));
        });
    });
}

template <typename... Args>
static inline
future<> verification_error(fs::path path, const char* fstr, Args&&... args) {
    auto emsg = fmt::format(fstr, std::forward<Args>(args)...);
    dblog.error("{}: {}", path.string(), emsg);
    return make_exception_future<>(std::runtime_error(emsg));
}

// Verify that all files and directories are owned by current uid
// and that files can be read and directories can be read, written, and looked up (execute)
// No other file types may exist.
future<> distributed_loader::verify_owner_and_mode(fs::path path) {
    return file_stat(path.string(), follow_symlink::no).then([path = std::move(path)] (stat_data sd) {
        // Under docker, we run with euid 0 and there is no reasonable way to enforce that the
        // in-container uid will have the same uid as files mounted from outside the container. So
        // just allow euid 0 as a special case. It should survive the file_accessible() checks below.
        // See #4823.
        if (geteuid() != 0 && sd.uid != geteuid()) {
            return verification_error(std::move(path), "File not owned by current euid: {}. Owner is: {}", geteuid(), sd.uid);
        }
        switch (sd.type) {
        case directory_entry_type::regular: {
            auto f = file_accessible(path.string(), access_flags::read);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "File cannot be accessed for read");
                }
                return make_ready_future<>();
            });
            break;
        }
        case directory_entry_type::directory: {
            auto f = file_accessible(path.string(), access_flags::read | access_flags::write | access_flags::execute);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "Directory cannot be accessed for read, write, and execute");
                }
                return lister::scan_dir(path, {}, [] (fs::path dir, directory_entry de) {
                    return verify_owner_and_mode(dir / de.name);
                });
            });
            break;
        }
        default:
            return verification_error(std::move(path), "Must be either a regular file or a directory (type={})", static_cast<int>(sd.type));
        }
    });
};

future<>
distributed_loader::process_sstable_dir(sharded<sstables::sstable_directory>& dir) {
    return dir.invoke_on_all([&dir] (sstables::sstable_directory& d) {
        // Supposed to be called with the node either down or on behalf of maintenance tasks
        // like nodetool refresh
        return d.process_sstable_dir(service::get_local_streaming_read_priority()).then([&dir, &d] {
            return d.move_foreign_sstables(dir);
        });
    }).then([&dir] {
        return dir.invoke_on_all([&dir] (sstables::sstable_directory& d) {
            return d.commit_directory_changes();
        });
    });
}

future<>
distributed_loader::lock_table(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring cf_name) {
    return dir.invoke_on_all([&db, ks_name, cf_name] (sstables::sstable_directory& d) {
        auto& table = db.local().find_column_family(ks_name, cf_name);
        d.store_phaser(table.write_in_progress());
        return make_ready_future<>();
    });
}

// Helper structure for resharding.
//
// Describes the sstables (represented by their foreign_sstable_open_info) that are shared and
// need to be resharded. Each shard will keep one such descriptor, that contains the list of
// SSTables assigned to it, and their total size. The total size is used to make sure we are
// fairly balancing SSTables among shards.
struct reshard_shard_descriptor {
    sstables::sstable_directory::sstable_info_vector info_vec;
    uint64_t uncompressed_data_size = 0;

    bool total_size_smaller(const reshard_shard_descriptor& rhs) const {
        return uncompressed_data_size < rhs.uncompressed_data_size;
    }

    uint64_t size() const {
        return uncompressed_data_size;
    }
};

// Collects shared SSTables from all shards and returns a vector containing them all.
// This function assumes that the list of SSTables can be fairly big so it is careful to
// manipulate it in a do_for_each loop (which yields) instead of using standard accumulators.
future<sstables::sstable_directory::sstable_info_vector>
collect_all_shared_sstables(sharded<sstables::sstable_directory>& dir) {
    return do_with(sstables::sstable_directory::sstable_info_vector(), [&dir] (sstables::sstable_directory::sstable_info_vector& info_vec) {
        // We want to make sure that each distributed object reshards about the same amount of data.
        // Each sharded object has its own shared SSTables. We can use a clever algorithm in which they
        // all distributely figure out which SSTables to exchange, but we'll keep it simple and move all
        // their foreign_sstable_open_info to a coordinator (the shard who called this function). We can
        // move in bulk and that's efficient. That shard can then distribute the work among all the
        // others who will reshard.
        auto coordinator = this_shard_id();
        // We will first move all of the foreign open info to temporary storage so that we can sort
        // them. We want to distribute bigger sstables first.
        return dir.invoke_on_all([&info_vec, coordinator] (sstables::sstable_directory& d) {
            return smp::submit_to(coordinator, [&info_vec, info = d.retrieve_shared_sstables()] () mutable {
                // We want do_for_each here instead of a loop to avoid stalls. Resharding can be
                // called during node operations too. For example, if it is called to load new
                // SSTables into the system.
                return do_for_each(info, [&info_vec] (sstables::foreign_sstable_open_info& info) {
                    info_vec.push_back(std::move(info));
                });
            });
        }).then([&info_vec] () mutable {
            return make_ready_future<sstables::sstable_directory::sstable_info_vector>(std::move(info_vec));
        });
    });
}

// Given a vector of shared sstables to be resharded, distribute it among all shards.
// The vector is first sorted to make sure that we are moving the biggest SSTables first.
//
// Returns a reshard_shard_descriptor per shard indicating the work that each shard has to do.
future<std::vector<reshard_shard_descriptor>>
distribute_reshard_jobs(sstables::sstable_directory::sstable_info_vector source) {
    return do_with(std::move(source), std::vector<reshard_shard_descriptor>(smp::count),
            [] (sstables::sstable_directory::sstable_info_vector& source, std::vector<reshard_shard_descriptor>& destinations) mutable {
        std::sort(source.begin(), source.end(), [] (const sstables::foreign_sstable_open_info& a, const sstables::foreign_sstable_open_info& b) {
            // Sort on descending SSTable sizes.
            return a.uncompressed_data_size > b.uncompressed_data_size;
        });
        return do_for_each(source, [&destinations] (sstables::foreign_sstable_open_info& info) mutable {
            auto shard_it = boost::min_element(destinations, std::mem_fn(&reshard_shard_descriptor::total_size_smaller));
            shard_it->uncompressed_data_size += info.uncompressed_data_size;
            shard_it->info_vec.push_back(std::move(info));
        }).then([&destinations] () mutable {
            return make_ready_future<std::vector<reshard_shard_descriptor>>(std::move(destinations));
        });
    });
}

future<> run_resharding_jobs(sharded<sstables::sstable_directory>& dir, std::vector<reshard_shard_descriptor> reshard_jobs,
                             sharded<database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator) {

    uint64_t total_size = boost::accumulate(reshard_jobs | boost::adaptors::transformed(std::mem_fn(&reshard_shard_descriptor::size)), uint64_t(0));
    if (total_size == 0) {
        return make_ready_future<>();
    }

    return do_with(std::move(reshard_jobs), [&dir, &db, ks_name, table_name, creator = std::move(creator), total_size] (std::vector<reshard_shard_descriptor>& reshard_jobs) {
        auto total_size_mb = total_size / 1000000.0;
        auto start = std::chrono::steady_clock::now();
        dblog.info("{}", fmt::format("Resharding {:.2f} MB", total_size_mb));

        return dir.invoke_on_all([&dir, &db, &reshard_jobs, ks_name, table_name, creator] (sstables::sstable_directory& d) mutable {
            auto& table = db.local().find_column_family(ks_name, table_name);
            auto info_vec = std::move(reshard_jobs[this_shard_id()].info_vec);
            auto& cm = table.get_compaction_manager();
            auto max_threshold = table.schema()->max_compaction_threshold();
            auto& iop = service::get_local_streaming_read_priority();
            return d.reshard(std::move(info_vec), cm, table, max_threshold, creator, iop).then([&d, &dir] {
                return d.move_foreign_sstables(dir);
            });
        }).then([start, total_size_mb] {
            // add a microsecond to prevent division by zero
            auto now = std::chrono::steady_clock::now() + 1us;
            auto seconds = std::chrono::duration_cast<std::chrono::duration<float>>(now - start).count();
            dblog.info("{}", fmt::format("Resharded {:.2f} MB in {:.2f} seconds, {:.2f} MB/s", total_size_mb, seconds, (total_size_mb / seconds)));
            return make_ready_future<>();
        });
    });
}

// Global resharding function. Done in two parts:
//  - The first part spreads the foreign_sstable_open_info across shards so that all of them are
//    resharding about the same amount of data
//  - The second part calls each shard's distributed object to reshard the SSTables they were
//    assigned.
future<>
distributed_loader::reshard(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator) {
    return collect_all_shared_sstables(dir).then([] (sstables::sstable_directory::sstable_info_vector all_jobs) mutable {
        return distribute_reshard_jobs(std::move(all_jobs));
    }).then([&dir, &db, ks_name, table_name, creator = std::move(creator)] (std::vector<reshard_shard_descriptor> destinations) mutable {
        return run_resharding_jobs(dir, std::move(destinations), db, ks_name, table_name, std::move(creator));
    });
}

future<>
distributed_loader::process_upload_dir(distributed<database>& db, sstring ks, sstring cf) {
    seastar::thread_attributes attr;
    attr.sched_group = db.local().get_streaming_scheduling_group();

    return seastar::async(std::move(attr), [&db, ks = std::move(ks), cf = std::move(cf)] {
        global_column_family_ptr global_table(db, ks, cf);

        sharded<sstables::sstable_directory> directory;
        auto upload = fs::path(global_table->dir()) / "upload";
        directory.start(upload, 4,
            sstables::sstable_directory::need_mutate_level::yes,
            sstables::sstable_directory::lack_of_toc_fatal::no,
            sstables::sstable_directory::enable_dangerous_direct_import_of_cassandra_counters(db.local().get_config().enable_dangerous_direct_import_of_cassandra_counters()),
            sstables::sstable_directory::allow_loading_materialized_view::no,
            [&global_table] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                return global_table->make_sstable(dir.native(), gen, v, f, &error_handler_gen_for_upload_dir);

        }).get();

        auto stop = defer([&directory] {
            directory.stop().get();
        });

        lock_table(directory, db, ks, cf).get();
        process_sstable_dir(directory).get();
        auto highest_generation_seen = directory.map_reduce0(
            std::mem_fn(&sstables::sstable_directory::highest_generation_seen),
            int64_t(0),
            [] (int64_t a, int64_t b) { return std::max(a, b); }
        ).get0();

        auto shard_generation_base = highest_generation_seen / smp::count + 1;

        // We still want to do our best to keep the generation numbers shard-friendly.
        // Each destination shard will manage its own generation counter.
        std::vector<std::atomic<int64_t>> shard_gen(smp::count);
        for (shard_id s = 0; s < smp::count; ++s) {
            shard_gen[s].store(shard_generation_base * smp::count + s, std::memory_order_relaxed);
        }

        reshard(directory, db, ks, cf, [&global_table, upload, &shard_gen] (shard_id shard) mutable {
            // we need generation calculated by instance of cf at requested shard
            auto gen = shard_gen[shard].fetch_add(smp::count, std::memory_order_relaxed);

            return global_table->make_sstable(upload.native(), gen,
                    global_table->get_sstables_manager().get_highest_supported_format(),
                    sstables::sstable::format_types::big, &error_handler_gen_for_upload_dir);
        }).get();
    });
}

// This function will iterate through upload directory in column family,
// and will do the following for each sstable found:
// 1) Mutate sstable level to 0.
// 2) Check if view updates need to be generated from this sstable. If so, leave it intact for now.
// 3) Otherwise, create hard links to its components in column family dir.
// 4) Remove all of its components in upload directory.
// At the end, it's expected that upload dir contains only staging sstables
// which need to wait until view updates are generated from them.
//
// Return a vector containing descriptor of sstables to be loaded.
future<std::vector<sstables::entry_descriptor>>
distributed_loader::flush_upload_dir(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks, sstring ks_name, sstring cf_name) {
    return seastar::async([&db, &sys_dist_ks, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] {
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> flushed;

        auto& cf = db.local().find_column_family(ks_name, cf_name);
        auto upload_dir = fs::path(cf._config.datadir) / "upload";
        verify_owner_and_mode(upload_dir).get();
        lister::scan_dir(upload_dir, { directory_entry_type::regular }, [&descriptors] (fs::path parent_dir, directory_entry de) {
              auto comps = sstables::entry_descriptor::make_descriptor(parent_dir.native(), de.name);
              if (comps.component != component_type::TOC) {
                  return make_ready_future<>();
              }
              descriptors.emplace(comps.generation, std::move(comps));
              return make_ready_future<>();
        }, &sstables::manifest_json_filter).get();

        flushed.reserve(descriptors.size());
        for (auto& [generation, comps] : descriptors) {
            auto descriptors = db.invoke_on(column_family::calculate_shard_from_sstable_generation(generation), [&sys_dist_ks, ks_name, cf_name, comps] (database& db) {
                return seastar::async([&db, &sys_dist_ks, ks_name = std::move(ks_name), cf_name = std::move(cf_name), comps = std::move(comps)] () mutable {
                    auto& cf = db.find_column_family(ks_name, cf_name);
                    auto sst = cf.make_sstable(cf._config.datadir + "/upload", comps.generation, comps.version, comps.format, &error_handler_gen_for_upload_dir);
                    auto gen = cf.calculate_generation_for_new_table();

                    sst->read_toc().get();
                    schema_ptr s = cf.schema();
                    if (s->is_counter() && !sst->has_scylla_component()) {
                        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
                        if (db.get_config().enable_dangerous_direct_import_of_cassandra_counters()) {
                            dblog.info("{} But trying to continue on user's request.", error);
                        } else {
                            dblog.error("{} Use sstableloader instead.", error);
                            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
                        }
                    }
                    if (s->is_view()) {
                        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
                    }
                    sst->mutate_sstable_level(0).get();
                    const bool use_view_update_path = db::view::check_needs_view_update_path(sys_dist_ks.local(), cf, streaming::stream_reason::repair).get0();
                    sstring datadir = cf._config.datadir;
                    if (use_view_update_path) {
                        // Move to staging directory to avoid clashes with future uploads. Unique generation number ensures no collisions.
                        datadir += "/staging";
                    }
                    sst->create_links(datadir, gen).get();
                    sstables::remove_by_toc_name(sst->toc_filename(), error_handler_for_upload_dir()).get();
                    comps.generation = gen;
                    comps.sstdir = std::move(datadir);
                    return std::move(comps);
                });
            }).get0();

            flushed.push_back(std::move(descriptors));
        }
        return std::vector<sstables::entry_descriptor>(std::move(flushed));
    });
}

future<> distributed_loader::open_sstable(distributed<database>& db, sstables::entry_descriptor comps,
        std::function<future<> (column_family&, sstables::foreign_sstable_open_info)> func, const io_priority_class& pc) {
    // loads components of a sstable from shard S and share it with all other
    // shards. Which shard a sstable will be opened at is decided using
    // calculate_shard_from_sstable_generation(), which is the inverse of
    // calculate_generation_for_new_table(). That ensures every sstable is
    // shard-local if reshard wasn't performed. This approach is also expected
    // to distribute evenly the resource usage among all shards.
    static thread_local std::unordered_map<sstring, named_semaphore> named_semaphores;    

    return db.invoke_on(column_family::calculate_shard_from_sstable_generation(comps.generation),
            [&db, comps = std::move(comps), func = std::move(func), &pc] (database& local) {

        // check if we should bypass concurrency throttle for this keyspace
        // we still only allow a single sstable per shard extra to be loaded, 
        // to avoid concurrency explosion
        auto& sem = load_prio_keyspaces.count(comps.ks)
            ? named_semaphores.try_emplace(comps.ks, 1, named_semaphore_exception_factory{comps.ks}).first->second
            : local.sstable_load_concurrency_sem()
            ;

        return with_semaphore(sem, 1, [&db, &local, comps = std::move(comps), func = std::move(func), &pc] {
            auto& cf = local.find_column_family(comps.ks, comps.cf);
            auto sst = cf.make_sstable(comps.sstdir, comps.generation, comps.version, comps.format);
            auto f = sst->load(pc).then([sst = std::move(sst)] {
                return sst->load_shared_components();
            });
            return f.then([&db, comps = std::move(comps), func = std::move(func)] (sstables::sstable_open_info info) {
                // shared components loaded, now opening sstable in all shards that own it with shared components
                return do_with(std::move(info), [&db, comps = std::move(comps), func = std::move(func)] (auto& info) {
                    // All shards that own the sstable is interested in it in addition to shard that
                    // is responsible for its generation. We may need to add manually this shard
                    // because sstable may not contain data that belong to it.
                    auto shards_interested_in_this_sstable = boost::copy_range<std::unordered_set<shard_id>>(info.owners);
                    shard_id shard_responsible_for_generation = column_family::calculate_shard_from_sstable_generation(comps.generation);
                    shards_interested_in_this_sstable.insert(shard_responsible_for_generation);

                    return invoke_shards_with_ptr(std::move(shards_interested_in_this_sstable), db, std::move(info.components),
                            [owners = info.owners, data = info.data.dup(), index = info.index.dup(), comps, func] (database& db, auto components) {
                        auto& cf = db.find_column_family(comps.ks, comps.cf);
                        return func(cf, sstables::foreign_sstable_open_info{std::move(components), owners, data, index});
                    });
                });
            });
        });
    });
}

// invokes each descriptor at its target shard, which involves forwarding sstables too.
static future<> invoke_all_resharding_jobs(global_column_family_ptr cf, sstring directory, std::vector<sstables::resharding_descriptor> jobs,
        std::function<future<> (std::vector<sstables::shared_sstable>, uint32_t level, uint64_t max_sstable_bytes)> func) {
    return parallel_for_each(std::move(jobs), [cf, func, &directory] (sstables::resharding_descriptor& job) mutable {
        return forward_sstables_to(job.reshard_at, directory, std::move(job.sstables), cf,
                [cf, func, level = job.level, max_sstable_bytes = job.max_sstable_bytes] (auto sstables) {
            // compaction manager ensures that only one reshard operation will run per shard.
            auto job = [func, sstables = std::move(sstables), level, max_sstable_bytes] () mutable {
                return func(std::move(sstables), level, max_sstable_bytes);
            };
            return cf->get_compaction_manager().run_custom_job(&*cf, "resharding", std::move(job));
        });
    });
}

static std::vector<sstables::shared_sstable> sstables_for_shard(const std::vector<sstables::shared_sstable>& sstables, shard_id shard) {
    auto belongs_to_shard = [] (const sstables::shared_sstable& sst, unsigned shard) {
        auto& shards = sst->get_shards_for_this_sstable();
        return boost::range::find(shards, shard) != shards.end();
    };

    return boost::copy_range<std::vector<sstables::shared_sstable>>(sstables
        | boost::adaptors::filtered([&] (auto& sst) { return belongs_to_shard(sst, shard); }));
}

void distributed_loader::reshard(distributed<database>& db, sstring ks_name, sstring cf_name) {
    assert(this_shard_id() == 0); // NOTE: should always run on shard 0!

    // ensures that only one column family is resharded at a time (that's okay because
    // actual resharding is parallelized), and that's needed to prevent the same column
    // family from being resharded in parallel (that could happen, for example, if
    // refresh (triggers resharding) is issued by user while resharding is going on).
    static semaphore sem(1);

    // FIXME: discarded future.
    (void)with_semaphore(sem, 1, [&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
        return seastar::async([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
            global_column_family_ptr cf(db, ks_name, cf_name);

            if (!cf->get_compaction_manager().enabled()) {
                return;
            }
            // fast path to detect that this column family doesn't need reshard.
            if (!worth_resharding(db, cf).get0()) {
                dblog.debug("Nothing to reshard for {}.{}", cf->schema()->ks_name(), cf->schema()->cf_name());
                return;
            }

            parallel_for_each(cf->_config.all_datadirs, [&db, cf] (const sstring& directory) {
                auto candidates = get_all_shared_sstables(db, directory, cf).get0();
                dblog.debug("{} candidates for resharding for {}.{}", candidates.size(), cf->schema()->ks_name(), cf->schema()->cf_name());
                auto jobs = cf->get_compaction_strategy().get_resharding_jobs(*cf, std::move(candidates));
                dblog.debug("{} resharding jobs for {}.{}", jobs.size(), cf->schema()->ks_name(), cf->schema()->cf_name());

                return invoke_all_resharding_jobs(cf, directory, std::move(jobs), [directory, &cf] (auto sstables, auto level, auto max_sstable_bytes) {
                    // FIXME: run it in maintenance priority.
                    // Resharding, currently, cannot provide compaction with a snapshot of the sstable set
                    // which spans all shards that input sstables belong to, so expiration is disabled.
                    std::optional<sstables::sstable_set> sstable_set = std::nullopt;
                    sstables::compaction_descriptor descriptor(sstables, std::move(sstable_set), service::get_local_compaction_priority(),
                                                               level, max_sstable_bytes);
                    descriptor.options = sstables::compaction_options::make_reshard();
                    descriptor.creator = [&cf, directory] (shard_id shard) mutable {
                        // we need generation calculated by instance of cf at requested shard,
                        // or resource usage wouldn't be fairly distributed among shards.
                        auto gen = smp::submit_to(shard, [&cf] () {
                            return cf->calculate_generation_for_new_table();
                        }).get0();

                        return cf->make_sstable(directory, gen,
                            cf->get_sstables_manager().get_highest_supported_format(),
                            sstables::sstable::format_types::big);
                    };
                    auto f = sstables::compact_sstables(std::move(descriptor), *cf);

                    return f.then([&cf, sstables = std::move(sstables), directory] (sstables::compaction_info info) mutable {
                        auto new_sstables = std::move(info.new_sstables);
                        // an input sstable may belong to shard 1 and 2 and only have data which
                        // token belongs to shard 1. That means resharding will only create a
                        // sstable for shard 1, but both shards opened the sstable. So our code
                        // below should ask both shards to remove the resharded table, or it
                        // wouldn't be deleted by our deletion manager, and resharding would be
                        // triggered again in the subsequent boot.
                        return parallel_for_each(boost::irange(0u, smp::count), [&cf, directory, sstables, new_sstables] (auto shard) {
                            auto old_sstables_for_shard = sstables_for_shard(sstables, shard);
                            // nothing to do if no input sstable belongs to this shard.
                            if (old_sstables_for_shard.empty()) {
                                return make_ready_future<>();
                            }
                            auto new_sstables_for_shard = sstables_for_shard(new_sstables, shard);
                            // sanity checks
                            for (auto& sst : new_sstables_for_shard) {
                                auto& shards = sst->get_shards_for_this_sstable();
                                if (shards.size() != 1) {
                                    throw std::runtime_error(format("resharded sstable {} doesn't belong to only one shard", sst->get_filename()));
                                }
                                if (shards.front() != shard) {
                                    throw std::runtime_error(format("resharded sstable {} should belong to shard {:d}", sst->get_filename(), shard));
                                }
                            }

                            std::unordered_set<uint64_t> ancestors;
                            boost::range::transform(old_sstables_for_shard, std::inserter(ancestors, ancestors.end()),
                                std::mem_fn(&sstables::sstable::generation));

                            if (new_sstables_for_shard.empty()) {
                                // handles case where sstable needing rewrite doesn't produce any sstable
                                // for a shard it belongs to when resharded (the reason is explained above).
                                return smp::submit_to(shard, [cf, ancestors = std::move(ancestors)] () mutable {
                                    return cf->remove_ancestors_needed_rewrite(ancestors);
                                });
                            } else {
                                return forward_sstables_to(shard, directory, new_sstables_for_shard, cf, [cf, ancestors = std::move(ancestors)] (std::vector<sstables::shared_sstable> sstables) mutable {
                                    return cf->replace_ancestors_needed_rewrite(std::move(ancestors), std::move(sstables));
                                });
                            }
                        }).then([&cf, sstables] {
                            // schedule deletion of shared sstables after we're certain that new unshared ones were successfully forwarded to respective shards.
                            (void)sstables::delete_atomically(sstables).handle_exception([op = sstables::background_jobs().start()] (std::exception_ptr eptr) {
                                try {
                                    std::rethrow_exception(eptr);
                                } catch (...) {
                                    dblog.warn("Exception in resharding when deleting sstable file: {}", eptr);
                                }
                            }).then([cf, sstables = std::move(sstables)] {
                                // Refresh cache's snapshot of shards involved in resharding to prevent the cache from
                                // holding reference to deleted files which results in disk space not being released.
                                std::unordered_set<shard_id> owner_shards;
                                for (auto& sst : sstables) {
                                    const auto& shards = sst->get_shards_for_this_sstable();
                                    owner_shards.insert(shards.begin(), shards.end());
                                    if (owner_shards.size() == smp::count) {
                                        break;
                                    }
                                }
                                return parallel_for_each(std::move(owner_shards), [cf] (shard_id shard) {
                                    return smp::submit_to(shard, [cf] () mutable {
                                        cf->_cache.refresh_snapshot();
                                    });
                                });
                            });
                        });
                    });
                });
            }).get();
        });
    });
}

future<> distributed_loader::load_new_sstables(distributed<database>& db, distributed<db::view::view_update_generator>& view_update_generator,
        sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables) {
    return parallel_for_each(new_tables, [&] (auto comps) {
        auto cf_sstable_open = [comps] (column_family& cf, sstables::foreign_sstable_open_info info) {
            auto f = cf.open_sstable(std::move(info), comps.sstdir, comps.generation, comps.version, comps.format);
            return f.then([&cf] (sstables::shared_sstable sst) mutable {
                if (sst) {
                    cf._sstables_opened_but_not_loaded.push_back(sst);
                }
                return make_ready_future<>();
            });
        };
        return distributed_loader::open_sstable(db, comps, cf_sstable_open, service::get_local_compaction_priority())
            .handle_exception([comps, ks, cf] (std::exception_ptr ep) {
                auto name = sstables::sstable::filename(comps.sstdir, ks, cf, comps.version, comps.generation, comps.format, sstables::component_type::TOC);
                dblog.error("Failed to open {}: {}", name, ep);
                return make_exception_future<>(ep);
            });
    }).then([&db, &view_update_generator, ks, cf] {
        return db.invoke_on_all([&view_update_generator, ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            return cf.get_row_cache().invalidate([&view_update_generator, &cf] () noexcept {
                // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                // atomically load all opened sstables into column family.
                for (auto& sst : cf._sstables_opened_but_not_loaded) {
                    try {
                        cf.load_sstable(sst, true);
                    } catch(...) {
                        dblog.error("Failed to load {}: {}. Aborting.", sst->toc_filename(), std::current_exception());
                        abort();
                    }
                    if (sst->requires_view_building()) {
                        // FIXME: discarded future.
                        (void)view_update_generator.local().register_staging_sstable(sst, cf.shared_from_this());
                    }
                }
                cf._sstables_opened_but_not_loaded.clear();
                cf.trigger_compaction();
            });
        });
    }).handle_exception([&db, ks, cf] (std::exception_ptr ep) {
        return db.invoke_on_all([ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            cf._sstables_opened_but_not_loaded.clear();
        }).then([ep] {
            return make_exception_future<>(ep);
        });
    });
}

future<sstables::entry_descriptor> distributed_loader::probe_file(distributed<database>& db, sstring sstdir, sstring fname) {
    using namespace sstables;

    entry_descriptor comps = entry_descriptor::make_descriptor(sstdir, fname);

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps.component != component_type::TOC) {
        return make_ready_future<entry_descriptor>(std::move(comps));
    }
    auto cf_sstable_open = [sstdir, comps, fname] (column_family& cf, sstables::foreign_sstable_open_info info) {
        cf.update_sstables_known_generation(comps.generation);
        if (shared_sstable sst = cf.get_staging_sstable(comps.generation)) {
            dblog.warn("SSTable {} is already present in staging/ directory. Moving from staging will be retried.", sst->get_filename());
            return sst->move_to_new_dir(comps.sstdir, comps.generation);
        }
        {
            auto i = boost::range::find_if(*cf._sstables->all(), [gen = comps.generation] (sstables::shared_sstable sst) { return sst->generation() == gen; });
            if (i != cf._sstables->all()->end()) {
                auto new_toc = sstdir + "/" + fname;
                throw std::runtime_error(format("Attempted to add sstable generation {:d} twice: new={} existing={}",
                                                comps.generation, new_toc, (*i)->toc_filename()));
            }
        }
        return cf.open_sstable(std::move(info), sstdir, comps.generation, comps.version, comps.format).then([&cf] (sstables::shared_sstable sst) mutable {
            if (sst) {
                return cf.get_row_cache().invalidate([&cf, sst = std::move(sst)] () mutable noexcept {
                    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                    cf.load_sstable(sst);
                });
            }
            return make_ready_future<>();
        });
    };

    return distributed_loader::open_sstable(db, comps, cf_sstable_open).then_wrapped([fname] (future<> f) {
        try {
            f.get();
        } catch (malformed_sstable_exception& e) {
            dblog.error("malformed sstable {}: {}. Refusing to boot", fname, e.what());
            throw;
        } catch(...) {
            dblog.error("Unrecognized error while processing {}: {}. Refusing to boot",
                fname, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([comps] () mutable {
        return make_ready_future<entry_descriptor>(std::move(comps));
    });
}

static future<> execute_futures(std::vector<future<>>& futures) {
    return seastar::when_all(futures.begin(), futures.end()).then([] (std::vector<future<>> ret) {
        std::exception_ptr eptr;

        for (auto& f : ret) {
            try {
                if (eptr) {
                    f.ignore_ready_future();
                } else {
                    f.get();
                }
            } catch(...) {
                eptr = std::current_exception();
            }
        }

        if (eptr) {
            return make_exception_future<>(eptr);
        }
        return make_ready_future<>();
    });
}

future<> distributed_loader::cleanup_column_family_temp_sst_dirs(sstring sstdir) {
    return do_with(std::vector<future<>>(), [sstdir = std::move(sstdir)] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::directory }, [&futures] (fs::path sstdir, directory_entry de) {
            // push futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            fs::path dirpath = sstdir / de.name;
            if (sstables::sstable::is_temp_dir(dirpath)) {
                dblog.info("Found temporary sstable directory: {}, removing", dirpath);
                futures.push_back(io_check([dirpath = std::move(dirpath)] () { return lister::rmdir(dirpath); }));
            }
            return make_ready_future<>();
        }).then([&futures] {
            return execute_futures(futures);
        });
    });
}

future<> distributed_loader::handle_sstables_pending_delete(sstring pending_delete_dir) {
    return do_with(std::vector<future<>>(), [dir = std::move(pending_delete_dir)] (std::vector<future<>>& futures) {
        return lister::scan_dir(dir, { directory_entry_type::regular }, [&futures] (fs::path dir, directory_entry de) {
            // push nested futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            fs::path file_path = dir / de.name;
            if (file_path.extension() == ".tmp") {
                dblog.info("Found temporary pending_delete log file: {}, deleting", file_path);
                futures.push_back(remove_file(file_path.string()));
            } else if (file_path.extension() == ".log") {
                dblog.info("Found pending_delete log file: {}, replaying", file_path);
                auto f = sstables::replay_pending_delete_log(file_path.string()).then([file_path = std::move(file_path)] {
                    dblog.debug("Replayed {}, removing", file_path);
                    return remove_file(file_path.string());
                });
                futures.push_back(std::move(f));
            } else {
                dblog.debug("Found unknown file in pending_delete directory: {}, ignoring", file_path);
            }
            return make_ready_future<>();
        }).then([&futures] {
            return execute_futures(futures);
        });
    });
}

future<> distributed_loader::do_populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
    // We can catch most errors when we try to load an sstable. But if the TOC
    // file is the one missing, we won't try to load the sstable at all. This
    // case is still an invalid case, but it is way easier for us to treat it
    // by waiting for all files to be loaded, and then checking if we saw a
    // file during scan_dir, without its corresponding TOC.
    enum class component_status {
        has_some_file,
        has_toc_file,
        has_temporary_toc_file,
    };

    struct sstable_descriptor {
        component_status status;
        sstables::sstable::version_types version;
        sstables::sstable::format_types format;
    };

    auto verifier = make_lw_shared<std::unordered_map<unsigned long, sstable_descriptor>>();

    return do_with(std::vector<future<>>(), [&db, sstdir = std::move(sstdir), verifier, ks, cf] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::regular }, [&db, verifier, &futures] (fs::path sstdir, directory_entry de) {
            // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")

            // push future returned by probe_file into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            auto f = distributed_loader::probe_file(db, sstdir.native(), de.name).then([verifier, sstdir, de] (auto entry) {
                if (entry.component == component_type::TemporaryStatistics) {
                    return remove_file(sstables::sstable::filename(sstdir.native(), entry.ks, entry.cf, entry.version, entry.generation,
                        entry.format, component_type::TemporaryStatistics));
                }

                if (verifier->count(entry.generation)) {
                    if (verifier->at(entry.generation).status == component_status::has_toc_file) {
                        fs::path file_path(sstdir / de.name);
                        if (entry.component == component_type::TOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. TOC file already processed", file_path.native());
                        } else if (entry.component == component_type::TemporaryTOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. Temporary TOC file found after TOC file was processed", file_path.native());
                        }
                    } else if (entry.component == component_type::TOC) {
                        verifier->at(entry.generation).status = component_status::has_toc_file;
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->at(entry.generation).status = component_status::has_temporary_toc_file;
                    }
                } else {
                    if (entry.component == component_type::TOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_toc_file, entry.version, entry.format});
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_temporary_toc_file, entry.version, entry.format});
                    } else {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_some_file, entry.version, entry.format});
                    }
                }
                return make_ready_future<>();
            });

            futures.push_back(std::move(f));

            return make_ready_future<>();
        }, &sstables::manifest_json_filter).then([&futures] {
            return execute_futures(futures);
        }).then([verifier, sstdir, ks = std::move(ks), cf = std::move(cf)] {
            return do_for_each(*verifier, [sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf), verifier] (auto v) {
                if (v.second.status == component_status::has_temporary_toc_file) {
                    unsigned long gen = v.first;
                    sstables::sstable::version_types version = v.second.version;
                    sstables::sstable::format_types format = v.second.format;

                    if (this_shard_id() != 0) {
                        dblog.debug("At directory: {}, partial SSTable with generation {} not relevant for this shard, ignoring", sstdir, v.first);
                        return make_ready_future<>();
                    }
                    // shard 0 is the responsible for removing a partial sstable.
                    return sstables::sstable::remove_sstable_with_temp_toc(ks, cf, sstdir, gen, version, format);
                } else if (v.second.status != component_status::has_toc_file) {
                    throw sstables::malformed_sstable_exception(format("At directory: {}: no TOC found for SSTable with generation {:d}!. Refusing to boot", sstdir, v.first));
                }
                return make_ready_future<>();
            });
        });
    });

}

future<> distributed_loader::populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
    return async([&db, sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf)] {
        assert(this_shard_id() == 0);
        // First pass, cleanup temporary sstable directories and sstables pending delete.
        cleanup_column_family_temp_sst_dirs(sstdir).get();
        auto pending_delete_dir = sstdir + "/" + sstables::sstable::pending_delete_dir_basename();
        auto exists = file_exists(pending_delete_dir).get0();
        if (exists) {
            handle_sstables_pending_delete(pending_delete_dir).get();
        }
        // Second pass, cleanup sstables with temporary TOCs and load the rest.
        do_populate_column_family(db, std::move(sstdir), std::move(ks), std::move(cf)).get();
    });
}

future<> distributed_loader::populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name) {
    auto ksdir = datadir + "/" + ks_name;
    auto& keyspaces = db.local().get_keyspaces();
    auto i = keyspaces.find(ks_name);
    if (i == keyspaces.end()) {
        dblog.warn("Skipping undefined keyspace: {}", ks_name);
        return make_ready_future<>();
    } else {
        dblog.info("Populating Keyspace {}", ks_name);
        auto& ks = i->second;
        auto& column_families = db.local().get_column_families();

        return parallel_for_each(ks.metadata()->cf_meta_data() | boost::adaptors::map_values,
            [ks_name, ksdir, &ks, &column_families, &db] (schema_ptr s) {
                utils::UUID uuid = s->id();
                lw_shared_ptr<column_family> cf = column_families[uuid];
                sstring cfname = cf->schema()->cf_name();
                auto sstdir = ks.column_family_directory(ksdir, cfname, uuid);
                dblog.info("Keyspace {}: Reading CF {} id={} version={}", ks_name, cfname, uuid, s->version());
                return ks.make_directory_for_column_family(cfname, uuid).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir + "/staging", ks_name, cfname);
                }).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir, ks_name, cfname);
                }).handle_exception([ks_name, cfname, sstdir](std::exception_ptr eptr) {
                    std::string msg =
                        format("Exception while populating keyspace '{}' with column family '{}' from file '{}': {}",
                               ks_name, cfname, sstdir, eptr);
                    dblog.error("Exception while populating keyspace '{}' with column family '{}' from file '{}': {}",
                                ks_name, cfname, sstdir, eptr);
                    throw std::runtime_error(msg.c_str());
                });
            });
    }
}

future<> distributed_loader::init_system_keyspace(distributed<database>& db) {
    population_started = true;

    return seastar::async([&db] {
        // We need to init commitlog on shard0 before it is inited on other shards
        // because it obtains the list of pre-existing segments for replay, which must
        // not include reserve segments created by active commitlogs.
        db.invoke_on(0, [] (database& db) {
            return db.init_commitlog();
        }).get();
        db.invoke_on_all([] (database& db) {
            if (this_shard_id() == 0) {
                return make_ready_future<>();
            }
            return db.init_commitlog();
        }).get();

        db.invoke_on_all([] (database& db) {
            auto& cfg = db.get_config();
            bool durable = cfg.data_file_directories().size() > 0;
            db::system_keyspace::make(db, durable, cfg.volatile_system_keyspace_for_testing());
        }).get();

        const auto& cfg = db.local().get_config();
        for (auto& data_dir : cfg.data_file_directories()) {
            for (auto ksname : system_keyspaces) {
                io_check([name = data_dir + "/" + ksname] { return touch_directory(name); }).get();
                distributed_loader::populate_keyspace(db, data_dir, ksname).get();
            }
        }

        db.invoke_on_all([] (database& db) {
            for (auto ksname : system_keyspaces) {
                auto& ks = db.find_keyspace(ksname);
                for (auto& pair : ks.metadata()->cf_meta_data()) {
                    auto cfm = pair.second;
                    auto& cf = db.find_column_family(cfm);
                    cf.mark_ready_for_writes();
                }
                // for system keyspaces, we only do this post all population, and
                // only as a consistency measure.
                // change this if it is ever needed to sync system keyspace
                // population
                ks.mark_as_populated();
            }
            return make_ready_future<>();
        }).get();
    });
}

future<> distributed_loader::ensure_system_table_directories(distributed<database>& db) {
    return parallel_for_each(system_keyspaces, [&db](sstring ksname) {
        auto& ks = db.local().find_keyspace(ksname);
        return parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
            auto cfm = pair.second;
            return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
        });
    });
}

future<> distributed_loader::init_non_system_keyspaces(distributed<database>& db,
        distributed<service::storage_proxy>& proxy, distributed<service::migration_manager>& mm) {
    return seastar::async([&db, &proxy, &mm] {
        db.invoke_on_all([&proxy, &mm] (database& db) {
            return db.parse_system_tables(proxy, mm);
        }).get();

        const auto& cfg = db.local().get_config();
        using ks_dirs = std::unordered_multimap<sstring, sstring>;

        ks_dirs dirs;

        parallel_for_each(cfg.data_file_directories(), [&db, &dirs] (sstring directory) {
            // we want to collect the directories first, so we can get a full set of potential dirs
            return lister::scan_dir(directory, { directory_entry_type::directory }, [&dirs] (fs::path datadir, directory_entry de) {
                if (!is_system_keyspace(de.name)) {
                    dirs.emplace(de.name, datadir.native());
                }
                return make_ready_future<>();
            });
        }).get();

        db.invoke_on_all([&dirs] (database& db) {
            for (auto& [name, ks] : db.get_keyspaces()) {
                // mark all user keyspaces that are _not_ on disk as already
                // populated.
                if (!dirs.count(ks.metadata()->name())) {
                    ks.mark_as_populated();
                }
            }
        }).get();

        std::vector<future<>> futures;

        // treat "dirs" as immutable to avoid modifying it while still in 
        // a range-iteration. Also to simplify the "finally"
        for (auto i = dirs.begin(); i != dirs.end();) {
            auto& ks_name = i->first;
            auto e = dirs.equal_range(ks_name).second;
            auto j = i++;
            // might have more than one dir for a keyspace iff data_file_directories is > 1 and
            // somehow someone placed sstables in more than one of them for a given ks. (import?) 
            futures.emplace_back(parallel_for_each(j, e, [&](const std::pair<sstring, sstring>& p) {
                auto& datadir = p.second;
                return distributed_loader::populate_keyspace(db, datadir, ks_name);
            }).finally([&] {
                return db.invoke_on_all([ks_name] (database& db) {
                    // can be false if running test environment
                    // or ks_name was just a borked directory not representing
                    // a keyspace in schema tables.
                    if (db.has_keyspace(ks_name)) {
                        db.find_keyspace(ks_name).mark_as_populated();
                    }
                    return make_ready_future<>();
                });
            }));
        }

        execute_futures(futures).get();

        db.invoke_on_all([] (database& db) {
            return parallel_for_each(db.get_non_system_column_families(), [] (lw_shared_ptr<table> table) {
                // Make sure this is called even if the table is empty
                table->mark_ready_for_writes();
                return make_ready_future<>();
            });
        }).get();
    });
}

