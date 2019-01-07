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
#include "sstables/remove.hh"
#include "service/priority_manager.hh"
#include "auth/common.hh"
#include "tracing/trace_keyspace_helper.hh"
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>

extern logging::logger dblog;

sstables::sstable::version_types get_highest_supported_format();

static const std::unordered_set<sstring> system_keyspaces = {
                db::system_keyspace::NAME, db::schema_tables::NAME
};

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

// make a set of sstables available at another shard.
template <typename Func>
static future<> forward_sstables_to(shard_id shard, sstring directory, std::vector<sstables::shared_sstable> sstables, global_column_family_ptr cf, Func&& func) {
    return seastar::async([sstables = std::move(sstables), directory, shard, cf, func] () mutable {
        auto infos = boost::copy_range<std::vector<sstables::foreign_sstable_open_info>>(sstables
            | boost::adaptors::transformed([] (auto&& sst) { return sst->get_open_info().get0(); }));

        smp::submit_to(shard, [cf, func, infos = std::move(infos), directory] () mutable {
            return load_sstables_with_open_info(std::move(infos), cf->schema(), directory, [] (auto& p) {
                return true;
            }).then([func] (std::vector<sstables::shared_sstable> sstables) {
                return func(std::move(sstables));
            });
        }).get();
    });
}

template <typename Pred>
static future<std::vector<sstables::shared_sstable>>
load_sstables_with_open_info(std::vector<sstables::foreign_sstable_open_info> ssts_info, schema_ptr s, sstring dir, Pred&& pred) {
    return do_with(std::vector<sstables::shared_sstable>(), [ssts_info = std::move(ssts_info), s, dir, pred] (auto& ssts) mutable {
        return parallel_for_each(std::move(ssts_info), [&ssts, s, dir, pred] (auto& info) mutable {
            if (!pred(info)) {
                return make_ready_future<>();
            }
            auto sst = sstables::make_sstable(s, dir, info.generation, info.version, info.format);
            return sst->load(std::move(info)).then([&ssts, sst] {
                ssts.push_back(std::move(sst));
                return make_ready_future<>();
            });
        }).then([&ssts] () mutable {
            return std::move(ssts);
        });
    });
}

// Return all sstables that need resharding in the system. Only one instance of a shared sstable is returned.
static future<std::vector<sstables::shared_sstable>> get_all_shared_sstables(distributed<database>& db, sstring sstdir, global_column_family_ptr cf) {
    class all_shared_sstables {
        schema_ptr _schema;
        sstring _dir;
        std::unordered_map<int64_t, sstables::shared_sstable> _result;
    public:
        all_shared_sstables(const sstring& sstdir, global_column_family_ptr cf) : _schema(cf->schema()), _dir(sstdir) {}

        future<> operator()(std::vector<sstables::foreign_sstable_open_info> ssts_info) {
            return load_sstables_with_open_info(std::move(ssts_info), _schema, _dir, [this] (auto& info) {
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

// This function will iterate through upload directory in column family,
// and will do the following for each sstable found:
// 1) Mutate sstable level to 0.
// 2) Create hard links to its components in column family dir.
// 3) Remove all of its components in upload directory.
// At the end, it's expected that upload dir is empty and all of its
// previous content was moved to column family dir.
//
// Return a vector containing descriptor of sstables to be loaded.
future<std::vector<sstables::entry_descriptor>>
distributed_loader::flush_upload_dir(distributed<database>& db, sstring ks_name, sstring cf_name) {
    struct work {
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> flushed;
    };

    return do_with(work(), [&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] (work& work) {
        auto& cf = db.local().find_column_family(ks_name, cf_name);

        return lister::scan_dir(fs::path(cf._config.datadir) / "upload", { directory_entry_type::regular },
                [&work] (fs::path parent_dir, directory_entry de) {
            auto comps = sstables::entry_descriptor::make_descriptor(parent_dir.native(), de.name);
            if (comps.component != component_type::TOC) {
                return make_ready_future<>();
            }
            work.descriptors.emplace(comps.generation, std::move(comps));
            return make_ready_future<>();
        }, &column_family::manifest_json_filter).then([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name), &work] {
            work.flushed.reserve(work.descriptors.size());

            return do_for_each(work.descriptors, [&db, ks_name, cf_name, &work] (auto& pair) {
                return db.invoke_on(column_family::calculate_shard_from_sstable_generation(pair.first),
                        [ks_name, cf_name, &work, comps = pair.second] (database& db) {
                    auto& cf = db.find_column_family(ks_name, cf_name);

                    auto sst = sstables::make_sstable(cf.schema(), cf._config.datadir + "/upload", comps.generation,
                        comps.version, comps.format, gc_clock::now(),
                        [] (disk_error_signal_type&) { return error_handler_for_upload_dir(); });
                    auto gen = cf.calculate_generation_for_new_table();

                    // Read toc content as it will be needed for moving and deleting a sstable.
                    return sst->read_toc().then([sst, s = cf.schema()] {
                        if (s->is_counter() && !sst->has_scylla_component()) {
                            return make_exception_future<>(std::runtime_error("Loading non-Scylla SSTables containing counters is not supported. Use sstableloader instead."));
                        }
                        if (s->is_view()) {
                            return make_exception_future<>(std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead."));
                        }
                        return sst->mutate_sstable_level(0);
                    }).then([&cf, sst, gen] {
                        return sst->create_links(cf._config.datadir, gen);
                    }).then([sst] {
                        return sstables::remove_by_toc_name(sst->toc_filename(), error_handler_for_upload_dir());
                    }).then([sst, &cf, gen, comps = comps, &work] () mutable {
                        comps.generation = gen;
                        comps.sstdir = cf._config.datadir;
                        return make_ready_future<sstables::entry_descriptor>(std::move(comps));
                    });
                }).then([&work] (sstables::entry_descriptor comps) mutable {
                    work.flushed.push_back(std::move(comps));
                    return make_ready_future<>();
                });
            });
        }).then([&work] {
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(work.flushed));
        });
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

    return db.invoke_on(column_family::calculate_shard_from_sstable_generation(comps.generation),
            [&db, comps = std::move(comps), func = std::move(func), &pc] (database& local) {

        return with_semaphore(local.sstable_load_concurrency_sem(), 1, [&db, &local, comps = std::move(comps), func = std::move(func), &pc] {
            auto& cf = local.find_column_family(comps.ks, comps.cf);

            auto f = sstables::sstable::load_shared_components(cf.schema(), comps.sstdir, comps.generation, comps.version, comps.format, pc);
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
template <typename Func>
static future<> invoke_all_resharding_jobs(global_column_family_ptr cf, sstring directory, std::vector<sstables::resharding_descriptor> jobs, Func&& func) {
    return parallel_for_each(std::move(jobs), [cf, func, &directory] (sstables::resharding_descriptor& job) mutable {
        return forward_sstables_to(job.reshard_at, directory, std::move(job.sstables), cf,
                [cf, func, level = job.level, max_sstable_bytes = job.max_sstable_bytes] (auto sstables) {
            // compaction manager ensures that only one reshard operation will run per shard.
            auto job = [func, sstables = std::move(sstables), level, max_sstable_bytes] () mutable {
                return func(std::move(sstables), level, max_sstable_bytes);
            };
            return cf->get_compaction_manager().run_resharding_job(&*cf, std::move(job));
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

static future<> populate(distributed<database>& db, sstring datadir) {
    return lister::scan_dir(datadir, { directory_entry_type::directory }, [&db] (fs::path datadir, directory_entry de) {
        auto& ks_name = de.name;
        if (is_system_keyspace(ks_name)) {
            return make_ready_future<>();
        }
        return distributed_loader::populate_keyspace(db, datadir.native(), ks_name);
    });
}

void distributed_loader::reshard(distributed<database>& db, sstring ks_name, sstring cf_name) {
    assert(engine().cpu_id() == 0); // NOTE: should always run on shard 0!

    // ensures that only one column family is resharded at a time (that's okay because
    // actual resharding is parallelized), and that's needed to prevent the same column
    // family from being resharded in parallel (that could happen, for example, if
    // refresh (triggers resharding) is issued by user while resharding is going on).
    static semaphore sem(1);

    with_semaphore(sem, 1, [&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
        return seastar::async([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
            global_column_family_ptr cf(db, ks_name, cf_name);

            if (cf->get_compaction_manager().stopped()) {
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
                    auto creator = [&cf, directory] (shard_id shard) mutable {
                        // we need generation calculated by instance of cf at requested shard,
                        // or resource usage wouldn't be fairly distributed among shards.
                        auto gen = smp::submit_to(shard, [&cf] () {
                            return cf->calculate_generation_for_new_table();
                        }).get0();

                        auto sst = sstables::make_sstable(cf->schema(), directory, gen,
                            get_highest_supported_format(), sstables::sstable::format_types::big,
                            gc_clock::now(), default_io_error_handler_gen());
                        return sst;
                    };
                    auto f = sstables::reshard_sstables(sstables, *cf, creator, max_sstable_bytes, level);

                    return f.then([&cf, sstables = std::move(sstables), directory] (std::vector<sstables::shared_sstable> new_sstables) mutable {
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
                                    cf->remove_ancestors_needed_rewrite(ancestors);
                                });
                            } else {
                                return forward_sstables_to(shard, directory, new_sstables_for_shard, cf, [cf, ancestors = std::move(ancestors)] (auto sstables) {
                                    cf->replace_ancestors_needed_rewrite(std::move(ancestors), std::move(sstables));
                                });
                            }
                        }).then([&cf, sstables] {
                            // schedule deletion of shared sstables after we're certain that new unshared ones were successfully forwarded to respective shards.
                            sstables::delete_atomically(std::move(sstables), *cf->get_large_partition_handler()).handle_exception([op = sstables::background_jobs().start()] (std::exception_ptr eptr) {
                                try {
                                    std::rethrow_exception(eptr);
                                } catch (...) {
                                    dblog.warn("Exception in resharding when deleting sstable file: {}", eptr);
                                }
                            });
                        });
                    });
                });
            }).get();
        });
    });
}

future<> distributed_loader::load_new_sstables(distributed<database>& db, sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables) {
    return parallel_for_each(new_tables, [&db] (auto comps) {
        auto cf_sstable_open = [comps] (column_family& cf, sstables::foreign_sstable_open_info info) {
            auto f = cf.open_sstable(std::move(info), comps.sstdir, comps.generation, comps.version, comps.format);
            return f.then([&cf] (sstables::shared_sstable sst) mutable {
                if (sst) {
                    cf._sstables_opened_but_not_loaded.push_back(sst);
                }
                return make_ready_future<>();
            });
        };
        return distributed_loader::open_sstable(db, comps, cf_sstable_open, service::get_local_compaction_priority());
    }).then([&db, ks, cf] {
        return db.invoke_on_all([ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            return cf.get_row_cache().invalidate([&cf] () noexcept {
                // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                // atomically load all opened sstables into column family.
                for (auto& sst : cf._sstables_opened_but_not_loaded) {
                    cf.load_sstable(sst, true);
                }
                cf._sstables_opened_but_not_loaded.clear();
                cf.trigger_compaction();
            });
        });
    }).then([&db, ks, cf] () mutable {
        return smp::submit_to(0, [&db, ks = std::move(ks), cf = std::move(cf)] () mutable {
            distributed_loader::reshard(db, std::move(ks), std::move(cf));
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
            return seastar::async([sst = std::move(sst), comps = std::move(comps)] () {
                sst->move_to_new_dir_in_thread(comps.sstdir, comps.generation);
            });
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

future<> distributed_loader::populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
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

            if (de.type && *de.type == directory_entry_type::directory && sstables::sstable::is_temp_dir(de.name)) {
                return lister::rmdir(sstdir / de.name);
            }

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

            // push future returned by probe_file into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            futures.push_back(std::move(f));

            return make_ready_future<>();
        }, &column_family::manifest_json_filter).then([&futures] {
            return when_all(futures.begin(), futures.end()).then([] (std::vector<future<>> ret) {
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
        }).then([verifier, sstdir, ks = std::move(ks), cf = std::move(cf)] {
            return do_for_each(*verifier, [sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf), verifier] (auto v) {
                if (v.second.status == component_status::has_temporary_toc_file) {
                    unsigned long gen = v.first;
                    sstables::sstable::version_types version = v.second.version;
                    sstables::sstable::format_types format = v.second.format;

                    if (engine().cpu_id() != 0) {
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
    return seastar::async([&db] {
        // We need to init commitlog on shard0 before it is inited on other shards
        // because it obtains the list of pre-existing segments for replay, which must
        // not include reserve segments created by active commitlogs.
        db.invoke_on(0, [] (database& db) {
            return db.init_commitlog();
        }).get();
        db.invoke_on_all([] (database& db) {
            if (engine().cpu_id() == 0) {
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
                io_check(touch_directory, data_dir + "/" + ksname).get();
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

future<> distributed_loader::init_non_system_keyspaces(distributed<database>& db, distributed<service::storage_proxy>& proxy) {
    return seastar::async([&db, &proxy] {
        db.invoke_on_all([&proxy] (database& db) {
            return db.parse_system_tables(proxy);
        }).get();

        const auto& cfg = db.local().get_config();
        parallel_for_each(cfg.data_file_directories(), [&db] (sstring directory) {
            return populate(db, directory);
        }).get();

        db.invoke_on_all([] (database& db) {
            return parallel_for_each(db.get_non_system_column_families(), [] (lw_shared_ptr<table> table) {
                // Make sure this is called even if the table is empty
                table->mark_ready_for_writes();
                return make_ready_future<>();
            });
        }).get();
    });
}

