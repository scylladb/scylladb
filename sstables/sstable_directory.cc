/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <boost/range/adaptor/map.hpp>
#include "sstables/sstable_directory.hh"
#include "sstables/sstables.hh"
#include "sstables/compaction_manager.hh"
#include "log.hh"
#include "sstable_directory.hh"
#include "lister.hh"
#include "database.hh"

static logging::logger dirlog("sstable_directory");

namespace sstables {

bool manifest_json_filter(const fs::path&, const directory_entry& entry) {
    // Filter out directories. If type of the entry is unknown - check its name.
    if (entry.type.value_or(directory_entry_type::regular) != directory_entry_type::directory && (entry.name == "manifest.json" || entry.name == "schema.cql")) {
        return false;
    }

    return true;
}

sstable_directory::sstable_directory(fs::path sstable_dir,
        unsigned load_parallelism,
        semaphore& load_semaphore,
        need_mutate_level need_mutate_level,
        lack_of_toc_fatal throw_on_missing_toc,
        enable_dangerous_direct_import_of_cassandra_counters eddiocc,
        allow_loading_materialized_view allow_mv,
        sstable_object_from_existing_fn sstable_from_existing)
    : _sstable_dir(std::move(sstable_dir))
    , _load_parallelism(load_parallelism)
    , _load_semaphore(load_semaphore)
    , _need_mutate_level(need_mutate_level)
    , _throw_on_missing_toc(throw_on_missing_toc)
    , _enable_dangerous_direct_import_of_cassandra_counters(eddiocc)
    , _allow_loading_materialized_view(allow_mv)
    , _sstable_object_from_existing_sstable(std::move(sstable_from_existing))
    , _unshared_remote_sstables(smp::count)
{}

void
sstable_directory::handle_component(scan_state& state, sstables::entry_descriptor desc, fs::path filename) {
    // If not owned by us, skip
    if ((desc.generation % smp::count) != this_shard_id()) {
        return;
    }

    dirlog.trace("for SSTable directory, scanning {}", filename);
    state.generations_found.emplace(desc.generation, filename);

    switch (desc.component) {
    case component_type::TemporaryStatistics:
        // We generate TemporaryStatistics when we rewrite the Statistics file,
        // for instance on mutate_level. We should delete it - so we mark it for deletion
        // here, but just the component. The old statistics file should still be there
        // and we'll go with it.
        _files_for_removal.insert(filename.native());
        break;
    case component_type::TOC:
        state.descriptors.emplace(desc.generation, std::move(desc));
        break;
    case component_type::TemporaryTOC:
        state.temp_toc_found.push_back(std::move(desc));
        break;
    default:
        // Do nothing, and will validate when trying to load the file.
        break;
    }
}

void sstable_directory::validate(sstables::shared_sstable sst) const {
    schema_ptr s = sst->get_schema();
    if (s->is_counter() && !sst->has_scylla_component()) {
        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
        if (_enable_dangerous_direct_import_of_cassandra_counters) {
            dirlog.info("{} But trying to continue on user's request.", error);
        } else {
            dirlog.error("{} Use sstableloader instead.", error);
            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
        }
    }
    if (s->is_view() && !_allow_loading_materialized_view) {
        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
    }
}

future<>
sstable_directory::process_descriptor(sstables::entry_descriptor desc, const ::io_priority_class& iop, bool sort_sstables_according_to_owner) {
    if (desc.version > _max_version_seen) {
        _max_version_seen = desc.version;
    }

    auto sst = _sstable_object_from_existing_sstable(_sstable_dir, desc.generation, desc.version, desc.format);
    return sst->load(iop).then([this, sst] {
        validate(sst);
        if (_need_mutate_level) {
            dirlog.trace("Mutating {} to level 0\n", sst->get_filename());
            return sst->mutate_sstable_level(0);
        } else {
            return make_ready_future<>();
        }
    }).then([sst, sort_sstables_according_to_owner, this] {
        if (sort_sstables_according_to_owner) {
            return sort_sstable(sst);
        } else {
            dirlog.debug("Added {} to unsorted sstables list", sst->get_filename());
            _unsorted_sstables.push_back(sst);
            return make_ready_future<>();
        }
    });
}

future<>
sstable_directory::sort_sstable(sstables::shared_sstable sst) {
    return sst->get_open_info().then([sst, this] (sstables::foreign_sstable_open_info info) {
        auto shards = sst->get_shards_for_this_sstable();
        if (shards.size() == 1) {
            if (shards[0] == this_shard_id()) {
                dirlog.trace("{} identified as a local unshared SSTable", sst->get_filename());
                _unshared_local_sstables.push_back(sst);
            } else {
                dirlog.trace("{} identified as a remote unshared SSTable", sst->get_filename());
                _unshared_remote_sstables[shards[0]].push_back(std::move(info));
            }
        } else {
            dirlog.trace("{} identified as a shared SSTable", sst->get_filename());
            _shared_sstable_info.push_back(std::move(info));
        }
        return make_ready_future<>();
    });
}

int64_t
sstable_directory::highest_generation_seen() const {
    return _max_generation_seen;
}

sstables::sstable_version_types
sstable_directory::highest_version_seen() const {
    return _max_version_seen;
}

future<>
sstable_directory::process_sstable_dir(const ::io_priority_class& iop, bool sort_sstables_according_to_owner) {
    dirlog.debug("Start processing directory {} for SSTables", _sstable_dir);

    // It seems wasteful that each shard is repeating this scan, and to some extent it is.
    // However, we still want to open the files and especially call process_dir() in a distributed
    // fashion not to overload any shard. Also in the common case the SSTables will all be
    // unshared and be on the right shard based on their generation number. In light of that there are
    // two advantages of having each shard repeat the directory listing:
    //
    // - The directory listing part already interacts with data_structures inside scan_state. We
    //   would have to either transfer a lot of file information among shards or complicate the code
    //   to make sure they all update their own version of scan_state and then merge it.
    // - If all shards scan in parallel, they can start loading sooner. That is faster than having
    //   a separate step to fetch all files, followed by another step to distribute and process.
    return do_with(scan_state{}, [this, sort_sstables_according_to_owner, &iop] (scan_state& state) {
        return lister::scan_dir(_sstable_dir, { directory_entry_type::regular },
                [this, sort_sstables_according_to_owner, &state] (fs::path parent_dir, directory_entry de) {
            auto comps = sstables::entry_descriptor::make_descriptor(_sstable_dir.native(), de.name);
            handle_component(state, std::move(comps), parent_dir / fs::path(de.name));
            return make_ready_future<>();
        }, &manifest_json_filter).then([this, sort_sstables_according_to_owner, &state, &iop] {
            // Always okay to delete files with a temporary TOC. We want to do it before we process
            // the generations seen: it's okay to reuse those generations since the files will have
            // been deleted anyway.
            for (auto& desc: state.temp_toc_found) {
                auto range = state.generations_found.equal_range(desc.generation);
                for (auto it = range.first; it != range.second; ++it) {
                    auto& path = it->second;
                    dirlog.trace("Scheduling to remove file {}, from an SSTable with a Temporary TOC", path.native());
                    _files_for_removal.insert(path.native());
                }
                state.generations_found.erase(range.first, range.second);
                state.descriptors.erase(desc.generation);
            }

            _max_generation_seen =  boost::accumulate(state.generations_found | boost::adaptors::map_keys, int64_t(0), [] (int64_t a, int64_t b) {
                return std::max<int64_t>(a, b);
            });

            dirlog.debug("After {} scanned, seen generation {}. {} descriptors found, {} different files found ",
                    _sstable_dir, _max_generation_seen, state.descriptors.size(), state.generations_found.size());

            // _descriptors is everything with a TOC. So after we remove this, what's left is
            // SSTables for which a TOC was not found.
            return parallel_for_each_restricted(state.descriptors, [this, sort_sstables_according_to_owner, &state, &iop] (std::tuple<int64_t, sstables::entry_descriptor>&& t) {
                auto& desc = std::get<1>(t);
                state.generations_found.erase(desc.generation);
                // This will try to pre-load this file and throw an exception if it is invalid
                return process_descriptor(std::move(desc), iop, sort_sstables_according_to_owner);
            }).then([this, &state] {
                // For files missing TOC, it depends on where this is coming from.
                // If scylla was supposed to have generated this SSTable, this is not okay and
                // we refuse to proceed. If this coming from, say, an import, then we just delete,
                // log and proceed.
                for (auto& path : state.generations_found | boost::adaptors::map_values) {
                    if (_throw_on_missing_toc) {
                        throw sstables::malformed_sstable_exception(format("At directory: {}: no TOC found for SSTable {}!. Refusing to boot", _sstable_dir.native(), path.native()));
                    } else {
                        dirlog.info("Found incomplete SSTable {} at directory {}. Removing", path.native(), _sstable_dir.native());
                        _files_for_removal.insert(path.native());
                    }
                }
            });
        });
    });
}

future<>
sstable_directory::commit_directory_changes() {
    // Remove all files scheduled for removal
    return parallel_for_each(std::exchange(_files_for_removal, {}), [] (sstring path) {
        dirlog.info("Removing file {}", path);
        return remove_file(std::move(path));
    });
}

future<>
sstable_directory::move_foreign_sstables(sharded<sstable_directory>& source_directory) {
    return parallel_for_each(boost::irange(0u, smp::count), [this, &source_directory] (unsigned shard_id) mutable {
        auto info_vec = std::exchange(_unshared_remote_sstables[shard_id], {});
        if (info_vec.empty()) {
            return make_ready_future<>();
        }
        // Should be empty, since an SSTable that belongs to this shard is not remote.
        assert(shard_id != this_shard_id());
        dirlog.debug("Moving {} unshared SSTables to shard {} ", info_vec.size(), shard_id);
        return source_directory.invoke_on(shard_id, &sstables::sstable_directory::load_foreign_sstables, std::move(info_vec));
    });
}

future<>
sstable_directory::load_foreign_sstables(sstable_info_vector info_vec) {
    return parallel_for_each_restricted(info_vec, [this] (sstables::foreign_sstable_open_info& info) {
        auto sst = _sstable_object_from_existing_sstable(_sstable_dir, info.generation, info.version, info.format);
        return sst->load(std::move(info)).then([sst, this] {
            _unshared_local_sstables.push_back(sst);
            return make_ready_future<>();
        });
    });
}

future<>
sstable_directory::remove_input_sstables_from_resharding(std::vector<sstables::shared_sstable> sstlist) {
    dirlog.debug("Removing {} resharded SSTables", sstlist.size());
    return parallel_for_each(std::move(sstlist), [] (const sstables::shared_sstable& sst) {
        dirlog.trace("Removing resharded SSTable {}", sst->get_filename());
        return sst->unlink().then([sst] {});
    });
}

future<>
sstable_directory::collect_output_sstables_from_resharding(std::vector<sstables::shared_sstable> resharded_sstables) {
    dirlog.debug("Collecting {} resharded SSTables", resharded_sstables.size());
    return parallel_for_each(std::move(resharded_sstables), [this] (sstables::shared_sstable sst) {
        auto shards = sst->get_shards_for_this_sstable();
        assert(shards.size() == 1);
        auto shard = shards[0];

        if (shard == this_shard_id()) {
            dirlog.trace("Collected resharded SSTable {} already local", sst->get_filename());
            _unshared_local_sstables.push_back(std::move(sst));
            return make_ready_future<>();
        }
        dirlog.trace("Collected resharded SSTable {} is remote. Storing it", sst->get_filename());
        return sst->get_open_info().then([this, shard, sst] (sstables::foreign_sstable_open_info info) {
            _unshared_remote_sstables[shard].push_back(std::move(info));
            return make_ready_future<>();
        });
    });
}

future<>
sstable_directory::remove_input_sstables_from_reshaping(std::vector<sstables::shared_sstable> sstlist) {
    // When removing input sstables from reshaping: Those SSTables used to be in the unshared local
    // list. So not only do we have to remove them, we also have to update the list. Because we're
    // dealing with a vector it's easier to just reconstruct the list.
    dirlog.debug("Removing {} reshaped SSTables", sstlist.size());
    return do_with(std::move(sstlist), std::unordered_set<sstables::shared_sstable>(),
            [this] (std::vector<sstables::shared_sstable>& sstlist, std::unordered_set<sstables::shared_sstable>& exclude) {

        for (auto& sst : sstlist) {
            exclude.insert(sst);
        }

        auto old = std::exchange(_unshared_local_sstables, {});

        for (auto& sst : old) {
            if (!exclude.contains(sst)) {
                _unshared_local_sstables.push_back(sst);
            }
        }

        // Do this last for exception safety. If there is an exception on unlink we
        // want to at least leave the SSTable unshared list in a sane state.
        return parallel_for_each(std::move(sstlist), [] (sstables::shared_sstable sst) {
            return sst->unlink();
        }).then([] {
            dirlog.debug("Finished removing all SSTables");
        });
    });
}


future<>
sstable_directory::collect_output_sstables_from_reshaping(std::vector<sstables::shared_sstable> reshaped_sstables) {
    dirlog.debug("Collecting {} reshaped SSTables", reshaped_sstables.size());
    return parallel_for_each(std::move(reshaped_sstables), [this] (sstables::shared_sstable sst) {
        _unshared_local_sstables.push_back(std::move(sst));
        return make_ready_future<>();
    });
}

future<uint64_t> sstable_directory::reshape(compaction_manager& cm, table& table, sstables::compaction_sstable_creator_fn creator, const ::io_priority_class& iop, sstables::reshape_mode mode)
{
    return do_with(uint64_t(0), [this, &cm, &table, creator = std::move(creator), iop, mode] (uint64_t & reshaped_size) mutable {
        return repeat([this, &cm, &table, creator = std::move(creator), iop, &reshaped_size, mode] () mutable {
            auto desc = table.get_compaction_strategy().get_reshaping_job(_unshared_local_sstables, table.schema(), iop, mode);
            if (desc.sstables.empty()) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            if (!reshaped_size) {
                dirlog.info("Table {}.{} with compaction strategy {} found SSTables that need reshape. Starting reshape process", table.schema()->ks_name(), table.schema()->cf_name(), table.get_compaction_strategy().name());
            }

            std::vector<sstables::shared_sstable> sstlist;
            for (auto& sst : desc.sstables) {
                reshaped_size += sst->data_size();
                sstlist.push_back(sst);
            }

            desc.creator = creator;

            return cm.run_custom_job(&table, "reshape", [this, &table, sstlist = std::move(sstlist), desc = std::move(desc)] () mutable {
                return sstables::compact_sstables(std::move(desc), table).then([this, sstlist = std::move(sstlist)] (sstables::compaction_info result) mutable {
                    return remove_input_sstables_from_reshaping(std::move(sstlist)).then([this, new_sstables = std::move(result.new_sstables)] () mutable {
                        return collect_output_sstables_from_reshaping(std::move(new_sstables));
                    });
                });
            }).then([] {
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).then([&reshaped_size] {
            return make_ready_future<uint64_t>(reshaped_size);
        });
    });
}

future<>
sstable_directory::reshard(sstable_info_vector shared_info, compaction_manager& cm, table& table,
                           unsigned max_sstables_per_job, sstables::compaction_sstable_creator_fn creator, const ::io_priority_class& iop)
{
    // Resharding doesn't like empty sstable sets, so bail early. There is nothing
    // to reshard in this shard.
    if (shared_info.empty()) {
        return make_ready_future<>();
    }

    // We want to reshard many SSTables at a time for efficiency. However if we have to many we may
    // be risking OOM.
    auto num_jobs = (shared_info.size() + max_sstables_per_job - 1) / max_sstables_per_job;
    auto sstables_per_job = shared_info.size() / num_jobs;

    using reshard_buckets = std::vector<std::vector<sstables::shared_sstable>>;
    return do_with(reshard_buckets(1), [this, &cm, &table, sstables_per_job, iop, num_jobs, creator = std::move(creator), shared_info = std::move(shared_info)] (reshard_buckets& buckets) mutable {
        return parallel_for_each(shared_info, [this, sstables_per_job, num_jobs, &buckets] (sstables::foreign_sstable_open_info& info) {
            auto sst = _sstable_object_from_existing_sstable(_sstable_dir, info.generation, info.version, info.format);
            return sst->load(std::move(info)).then([this, &buckets, sstables_per_job, num_jobs, sst = std::move(sst)] () mutable {
                // Last bucket gets leftover SSTables
                if ((buckets.back().size() >= sstables_per_job) && (buckets.size() < num_jobs)) {
                    buckets.emplace_back();
                }
                buckets.back().push_back(std::move(sst));
            });
        }).then([this, &cm, &table, &buckets, iop, creator = std::move(creator)] () mutable {
            // There is a semaphore inside the compaction manager in run_resharding_jobs. So we
            // parallel_for_each so the statistics about pending jobs are updated to reflect all
            // jobs. But only one will run in parallel at a time
            return parallel_for_each(buckets, [this, iop, &cm, &table, creator = std::move(creator)] (std::vector<sstables::shared_sstable>& sstlist) mutable {
                return cm.run_custom_job(&table, "resharding", [this, iop, &cm, &table, creator, &sstlist] () {
                    sstables::compaction_descriptor desc(sstlist, {}, iop);
                    desc.options = sstables::compaction_options::make_reshard();
                    desc.creator = std::move(creator);

                    return sstables::compact_sstables(std::move(desc), table).then([this, &sstlist] (sstables::compaction_info result) {
                        // input sstables are moved, to guarantee their resources are released once we're done
                        // resharding them.
                        return when_all_succeed(collect_output_sstables_from_resharding(std::move(result.new_sstables)), remove_input_sstables_from_resharding(std::move(sstlist))).discard_result();
                    });
                });
            });
        });
    });
}

future<>
sstable_directory::do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func) {
    return parallel_for_each_restricted(_unshared_local_sstables, std::move(func));
}

template <typename Container, typename Func>
future<>
sstable_directory::parallel_for_each_restricted(Container&& C, Func&& func) {
    return do_with(std::move(C), [this, func = std::move(func)] (Container& c) mutable {
      return max_concurrent_for_each(c, _load_parallelism, [this, func = std::move(func)] (auto& el) mutable {
        return with_semaphore(_load_semaphore, 1, [this, func,  el = std::move(el)] () mutable {
            return func(el);
        });
      });
    });
}

void
sstable_directory::store_phaser(utils::phased_barrier::operation op) {
    _operation_barrier.emplace(std::move(op));
}

sstable_directory::sstable_info_vector
sstable_directory::retrieve_shared_sstables() {
    return std::exchange(_shared_sstable_info, {});
}

}
