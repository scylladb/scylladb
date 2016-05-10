/*
 * Copyright (C) 2014 ScyllaDB
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

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "query-result-writer.hh"
#include "nway_merger.hh"
#include "cql3/column_identifier.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"
#include "sstables/compaction.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "locator/simple_snitch.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "mutation_query.hh"
#include "sstable_mutation_readers.hh"
#include <core/fstream.hh>
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "schema_registry.hh"
#include "service/priority_manager.hh"

#include "checked-file-impl.hh"
#include "disk-error-handler.hh"

using namespace std::chrono_literals;

logging::logger dblog("database");

// Slight extension to the flush_queue type.
class column_family::memtable_flush_queue : public utils::flush_queue<db::replay_position> {
public:
    template<typename Func, typename Post>
    auto run_cf_flush(db::replay_position rp, Func&& func, Post&& post) {
        // special case: empty rp, yet still data.
        // We generate a few memtables with no valid, "high_rp", yet
        // still containing data -> actual flush.
        // And to make matters worse, we can initiate a flush of N such
        // tables at the same time.
        // Just queue them at the end of the queue and treat them as such.
        if (rp == db::replay_position() && !empty()) {
            rp = highest_key();
        }
        return run_with_ordered_post_op(rp, std::forward<Func>(func), std::forward<Post>(post));
    }
};

lw_shared_ptr<memtable_list>
column_family::make_memtable_list() {
    auto seal = [this] { return seal_active_memtable(); };
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.max_memtable_size, _config.dirty_memory_region_group);
}

lw_shared_ptr<memtable_list>
column_family::make_streaming_memtable_list() {
    auto seal = [this] { return seal_active_streaming_memtable_delayed(); };
    auto get_schema =  [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.max_streaming_memtable_size, _config.streaming_dirty_memory_region_group);
}

column_family::column_family(schema_ptr schema, config config, db::commitlog& cl, compaction_manager& compaction_manager)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _memtables(make_memtable_list())
    , _streaming_memtables(_config.enable_disk_writes ? make_streaming_memtable_list() : make_memtable_list())
    , _sstables(make_lw_shared<sstable_list>())
    , _cache(_schema, sstables_as_mutation_source(), sstables_as_key_source(), global_cache_tracker())
    , _commitlog(&cl)
    , _compaction_manager(compaction_manager)
    , _flush_queue(std::make_unique<memtable_flush_queue>())
{
    if (!_config.enable_disk_writes) {
        dblog.warn("Writes disabled, column family no durable.");
    }
}

column_family::column_family(schema_ptr schema, config config, no_commitlog cl, compaction_manager& compaction_manager)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _memtables(make_memtable_list())
    , _streaming_memtables(_config.enable_disk_writes ? make_streaming_memtable_list() : make_memtable_list())
    , _sstables(make_lw_shared<sstable_list>())
    , _cache(_schema, sstables_as_mutation_source(), sstables_as_key_source(), global_cache_tracker())
    , _commitlog(nullptr)
    , _compaction_manager(compaction_manager)
    , _flush_queue(std::make_unique<memtable_flush_queue>())
{
    if (!_config.enable_disk_writes) {
        dblog.warn("Writes disabled, column family no durable.");
    }
}

partition_presence_checker
column_family::make_partition_presence_checker(lw_shared_ptr<sstable_list> old_sstables) {
    return [this, old_sstables = std::move(old_sstables)] (partition_key_view key) {
        for (auto&& s : *old_sstables) {
            if (s.second->filter_has_key(*_schema, key)) {
                return partition_presence_checker_result::maybe_exists;
            }
        }
        return partition_presence_checker_result::definitely_doesnt_exist;
    };
}

mutation_source
column_family::sstables_as_mutation_source() {
    return mutation_source([this] (schema_ptr s, const query::partition_range& r, const io_priority_class& pc) {
        return make_sstable_reader(std::move(s), r, pc);
    });
}

// define in .cc, since sstable is forward-declared in .hh
column_family::~column_family() {
}


logalloc::occupancy_stats column_family::occupancy() const {
    logalloc::occupancy_stats res;
    for (auto m : *_memtables) {
        res += m->region().occupancy();
    }
    for (auto m : *_streaming_memtables) {
        res += m->region().occupancy();
    }
    return res;
}

static
bool belongs_to_current_shard(const mutation& m) {
    return dht::shard_of(m.token()) == engine().cpu_id();
}

class range_sstable_reader final : public mutation_reader::impl {
    const query::partition_range& _pr;
    lw_shared_ptr<sstable_list> _sstables;
    mutation_reader _reader;
    // Use a pointer instead of copying, so we don't need to regenerate the reader if
    // the priority changes.
    const io_priority_class& _pc;
public:
    range_sstable_reader(schema_ptr s, lw_shared_ptr<sstable_list> sstables, const query::partition_range& pr, const io_priority_class& pc)
        : _pr(pr)
        , _sstables(std::move(sstables))
        , _pc(pc)
    {
        std::vector<mutation_reader> readers;
        for (const lw_shared_ptr<sstables::sstable>& sst : *_sstables | boost::adaptors::map_values) {
            // FIXME: make sstable::read_range_rows() return ::mutation_reader so that we can drop this wrapper.
            mutation_reader reader = make_mutation_reader<sstable_range_wrapping_reader>(sst, s, pr, pc);
            if (sst->is_shared()) {
                reader = make_filtering_reader(std::move(reader), belongs_to_current_shard);
            }
            readers.emplace_back(std::move(reader));
        }
        _reader = make_combined_reader(std::move(readers));
    }

    range_sstable_reader(range_sstable_reader&&) = delete; // reader takes reference to member fields

    virtual future<mutation_opt> operator()() override {
        return _reader();
    }
};

class single_key_sstable_reader final : public mutation_reader::impl {
    schema_ptr _schema;
    sstables::key _key;
    mutation_opt _m;
    bool _done = false;
    lw_shared_ptr<sstable_list> _sstables;
    // Use a pointer instead of copying, so we don't need to regenerate the reader if
    // the priority changes.
    const io_priority_class& _pc;
public:
    single_key_sstable_reader(schema_ptr schema, lw_shared_ptr<sstable_list> sstables, const partition_key& key, const io_priority_class& pc)
        : _schema(std::move(schema))
        , _key(sstables::key::from_partition_key(*_schema, key))
        , _sstables(std::move(sstables))
        , _pc(pc)
    { }

    virtual future<mutation_opt> operator()() override {
        if (_done) {
            return make_ready_future<mutation_opt>();
        }
        return parallel_for_each(*_sstables | boost::adaptors::map_values, [this](const lw_shared_ptr<sstables::sstable>& sstable) {
            return sstable->read_row(_schema, _key, _pc).then([this](mutation_opt mo) {
                apply(_m, std::move(mo));
            });
        }).then([this] {
            _done = true;
            return std::move(_m);
        });
    }
};

mutation_reader
column_family::make_sstable_reader(schema_ptr s, const query::partition_range& pr, const io_priority_class& pc) const {
    if (pr.is_singular() && pr.start()->value().has_key()) {
        const dht::ring_position& pos = pr.start()->value();
        if (dht::shard_of(pos.token()) != engine().cpu_id()) {
            return make_empty_reader(); // range doesn't belong to this shard
        }
        return make_mutation_reader<single_key_sstable_reader>(std::move(s), _sstables, *pos.key(), pc);
    } else {
        // range_sstable_reader is not movable so we need to wrap it
        return make_mutation_reader<range_sstable_reader>(std::move(s), _sstables, pr, pc);
    }
}

key_source column_family::sstables_as_key_source() const {
    return key_source([this] (const query::partition_range& range, const io_priority_class& pc) {
        std::vector<key_reader> readers;
        readers.reserve(_sstables->size());
        std::transform(_sstables->begin(), _sstables->end(), std::back_inserter(readers), [&] (auto&& entry) {
            auto& sst = entry.second;
            auto rd = sstables::make_key_reader(_schema, sst, range, pc);
            if (sst->is_shared()) {
                rd = make_filtering_reader(std::move(rd), [] (const dht::decorated_key& dk) {
                    return dht::shard_of(dk.token()) == engine().cpu_id();
                });
            }
            return rd;
        });
        return make_combined_reader(_schema, std::move(readers));
    });
}

// Exposed for testing, not performance critical.
future<column_family::const_mutation_partition_ptr>
column_family::find_partition(schema_ptr s, const dht::decorated_key& key) const {
    return do_with(query::partition_range::make_singular(key), [s = std::move(s), this] (auto& range) {
        return do_with(this->make_reader(s, range), [] (mutation_reader& reader) {
            return reader().then([] (mutation_opt&& mo) -> std::unique_ptr<const mutation_partition> {
                if (!mo) {
                    return {};
                }
                return std::make_unique<const mutation_partition>(std::move(mo->partition()));
            });
        });
    });
}

future<column_family::const_mutation_partition_ptr>
column_family::find_partition_slow(schema_ptr s, const partition_key& key) const {
    return find_partition(s, dht::global_partitioner().decorate_key(*s, key));
}

future<column_family::const_row_ptr>
column_family::find_row(schema_ptr s, const dht::decorated_key& partition_key, clustering_key clustering_key) const {
    return find_partition(std::move(s), partition_key).then([clustering_key = std::move(clustering_key)] (const_mutation_partition_ptr p) {
        if (!p) {
            return make_ready_future<const_row_ptr>();
        }
        auto r = p->find_row(clustering_key);
        if (r) {
            // FIXME: remove copy if only one data source
            return make_ready_future<const_row_ptr>(std::make_unique<row>(*r));
        } else {
            return make_ready_future<const_row_ptr>();
        }
    });
}

mutation_reader
column_family::make_reader(schema_ptr s, const query::partition_range& range, const io_priority_class& pc) const {
    if (query::is_wrap_around(range, *s)) {
        // make_combined_reader() can't handle streams that wrap around yet.
        fail(unimplemented::cause::WRAP_AROUND);
    }

    std::vector<mutation_reader> readers;
    readers.reserve(_memtables->size() + _sstables->size());

    // We're assuming that cache and memtables are both read atomically
    // for single-key queries, so we don't need to special case memtable
    // undergoing a move to cache. At any given point in time between
    // deferring points the sum of data in memtable and cache is coherent. If
    // single-key queries for each data source were performed across deferring
    // points, it would be possible that partitions which are ahead of the
    // memtable cursor would be placed behind the cache cursor, resulting in
    // those partitions being missing in the combined reader.
    //
    // We need to handle this in range queries though, as they are always
    // deferring. scanning_reader from memtable.cc is falling back to reading
    // the sstable when memtable is flushed. After memtable is moved to cache,
    // new readers will no longer use the old memtable, but until then
    // performance may suffer. We should fix this when we add support for
    // range queries in cache, so that scans can always be satisfied form
    // memtable and cache only, as long as data is not evicted.
    //
    // https://github.com/scylladb/scylla/issues/309
    // https://github.com/scylladb/scylla/issues/185

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_reader(s, range, pc));
    }

    if (_config.enable_cache) {
        readers.emplace_back(_cache.make_reader(s, range, pc));
    } else {
        readers.emplace_back(make_sstable_reader(s, range, pc));
    }

    return make_combined_reader(std::move(readers));
}

// Not performance critical. Currently used for testing only.
template <typename Func>
future<bool>
column_family::for_all_partitions(schema_ptr s, Func&& func) const {
    static_assert(std::is_same<bool, std::result_of_t<Func(const dht::decorated_key&, const mutation_partition&)>>::value,
                  "bad Func signature");

    struct iteration_state {
        mutation_reader reader;
        Func func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(schema_ptr s, const column_family& cf, Func&& func)
            : reader(cf.make_reader(std::move(s)))
            , func(std::move(func))
        { }
    };

    return do_with(iteration_state(std::move(s), *this, std::move(func)), [] (iteration_state& is) {
        return do_until([&is] { return is.done(); }, [&is] {
            return is.reader().then([&is](mutation_opt&& mo) {
                if (!mo) {
                    is.empty = true;
                } else {
                    is.ok = is.func(mo->decorated_key(), mo->partition());
                }
            });
        }).then([&is] {
            return is.ok;
        });
    });
}

future<bool>
column_family::for_all_partitions_slow(schema_ptr s, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    return for_all_partitions(std::move(s), std::move(func));
}

class lister {
public:
    using dir_entry_types = std::unordered_set<directory_entry_type, enum_hash<directory_entry_type>>;
    using walker_type = std::function<future<> (directory_entry)>;
    using filter_type = std::function<bool (const sstring&)>;
private:
    file _f;
    walker_type _walker;
    filter_type _filter;
    dir_entry_types _expected_type;
    subscription<directory_entry> _listing;
    sstring _dirname;

public:
    lister(file f, dir_entry_types type, walker_type walker, sstring dirname)
            : _f(std::move(f))
            , _walker(std::move(walker))
            , _filter([] (const sstring& fname) { return true; })
            , _expected_type(type)
            , _listing(_f.list_directory([this] (directory_entry de) { return _visit(de); }))
            , _dirname(dirname) {
    }

    lister(file f, dir_entry_types type, walker_type walker, filter_type filter, sstring dirname)
            : lister(std::move(f), type, std::move(walker), dirname) {
        _filter = std::move(filter);
    }

    static future<> scan_dir(sstring name, dir_entry_types type, walker_type walker, filter_type filter = [] (const sstring& fname) { return true; });
protected:
    future<> _visit(directory_entry de) {

        return guarantee_type(std::move(de)).then([this] (directory_entry de) {
            // Hide all synthetic directories and hidden files.
            if ((!_expected_type.count(*(de.type))) || (de.name[0] == '.')) {
                return make_ready_future<>();
            }

            // apply a filter
            if (!_filter(_dirname + "/" + de.name)) {
                return make_ready_future<>();
            }

            return _walker(de);
        });

    }
    future<> done() { return _listing.done(); }
private:
    future<directory_entry> guarantee_type(directory_entry de) {
        if (de.type) {
            return make_ready_future<directory_entry>(std::move(de));
        } else {
            auto f = engine().file_type(_dirname + "/" + de.name);
            return f.then([de = std::move(de)] (std::experimental::optional<directory_entry_type> t) mutable {
                de.type = t;
                return make_ready_future<directory_entry>(std::move(de));
            });
        }
    }
};


future<> lister::scan_dir(sstring name, lister::dir_entry_types type, walker_type walker, filter_type filter) {
    return open_checked_directory(general_disk_error, name).then([type, walker = std::move(walker), filter = std::move(filter), name] (file f) {
            auto l = make_lw_shared<lister>(std::move(f), type, walker, filter, name);
            return l->done().then([l] { });
    });
}

static std::vector<sstring> parse_fname(sstring filename) {
    std::vector<sstring> comps;
    boost::split(comps , filename ,boost::is_any_of(".-"));
    return comps;
}

static bool belongs_to_current_shard(const schema& s, const partition_key& first, const partition_key& last) {
    auto key_shard = [&s] (const partition_key& pk) {
        auto token = dht::global_partitioner().get_token(s, pk);
        return dht::shard_of(token);
    };
    auto s1 = key_shard(first);
    auto s2 = key_shard(last);
    auto me = engine().cpu_id();
    return (s1 <= me) && (me <= s2);
}

static bool belongs_to_current_shard(const schema& s, range<partition_key> r) {
    assert(r.start());
    assert(r.end());
    return belongs_to_current_shard(s, r.start()->value(), r.end()->value());
}

future<sstables::entry_descriptor> column_family::probe_file(sstring sstdir, sstring fname) {

    using namespace sstables;

    entry_descriptor comps = entry_descriptor::make_descriptor(fname);

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps.component != sstable::component_type::TOC) {
        return make_ready_future<entry_descriptor>(std::move(comps));
    }

    update_sstables_known_generation(comps.generation);

    {
        auto i = _sstables->find(comps.generation);
        if (i != _sstables->end()) {
            auto new_toc = sstdir + "/" + fname;
            throw std::runtime_error(sprint("Attempted to add sstable generation %d twice: new=%s existing=%s",
                                            comps.generation, new_toc, i->second->toc_filename()));
        }
    }

    auto sst = std::make_unique<sstables::sstable>(_schema->ks_name(), _schema->cf_name(), sstdir, comps.generation, comps.version, comps.format);
    auto fut = sst->get_sstable_key_range(*_schema);
    return std::move(fut).then([this, sst = std::move(sst), sstdir = std::move(sstdir), comps] (range<partition_key> r) mutable {
        // Checks whether or not sstable belongs to current shard.
        if (!belongs_to_current_shard(*_schema, std::move(r))) {
            dblog.debug("sstable {} not relevant for this shard, ignoring",
                    sstables::sstable::filename(sstdir, _schema->ks_name(), _schema->cf_name(), comps.version, comps.generation, comps.format,
                            sstables::sstable::component_type::Data));
            sstable::mark_sstable_for_deletion(_schema->ks_name(), _schema->cf_name(), sstdir, comps.generation, comps.version, comps.format);
            return make_ready_future<>();
        }

        auto fut = sst->load();
        return std::move(fut).then([this, sst = std::move(sst)] () mutable {
            add_sstable(std::move(*sst));
            return make_ready_future<>();
        });
    }).then_wrapped([fname, comps] (future<> f) {
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
        return make_ready_future<entry_descriptor>(std::move(comps));
    });
}

void column_family::update_stats_for_new_sstable(uint64_t disk_space_used_by_sstable) {
    _stats.live_disk_space_used += disk_space_used_by_sstable;
    _stats.total_disk_space_used += disk_space_used_by_sstable;
    _stats.live_sstable_count++;
}

void column_family::add_sstable(sstables::sstable&& sstable) {
    add_sstable(make_lw_shared(std::move(sstable)));
}

void column_family::add_sstable(lw_shared_ptr<sstables::sstable> sstable) {
    auto generation = sstable->generation();
    // allow in-progress reads to continue using old list
    _sstables = make_lw_shared<sstable_list>(*_sstables);
    update_stats_for_new_sstable(sstable->bytes_on_disk());
    _sstables->emplace(generation, std::move(sstable));
}

future<>
column_family::update_cache(memtable& m, lw_shared_ptr<sstable_list> old_sstables) {
    if (_config.enable_cache) {
       // be careful to use the old sstable list, since the new one will hit every
       // mutation in m.
       return _cache.update(m, make_partition_presence_checker(std::move(old_sstables)));
    } else {
       return make_ready_future<>();
    }
}

// FIXME: because we are coalescing, it could be that mutations belonging to the same
// range end up in two different tables. Technically, we should wait for both. However,
// the only way we have to make this happen now is to wait on all previous writes. This
// certainly is an overkill, so we won't do it. We can fix this longer term by looking
// at the PREPARE messages, and then noting what is the minimum future we should be
// waiting for.
future<>
column_family::seal_active_streaming_memtable_delayed() {
    auto old = _streaming_memtables->back();
    if (old->empty()) {
        return make_ready_future<>();
    }

    if (_streaming_memtables->should_flush()) {
        return seal_active_streaming_memtable();
    }

    if (!_delayed_streaming_flush.armed()) {
            // We don't want to wait for too long, because the incoming mutations will not be available
            // until we flush them to SSTables. On top of that, if the sender ran out of messages, it won't
            // send more until we respond to some - which depends on these futures resolving. Sure enough,
            // the real fix for that second one is to have better communication between sender and receiver,
            // but that's not realistic ATM. If we did have better negotiation here, we would not need a timer
            // at all.
            _delayed_streaming_flush.arm(2s);
    }

    return with_gate(_streaming_flush_gate, [this, old] {
        return _waiting_streaming_flushes.get_shared_future();
    });
}

future<>
column_family::seal_active_streaming_memtable() {
    auto old = _streaming_memtables->back();
    if (old->empty()) {
        return make_ready_future<>();
    }
    _streaming_memtables->add_memtable();
    _streaming_memtables->erase(old);
    return with_gate(_streaming_flush_gate, [this, old] {
        _delayed_streaming_flush.cancel();

        auto current_waiters = std::exchange(_waiting_streaming_flushes, shared_promise<>());
        auto f = current_waiters.get_shared_future(); // for this seal

        with_lock(_sstables_lock.for_read(), [this, old] {
            auto newtab = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(),
                _config.datadir, calculate_generation_for_new_table(),
                sstables::sstable::version_types::ka,
                sstables::sstable::format_types::big);

            newtab->set_unshared();

            auto&& priority = service::get_local_streaming_write_priority();
            // This is somewhat similar to the main memtable flush, but with important differences.
            //
            // The first difference, is that we don't keep aggregate collectd statistics about this one.
            // If we ever need to, we'll keep them separate statistics, but we don't want to polute the
            // main stats about memtables with streaming memtables.
            //
            // Second, we will not bother touching the cache after this flush. The current streaming code
            // will invalidate the ranges it touches, so we won't do it twice. Even when that changes, the
            // cache management code in here will have to differ from the main memtable's one. Please see
            // the comment at flush_streaming_mutations() for details.
            //
            // Lastly, we don't have any commitlog RP to update, and we don't need to deal manipulate the
            // memtable list, since this memtable was not available for reading up until this point.
            return newtab->write_components(*old, incremental_backups_enabled(), priority).then([this, newtab, old] {
                return newtab->open_data();
            }).then([this, old, newtab] () {
                add_sstable(newtab);
                trigger_compaction();
            }).handle_exception([] (auto ep) {
                dblog.error("failed to write streamed sstable: {}", ep);
                return make_exception_future<>(ep);
            });
            // We will also not have any retry logic. If we fail here, we'll fail the streaming and let
            // the upper layers know. They can then apply any logic they want here.
        }).then_wrapped([this, current_waiters = std::move(current_waiters)] (future <> f) mutable {
            if (f.failed()) {
                current_waiters.set_exception(f.get_exception());
            } else {
                current_waiters.set_value();
            }
        });

        return f;
    });
}

future<>
column_family::seal_active_memtable() {
    auto old = _memtables->back();
    dblog.debug("Sealing active memtable, partitions: {}, occupancy: {}", old->partition_count(), old->occupancy());

    if (!_config.enable_disk_writes) {
       return make_ready_future<>();
    }

    if (old->empty()) {
        dblog.debug("Memtable is empty");
        return make_ready_future<>();
    }
    _memtables->add_memtable();

    assert(_highest_flushed_rp < old->replay_position()
    || (_highest_flushed_rp == db::replay_position() && old->replay_position() == db::replay_position())
    );
    _highest_flushed_rp = old->replay_position();

    return _flush_queue->run_cf_flush(old->replay_position(), [old, this] {
        return repeat([this, old] {
            return with_lock(_sstables_lock.for_read(), [this, old] {
                _flush_queue->check_open_gate();
                return try_flush_memtable_to_sstable(old);
            });
        });
    }, [old, this] {
        if (_commitlog) {
            _commitlog->discard_completed_segments(_schema->id(), old->replay_position());
        }
    });
    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<stop_iteration>
column_family::try_flush_memtable_to_sstable(lw_shared_ptr<memtable> old) {
    auto gen = calculate_generation_for_new_table();

    auto newtab = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(),
        _config.datadir, gen,
        sstables::sstable::version_types::ka,
        sstables::sstable::format_types::big);

    auto memtable_size = old->occupancy().total_space();

    _config.cf_stats->pending_memtables_flushes_count++;
    _config.cf_stats->pending_memtables_flushes_bytes += memtable_size;
    newtab->set_unshared();
    dblog.debug("Flushing to {}", newtab->get_filename());
    // Note that due to our sharded architecture, it is possible that
    // in the face of a value change some shards will backup sstables
    // while others won't.
    //
    // This is, in theory, possible to mitigate through a rwlock.
    // However, this doesn't differ from the situation where all tables
    // are coming from a single shard and the toggle happens in the
    // middle of them.
    //
    // The code as is guarantees that we'll never partially backup a
    // single sstable, so that is enough of a guarantee.
    auto&& priority = service::get_local_memtable_flush_priority();
    return newtab->write_components(*old, incremental_backups_enabled(), priority).then([this, newtab, old] {
        return newtab->open_data();
    }).then_wrapped([this, old, newtab, memtable_size] (future<> ret) {
        _config.cf_stats->pending_memtables_flushes_count--;
        _config.cf_stats->pending_memtables_flushes_bytes -= memtable_size;
        dblog.debug("Flushing done");
        try {
            ret.get();

            // We must add sstable before we call update_cache(), because
            // memtable's data after moving to cache can be evicted at any time.
            auto old_sstables = _sstables;
            add_sstable(newtab);
            old->mark_flushed(newtab);

            trigger_compaction();

            return update_cache(*old, std::move(old_sstables)).then_wrapped([this, old] (future<> f) {
                try {
                    f.get();
                } catch(...) {
                    dblog.error("failed to move memtable to cache: {}", std::current_exception());
                }

                _memtables->erase(old);
                dblog.debug("Memtable replaced");

                return make_ready_future<stop_iteration>(stop_iteration::yes);
            });
        } catch (...) {
            dblog.error("failed to write sstable: {}", std::current_exception());
        }
        return sleep(10s).then([] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    });
}

void
column_family::start() {
    // FIXME: add option to disable automatic compaction.
    start_compaction();
}

future<>
column_family::stop() {
    seal_active_memtable();
    seal_active_streaming_memtable();
    return _compaction_manager.remove(this).then([this] {
        // Nest, instead of using when_all, so we don't lose any exceptions.
        return _flush_queue->close().then([this] {
            return _streaming_flush_gate.close();
        });
    }).then([this] {
        return _sstable_deletion_gate.close();
    });
}


future<std::vector<sstables::entry_descriptor>>
column_family::reshuffle_sstables(std::set<int64_t> all_generations, int64_t start) {
    struct work {
        int64_t current_gen;
        std::set<int64_t> all_generations; // Stores generation of all live sstables in the system.
        sstable_list sstables;
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> reshuffled;
        work(int64_t start, std::set<int64_t> gens)
            : current_gen(start ? start : 1)
            , all_generations(gens) {}
    };

    return do_with(work(start, std::move(all_generations)), [this] (work& work) {
        return lister::scan_dir(_config.datadir, { directory_entry_type::regular }, [this, &work] (directory_entry de) {
            auto comps = sstables::entry_descriptor::make_descriptor(de.name);
            if (comps.component != sstables::sstable::component_type::TOC) {
                return make_ready_future<>();
            }
            // Skip generations that were already loaded by Scylla at a previous stage.
            if (work.all_generations.count(comps.generation) != 0) {
                return make_ready_future<>();
            }
            auto sst = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(),
                                                         _config.datadir, comps.generation,
                                                         comps.version, comps.format);
            work.sstables.emplace(comps.generation, std::move(sst));
            work.descriptors.emplace(comps.generation, std::move(comps));
            // FIXME: This is the only place in which we actually issue disk activity aside from
            // directory metadata operations.
            //
            // But without the TOC information, we don't know which files we should link.
            // The alternative to that would be to change create link to try creating a
            // link for all possible files and handling the failures gracefuly, but that's not
            // exactly fast either.
            //
            // Those SSTables are not known by anyone in the system. So we don't have any kind of
            // object describing them. There isn't too much of a choice.
            return work.sstables[comps.generation]->read_toc();
        }, &manifest_json_filter).then([&work] {
            // Note: cannot be parallel because we will be shuffling things around at this stage. Can't race.
            return do_for_each(work.sstables, [&work] (auto& pair) {
                auto&& comps = std::move(work.descriptors.at(pair.first));
                comps.generation = work.current_gen;
                work.reshuffled.push_back(std::move(comps));

                if (pair.first == work.current_gen) {
                    ++work.current_gen;
                    return make_ready_future<>();
                }
                return pair.second->set_generation(work.current_gen++);
            });
        }).then([&work] {
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(work.reshuffled));
        });
    });
}

void column_family::rebuild_statistics() {
    // zeroing live_disk_space_used and live_sstable_count because the
    // sstable list was re-created
    _stats.live_disk_space_used = 0;
    _stats.live_sstable_count = 0;

    for (auto&& tab : boost::range::join(_sstables_compacted_but_not_deleted,
                    // this might seem dangerous, but "move" here just avoids constness,
                    // making the two ranges compatible when compiling with boost 1.55.
                    // Noone is actually moving anything...
                                         std::move(*_sstables) | boost::adaptors::map_values)) {
        update_stats_for_new_sstable(tab->data_size());
    }
}

void
column_family::rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
                                    const std::vector<sstables::shared_sstable>& sstables_to_remove) {
    // Build a new list of _sstables: We remove from the existing list the
    // tables we compacted (by now, there might be more sstables flushed
    // later), and we add the new tables generated by the compaction.
    // We create a new list rather than modifying it in-place, so that
    // on-going reads can continue to use the old list.
    //
    // We only remove old sstables after they are successfully deleted,
    // to avoid a new compaction from ignoring data in the old sstables
    // if the deletion fails (note deletion of shared sstables can take
    // unbounded time, because all shards must agree on the deletion).
    auto current_sstables = _sstables;
    auto new_sstable_list = make_lw_shared<sstable_list>();
    auto new_compacted_but_not_deleted = _sstables_compacted_but_not_deleted;


    std::unordered_set<sstables::shared_sstable> s(
           sstables_to_remove.begin(), sstables_to_remove.end());

    // First, add the new sstables.

    // this might seem dangerous, but "move" here just avoids constness,
    // making the two ranges compatible when compiling with boost 1.55.
    // Noone is actually moving anything...
    for (auto&& tab : boost::range::join(new_sstables, std::move(*current_sstables) | boost::adaptors::map_values)) {
        // Checks if oldtab is a sstable not being compacted.
        if (!s.count(tab)) {
            new_sstable_list->emplace(tab->generation(), tab);
        } else {
            new_compacted_but_not_deleted.push_back(tab);
        }
    }
    _sstables = std::move(new_sstable_list);
    _sstables_compacted_but_not_deleted = std::move(new_compacted_but_not_deleted);

    rebuild_statistics();

    // Second, delete the old sstables.  This is done in the background, so we can
    // consider this compaction completed.
    seastar::with_gate(_sstable_deletion_gate, [this, sstables_to_remove] {
        return sstables::delete_atomically(sstables_to_remove).then([this, sstables_to_remove] {
            auto current_sstables = _sstables;
            auto new_sstable_list = make_lw_shared<sstable_list>();

            std::unordered_set<sstables::shared_sstable> s(
                   sstables_to_remove.begin(), sstables_to_remove.end());
            auto e = boost::range::remove_if(_sstables_compacted_but_not_deleted, [&] (sstables::shared_sstable sst) -> bool {
                return s.count(sst);
            });
            _sstables_compacted_but_not_deleted.erase(e, _sstables_compacted_but_not_deleted.end());
            rebuild_statistics();
        });
    });
}

future<>
column_family::compact_sstables(sstables::compaction_descriptor descriptor, bool cleanup) {
    if (!descriptor.sstables.size()) {
        // if there is nothing to compact, just return.
        return make_ready_future<>();
    }

    return with_lock(_sstables_lock.for_read(), [this, descriptor = std::move(descriptor), cleanup] {
        auto sstables_to_compact = make_lw_shared<std::vector<sstables::shared_sstable>>(std::move(descriptor.sstables));

        auto create_sstable = [this] {
                auto gen = this->calculate_generation_for_new_table();
                // FIXME: use "tmp" marker in names of incomplete sstable
                auto sst = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(), _config.datadir, gen,
                        sstables::sstable::version_types::ka,
                        sstables::sstable::format_types::big);
                sst->set_unshared();
                return sst;
        };
        return sstables::compact_sstables(*sstables_to_compact, *this, create_sstable, descriptor.max_sstable_bytes, descriptor.level,
                cleanup).then([this, sstables_to_compact] (auto new_sstables) {
            return this->rebuild_sstable_list(new_sstables, *sstables_to_compact);
        });
    });
}

static bool needs_cleanup(const lw_shared_ptr<sstables::sstable>& sst,
                   const lw_shared_ptr<std::vector<range<dht::token>>>& owned_ranges,
                   schema_ptr s) {
    auto first = sst->get_first_partition_key(*s);
    auto last = sst->get_last_partition_key(*s);
    auto first_token = dht::global_partitioner().get_token(*s, first);
    auto last_token = dht::global_partitioner().get_token(*s, last);
    range<dht::token> sst_token_range = range<dht::token>::make(first_token, last_token);

    // return true iff sst partition range isn't fully contained in any of the owned ranges.
    for (auto& r : *owned_ranges) {
        if (r.contains(sst_token_range, dht::token_comparator())) {
            return false;
        }
    }
    return true;
}

future<> column_family::cleanup_sstables(sstables::compaction_descriptor descriptor) {
    std::vector<range<dht::token>> r = service::get_local_storage_service().get_local_ranges(_schema->ks_name());
    auto owned_ranges = make_lw_shared<std::vector<range<dht::token>>>(std::move(r));
    auto sstables_to_cleanup = make_lw_shared<std::vector<sstables::shared_sstable>>(std::move(descriptor.sstables));

    return parallel_for_each(*sstables_to_cleanup, [this, owned_ranges = std::move(owned_ranges), sstables_to_cleanup] (auto& sst) {
        if (!owned_ranges->empty() && !needs_cleanup(sst, owned_ranges, _schema)) {
           return make_ready_future<>();
        }

        std::vector<sstables::shared_sstable> sstable_to_compact({ sst });
        return this->compact_sstables(sstables::compaction_descriptor(std::move(sstable_to_compact)), true);
    });
}

future<>
column_family::load_new_sstables(std::vector<sstables::entry_descriptor> new_tables) {
    return parallel_for_each(new_tables, [this] (auto comps) {
        auto sst = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(), _config.datadir, comps.generation, comps.version, comps.format);
        return sst->load().then([this, sst] {
            // This sets in-memory level of sstable to 0.
            // When loading a migrated sstable, it's important to set it to level 0 because
            // leveled compaction relies on a level > 0 having no overlapping sstables.
            // If Scylla reboots before migrated sstable gets compacted, leveled strategy
            // is smart enough to detect a sstable that overlaps and set its in-memory
            // level to 0.
            return sst->set_sstable_level(0);
        }).then([this, sst] {
            auto first = sst->get_first_partition_key(*_schema);
            auto last = sst->get_last_partition_key(*_schema);
            if (belongs_to_current_shard(*_schema, first, last)) {
                this->add_sstable(sst);
            } else {
                sst->mark_for_deletion();
            }
            return make_ready_future<>();
        });
    });
}

// FIXME: this is just an example, should be changed to something more general
// Note: We assume that the column_family does not get destroyed during compaction.
future<>
column_family::compact_all_sstables() {
    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(_sstables->size());
    for (auto&& entry : *_sstables) {
        sstables.push_back(entry.second);
    }
    // FIXME: check if the lower bound min_compaction_threshold() from schema
    // should be taken into account before proceeding with compaction.
    return compact_sstables(sstables::compaction_descriptor(std::move(sstables)));
}

void column_family::start_compaction() {
    set_compaction_strategy(_schema->compaction_strategy());
}

void column_family::trigger_compaction() {
    // Submitting compaction job to compaction manager.
    // #934 - always inc the pending counter, to help
    // indicate the want for compaction.
    _stats.pending_compactions++;
    do_trigger_compaction(); // see below
}

void column_family::do_trigger_compaction() {
    // But only submit if we're not locked out
    if (!_compaction_disabled) {
        _compaction_manager.submit(this);
    }
}

future<> column_family::run_compaction(sstables::compaction_descriptor descriptor) {
    assert(_stats.pending_compactions > 0);
    return compact_sstables(std::move(descriptor)).then([this] {
        // only do this on success. (no exceptions)
        // in that case, we rely on it being still set
        // for reqeueuing
        _stats.pending_compactions--;
    });
}

void column_family::set_compaction_strategy(sstables::compaction_strategy_type strategy) {
    _compaction_strategy = make_compaction_strategy(strategy, _schema->compaction_strategy_options());
}

bool column_family::compaction_manager_queued() const {
    return _compaction_manager_queued;
}

void column_family::set_compaction_manager_queued(bool compaction_manager_queued) {
    _compaction_manager_queued = compaction_manager_queued;
}

bool column_family::pending_compactions() const {
    return _stats.pending_compactions > 0;
}

size_t column_family::sstables_count() {
    return _sstables->size();
}

int64_t column_family::get_unleveled_sstables() const {
    // TODO: when we support leveled compaction, we should return the number of
    // SSTables in L0. If leveled compaction is enabled in this column family,
    // then we should return zero, as we currently do.
    return 0;
}

lw_shared_ptr<sstable_list> column_family::get_sstables() {
    return _sstables;
}

// Gets the list of all sstables in the column family, including ones that are
// not used for active queries because they have already been compacted, but are
// waiting for delete_atomically() to return.
//
// As long as we haven't deleted them, compaction needs to ensure it doesn't
// garbage-collect a tombstone that covers data in an sstable that may not be
// successfully deleted.
lw_shared_ptr<sstable_list> column_family::get_sstables_including_compacted_undeleted() {
    if (_sstables_compacted_but_not_deleted.empty()) {
        return _sstables;
    }
    auto ret = make_lw_shared(*_sstables);
    for (auto&& s : _sstables_compacted_but_not_deleted) {
        ret->insert(std::make_pair(s->generation(), s));
    }
    return ret;
}

inline bool column_family::manifest_json_filter(const sstring& fname) {
    using namespace boost::filesystem;

    path entry_path(fname);
    if (!is_directory(status(entry_path)) && entry_path.filename() == path("manifest.json")) {
        return false;
    }

    return true;
}

future<> column_family::populate(sstring sstdir) {
    // We can catch most errors when we try to load an sstable. But if the TOC
    // file is the one missing, we won't try to load the sstable at all. This
    // case is still an invalid case, but it is way easier for us to treat it
    // by waiting for all files to be loaded, and then checking if we saw a
    // file during scan_dir, without its corresponding TOC.
    enum class status {
        has_some_file,
        has_toc_file,
        has_temporary_toc_file,
    };

    struct sstable_descriptor {
        std::experimental::optional<sstables::sstable::version_types> version;
        std::experimental::optional<sstables::sstable::format_types> format;
    };

    auto verifier = make_lw_shared<std::unordered_map<unsigned long, status>>();
    auto descriptor = make_lw_shared<sstable_descriptor>();

    return do_with(std::vector<future<>>(), [this, sstdir, verifier, descriptor] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::regular }, [this, sstdir, verifier, descriptor, &futures] (directory_entry de) {
            // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")
            auto f = probe_file(sstdir, de.name).then([verifier, descriptor] (auto entry) {
                if (verifier->count(entry.generation)) {
                    if (verifier->at(entry.generation) == status::has_toc_file) {
                        if (entry.component == sstables::sstable::component_type::TOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. TOC file already processed");
                        } else if (entry.component == sstables::sstable::component_type::TemporaryTOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. Temporary TOC file found after TOC file was processed");
                        }
                    } else if (entry.component == sstables::sstable::component_type::TOC) {
                        verifier->at(entry.generation) = status::has_toc_file;
                    } else if (entry.component == sstables::sstable::component_type::TemporaryTOC) {
                        verifier->at(entry.generation) = status::has_temporary_toc_file;
                    }
                } else {
                    if (entry.component == sstables::sstable::component_type::TOC) {
                        verifier->emplace(entry.generation, status::has_toc_file);
                    } else if (entry.component == sstables::sstable::component_type::TemporaryTOC) {
                        verifier->emplace(entry.generation, status::has_temporary_toc_file);
                    } else {
                        verifier->emplace(entry.generation, status::has_some_file);
                    }
                }

                // Retrieve both version and format used for this column family.
                if (!descriptor->version) {
                    descriptor->version = entry.version;
                }
                if (!descriptor->format) {
                    descriptor->format = entry.format;
                }
            });

            // push future returned by probe_file into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            futures.push_back(std::move(f));

            return make_ready_future<>();
        }, &manifest_json_filter).then([&futures] {
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
        }).then([verifier, sstdir, descriptor, this] {
            return parallel_for_each(*verifier, [sstdir = std::move(sstdir), descriptor, this] (auto v) {
                if (v.second == status::has_temporary_toc_file) {
                    unsigned long gen = v.first;
                    assert(descriptor->version);
                    sstables::sstable::version_types version = descriptor->version.value();
                    assert(descriptor->format);
                    sstables::sstable::format_types format = descriptor->format.value();

                    if (engine().cpu_id() != 0) {
                        dblog.debug("At directory: {}, partial SSTable with generation {} not relevant for this shard, ignoring", sstdir, v.first);
                        return make_ready_future<>();
                    }
                    // shard 0 is the responsible for removing a partial sstable.
                    return sstables::sstable::remove_sstable_with_temp_toc(_schema->ks_name(), _schema->cf_name(), sstdir, gen, version, format);
                } else if (v.second != status::has_toc_file) {
                    throw sstables::malformed_sstable_exception(sprint("At directory: %s: no TOC found for SSTable with generation %d!. Refusing to boot", sstdir, v.first));
                }
                return make_ready_future<>();
            });
        });
    }).then([this] {
        // Make sure this is called even if CF is empty
        mark_ready_for_writes();
    });
}

utils::UUID database::empty_version = utils::UUID_gen::get_name_UUID(bytes{});

database::database() : database(db::config())
{}

database::database(const db::config& cfg)
    : _streaming_dirty_memory_region_group(&_dirty_memory_region_group)
    , _cfg(std::make_unique<db::config>(cfg))
    , _memtable_total_space([this] {
        _stats = make_lw_shared<db_stats>();

        auto memtable_total_space = size_t(_cfg->memtable_total_space_in_mb()) << 20;
        if (!memtable_total_space) {
            return memory::stats().total_memory() / 2;
        }
        return memtable_total_space;
    }())
    , _streaming_memtable_total_space(_memtable_total_space / 4)
    , _version(empty_version)
    , _enable_incremental_backups(cfg.incremental_backups())
    , _memtables_throttler(_memtable_total_space, _dirty_memory_region_group)
    , _streaming_throttler(_streaming_memtable_total_space,
                           _streaming_dirty_memory_region_group,
                           &_memtables_throttler
    )
{
    // Start compaction manager with two tasks for handling compaction jobs.
    _compaction_manager.start(2);
    setup_collectd();

    dblog.info("Row: max_vector_size: {}, internal_count: {}", size_t(row::max_vector_size), size_t(row::internal_count));
}

void
database::setup_collectd() {
    _collectd.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("memory"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "dirty")
                , scollectd::make_typed(scollectd::data_type::GAUGE, [this] {
            return _dirty_memory_region_group.memory_used();
    })));

    _collectd.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("memtables"
                , scollectd::per_cpu_plugin_instance
                , "queue_length", "pending_flushes")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _cf_stats.pending_memtables_flushes_count)
    ));

    _collectd.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("memtables"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "pending_flushes")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _cf_stats.pending_memtables_flushes_bytes)
    ));

    _collectd.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("database"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "total_writes")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _stats->total_writes)
    ));

    _collectd.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("database"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "total_reads")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _stats->total_reads)
    ));
}

database::~database() {
}

void database::update_version(const utils::UUID& version) {
    _version = version;
}

const utils::UUID& database::get_version() const {
    return _version;
}

future<> database::populate_keyspace(sstring datadir, sstring ks_name) {
    auto ksdir = datadir + "/" + ks_name;
    auto i = _keyspaces.find(ks_name);
    if (i == _keyspaces.end()) {
        dblog.warn("Skipping undefined keyspace: {}", ks_name);
    } else {
        dblog.info("Populating Keyspace {}", ks_name);
        return lister::scan_dir(ksdir, { directory_entry_type::directory }, [this, ksdir, ks_name] (directory_entry de) {
            auto comps = parse_fname(de.name);
            if (comps.size() < 2) {
                dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
                return make_ready_future<>();
            }

            sstring cfname = comps[0];
            sstring uuidst = comps[1];

            try {
                auto&& uuid = [&] {
                    try {
                        return find_uuid(ks_name, cfname);
                    } catch (const std::out_of_range& e) {
                        std::throw_with_nested(no_such_column_family(ks_name, cfname));
                    }
                }();
                auto& cf = find_column_family(uuid);

                // #870: Check that the directory name matches
                // the current, expected UUID of the CF.
                if (utils::UUID(uuidst) == uuid) {
                    // FIXME: Increase parallelism.
                    auto sstdir = ksdir + "/" + de.name;
                    dblog.info("Keyspace {}: Reading CF {} ", ksdir, cfname);
                    return cf.populate(sstdir);
                }
                // Nope. Warn and ignore.
                dblog.info("Keyspace {}: Skipping obsolete version of CF {} ({})", ksdir, cfname, uuidst);
            } catch (marshal_exception&) {
                // Bogus UUID part of directory name
                dblog.warn("{}, CF {}: malformed UUID: {}. Ignoring", ksdir, comps[0], uuidst);
            } catch (no_such_column_family&) {
                dblog.warn("{}, CF {}: schema not loaded!", ksdir, comps[0]);
            }
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, { directory_entry_type::directory }, [this, datadir] (directory_entry de) {
        auto& ks_name = de.name;
        if (ks_name == "system") {
            return make_ready_future<>();
        }
        return populate_keyspace(datadir, ks_name);
    });
}

template <typename Func>
static future<>
do_parse_system_tables(distributed<service::storage_proxy>& proxy, const sstring& _cf_name, Func&& func) {
    using namespace db::schema_tables;
    static_assert(std::is_same<future<>, std::result_of_t<Func(schema_result_value_type&)>>::value,
                  "bad Func signature");


    auto cf_name = make_lw_shared<sstring>(_cf_name);
    return db::system_keyspace::query(proxy, *cf_name).then([] (auto rs) {
        auto names = std::set<sstring>();
        for (auto& r : rs->rows()) {
            auto keyspace_name = r.template get_nonnull<sstring>("keyspace_name");
            names.emplace(keyspace_name);
        }
        return std::move(names);
    }).then([&proxy, cf_name, func = std::forward<Func>(func)] (std::set<sstring>&& names) mutable {
        return parallel_for_each(names.begin(), names.end(), [&proxy, cf_name, func = std::forward<Func>(func)] (sstring name) mutable {
            if (name == "system") {
                return make_ready_future<>();
            }

            return read_schema_partition_for_keyspace(proxy, *cf_name, name).then([func, cf_name] (auto&& v) mutable {
                return do_with(std::move(v), [func = std::forward<Func>(func), cf_name] (auto& v) {
                    return func(v).then_wrapped([cf_name, &v] (future<> f) {
                        try {
                            f.get();
                        } catch (std::exception& e) {
                            dblog.error("Skipping: {}. Exception occurred when loading system table {}: {}", v.first, *cf_name, e.what());
                        }
                    });
                });
            });
        });
    });
}

future<> database::parse_system_tables(distributed<service::storage_proxy>& proxy) {
    using namespace db::schema_tables;
    return do_parse_system_tables(proxy, db::schema_tables::KEYSPACES, [this] (schema_result_value_type &v) {
        auto ksm = create_keyspace_from_schema_partition(v);
        return create_keyspace(ksm);
    }).then([&proxy, this] {
        return do_parse_system_tables(proxy, db::schema_tables::USERTYPES, [this, &proxy] (schema_result_value_type &v) {
            auto&& user_types = create_types_from_schema_partition(v);
            auto& ks = this->find_keyspace(v.first);
            for (auto&& type : user_types) {
                ks.add_user_type(type);
            }
            return make_ready_future<>();
        });
    }).then([&proxy, this] {
        return do_parse_system_tables(proxy, db::schema_tables::COLUMNFAMILIES, [this, &proxy] (schema_result_value_type &v) {
            return create_tables_from_tables_partition(proxy, v.second).then([this] (std::map<sstring, schema_ptr> tables) {
                return parallel_for_each(tables.begin(), tables.end(), [this] (auto& t) {
                    auto s = t.second;
                    auto& ks = this->find_keyspace(s->ks_name());
                    auto cfg = ks.make_column_family_config(*s);
                    this->add_column_family(s, std::move(cfg));
                    return ks.make_directory_for_column_family(s->cf_name(), s->id()).then([s] {});
                });
            });
        });
    });
}

future<>
database::init_system_keyspace() {
    bool durable = _cfg->data_file_directories().size() > 0;
    db::system_keyspace::make(*this, durable, _cfg->volatile_system_keyspace_for_testing());

    // FIXME support multiple directories
    return io_check(touch_directory, _cfg->data_file_directories()[0] + "/" + db::system_keyspace::NAME).then([this] {
        return populate_keyspace(_cfg->data_file_directories()[0], db::system_keyspace::NAME).then([this]() {
            return init_commitlog();
        });
    }).then([this] {
        auto& ks = find_keyspace(db::system_keyspace::NAME);
        return parallel_for_each(ks.metadata()->cf_meta_data(), [this] (auto& pair) {
            auto cfm = pair.second;
            auto& cf = this->find_column_family(cfm);
            cf.mark_ready_for_writes();
            return make_ready_future<>();
        });
    });
}

future<>
database::load_sstables(distributed<service::storage_proxy>& proxy) {
	return parse_system_tables(proxy).then([this] {
		return populate(_cfg->data_file_directories()[0]);
	});
}

future<>
database::init_commitlog() {
    return db::commitlog::create_commitlog(*_cfg).then([this](db::commitlog&& log) {
        _commitlog = std::make_unique<db::commitlog>(std::move(log));
        _commitlog->add_flush_handler([this](db::cf_id_type id, db::replay_position pos) {
            if (_column_families.count(id) == 0) {
                // the CF has been removed.
                _commitlog->discard_completed_segments(id, pos);
                return;
            }
            _column_families[id]->flush(pos);
        }).release(); // we have longer life time than CL. Ignore reg anchor
    });
}

unsigned
database::shard_of(const dht::token& t) {
    return dht::shard_of(t);
}

unsigned
database::shard_of(const mutation& m) {
    return shard_of(m.token());
}

unsigned
database::shard_of(const frozen_mutation& m) {
    // FIXME: This lookup wouldn't be necessary if we
    // sent the partition key in legacy form or together
    // with token.
    schema_ptr schema = find_schema(m.column_family_id());
    return shard_of(dht::global_partitioner().get_token(*schema, m.key(*schema)));
}

void database::add_keyspace(sstring name, keyspace k) {
    if (_keyspaces.count(name) != 0) {
        throw std::invalid_argument("Keyspace " + name + " already exists");
    }
    _keyspaces.emplace(std::move(name), std::move(k));
}

void database::update_keyspace(const sstring& name) {
    throw std::runtime_error("update keyspace not implemented");
}

void database::drop_keyspace(const sstring& name) {
    _keyspaces.erase(name);
}

void database::add_column_family(schema_ptr schema, column_family::config cfg) {
    schema = local_schema_registry().learn(schema);
    schema->registry_entry()->mark_synced();
    auto uuid = schema->id();
    lw_shared_ptr<column_family> cf;
    if (cfg.enable_commitlog && _commitlog) {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), *_commitlog, _compaction_manager);
    } else {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), column_family::no_commitlog(), _compaction_manager);
    }

    auto ks = _keyspaces.find(schema->ks_name());
    if (ks == _keyspaces.end()) {
        throw std::invalid_argument("Keyspace " + schema->ks_name() + " not defined");
    }
    if (_column_families.count(uuid) != 0) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
    if (_ks_cf_to_uuid.count(kscf) != 0) {
        throw std::invalid_argument("Column family " + schema->cf_name() + " exists");
    }
    ks->second.add_column_family(schema);
    cf->start();
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
}

future<> database::drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func tsf) {
    auto uuid = find_uuid(ks_name, cf_name);
    auto& ks = find_keyspace(ks_name);
    auto cf = _column_families.at(uuid);
    _column_families.erase(uuid);
    ks.metadata()->remove_column_family(cf->schema());
    _ks_cf_to_uuid.erase(std::make_pair(ks_name, cf_name));
    return truncate(ks, *cf, std::move(tsf)).then([this, cf] {
        return cf->stop();
    }).then([this, cf] {
        return make_ready_future<>();
    });
}

const utils::UUID& database::find_uuid(const sstring& ks, const sstring& cf) const {
    try {
        return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
    } catch (...) {
        throw std::out_of_range("");
    }
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(const sstring& name) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(const sstring& name) const {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

bool database::has_keyspace(const sstring& name) const {
    return _keyspaces.count(name) != 0;
}

std::vector<sstring>  database::get_non_system_keyspaces() const {
    std::vector<sstring> res;
    for (auto const &i : _keyspaces) {
        if (i.first != db::system_keyspace::NAME) {
            res.push_back(i.first);
        }
    }
    return res;
}

column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

const column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) const {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

bool database::column_family_exists(const utils::UUID& uuid) const {
    return _column_families.count(uuid);
}

void
keyspace::create_replication_strategy(const std::map<sstring, sstring>& options) {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    _replication_strategy =
            abstract_replication_strategy::create_replication_strategy(
                _metadata->name(), _metadata->strategy_name(),
                ss.get_token_metadata(), options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
}


const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return *_replication_strategy;
}

void
keyspace::set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy) {
    _replication_strategy = std::move(replication_strategy);
}

column_family::config
keyspace::make_column_family_config(const schema& s) const {
    column_family::config cfg;
    cfg.datadir = column_family_directory(s.cf_name(), s.id());
    cfg.enable_disk_reads = _config.enable_disk_reads;
    cfg.enable_disk_writes = _config.enable_disk_writes;
    cfg.enable_commitlog = _config.enable_commitlog;
    cfg.enable_cache = _config.enable_cache;
    cfg.max_memtable_size = _config.max_memtable_size;
    cfg.max_streaming_memtable_size = _config.max_streaming_memtable_size;
    cfg.dirty_memory_region_group = _config.dirty_memory_region_group;
    cfg.streaming_dirty_memory_region_group = _config.streaming_dirty_memory_region_group;
    cfg.cf_stats = _config.cf_stats;
    cfg.enable_incremental_backups = _config.enable_incremental_backups;

    return cfg;
}

sstring
keyspace::column_family_directory(const sstring& name, utils::UUID uuid) const {
    auto uuid_sstring = uuid.to_sstring();
    boost::erase_all(uuid_sstring, "-");
    return sprint("%s/%s-%s", _config.datadir, name, uuid_sstring);
}

future<>
keyspace::make_directory_for_column_family(const sstring& name, utils::UUID uuid) {
    return io_check(touch_directory, column_family_directory(name, uuid));
}

no_such_keyspace::no_such_keyspace(const sstring& ks_name)
    : runtime_error{sprint("Can't find a keyspace %s", ks_name)}
{
}

no_such_column_family::no_such_column_family(const utils::UUID& uuid)
    : runtime_error{sprint("Can't find a column family with UUID %s", uuid)}
{
}

no_such_column_family::no_such_column_family(const sstring& ks_name, const sstring& cf_name)
    : runtime_error{sprint("Can't find a column family %s in keyspace %s", cf_name, ks_name)}
{
}

column_family& database::find_column_family(const schema_ptr& schema) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const {
    return find_column_family(schema->id());
}

void keyspace_metadata::validate() const {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    abstract_replication_strategy::validate_replication_strategy(name(), strategy_name(), ss.get_token_metadata(), strategy_options());
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const {
    try {
        return find_schema(find_uuid(ks_name, cf_name));
    } catch (std::out_of_range&) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

schema_ptr database::find_schema(const utils::UUID& uuid) const {
    return find_column_family(uuid).schema();
}

bool database::has_schema(const sstring& ks_name, const sstring& cf_name) const {
    return _ks_cf_to_uuid.count(std::make_pair(ks_name, cf_name)) > 0;
}


void database::create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    keyspace ks(ksm, std::move(make_keyspace_config(*ksm)));
    ks.create_replication_strategy(ksm->strategy_options());
    _keyspaces.emplace(ksm->name(), std::move(ks));
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    auto i = _keyspaces.find(ksm->name());
    if (i != _keyspaces.end()) {
        return make_ready_future<>();
    }

    create_in_memory_keyspace(ksm);
    auto& datadir = _keyspaces.at(ksm->name()).datadir();
    if (datadir != "") {
        return io_check(touch_directory, datadir);
    } else {
        return make_ready_future<>();
    }
}

std::set<sstring>
database::existing_index_names(const sstring& cf_to_exclude) const {
    std::set<sstring> names;
    for (auto& p : _column_families) {
        auto& cf = *p.second;
        if (!cf_to_exclude.empty() && cf.schema()->cf_name() == cf_to_exclude) {
            continue;
        }
        for (auto& cd : cf.schema()->all_columns_in_select_order()) {
            if (cd.idx_info.index_name) {
                names.emplace(*cd.idx_info.index_name);
            }
        }
    }
    return names;
}

// Based on:
//  - org.apache.cassandra.db.AbstractCell#reconcile()
//  - org.apache.cassandra.db.BufferExpiringCell#reconcile()
//  - org.apache.cassandra.db.BufferDeletedCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        auto c = compare_unsigned(left.value(), right.value());
        if (c != 0) {
            return c;
        }
        if (left.is_live_and_has_ttl()
            && right.is_live_and_has_ttl()
            && left.expiry() != right.expiry())
        {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint32_t) left.deletion_time().time_since_epoch().count()
                   < (uint32_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

struct query_state {
    explicit query_state(schema_ptr s,
                         const query::read_command& cmd,
                         query::result_request request,
                         const std::vector<query::partition_range>& ranges)
            : schema(std::move(s))
            , cmd(cmd)
            , builder(cmd.slice, request)
            , limit(cmd.row_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
    schema_ptr schema;
    const query::read_command& cmd;
    query::result::builder builder;
    uint32_t limit;
    bool range_empty = false;   // Avoid ubsan false-positive when moving after construction
    std::vector<query::partition_range>::const_iterator current_partition_range;
    std::vector<query::partition_range>::const_iterator range_end;
    mutation_reader reader;
    bool done() const {
        return !limit || current_partition_range == range_end;
    }
};

future<lw_shared_ptr<query::result>>
column_family::query(schema_ptr s, const query::read_command& cmd, query::result_request request, const std::vector<query::partition_range>& partition_ranges) {
    utils::latency_counter lc;
    _stats.reads.set_latency(lc);
    auto qs_ptr = std::make_unique<query_state>(std::move(s), cmd, request, partition_ranges);
    auto& qs = *qs_ptr;
    {
        return do_until(std::bind(&query_state::done, &qs), [this, &qs] {
            auto&& range = *qs.current_partition_range++;
            auto add_partition = [&qs] (uint32_t live_rows, mutation&& m) {
                auto pb = qs.builder.add_partition(*qs.schema, m.key());
                m.partition().query_compacted(pb, *qs.schema, live_rows);
            };
            return do_with(querying_reader(qs.schema, as_mutation_source(), range, qs.cmd.slice, qs.limit, qs.cmd.timestamp, add_partition),
                           [] (auto&& rd) { return rd.read(); });
        }).then([qs_ptr = std::move(qs_ptr), &qs] {
            return make_ready_future<lw_shared_ptr<query::result>>(
                    make_lw_shared<query::result>(qs.builder.build()));
        }).finally([lc, this]() mutable {
        _stats.reads.mark(lc);
        if (lc.is_start()) {
            _stats.estimated_read.add(lc.latency(), _stats.reads.count);
        }
        });
    }
}

mutation_source
column_family::as_mutation_source() const {
    return mutation_source([this] (schema_ptr s, const query::partition_range& range, const io_priority_class& pc) {
        return this->make_reader(std::move(s), range, pc);
    });
}

future<lw_shared_ptr<query::result>>
database::query(schema_ptr s, const query::read_command& cmd, query::result_request request, const std::vector<query::partition_range>& ranges) {
    column_family& cf = find_column_family(cmd.cf_id);
    return cf.query(std::move(s), cmd, request, ranges).then([this, s = _stats] (auto&& res) {
        ++s->total_reads;
        return std::move(res);
    });
}

future<reconcilable_result>
database::query_mutations(schema_ptr s, const query::read_command& cmd, const query::partition_range& range) {
    column_family& cf = find_column_family(cmd.cf_id);
    return mutation_query(std::move(s), cf.as_mutation_source(), range, cmd.slice, cmd.row_limit, cmd.timestamp).then([this, s = _stats] (auto&& res) {
        ++s->total_reads;
        return std::move(res);
    });
}

std::unordered_set<sstring> database::get_initial_tokens() {
    std::unordered_set<sstring> tokens;
    sstring tokens_string = get_config().initial_token();
    try {
        boost::split(tokens, tokens_string, boost::is_any_of(sstring(",")));
    } catch (...) {
        throw std::runtime_error(sprint("Unable to parse initial_token=%s", tokens_string));
    }
    tokens.erase("");
    return tokens;
}

std::experimental::optional<gms::inet_address> database::get_replace_address() {
    auto& cfg = get_config();
    sstring replace_address = cfg.replace_address();
    sstring replace_address_first_boot = cfg.replace_address_first_boot();
    try {
        if (!replace_address.empty()) {
            return gms::inet_address(replace_address);
        } else if (!replace_address_first_boot.empty()) {
            return gms::inet_address(replace_address_first_boot);
        }
        return std::experimental::nullopt;
    } catch (...) {
        return std::experimental::nullopt;
    }
}

bool database::is_replacing() {
    sstring replace_address_first_boot = get_config().replace_address_first_boot();
    if (!replace_address_first_boot.empty() && db::system_keyspace::bootstrap_complete()) {
        dblog.info("Replace address on first boot requested; this node is already bootstrapped");
        return false;
    }
    return bool(get_replace_address());
}

std::ostream& operator<<(std::ostream& out, const atomic_cell_or_collection& c) {
    return out << to_hex(c._data);
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    const ::schema& s = *m.schema();
    fprint(os, "{%s.%s key %s data ", s.ks_name(), s.cf_name(), m.decorated_key());
    os << m.partition() << "}";
    return os;
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    return fprint(out, "{column_family: %s/%s}", cf._schema->ks_name(), cf._schema->cf_name());
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    for (auto&& e : db._column_families) {
        auto&& cf = *e.second;
        out << "(" << e.first.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

void
column_family::apply(const mutation& m, const db::replay_position& rp) {
    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    _memtables->active_memtable().apply(m, rp);
    _memtables->seal_on_overflow();
    _stats.writes.mark(lc);
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency(), _stats.writes.count);
    }
}

void
column_family::apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& rp) {
    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    check_valid_rp(rp);
    _memtables->active_memtable().apply(m, m_schema, rp);
    _memtables->seal_on_overflow();
    _stats.writes.mark(lc);
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency(), _stats.writes.count);
    }
}

void column_family::apply_streaming_mutation(schema_ptr m_schema, const frozen_mutation& m) {
    _streaming_memtables->active_memtable().apply(m, m_schema);
    _streaming_memtables->seal_on_overflow();
}

void
column_family::check_valid_rp(const db::replay_position& rp) const {
    if (rp < _highest_flushed_rp) {
        throw replay_position_reordered_exception();
    }
}

future<> database::apply_in_memory(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& rp) {
    try {
        auto& cf = find_column_family(m.column_family_id());
        cf.apply(m, m_schema, rp);
    } catch (no_such_column_family&) {
        dblog.error("Attempting to mutate non-existent table {}", m.column_family_id());
    }
    return make_ready_future<>();
}

future<> database::do_apply(schema_ptr s, const frozen_mutation& m) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    auto uuid = m.column_family_id();
    auto& cf = find_column_family(uuid);
    if (!s->is_synced()) {
        throw std::runtime_error(sprint("attempted to mutate using not synced schema of %s.%s, version=%s",
                                 s->ks_name(), s->cf_name(), s->version()));
    }
    if (cf.commitlog() != nullptr) {
        commitlog_entry_writer cew(s, m);
        return cf.commitlog()->add_entry(uuid, cew).then([&m, this, s](auto rp) {
            try {
                return this->apply_in_memory(m, s, rp);
            } catch (replay_position_reordered_exception&) {
                // expensive, but we're assuming this is super rare.
                // if we failed to apply the mutation due to future re-ordering
                // (which should be the ever only reason for rp mismatch in CF)
                // let's just try again, add the mutation to the CL once more,
                // and assume success in inevitable eventually.
                dblog.debug("replay_position reordering detected");
                return this->apply(s, m);
            }
        });
    }
    return apply_in_memory(m, s, db::replay_position());
}

future<> throttle_state::throttle() {
    if (!should_throttle() && _throttled_requests.empty()) {
        // All is well, go ahead
        return make_ready_future<>();
    }
    // We must throttle, wait a bit
    if (_throttled_requests.empty()) {
        _throttling_timer.arm_periodic(10ms);
    }
    _throttled_requests.emplace_back();
    return _throttled_requests.back().get_future();
}

void throttle_state::unthrottle() {
    // Release one request per free 1MB we have
    // FIXME: improve this
    if (should_throttle()) {
        return;
    }
    size_t avail = std::max((_max_space - _region_group.memory_used()) >> 20, size_t(1));
    avail = std::min(_throttled_requests.size(), avail);
    for (size_t i = 0; i < avail; ++i) {
        _throttled_requests.front().set_value();
        _throttled_requests.pop_front();
    }
    if (_throttled_requests.empty()) {
        _throttling_timer.cancel();
    }
}

future<> database::apply(schema_ptr s, const frozen_mutation& m) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply {}", m.pretty_printer(s));
    }
    return _memtables_throttler.throttle().then([this, &m, s = std::move(s)] {
        return do_apply(std::move(s), m);
    }).then([this, s = _stats] {
        ++s->total_writes;
    });
}

future<> database::apply_streaming_mutation(schema_ptr s, const frozen_mutation& m) {
    if (!s->is_synced()) {
        throw std::runtime_error(sprint("attempted to mutate using not synced schema of %s.%s, version=%s",
                                 s->ks_name(), s->cf_name(), s->version()));
    }

    // TODO (maybe): This will use the same memory region group as memtables, so when
    // one of them throttles, both will.
    //
    // It would be possible to provide further QoS for CQL originated memtables
    // by keeping the streaming memtables into a different region group, with its own
    // separate limit.
    //
    // Because, however, there are many other limits in play that may kick in,
    // I am not convinced that this will ever be a problem.
    //
    // If we do find ourselves in the situation that we are throttling incoming
    // writes due to high level of streaming writes, and we are sure that this
    // is the best solution, we can just change the memtable creation method so
    // that each kind of memtable creates from a different region group - and then
    // update the throttle conditions accordingly.
    return _streaming_throttler.throttle().then([this, &m, s = std::move(s)] {
        auto uuid = m.column_family_id();
        auto& cf = find_column_family(uuid);
        cf.apply_streaming_mutation(s, std::move(m));
    });
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm) {
    // FIXME support multiple directories
    keyspace::config cfg;
    if (_cfg->data_file_directories().size() > 0) {
        cfg.datadir = sprint("%s/%s", _cfg->data_file_directories()[0], ksm.name());
        cfg.enable_disk_writes = !_cfg->enable_in_memory_data_store();
        cfg.enable_disk_reads = true; // we allways read from disk
        cfg.enable_commitlog = ksm.durable_writes() && _cfg->enable_commitlog() && !_cfg->enable_in_memory_data_store();
        cfg.enable_cache = _cfg->enable_cache();
        cfg.max_memtable_size = _memtable_total_space * _cfg->memtable_cleanup_threshold();
        // We should guarantee that at least two memtable are available, otherwise after flush, adding another memtable would
        // easily take us into throttling until the first one is flushed.
        cfg.max_streaming_memtable_size = std::min(cfg.max_memtable_size, _streaming_memtable_total_space / 2);

    } else {
        cfg.datadir = "";
        cfg.enable_disk_writes = false;
        cfg.enable_disk_reads = false;
        cfg.enable_commitlog = false;
        cfg.enable_cache = false;
        cfg.max_memtable_size = std::numeric_limits<size_t>::max();
        // All writes should go to the main memtable list if we're not durable
        cfg.max_streaming_memtable_size = 0;
    }
    cfg.dirty_memory_region_group = &_dirty_memory_region_group;
    cfg.streaming_dirty_memory_region_group = &_streaming_dirty_memory_region_group;
    cfg.cf_stats = &_cf_stats;
    cfg.enable_incremental_backups = _enable_incremental_backups;
    return cfg;
}

namespace db {

std::ostream& operator<<(std::ostream& os, db::consistency_level cl) {
    switch (cl) {
    case db::consistency_level::ANY: return os << "ANY";
    case db::consistency_level::ONE: return os << "ONE";
    case db::consistency_level::TWO: return os << "TWO";
    case db::consistency_level::THREE: return os << "THREE";
    case db::consistency_level::QUORUM: return os << "QUORUM";
    case db::consistency_level::ALL: return os << "ALL";
    case db::consistency_level::LOCAL_QUORUM: return os << "LOCAL_QUORUM";
    case db::consistency_level::EACH_QUORUM: return os << "EACH_QUORUM";
    case db::consistency_level::SERIAL: return os << "SERIAL";
    case db::consistency_level::LOCAL_SERIAL: return os << "LOCAL_SERIAL";
    case db::consistency_level::LOCAL_ONE: return os << "LOCAL";
    default: abort();
    }
}

}

std::ostream&
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return to_hex(x); };
    return fprint(os, "prefix{%s}", ::join(":", ecp._v | boost::adaptors::transformed(enhex)));
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view& acv) {
    if (acv.is_live()) {
        return fprint(os, "atomic_cell{%s;ts=%d;expiry=%d,ttl=%d}",
            to_hex(acv.value()),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fprint(os, "atomic_cell{DEAD;ts=%d;deletion_time=%d}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell& ac) {
    return os << atomic_cell_view(ac);
}

future<>
database::stop() {
    return _compaction_manager.stop().then([this] {
        // try to ensure that CL has done disk flushing
        if (_commitlog != nullptr) {
            return _commitlog->shutdown();
        }
        return make_ready_future<>();
    }).then([this] {
        return parallel_for_each(_column_families, [this] (auto& val_pair) {
            return val_pair.second->stop();
        });
    });
}

future<> database::flush_all_memtables() {
    return parallel_for_each(_column_families, [this] (auto& cfp) {
        return cfp.second->flush();
    });
}

future<> database::truncate(sstring ksname, sstring cfname, timestamp_func tsf) {
    auto& ks = find_keyspace(ksname);
    auto& cf = find_column_family(ksname, cfname);
    return truncate(ks, cf, std::move(tsf));
}

future<> database::truncate(const keyspace& ks, column_family& cf, timestamp_func tsf)
{
    const auto durable = ks.metadata()->durable_writes();
    const auto auto_snapshot = get_config().auto_snapshot();

    future<> f = make_ready_future<>();
    if (durable || auto_snapshot) {
        // TODO:
        // this is not really a guarantee at all that we've actually
        // gotten all things to disk. Again, need queue-ish or something.
        f = cf.flush();
    } else {
        cf.clear();
    }

    return cf.run_with_compaction_disabled([f = std::move(f), &cf, auto_snapshot, tsf = std::move(tsf)]() mutable {
        return f.then([&cf, auto_snapshot, tsf = std::move(tsf)] {
            dblog.debug("Discarding sstable data for truncated CF + indexes");
            // TODO: notify truncation

            return tsf().then([&cf, auto_snapshot](db_clock::time_point truncated_at) {
                future<> f = make_ready_future<>();
                if (auto_snapshot) {
                    auto name = sprint("%d-%s", truncated_at.time_since_epoch().count(), cf.schema()->cf_name());
                    f = cf.snapshot(name);
                }
                return f.then([&cf, truncated_at] {
                    return cf.discard_sstables(truncated_at).then([&cf, truncated_at](db::replay_position rp) {
                        // TODO: indexes.
                        return db::system_keyspace::save_truncation_record(cf, truncated_at, rp);
                    });
                });
            });
        });
    });
}

const sstring& database::get_snitch_name() const {
    return _cfg->endpoint_snitch();
}

// For the filesystem operations, this code will assume that all keyspaces are visible in all shards
// (as we have been doing for a lot of the other operations, like the snapshot itself).
future<> database::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names) {
    std::vector<std::reference_wrapper<keyspace>> keyspaces;

    if (keyspace_names.empty()) {
        // if keyspace names are not given - apply to all existing local keyspaces
        for (auto& ks: _keyspaces) {
            keyspaces.push_back(std::reference_wrapper<keyspace>(ks.second));
        }
    } else {
        for (auto& ksname: keyspace_names) {
            try {
                keyspaces.push_back(std::reference_wrapper<keyspace>(find_keyspace(ksname)));
            } catch (no_such_keyspace& e) {
                return make_exception_future(std::current_exception());
            }
        }
    }

    return parallel_for_each(keyspaces, [this, tag] (auto& ks) {
        return parallel_for_each(ks.get().metadata()->cf_meta_data(), [this, tag] (auto& pair) {
            auto& cf = this->find_column_family(pair.second);
            return cf.clear_snapshot(tag);
         }).then_wrapped([] (future<> f) {
            dblog.debug("Cleared out snapshot directories");
         });
    });
}

future<> update_schema_version_and_announce(distributed<service::storage_proxy>& proxy)
{
    return db::schema_tables::calculate_schema_digest(proxy).then([&proxy] (utils::UUID uuid) {
        return proxy.local().get_db().invoke_on_all([uuid] (database& db) {
            db.update_version(uuid);
            return make_ready_future<>();
        }).then([uuid] {
            return db::system_keyspace::update_schema_version(uuid).then([uuid] {
                return service::get_local_migration_manager().passive_announce(uuid);
            });
        });
    });
}

// Snapshots: snapshotting the files themselves is easy: if more than one CF
// happens to link an SSTable twice, all but one will fail, and we will end up
// with one copy.
//
// The problem for us, is that the snapshot procedure is supposed to leave a
// manifest file inside its directory.  So if we just call snapshot() from
// multiple shards, only the last one will succeed, writing its own SSTables to
// the manifest leaving all other shards' SSTables unaccounted for.
//
// Moreover, for things like drop table, the operation should only proceed when the
// snapshot is complete. That includes the manifest file being correctly written,
// and for this reason we need to wait for all shards to finish their snapshotting
// before we can move on.
//
// To know which files we must account for in the manifest, we will keep an
// SSTable set.  Theoretically, we could just rescan the snapshot directory and
// see what's in there. But we would need to wait for all shards to finish
// before we can do that anyway. That is the hard part, and once that is done
// keeping the files set is not really a big deal.
//
// This code assumes that all shards will be snapshotting at the same time. So
// far this is a safe assumption, but if we ever want to take snapshots from a
// group of shards only, this code will have to be updated to account for that.
struct snapshot_manager {
    std::unordered_set<sstring> files;
    semaphore requests;
    semaphore manifest_write;
    snapshot_manager() : requests(0), manifest_write(0) {}
};
static thread_local std::unordered_map<sstring, lw_shared_ptr<snapshot_manager>> pending_snapshots;

static future<>
seal_snapshot(sstring jsondir) {
    std::ostringstream ss;
    int n = 0;
    ss << "{" << std::endl << "\t\"files\" : [ ";
    for (auto&& rf: pending_snapshots.at(jsondir)->files) {
        if (n++ > 0) {
            ss << ", ";
        }
        ss << "\"" << rf << "\"";
    }
    ss << " ]" << std::endl << "}" << std::endl;

    auto json = ss.str();
    auto jsonfile = jsondir + "/manifest.json";

    dblog.debug("Storing manifest {}", jsonfile);

    return io_check(recursive_touch_directory, jsondir).then([jsonfile, json = std::move(json)] {
        return open_checked_file_dma(general_disk_error, jsonfile, open_flags::wo | open_flags::create | open_flags::truncate).then([json](file f) {
            return do_with(make_file_output_stream(std::move(f)), [json] (output_stream<char>& out) {
                return out.write(json.c_str(), json.size()).then([&out] {
                   return out.flush();
                }).then([&out] {
                   return out.close();
                });
            });
        });
    }).then([jsondir] {
        return io_check(sync_directory, std::move(jsondir));
    }).finally([jsondir] {
        pending_snapshots.erase(jsondir);
        return make_ready_future<>();
    });
}

future<> column_family::snapshot(sstring name) {
    return flush().then([this, name = std::move(name)]() {
        auto tables = boost::copy_range<std::vector<sstables::shared_sstable>>(*_sstables | boost::adaptors::map_values);
        return do_with(std::move(tables), [this, name](std::vector<sstables::shared_sstable> & tables) {
            auto jsondir = _config.datadir + "/snapshots/" + name;

            return parallel_for_each(tables, [name](sstables::shared_sstable sstable) {
                auto dir = sstable->get_dir() + "/snapshots/" + name;
                return io_check(recursive_touch_directory, dir).then([sstable, dir] {
                    return sstable->create_links(dir).then_wrapped([] (future<> f) {
                        // If the SSTables are shared, one of the CPUs will fail here.
                        // That is completely fine, though. We only need one link.
                        try {
                            f.get();
                        } catch (std::system_error& e) {
                            if (e.code() != std::error_code(EEXIST, std::system_category())) {
                                throw;
                            }
                        }
                        return make_ready_future<>();
                    });
                });
            }).then([jsondir, &tables] {
                // This is not just an optimization. If we have no files, jsondir may not have been created,
                // and sync_directory would throw.
                if (tables.size()) {
                    return io_check(sync_directory, std::move(jsondir));
                } else {
                    return make_ready_future<>();
                }
            }).finally([this, &tables, jsondir] {
                auto shard = std::hash<sstring>()(jsondir) % smp::count;
                std::unordered_set<sstring> table_names;
                for (auto& sst : tables) {
                    auto f = sst->get_filename();
                    auto rf = f.substr(sst->get_dir().size() + 1);
                    table_names.insert(std::move(rf));
                }
                return smp::submit_to(shard, [requester = engine().cpu_id(), jsondir = std::move(jsondir),
                                              tables = std::move(table_names), datadir = _config.datadir] {

                    if (pending_snapshots.count(jsondir) == 0) {
                        pending_snapshots.emplace(jsondir, make_lw_shared<snapshot_manager>());
                    }
                    auto snapshot = pending_snapshots.at(jsondir);
                    for (auto&& sst: tables) {
                        snapshot->files.insert(std::move(sst));
                    }

                    snapshot->requests.signal(1);
                    auto my_work = make_ready_future<>();
                    if (requester == engine().cpu_id()) {
                        my_work = snapshot->requests.wait(smp::count).then([jsondir = std::move(jsondir),
                                                                            snapshot] () mutable {
                            return seal_snapshot(jsondir).then([snapshot] {
                                snapshot->manifest_write.signal(smp::count);
                                return make_ready_future<>();
                            });
                        });
                    }
                    return my_work.then([snapshot] {
                        return snapshot->manifest_write.wait(1);
                    }).then([snapshot] {});
                });
            });
        });
    });
}

future<bool> column_family::snapshot_exists(sstring tag) {
    sstring jsondir = _config.datadir + "/snapshots/" + tag;
    return open_checked_directory(general_disk_error, std::move(jsondir)).then_wrapped([] (future<file> f) {
        try {
            f.get0();
            return make_ready_future<bool>(true);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw;
            }
            return make_ready_future<bool>(false);
        }
    });
}

enum class missing { no, yes };
static missing
file_missing(future<> f) {
    try {
        f.get();
        return missing::no;
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw;
        }
        return missing::yes;
    }
}

future<> column_family::clear_snapshot(sstring tag) {
    sstring jsondir = _config.datadir + "/snapshots/";
    sstring parent = _config.datadir;
    if (!tag.empty()) {
        jsondir += tag;
        parent += "/snapshots/";
    }

    lister::dir_entry_types dir_and_files = { directory_entry_type::regular, directory_entry_type::directory };
    return lister::scan_dir(jsondir, dir_and_files, [this, curr_dir = jsondir, dir_and_files, tag] (directory_entry de) {
        // FIXME: We really need a better directory walker. This should eventually be part of the seastar infrastructure.
        // It's hard to write this in a fully recursive manner because we need to keep information about the parent directory,
        // so we can remove the file. For now, we'll take advantage of the fact that we will at most visit 2 levels and keep
        // it ugly but simple.
        auto recurse = make_ready_future<>();
        if (de.type == directory_entry_type::directory) {
            // Should only recurse when tag is empty, meaning delete all snapshots
            if (!tag.empty()) {
                throw std::runtime_error(sprint("Unexpected directory %s found at %s! Aborting", de.name, curr_dir));
            }
            auto newdir = curr_dir + "/" + de.name;
            recurse = lister::scan_dir(newdir, dir_and_files, [this, curr_dir = newdir] (directory_entry de) {
                return io_check(remove_file, curr_dir + "/" + de.name);
            });
        }
        return recurse.then([fname = curr_dir + "/" + de.name] {
            return io_check(remove_file, fname);
        });
    }).then_wrapped([jsondir] (future<> f) {
        // Fine if directory does not exist. If it did, we delete it
        if (file_missing(std::move(f)) == missing::no) {
            return io_check(remove_file, jsondir);
        }
        return make_ready_future<>();
    }).then([parent] {
        return io_check(sync_directory, parent).then_wrapped([] (future<> f) {
            // Should always exist for empty tags, but may not exist for a single tag if we never took
            // snapshots. We will check this here just to mask out the exception, without silencing
            // unexpected ones.
            file_missing(std::move(f));
            return make_ready_future<>();
        });
    });
}

future<std::unordered_map<sstring, column_family::snapshot_details>> column_family::get_snapshot_details() {
    std::unordered_map<sstring, snapshot_details> all_snapshots;
    return do_with(std::move(all_snapshots), [this] (auto& all_snapshots) {
        return io_check([&] { return engine().file_exists(_config.datadir + "/snapshots"); }).then([this, &all_snapshots](bool file_exists) {
            if (!file_exists) {
                return make_ready_future<>();
            }
            return lister::scan_dir(_config.datadir + "/snapshots",  { directory_entry_type::directory }, [this, &all_snapshots] (directory_entry de) {
            auto snapshot_name = de.name;
            auto snapshot = _config.datadir + "/snapshots/" + snapshot_name;
            all_snapshots.emplace(snapshot_name, snapshot_details());
            return lister::scan_dir(snapshot,  { directory_entry_type::regular }, [this, &all_snapshots, snapshot, snapshot_name] (directory_entry de) {
                return io_check(file_size, snapshot + "/" + de.name).then([this, &all_snapshots, snapshot_name, name = de.name] (auto size) {
                    // The manifest is the only file expected to be in this directory not belonging to the SSTable.
                    // For it, we account the total size, but zero it for the true size calculation.
                    //
                    // All the others should just generate an exception: there is something wrong, so don't blindly
                    // add it to the size.
                    if (name != "manifest.json") {
                        sstables::entry_descriptor::make_descriptor(name);
                        all_snapshots.at(snapshot_name).total += size;
                    } else {
                        size = 0;
                    }
                    return make_ready_future<uint64_t>(size);
                }).then([this, &all_snapshots, snapshot_name, name = de.name] (auto size) {
                    // FIXME: When we support multiple data directories, the file may not necessarily
                    // live in this same location. May have to test others as well.
                    return io_check(file_size, _config.datadir + "/" + name).then_wrapped([&all_snapshots, snapshot_name, size] (auto fut) {
                        try {
                            // File exists in the main SSTable directory. Snapshots are not contributing to size
                            fut.get0();
                        } catch (std::system_error& e) {
                            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                                throw;
                            }
                            all_snapshots.at(snapshot_name).live += size;
                        }
                        return make_ready_future<>();
                    });
                });
            });
        });
        }).then([&all_snapshots] {
            return std::move(all_snapshots);
        });
    });
}

future<> column_family::flush() {
    // FIXME: this will synchronously wait for this write to finish, but doesn't guarantee
    // anything about previous writes.
    _stats.pending_flushes++;
    return seal_active_memtable().finally([this]() mutable {
        _stats.pending_flushes--;
        // In origin memtable_switch_count is incremented inside
        // ColumnFamilyMeetrics Flush.run
        _stats.memtable_switch_count++;
        return make_ready_future<>();
    });
}

future<> column_family::flush(const db::replay_position& pos) {
    // Technically possible if we've already issued the
    // sstable write, but it is not done yet.
    if (pos < _highest_flushed_rp) {
        return make_ready_future<>();
    }

    // TODO: Origin looks at "secondary" memtables
    // It also consideres "minReplayPosition", which is simply where
    // the CL "started" (the first ever RP in this run).
    // We ignore this for now and just say that if we're asked for
    // a CF and it exists, we pretty much have to have data that needs
    // flushing. Let's do it.
    return seal_active_memtable();
}

// FIXME: We can do much better than this in terms of cache management. Right
// now, we only have to flush the touched ranges because of the possibility of
// streaming containing token ownership changes.
//
// Right now we can't differentiate between that and a normal repair process,
// so we always flush. When we can differentiate those streams, we should not
// be indiscriminately touching the cache during repair. We will just have to
// invalidate the entries that are relevant to things we already have in the cache.
future<> column_family::flush_streaming_mutations(std::vector<query::partition_range> ranges) {
    // This will effectively take the gate twice for this call. The proper way to fix that would
    // be to change seal_active_streaming_memtable_delayed to take a range parameter. However, we
    // need this code to go away as soon as we can (see FIXME above). So the double gate is a better
    // temporary counter measure.
    return with_gate(_streaming_flush_gate, [this, ranges = std::move(ranges)] {
        return seal_active_streaming_memtable_delayed().finally([this, ranges = std::move(ranges)] {
            if (_config.enable_cache) {
                for (auto& range : ranges) {
                    _cache.invalidate(range);
                }
            }
        });
    });
}

void column_family::clear() {
    _cache.clear();
    _memtables->clear();
    _memtables->add_memtable();
    _streaming_memtables->clear();
    _streaming_memtables->add_memtable();
}

// NOTE: does not need to be futurized, but might eventually, depending on
// if we implement notifications, whatnot.
future<db::replay_position> column_family::discard_sstables(db_clock::time_point truncated_at) {
    assert(_compaction_disabled > 0);
    assert(!compaction_manager_queued());

    return with_lock(_sstables_lock.for_read(), [this, truncated_at] {
        db::replay_position rp;
        auto gc_trunc = to_gc_clock(truncated_at);

        auto pruned = make_lw_shared<sstable_list>();
        std::vector<sstables::shared_sstable> remove;

        for (auto&p : *_sstables) {
            if (p.second->max_data_age() <= gc_trunc) {
                rp = std::max(p.second->get_stats_metadata().position, rp);
                remove.emplace_back(p.second);
                continue;
            }
            pruned->emplace(p.first, p.second);
        }

        _sstables = std::move(pruned);
        dblog.debug("cleaning out row cache");
        _cache.clear();

        return parallel_for_each(remove, [](sstables::shared_sstable s) {
            return sstables::delete_atomically({s});
        }).then([rp] {
            return make_ready_future<db::replay_position>(rp);
        }).finally([remove] {}); // keep the objects alive until here.
    });
}


std::ostream& operator<<(std::ostream& os, const user_types_metadata& m) {
    os << "org.apache.cassandra.config.UTMetaData@" << &m;
    return os;
}

std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m) {
    os << "KSMetaData{";
    os << "name=" << m._name;
    os << ", strategyClass=" << m._strategy_name;
    os << ", strategyOptions={";
    int n = 0;
    for (auto& p : m._strategy_options) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", cfMetaData={";
    n = 0;
    for (auto& p : m._cf_meta_data) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", durable_writes=" << m._durable_writes;
    os << ", userTypes=" << m._user_types;
    os << "}";
    return os;
}

void column_family::set_schema(schema_ptr s) {
    dblog.debug("Changing schema version of {}.{} ({}) from {} to {}",
                _schema->ks_name(), _schema->cf_name(), _schema->id(), _schema->version(), s->version());

    for (auto& m : *_memtables) {
        m->set_schema(s);
    }

    for (auto& m : *_streaming_memtables) {
        m->set_schema(s);
    }

    _cache.set_schema(s);
    _schema = std::move(s);
}
