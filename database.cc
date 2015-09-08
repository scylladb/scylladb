/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/serializer.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "query-result-writer.hh"
#include "nway_merger.hh"
#include "cql3/column_identifier.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
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
#include <boost/range/algorithm/find.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "mutation_query.hh"

using namespace std::chrono_literals;

logging::logger dblog("database");

column_family::column_family(schema_ptr schema, config config, db::commitlog& cl, compaction_manager& compaction_manager)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _memtables(make_lw_shared(memtable_list{}))
    , _sstables(make_lw_shared<sstable_list>())
    , _cache(_schema, sstables_as_mutation_source(), global_cache_tracker())
    , _commitlog(&cl)
    , _compaction_manager(compaction_manager)
{
    add_memtable();
    if (!_config.enable_disk_writes) {
        dblog.warn("Writes disabled, column family no durable.");
    }
}

column_family::column_family(schema_ptr schema, config config, no_commitlog cl, compaction_manager& compaction_manager)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _memtables(make_lw_shared(memtable_list{}))
    , _sstables(make_lw_shared<sstable_list>())
    , _cache(_schema, sstables_as_mutation_source(), global_cache_tracker())
    , _commitlog(nullptr)
    , _compaction_manager(compaction_manager)
{
    add_memtable();
    if (!_config.enable_disk_writes) {
        dblog.warn("Writes disabled, column family no durable.");
    }
}

partition_presence_checker
column_family::make_partition_presence_checker(lw_shared_ptr<sstable_list> old_sstables) {
    return [this, old_sstables = std::move(old_sstables)] (const partition_key& key) {
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
    return [this] (const query::partition_range& r) {
        return make_sstable_reader(r);
    };
}

// define in .cc, since sstable is forward-declared in .hh
column_family::~column_family() {
}

static
bool belongs_to_current_shard(const mutation& m) {
    return dht::shard_of(m.token()) == engine().cpu_id();
}

class sstable_range_wrapping_reader final : public mutation_reader::impl {
    lw_shared_ptr<sstables::sstable> _sst;
    sstables::mutation_reader _smr;
public:
    sstable_range_wrapping_reader(lw_shared_ptr<sstables::sstable> sst,
            schema_ptr s, const query::partition_range& pr)
            : _sst(sst)
            , _smr(sst->read_range_rows(std::move(s), pr)) {
    }
    virtual future<mutation_opt> operator()() override {
        return _smr.read();
    }
};

class range_sstable_reader final : public mutation_reader::impl {
    const query::partition_range& _pr;
    lw_shared_ptr<sstable_list> _sstables;
    mutation_reader _reader;
public:
    range_sstable_reader(schema_ptr s, lw_shared_ptr<sstable_list> sstables, const query::partition_range& pr)
        : _pr(pr)
        , _sstables(std::move(sstables))
    {
        std::vector<mutation_reader> readers;
        for (const lw_shared_ptr<sstables::sstable>& sst : *_sstables | boost::adaptors::map_values) {
            // FIXME: make sstable::read_range_rows() return ::mutation_reader so that we can drop this wrapper.
            mutation_reader reader = make_mutation_reader<sstable_range_wrapping_reader>(sst, s, pr);
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
public:
    single_key_sstable_reader(schema_ptr schema, lw_shared_ptr<sstable_list> sstables, const partition_key& key)
        : _schema(std::move(schema))
        , _key(sstables::key::from_partition_key(*_schema, key))
        , _sstables(std::move(sstables))
    { }

    virtual future<mutation_opt> operator()() override {
        if (_done) {
            return make_ready_future<mutation_opt>();
        }
        return parallel_for_each(*_sstables | boost::adaptors::map_values, [this](const lw_shared_ptr<sstables::sstable>& sstable) {
            return sstable->read_row(_schema, _key).then([this](mutation_opt mo) {
                apply(_m, std::move(mo));
            });
        }).then([this] {
            _done = true;
            return std::move(_m);
        });
    }
};

mutation_reader
column_family::make_sstable_reader(const query::partition_range& pr) const {
    if (pr.is_singular() && pr.start()->value().has_key()) {
        const dht::ring_position& pos = pr.start()->value();
        if (dht::shard_of(pos.token()) != engine().cpu_id()) {
            return make_empty_reader(); // range doesn't belong to this shard
        }
        return make_mutation_reader<single_key_sstable_reader>(_schema, _sstables, *pos.key());
    } else {
        // range_sstable_reader is not movable so we need to wrap it
        return make_mutation_reader<range_sstable_reader>(_schema, _sstables, pr);
    }
}

// Exposed for testing, not performance critical.
future<column_family::const_mutation_partition_ptr>
column_family::find_partition(const dht::decorated_key& key) const {
    return do_with(query::partition_range::make_singular(key), [this] (auto& range) {
        return do_with(this->make_reader(range), [] (mutation_reader& reader) {
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
column_family::find_partition_slow(const partition_key& key) const {
    return find_partition(dht::global_partitioner().decorate_key(*_schema, key));
}

future<column_family::const_row_ptr>
column_family::find_row(const dht::decorated_key& partition_key, clustering_key clustering_key) const {
    return find_partition(partition_key).then([clustering_key = std::move(clustering_key)] (const_mutation_partition_ptr p) {
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
column_family::make_reader(const query::partition_range& range) const {
    if (query::is_wrap_around(range, *_schema)) {
        // make_combined_reader() can't handle streams that wrap around yet.
        fail(unimplemented::cause::WRAP_AROUND);
    }

    std::vector<mutation_reader> readers;
    readers.reserve(_memtables->size() + _sstables->size());

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_reader(range));
    }

    if (_config.enable_cache) {
        readers.emplace_back(_cache.make_reader(range));
    } else {
        readers.emplace_back(make_sstable_reader(range));
    }

    return make_combined_reader(std::move(readers));
}

template <typename Func>
future<bool>
column_family::for_all_partitions(Func&& func) const {
    static_assert(std::is_same<bool, std::result_of_t<Func(const dht::decorated_key&, const mutation_partition&)>>::value,
                  "bad Func signature");

    struct iteration_state {
        mutation_reader reader;
        Func func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(const column_family& cf, Func&& func)
            : reader(cf.make_reader())
            , func(std::move(func))
        { }
    };

    return do_with(iteration_state(*this, std::move(func)), [] (iteration_state& is) {
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
column_family::for_all_partitions_slow(std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    return for_all_partitions(std::move(func));
}

class lister {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    directory_entry_type _expected_type;
    subscription<directory_entry> _listing;
    sstring _dirname;

public:
    lister(file f, directory_entry_type type, std::function<future<> (directory_entry)> walker, sstring dirname)
            : _f(std::move(f))
            , _walker(std::move(walker))
            , _expected_type(type)
            , _listing(_f.list_directory([this] (directory_entry de) { return _visit(de); }))
            , _dirname(dirname) {
    }

    static future<> scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker);
protected:
    future<> _visit(directory_entry de) {

        return guarantee_type(std::move(de)).then([this] (directory_entry de) {
            // Hide all synthetic directories and hidden files.
            if ((de.type != _expected_type) || (de.name[0] == '.')) {
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


future<> lister::scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker) {

    return engine().open_directory(name).then([type, walker = std::move(walker), name] (file f) {
        auto l = make_lw_shared<lister>(std::move(f), type, walker, name);
        return l->done().then([l] { });
    });
}

static std::vector<sstring> parse_fname(sstring filename) {
    std::vector<sstring> comps;
    boost::split(comps , filename ,boost::is_any_of(".-"));
    return comps;
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

    // Make sure new sstables don't overwrite this one.
    _sstable_generation = std::max<uint64_t>(_sstable_generation, comps.generation /  smp::count + 1);
    assert(_sstables->count(comps.generation) == 0);

    auto sst = std::make_unique<sstables::sstable>(_schema->ks_name(), _schema->cf_name(), sstdir, comps.generation, comps.version, comps.format);
    auto fut = sst->load();
    return std::move(fut).then([this, sst = std::move(sst)] () mutable {
        add_sstable(std::move(*sst));
        return make_ready_future<>();
    }).then_wrapped([fname, comps = std::move(comps)] (future<> f) {
        try {
            f.get();
        } catch (malformed_sstable_exception& e) {
            dblog.error("malformed sstable {}: {}. Refusing to boot", fname, e.what());
            throw;
        } catch(...) {
            dblog.error("Unrecognized error while processing {}: Refusing to boot", fname);
            throw;
        }
        return make_ready_future<entry_descriptor>(std::move(comps));
    });
}

void column_family::update_stats_for_new_sstable(uint64_t new_sstable_data_size) {
    _stats.live_disk_space_used += new_sstable_data_size;
    _stats.total_disk_space_used += new_sstable_data_size;
    _stats.live_sstable_count++;
}

void column_family::add_sstable(sstables::sstable&& sstable) {
    add_sstable(make_lw_shared(std::move(sstable)));
}

void column_family::add_sstable(lw_shared_ptr<sstables::sstable> sstable) {
    auto key_shard = [this] (const partition_key& pk) {
        auto token = dht::global_partitioner().get_token(*_schema, pk);
        return dht::shard_of(token);
    };
    auto s1 = key_shard(sstable->get_first_partition_key(*_schema));
    auto s2 = key_shard(sstable->get_last_partition_key(*_schema));
    auto me = engine().cpu_id();
    auto included = (s1 <= me) && (me <= s2);
    if (!included) {
        dblog.info("sstable {} not relevant for this shard, ignoring", sstable->get_filename());
        return;
    }
    auto generation = sstable->generation();
    // allow in-progress reads to continue using old list
    _sstables = make_lw_shared<sstable_list>(*_sstables);
    update_stats_for_new_sstable(sstable->data_size());
    _sstables->emplace(generation, std::move(sstable));
}

void column_family::add_memtable() {
    // allow in-progress reads to continue using old list
    _memtables = make_lw_shared(memtable_list(*_memtables));
    _memtables->emplace_back(make_lw_shared<memtable>(_schema, _config.dirty_memory_region_group));
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
    add_memtable();

    assert(_highest_flushed_rp < old->replay_position()
    || (_highest_flushed_rp == db::replay_position() && old->replay_position() == db::replay_position())
    );
    _highest_flushed_rp = old->replay_position();

    return seastar::with_gate(_in_flight_seals, [old, this] {
        return flush_memtable_to_sstable(old);
    });
    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<stop_iteration>
column_family::try_flush_memtable_to_sstable(lw_shared_ptr<memtable> old) {
    // FIXME: better way of ensuring we don't attempt to
    //        overwrite an existing table.
    auto gen = _sstable_generation++ * smp::count + engine().cpu_id();

    auto newtab = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(),
        _config.datadir, gen,
        sstables::sstable::version_types::ka,
        sstables::sstable::format_types::big);

    newtab->set_unshared();
    dblog.debug("Flushing to {}", newtab->get_filename());
    return newtab->write_components(*old).then([this, newtab, old] {
        return newtab->load();
    }).then([this, old, newtab] {
        dblog.debug("Flushing done");
        // We must add sstable before we call update_cache(), because
        // memtable's data after moving to cache can be evicted at any time.
        auto old_sstables = _sstables;
        add_sstable(newtab);
        return update_cache(*old, std::move(old_sstables));
    }).then_wrapped([this, old] (future<> ret) {
        try {
            ret.get();

            // FIXME: until the surrounding function returns a future and
            // caller ensures ordering (i.e. finish flushing one or more sequential tables before
            // doing the discard), this below is _not_ correct, since the use of replay_position
            // depends on us reporting the factual highest position we've actually flushed,
            // _and_ all positions (for a given UUID) below having been dealt with.
            //
            // Note that the whole scheme is also dependent on memtables being "allocated" in order,
            // i.e. we may not flush a younger memtable before and older, and we need to use the
            // highest rp.
            if (_commitlog) {
                _commitlog->discard_completed_segments(_schema->id(), old->replay_position());
            }
            _memtables->erase(boost::range::find(*_memtables, old));
            dblog.debug("Memtable replaced");
            trigger_compaction();
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        } catch (std::exception& e) {
            dblog.error("failed to write sstable: {}", e.what());
        } catch (...) {
            dblog.error("failed to write sstable: unknown error");
        }
        return sleep(10s).then([] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    });
}

future<>
column_family::flush_memtable_to_sstable(lw_shared_ptr<memtable> memt) {
    return repeat([this, memt] {
        return seastar::with_gate(_in_flight_seals, [memt, this] {
            return try_flush_memtable_to_sstable(memt);
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

    return _compaction_manager.remove(this).then([this] {
        return _in_flight_seals.close().then([this] {
            return make_ready_future<>();
        });
    });
}

future<>
column_family::compact_sstables(std::vector<sstables::shared_sstable> sstables) {
    if (!sstables.size()) {
        // if there is nothing to compact, just return.
        return make_ready_future<>();
    }

    auto sstables_to_compact = make_lw_shared<std::vector<sstables::shared_sstable>>(std::move(sstables));

    auto new_tables = make_lw_shared<std::vector<
            std::pair<unsigned, sstables::shared_sstable>>>();
    auto create_sstable = [this, new_tables] {
            // FIXME: this generation calculation should be in a function.
            auto gen = _sstable_generation++ * smp::count + engine().cpu_id();
            // FIXME: use "tmp" marker in names of incomplete sstable
            auto sst = make_lw_shared<sstables::sstable>(_schema->ks_name(), _schema->cf_name(), _config.datadir, gen,
                    sstables::sstable::version_types::ka,
                    sstables::sstable::format_types::big);
            sst->set_unshared();
            new_tables->emplace_back(gen, sst);
            return sst;
    };
    return sstables::compact_sstables(*sstables_to_compact, *this,
            create_sstable).then([this, new_tables, sstables_to_compact] {
        // Build a new list of _sstables: We remove from the existing list the
        // tables we compacted (by now, there might be more sstables flushed
        // later), and we add the new tables generated by the compaction.
        // We create a new list rather than modifying it in-place, so that
        // on-going reads can continue to use the old list.
        auto current_sstables = _sstables;
        _sstables = make_lw_shared<sstable_list>();

        // zeroing live_disk_space_used and live_sstable_count because the
        // sstable list is re-created below.
        _stats.live_disk_space_used = 0;
        _stats.live_sstable_count = 0;

        std::unordered_set<sstables::shared_sstable> s(
                sstables_to_compact->begin(), sstables_to_compact->end());
        for (const auto& oldtab : *current_sstables) {
            if (!s.count(oldtab.second)) {
                update_stats_for_new_sstable(oldtab.second->data_size());
                _sstables->emplace(oldtab.first, oldtab.second);
            }
        }

        for (const auto& newtab : *new_tables) {
            // FIXME: rename the new sstable(s). Verify a rename doesn't cause
            // problems for the sstable object.
            update_stats_for_new_sstable(newtab.second->data_size());
            _sstables->emplace(newtab.first, newtab.second);
        }

        for (const auto& oldtab : *sstables_to_compact) {
            oldtab->mark_for_deletion();
        }
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
    return compact_sstables(std::move(sstables));
}

void column_family::start_compaction() {
    set_compaction_strategy(_schema->compaction_strategy());
}

void column_family::trigger_compaction() {
    // Submitting compaction job to compaction manager.
    _stats.pending_compactions++;
    _compaction_manager.submit(this);
}

future<> column_family::run_compaction() {
    _stats.pending_compactions--;
    sstables::compaction_strategy strategy = _compaction_strategy;
    return do_with(std::move(strategy), [this] (sstables::compaction_strategy& cs) {
        return cs.compact(*this);
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

future<> column_family::populate(sstring sstdir) {
    // We can catch most errors when we try to load an sstable. But if the TOC
    // file is the one missing, we won't try to load the sstable at all. This
    // case is still an invalid case, but it is way easier for us to treat it
    // by waiting for all files to be loaded, and then checking if we saw a
    // file during scan_dir, without its corresponding TOC.
    enum class status {
        has_some_file,
        has_toc_file,
    };

    auto verifier = make_lw_shared<std::unordered_map<unsigned long, status>>();
    return lister::scan_dir(sstdir, directory_entry_type::regular, [this, sstdir, verifier] (directory_entry de) {
        // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")
        return probe_file(sstdir, de.name).then([verifier] (auto entry) {
            if (verifier->count(entry.generation)) {
                if (verifier->at(entry.generation) == status::has_toc_file) {
                    if (entry.component == sstables::sstable::component_type::TOC) {
                        throw sstables::malformed_sstable_exception("Invalid State encountered. TOC file already processed");
                    }
                } else if (entry.component == sstables::sstable::component_type::TOC) {
                    verifier->at(entry.generation) = status::has_toc_file;
                }
            } else {
                if (entry.component == sstables::sstable::component_type::TOC) {
                    verifier->emplace(entry.generation, status::has_toc_file);
                } else {
                    verifier->emplace(entry.generation, status::has_some_file);
                }
            }
        });
    }).then([verifier, sstdir] {
        return parallel_for_each(*verifier, [sstdir = std::move(sstdir)] (auto v) {
            if (v.second != status::has_toc_file) {
                throw sstables::malformed_sstable_exception(sprint("At directory: %s: no TOC found for SSTable with generation %d!. Refusing to boot", sstdir, v.first));
            }
            return make_ready_future<>();
        });
    });
}

utils::UUID database::empty_version = utils::UUID_gen::get_name_UUID(bytes{});

database::database() : database(db::config())
{}

database::database(const db::config& cfg)
    : _cfg(std::make_unique<db::config>(cfg))
    , _version(empty_version)
{
    _memtable_total_space = size_t(_cfg->memtable_total_space_in_mb()) << 20;
    if (!_memtable_total_space) {
        _memtable_total_space = memory::stats().total_memory() / 2;
    }
    bool durable = cfg.data_file_directories().size() > 0;
    db::system_keyspace::make(*this, durable, _cfg->volatile_system_keyspace_for_testing());
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
        dblog.warn("Populating Keyspace {}", ks_name);
        return lister::scan_dir(ksdir, directory_entry_type::directory, [this, ksdir, ks_name] (directory_entry de) {
            auto comps = parse_fname(de.name);
            if (comps.size() < 2) {
                dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
                return make_ready_future<>();
            }
            sstring cfname = comps[0];

            auto sstdir = ksdir + "/" + de.name;

            try {
                auto& cf = find_column_family(ks_name, cfname);
                dblog.info("Keyspace {}: Reading CF {} ", ksdir, cfname);
                // FIXME: Increase parallelism.
                return cf.populate(sstdir);
            } catch (no_such_column_family&) {
                dblog.warn("{}, CF {}: schema not loaded!", ksdir, comps[0]);
                return make_ready_future<>();
            }
        });
    }
    return make_ready_future<>();
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, directory_entry_type::directory, [this, datadir] (directory_entry de) {
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
    static_assert(std::is_same<future<>, std::result_of_t<Func(schema_result::value_type&)>>::value,
                  "bad Func signature");


    auto cf_name = make_lw_shared<sstring>(_cf_name);
    return db::system_keyspace::query(proxy.local(), *cf_name).then([&proxy] (auto rs) {
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

            return read_schema_partition_for_keyspace(proxy.local(), *cf_name, name).then([func, cf_name] (auto&& v) mutable {
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
    return do_parse_system_tables(proxy, db::schema_tables::KEYSPACES, [this] (schema_result::value_type &v) {
        auto ksm = create_keyspace_from_schema_partition(v);
        return create_keyspace(ksm);
    }).then([&proxy, this] {
        return do_parse_system_tables(proxy, db::schema_tables::COLUMNFAMILIES, [this, &proxy] (schema_result::value_type &v) {
            return create_tables_from_tables_partition(proxy.local(), v.second).then([this] (std::map<sstring, schema_ptr> tables) {
                for (auto& t: tables) {
                    auto s = t.second;
                    auto& ks = this->find_keyspace(s->ks_name());
                    auto cfg = ks.make_column_family_config(*s);
                    this->add_column_family(std::move(s), std::move(cfg));
                }
            });
        });
    });
}

future<>
database::init_system_keyspace() {
    // FIXME support multiple directories
    return touch_directory(_cfg->data_file_directories()[0] + "/" + db::system_keyspace::NAME).then([this] {
        return populate_keyspace(_cfg->data_file_directories()[0], db::system_keyspace::NAME).then([this]() {
            return init_commitlog();
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
    throw std::runtime_error("not implemented");
}

void database::drop_keyspace(const sstring& name) {
    throw std::runtime_error("not implemented");
}

void database::add_column_family(schema_ptr schema, column_family::config cfg) {
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

future<> database::update_column_family(const sstring& ks_name, const sstring& cf_name) {
    auto& proxy = service::get_local_storage_proxy();
    auto old_cfm = find_schema(ks_name, cf_name);
    return db::schema_tables::create_table_from_name(proxy, ks_name, cf_name).then([old_cfm] (auto&& new_cfm) {
        if (old_cfm->id() != new_cfm->id()) {
            return make_exception_future<>(exceptions::configuration_exception(sprint("Column family ID mismatch (found %s; expected %s)", new_cfm->id(), old_cfm->id())));
        }
        return make_exception_future<>(std::runtime_error("update column family not implemented"));
    });
}

void database::drop_column_family(const sstring& ks_name, const sstring& cf_name) {
    throw std::runtime_error("not implemented");
}

const utils::UUID& database::find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range) {
    return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const throw (std::out_of_range) {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(const sstring& name) throw (no_such_keyspace) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(const sstring& name) const throw (no_such_keyspace) {
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

column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family("Can't find a  column family " + cf_name + " in a keyspace " + ks_name));
    }
}

const column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family("Can't find a  column family " + cf_name + " in a keyspace " + ks_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) throw (no_such_column_family) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family("Can't find a column family with UUID: " + uuid.to_sstring()));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const throw (no_such_column_family) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family("Can't find a column family with UUID: " + uuid.to_sstring()));
    }
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
    cfg.dirty_memory_region_group = _config.dirty_memory_region_group;

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
    return make_directory(column_family_directory(name, uuid));
}

column_family& database::find_column_family(const schema_ptr& schema) throw (no_such_column_family) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const throw (no_such_column_family) {
    return find_column_family(schema->id());
}

void keyspace_metadata::validate() const {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    abstract_replication_strategy::validate_replication_strategy(name(), strategy_name(), ss.get_token_metadata(), strategy_options());
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    try {
        return find_schema(find_uuid(ks_name, cf_name));
    } catch (std::out_of_range&) {
        std::throw_with_nested(no_such_column_family(ks_name + ":" + cf_name));
    }
}

schema_ptr database::find_schema(const utils::UUID& uuid) const throw (no_such_column_family) {
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
        return touch_directory(datadir);
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
    explicit query_state(const query::read_command& cmd, const std::vector<query::partition_range>& ranges)
            : cmd(cmd)
            , builder(cmd.slice)
            , limit(cmd.row_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
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
column_family::query(const query::read_command& cmd, const std::vector<query::partition_range>& partition_ranges) const {
    return do_with(query_state(cmd, partition_ranges), [this] (query_state& qs) {
        return do_until(std::bind(&query_state::done, &qs), [this, &qs] {
            auto&& range = *qs.current_partition_range++;
            qs.reader = make_reader(range);
            qs.range_empty = false;
            return do_until([&qs] { return !qs.limit || qs.range_empty; }, [this, &qs] {
                return qs.reader().then([this, &qs](mutation_opt mo) {
                    if (mo) {
                        auto p_builder = qs.builder.add_partition(mo->key());
                        auto is_distinct = qs.cmd.slice.options.contains(query::partition_slice::option::distinct);
                        auto limit = !is_distinct ? qs.limit : 1;
                        mo->partition().query(p_builder, *_schema, qs.cmd.timestamp, limit);
                        qs.limit -= p_builder.row_count();
                    } else {
                        qs.range_empty = true;
                    }
                });
            });
        }).then([&qs] {
            return make_ready_future<lw_shared_ptr<query::result>>(
                    make_lw_shared<query::result>(qs.builder.build()));
        });
    });
}

mutation_source
column_family::as_mutation_source() const {
    return [this] (const query::partition_range& range) {
        return this->make_reader(range);
    };
}

future<lw_shared_ptr<query::result>>
database::query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges) {
    static auto make_empty = [] {
        return make_ready_future<lw_shared_ptr<query::result>>(make_lw_shared(query::result()));
    };

    try {
        column_family& cf = find_column_family(cmd.cf_id);
        return cf.query(cmd, ranges);
    } catch (const no_such_column_family&) {
        // FIXME: load from sstables
        return make_empty();
    }
}

future<reconcilable_result>
database::query_mutations(const query::read_command& cmd, const query::partition_range& range) {
    try {
        column_family& cf = find_column_family(cmd.cf_id);
        return mutation_query(cf.as_mutation_source(), range, cmd.slice, cmd.row_limit, cmd.timestamp);
    } catch (const no_such_column_family&) {
        // FIXME: load from sstables
        return make_ready_future<reconcilable_result>(reconcilable_result());
    }
}

std::ostream& operator<<(std::ostream& out, const atomic_cell_or_collection& c) {
    return out << to_hex(c._data);
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    fprint(os, "{mutation: schema %p key %s data ", m.schema().get(), m.decorated_key());
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

future<> database::apply_in_memory(const frozen_mutation& m, const db::replay_position& rp) {
    try {
        auto& cf = find_column_family(m.column_family_id());
        cf.apply(m, rp);
    } catch (no_such_column_family&) {
        // TODO: log a warning
        // FIXME: load keyspace meta-data from storage
    }
    return make_ready_future<>();
}

future<> database::do_apply(const frozen_mutation& m) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    auto& cf = find_column_family(m.column_family_id());
    if (cf.commitlog() != nullptr) {
        auto uuid = m.column_family_id();
        bytes_view repr = m.representation();
        auto write_repr = [repr] (data_output& out) { out.write(repr.begin(), repr.end()); };
        return cf.commitlog()->add_mutation(uuid, repr.size(), write_repr).then([&m, this](auto rp) {
            try {
                return this->apply_in_memory(m, rp);
            } catch (replay_position_reordered_exception&) {
                // expensive, but we're assuming this is super rare.
                // if we failed to apply the mutation due to future re-ordering
                // (which should be the ever only reason for rp mismatch in CF)
                // let's just try again, add the mutation to the CL once more,
                // and assume success in inevitable eventually.
                dblog.debug("replay_position reordering detected");
                return this->apply(m);
            }
        });
    }
    return apply_in_memory(m, db::replay_position());
}

future<> database::throttle() {
    if (_dirty_memory_region_group.memory_used() < _memtable_total_space
            && _throttled_requests.empty()) {
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

void database::unthrottle() {
    // Release one request per free 1MB we have
    // FIXME: improve this
    if (_dirty_memory_region_group.memory_used() >= _memtable_total_space) {
        return;
    }
    size_t avail = (_memtable_total_space - _dirty_memory_region_group.memory_used()) >> 20;
    avail = std::min(_throttled_requests.size(), avail);
    for (size_t i = 0; i < avail; ++i) {
        _throttled_requests.front().set_value();
        _throttled_requests.pop_front();
    }
    if (_throttled_requests.empty()) {
        _throttling_timer.cancel();
    }
}

future<> database::apply(const frozen_mutation& m) {
    return throttle().then([this, &m] {
        return do_apply(m);
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
    } else {
        cfg.datadir = "";
        cfg.enable_disk_writes = false;
        cfg.enable_disk_reads = false;
        cfg.enable_commitlog = false;
        cfg.enable_cache = false;
        cfg.max_memtable_size = std::numeric_limits<size_t>::max();
    }
    cfg.dirty_memory_region_group = &_dirty_memory_region_group;
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
            return _commitlog->sync_all_segments();
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

const sstring& database::get_snitch_name() const {
    return _cfg->endpoint_snitch();
}

future<> update_schema_version_and_announce(service::storage_proxy& proxy)
{
    return db::schema_tables::calculate_schema_digest(proxy).then([&proxy] (utils::UUID uuid) {
        return proxy.get_db().invoke_on_all([uuid] (database& db) {
            db.update_version(uuid);
            return make_ready_future<>();
        }).then([uuid] {
            return db::system_keyspace::update_schema_version(uuid).then([uuid] {
                return service::get_local_migration_manager().passive_announce(uuid);
            });
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
