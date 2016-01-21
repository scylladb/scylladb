/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
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

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "dht/i_partitioner.hh"
#include "locator/abstract_replication_strategy.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include "cql3/column_specification.hh"
#include "db/commitlog/replay_position.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "keys.hh"
#include "mutation.hh"
#include "memtable.hh"
#include <list>
#include "mutation_reader.hh"
#include "row_cache.hh"
#include "compaction_strategy.hh"
#include "sstables/compaction_manager.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/histogram.hh"
#include "sstables/estimated_histogram.hh"
#include "sstables/compaction.hh"
#include "key_reader.hh"
#include <seastar/core/rwlock.hh>

class frozen_mutation;
class reconcilable_result;

namespace service {
class storage_proxy;
}

namespace sstables {

class sstable;
class entry_descriptor;
}

namespace db {
template<typename T>
class serializer;

class commitlog;
class config;

namespace system_keyspace {
void make(database& db, bool durable, bool volatile_testing_only);
}
}

class replay_position_reordered_exception : public std::exception {};

using memtable_list = std::vector<lw_shared_ptr<memtable>>;
using sstable_list = sstables::sstable_list;

// The CF has a "stats" structure. But we don't want all fields here,
// since some of them are fairly complex for exporting to collectd. Also,
// that structure matches what we export via the API, so better leave it
// untouched. And we need more fields. We will summarize it in here what
// we need.
struct cf_stats {
    int64_t pending_memtables_flushes_count = 0;
    int64_t pending_memtables_flushes_bytes = 0;
};

class column_family {
public:
    struct config {
        sstring datadir;
        bool enable_disk_writes = true;
        bool enable_disk_reads = true;
        bool enable_cache = true;
        bool enable_commitlog = true;
        bool enable_incremental_backups = false;
        size_t max_memtable_size = 5'000'000;
        logalloc::region_group* dirty_memory_region_group = nullptr;
        ::cf_stats* cf_stats = nullptr;
    };
    struct no_commitlog {};
    struct stats {
        /** Number of times flush has resulted in the memtable being switched out. */
        int64_t memtable_switch_count = 0;
        /** Estimated number of tasks pending for this column family */
        int64_t pending_flushes = 0;
        int64_t live_disk_space_used = 0;
        int64_t total_disk_space_used = 0;
        int64_t live_sstable_count = 0;
        /** Estimated number of compactions pending for this column family */
        int64_t pending_compactions = 0;
        utils::ihistogram reads{256};
        utils::ihistogram writes{256};
        sstables::estimated_histogram estimated_read;
        sstables::estimated_histogram estimated_write;
        sstables::estimated_histogram estimated_sstable_per_read;
        utils::ihistogram tombstone_scanned;
        utils::ihistogram live_scanned;
    };

    struct snapshot_details {
        int64_t total;
        int64_t live;
    };
private:
    schema_ptr _schema;
    config _config;
    stats _stats;
    lw_shared_ptr<memtable_list> _memtables;
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    lw_shared_ptr<sstable_list> _sstables;
    // There are situations in which we need to stop writing sstables. Flushers will take
    // the read lock, and the ones that wish to stop that process will take the write lock.
    rwlock _sstables_lock;
    mutable row_cache _cache; // Cache covers only sstables.
    int64_t _sstable_generation = 1;
    unsigned _mutation_count = 0;
    db::replay_position _highest_flushed_rp;
    // Provided by the database that owns this commitlog
    db::commitlog* _commitlog;
    sstables::compaction_strategy _compaction_strategy;
    compaction_manager& _compaction_manager;
    // Whether or not a cf is queued by its compaction manager.
    bool _compaction_manager_queued = false;
    int _compaction_disabled = 0;
    class memtable_flush_queue;
    std::unique_ptr<memtable_flush_queue> _flush_queue;
    // Store generation of sstables being compacted at the moment. That's needed to prevent a
    // sstable from being compacted twice.
    std::unordered_set<unsigned long> _compacting_generations;
private:
    void update_stats_for_new_sstable(uint64_t new_sstable_data_size);
    void add_sstable(sstables::sstable&& sstable);
    void add_sstable(lw_shared_ptr<sstables::sstable> sstable);
    void add_memtable();
    future<stop_iteration> try_flush_memtable_to_sstable(lw_shared_ptr<memtable> memt);
    future<> update_cache(memtable&, lw_shared_ptr<sstable_list> old_sstables);
    struct merge_comparator;

    // update the sstable generation, making sure that new new sstables don't overwrite this one.
    void update_sstables_known_generation(unsigned generation) {
        _sstable_generation = std::max<uint64_t>(_sstable_generation, generation /  smp::count + 1);
    }

    uint64_t calculate_generation_for_new_table() {
        return _sstable_generation++ * smp::count + engine().cpu_id();
    }

    // Rebuild existing _sstables with new_sstables added to it and sstables_to_remove removed from it.
    void rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
                              const std::vector<sstables::shared_sstable>& sstables_to_remove);
private:
    // Creates a mutation reader which covers sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_sstable_reader(schema_ptr schema, const query::partition_range& range) const;

    mutation_source sstables_as_mutation_source();
    key_source sstables_as_key_source() const;
    partition_presence_checker make_partition_presence_checker(lw_shared_ptr<sstable_list> old_sstables);
    std::chrono::steady_clock::time_point _sstable_writes_disabled_at;
public:
    // Creates a mutation reader which covers all data sources for this column family.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // Note: for data queries use query() instead.
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_reader(schema_ptr schema, const query::partition_range& range = query::full_partition_range) const;

    mutation_source as_mutation_source() const;

    // Queries can be satisfied from multiple data sources, so they are returned
    // as temporaries.
    //
    // FIXME: in case a query is satisfied from a single memtable, avoid a copy
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
    using const_row_ptr = std::unique_ptr<const row>;
    memtable& active_memtable() { return *_memtables->back(); }
    const row_cache& get_row_cache() const {
        return _cache;
    }

    row_cache& get_row_cache() {
        return _cache;
    }

    logalloc::occupancy_stats occupancy() const;
public:
    column_family(schema_ptr schema, config cfg, db::commitlog& cl, compaction_manager&);
    column_family(schema_ptr schema, config cfg, no_commitlog, compaction_manager&);
    column_family(column_family&&) = delete; // 'this' is being captured during construction
    ~column_family();
    const schema_ptr& schema() const { return _schema; }
    void set_schema(schema_ptr);
    db::commitlog* commitlog() { return _commitlog; }
    future<const_mutation_partition_ptr> find_partition(schema_ptr, const dht::decorated_key& key) const;
    future<const_mutation_partition_ptr> find_partition_slow(schema_ptr, const partition_key& key) const;
    future<const_row_ptr> find_row(schema_ptr, const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    // Applies given mutation to this column family
    // The mutation is always upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& = db::replay_position());
    void apply(const mutation& m, const db::replay_position& = db::replay_position());

    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(schema_ptr,
        const query::read_command& cmd,
        const std::vector<query::partition_range>& ranges);

    future<> populate(sstring datadir);

    void start();
    future<> stop();
    future<> flush();
    future<> flush(const db::replay_position&);
    void clear(); // discards memtable(s) without flushing them to disk.
    future<db::replay_position> discard_sstables(db_clock::time_point);

    // Important warning: disabling writes will only have an effect in the current shard.
    // The other shards will keep writing tables at will. Therefore, you very likely need
    // to call this separately in all shards first, to guarantee that none of them are writing
    // new data before you can safely assume that the whole node is disabled.
    future<int64_t> disable_sstable_write() {
        _sstable_writes_disabled_at = std::chrono::steady_clock::now();
        return _sstables_lock.write_lock().then([this] {
            return make_ready_future<int64_t>((*_sstables->end()).first);
        });
    }

    // SSTable writes are now allowed again, and generation is updated to new_generation
    // returns the amount of microseconds elapsed since we disabled writes.
    std::chrono::steady_clock::duration enable_sstable_write(int64_t new_generation) {
        update_sstables_known_generation(new_generation);
        _sstables_lock.write_unlock();
        return std::chrono::steady_clock::now() - _sstable_writes_disabled_at;
    }

    // Make sure the generation numbers are sequential, starting from "start".
    // Generations before "start" are left untouched.
    //
    // Return the highest generation number seen so far
    //
    // Word of warning: although this function will reshuffle anything over "start", it is
    // very dangerous to do that with live SSTables. This is meant to be used with SSTables
    // that are not yet managed by the system.
    //
    // An example usage would query all shards asking what is the highest SSTable number known
    // to them, and then pass that + 1 as "start".
    future<std::vector<sstables::entry_descriptor>> reshuffle_sstables(int64_t start);

    // FIXME: this is just an example, should be changed to something more
    // general. compact_all_sstables() starts a compaction of all sstables.
    // It doesn't flush the current memtable first. It's just a ad-hoc method,
    // not a real compaction policy.
    future<> compact_all_sstables();
    // Compact all sstables provided in the vector.
    // If cleanup is set to true, compaction_sstables will run on behalf of a cleanup job,
    // meaning that irrelevant keys will be discarded.
    future<> compact_sstables(sstables::compaction_descriptor descriptor, bool cleanup = false);
    // Performs a cleanup on each sstable of this column family, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> cleanup_sstables(sstables::compaction_descriptor descriptor);

    future<bool> snapshot_exists(sstring name);

    future<> load_new_sstables(std::vector<sstables::entry_descriptor> new_tables);
    future<> snapshot(sstring name);
    future<> clear_snapshot(sstring name);
    future<std::unordered_map<sstring, snapshot_details>> get_snapshot_details();

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    lw_shared_ptr<sstable_list> get_sstables();
    size_t sstables_count();
    int64_t get_unleveled_sstables() const;

    void start_compaction();
    void trigger_compaction();
    future<> run_compaction(sstables::compaction_descriptor descriptor);
    void set_compaction_strategy(sstables::compaction_strategy_type strategy);
    const sstables::compaction_strategy& get_compaction_strategy() const {
        return _compaction_strategy;
    }

    sstables::compaction_strategy& get_compaction_strategy() {
        return _compaction_strategy;
    }

    bool compaction_manager_queued() const;
    void set_compaction_manager_queued(bool compaction_manager_queued);
    bool pending_compactions() const;

    const stats& get_stats() const {
        return _stats;
    }

    compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
    }

    template<typename Func, typename Result = futurize_t<std::result_of_t<Func()>>>
    Result run_with_compaction_disabled(Func && func) {
        ++_compaction_disabled;
        return _compaction_manager.remove(this).then(std::forward<Func>(func)).finally([this] {
            if (--_compaction_disabled == 0) {
                trigger_compaction();
            }
        });
    }

    std::unordered_set<unsigned long>& compacting_generations() {
        return _compacting_generations;
    }
private:
    // One does not need to wait on this future if all we are interested in, is
    // initiating the write.  The writes initiated here will eventually
    // complete, and the seastar::gate below will make sure they are all
    // completed before we stop() this column_family.
    //
    // But it is possible to synchronously wait for the seal to complete by
    // waiting on this future. This is useful in situations where we want to
    // synchronously flush data to disk.
    //
    // FIXME: A better interface would guarantee that all writes before this
    // one are also complete
    future<> seal_active_memtable();

    // filter manifest.json files out
    static bool manifest_json_filter(const sstring& fname);

    seastar::gate _in_flight_seals;

    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    // Func signature: bool (const decorated_key& dk, const mutation_partition& mp)
    template <typename Func>
    future<bool> for_all_partitions(schema_ptr, Func&& func) const;
    future<sstables::entry_descriptor> probe_file(sstring sstdir, sstring fname);
    void seal_on_overflow();
    void check_valid_rp(const db::replay_position&) const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(schema_ptr, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
    // Testing purposes.
    friend class column_family_test;
};

class user_types_metadata {
    std::unordered_map<bytes, user_type> _user_types;
public:
    user_type get_type(bytes name) const {
        return _user_types.at(name);
    }
    const std::unordered_map<bytes, user_type>& get_all_types() const {
        return _user_types;
    }
    void add_type(user_type type) {
        auto i = _user_types.find(type->_name);
        assert(i == _user_types.end() || type->is_compatible_with(*i->second));
        _user_types[type->_name] = std::move(type);
    }
    void remove_type(user_type type) {
        _user_types.erase(type->_name);
    }
    friend std::ostream& operator<<(std::ostream& os, const user_types_metadata& m);
};

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    std::map<sstring, sstring> _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    lw_shared_ptr<user_types_metadata> _user_types;
public:
    keyspace_metadata(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{},
                 lw_shared_ptr<user_types_metadata> user_types = make_lw_shared<user_types_metadata>())
        : _name{std::move(name)}
        , _strategy_name{strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name}
        , _strategy_options{std::move(strategy_options)}
        , _durable_writes{durable_writes}
        , _user_types{std::move(user_types)}
    {
        for (auto&& s : cf_defs) {
            _cf_meta_data.emplace(s->cf_name(), s);
        }
    }
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{})
    {
        return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, durables_writes, cf_defs);
    }
    void validate() const;
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const std::map<sstring, sstring>& strategy_options() const {
        return _strategy_options;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    const lw_shared_ptr<user_types_metadata>& user_types() const {
        return _user_types;
    }
    void add_column_family(const schema_ptr& s) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    friend std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m);
};

class keyspace {
public:
    struct config {
        sstring datadir;
        bool enable_commitlog = true;
        bool enable_disk_reads = true;
        bool enable_disk_writes = true;
        bool enable_cache = true;
        bool enable_incremental_backups = false;
        size_t max_memtable_size = 5'000'000;
        logalloc::region_group* dirty_memory_region_group = nullptr;
        ::cf_stats* cf_stats = nullptr;
    };
private:
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
    lw_shared_ptr<keyspace_metadata> _metadata;
    config _config;
public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg)
        : _metadata(std::move(metadata))
        , _config(std::move(cfg))
    {}
    user_types_metadata _user_types;
    const lw_shared_ptr<keyspace_metadata>& metadata() const {
        return _metadata;
    }
    void create_replication_strategy(const std::map<sstring, sstring>& options);
    locator::abstract_replication_strategy& get_replication_strategy();
    const locator::abstract_replication_strategy& get_replication_strategy() const;
    column_family::config make_column_family_config(const schema& s) const;
    future<> make_directory_for_column_family(const sstring& name, utils::UUID uuid);
    void add_column_family(const schema_ptr& s) {
        _metadata->add_column_family(s);
    }

    // FIXME to allow simple registration at boostrap
    void set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy);

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    const sstring& datadir() const {
        return _config.datadir;
    }
private:
    sstring column_family_directory(const sstring& name, utils::UUID uuid) const;
};

class no_such_keyspace : public std::runtime_error {
public:
    no_such_keyspace(const sstring& ks_name);
};

class no_such_column_family : public std::runtime_error {
public:
    no_such_column_family(const utils::UUID& uuid);
    no_such_column_family(const sstring& ks_name, const sstring& cf_name);
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
    ::cf_stats _cf_stats;
    logalloc::region_group _dirty_memory_region_group;
    std::unordered_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> _column_families;
    std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash> _ks_cf_to_uuid;
    std::unique_ptr<db::commitlog> _commitlog;
    std::unique_ptr<db::config> _cfg;
    size_t _memtable_total_space = 500 << 20;
    utils::UUID _version;
    // compaction_manager object is referenced by all column families of a database.
    compaction_manager _compaction_manager;
    std::vector<scollectd::registration> _collectd;
    timer<> _throttling_timer{[this] { unthrottle(); }};
    circular_buffer<promise<>> _throttled_requests;

    future<> init_commitlog();
    future<> apply_in_memory(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position&);
    future<> populate(sstring datadir);
    future<> populate_keyspace(sstring datadir, sstring ks_name);

private:
    // Unless you are an earlier boostraper or the database itself, you should
    // not be using this directly.  Go for the public create_keyspace instead.
    void add_keyspace(sstring name, keyspace k);
    void create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);
    friend void db::system_keyspace::make(database& db, bool durable, bool volatile_testing_only);
    void setup_collectd();
    future<> throttle();
    future<> do_apply(schema_ptr, const frozen_mutation&);
    void unthrottle();
public:
    static utils::UUID empty_version;

    future<> parse_system_tables(distributed<service::storage_proxy>&);
    database();
    database(const db::config&);
    database(database&&) = delete;
    ~database();

    void update_version(const utils::UUID& version);

    const utils::UUID& get_version() const;

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    compaction_manager& get_compaction_manager() {
        return _compaction_manager;
    }
    const compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
    }

    future<> init_system_keyspace();
    future<> load_sstables(distributed<service::storage_proxy>& p); // after init_system_keyspace()

    void add_column_family(schema_ptr schema, column_family::config cfg);

    future<> drop_column_family(db_clock::time_point changed_at, const sstring& ks_name, const sstring& cf_name);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range);
    const utils::UUID& find_uuid(const schema_ptr&) const throw (std::out_of_range);

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(const sstring& name) throw (no_such_keyspace);
    const keyspace& find_keyspace(const sstring& name) const throw (no_such_keyspace);
    bool has_keyspace(const sstring& name) const;
    void update_keyspace(const sstring& name);
    void drop_keyspace(const sstring& name);
    const auto& keyspaces() const { return _keyspaces; }
    std::vector<sstring> get_non_system_keyspaces() const;
    column_family& find_column_family(const sstring& ks, const sstring& name) throw (no_such_column_family);
    const column_family& find_column_family(const sstring& ks, const sstring& name) const throw (no_such_column_family);
    column_family& find_column_family(const utils::UUID&) throw (no_such_column_family);
    const column_family& find_column_family(const utils::UUID&) const throw (no_such_column_family);
    column_family& find_column_family(const schema_ptr&) throw (no_such_column_family);
    const column_family& find_column_family(const schema_ptr&) const throw (no_such_column_family);
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family);
    schema_ptr find_schema(const utils::UUID&) const throw (no_such_column_family);
    bool has_schema(const sstring& ks_name, const sstring& cf_name) const;
    std::set<sstring> existing_index_names(const sstring& cf_to_exclude = sstring()) const;
    future<> stop();
    unsigned shard_of(const dht::token& t);
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<lw_shared_ptr<query::result>> query(schema_ptr, const query::read_command& cmd, const std::vector<query::partition_range>& ranges);
    future<reconcilable_result> query_mutations(schema_ptr, const query::read_command& cmd, const query::partition_range& range);
    future<> apply(schema_ptr, const frozen_mutation&);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm);
    const sstring& get_snitch_name() const;
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names);

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const std::unordered_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }

    std::unordered_map<sstring, keyspace>& get_keyspaces() {
        return _keyspaces;
    }

    const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() const {
        return _column_families;
    }

    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() {
        return _column_families;
    }

    const std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash>&
    get_column_families_mapping() const {
        return _ks_cf_to_uuid;
    }

    const db::config& get_config() const {
        return *_cfg;
    }

    future<> flush_all_memtables();
    /** Truncates the given column family */
    future<> truncate(db_clock::time_point truncated_at, sstring ksname, sstring cfname);
    future<> truncate(db_clock::time_point truncated_at, const keyspace& ks, column_family& cf);

    const logalloc::region_group& dirty_memory_region_group() const {
        return _dirty_memory_region_group;
    }

    std::unordered_set<sstring> get_initial_tokens();
    std::experimental::optional<gms::inet_address> get_replace_address();
    bool is_replacing();
};

// FIXME: stub
class secondary_index_manager {};

inline
void
column_family::apply(const mutation& m, const db::replay_position& rp) {
    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    active_memtable().apply(m, rp);
    seal_on_overflow();
    _stats.writes.mark(lc);
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency(), _stats.writes.count);
    }
}

inline
void
column_family::seal_on_overflow() {
    ++_mutation_count;
    if (active_memtable().occupancy().total_space() >= _config.max_memtable_size) {
        // FIXME: if sparse, do some in-memory compaction first
        // FIXME: maybe merge with other in-memory memtables
        _mutation_count = 0;
        seal_active_memtable();
    }
}

inline
void
column_family::check_valid_rp(const db::replay_position& rp) const {
    if (rp < _highest_flushed_rp) {
        throw replay_position_reordered_exception();
    }
}

inline
void
column_family::apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& rp) {
    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    check_valid_rp(rp);
    active_memtable().apply(m, m_schema, rp);
    seal_on_overflow();
    _stats.writes.mark(lc);
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency(), _stats.writes.count);
    }
}

future<> update_schema_version_and_announce(distributed<service::storage_proxy>& proxy);

#endif /* DATABASE_HH_ */
