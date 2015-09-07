/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
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
#include "utils/compaction_manager.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/histogram.hh"

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
using sstable_list = std::map<unsigned long, lw_shared_ptr<sstables::sstable>>;

class column_family {
public:
    struct config {
        sstring datadir;
        bool enable_disk_writes = true;
        bool enable_disk_reads = true;
        bool enable_cache = true;
        bool enable_commitlog = true;
        size_t max_memtable_size = 5'000'000;
        logalloc::region_group* dirty_memory_region_group = nullptr;
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
        utils::ihistogram reads{256, 100};
        utils::ihistogram writes{256, 100};
    };

private:
    schema_ptr _schema;
    config _config;
    stats _stats;
    lw_shared_ptr<memtable_list> _memtables;
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    lw_shared_ptr<sstable_list> _sstables;
    mutable row_cache _cache; // Cache covers only sstables.
    unsigned _sstable_generation = 1;
    unsigned _mutation_count = 0;
    db::replay_position _highest_flushed_rp;
    // Provided by the database that owns this commitlog
    db::commitlog* _commitlog;
    sstables::compaction_strategy _compaction_strategy;
    compaction_manager& _compaction_manager;
    // Whether or not a cf is queued by its compaction manager.
    bool _compaction_manager_queued = false;
private:
    void update_stats_for_new_sstable(uint64_t new_sstable_data_size);
    void add_sstable(sstables::sstable&& sstable);
    void add_sstable(lw_shared_ptr<sstables::sstable> sstable);
    void add_memtable();
    future<> flush_memtable_to_sstable(lw_shared_ptr<memtable> memt);
    future<stop_iteration> try_flush_memtable_to_sstable(lw_shared_ptr<memtable> memt);
    future<> update_cache(memtable&, lw_shared_ptr<sstable_list> old_sstables);
    struct merge_comparator;
private:
    // Creates a mutation reader which covers sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    mutation_reader make_sstable_reader(const query::partition_range& range) const;

    mutation_source sstables_as_mutation_source();
    partition_presence_checker make_partition_presence_checker(lw_shared_ptr<sstable_list> old_sstables);
public:
    // Creates a mutation reader which covers all data sources for this column family.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // Note: for data queries use query() instead.
    // The 'range' parameter must be live as long as the reader is used.
    mutation_reader make_reader(const query::partition_range& range = query::full_partition_range) const;

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
public:
    column_family(schema_ptr schema, config cfg, db::commitlog& cl, compaction_manager&);
    column_family(schema_ptr schema, config cfg, no_commitlog, compaction_manager&);
    column_family(column_family&&) = delete; // 'this' is being captured during construction
    ~column_family();
    schema_ptr schema() const { return _schema; }
    db::commitlog* commitlog() { return _commitlog; }
    future<const_mutation_partition_ptr> find_partition(const dht::decorated_key& key) const;
    future<const_mutation_partition_ptr> find_partition_slow(const partition_key& key) const;
    future<const_row_ptr> find_row(const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    void apply(const frozen_mutation& m, const db::replay_position& = db::replay_position());
    void apply(const mutation& m, const db::replay_position& = db::replay_position());

    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges) const;

    future<> populate(sstring datadir);

    void start();
    future<> stop();
    future<> flush();
    future<> flush(const db::replay_position&);

    // FIXME: this is just an example, should be changed to something more
    // general. compact_all_sstables() starts a compaction of all sstables.
    // It doesn't flush the current memtable first. It's just a ad-hoc method,
    // not a real compaction policy.
    future<> compact_all_sstables();
    // Compact all sstables provided in the vector.
    future<> compact_sstables(std::vector<lw_shared_ptr<sstables::sstable>> sstables);

    lw_shared_ptr<sstable_list> get_sstables();
    size_t sstables_count();
    int64_t get_unleveled_sstables() const;

    void start_compaction();
    void trigger_compaction();
    future<> run_compaction();
    void set_compaction_strategy(sstables::compaction_strategy_type strategy);
    bool compaction_manager_queued() const;
    void set_compaction_manager_queued(bool compaction_manager_queued);
    bool pending_compactions() const;

    const stats& get_stats() const {
        return _stats;
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

    seastar::gate _in_flight_seals;

    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    // Func signature: bool (const decorated_key& dk, const mutation_partition& mp)
    template <typename Func>
    future<bool> for_all_partitions(Func&& func) const;
    future<sstables::entry_descriptor> probe_file(sstring sstdir, sstring fname);
    void seal_on_overflow();
    void check_valid_rp(const db::replay_position&) const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

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
        size_t max_memtable_size = 5'000'000;
        logalloc::region_group* dirty_memory_region_group = nullptr;
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

    const sstring& datadir() const {
        return _config.datadir;
    }
private:
    sstring column_family_directory(const sstring& name, utils::UUID uuid) const;
};

class no_such_keyspace : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class no_such_column_family : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
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
    future<> apply_in_memory(const frozen_mutation&, const db::replay_position&);
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
    future<> do_apply(const frozen_mutation&);
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

    const compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
    }

    future<> init_system_keyspace();
    future<> load_sstables(distributed<service::storage_proxy>& p); // after init_system_keyspace()

    void add_column_family(schema_ptr schema, column_family::config cfg);

    future<> update_column_family(const sstring& ks_name, const sstring& cf_name);
    void drop_column_family(const sstring& ks_name, const sstring& cf_name);

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
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges);
    future<reconcilable_result> query_mutations(const query::read_command& cmd, const query::partition_range& range);
    future<> apply(const frozen_mutation&);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm);
    const sstring& get_snitch_name() const;

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const std::unordered_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }
    const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() const {
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
column_family::apply(const frozen_mutation& m, const db::replay_position& rp) {
    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    check_valid_rp(rp);
    active_memtable().apply(m, rp);
    seal_on_overflow();
    _stats.writes.mark(lc);
}

future<> update_schema_version_and_announce(service::storage_proxy& proxy);

#endif /* DATABASE_HH_ */
