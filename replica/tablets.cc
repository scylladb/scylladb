/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>

#include "types/types.hh"
#include "types/tuple.hh"
#include "types/list.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_builder.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/stats.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "sstables/sstable_set.hh"
#include "dht/token.hh"
#include "mutation/async_utils.hh"

namespace replica {

using namespace locator;

static thread_local auto replica_type = tuple_type_impl::get_instance({uuid_type, int32_type});
static thread_local auto replica_set_type = list_type_impl::get_instance(replica_type, false);
static thread_local auto tablet_info_type = tuple_type_impl::get_instance({long_type, long_type, replica_set_type});

data_type get_replica_set_type() {
    return replica_set_type;
}

data_type get_tablet_info_type() {
    return tablet_info_type;
}

schema_ptr make_tablets_schema() {
    // FIXME: Allow UDTs in system keyspace:
    // CREATE TYPE tablet_replica (replica_id uuid, shard int);
    // replica_set_type = frozen<list<tablet_replica>>
    auto id = generate_legacy_id(db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    return schema_builder(db::system_keyspace::NAME, db::system_keyspace::TABLETS, id)
            .with_column("table_id", uuid_type, column_kind::partition_key)
            .with_column("tablet_count", int32_type, column_kind::static_column)
            .with_column("keyspace_name", utf8_type, column_kind::static_column)
            .with_column("table_name", utf8_type, column_kind::static_column)
            .with_column("last_token", long_type, column_kind::clustering_key)
            .with_column("replicas", replica_set_type)
            .with_column("new_replicas", replica_set_type)
            .with_column("stage", utf8_type)
            .with_column("transition", utf8_type)
            .with_column("session", uuid_type)
            .with_column("resize_type", utf8_type, column_kind::static_column)
            .with_column("resize_seq_number", long_type, column_kind::static_column)
            .with_version(db::system_keyspace::generate_schema_version(id))
            .build();
}

std::vector<data_value> replicas_to_data_value(const tablet_replica_set& replicas) {
    std::vector<data_value> result;
    result.reserve(replicas.size());
    for (auto&& replica : replicas) {
        result.emplace_back(make_tuple_value(replica_type, {
                data_value(utils::UUID(replica.host.uuid())),
                data_value(int(replica.shard))
        }));
    }
    return result;
};

future<mutation>
tablet_map_to_mutation(const tablet_map& tablets, table_id id, const sstring& keyspace_name, const sstring& table_name,
                       api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    mutation m(s, partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    ));
    m.partition().apply(tombstone(tombstone_ts, gc_now));
    m.set_static_cell("tablet_count", data_value(int(tablets.tablet_count())), ts);
    m.set_static_cell("keyspace_name", data_value(keyspace_name), ts);
    m.set_static_cell("table_name", data_value(table_name), ts);
    m.set_static_cell("resize_type", data_value(tablets.resize_decision().type_name()), ts);
    m.set_static_cell("resize_seq_number", data_value(int64_t(tablets.resize_decision().sequence_number)), ts);

    tablet_id tid = tablets.first_tablet();
    for (auto&& tablet : tablets.tablets()) {
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
        m.set_clustered_cell(ck, "replicas", make_list_value(replica_set_type, replicas_to_data_value(tablet.replicas)), ts);
        if (auto tr_info = tablets.get_tablet_transition_info(tid)) {
            m.set_clustered_cell(ck, "stage", tablet_transition_stage_to_string(tr_info->stage), ts);
            m.set_clustered_cell(ck, "transition", tablet_transition_kind_to_string(tr_info->transition), ts);
            m.set_clustered_cell(ck, "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(tr_info->next)), ts);
            if (tr_info->session_id) {
                m.set_clustered_cell(ck, "session", data_value(tr_info->session_id.uuid()), ts);
            }
        }
        tid = *tablets.next_tablet(tid);
        co_await coroutine::maybe_yield();
    }
    co_return std::move(m);
}

tablet_mutation_builder&
tablet_mutation_builder::set_new_replicas(dht::token last_token, locator::tablet_replica_set replicas) {
    _m.set_clustered_cell(get_ck(last_token), "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(replicas)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_replicas(dht::token last_token, locator::tablet_replica_set replicas) {
    _m.set_clustered_cell(get_ck(last_token), "replicas", make_list_value(replica_set_type, replicas_to_data_value(replicas)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_stage(dht::token last_token, locator::tablet_transition_stage stage) {
    _m.set_clustered_cell(get_ck(last_token), "stage", data_value(tablet_transition_stage_to_string(stage)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_transition(dht::token last_token, locator::tablet_transition_kind kind) {
    _m.set_clustered_cell(get_ck(last_token), "transition", data_value(tablet_transition_kind_to_string(kind)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_session(dht::token last_token, service::session_id session_id) {
    _m.set_clustered_cell(get_ck(last_token), "session", data_value(session_id.uuid()), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_session(dht::token last_token) {
    auto session_col = _s->get_column_definition("session");
    _m.set_clustered_cell(get_ck(last_token), *session_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_transition(dht::token last_token) {
    auto ck = get_ck(last_token);
    auto stage_col = _s->get_column_definition("stage");
    _m.set_clustered_cell(ck, *stage_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto transition_col = _s->get_column_definition("transition");
    _m.set_clustered_cell(ck, *transition_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto new_replicas_col = _s->get_column_definition("new_replicas");
    _m.set_clustered_cell(ck, *new_replicas_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto session_col = _s->get_column_definition("session");
    _m.set_clustered_cell(ck, *session_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_resize_decision(locator::resize_decision resize_decision) {
    _m.set_static_cell("resize_type", data_value(resize_decision.type_name()), _ts);
    _m.set_static_cell("resize_seq_number", data_value(int64_t(resize_decision.sequence_number)), _ts);
    return *this;
}

mutation make_drop_tablet_map_mutation(table_id id, api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    mutation m(s, partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    ));
    m.partition().apply(tombstone(ts, gc_clock::now()));
    return m;
}

tablet_replica_set tablet_replica_set_from_cell(const data_value& v) {
    tablet_replica_set result;
    auto list_v = value_cast<list_type_impl::native_type>(v);
    result.reserve(list_v.size());
    for (const data_value& replica_v : list_v) {
        std::vector<data_value> replica_dv = value_cast<tuple_type_impl::native_type>(replica_v);
        result.emplace_back(
            host_id(value_cast<utils::UUID>(replica_dv[0])),
            shard_id(value_cast<int>(replica_dv[1]))
        );
    }
    return result;
}

static
tablet_replica_set deserialize_replica_set(cql3::untyped_result_set_row::view_type raw_value) {
    return tablet_replica_set_from_cell(
            replica_set_type->deserialize_value(raw_value));
}

future<> save_tablet_metadata(replica::database& db, const tablet_metadata& tm, api::timestamp_type ts) {
    tablet_logger.trace("Saving tablet metadata: {}", tm);
    std::vector<mutation> muts;
    muts.reserve(tm.all_tables().size());
    for (auto&& [id, tablets] : tm.all_tables()) {
        // FIXME: Should we ignore missing tables? Currently doesn't matter because this is only used in tests.
        auto s = db.find_schema(id);
        muts.emplace_back(
                co_await tablet_map_to_mutation(*tablets, id, s->ks_name(), s->cf_name(), ts));
    }
    co_await db.apply(freeze(muts), db::no_timeout);
}

static table_id to_tablet_metadata_key(const schema& s, const partition_key& key) {
    const auto elements = key.explode(s);
    return ::table_id(value_cast<utils::UUID>(uuid_type->deserialize_value(elements.front())));
}

static dht::token to_tablet_metadata_row_key(const schema& s, const clustering_key& key) {
    const auto elements = key.explode(s);
    return dht::token::from_int64(value_cast<int64_t>(long_type->deserialize_value(elements[0])));
}

static void do_update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const schema& s, const mutation& m) {
    const auto table_id = to_tablet_metadata_key(s, m.key());
    auto it = hint.tables.try_emplace(table_id, locator::tablet_metadata_change_hint::table_hint{table_id, {}}).first;

    const auto& mp = m.partition();
    auto& tokens = it->second.tokens;

    if (mp.partition_tombstone() || !mp.row_tombstones().empty() || !mp.static_row().empty()) {
        // If there is a partition tombstone, range tombstone or static row,
        // update the entire partition. Also clear any row hints that might be
        // present to force a full read of the partition.
        tokens.clear();
        return;
    }

    for (const auto& row : mp.clustered_rows()) {
        // TODO: we do not handle deletions yet, will revisit when tablet count
        // reduction is worked out.
        if (row.row().deleted_at()) {
            tokens.clear();
            return;
        }
        tokens.push_back(to_tablet_metadata_row_key(s, row.key()));
    }
}

std::optional<locator::tablet_metadata_change_hint> get_tablet_metadata_change_hint(const std::vector<canonical_mutation>& mutations) {
    tablet_logger.trace("tablet_metadata_change_hint({})", mutations.size());
    auto s = db::system_keyspace::tablets();

    std::optional<locator::tablet_metadata_change_hint> hint;

    for (const auto& cm : mutations) {
        tablet_logger.trace("tablet_metadata_change_hint() {} == {}", cm.column_family_id(), s->id());
        if (cm.column_family_id() != s->id()) {
            continue;
        }
        if (!hint) {
            hint.emplace();
            hint->tables.reserve(mutations.size());
        }
        do_update_tablet_metadata_change_hint(*hint, *s, cm.to_mutation(s));
    }

    return hint;
}

void update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const mutation& m) {
    auto s = db::system_keyspace::tablets();
    if (m.column_family_id() != s->id()) {
        return;
    }
    do_update_tablet_metadata_change_hint(hint, *s, m);
}

namespace {

tablet_id process_one_row(table_id table, tablet_map& map, tablet_id tid, const cql3::untyped_result_set_row& row) {
    tablet_replica_set tablet_replicas;
    if (row.has("replicas")) {
        tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
    }

    tablet_replica_set new_tablet_replicas;
    if (row.has("new_replicas")) {
        new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
    }

    if (row.has("stage")) {
        auto stage = tablet_transition_stage_from_string(row.get_as<sstring>("stage"));
        auto transition = tablet_transition_kind_from_string(row.get_as<sstring>("transition"));

        std::unordered_set<tablet_replica> pending(new_tablet_replicas.begin(), new_tablet_replicas.end());
        for (auto&& r : tablet_replicas) {
            pending.erase(r);
        }
        std::optional<tablet_replica> pending_replica;
        if (pending.size() > 1) {
            throw std::runtime_error(format("Too many pending replicas for table {} tablet {}: {}",
                                            table, tid, pending));
        }
        if (pending.size() != 0) {
            pending_replica = *pending.begin();
        }
        service::session_id session_id;
        if (row.has("session")) {
            session_id = service::session_id(row.get_as<utils::UUID>("session"));
        }
        map.set_tablet_transition_info(tid, tablet_transition_info{stage, transition,
                std::move(new_tablet_replicas), pending_replica, session_id});
    }

    map.set_tablet(tid, tablet_info{std::move(tablet_replicas)});

    auto persisted_last_token = dht::token::from_int64(row.get_as<int64_t>("last_token"));
    auto current_last_token = map.get_last_token(tid);
    if (current_last_token != persisted_last_token) {
        tablet_logger.debug("current tablet_map: {}", map);
        throw std::runtime_error(format("last_token mismatch between on-disk ({}) and in-memory ({}) tablet map for table {} tablet {}",
                                        persisted_last_token, current_last_token, table, tid));
    }

    return *map.next_tablet(tid);
}

struct tablet_metadata_builder {
    tablet_metadata& tm;
    struct active_tablet_map {
        table_id table;
        tablet_map map;
        tablet_id tid;
    };
    std::optional<active_tablet_map> current;

    void process_row(const cql3::untyped_result_set_row& row) {
        auto table = table_id(row.get_as<utils::UUID>("table_id"));

        if (!current || current->table != table) {
            if (current) {
                tm.set_tablet_map(current->table, std::move(current->map));
            }
            auto tablet_count = row.get_as<int>("tablet_count");
            auto tmap = tablet_map(tablet_count);
            current = active_tablet_map{table, tmap, tmap.first_tablet()};

            // Resize decision fields are static columns, so set them only once per table.
            if (row.has("resize_type") && row.has("resize_seq_number")) {
                auto resize_type_name = row.get_as<sstring>("resize_type");
                int64_t resize_seq_number = row.get_as<int64_t>("resize_seq_number");

                locator::resize_decision resize_decision(std::move(resize_type_name), resize_seq_number);
                current->map.set_resize_decision(std::move(resize_decision));
            }
        }

        current->tid = process_one_row(current->table, current->map, current->tid, row);
    }

    void on_end_of_stream() {
        if (current) {
            tm.set_tablet_map(current->table, std::move(current->map));
        }
    }
};

} // anonymous namespace

future<tablet_metadata> read_tablet_metadata(cql3::query_processor& qp) {
    tablet_metadata tm;
    tablet_metadata_builder builder{tm};
    tablet_logger.trace("Start reading tablet metadata");
    try {
        co_await qp.query_internal("select * from system.tablets",
           [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
               builder.process_row(row);
               return make_ready_future<stop_iteration>(stop_iteration::no);
           });
    } catch (...) {
        if (builder.current) {
            std::throw_with_nested(std::runtime_error(format("Failed to read tablet metadata for table {}", builder.current->table)));
        } else {
            std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
        }
    }
    builder.on_end_of_stream();
    tablet_logger.trace("Read tablet metadata: {}", tm);
    co_return std::move(tm);
}

future<std::unordered_set<locator::host_id>> read_required_hosts(cql3::query_processor& qp) {
    std::unordered_set<locator::host_id> hosts;

    auto process_row = [&] (const cql3::untyped_result_set_row& row) {
        tablet_replica_set tablet_replicas;
        if (row.has("replicas")) {
            tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
        }

        for (auto&& r : tablet_replicas) {
            hosts.insert(r.host);
        }

        if (row.has("new_replicas")) {
            tablet_replica_set new_tablet_replicas;
            new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
            for (auto&& r : new_tablet_replicas) {
                hosts.insert(r.host);
            }
        }
    };

    try {
        co_await qp.query_internal("select replicas, new_replicas from system.tablets",
           [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
               process_row(row);
               return make_ready_future<stop_iteration>(stop_iteration::no);
           });
    } catch (...) {
        std::throw_with_nested(std::runtime_error("Failed to read tablet required hosts"));
    }

    co_return std::move(hosts);
}

static future<>
do_update_tablet_metadata_partition(cql3::query_processor& qp, tablet_metadata& tm, const tablet_metadata_change_hint::table_hint& hint) {
    tablet_metadata_builder builder{tm};
    co_await qp.query_internal(
            "select * from system.tablets where table_id = ?",
            db::consistency_level::ONE,
            {data_value(hint.table_id.uuid())},
            1000,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                builder.process_row(row);
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
    if (builder.current) {
        tm.set_tablet_map(builder.current->table, std::move(builder.current->map));
    } else {
        tm.drop_tablet_map(hint.table_id);
    }
}

static future<>
do_update_tablet_metadata_rows(cql3::query_processor& qp, tablet_map& tmap, const tablet_metadata_change_hint::table_hint& hint) {
    for (const auto token : hint.tokens) {
        auto res = co_await qp.execute_internal(
                "select * from system.tablets where table_id = ? and last_token = ?",
                db::consistency_level::ONE,
                {data_value(hint.table_id.uuid()), data_value(dht::token::to_int64(token))},
                cql3::query_processor::cache_internal::yes);
        const auto tid = tmap.get_tablet_id(token);
        if (res->empty()) {
            throw std::runtime_error("Failed to update tablet metadata: updated row is empty");
        } else {
            tmap.clear_tablet_transition_info(tid);
            process_one_row(hint.table_id, tmap, tid, res->one());
        }
    }
}

future<> update_tablet_metadata(cql3::query_processor& qp, tablet_metadata& tm, const locator::tablet_metadata_change_hint& hint) {
    try {
        for (const auto& [_, table_hint] : hint.tables) {
            if (table_hint.tokens.empty()) {
                co_await do_update_tablet_metadata_partition(qp, tm, table_hint);
            } else {
                co_await tm.mutate_tablet_map_async(table_hint.table_id, [&] (tablet_map& tmap) -> future<> {
                    co_await do_update_tablet_metadata_rows(qp, tmap, table_hint);
                });
            }
        }
    } catch (...) {
        std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
    }
    tablet_logger.trace("Updated tablet metadata: {}", tm);
}

future<std::vector<canonical_mutation>> read_tablet_mutations(seastar::sharded<replica::database>& db) {
    auto s = db::system_keyspace::tablets();
    auto rs = co_await db::system_keyspace::query_mutations(db, db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    std::vector<canonical_mutation> result;
    result.reserve(rs->partitions().size());
    for (auto& p: rs->partitions()) {
        result.emplace_back(co_await make_canonical_mutation_gently(co_await unfreeze_gently(p.mut(), s)));
    }
    co_return std::move(result);
}

// This sstable set provides access to all the stables in the table, using a snapshot of all
// its tablets/storage_groups compound_sstable_set:s.
// The managed sets cannot be modified through tablet_sstable_set, but only jointly read from, so insert() and erase() are disabled.
class tablet_sstable_set : public sstables::sstable_set_impl {
    schema_ptr _schema;
    locator::tablet_map _tablet_map;
    // Keep a single (compound) sstable_set per tablet/storage_group
    absl::flat_hash_map<size_t, lw_shared_ptr<const sstables::sstable_set>, absl::Hash<size_t>> _sstable_sets;
    // Used when ordering is required for correctness, but hot paths will use flat_hash_map
    // which provides faster lookup time.
    std::set<size_t> _sstable_set_ids;
    size_t _size = 0;
    uint64_t _bytes_on_disk = 0;

public:
    tablet_sstable_set(schema_ptr s, const storage_group_manager& sgm, const locator::tablet_map& tmap)
        : _schema(std::move(s))
        , _tablet_map(tmap.tablet_count())
    {
        sgm.for_each_storage_group([this] (size_t id, storage_group& sg) {
            auto set = sg.make_sstable_set();
            _size += set->size();
            _bytes_on_disk += set->bytes_on_disk();
            _sstable_sets[id] = std::move(set);
            _sstable_set_ids.insert(id);
        });
    }

    static lw_shared_ptr<sstables::sstable_set> make(schema_ptr s, const storage_group_manager& sgm, const locator::tablet_map& tmap) {
        return make_lw_shared<sstables::sstable_set>(std::make_unique<tablet_sstable_set>(std::move(s), sgm, tmap));
    }

    const schema_ptr& schema() const noexcept {
        return _schema;
    }

    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<tablet_sstable_set>(*this);
    }

    virtual std::vector<sstables::shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const sstables::shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const sstables::shared_sstable&)> func) const override;
    virtual bool insert(sstables::shared_sstable sst) override;
    virtual bool erase(sstables::shared_sstable sst) override;
    virtual size_t size() const noexcept override {
        return _size;
    }
    virtual uint64_t bytes_on_disk() const noexcept override {
        return _bytes_on_disk;
    }
    virtual selector_and_schema_t make_incremental_selector() const override;

    virtual mutation_reader create_single_key_sstable_reader(
            replica::column_family*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding,
            const sstables::sstable_predicate&) const override;

    // Will always return an engaged sstable set ptr.
    const lw_shared_ptr<const sstables::sstable_set>& find_sstable_set(size_t i) const {
        auto it = _sstable_sets.find(i);
        if (it == _sstable_sets.end() || !it->second) [[unlikely]] {
            on_internal_error(tablet_logger, format("SSTable set wasn't found for tablet {} of table {}.{}", i, schema()->ks_name(), schema()->cf_name()));
        }
        return it->second;
    }

private:
    size_t group_of(const dht::token& t) const noexcept {
        return _tablet_map.get_tablet_id(t).id;
    }
    dht::token first_token_of(size_t idx) const noexcept {
#ifndef SCYLLA_BUILD_MODE_RELEASE
        if (idx >= _tablet_map.tablet_count()) {
            on_fatal_internal_error(tablet_logger, format("first_token_of: idx={} out of range", idx));
        }
#endif
        return _tablet_map.get_first_token(tablet_id(idx));
    }
    dht::token last_token_of(size_t idx) const noexcept {
#ifndef SCYLLA_BUILD_MODE_RELEASE
        if (idx >= _tablet_map.tablet_count()) {
            on_fatal_internal_error(tablet_logger, format("last_token_of: idx={} out of range", idx));
        }
#endif
        return _tablet_map.get_last_token(tablet_id(idx));
    }
    stop_iteration for_each_sstable_set_until(const dht::partition_range&, std::function<stop_iteration(lw_shared_ptr<const sstables::sstable_set>)>) const;
    future<stop_iteration> for_each_sstable_set_gently_until(const dht::partition_range&, std::function<future<stop_iteration>(lw_shared_ptr<const sstables::sstable_set>)>) const;

    auto subrange(const dht::partition_range& pr) const {
        size_t candidate_start = pr.start() ? group_of(pr.start()->value().token()) : size_t(0);
        size_t candidate_end = pr.end() ? group_of(pr.end()->value().token()) : (_tablet_map.tablet_count() - 1);
        return std::ranges::subrange(_sstable_set_ids.lower_bound(candidate_start), _sstable_set_ids.upper_bound(candidate_end));
    }

    friend class tablet_incremental_selector;
};

lw_shared_ptr<sstables::sstable_set> make_tablet_sstable_set(schema_ptr s, const storage_group_manager& sgm, const locator::tablet_map& tmap) {
    return tablet_sstable_set::make(std::move(s), sgm, tmap);
}

future<std::optional<tablet_transition_stage>> read_tablet_transition_stage(cql3::query_processor& qp, table_id tid, dht::token last_token) {
    auto rs = co_await qp.execute_internal("select stage from system.tablets where table_id = ? and last_token = ?",
            {tid.uuid(), dht::token::to_int64(last_token)}, cql3::query_processor::cache_internal::no);
    if (rs->empty() || !rs->one().has("stage")) {
        co_return std::nullopt;
    }

    co_return tablet_transition_stage_from_string(rs->one().get_as<sstring>("stage"));
}

stop_iteration tablet_sstable_set::for_each_sstable_set_until(const dht::partition_range& pr, std::function<stop_iteration(lw_shared_ptr<const sstables::sstable_set>)> func) const {
    for (const auto& i : subrange(pr)) {
        const auto& set = find_sstable_set(i);
        if (func(set) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> tablet_sstable_set::for_each_sstable_set_gently_until(const dht::partition_range& pr, std::function<future<stop_iteration>(lw_shared_ptr<const sstables::sstable_set>)> func) const {
    for (const auto& i : subrange(pr)) {
        const auto& set = find_sstable_set(i);
        if (co_await func(set) == stop_iteration::yes) {
            co_return stop_iteration::yes;
        }
    }
    co_return stop_iteration::no;
}

std::vector<sstables::shared_sstable> tablet_sstable_set::select(const dht::partition_range& range) const {
    std::vector<sstables::shared_sstable> ret;
    ret.reserve(size());
    for_each_sstable_set_until(range, [&] (lw_shared_ptr<const sstables::sstable_set> set) {
        auto ssts = set->select(range);
        if (ret.empty()) {
            ret = std::move(ssts);
        } else {
            std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
        }
        return stop_iteration::no;
    });
    tablet_logger.debug("tablet_sstable_set::select: range={} ret={}", range, ret.size());
    return ret;
}

lw_shared_ptr<const sstable_list> tablet_sstable_set::all() const {
    auto ret = make_lw_shared<sstable_list>();
    ret->reserve(size());
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<const sstables::sstable_set> set) {
        set->for_each_sstable([&] (const sstables::shared_sstable& sst) {
            ret->insert(sst);
        });
        return stop_iteration::no;
    });
    return ret;
}

stop_iteration tablet_sstable_set::for_each_sstable_until(std::function<stop_iteration(const sstables::shared_sstable&)> func) const {
    return for_each_sstable_set_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<const sstables::sstable_set> set) {
        return set->for_each_sstable_until(func);
    });
}

future<stop_iteration> tablet_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const sstables::shared_sstable&)> func) const {
    return for_each_sstable_set_gently_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<const sstables::sstable_set> set) {
        return set->for_each_sstable_gently_until(func);
    });
}

bool tablet_sstable_set::insert(sstables::shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
bool tablet_sstable_set::erase(sstables::shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}

class tablet_incremental_selector : public sstables::incremental_selector_impl {
    const tablet_sstable_set& _tset;

    // _cur_set and _cur_selector contain a snapshot
    // for the currently selected compaction_group.
    lw_shared_ptr<const sstables::sstable_set> _cur_set;
    std::optional<sstables::sstable_set::incremental_selector> _cur_selector;
    dht::token _lowest_next_token = dht::maximum_token();

public:
    tablet_incremental_selector(const tablet_sstable_set& tset)
            : _tset(tset)
    {}

    virtual std::tuple<dht::partition_range, std::vector<sstables::shared_sstable>, dht::ring_position_ext> select(const selector_pos& s) override {
        // Always return minimum singular range, such that incremental_selector::select() will always call this function,
        // which in turn will find the next sstable set to select sstables from.
        const dht::partition_range current_range = dht::partition_range::make_singular(dht::ring_position::min());

        // pos must be monotonically increasing in the weak sense
        // but caller can skip to a position outside the current set
        const dht::ring_position_view& pos = s.pos;
        auto token = pos.token();
        if (!_cur_set || pos.token() >= _lowest_next_token) {
            auto idx = _tset.group_of(token);
            auto pr_end = s.range ? dht::ring_position_view::for_range_end(*s.range) : dht::ring_position_view::max();
            // End of stream is reached when pos is past the end of the read range (i.e. exclude tablets
            // that doesn't intersect with the range).
            if (dht::ring_position_tri_compare(*_tset.schema(), pos, pr_end) <= 0 && _tset._sstable_set_ids.contains(idx)) {
                _cur_set = _tset.find_sstable_set(idx);
            }
            // Set the next token to point to the next engaged storage group.
            // It will be considered later on when the _cur_set is exhausted
            _lowest_next_token = find_lowest_next_token(idx);
        }

        if (!_cur_set) {
            auto lowest_next_position = _lowest_next_token.is_maximum()
                ? dht::ring_position_ext::max()
                : dht::ring_position_ext::starting_at(_lowest_next_token);
            tablet_logger.debug("tablet_incremental_selector {}.{}: select pos={}: returning 0 sstables, next_pos={}",
                    _tset.schema()->ks_name(), _tset.schema()->cf_name(), pos, lowest_next_position);
            return std::make_tuple(std::move(current_range), std::vector<sstables::shared_sstable>{}, lowest_next_position);
        }

        _cur_selector.emplace(_cur_set->make_incremental_selector());

        auto res = _cur_selector->select(s);
        // Return all sstables selected on the requested position from the first matching sstable set.
        // This assumes that the underlying sstable sets are disjoint in their token ranges so
        // only one of them contain any given token.
        auto sstables = std::move(res.sstables);
        // Return the lowest next position, such that this function will be called again to select the
        // lowest next position from the selector which previously returned it.
        // Until the current selector is exhausted. In that case,
        // jump to the next compaction_group sstable set.
        dht::ring_position_ext next_position = res.next_position;
        if (next_position.is_max()) {
            // _cur_selector is exhausted.
            // Return a position starting at `_lowest_next_token`
            // that was calculated for the _cur_set
            // (unless it's already maximum_token in which case we just return next_position == ring_position::max()).
            _cur_set = {};
            _cur_selector.reset();
            if (!_lowest_next_token.is_maximum()) {
                next_position = dht::ring_position_ext::starting_at(_lowest_next_token);
            }
        }

        tablet_logger.debug("tablet_incremental_selector {}.{}: select pos={}: returning {} sstables, next_pos={}",
                _tset.schema()->ks_name(), _tset.schema()->cf_name(), pos, sstables.size(), next_position);
        return std::make_tuple(std::move(current_range), std::move(sstables), std::move(next_position));
    }

private:
    // Find the start token of the first engaged sstable_set
    // starting the search from `current_idx` (exclusive).
    dht::token find_lowest_next_token(size_t current_idx) {
        auto it = _tset._sstable_set_ids.upper_bound(current_idx);
        if (it != _tset._sstable_set_ids.end()) {
            return _tset.first_token_of(*it);
        }
        return dht::maximum_token();
    }
};

mutation_reader
tablet_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstables::sstable_predicate& predicate) const {
    // The singular partition_range start bound must be engaged.
    auto idx = group_of(pr.start()->value().token());
    const auto& set = find_sstable_set(idx);
    return set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
}

sstables::sstable_set_impl::selector_and_schema_t tablet_sstable_set::make_incremental_selector() const {
    return std::make_tuple(std::make_unique<tablet_incremental_selector>(*this), *_schema);
}

} // namespace replica
