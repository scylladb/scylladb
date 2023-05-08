/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "dht/i_partitioner.hh"
#include "gms/feature_service.hh"
#include "repair/row_level.hh"

dht::token create_token_from_key(const dht::i_partitioner& partitioner, sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = partitioner.get_token(key_view);
    assert(token == partitioner.get_token(key_view));
    return token;
}

range<dht::token> create_token_range_from_keys(const dht::sharder& sinfo, const dht::i_partitioner& partitioner, sstring start_key, sstring end_key) {
    dht::token start = create_token_from_key(partitioner, start_key);
    assert(this_shard_id() == sinfo.shard_of(start));
    dht::token end = create_token_from_key(partitioner, end_key);
    assert(this_shard_id() == sinfo.shard_of(end));
    assert(end >= start);
    return range<dht::token>::make(start, end);
}

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

table_for_tests::data::data()
    : semaphore(reader_concurrency_semaphore::no_limits{}, "table_for_tests")
{ }

table_for_tests::data::~data() {}

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager)
    : table_for_tests(
        sstables_manager,
        schema_builder(some_keyspace, some_column_family)
            .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
            .build()
    )
{ }

class table_for_tests::table_state : public compaction::table_state {
    table_for_tests::data& _data;
    sstables::sstables_manager& _sstables_manager;
    std::vector<sstables::shared_sstable> _compacted_undeleted;
    tombstone_gc_state _tombstone_gc_state;
    mutable compaction_backlog_tracker _backlog_tracker;
    seastar::condition_variable _staging_condition;
private:
    replica::table& table() const noexcept {
        return *_data.cf;
    }
public:
    explicit table_state(table_for_tests::data& data, sstables::sstables_manager& sstables_manager)
            : _data(data)
            , _sstables_manager(sstables_manager)
            , _tombstone_gc_state(nullptr)
            , _backlog_tracker(get_compaction_strategy().make_backlog_tracker())
    {
    }
    const schema_ptr& schema() const noexcept override {
        return table().schema();
    }
    unsigned min_compaction_threshold() const noexcept override {
        return schema()->min_compaction_threshold();
    }
    bool compaction_enforce_min_threshold() const noexcept override {
        return true;
    }
    const sstables::sstable_set& main_sstable_set() const override {
        return table().as_table_state().main_sstable_set();
    }
    const sstables::sstable_set& maintenance_sstable_set() const override {
        return table().as_table_state().maintenance_sstable_set();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point query_time) const override {
        return sstables::get_fully_expired_sstables(*this, sstables, query_time);
    }
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override {
        return _compacted_undeleted;
    }
    sstables::compaction_strategy& get_compaction_strategy() const noexcept override {
        return table().get_compaction_strategy();
    }
    reader_permit make_compaction_reader_permit() const override {
        return _data.semaphore.make_tracking_only_permit(&*schema(), "table_for_tests::table_state", db::no_timeout);
    }
    sstables::sstables_manager& get_sstables_manager() noexcept override {
        return _sstables_manager;
    }
    sstables::shared_sstable make_sstable() const override {
        return table().make_sstable();
    }
    sstables::sstable_writer_config configure_writer(sstring origin) const override {
        return _sstables_manager.configure_writer(std::move(origin));
    }

    api::timestamp_type min_memtable_timestamp() const override {
        return table().min_memtable_timestamp();
    }
    future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        return table().as_table_state().on_compaction_completion(std::move(desc), offstrategy);
    }
    bool is_auto_compaction_disabled_by_user() const noexcept override {
        return table().is_auto_compaction_disabled_by_user();
    }
    const tombstone_gc_state& get_tombstone_gc_state() const noexcept override {
        return _tombstone_gc_state;
    }
    compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
    seastar::condition_variable& get_staging_done_condition() noexcept override {
        return _staging_condition;
    }
};

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s, std::optional<sstring> datadir)
    : _data(make_lw_shared<data>())
{
    _data->s = s;
    _data->cfg = replica::table::config{.compaction_concurrency_semaphore = &_data->semaphore};
    _data->cfg.enable_disk_writes = bool(datadir);
    _data->cfg.datadir = datadir.value_or(sstring());
    _data->cfg.cf_stats = &_data->cf_stats;
    _data->cfg.enable_commitlog = false;
    _data->cm.enable();
    _data->cf = make_lw_shared<replica::column_family>(_data->s, _data->cfg, replica::column_family::no_commitlog(), _data->cm, sstables_manager, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
    _data->table_s = std::make_unique<table_state>(*_data, sstables_manager);
    _data->cm.add(*_data->table_s);
}

compaction::table_state& table_for_tests::as_table_state() noexcept {
    return *_data->table_s;
}

future<> table_for_tests::stop() {
    auto data = _data;
    co_await data->cm.remove(*data->table_s);
    co_await when_all_succeed(data->cm.stop(), data->semaphore.stop()).discard_result();
}

namespace sstables {

test_env::impl::impl(test_env_config cfg)
    : dir_sem(1)
    , feature_service(gms::feature_config_from_db_config(db_config))
    , mgr(cfg.large_data_handler == nullptr ? nop_ld_handler : *cfg.large_data_handler, db_config, feature_service, cache_tracker, memory::stats().total_memory(), dir_sem)
    , semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::test_env")
{ }

}
