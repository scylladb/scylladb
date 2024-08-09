/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/util/closeable.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_services.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"

#include "schema/schema.hh"
#include "schema/schema_builder.hh"

#include "sstables/sstables.hh"
#include "sstables/compress.hh"
#include "compaction/compaction.hh"
#include "compaction/compaction_manager.hh"
#include "replica/compaction_group.hh"

using namespace sstables;

static sstables::shared_sstable generate_sstable(schema_ptr s, std::function<shared_sstable()> sst_gen, noncopyable_function<bool(dht::token)> token_filter) {
    auto make_insert = [&] (const dht::decorated_key& key) {
        static thread_local int32_t value = 1;

        mutation m(s, key);
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(value++)});
        m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), api::timestamp_clock::now().time_since_epoch().count());
        return m;
    };

    auto keys = tests::generate_partition_keys(100, s);
    std::vector<mutation> muts;

    muts.reserve(keys.size());
    for (auto& k : keys) {
        if (token_filter(k.token())) {
            muts.push_back(make_insert(k));
        }
    }
    return make_sstable_containing(sst_gen, std::move(muts));
}

static sstables::shared_sstable sstable_that_needs_split(schema_ptr s, std::function<shared_sstable()> sst_gen) {
    return generate_sstable(std::move(s), std::move(sst_gen), [] (dht::token) { return true; });
}

class single_compaction_group : public compaction::table_state {
private:
    schema_ptr _schema;
    sstables::sstables_manager& _sst_man;
    sstables::sstable_set _main_set;
    sstables::sstable_set _maintenance_set;
    std::vector<sstables::shared_sstable> _compacted_undeleted_sstables;
    mutable sstables::compaction_strategy _compaction_strategy;
    compaction_strategy_state _compaction_strategy_state;
    tombstone_gc_state _tombstone_gc_state;
    compaction_backlog_tracker _backlog_tracker;
    condition_variable _staging_done_condition;
    std::function<shared_sstable()> _sstable_factory;
    mutable tests::reader_concurrency_semaphore_wrapper _semaphore;
public:
    single_compaction_group(table_for_tests& t, sstables::sstables_manager& sst_man, std::function<shared_sstable()> sstable_factory)
            : _schema(t.schema())
            , _sst_man(sst_man)
            , _main_set(sstables::make_partitioned_sstable_set(_schema, false))
            , _maintenance_set(sstables::make_partitioned_sstable_set(_schema, false))
            , _compaction_strategy(sstables::make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
            , _compaction_strategy_state(compaction::compaction_strategy_state::make(_compaction_strategy))
            , _tombstone_gc_state(nullptr)
            , _backlog_tracker(_compaction_strategy.make_backlog_tracker())
            , _sstable_factory(std::move(sstable_factory))
    {
        t->get_compaction_manager().add(*this);
    }

    future<> stop(table_for_tests& t) {
        return t->get_compaction_manager().remove(*this);
    }

    void rebuild_main_set(std::vector<shared_sstable> to_add, std::vector<shared_sstable> to_remove) {
        for (auto& sst : to_remove) {
            _main_set.erase(sst);
        }
        for (auto& sst : to_add) {
            _main_set.insert(sst);
        }
    }

    virtual const schema_ptr& schema() const noexcept override { return _schema; }
    virtual unsigned min_compaction_threshold() const noexcept override { return _schema->min_compaction_threshold(); }
    virtual bool compaction_enforce_min_threshold() const noexcept override { return false; }
    virtual const sstables::sstable_set& main_sstable_set() const override { return _main_set; }
    virtual const sstables::sstable_set& maintenance_sstable_set() const override { return _maintenance_set; }
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point compaction_time) const override { return {}; }
    virtual const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override { return _compacted_undeleted_sstables; }
    virtual sstables::compaction_strategy& get_compaction_strategy() const noexcept override { return _compaction_strategy; }
    virtual compaction_strategy_state& get_compaction_strategy_state() noexcept override { return _compaction_strategy_state; }
    virtual reader_permit make_compaction_reader_permit() const override { return _semaphore.make_permit(); }
    virtual sstables::sstables_manager& get_sstables_manager() noexcept override { return _sst_man; }
    virtual sstables::shared_sstable make_sstable() const override { return _sstable_factory(); }
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const override { return _sst_man.configure_writer(std::move(origin)); }
    virtual api::timestamp_type min_memtable_timestamp() const override { return api::min_timestamp; }
    virtual bool memtable_has_key(const dht::decorated_key& key) const override { return false; }
    virtual future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        testlog.info("Adding {} sstable(s), removing {} sstables", desc.new_sstables.size(), desc.old_sstables.size());
        rebuild_main_set(desc.new_sstables, desc.old_sstables);
        return make_ready_future<>();
    }
    virtual bool is_auto_compaction_disabled_by_user() const noexcept override { return false; }
    virtual bool tombstone_gc_enabled() const noexcept override { return false; }
    virtual const tombstone_gc_state& get_tombstone_gc_state() const noexcept override { return _tombstone_gc_state; }
    virtual compaction_backlog_tracker& get_backlog_tracker() override { return _backlog_tracker; }
    virtual const std::string get_group_id() const noexcept override { return "0"; }
    virtual seastar::condition_variable& get_staging_done_condition() noexcept override { return _staging_done_condition; }
};

SEASTAR_TEST_CASE(basic_compaction_group_splitting_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "compaction_group_splitting")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", int32_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto t = env.make_table_for_tests(s);
        auto close_table = deferred_stop(t);
        t->start();

        auto sst_factory = env.make_sst_factory(s);
        auto classifier = [] (dht::token t) -> mutation_writer::token_group_id {
            return dht::compaction_group_of(1, t);
        };
        auto sstable_needs_split = [&] (const sstables::shared_sstable& sst) {
            return classifier(sst->get_first_decorated_key().token()) != classifier(sst->get_last_decorated_key().token());
        };

        auto run_test = [&] (std::vector<sstables::shared_sstable> ssts, size_t expected_output, noncopyable_function<void(const sstables::shared_sstable&)> validate) {
            auto compaction_group = std::make_unique<single_compaction_group>(t, env.manager(), sst_factory);

            compaction_group->rebuild_main_set(ssts, {});

            auto& cm = t->get_compaction_manager();
            auto expected_compaction_size = boost::accumulate(ssts | boost::adaptors::transformed([&] (auto& sst) {
                // sstables that doesn't need split will have compaction bypassed.
                return sstable_needs_split(sst) ? sst->bytes_on_disk() : size_t(0);
            }), int64_t(0));

            auto ret = cm.perform_split_compaction(*compaction_group, sstables::compaction_type_options::split{classifier}, tasks::task_info{}).get();
            BOOST_REQUIRE_EQUAL(ret->start_size, expected_compaction_size);

            BOOST_REQUIRE(compaction_group->main_sstable_set().size() == expected_output);
            compaction_group->main_sstable_set().for_each_sstable([&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(!sstable_needs_split(sst));
                validate(sst);
            });
            compaction_group->stop(t).get();
        };

        // sstable that needs split case will generate 2 sstables, one for left, another for right.
        {
            auto input = sstable_that_needs_split(s, sst_factory);
            std::unordered_set<mutation_writer::token_group_id> expected_ids { 0, 1 };
            run_test({ input }, 2, [&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(expected_ids.erase(classifier(sst->get_first_decorated_key().token())) == 1);
            });
            BOOST_REQUIRE(expected_ids.empty());
        }
        // sstable that doesn't need split won't actually be compacted
        {
            auto input = generate_sstable(s, sst_factory, [&] (dht::token t) { return classifier(t) == 0; });
            run_test({ input }, 1, [&] (const sstables::shared_sstable& sst) {
                BOOST_REQUIRE(sst->generation() == input->generation());
                BOOST_REQUIRE_EQUAL(0, classifier(sst->get_first_decorated_key().token()));
            });
        }

        // combination of both cases
        {
            auto input1 = sstable_that_needs_split(s, sst_factory);
            auto input2 = generate_sstable(s, sst_factory, [&] (dht::token t) { return classifier(t) == 0; });
            bool found_input2 = false;
            run_test({ input1, input2 }, 3, [&] (const sstables::shared_sstable& sst) {
                found_input2 |= sst->generation() == input2->generation();
            });
            BOOST_REQUIRE(found_input2);
        }
    });
}
