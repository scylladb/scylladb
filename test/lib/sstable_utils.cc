/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/sstable_utils.hh"

#include "replica/database.hh"
#include "replica/memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include "sstables/version.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/boost/sstable_test.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>

using namespace sstables;
using namespace std::chrono_literals;

lw_shared_ptr<replica::memtable> make_memtable(schema_ptr s, const std::vector<mutation>& muts) {
    auto mt = make_lw_shared<replica::memtable>(s);

    std::size_t i{0};
    for (auto&& m : muts) {
        mt->apply(m);
        // Give the reactor some time to breathe
        if (++i == 10) {
            seastar::thread::yield();
            i = 0;
        }
    }

    return mt;
}

std::vector<replica::memtable*> active_memtables(replica::table& t) {
    std::vector<replica::memtable*> active_memtables;
    t.for_each_active_memtable([&] (replica::memtable& mt) {
        active_memtables.push_back(&mt);
    });
    return active_memtables;
}

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, lw_shared_ptr<replica::memtable> mt) {
    return make_sstable_containing(sst_factory(), std::move(mt));
}

sstables::shared_sstable make_sstable_containing(sstables::shared_sstable sst, lw_shared_ptr<replica::memtable> mt) {
    write_memtable_to_sstable(*mt, sst).get();
    sstable_open_config cfg { .load_first_and_last_position_metadata = true };
    sst->open_data(cfg).get();
    return sst;
}

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts, validate do_validate) {
    return make_sstable_containing(sst_factory(), std::move(muts), do_validate);
}

sstables::shared_sstable make_sstable_containing(sstables::shared_sstable sst, std::vector<mutation> muts, validate do_validate) {
    schema_ptr s = muts[0].schema();
    make_sstable_containing(sst, make_memtable(s, muts));

    if (do_validate) {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        std::set<mutation, mutation_decorated_key_less_comparator> merged;
        for (auto&& m : muts) {
            auto it = merged.find(m);
            if (it == merged.end()) {
                merged.insert(std::move(m));
            } else {
                auto old = merged.extract(it);
                old.value().apply(std::move(m));
                merged.insert(std::move(old));
            }
        }

        // validate the sstable
        auto rd = assert_that(sst->as_mutation_source().make_reader_v2(s, semaphore.make_permit()));
        for (auto&& m : merged) {
            rd.produces(m);
        }
        rd.produces_end_of_stream();
    }
    return sst;
}

shared_sstable make_sstable_easy(test_env& env, mutation_reader rd, sstable_writer_config cfg,
        sstables::generation_type gen, const sstables::sstable::version_types version, int expected_partition, gc_clock::time_point query_time) {
    auto s = rd.schema();
    auto sst = env.make_sstable(s, gen, version, sstable_format_types::big, default_sstable_buffer_size, query_time);
    sst->write_components(std::move(rd), expected_partition, s, cfg, encoding_stats{}).get();
    sst->load(s->get_sharder()).get();
    return sst;
}

shared_sstable make_sstable_easy(test_env& env, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        sstables::generation_type gen, const sstable::version_types v, int estimated_partitions, gc_clock::time_point query_time) {
    return make_sstable_easy(env, mt->make_flat_reader(mt->schema(), env.make_reader_permit()), std::move(cfg), gen, v, estimated_partitions, query_time);
}

future<compaction_result> compact_sstables(test_env& env, sstables::compaction_descriptor descriptor, table_for_tests t,
                 std::function<shared_sstable()> creator, sstables::compaction_sstable_replacer_fn replacer, can_purge_tombstones can_purge) {
    auto& table_s = t.as_table_state();
    descriptor.creator = [creator = std::move(creator)] (shard_id dummy) mutable {
        return creator();
    };
    descriptor.replacer = std::move(replacer);
    if (can_purge) {
        descriptor.enable_garbage_collection(table_s.main_sstable_set());
    }
    sstables::compaction_result ret;
    co_await run_compaction_task(env, descriptor.run_identifier, table_s, [&] (sstables::compaction_data& cdata) {
        return do_with(compaction_progress_monitor{}, [&] (compaction_progress_monitor& progress_monitor) {
            return sstables::compact_sstables(std::move(descriptor), cdata, table_s, progress_monitor).then([&] (sstables::compaction_result res) {
                ret = std::move(res);
            });
        });
    });
    co_return ret;
}

class compaction_manager_test_task : public compaction::compaction_task_executor {
    sstables::run_id _run_id;
    noncopyable_function<future<> (sstables::compaction_data&)> _job;
    gate::holder _hold;

public:
    compaction_manager_test_task(compaction_manager& cm, table_state& table_s, sstables::run_id run_id, noncopyable_function<future<> (sstables::compaction_data&)> job)
        : compaction::compaction_task_executor(cm, throw_if_stopping::no, &table_s, sstables::compaction_type::Compaction, "Test compaction")
        , _run_id(run_id)
        , _job(std::move(job))
        , _hold(_compaction_state.gate.hold())
    { }

protected:
    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        setup_new_compaction(_run_id);
        return _job(_compaction_data).then([] {
            return make_ready_future<compaction_manager::compaction_stats_opt>(std::nullopt);
        });
    }
};

future<> run_compaction_task(test_env& env, sstables::run_id output_run_id, table_state& table_s, noncopyable_function<future<> (sstables::compaction_data&)> job) {
    auto& tcm = env.test_compaction_manager();
    auto task = make_shared<compaction_manager_test_task>(tcm.get_compaction_manager(), table_s, output_run_id, std::move(job));
    co_await tcm.perform_compaction(std::move(task));
}

shared_sstable verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, bytes key, std::function<void(mutation_opt&)> verify) {
    auto sstp = make_sstable_containing(std::move(sst), mt);
    return verify_mutation(env, std::move(sstp), std::move(key), std::move(verify));
}

shared_sstable verify_mutation(test_env& env, shared_sstable sstp, bytes key, std::function<void(mutation_opt&)> verify) {
    auto s = sstp->get_schema();
    auto pr = dht::partition_range::make_singular(make_dkey(s, key));
    auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
    auto close_rd = deferred_close(rd);
    auto mopt = read_mutation_from_mutation_reader(rd).get();
    verify(mopt);
    return sstp;
}

shared_sstable verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify) {
    auto sstp = make_sstable_containing(std::move(sst), mt);
    return verify_mutation(env, std::move(sstp), std::move(pr), std::move(verify));
}

shared_sstable verify_mutation(test_env& env, shared_sstable sstp, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify) {
    auto s = sstp->get_schema();
    auto rd = sstp->make_reader(s, env.make_reader_permit(), std::move(pr), s->full_slice());
    auto close_rd = deferred_close(rd);
    while (auto mopt = read_mutation_from_mutation_reader(rd).get()) {
        if (verify(mopt) == stop_iteration::yes) {
            break;
        }
    }
    return sstp;
}
