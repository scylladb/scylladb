/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/sstable_utils.hh"

#include "replica/database.hh"
#include "replica/memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "sstables/version.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/boost/sstable_test.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>

using namespace sstables;
using namespace std::chrono_literals;

future<lw_shared_ptr<replica::memtable>> make_memtable(schema_ptr s, const utils::chunked_vector<mutation>& muts) {
    auto mt = make_lw_shared<replica::memtable>(s);

    for (auto&& m : muts) {
        mt->apply(m);
        // Give the reactor some time to breathe
        co_await coroutine::maybe_yield();
    }

    co_return mt;
}

std::vector<replica::memtable*> active_memtables(replica::table& t) {
    std::vector<replica::memtable*> active_memtables;
    t.for_each_active_memtable([&] (replica::memtable& mt) {
        active_memtables.push_back(&mt);
    });
    return active_memtables;
}

future<sstables::shared_sstable> make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, lw_shared_ptr<replica::memtable> mt) {
    return make_sstable_containing(sst_factory(), std::move(mt));
}

future<sstables::shared_sstable> make_sstable_containing(sstables::shared_sstable sst, lw_shared_ptr<replica::memtable> mt) {
    co_await write_memtable_to_sstable(*mt, sst);
    sstable_open_config cfg { .load_first_and_last_position_metadata = true };
    co_await sst->open_data(cfg);
    co_return sst;
}

future<sstables::shared_sstable> make_sstable_containing(sstables::shared_sstable sst, utils::chunked_vector<mutation> muts, validate do_validate) {
    schema_ptr s = muts[0].schema();
    co_await make_sstable_containing(sst, co_await make_memtable(s, muts));

    if (do_validate) {
        reader_concurrency_semaphore sem(
            reader_concurrency_semaphore::no_limits{}, "make_sstable_containing", reader_concurrency_semaphore::register_metrics::no);

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
            co_await coroutine::maybe_yield();
        }

        // validate the sstable
        auto rd = sst->as_mutation_source().make_mutation_reader(s, sem.make_tracking_only_permit(nullptr, "test", db::no_timeout, {}));
        for (auto&& m : merged) {
            auto mo = co_await read_mutation_from_mutation_reader(rd);
            BOOST_REQUIRE(mo);
            assert_that(*mo).is_equal_to_compacted(m);
            co_await coroutine::maybe_yield();
        }
        co_await rd.close();
        co_await sem.stop();
    }
    co_return sst;
}

future<sstables::shared_sstable> make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, utils::chunked_vector<mutation> muts, validate do_validate) {
    return make_sstable_containing(sst_factory(), std::move(muts), do_validate);
}

shared_sstable make_sstable_easy(test_env& env, mutation_reader rd, sstable_writer_config cfg,
        sstables::generation_type gen, const sstables::sstable::version_types version, int expected_partition, db_clock::time_point query_time) {
    auto s = rd.schema();
    auto sst = env.make_sstable(s, gen, version, sstable_format_types::big, default_sstable_buffer_size, query_time);
    sst->write_components(std::move(rd), expected_partition, s, cfg, encoding_stats{}).get();
    sst->load(s->get_sharder()).get();
    return sst;
}

shared_sstable make_sstable_easy(test_env& env, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        sstables::generation_type gen, const sstable::version_types v, int estimated_partitions, db_clock::time_point query_time) {
    return make_sstable_easy(env, mt->make_mutation_reader(mt->schema(), env.make_reader_permit()), std::move(cfg), gen, v, estimated_partitions, query_time);
}

future<compaction::compaction_result> compact_sstables(test_env& env, compaction::compaction_descriptor descriptor, table_for_tests t,
                 std::function<shared_sstable()> creator, compaction::compaction_sstable_replacer_fn replacer, can_purge_tombstones can_purge) {
    auto& table_s = t.as_compaction_group_view();
    descriptor.creator = [creator = std::move(creator)] (shard_id dummy) mutable {
        return creator();
    };
    descriptor.replacer = std::move(replacer);
    if (can_purge) {
        descriptor.enable_garbage_collection(*co_await table_s.main_sstable_set());
    }
    compaction::compaction_result ret;
    co_await run_compaction_task(env, descriptor.run_identifier, table_s, [&] (compaction::compaction_data& cdata) {
        return do_with(compaction::compaction_progress_monitor{}, [&] (compaction::compaction_progress_monitor& progress_monitor) {
                return ::compaction::compact_sstables(std::move(descriptor), cdata, table_s, progress_monitor).then([&] (compaction::compaction_result res) {
                ret = std::move(res);
            });
        });
    });
    co_return ret;
}

class compaction_manager_test_task : public compaction::compaction_task_executor {
    sstables::run_id _run_id;
    noncopyable_function<future<> (compaction::compaction_data&)> _job;
    gate::holder _hold;

public:
    compaction_manager_test_task(compaction::compaction_manager& cm, compaction::compaction_group_view& table_s, sstables::run_id run_id, noncopyable_function<future<> (compaction::compaction_data&)> job)
        : compaction::compaction_task_executor(cm, compaction::throw_if_stopping::no, &table_s, compaction::compaction_type::Compaction, "Test compaction")
        , _run_id(run_id)
        , _job(std::move(job))
        , _hold(_compaction_state.gate.hold())
    { }

protected:
    virtual future<compaction::compaction_manager::compaction_stats_opt> do_run() override {
        setup_new_compaction(_run_id);
        return _job(_compaction_data).then([] {
            return make_ready_future<compaction::compaction_manager::compaction_stats_opt>(std::nullopt);
        });
    }
};

future<> run_compaction_task(test_env& env, sstables::run_id output_run_id, compaction::compaction_group_view& table_s, noncopyable_function<future<> (compaction::compaction_data&)> job) {
    auto& tcm = env.test_compaction_manager();
    auto task = make_shared<compaction_manager_test_task>(tcm.get_compaction_manager(), table_s, output_run_id, std::move(job));
    co_await tcm.perform_compaction(std::move(task));
}

future<sstables::shared_sstable> verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, bytes key, std::function<void(mutation_opt&)> verify) {
    auto sstp = co_await make_sstable_containing(std::move(sst), mt);
    co_return co_await verify_mutation(env, std::move(sstp), std::move(key), std::move(verify));
}

future<sstables::shared_sstable> verify_mutation(test_env& env, shared_sstable sstp, bytes key, std::function<void(mutation_opt&)> verify) {
    auto s = sstp->get_schema();
    auto pr = dht::partition_range::make_singular(make_dkey(s, key));
    auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
    auto mopt = co_await read_mutation_from_mutation_reader(rd);
    verify(mopt);
    co_await rd.close();
    co_return sstp;
}

future<sstables::shared_sstable> verify_mutation(test_env& env, shared_sstable sst, lw_shared_ptr<replica::memtable> mt, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify) {
    auto sstp = co_await make_sstable_containing(std::move(sst), mt);
    co_return co_await verify_mutation(env, std::move(sstp), std::move(pr), std::move(verify));
}

future<sstables::shared_sstable> verify_mutation(test_env& env, shared_sstable sstp, dht::partition_range pr, std::function<stop_iteration(mutation_opt&)> verify) {
    auto s = sstp->get_schema();
    auto rd = sstp->make_reader(s, env.make_reader_permit(), std::move(pr), s->full_slice());
    while (auto mopt = co_await read_mutation_from_mutation_reader(rd)) {
        if (verify(mopt) == stop_iteration::yes) {
            break;
        }
    }
    co_await rd.close();
    co_return sstp;
}

class corrupted_data_source_impl : public data_source_impl {
    input_stream<char> _wrapped;
    size_t _corrupted_byte;
    size_t _read_bytes;

    void maybe_corrupt(temporary_buffer<char>& buf) {
        if (_read_bytes <= _corrupted_byte && _corrupted_byte < _read_bytes + buf.size()) {
            buf.get_write()[_corrupted_byte - _read_bytes] ^= 1u;
        }
    }
public:
    corrupted_data_source_impl(input_stream<char> wrapped, size_t corrupted_byte)
        : _wrapped(std::move(wrapped))
        , _corrupted_byte(corrupted_byte)
        , _read_bytes(0)
    {}

    future<seastar::temporary_buffer<char>> get() override {
        auto inner = co_await _wrapped.read();
        maybe_corrupt(inner);
        _read_bytes += inner.size();

        co_return inner;
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        co_await _wrapped.skip(n);
        _read_bytes += n;

        co_return temporary_buffer<char>();
    }

    future<> close() override {
        return _wrapped.close();
    }
};

class corrupted_data_source : public data_source {
public:
    corrupted_data_source(input_stream<char> wrapped, size_t corrupted_byte)
        : data_source(std::make_unique<corrupted_data_source_impl>(std::move(wrapped), corrupted_byte))
    {}
};

class corrupted_sstable_stream_source_impl : public sstable_stream_source {
    std::unique_ptr<sstable_stream_source> _wrapped;
    size_t _corrupted_byte;
public:
    corrupted_sstable_stream_source_impl(std::unique_ptr<sstable_stream_source> wrapped, sstables::shared_sstable sst, component_type type, size_t corrupted_byte)
        : sstable_stream_source(std::move(sst), type)
        , _wrapped(std::move(wrapped))
        , _corrupted_byte(corrupted_byte)
    {}

    future<input_stream<char>> input(const file_input_stream_options& opts) const {
        auto inner = co_await _wrapped->input(opts);
        co_return input_stream<char>(corrupted_data_source(std::move(inner), _corrupted_byte));
    }
};

std::unique_ptr<sstable_stream_source> make_corrupted_sstable_stream_source(std::unique_ptr<sstable_stream_source> wrapped, sstables::shared_sstable sst, component_type type, size_t corrupted_byte) {
    return std::make_unique<corrupted_sstable_stream_source_impl>(std::move(wrapped), std::move(sst), type, corrupted_byte);
}