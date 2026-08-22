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
#include <fmt/ranges.h>

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

// The shard owning the reader's first fragment. Only a heuristic: the rest of
// the stream isn't checked, since consuming a reader ahead of the real write
// isn't possible without fully materializing it first.
static future<std::optional<shard_id>> shard_of_first_fragment(mutation_reader& rd) {
    auto* mf = co_await rd.peek();
    if (!mf || !mf->is_partition_start()) {
        co_return std::nullopt;
    }
    co_return rd.schema()->get_sharder().shard_of(mf->as_partition_start().key().token());
}

// The set of shards owning at least one of the given mutations' tokens.
static std::set<shard_id> shards_for_mutations(const schema& s, const utils::chunked_vector<mutation>& muts) {
    std::set<shard_id> shards;
    for (auto& m : muts) {
        shards.insert(s.get_sharder().shard_of(m.token()));
    }
    return shards;
}

// detect_shard: best-effort-attribute the write to the shard owning the data when
// cfg.shard is still the default. Skip when the caller (the mutations-vector
// overload below) has already resolved cfg.shard with certainty.
static future<sstables::shared_sstable> do_make_sstable_containing(sstables::shared_sstable sst, lw_shared_ptr<replica::memtable> mt,
        sstable_writer_config cfg, bool detect_shard = true) {
    reader_concurrency_semaphore sem(
        reader_concurrency_semaphore::no_limits{}, "make_sstable_containing", reader_concurrency_semaphore::register_metrics::no);

    std::exception_ptr ex;
    std::optional<mutation_reader> scan_rd;
    std::optional<mutation_reader> reader;
    try {
        if (detect_shard && cfg.shard == this_shard_id()) {
            // Non-destructive pre-scan: mt->make_mutation_reader() (unlike
            // make_flush_reader()) can be constructed and consumed without
            // disturbing the real flush pass below. Not a hard failure on
            // ambiguity here: some callers (e.g. sstable_resharding_test)
            // deliberately write memtables spanning every shard and manage
            // sharding metadata themselves.
            auto scan_permit = sem.make_tracking_only_permit(mt->schema(), "shard_scan", db::no_timeout, {});
            scan_rd.emplace(mt->make_mutation_reader(mt->schema(), std::move(scan_permit)));
            if (auto shard = co_await shard_of_first_fragment(*scan_rd)) {
                cfg.shard = *shard;
            }
            co_await scan_rd->close();
            scan_rd.reset();
        }
        auto permit = sem.make_tracking_only_permit(mt->schema(), "mt_to_sst", db::no_timeout, {});
        reader.emplace(mt->make_flush_reader(mt->schema(), std::move(permit)));
        auto rd = std::move(*reader);
        reader.reset();
        co_await sst->write_components(std::move(rd), mt->partition_count(), mt->schema(), cfg, mt->get_encoding_stats());
    } catch (...) {
        ex = std::current_exception();
    }
    if (scan_rd) {
        co_await scan_rd->close();
    }
    if (reader) {
        co_await reader->close();
    }
    co_await sem.stop();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }

    sstable_open_config open_cfg { .load_first_and_last_position_metadata = true };
    co_await sst->open_data(open_cfg);
    co_return sst;
}

future<sstables::shared_sstable> make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, lw_shared_ptr<replica::memtable> mt) {
    return make_sstable_containing(sst_factory(), std::move(mt));
}

future<sstables::shared_sstable> make_sstable_containing(sstables::shared_sstable sst, lw_shared_ptr<replica::memtable> mt) {
    auto cfg = sst->manager().configure_writer("memtable");
    return do_make_sstable_containing(std::move(sst), std::move(mt), std::move(cfg));
}

future<sstables::shared_sstable> make_sstable_containing(sstables::shared_sstable sst, utils::chunked_vector<mutation> muts, validate do_validate, std::optional<shard_id> shard) {
    BOOST_REQUIRE(!muts.empty());
    schema_ptr s = muts[0].schema();

    auto cfg = sst->manager().configure_writer("memtable");
    if (shard) {
        cfg.shard = *shard;
    } else {
        auto shards = shards_for_mutations(*s, muts);
        BOOST_REQUIRE_MESSAGE(shards.size() == 1,
            fmt::format("make_sstable_containing: mutations span {} shards ({}), but a single sstable can only be attributed to one",
                   shards.size(), fmt::join(shards, ",")));
        cfg.shard = *shards.begin();
    }

    auto mt = co_await make_memtable(s, muts);
    sst = co_await do_make_sstable_containing(std::move(sst), std::move(mt), std::move(cfg), /*detect_shard=*/false);

    if (do_validate) {
        reader_concurrency_semaphore sem(
            reader_concurrency_semaphore::no_limits{}, "make_sstable_containing", reader_concurrency_semaphore::register_metrics::no);

        std::exception_ptr ex;
        std::optional<mutation_reader> reader;
        try {
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

            reader.emplace(sst->as_mutation_source().make_mutation_reader(s, sem.make_tracking_only_permit(nullptr, "test", db::no_timeout, {})));
            for (auto&& m : merged) {
                auto mo = co_await read_mutation_from_mutation_reader(*reader);
                BOOST_REQUIRE(mo);
                assert_that(*mo).is_equal_to_compacted(m);
                co_await coroutine::maybe_yield();
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (reader) {
            co_await reader->close();
        }
        co_await sem.stop();
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }
    }
    co_return sst;
}

future<sstables::shared_sstable> make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, utils::chunked_vector<mutation> muts, validate do_validate, std::optional<shard_id> shard) {
    return make_sstable_containing(sst_factory(), std::move(muts), do_validate, shard);
}

shared_sstable make_sstable_easy(test_env& env, mutation_reader rd, sstable_writer_config cfg,
        sstables::generation_type gen, const sstables::sstable::version_types version, int expected_partition, db_clock::time_point query_time) {
    auto s = rd.schema();
    if (cfg.shard == this_shard_id()) {
        if (auto shard = shard_of_first_fragment(rd).get()) {
            cfg.shard = *shard;
        }
    }
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

void slightly_corrupt_sstable(sstables::shared_sstable sst, component_type component) {
    auto path = sstables::test(sst).filename(component).native();
    auto size = seastar::file_size(path).get();
    auto f = open_file_dma(path, open_flags::rw).get();
    auto close_f = deferred_close(f);
    const auto mem_align = f.memory_dma_alignment();
    const auto dma_align = f.disk_write_dma_alignment();
    auto block_offset = align_down(size - 1, dma_align);
    auto buf = seastar::temporary_buffer<char>::aligned(mem_align, dma_align);
    f.dma_read(block_offset, buf.get_write(), dma_align).get();
    if (component == component_type::Scylla && sst->get_component_digest(component_type::Scylla)) {
        // Modify the last bit of the data itself, not the digest.
        buf.get_write()[size - 1 - sizeof(uint32_t) - block_offset] ^= 1u;
    } else {
        // Flip one bit in the last byte of the file to corrupt it minimally.
        // Using a single-bit flip avoids creating values that overflow
        // during parsing.
        buf.get_write()[size - 1 - block_offset] += 1;
    }
    f.dma_write(block_offset, buf.get(), dma_align).get();
    f.truncate(size).get();
}

void corrupt_sstable(sstables::shared_sstable sst, component_type type) {
    auto f = sstables::test(sst).open_file(type, {}, {}).get();
    auto close_f = deferred_close(f);
    const auto wbuf_align = f.memory_dma_alignment();
    const auto wbuf_len = f.size().get();
    auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
    std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
    auto os = output_stream<char>(sstables::test(sst).get_storage().make_component_sink(*sst, type, open_flags::wo, {}).get());
    auto close_os = deferred_close(os);
    os.write(std::move(wbuf)).get();
}