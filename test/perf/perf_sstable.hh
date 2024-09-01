/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/util/closeable.hh>
#include <seastar/core/seastar.hh>

#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/time_window_compaction_strategy.hh"
#include "cell_locking.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"
#include "test/lib/random_utils.hh"
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/framework/features.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/error_of_mean.hpp>
#include <boost/range/irange.hpp>

using namespace sstables;


class test_setup {
    file _f;
    sstring _path;
    future<> _listing_done;

    static sstring& path() {
        static sstring _p = "test/resource/sstables/tests-temporary";
        return _p;
    };

public:
    test_setup(file f, sstring path)
            : _f(std::move(f))
            , _path(path)
            , _listing_done(_f.list_directory([this] (directory_entry de) { return remove(de); }).done()) {
    }
    ~test_setup() {
        // FIXME: discarded future.
        (void)_f.close().finally([save = _f] {});
    }
private:
    future<> remove(directory_entry de) {
        sstring t = _path + "/" + de.name;
        return file_type(t).then([t] (std::optional<directory_entry_type> det) {
            auto f = make_ready_future<>();

            if (!det) {
                throw std::runtime_error("Can't determine file type\n");
            } else if (det == directory_entry_type::directory) {
                f = empty_test_dir(t);
            }
            return f.then([t] {
                return remove_file(t);
            });
        });
    }
    future<> done() { return std::move(_listing_done); }

    static future<> empty_test_dir(sstring p = path()) {
        return open_directory(p).then([p] (file f) {
            auto l = make_lw_shared<test_setup>(std::move(f), p);
            return l->done().then([l] { });
        });
    }
public:
    static future<> create_empty_test_dir(sstring p = path()) {
        return make_directory(p).then_wrapped([p] (future<> f) {
            try {
                f.get();
            // it's fine if the directory exists, just shut down the exceptional future message
            } catch (std::exception& e) {}
            return empty_test_dir(p);
        });
    }
};

class perf_sstable_test_env {
    test_env _env;

public:
    struct conf {
        unsigned partitions;
        unsigned key_size;
        unsigned num_columns;
        unsigned column_size;
        unsigned sstables;
        size_t buffer_size;
        sstring dir;
        sstables::compaction_strategy_type compaction_strategy;
        api::timestamp_type timestamp_range;
    };

private:
    sstring dir() {
        return _cfg.dir + "/" + to_sstring(this_shard_id());
    }

    sstring random_string(unsigned size) {
        sstring str = uninitialized_string(size_t(size));
        for (auto& b: str) {
            b = _distribution(_generator);
        }
        return str;
    }

    sstring random_key() {
        return random_string(_cfg.key_size);
    }

    sstring random_column() {
        return random_string(_cfg.column_size);
    }

    conf _cfg;
    schema_ptr s;
    std::default_random_engine _generator;
    std::uniform_int_distribution<char> _distribution;
    lw_shared_ptr<replica::memtable> _mt;
    std::vector<shared_sstable> _sst;

    schema_ptr create_schema(sstables::compaction_strategy_type type) {
        std::vector<schema::column> columns;

        for (unsigned i = 0; i < _cfg.num_columns; ++i) {
            columns.push_back(schema::column{ to_bytes(format("column{:04d}", i)), utf8_type });
        }

        schema_builder builder(make_shared_schema(generate_legacy_id("ks", "perf-test"), "ks", "perf-test",
            // partition key
            {{"name", utf8_type}},
            // clustering key
            {},
            // regular columns
            { columns },
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "Perf tests"
        ));
        builder.set_compaction_strategy(type);
        return builder.build(schema_builder::compact_storage::no);
    }

    using clk = std::chrono::steady_clock;
    static auto now() {
        return clk::now();
    }

    enum class sst_reader {
        crawling,
        partitioned,
    };

    future<double> do_streaming(sst_reader reader_type) {
        const auto start = perf_sstable_test_env::now();

        // mimic the behavior of sstable_streamer::stream_sstable_mutations()
        auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, false));
        // stream all previously loaded sstables
        for (auto& sst : _sst) {
            sst_set->insert(sst);
        }
        // do not compact when performing streaming, as we focus on the read
        // performance
        auto reader = mutation_reader{nullptr};
        if (reader_type == sst_reader::crawling) {
            reader = sst_set->make_crawling_reader(s,
                                                   _env.make_reader_permit(),
                                                   tracing::trace_state_ptr{},
                                                   default_read_monitor_generator());
        } else {
            const auto full_partition_range = dht::partition_range::make_open_ended_both_sides();
            auto& slice = s->full_slice();
            reader = sst_set->make_range_sstable_reader(s,
                                                        _env.make_reader_permit(),
                                                        full_partition_range,
                                                        slice,
                                                        tracing::trace_state_ptr{},
                                                        streamed_mutation::forwarding::no,
                                                        mutation_reader::forwarding::no);
        }
        auto frag_stream = mutation_fragment_v1_stream{std::move(reader)};
        size_t num_partitions_processed = 0;
        while (auto frag = co_await frag_stream()) {
            if (frag->is_partition_start()) {
                ++num_partitions_processed;
            }
            auto frozen_frag = freeze(*s, *frag);
        }

        const auto end = perf_sstable_test_env::now();
        const auto duration = std::chrono::duration<double>(end - start).count();
        co_return num_partitions_processed / duration;
    }

public:
    perf_sstable_test_env(conf cfg) : _cfg(std::move(cfg))
           , s(create_schema(cfg.compaction_strategy))
           , _distribution('@', '~')
           , _mt(make_lw_shared<replica::memtable>(s))
    {}

    future<> stop() {
        _sst.clear();
        return _env.stop();
    }

    future<> fill_memtable() {
        auto idx = boost::irange(0, int(_cfg.partitions / _cfg.sstables));
        auto local_keys = tests::generate_partition_keys(int(_cfg.partitions / _cfg.sstables), s, local_shard_only::yes, tests::key_size{_cfg.key_size, _cfg.key_size});
        return do_for_each(idx.begin(), idx.end(), [this, local_keys = std::move(local_keys)] (auto iteration) {
            auto mut = mutation(this->s, local_keys.at(iteration));
            for (auto& cdef: this->s->regular_columns()) {
                const auto ts = _cfg.timestamp_range ? tests::random::get_int<api::timestamp_type>(-_cfg.timestamp_range, _cfg.timestamp_range) : 0;
                mut.set_clustered_cell(clustering_key::make_empty(), cdef, atomic_cell::make_live(*utf8_type, ts, utf8_type->decompose(this->random_column())));
            }
            this->_mt->apply(std::move(mut));
            return make_ready_future<>();
        });
    }

    future<> load_sstables(unsigned iterations) {
        std::filesystem::path sst_dir_path{this->dir()};
        auto dir = co_await open_directory(sst_dir_path.native());
        auto h = dir.list_directory([&](directory_entry de) -> future<> {
            std::filesystem::path sst_path = sst_dir_path / de.name.begin();
            auto entry = parse_path(sst_path, s->ks_name(), s->cf_name());
            if (entry.component == component_type::TOC) {
                auto sst = _env.make_sstable(s, this->dir(), entry.generation, entry.version);
                co_await sst->load(s->get_sharder());
                _sst.push_back(sst);
            }
        });
        co_await h.done();
    }

    // Mappers below
    future<double> flush_memtable(int idx) {
        return seastar::async([this, idx] {
            size_t partitions = _mt->partition_count();

            test_setup::create_empty_test_dir(dir()).get();
            auto sst = _env.make_sstable(s, dir(), sstables::generation_type(idx), sstables::get_highest_sstable_version(), sstable::format_types::big, _cfg.buffer_size);

            auto start = perf_sstable_test_env::now();
            write_memtable_to_sstable(*_mt, sst).get();
            auto end = perf_sstable_test_env::now();

            _mt->revert_flushed_memory();

            auto duration = std::chrono::duration<double>(end - start).count();
            return partitions / duration;
        });
    }

    future<double> compaction(int idx) {
        return test_setup::create_empty_test_dir(dir()).then([this] {
            return sstables::test_env::do_with_async_returning<double>([this] (sstables::test_env& env) {
                auto sst_gen = [this] () mutable {
                    return _env.make_sstable(s, dir(), _env.new_generation(), sstables::get_highest_sstable_version(), sstable::format_types::big, _cfg.buffer_size);
                };

                std::vector<shared_sstable> ssts;
                for (auto i = 0u; i < _cfg.sstables; i++) {
                    auto sst = sst_gen();
                    write_memtable_to_sstable(*_mt, sst).get();
                    sst->open_data().get();
                    _mt->revert_flushed_memory();
                    ssts.push_back(std::move(sst));
                }

                cache_tracker tracker;
                cell_locker_stats cl_stats;
                tasks::task_manager tm;
                auto cm = make_lw_shared<compaction_manager>(tm, compaction_manager::for_testing_tag{});
                auto cf = make_lw_shared<replica::column_family>(s, env.make_table_config(), make_lw_shared<replica::storage_options>(), *cm, env.manager(), cl_stats, tracker, nullptr);

                auto start = perf_sstable_test_env::now();

                auto descriptor = sstables::compaction_descriptor(std::move(ssts));
                descriptor.enable_garbage_collection(cf->get_sstable_set());
                descriptor.creator = [sst_gen = std::move(sst_gen)] (unsigned dummy) mutable {
                    return sst_gen();
                };
                descriptor.replacer = sstables::replacer_fn_no_op();
                auto cdata = compaction_manager::create_compaction_data();
                compaction_progress_monitor progress_monitor;
                auto ret = sstables::compact_sstables(std::move(descriptor), cdata, cf->try_get_table_state_with_static_sharding(), progress_monitor).get();
                auto end = perf_sstable_test_env::now();

                auto partitions_per_sstable = _cfg.partitions / _cfg.sstables;
                if (_cfg.compaction_strategy != sstables::compaction_strategy_type::time_window) {
                    SCYLLA_ASSERT(ret.new_sstables.size() == 1);
                }
                auto total_keys_written = std::accumulate(ret.new_sstables.begin(), ret.new_sstables.end(), uint64_t(0), [] (uint64_t n, const sstables::shared_sstable& sst) {
                    return n + sst->get_estimated_key_count();
                });
                SCYLLA_ASSERT(total_keys_written >= partitions_per_sstable);

                auto duration = std::chrono::duration<double>(end - start).count();
                return total_keys_written / duration;
            });
        });
    }

    future<double> read_all_indexes(int idx) {
        return do_with(test(_sst[0]), [this] (auto& sst) {
            const auto start = perf_sstable_test_env::now();

            return sst.read_indexes(_env.make_reader_permit()).then([start] (const auto& indexes) {
                auto end = perf_sstable_test_env::now();
                auto duration = std::chrono::duration<double>(end - start).count();
                return indexes.size() / duration;
            });
        });
    }

    future<double> read_sequential_partitions(int idx) {
        return with_closeable(_sst[0]->make_reader(s, _env.make_reader_permit(), query::full_partition_range, s->full_slice()), [this] (auto& r) {
            auto start = perf_sstable_test_env::now();
            auto total = make_lw_shared<size_t>(0);
            auto done = make_lw_shared<bool>(false);
            return do_until([done] { return *done; }, [this, done, total, &r] {
                return read_mutation_from_mutation_reader(r).then([this, done, total] (mutation_opt m) {
                    if (!m) {
                        *done = true;
                    } else {
                        auto row = m->partition().find_row(*s, clustering_key::make_empty());
                        if (!row || row->size() != _cfg.num_columns) {
                            throw std::invalid_argument("Invalid sstable found. Maybe you ran write mode with different num_columns settings?");
                        } else {
                            (*total)++;
                        }
                    }
                });
            }).then([total, start] {
                auto end = perf_sstable_test_env::now();
                auto duration = std::chrono::duration<double>(end - start).count();
                return *total / duration;
            });
        });
    }

    future<double> crawling_streaming(int idx) {
        return do_streaming(sst_reader::crawling);
    }

    future<double> partitioned_streaming(int idx) {
        return do_streaming(sst_reader::partitioned);
    }
};

// The function func should carry on with the test, and return the number of partitions processed.
// time_runs will then map reduce it, and return the aggregate partitions / sec for the whole system.
template <typename Func>
future<> time_runs(unsigned iterations, unsigned parallelism, distributed<perf_sstable_test_env>& dt, Func func) {
    using namespace boost::accumulators;
    auto acc = make_lw_shared<accumulator_set<double, features<tag::mean, tag::error_of<tag::mean>>>>();
    auto idx = boost::irange(0, int(iterations));
    return do_for_each(idx.begin(), idx.end(), [parallelism, acc, &dt, func] (auto iter) {
        auto idx = boost::irange(0, int(parallelism));
        return parallel_for_each(idx.begin(), idx.end(), [&dt, func, acc] (auto idx) {
            return dt.map_reduce(adder<double>(), func, std::move(idx)).then([acc] (double result) {
                auto& a = *acc;
                a(result);
                return make_ready_future<>();
            });
        });
    }).then([acc, iterations, parallelism] {
        std::cout << format("{:.2f}", mean(*acc)) << " +- " << format("{:.2f}", error_of<tag::mean>(*acc)) << " partitions / sec (" << iterations << " runs, " << parallelism << " concurrent ops)\n";
    });
}
