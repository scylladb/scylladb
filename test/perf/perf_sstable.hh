/*
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once

#include <seastar/util/closeable.hh>

#include "sstables/sstables.hh"
#include "sstables/compaction_manager.hh"
#include "cell_locking.hh"
#include "mutation_reader.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/framework/features.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/error_of_mean.hpp>
#include <boost/range/irange.hpp>

using namespace sstables;

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
    lw_shared_ptr<memtable> _mt;
    std::vector<shared_sstable> _sst;

    schema_ptr create_schema() {
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
        return builder.build(schema_builder::compact_storage::no);
    }

public:
    perf_sstable_test_env(conf cfg) : _cfg(std::move(cfg))
           , s(create_schema())
           , _distribution('@', '~')
           , _mt(make_lw_shared<memtable>(s))
    {}

    future<> stop() {
        _sst.clear();
        return _env.stop();
    }

    future<> fill_memtable() {
        auto idx = boost::irange(0, int(_cfg.partitions / _cfg.sstables));
        auto local_keys = make_local_keys(int(_cfg.partitions / _cfg.sstables), s, _cfg.key_size);
        return do_for_each(idx.begin(), idx.end(), [this, local_keys = std::move(local_keys)] (auto iteration) {
            auto key = partition_key::from_deeply_exploded(*s, { local_keys.at(iteration) });
            auto mut = mutation(this->s, key);
            for (auto& cdef: this->s->regular_columns()) {
                mut.set_clustered_cell(clustering_key::make_empty(), cdef, atomic_cell::make_live(*utf8_type, 0, utf8_type->decompose(this->random_column())));
            }
            this->_mt->apply(std::move(mut));
            return make_ready_future<>();
        });
    }

    future<> load_sstables(unsigned iterations) {
        _sst.push_back(_env.make_sstable(s, this->dir(), 0, sstables::get_highest_sstable_version(), sstable::format_types::big));
        return _sst.back()->load();
    }

    using clk = std::chrono::steady_clock;
    static auto now() {
        return clk::now();
    }


    // Mappers below
    future<double> flush_memtable(int idx) {
        return seastar::async([this, idx] {
            size_t partitions = _mt->partition_count();

            test_setup::create_empty_test_dir(dir()).get();
            auto sst = _env.make_sstable(s, dir(), idx, sstables::get_highest_sstable_version(), sstable::format_types::big, _cfg.buffer_size);

            auto start = perf_sstable_test_env::now();
            write_memtable_to_sstable_for_test(*_mt, sst).get();
            auto end = perf_sstable_test_env::now();

            _mt->revert_flushed_memory();

            auto duration = std::chrono::duration<double>(end - start).count();
            return partitions / duration;
        });
    }

    future<double> compaction(int idx) {
        return test_setup::create_empty_test_dir(dir()).then([this, idx] {
            return sstables::test_env::do_with_async_returning<double>([this, idx] (sstables::test_env& env) {
                auto sst_gen = [this, gen = make_lw_shared<unsigned>(idx)] () mutable {
                    return _env.make_sstable(s, dir(), (*gen)++, sstables::get_highest_sstable_version(), sstable::format_types::big, _cfg.buffer_size);
                };

                std::vector<shared_sstable> ssts;
                for (auto i = 0u; i < _cfg.sstables; i++) {
                    auto sst = sst_gen();
                    write_memtable_to_sstable_for_test(*_mt, sst).get();
                    sst->open_data().get();
                    _mt->revert_flushed_memory();
                    ssts.push_back(std::move(sst));
                }

                cache_tracker tracker;
                cell_locker_stats cl_stats;
                auto cm = make_lw_shared<compaction_manager>();
                auto cf = make_lw_shared<column_family>(s, column_family_test_config(env.manager()), column_family::no_commitlog(), *cm, cl_stats, tracker);

                auto start = perf_sstable_test_env::now();

                auto descriptor = sstables::compaction_descriptor(std::move(ssts), cf->get_sstable_set(), default_priority_class());
                descriptor.creator = [sst_gen = std::move(sst_gen)] (unsigned dummy) mutable {
                    return sst_gen();
                };
                descriptor.replacer = sstables::replacer_fn_no_op();
                auto ret = sstables::compact_sstables(std::move(descriptor), *cf).get0();
                auto end = perf_sstable_test_env::now();

                auto partitions_per_sstable = _cfg.partitions / _cfg.sstables;
                assert(ret.total_keys_written == partitions_per_sstable);

                auto duration = std::chrono::duration<double>(end - start).count();
                return ret.total_keys_written / duration;
            });
        });
    }

    future<double> read_all_indexes(int idx) {
        return do_with(test(_sst[0]), [] (auto& sst) {
            const auto start = perf_sstable_test_env::now();

            return sst.read_indexes().then([start] (const auto& indexes) {
                auto end = perf_sstable_test_env::now();
                auto duration = std::chrono::duration<double>(end - start).count();
                return indexes.size() / duration;
            });
        });
    }

    future<double> read_sequential_partitions(int idx) {
        return with_closeable(_sst[0]->make_reader(s, tests::make_permit(), query::full_partition_range, s->full_slice()), [this] (flat_mutation_reader& r) {
            auto start = perf_sstable_test_env::now();
            auto total = make_lw_shared<size_t>(0);
            auto done = make_lw_shared<bool>(false);
            return do_until([done] { return *done; }, [this, done, total, &r] {
                return read_mutation_from_flat_mutation_reader(r, db::no_timeout).then([this, done, total] (mutation_opt m) {
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
