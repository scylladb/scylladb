/*
 * Copyright 2015 Cloudius Systems
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
#include "../sstable_test.hh"
#include "sstables/sstables.hh"
#include "mutation_reader.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics.hpp>
#include <boost/range/irange.hpp>

using namespace sstables;

class test_env {
public:
    struct conf {
        unsigned partitions;
        unsigned key_size;
        unsigned num_columns;
        unsigned column_size;
        size_t buffer_size;
        sstring dir;
    };

private:
    sstring dir() {
        return _cfg.dir + "/" + to_sstring(engine().cpu_id());
    }

    sstring random_string(unsigned size) {
        sstring str(sstring::initialized_later{}, size_t(size));
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
    std::vector<lw_shared_ptr<sstable>> _sst;

    schema_ptr create_schema() {
        std::vector<schema::column> columns;

        for (unsigned i = 0; i < _cfg.num_columns; ++i) {
            columns.push_back(schema::column{ to_bytes(sprint("column%04d", i)), utf8_type });
        }

        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", "perf-test"), "ks", "perf-test",
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
        )));
        return builder.build(schema_builder::compact_storage::no);
    }

public:
    test_env(conf cfg) : _cfg(std::move(cfg))
           , s(create_schema())
           , _distribution('@', '~')
           , _mt(make_lw_shared<memtable>(s))
    {}

    future<> stop() { return make_ready_future<>(); }

    void fill_memtable() {
        for (unsigned i = 0; i < _cfg.partitions; i++) {
            auto key = partition_key::from_deeply_exploded(*s, { random_key() });
            auto mut = mutation(key, s);
            for (auto& cdef: s->regular_columns()) {
                mut.set_clustered_cell(clustering_key::make_empty(*s), cdef, atomic_cell::make_live(0, utf8_type->decompose(random_column())));
            }
            _mt->apply(std::move(mut));
        }
    }

    future<> load_sstables(unsigned iterations) {
        _sst.push_back(make_lw_shared<sstable>("ks", "cf", this->dir(), 0, sstable::version_types::ka, sstable::format_types::big));
        return _sst.back()->load();
    }

    using clk = std::chrono::steady_clock;
    static auto now() {
        return clk::now();
    }


    // Mappers below
    future<double> flush_memtable(int idx) {
        auto start = test_env::now();
        size_t partitions = _mt->partition_count();
        return test_setup::create_empty_test_dir(dir()).then([this, idx] {
            auto sst = sstables::test::make_test_sstable(_cfg.buffer_size, "ks", "cf", dir(), idx, sstable::version_types::ka, sstable::format_types::big);
            return sst->write_components(*_mt).then([sst] {});
        }).then([start, partitions] {
            auto end = test_env::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            return partitions / duration;
        });
    }

    future<double> read_all_indexes(int idx) {
        return do_with(test(_sst[0]), [] (auto& sst) {
            auto start = test_env::now();
            auto total = make_lw_shared<size_t>(0);
            auto& summary = sst.get_summary();
            auto idx = boost::irange(0, int(summary.header.size));

            return do_for_each(idx.begin(), idx.end(), [&sst, total] (uint64_t entry) {
                return sst.read_indexes(entry).then([total] (auto il) {
                    *total += il.size();
                });
            }).then([total, start] {
                auto end = test_env::now();
                auto duration = std::chrono::duration<double>(end - start).count();
                return *total / duration;
            });
        });
    }

    future<double> read_sequential_partitions(int idx) {
        return do_with(_sst[0]->read_rows(s), [this] (sstables::mutation_reader& r) {
            auto start = test_env::now();
            auto total = make_lw_shared<size_t>(0);
            auto done = make_lw_shared<bool>(false);
            return do_until([done] { return *done; }, [this, done, total, &r] {
                return r.read().then([this, done, total] (mutation_opt m) {
                    if (!m) {
                        *done = true;
                    } else {
                        auto row = m->partition().find_row(clustering_key::make_empty(*s));
                        if (!row || row->size() != _cfg.num_columns) {
                            throw std::invalid_argument("Invalid sstable found. Maybe you ran write mode with different num_columns settings?");
                        } else {
                            (*total)++;
                        }
                    }
                });
            }).then([total, start] {
                auto end = test_env::now();
                auto duration = std::chrono::duration<double>(end - start).count();
                return *total / duration;
            });
        });
    }
};

// The function func should carry on with the test, and return the number of partitions processed.
// time_runs will then map reduce it, and return the aggregate partitions / sec for the whole system.
template <typename Func>
future<> time_runs(unsigned iterations, unsigned parallelism, distributed<test_env>& dt, Func func) {
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
        std::cout << sprint("%.2f", mean(*acc)) << " +- " << sprint("%.2f", error_of<tag::mean>(*acc)) << " partitions / sec (" << iterations << " runs, " << parallelism << " concurrent ops)\n";
    });
}
