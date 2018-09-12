/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
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

#include <boost/range/adaptors.hpp>

#include <seastar/core/sleep.hh>
#include <seastar/tests/perf/perf_tests.hh>

#include "tests/simple_schema.hh"

#include "mutation_reader.hh"
#include "flat_mutation_reader.hh"
#include "memtable.hh"

namespace tests {

class combined {
    mutable simple_schema _schema;
    std::vector<mutation> _one_row;
    std::vector<mutation> _single;
    std::vector<std::vector<mutation>> _disjoint_interleaved;
    std::vector<std::vector<mutation>> _disjoint_ranges;
private:
    static std::vector<mutation> create_one_row(simple_schema&);
    static std::vector<mutation> create_single_stream(simple_schema&);
    static std::vector<std::vector<mutation>> create_disjoint_interleaved_streams(simple_schema&);
    static std::vector<std::vector<mutation>> create_disjoint_ranges_streams(simple_schema&);
protected:
    simple_schema& schema() const { return _schema; }
    const std::vector<mutation>& one_row_stream() const { return _one_row; }
    const std::vector<mutation>& single_stream() const { return _single; }
    const std::vector<std::vector<mutation>>& disjoint_interleaved_streams() const {
        return _disjoint_interleaved;
    }
    const std::vector<std::vector<mutation>>& disjoint_ranges_streams() const {
        return _disjoint_ranges;
    }
    future<> consume_all(flat_mutation_reader mr) const;
public:
    combined()
        : _one_row(create_one_row(_schema))
        , _single(create_single_stream(_schema))
        , _disjoint_interleaved(create_disjoint_interleaved_streams(_schema))
        , _disjoint_ranges(create_disjoint_ranges_streams(_schema))
    { }
};

std::vector<mutation> combined::create_one_row(simple_schema& s)
{
    return boost::copy_range<std::vector<mutation>>(
        s.make_pkeys(1)
        | boost::adaptors::transformed([&] (auto& dkey) {
            auto m = mutation(s.schema(), dkey);
            m.apply(s.make_row(s.make_ckey(0), "value"));
            return m;
        })
    );
}

std::vector<mutation> combined::create_single_stream(simple_schema& s)
{
    return boost::copy_range<std::vector<mutation>>(
        s.make_pkeys(32)
        | boost::adaptors::transformed([&] (auto& dkey) {
            auto m = mutation(s.schema(), dkey);
            for (auto i = 0; i < 16; i++) {
                m.apply(s.make_row(s.make_ckey(i), "value"));
            }
            return m;
        })
    );
}

std::vector<std::vector<mutation>> combined::create_disjoint_interleaved_streams(simple_schema& s)
{
    auto base = create_single_stream(s);
    std::vector<std::vector<mutation>> mss;
    for (auto i = 0; i < 4; i++) {
        mss.emplace_back(boost::copy_range<std::vector<mutation>>(
            base
            | boost::adaptors::sliced(i, base.size())
            | boost::adaptors::strided(4)
        ));
    }
    return mss;
}

std::vector<std::vector<mutation>> combined::create_disjoint_ranges_streams(simple_schema& s)
{
    auto base = create_single_stream(s);
    std::vector<std::vector<mutation>> mss;
    auto slice = base.size() / 4;
    for (auto i = 0; i < 4; i++) {
        mss.emplace_back(boost::copy_range<std::vector<mutation>>(
            base
            | boost::adaptors::sliced(i * slice, std::min((i + 1) * slice, base.size()))
        ));
    }
    return mss;
}

future<> combined::consume_all(flat_mutation_reader mr) const
{
    return do_with(std::move(mr), [] (auto& mr) {
        perf_tests::start_measuring_time();
        return mr.consume_pausable([] (mutation_fragment mf) {
            perf_tests::do_not_optimize(mf);
            return stop_iteration::no;
        }, db::no_timeout).then([] {
            perf_tests::stop_measuring_time();
        });
    });
}

PERF_TEST_F(combined, one_row)
{
    std::vector<flat_mutation_reader> mrs;
    mrs.emplace_back(flat_mutation_reader_from_mutations(one_row_stream()));
    return consume_all(make_combined_reader(schema().schema(), std::move(mrs)));
}

PERF_TEST_F(combined, single_active)
{
    std::vector<flat_mutation_reader> mrs;
    mrs.reserve(4);
    mrs.emplace_back(flat_mutation_reader_from_mutations(single_stream()));
    for (auto i = 0; i < 3; i++) {
        mrs.emplace_back(make_empty_flat_reader(schema().schema()));
    }
    return consume_all(make_combined_reader(schema().schema(), std::move(mrs)));
}

PERF_TEST_F(combined, many_overlapping)
{
    std::vector<flat_mutation_reader> mrs;
    mrs.reserve(4);
    for (auto i = 0; i < 4; i++) {
        mrs.emplace_back(flat_mutation_reader_from_mutations(single_stream()));
    }
    return consume_all(make_combined_reader(schema().schema(), std::move(mrs)));
}

PERF_TEST_F(combined, disjoint_interleaved)
{
    return consume_all(make_combined_reader(schema().schema(),
        boost::copy_range<std::vector<flat_mutation_reader>>(
            disjoint_interleaved_streams()
            | boost::adaptors::transformed([] (auto&& ms) {
                return flat_mutation_reader_from_mutations(std::move(ms));
            })
        )
    ));
}

PERF_TEST_F(combined, disjoint_ranges)
{
    return consume_all(make_combined_reader(schema().schema(),
        boost::copy_range<std::vector<flat_mutation_reader>>(
            disjoint_ranges_streams()
            | boost::adaptors::transformed([] (auto&& ms) {
                return flat_mutation_reader_from_mutations(std::move(ms));
            })
        )
    ));
}

class memtable {
    static constexpr size_t partition_count = 1000;
    static constexpr size_t row_count = 50;
    mutable simple_schema _schema;
    std::vector<dht::decorated_key> _dkeys;
    lw_shared_ptr<::memtable> _single_row;
    lw_shared_ptr<::memtable> _multi_row;
    std::optional<dht::partition_range> _partition_range;
public:
    memtable()
        : _dkeys(_schema.make_pkeys(partition_count))
        , _single_row(make_lw_shared<::memtable>(_schema.schema()))
        , _multi_row(make_lw_shared<::memtable>(_schema.schema()))
    {
        boost::for_each(
            _dkeys
            | boost::adaptors::transformed([&] (auto& dkey) {
                auto m = mutation(_schema.schema(), dkey);
                m.apply(_schema.make_row(_schema.make_ckey(0), "value"));
                return m;
            }),
            [&] (mutation m) {
                _single_row->apply(m);
            }
        );
        boost::for_each(
            _dkeys
            | boost::adaptors::transformed([&] (auto& dkey) {
                auto m = mutation(_schema.schema(), dkey);
                for (auto i = 0u; i < row_count; i++) {
                    m.apply(_schema.make_row(_schema.make_ckey(i), "value"));
                }
                return m;
            }),
            [&] (mutation m) {
                _multi_row->apply(m);
            }
        );
    }
protected:
    schema_ptr schema() const { return _schema.schema(); }

    ::memtable& single_row_mt() { return *_single_row; }
    ::memtable& multi_row_mt() { return *_multi_row; }

    const dht::partition_range& single_partition_range() {
        auto& dk = _dkeys[_dkeys.size() / 2];
        _partition_range.emplace(dht::partition_range::make_singular(dk));
        return *_partition_range;
    }

    const dht::partition_range& multi_partition_range(size_t n) {
        auto start_idx = (_dkeys.size() - n) / 2;
        auto& start_dk = _dkeys[start_idx];
        auto& end_dk = _dkeys[start_idx + n];
        _partition_range.emplace(dht::partition_range::make(dht::ring_position(start_dk),
                                                            {dht::ring_position(end_dk), false}));
        return *_partition_range;
    }

    future<> consume_all(flat_mutation_reader mr) const {
        return do_with(std::move(mr), [] (auto& mr) {
            return mr.consume_pausable([] (mutation_fragment mf) {
                perf_tests::do_not_optimize(mf);
                return stop_iteration::no;
            }, db::no_timeout);
        });
    }
};

PERF_TEST_F(memtable, one_partition_one_row)
{
    return consume_all(single_row_mt().make_flat_reader(schema(), single_partition_range()));
}

PERF_TEST_F(memtable, one_partition_many_rows)
{
    return consume_all(multi_row_mt().make_flat_reader(schema(), single_partition_range()));
}

PERF_TEST_F(memtable, many_partitions_one_row)
{
    return consume_all(single_row_mt().make_flat_reader(schema(), multi_partition_range(25)));
}

PERF_TEST_F(memtable, many_partitions_many_rows)
{
    return consume_all(multi_row_mt().make_flat_reader(schema(), multi_partition_range(25)));
}

}
