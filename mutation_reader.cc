/*
 * Copyright (C) 2015 ScyllaDB
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

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/move/iterator.hpp>

#include "mutation_reader.hh"
#include "core/future-util.hh"
#include "utils/move.hh"
#include "stdx.hh"

template<typename T>
T move_and_clear(T& obj) {
    T x = std::move(obj);
    obj = T();
    return x;
}

future<> combined_mutation_reader::prepare_next() {
    return parallel_for_each(_next, [this] (mutation_reader* mr) {
        return (*mr)().then([this, mr] (streamed_mutation_opt next) {
            if (next) {
                _ptables.emplace_back(mutation_and_reader { std::move(*next), mr });
                boost::range::push_heap(_ptables, &heap_compare);
            }
        });
    }).then([this] {
        _next.clear();
    });
}

future<streamed_mutation_opt> combined_mutation_reader::next() {
    if (_current.empty() && !_next.empty()) {
        return prepare_next().then([this] { return next(); });
    }
    if (_ptables.empty()) {
        return make_ready_future<streamed_mutation_opt>();
    };

    while (!_ptables.empty()) {
        boost::range::pop_heap(_ptables, &heap_compare);
        auto& candidate = _ptables.back();
        streamed_mutation& m = candidate.m;

        if (!_current.empty() && !_current.back().decorated_key().equal(*m.schema(), m.decorated_key())) {
            // key has changed, so emit accumulated mutation
            boost::range::push_heap(_ptables, &heap_compare);
            return make_ready_future<streamed_mutation_opt>(merge_mutations(move_and_clear(_current)));
        }

        _current.emplace_back(std::move(m));
        _next.emplace_back(candidate.read);
        _ptables.pop_back();
    }
    return make_ready_future<streamed_mutation_opt>(merge_mutations(move_and_clear(_current)));
}

void combined_mutation_reader::init_mutation_reader_set(std::vector<mutation_reader*> readers)
{
    _all_readers = std::move(readers);
    _next.assign(_all_readers.begin(), _all_readers.end());
    _ptables.reserve(_all_readers.size());
}

future<> combined_mutation_reader::fast_forward_to(std::vector<mutation_reader*> to_add, std::vector<mutation_reader*> to_remove, const dht::partition_range& pr)
{
    _ptables.clear();

    std::vector<mutation_reader*> new_readers;
    boost::range::sort(_all_readers);
    boost::range::sort(to_remove);
    boost::range::set_difference(_all_readers, to_remove, std::back_inserter(new_readers));
    _all_readers = std::move(new_readers);
    return parallel_for_each(_all_readers, [this, &pr] (mutation_reader* mr) {
        return mr->fast_forward_to(pr);
    }).then([this, to_add = std::move(to_add)] {
        _all_readers.insert(_all_readers.end(), to_add.begin(), to_add.end());
        _next.assign(_all_readers.begin(), _all_readers.end());
    });
}

combined_mutation_reader::combined_mutation_reader(std::vector<mutation_reader> readers)
    : _readers(std::move(readers))
{
    _next.reserve(_readers.size());
    _current.reserve(_readers.size());
    _ptables.reserve(_readers.size());

    for (auto&& r : _readers) {
        _next.emplace_back(&r);
    }
    _all_readers.assign(_next.begin(), _next.end());
}

future<> combined_mutation_reader::fast_forward_to(const dht::partition_range& pr) {
    _ptables.clear();
    _next.assign(_all_readers.begin(), _all_readers.end());
    return parallel_for_each(_next, [this, &pr] (mutation_reader* mr) {
        return mr->fast_forward_to(pr);
    });
}

future<streamed_mutation_opt> combined_mutation_reader::operator()() {
    return next();
}

mutation_reader
make_combined_reader(std::vector<mutation_reader> readers) {
    return make_mutation_reader<combined_mutation_reader>(std::move(readers));
}

mutation_reader
make_combined_reader(mutation_reader&& a, mutation_reader&& b) {
    std::vector<mutation_reader> v;
    v.reserve(2);
    v.push_back(std::move(a));
    v.push_back(std::move(b));
    return make_combined_reader(std::move(v));
}

class reader_returning final : public mutation_reader::impl {
    streamed_mutation _m;
    bool _done = false;
public:
    reader_returning(streamed_mutation m) : _m(std::move(m)) {
    }
    virtual future<streamed_mutation_opt> operator()() override {
        if (_done) {
            return make_ready_future<streamed_mutation_opt>();
        } else {
            _done = true;
            return make_ready_future<streamed_mutation_opt>(std::move(_m));
        }
    }
};

mutation_reader make_reader_returning(mutation m, streamed_mutation::forwarding fwd) {
    return make_mutation_reader<reader_returning>(streamed_mutation_from_mutation(std::move(m), std::move(fwd)));
}

mutation_reader make_reader_returning(streamed_mutation m) {
    return make_mutation_reader<reader_returning>(std::move(m));
}

class reader_returning_many final : public mutation_reader::impl {
    std::vector<streamed_mutation> _m;
    dht::partition_range _pr;
public:
    reader_returning_many(std::vector<streamed_mutation> m, const dht::partition_range& pr) : _m(std::move(m)), _pr(pr) {
        boost::range::reverse(_m);
    }
    virtual future<streamed_mutation_opt> operator()() override {
        while (!_m.empty()) {
            auto& sm = _m.back();
            dht::ring_position_comparator cmp(*sm.schema());
            if (_pr.before(sm.decorated_key(), cmp)) {
                _m.pop_back();
            } else if (_pr.after(sm.decorated_key(), cmp)) {
                break;
            } else {
                auto m = std::move(sm);
                _m.pop_back();
                return make_ready_future<streamed_mutation_opt>(std::move(m));
            }
        }
        return make_ready_future<streamed_mutation_opt>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        _pr = pr;
        return make_ready_future<>();
    }
};

mutation_reader make_reader_returning_many(std::vector<mutation> mutations, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    std::vector<streamed_mutation> streamed_mutations;
    streamed_mutations.reserve(mutations.size());
    for (auto& m : mutations) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*m.schema(), slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
        auto sm = streamed_mutation_from_mutation(mutation(m.schema(), m.decorated_key(), std::move(mp)), fwd);
        streamed_mutations.emplace_back(std::move(sm));
    }
    return make_mutation_reader<reader_returning_many>(std::move(streamed_mutations), query::full_partition_range);
}

mutation_reader make_reader_returning_many(std::vector<mutation> mutations, const dht::partition_range& pr) {
    std::vector<streamed_mutation> streamed_mutations;
    boost::range::transform(mutations, std::back_inserter(streamed_mutations), [] (auto& m) {
        return streamed_mutation_from_mutation(std::move(m));
    });
    return make_mutation_reader<reader_returning_many>(std::move(streamed_mutations), pr);
}

mutation_reader make_reader_returning_many(std::vector<streamed_mutation> mutations) {
    return make_mutation_reader<reader_returning_many>(std::move(mutations), query::full_partition_range);
}

class empty_reader final : public mutation_reader::impl {
public:
    virtual future<streamed_mutation_opt> operator()() override {
        return make_ready_future<streamed_mutation_opt>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_ready_future<>();
    }
};

mutation_reader make_empty_reader() {
    return make_mutation_reader<empty_reader>();
}


class restricting_mutation_reader : public mutation_reader::impl {
    const restricted_mutation_reader_config& _config;
    unsigned _weight = 0;
    bool _waited = false;
    mutation_reader _base;
public:
    restricting_mutation_reader(const restricted_mutation_reader_config& config, unsigned weight, mutation_reader&& base)
            : _config(config), _weight(weight), _base(std::move(base)) {
        if (_config.sem->waiters() >= _config.max_queue_length) {
            _config.raise_queue_overloaded_exception();
        }
    }
    ~restricting_mutation_reader() {
        if (_waited) {
            _config.sem->signal(_weight);
        }
    }
    future<streamed_mutation_opt> operator()() override {
        // FIXME: we should defer freeing until the mutation is freed, perhaps,
        //        rather than just returned
        if (_waited) {
            return _base();
        }
        auto waited = _config.timeout.count() != 0
                ? _config.sem->wait(_config.timeout, _weight)
                : _config.sem->wait(_weight);
        return waited.then([this] {
            _waited = true;
            return _base();
        });
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        return _base.fast_forward_to(pr);
    }
};

mutation_reader
make_restricted_reader(const restricted_mutation_reader_config& config, unsigned weight, mutation_reader&& base) {
    return make_mutation_reader<restricting_mutation_reader>(config, weight, std::move(base));
}

class multi_range_mutation_reader : public mutation_reader::impl {
public:
    using ranges_vector = dht::partition_range_vector;
private:
    const ranges_vector& _ranges;
    ranges_vector::const_iterator _current_range;
    mutation_reader _reader;
public:
    multi_range_mutation_reader(schema_ptr s, mutation_source source, const ranges_vector& ranges,
                                const query::partition_slice& slice, const io_priority_class& pc,
                                tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
                                mutation_reader::forwarding fwd_mr)
        : _ranges(ranges)
        , _current_range(_ranges.begin())
        , _reader(source(s, *_current_range, slice, pc, trace_state, fwd,
            _ranges.size() > 1 ? mutation_reader::forwarding::yes : fwd_mr))
    {
    }

    virtual future<streamed_mutation_opt> operator()() override {
        return repeat_until_value([this] {
            return _reader().then([this] (streamed_mutation_opt smopt) {
                if (smopt) {
                    return make_ready_future<stdx::optional<streamed_mutation_opt>>(std::move(smopt));
                }
                ++_current_range;
                if (_current_range == _ranges.end()) {
                    return make_ready_future<stdx::optional<streamed_mutation_opt>>(streamed_mutation_opt());
                }
                return _reader.fast_forward_to(*_current_range).then([] {
                    return make_ready_future<stdx::optional<streamed_mutation_opt>>();
                });
            });
        });
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        // When end of pr is reached, this reader will increment _current_range
        // and notice that it now points to _ranges.end().
        _current_range = std::prev(_ranges.end());
        return _reader.fast_forward_to(pr);
    }
};

mutation_reader
make_multi_range_reader(schema_ptr s, mutation_source source, const dht::partition_range_vector& ranges,
                        const query::partition_slice& slice, const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
                        mutation_reader::forwarding fwd_mr)
{
    return make_mutation_reader<multi_range_mutation_reader>(std::move(s), std::move(source), ranges,
                                                             slice, pc, std::move(trace_state), fwd, fwd_mr);
}

snapshot_source make_empty_snapshot_source() {
    return snapshot_source([] {
        return make_empty_mutation_source();
    });
}

mutation_source make_empty_mutation_source() {
    return mutation_source([] (schema_ptr s,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd) {
        return make_empty_reader();
    });
}
