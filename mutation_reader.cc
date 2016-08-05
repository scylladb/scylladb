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

#include "mutation_reader.hh"
#include "core/future-util.hh"
#include "utils/move.hh"

namespace stdx = std::experimental;

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

combined_mutation_reader::combined_mutation_reader(std::vector<mutation_reader> readers)
    : _readers(std::move(readers))
{
    _next.reserve(_readers.size());
    _current.reserve(_readers.size());
    _ptables.reserve(_readers.size());

    for (auto&& r : _readers) {
        _next.emplace_back(&r);
    }
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

mutation_reader make_reader_returning(mutation m) {
    return make_mutation_reader<reader_returning>(streamed_mutation_from_mutation(std::move(m)));
}

mutation_reader make_reader_returning(streamed_mutation m) {
    return make_mutation_reader<reader_returning>(std::move(m));
}

class reader_returning_many final : public mutation_reader::impl {
    std::vector<streamed_mutation> _m;
    bool _done = false;
public:
    reader_returning_many(std::vector<streamed_mutation> m) : _m(std::move(m)) {
        boost::range::reverse(_m);
    }
    virtual future<streamed_mutation_opt> operator()() override {
        if (_m.empty()) {
            return make_ready_future<streamed_mutation_opt>();
        }
        auto m = std::move(_m.back());
        _m.pop_back();
        return make_ready_future<streamed_mutation_opt>(std::move(m));
    }
};

mutation_reader make_reader_returning_many(std::vector<mutation> mutations, const query::partition_slice& slice) {
    std::vector<streamed_mutation> streamed_mutations;
    streamed_mutations.reserve(mutations.size());
    for (auto& m : mutations) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*m.schema(), slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
        auto sm = streamed_mutation_from_mutation(mutation(m.schema(), m.decorated_key(), std::move(mp)));
        streamed_mutations.emplace_back(std::move(sm));
    }
    return make_mutation_reader<reader_returning_many>(std::move(streamed_mutations));
}

mutation_reader make_reader_returning_many(std::vector<streamed_mutation> mutations) {
    return make_mutation_reader<reader_returning_many>(std::move(mutations));
}

class empty_reader final : public mutation_reader::impl {
public:
    virtual future<streamed_mutation_opt> operator()() override {
        return make_ready_future<streamed_mutation_opt>();
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
};

mutation_reader
make_restricted_reader(const restricted_mutation_reader_config& config, unsigned weight, mutation_reader&& base) {
    return make_mutation_reader<restricting_mutation_reader>(config, weight, std::move(base));
}
