/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/range/algorithm/heap_algorithm.hpp>

#include "mutation_reader.hh"
#include "core/future-util.hh"

namespace stdx = std::experimental;

template<typename T>
inline
std::experimental::optional<T>
move_and_disengage(std::experimental::optional<T>& opt) {
    auto t = std::move(opt);
    opt = std::experimental::nullopt;
    return t;
}

// Combines multiple mutation_readers into one.
class combined_reader {
    std::vector<mutation_reader> _readers;
    struct mutation_and_reader {
        mutation m;
        mutation_reader* read;
    };
    std::vector<mutation_and_reader> _ptables;
    // comparison function for std::make_heap()/std::push_heap()
    static bool heap_compare(const mutation_and_reader& a, const mutation_and_reader& b) {
        auto&& s = a.m.schema();
        // order of comparison is inverted, because heaps produce greatest value first
        return b.m.decorated_key().less_compare(*s, a.m.decorated_key());
    }
    mutation_opt _current;
    bool _inited = false;
private:
    // Produces next mutation or disengaged optional if there are no more.
    //
    // Entry conditions:
    //  - either _ptables is empty or_ptables.back() is the next item to be consumed.
    //  - the _ptables heap is in invalid state (if not empty), waiting for pop_back or push_heap.
    future<mutation_opt> next() {
        if (_ptables.empty()) {
            return make_ready_future<mutation_opt>(move_and_disengage(_current));
        };


        auto& candidate = _ptables.back();
        mutation& m = candidate.m;

        if (_current && !_current->decorated_key().equal(*m.schema(), m.decorated_key())) {
            // key has changed, so emit accumulated mutation
            return make_ready_future<mutation_opt>(move_and_disengage(_current));
        }

        apply(_current, std::move(m));

        return (*candidate.read)().then([this] (mutation_opt&& more) {
            // Restore heap to valid state
            if (!more) {
                _ptables.pop_back();
            } else {
                _ptables.back().m = std::move(*more);
                boost::range::push_heap(_ptables, &heap_compare);
            }

            boost::range::pop_heap(_ptables, &heap_compare);
            return next();
        });
    }
public:
    combined_reader(std::vector<mutation_reader> readers)
        : _readers(std::move(readers))
    { }

    future<mutation_opt> operator()() {
        if (!_inited) {
            return parallel_for_each(_readers, [this] (mutation_reader& reader) {
                return reader().then([this, &reader](mutation_opt&& m) {
                    if (m) {
                        _ptables.push_back({std::move(*m), &reader});
                    }
                });
            }).then([this] {
                boost::range::make_heap(_ptables, &heap_compare);
                boost::range::pop_heap(_ptables, &heap_compare);
                _inited = true;
                return next();
            });
        }

        return next();
    }
};

mutation_reader
make_combined_reader(std::vector<mutation_reader> readers) {
    return combined_reader(std::move(readers));
}

class joining_reader final {
    std::vector<mutation_reader> _readers;
    std::vector<mutation_reader>::iterator _current;
public:
    joining_reader(std::vector<mutation_reader> readers)
            : _readers(std::move(readers))
            , _current(_readers.begin()) {
    }
    joining_reader(joining_reader&&) = default;
    future<mutation_opt> operator()() {
        if (_current == _readers.end()) {
            return make_ready_future<mutation_opt>(stdx::nullopt);
        }
        return (*_current)().then([this] (mutation_opt m) {
            if (!m) {
                ++_current;
                return operator()();
            } else {
                return make_ready_future<mutation_opt>(std::move(m));
            }
        });
    }
};

mutation_reader
make_combined_reader(mutation_reader&& a, mutation_reader&& b) {
    std::vector<mutation_reader> v;
    v.reserve(2);
    v.push_back(std::move(a));
    v.push_back(std::move(b));
    return make_combined_reader(std::move(v));
}

mutation_reader
make_joining_reader(std::vector<mutation_reader> readers) {
    return joining_reader(std::move(readers));
}

class lazy_reader final {
    std::function<mutation_reader ()> _make_reader;
    stdx::optional<mutation_reader> _reader;
public:
    lazy_reader(std::function<mutation_reader ()> make_reader)
            : _make_reader(std::move(make_reader)) {
    }
    future<mutation_opt> operator()() {
        if (!_reader) {
            _reader = _make_reader();
        }
        return (*_reader)();
    }
};

mutation_reader
make_lazy_reader(std::function<mutation_reader ()> make_reader) {
    return lazy_reader(std::move(make_reader));
}

mutation_reader make_reader_returning(mutation m) {
    return [m = std::move(m), done = false] () mutable {
        if (done) {
            return make_ready_future<mutation_opt>();
        } else {
            done = true;
            return make_ready_future<mutation_opt>(std::move(m));
        }
    };
}

mutation_reader make_reader_returning_many(std::vector<mutation> mutations) {
    std::reverse(mutations.begin(), mutations.end());
    return [mutations = std::move(mutations)] () mutable -> future<mutation_opt> {
        if (mutations.empty()) {
            return make_ready_future<mutation_opt>();
        }
        auto m = std::move(mutations.back());
        mutations.pop_back();
        return make_ready_future<mutation_opt>(std::move(m));
    };
}

mutation_reader make_empty_reader() {
    return [] { return make_ready_future<mutation_opt>(); };
}
