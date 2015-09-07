/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <vector>

#include "mutation.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"

// A mutation_reader is an object which allows iterating on mutations: invoke
// the function to get a future for the next mutation, with an unset optional
// marking the end of iteration. After calling mutation_reader's operator(),
// caller must keep the object alive until the returned future is fulfilled.
//
// The mutations returned have strictly monotonically increasing keys. Two
// consecutive mutations never have equal keys.
//
// TODO: When iterating over mutations, we don't need a schema_ptr for every
// single one as it is normally the same for all of them. So "mutation" might
// not be the optimal object to use here.
class mutation_reader final {
public:
    class impl {
    public:
        virtual ~impl() {}
        virtual future<mutation_opt> operator()() = 0;
    };
private:
    class null_impl final : public impl {
    public:
        virtual future<mutation_opt> operator()() override { throw std::bad_function_call(); }
    };
private:
    std::unique_ptr<impl> _impl;
public:
    mutation_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}
    mutation_reader() : mutation_reader(std::make_unique<null_impl>()) {}
    mutation_reader(mutation_reader&&) = default;
    mutation_reader(const mutation_reader&) = delete;
    mutation_reader& operator=(mutation_reader&&) = default;
    mutation_reader& operator=(const mutation_reader&) = delete;
    future<mutation_opt> operator()() { return _impl->operator()(); }
};

// Impl: derived from mutation_reader::impl; Args/args: arguments for Impl's constructor
template <typename Impl, typename... Args>
inline
mutation_reader
make_mutation_reader(Args&&... args) {
    return mutation_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

mutation_reader make_combined_reader(std::vector<mutation_reader>);
mutation_reader make_combined_reader(mutation_reader&& a, mutation_reader&& b);
// reads from the input readers, in order
mutation_reader make_joining_reader(std::vector<mutation_reader> readers);
mutation_reader make_reader_returning(mutation);
mutation_reader make_reader_returning_many(std::vector<mutation>);
mutation_reader make_empty_reader();
// Returns a reader that is lazily constructed on the first call.  Useful
// when creating the reader involves disk I/O or a shard call
mutation_reader make_lazy_reader(std::function<mutation_reader ()> make_reader);

template <typename MutationFilter>
class filtering_reader : public mutation_reader::impl {
    mutation_reader _rd;
    MutationFilter _filter;
    mutation_opt _current;
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const mutation&)>>::value, "bad MutationFilter signature");
public:
    filtering_reader(mutation_reader rd, MutationFilter&& filter)
            : _rd(std::move(rd)), _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<mutation_opt> operator()() override {\
        return repeat([this] {
            return _rd().then([this] (mutation_opt&& mo) mutable {
                if (!mo) {
                    _current = std::move(mo);
                    return stop_iteration::yes;
                } else {
                    if (_filter(*mo)) {
                        _current = std::move(mo);
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                }
            });
        }).then([this] {
            return make_ready_future<mutation_opt>(std::move(_current));
        });
    };
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
mutation_reader make_filtering_reader(mutation_reader rd, MutationFilter&& filter) {
    return make_mutation_reader<filtering_reader<MutationFilter>>(std::move(rd), std::forward<MutationFilter>(filter));
}

// Calls the consumer for each element of the reader's stream until end of stream
// is reached or the consumer requests iteration to stop by returning stop_iteration::yes.
// The consumer should accept mutation as the argument and return stop_iteration.
// The returned future<> resolves when consumption ends.
template <typename Consumer>
inline
future<> consume(mutation_reader& reader, Consumer consumer) {
    static_assert(std::is_same<future<stop_iteration>, futurize_t<std::result_of_t<Consumer(mutation&&)>>>::value, "bad Consumer signature");
    using futurator = futurize<std::result_of_t<Consumer(mutation&&)>>;

    return do_with(std::move(consumer), [&reader] (Consumer& c) -> future<> {
        return repeat([&reader, &c] () {
            return reader().then([&c] (mutation_opt&& mo) -> future<stop_iteration> {
                if (!mo) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return futurator::apply(c, std::move(*mo));
            });
        });
    });
}

// mutation_source represents source of data in mutation form. The data source
// can be queried multiple times and in parallel. For each query it returns
// independent mutation_reader.
using mutation_source = std::function<mutation_reader(const query::partition_range& range)>;

/// A partition_presence_checker quickly returns whether a key is known not to exist
/// in a data source (it may return false positives, but not false negatives).
enum class partition_presence_checker_result {
    definitely_doesnt_exist,
    maybe_exists
};
using partition_presence_checker = std::function<partition_presence_checker_result (const partition_key& key)>;

inline
partition_presence_checker make_default_partition_presence_checker() {
    return [] (const partition_key& key) { return partition_presence_checker_result::maybe_exists; };
}
