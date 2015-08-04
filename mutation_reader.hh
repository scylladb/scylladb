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
using mutation_reader = std::function<future<mutation_opt>()>;

mutation_reader make_combined_reader(std::vector<mutation_reader>);
// reads from the input readers, in order
mutation_reader make_joining_reader(std::vector<mutation_reader> readers);
mutation_reader make_reader_returning(mutation);
mutation_reader make_reader_returning_many(std::vector<mutation>);
mutation_reader make_empty_reader();


// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
mutation_reader make_filtering_reader(mutation_reader rd, MutationFilter&& filter) {
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const mutation&)>>::value, "bad MutationFilter signature");

    return [rd = std::move(rd), filter = std::forward<MutationFilter>(filter), current = mutation_opt()] () mutable {
        return repeat([&rd, &filter, &current] () mutable {
            return rd().then([&filter, &current] (mutation_opt&& mo) mutable {
                if (!mo) {
                    current = std::move(mo);
                    return stop_iteration::yes;
                } else {
                    if (filter(*mo)) {
                        current = std::move(mo);
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                }
            });
        }).then([&current] {
            return make_ready_future<mutation_opt>(std::move(current));
        });
    };
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
