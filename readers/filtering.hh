/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader.hh"

template <typename MutationFilter>
requires std::is_invocable_r_v<bool, MutationFilter, const dht::decorated_key&>
class filtering_reader : public mutation_reader::impl {
    mutation_reader _rd;
    MutationFilter _filter;
public:
    filtering_reader(mutation_reader rd, MutationFilter&& filter)
        : impl(rd.schema(), rd.permit())
        , _rd(std::move(rd))
        , _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this] {
            return _rd.fill_buffer().then([this] {
                return do_until([this] { return _rd.is_buffer_empty(); }, [this] {
                    auto mf = _rd.pop_mutation_fragment();
                    if (mf.is_partition_start()) {
                        auto& dk = mf.as_partition_start().key();
                        if (!_filter(dk)) {
                            return _rd.next_partition();
                        }
                    }
                    push_mutation_fragment(std::move(mf));
                    return make_ready_future<>();
                }).then([this] {
                    _end_of_stream = _rd.is_end_of_stream();
                });
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = false;
            return _rd.next_partition();
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        return _rd.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        clear_buffer();
        _end_of_stream = false;
        return _rd.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _rd.close();
    }
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
