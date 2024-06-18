/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_future.hh>

#include "readers/mutation_reader.hh"

// A reader which allows to insert a deferring operation before reading.
// All calls will first wait for a future to resolve, then forward to a given underlying reader.
class chained_delegating_reader : public mutation_reader::impl {
    std::unique_ptr<mutation_reader> _underlying;
    std::function<future<mutation_reader>()> _populate_reader;
    std::function<void()> _on_destroyed;
    
public:
    chained_delegating_reader(schema_ptr s, std::function<future<mutation_reader>()>&& populate, reader_permit permit, std::function<void()> on_destroyed = []{})
        : impl(s, std::move(permit))
        , _populate_reader(std::move(populate))
        , _on_destroyed(std::move(on_destroyed))
    { }

    chained_delegating_reader(chained_delegating_reader&& rd) = delete;

    ~chained_delegating_reader() {
        _on_destroyed();
    }

    virtual future<> fill_buffer() override {
        if (!_underlying) {
            return _populate_reader().then([this] (mutation_reader&& rd) {
                _underlying = std::make_unique<mutation_reader>(std::move(rd));
                return fill_buffer();
            });
        }

        if (is_buffer_full()) {
            return make_ready_future<>();
        }

        return _underlying->fill_buffer().then([this] {
            _end_of_stream = _underlying->is_end_of_stream();
            _underlying->move_buffer_content_to(*this);
        });
    }

    virtual future<> fast_forward_to(position_range pr) override {
        if (!_underlying) {
            return _populate_reader().then([this, pr = std::move(pr)] (mutation_reader&& rd) mutable {
                _underlying = std::make_unique<mutation_reader>(std::move(rd));
                return fast_forward_to(pr);
            });
        }

        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(std::move(pr));
    }

    virtual future<> next_partition() override {
        if (!_underlying) {
            return make_ready_future<>();
        }

        clear_buffer_to_next_partition();
        auto f = make_ready_future<>();
        if (is_buffer_empty()) {
            f = _underlying->next_partition();
        }
        _end_of_stream = _underlying->is_end_of_stream() && _underlying->is_buffer_empty();

        return f;
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        if (!_underlying) {
            return _populate_reader().then([this, &pr] (mutation_reader&& rd) mutable {
                _underlying = std::make_unique<mutation_reader>(std::move(rd));
                return fast_forward_to(pr);
            });
        }

        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(pr);
    }

    virtual future<> close() noexcept override {
        if (_underlying) {
            return _underlying->close();
        }
        return make_ready_future<>();
    }
};
