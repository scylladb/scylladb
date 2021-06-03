/*
 * Copyright 2020-present ScyllaDB
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

#include <seastar/core/shared_future.hh>

#include "flat_mutation_reader.hh"

// A reader which allows to insert a deferring operation before reading.
// All calls will first wait for a future to resolve, then forward to a given underlying reader.
class chained_delegating_reader : public flat_mutation_reader::impl {
    std::unique_ptr<flat_mutation_reader> _underlying;
    std::function<future<flat_mutation_reader>(db::timeout_clock::time_point)> _populate_reader;
    std::function<void()> _on_destroyed;
    
public:
    chained_delegating_reader(schema_ptr s, std::function<future<flat_mutation_reader>(db::timeout_clock::time_point)>&& populate, reader_permit permit, std::function<void()> on_destroyed = []{})
        : impl(s, std::move(permit))
        , _populate_reader(std::move(populate))
        , _on_destroyed(std::move(on_destroyed))
    { }

    chained_delegating_reader(chained_delegating_reader&& rd) = delete;

    ~chained_delegating_reader() {
        _on_destroyed();
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        if (!_underlying) {
            return _populate_reader(timeout).then([this, timeout] (flat_mutation_reader&& rd) {
                _underlying = std::make_unique<flat_mutation_reader>(std::move(rd));
                return fill_buffer(timeout);
            });
        }

        if (is_buffer_full()) {
            return make_ready_future<>();
        }

        return _underlying->fill_buffer(timeout).then([this] {
            _end_of_stream = _underlying->is_end_of_stream();
            _underlying->move_buffer_content_to(*this);
        });
    }

    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        if (!_underlying) {
            return _populate_reader(timeout).then([this, timeout, pr = std::move(pr)] (flat_mutation_reader&& rd) mutable {
                _underlying = std::make_unique<flat_mutation_reader>(std::move(rd));
                return fast_forward_to(pr, timeout);
            });
        }

        _end_of_stream = false;
        forward_buffer_to(pr.start());
        return _underlying->fast_forward_to(std::move(pr), timeout);
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

    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        if (!_underlying) {
            return _populate_reader(timeout).then([this, timeout, &pr] (flat_mutation_reader&& rd) mutable {
                _underlying = std::make_unique<flat_mutation_reader>(std::move(rd));
                return fast_forward_to(pr, timeout);
            });
        }

        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(pr, timeout);
    }

    virtual future<> close() noexcept override {
        if (_underlying) {
            return _underlying->close();
        }
        return make_ready_future<>();
    }
};
