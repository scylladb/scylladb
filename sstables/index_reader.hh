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

#pragma once
#include "sstables.hh"
#include "consumer.hh"
#include "downsampling.hh"
#include "sstables/shared_index_lists.hh"

namespace sstables {

class index_consumer {
    uint64_t max_quantity;
public:
    index_list indexes;

    index_consumer(uint64_t q) : max_quantity(q) {
        indexes.reserve(q);
    }

    bool should_continue() {
        return indexes.size() < max_quantity;
    }
    void consume_entry(index_entry&& ie, uint64_t offset) {
        indexes.push_back(std::move(ie));
    }
    void reset() {
        indexes.clear();
    }
};

// IndexConsumer is a concept that implements:
//
// bool should_continue();
// void consume_entry(index_entry&& ie, uintt64_t offset);
template <class IndexConsumer>
class index_consume_entry_context: public data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>> {
    using proceed = data_consumer::proceed;
    using continuous_data_consumer = data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>>;
private:
    IndexConsumer& _consumer;
    uint64_t _entry_offset;

    enum class state {
        START,
        KEY_SIZE,
        KEY_BYTES,
        POSITION,
        PROMOTED_SIZE,
        PROMOTED_BYTES,
        CONSUME_ENTRY,
    } _state = state::START;

    temporary_buffer<char> _key;
    temporary_buffer<char> _promoted;

public:
    void verify_end_state() {
    }

    bool non_consuming() const {
        return ((_state == state::CONSUME_ENTRY) || (_state == state::START) ||
                ((_state == state::PROMOTED_BYTES) && (continuous_data_consumer::_prestate == continuous_data_consumer::prestate::NONE)));
    }

    proceed process_state(temporary_buffer<char>& data) {
        switch (_state) {
        // START comes first, to make the handling of the 0-quantity case simpler
        case state::START:
            if (!_consumer.should_continue()) {
                return proceed::no;
            }
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
        case state::KEY_BYTES:
            if (this->read_bytes(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
        case state::POSITION:
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_SIZE;
                break;
            }
        case state::PROMOTED_SIZE:
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_BYTES;
                break;
            }
        case state::PROMOTED_BYTES:
            if (this->read_bytes(data, this->_u32, _promoted) != continuous_data_consumer::read_status::ready) {
                _state = state::CONSUME_ENTRY;
                break;
            }
        case state::CONSUME_ENTRY: {
            auto len = (_key.size() + _promoted.size() + 14);
            _consumer.consume_entry(index_entry(std::move(_key), this->_u64, std::move(_promoted)), _entry_offset);
            _entry_offset += len;
            _state = state::START;
        }
            break;
        default:
            throw malformed_sstable_exception("unknown state");
        }
        return proceed::yes;
    }

    index_consume_entry_context(IndexConsumer& consumer,
            input_stream<char>&& input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(input), start, maxlen)
        , _consumer(consumer), _entry_offset(start)
    {}

    void reset(uint64_t offset) {
        _state = state::START;
        _entry_offset = offset;
        _consumer.reset();
    }
};

// Less-comparator for lookups in the partition index.
class index_comparator {
    const schema& _s;
public:
    index_comparator(const schema& s) : _s(s) {}

    int tri_cmp(key_view k2, dht::ring_position_view pos) const {
        return -pos.tri_compare(_s, k2);
    }

    bool operator()(const summary_entry& e, dht::ring_position_view rp) const {
        return tri_cmp(e.get_key(), rp) < 0;
    }

    bool operator()(const index_entry& e, dht::ring_position_view rp) const {
        return tri_cmp(e.get_key(), rp) < 0;
    }

    bool operator()(dht::ring_position_view rp, const summary_entry& e) const {
        return tri_cmp(e.get_key(), rp) > 0;
    }

    bool operator()(dht::ring_position_view rp, const index_entry& e) const {
        return tri_cmp(e.get_key(), rp) > 0;
    }
};

// Provides access to sstable indexes.
//
// Maintains logical cursor to sstable elements (partitions, cells).
// Initially the cursor is positioned on the first partition in the sstable.
// The cursor can be advanced forward using advance_to().
//
// If eof() then the cursor is positioned past all partitions in the sstable.
class index_reader {
    shared_sstable _sstable;
    shared_index_lists::list_ptr _current_list;
    const io_priority_class& _pc;

    struct reader {
        index_consumer _consumer;
        index_consume_entry_context<index_consumer> _context;

        static auto create_file_input_stream(shared_sstable sst, const io_priority_class& pc, uint64_t begin, uint64_t end) {
            file_input_stream_options options;
            options.buffer_size = sst->sstable_buffer_size;
            options.read_ahead = 2;
            options.io_priority_class = pc;
            return make_file_input_stream(sst->_index_file, begin, end - begin, std::move(options));
        }

        reader(shared_sstable sst, const io_priority_class& pc, uint64_t begin, uint64_t end, uint64_t quantity)
            : _consumer(quantity)
            , _context(_consumer, create_file_input_stream(sst, pc, begin, end), begin, end - begin)
        { }
    };

    stdx::optional<reader> _reader;

    uint64_t _previous_summary_idx = 0;
    uint64_t _current_summary_idx = 0;
    uint64_t _current_index_idx = 0;
    uint64_t _data_file_position = 0;
private:
    future<> advance_to_end() {
        _data_file_position = data_file_end();
        _current_list = {};
        return close_reader().finally([this] {
            _reader = stdx::nullopt;
        });
    }

    // Must be called for non-decreasing summary_idx.
    future<> advance_to_page(uint64_t summary_idx) {
        assert(!_current_list || _current_summary_idx <= summary_idx);
        if (_current_list && _current_summary_idx == summary_idx) {
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        if (summary_idx >= summary.header.size) {
            return advance_to_end();
        }

        auto loader = [this] (uint64_t summary_idx) -> future<index_list> {
            auto& summary = _sstable->get_summary();
            uint64_t position = summary.entries[summary_idx].position;
            uint64_t quantity = downsampling::get_effective_index_interval_after_index(summary_idx, summary.header.sampling_level,
                summary.header.min_index_interval);

            uint64_t end;
            if (summary_idx + 1 >= summary.header.size) {
                end = _sstable->index_size();
            } else {
                end = summary.entries[summary_idx + 1].position;
            }

            return close_reader().then_wrapped([this, position, end, quantity, summary_idx] (auto&& f) {
                try {
                    f.get();
                    _reader.emplace(_sstable, _pc, position, end, quantity);
                } catch (...) {
                    _reader = stdx::nullopt;
                    throw;
                }
                return _reader->_context.consume_input(_reader->_context).then([this] {
                    return std::move(_reader->_consumer.indexes);
                });
            });
        };

        return _sstable->_index_lists.get_or_load(summary_idx, loader).then([this, summary_idx] (shared_index_lists::list_ptr ref) {
            _current_list = std::move(ref);
            _current_summary_idx = summary_idx;
            _current_index_idx = 0;
            assert(!_current_list->empty());
            _data_file_position = (*_current_list)[0].position();
        });
    }
public:
    future<> advance_to_start(const dht::partition_range& range) {
        if (range.start()) {
            return advance_to(dht::ring_position_view(range.start()->value(),
                              dht::ring_position_view::after_key(!range.start()->is_inclusive())));
        }
        return make_ready_future<>();
    }

    future<> advance_to_end(const dht::partition_range& range) {
        if (range.end()) {
            return advance_to(dht::ring_position_view(range.end()->value(),
                              dht::ring_position_view::after_key(range.end()->is_inclusive())));
        }
        return advance_to_end();
    }
public:
    index_reader(shared_sstable sst, const io_priority_class& pc)
        : _sstable(std::move(sst))
        , _pc(pc)
    { }

    // Cannot be used twice on the same summary_idx and together with advance_to().
    // @deprecated
    future<index_list> get_index_entries(uint64_t summary_idx) {
        return advance_to_page(summary_idx).then([this] {
            return _current_list ? _current_list.release() : index_list();
        });
    }
public:
    // Positions the cursor on the first partition which is not smaller than pos (like std::lower_bound).
    // Must be called for non-decreasing positions.
    future<> advance_to(dht::ring_position_view pos) {
        auto& summary = _sstable->get_summary();
        _previous_summary_idx = std::distance(std::begin(summary.entries),
            std::lower_bound(summary.entries.begin() + _previous_summary_idx, summary.entries.end(), pos, index_comparator(*_sstable->_schema)));

        if (_previous_summary_idx == 0) {
            return make_ready_future<>();
        }

        auto summary_idx = _previous_summary_idx - 1;

        // Despite the requirement that the values of 'pos' in subsequent calls
        // are increasing we still may encounter a situation when we try to read
        // the previous bucket.
        // For example, let's say we have index like this:
        // summary:  A       K       ...
        // index:    A C D F K M N O ...
        // Now, we want to get positions for range [G, J]. We start with [G,
        // summary look up will tel us to check the first bucket. However, there
        // is no G in that bucket so we read the following one to get the
        // position (see the advance_to_page() call below). After we've got it, it's time to
        // get J] position. Again, summary points us to the first bucket and we
        // hit an assert since the reader is already at the second bucket and we
        // cannot go backward.
        // The solution is this condition above. If our lookup requires reading
        // the previous bucket we assume that the entry doesn't exist and return
        // the position of the first one in the current index bucket.
        if (summary_idx + 1 == _current_summary_idx) {
            return make_ready_future<>();
        }

        return advance_to_page(summary_idx).then([this, pos, summary_idx] {
            index_list& il = *_current_list;
            auto i = std::lower_bound(il.begin() + _current_index_idx, il.end(), pos, index_comparator(*_sstable->_schema));
            if (i == il.end()) {
                return advance_to_page(summary_idx + 1);
            }
            _current_index_idx = std::distance(il.begin(), i);
            _data_file_position = i->position();
            return make_ready_future<>();
        });
    }

    // Returns position in the data file of the cursor.
    // Returns non-decreasing positions.
    // When eof(), returns data_file_end().
    uint64_t data_file_position() const {
        return _data_file_position;
    }

    // Returns position right after all partitions in the sstable
    uint64_t data_file_end() const {
        return _sstable->data_size();
    }

    bool eof() const {
        return _data_file_position == data_file_end();
    }
private:
    future<> close_reader() {
        if (_reader) {
            return _reader->_context.close();
        }
        return make_ready_future<>();
    }
public:
    future<sstable::disk_read_range> get_disk_read_range(const dht::partition_range& range) {
        return advance_to_start(range).then([this, &range] () {
            uint64_t start = data_file_position();
            return advance_to_end(range).then([this, &range, start] () {
                uint64_t end = data_file_position();
                return sstable::disk_read_range(start, end);
            });
        });
    }

    future<> close() {
        return close_reader();
    }
};

}
