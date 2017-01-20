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

    int tri_cmp(key_view k2, const dht::ring_position& pos) const {
        auto k2_token = dht::global_partitioner().get_token(k2);

        if (k2_token == pos.token()) {
            if (pos.has_key()) {
                return k2.tri_compare(_s, *pos.key());
            } else {
                return -pos.relation_to_keys();
            }
        } else {
            return k2_token < pos.token() ? -1 : 1;
        }
    }

    bool operator()(const summary_entry& e, const dht::ring_position& rp) const {
        return tri_cmp(e.get_key(), rp) < 0;
    }

    bool operator()(const index_entry& e, const dht::ring_position& rp) const {
        return tri_cmp(e.get_key(), rp) < 0;
    }

    bool operator()(const dht::ring_position& rp, const summary_entry& e) const {
        return tri_cmp(e.get_key(), rp) > 0;
    }

    bool operator()(const dht::ring_position& rp, const index_entry& e) const {
        return tri_cmp(e.get_key(), rp) > 0;
    }
};

class index_reader {
    shared_sstable _sstable;
    const io_priority_class& _pc;

    struct reader {
        index_consumer _consumer;
        index_consume_entry_context<index_consumer> _context;
        uint64_t _current_summary_idx;

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

    index_list _previous_bucket;
    static constexpr uint64_t invalid_idx = std::numeric_limits<uint64_t>::max();
    uint64_t _previous_summary_idx = invalid_idx;
private:
    future<> read_index_entries(uint64_t summary_idx) {
        assert(!_reader || _reader->_current_summary_idx <= summary_idx);
        if (_reader && _reader->_current_summary_idx == summary_idx) {
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        if (summary_idx >= summary.header.size) {
            return close_reader().finally([this] {
                _reader = stdx::nullopt;
            });
        }

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
            _reader->_current_summary_idx = summary_idx;
            return _reader->_context.consume_input(_reader->_context);
        });
    }

    future<uint64_t> data_end_position(uint64_t summary_idx) {
        // We should only go to the end of the file if we are in the last summary group.
        // Otherwise, we will determine the end position of the current data read by looking
        // at the first index in the next summary group.
        auto& summary = _sstable->get_summary();
        if (size_t(summary_idx + 1) >= summary.entries.size()) {
            return make_ready_future<uint64_t>(_sstable->data_size());
        }
        return read_index_entries(summary_idx + 1).then([this] {
            return _reader->_consumer.indexes.front().position();
        });
    }

    future<uint64_t> start_position(const schema& s, const dht::partition_range& range) {
        return range.start() ? (range.start()->is_inclusive()
                                ? lower_bound(s, range.start()->value())
                                : upper_bound(s, range.start()->value()))
                             : make_ready_future<uint64_t>(0);
    }

    future<uint64_t> end_position(const schema& s, const dht::partition_range& range) {
        return range.end() ? (range.end()->is_inclusive()
                              ? upper_bound(s, range.end()->value())
                              : lower_bound(s, range.end()->value()))
                           : make_ready_future<uint64_t>(_sstable->data_size());
    };
public:
    index_reader(shared_sstable sst, const io_priority_class& pc)
        : _sstable(std::move(sst))
        , _pc(pc)
    { }

    future<index_list> get_index_entries(uint64_t summary_idx) {
        return read_index_entries(summary_idx).then([this] {
            return _reader ? std::move(_reader->_consumer.indexes) : index_list();
        });
    }
private:
    enum class bound_kind { lower, upper };

    template<bound_kind bound>
    future<uint64_t> find_bound(const schema& s, const dht::ring_position& pos) {
        auto do_find_bound = [] (auto begin, auto end, const dht::ring_position& pos, const index_comparator& cmp) {
            if (bound == bound_kind::lower) {
                return std::lower_bound(begin, end, pos, cmp);
            } else {
                return std::upper_bound(begin, end, pos, cmp);
            }
        };

        auto& summary = _sstable->get_summary();
        uint64_t summary_idx = std::distance(std::begin(summary.entries),
             do_find_bound(summary.entries.begin(), summary.entries.end(), pos, index_comparator(s)));

        if (summary_idx == 0) {
            return make_ready_future<uint64_t>(0);
        }

        --summary_idx;

        // Despite the requirement that the values of 'pos' in subsequent calls
        // are increasing we still may encounter a situation when we try to read
        // the previous bucket.
        // For example, let's say we have index like this:
        // summary:  A       K       ...
        // index:    A C D F K M N O ...
        // Now, we want to get positions for range [G, J]. We start with [G,
        // summary look up will tel us to check the first bucket. However, there
        // is no G in that bucket so we read the following one to get the
        // position (see data_end_position()). After we've got it, it's time to
        // get J] position. Again, summary points us to the first bucket and we
        // hit an assert since the reader is already at the second bucket and we
        // cannot go backward.
        // The solution is this condition above. If our lookup requires reading
        // the previous bucket we assume that the entry doesn't exist and return
        // the position of the first one in the current index bucket.
        if (_reader && summary_idx + 1 == _reader->_current_summary_idx) {
            return make_ready_future<uint64_t>(_reader->_consumer.indexes.front().position());
        }

        return read_index_entries(summary_idx).then([this, &s, pos, summary_idx, do_find_bound = std::move(do_find_bound)] {
            if (!_reader) {
                return data_end_position(summary_idx);
            }
            auto& il = _reader->_consumer.indexes;
            auto i = do_find_bound(il.begin(), il.end(), pos, index_comparator(s));
            if (i == il.end()) {
                return data_end_position(summary_idx);
            }
            return make_ready_future<uint64_t>(i->position());
        });
    }

    future<uint64_t> lower_bound(const schema& s, const dht::ring_position& pos) {
        return find_bound<bound_kind::lower>(s, pos);
    }

    future<uint64_t> upper_bound(const schema& s, const dht::ring_position& pos) {
        return find_bound<bound_kind::upper>(s, pos);
    }
    future<> close_reader() {
        if (_reader) {
            return _reader->_context.close();
        }
        return make_ready_future<>();
    }
public:
    future<sstable::disk_read_range> get_disk_read_range(const schema& s, const dht::partition_range& range) {
        return start_position(s, range).then([this, &s, &range] (uint64_t start) {
            return end_position(s, range).then([&s, &range, start] (uint64_t end) {
                return sstable::disk_read_range(start, end);
            });
        });
    }

    future<> close() {
        return close_reader();
    }
};

}
