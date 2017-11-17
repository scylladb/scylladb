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
    dht::ring_position_comparator _tri_cmp;
public:
    index_comparator(const schema& s) : _tri_cmp(s) {}

    bool operator()(const summary_entry& e, dht::ring_position_view rp) const {
        return _tri_cmp(e.get_decorated_key(), rp) < 0;
    }

    bool operator()(const index_entry& e, dht::ring_position_view rp) const {
        return _tri_cmp(e.get_decorated_key(), rp) < 0;
    }

    bool operator()(dht::ring_position_view rp, const summary_entry& e) const {
        return _tri_cmp(e.get_decorated_key(), rp) > 0;
    }

    bool operator()(dht::ring_position_view rp, const index_entry& e) const {
        return _tri_cmp(e.get_decorated_key(), rp) > 0;
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

    // We keep two pages alive so that when we have two index readers where
    // one is catching up with the other, each page will be read only once.
    // There is a case when a single advance_to() may need to read two pages.
    shared_index_lists::list_ptr _prev_list;

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
    uint64_t _current_pi_idx = 0; // Points to upper bound of the cursor.
    uint64_t _data_file_position = 0;
    indexable_element _element = indexable_element::partition;
private:
    future<> advance_to_end() {
        sstlog.trace("index {}: advance_to_end()", this);
        _data_file_position = data_file_end();
        _element = indexable_element::partition;
        _prev_list = std::move(_current_list);
        return close_reader().finally([this] {
            _reader = stdx::nullopt;
        });
    }

    // Must be called for non-decreasing summary_idx.
    future<> advance_to_page(uint64_t summary_idx) {
        sstlog.trace("index {}: advance_to_page({})", this, summary_idx);
        assert(!_current_list || _current_summary_idx <= summary_idx);
        if (_current_list && _current_summary_idx == summary_idx) {
            sstlog.trace("index {}: same page", this);
            return make_ready_future<>();
        }

        auto& summary = _sstable->get_summary();
        if (summary_idx >= summary.header.size) {
            sstlog.trace("index {}: eof", this);
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
            _prev_list = std::move(_current_list);
            _current_list = std::move(ref);
            _current_summary_idx = summary_idx;
            _current_index_idx = 0;
            _current_pi_idx = 0;
            assert(!_current_list->empty());
            _data_file_position = (*_current_list)[0].position();
            _element = indexable_element::partition;

            if (sstlog.is_enabled(seastar::log_level::trace)) {
                sstlog.trace("index {}: page:", this);
                for (const index_entry& e : *_current_list) {
                    auto dk = dht::global_partitioner().decorate_key(*_sstable->_schema,
                        e.get_key().to_partition_key(*_sstable->_schema));
                    sstlog.trace("  {} -> {}", dk, e.position());
                }
            }
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

    index_reader(shared_sstable sst, const io_priority_class& pc)
        : _sstable(std::move(sst))
        , _pc(pc)
    {
        sstlog.trace("index {}: index_reader for {}", this, _sstable->get_filename());
    }

    index_reader(const index_reader& r)
        : _sstable(r._sstable)
        , _current_list(r._current_list)
        , _prev_list(r._prev_list)
        , _pc(r._pc)
        , _previous_summary_idx(r._previous_summary_idx)
        , _current_summary_idx(r._current_summary_idx)
        , _current_index_idx(r._current_index_idx)
        , _current_pi_idx(r._current_pi_idx)
        , _data_file_position(r._data_file_position)
        , _element(r._element)
    {
        sstlog.trace("index {}: index_reader for {}", this, _sstable->get_filename());
    }

    // Valid if partition_data_ready()
    index_entry& current_partition_entry() {
        assert(_current_list);
        return (*_current_list)[_current_index_idx];
    }

    // Returns tombstone for current partition, if it was recorded in the sstable.
    // It may be unavailable for old sstables for which this information was not generated.
    // Can be called only when partition_data_ready().
    stdx::optional<sstables::deletion_time> partition_tombstone() {
        index_entry& e = current_partition_entry();
        auto pi = e.get_promoted_index_view();
        if (!pi) {
            return stdx::nullopt;
        }
        return pi.get_deletion_time();
    }

    // Returns the key for current partition.
    // Can be called only when partition_data_ready().
    // The result is valid as long as index_reader is valid.
    key_view partition_key() {
        index_entry& e = current_partition_entry();
        return e.get_key();
    }

    // Tells whether details about current partition can be accessed.
    // If this returns false, you have to call read_partition_data().
    //
    // Calling read_partition_data() may involve doing I/O. The reason
    // why control over this is exposed and not done under the hood is that
    // in some cases it only makes sense to access partition details from index
    // if it is readily available, and if it is not, we're better off obtaining
    // them by continuing reading from sstable.
    bool partition_data_ready() const {
        return static_cast<bool>(_current_list);
    }

    // Ensures that partition_data_ready() returns true.
    // Can be called only when !eof()
    future<> read_partition_data() {
        assert(!eof());
        if (partition_data_ready()) {
            return make_ready_future<>();
        }
        // The only case when _current_list may be missing is when the cursor is at the beginning
        assert(_current_summary_idx == 0);
        return advance_to_page(0);
    }

    // Forwards the cursor to given position in current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may forward the cursor to some position pos' which precedes pos, even though
    // there exist rows with positions in the range [pos', pos].
    //
    // Must be called for non-decreasing positions.
    // Must be called only after advanced to some partition and !eof().
    future<> advance_to(position_in_partition_view pos) {
        sstlog.trace("index {}: advance_to({}), current data_file_pos={}", this, pos, _data_file_position);

        if (!partition_data_ready()) {
            return read_partition_data().then([this, pos] {
                sstlog.trace("index {}: page done", this);
                assert(partition_data_ready());
                return advance_to(pos);
            });
        }

        const schema& s = *_sstable->_schema;
        index_entry& e = current_partition_entry();
        promoted_index* pi = nullptr;
        try {
            pi = e.get_promoted_index(s);
        } catch (...) {
            sstlog.error("Failed to get promoted index for sstable {}, page {}, index {}: {}", _sstable->get_filename(),
                _current_summary_idx, _current_index_idx, std::current_exception());
        }
        if (!pi) {
            sstlog.trace("index {}: no promoted index", this);
            return make_ready_future<>();
        }

        if (sstlog.is_enabled(seastar::log_level::trace)) {
            sstlog.trace("index {}: promoted index:", this);
            for (auto&& e : pi->entries) {
                sstlog.trace("  {}-{}: +{} len={}", e.start, e.end, e.offset, e.width);
            }
        }

        auto cmp_with_start = [pos_cmp = position_in_partition::composite_less_compare(s)]
            (position_in_partition_view pos, const promoted_index::entry& e) -> bool {
            return pos_cmp(pos, e.start);
        };

        // Optimize short skips which typically land in the same block
        if (_current_pi_idx >= pi->entries.size() || cmp_with_start(pos, pi->entries[_current_pi_idx])) {
            sstlog.trace("index {}: position in current block", this);
            return make_ready_future<>();
        }

        auto i = std::upper_bound(pi->entries.begin() + _current_pi_idx, pi->entries.end(), pos, cmp_with_start);
        _current_pi_idx = std::distance(pi->entries.begin(), i);
        if (i != pi->entries.begin()) {
            --i;
        }
        _data_file_position = e.position() + i->offset;
        _element = indexable_element::cell;
        sstlog.trace("index {}: skipped to cell, _current_pi_idx={}, _data_file_position={}", this, _current_pi_idx, _data_file_position);
        return make_ready_future<>();
    }

    // Forwards the cursor to a position which is greater than given position in current partition.
    //
    // Note that the index within partition, unlike the partition index, doesn't cover all keys.
    // So this may not forward to the smallest position which is greater than pos.
    //
    // May advance to the next partition if it's not possible to find a suitable position inside
    // current partition.
    //
    // Must be called only when !eof().
    future<> advance_past(position_in_partition_view pos) {
        sstlog.trace("index {}: advance_past({}), current data_file_pos={}", this, pos, _data_file_position);

        if (!partition_data_ready()) {
            return read_partition_data().then([this, pos] {
                assert(partition_data_ready());
                return advance_past(pos);
            });
        }

        const schema& s = *_sstable->_schema;
        index_entry& e = current_partition_entry();
        promoted_index* pi = nullptr;
        try {
            pi = e.get_promoted_index(s);
        } catch (...) {
            sstlog.error("Failed to get promoted index for sstable {}, page {}, index {}: {}", _sstable->get_filename(),
                _current_summary_idx, _current_index_idx, std::current_exception());
        }
        if (!pi || pi->entries.empty()) {
            sstlog.trace("index {}: no promoted index", this);
            return advance_to_next_partition();
        }

        auto cmp_with_start = [pos_cmp = position_in_partition::composite_less_compare(s)]
            (position_in_partition_view pos, const promoted_index::entry& e) -> bool {
            return pos_cmp(pos, e.start);
        };

        auto i = std::upper_bound(pi->entries.begin() + _current_pi_idx, pi->entries.end(), pos, cmp_with_start);
        _current_pi_idx = std::distance(pi->entries.begin(), i);
        if (i == pi->entries.end()) {
            return advance_to_next_partition();
        }

        _data_file_position = e.position() + i->offset;
        _element = indexable_element::cell;
        sstlog.trace("index {}: skipped to cell, _current_pi_idx={}, _data_file_position={}", this, _current_pi_idx, _data_file_position);
        return make_ready_future<>();
    }

    // Like advance_to(dht::ring_position_view), but returns information whether the key was found
    future<bool> advance_and_check_if_present(dht::ring_position_view key) {
        return advance_to(key).then([this, key] {
            if (eof()) {
                return make_ready_future<bool>(false);
            }
            return read_partition_data().then([this, key] {
                index_comparator cmp(*_sstable->_schema);
                return cmp(key, current_partition_entry()) == 0;
            });
        });
    }

    // Moves the cursor to the beginning of next partition.
    // Can be called only when !eof().
    future<> advance_to_next_partition() {
        sstlog.trace("index {}: advance_to_next_partition()", this);
        if (!_current_list) {
            return advance_to_page(0).then([this] {
                return advance_to_next_partition();
            });
        }
        if (_current_index_idx + 1 < _current_list->size()) {
            ++_current_index_idx;
            _current_pi_idx = 0;
            _data_file_position = (*_current_list)[_current_index_idx].position();
            _element = indexable_element::partition;
            return make_ready_future<>();
        }
        auto& summary = _sstable->get_summary();
        if (_current_summary_idx + 1 < summary.header.size) {
            return advance_to_page(_current_summary_idx + 1);
        }
        return advance_to_end();
    }

    // Positions the cursor on the first partition which is not smaller than pos (like std::lower_bound).
    // Must be called for non-decreasing positions.
    future<> advance_to(dht::ring_position_view pos) {
        sstlog.trace("index {}: advance_to({}), _previous_summary_idx={}, _current_summary_idx={}", this, pos, _previous_summary_idx, _current_summary_idx);

        if (pos.is_min()) {
            sstlog.trace("index {}: first entry", this);
            return make_ready_future<>();
        } else if (pos.is_max()) {
            return advance_to_end();
        }

        auto& summary = _sstable->get_summary();
        _previous_summary_idx = std::distance(std::begin(summary.entries),
            std::lower_bound(summary.entries.begin() + _previous_summary_idx, summary.entries.end(), pos, index_comparator(*_sstable->_schema)));

        if (_previous_summary_idx == 0) {
            sstlog.trace("index {}: first entry", this);
            return make_ready_future<>();
        }

        auto summary_idx = _previous_summary_idx - 1;

        sstlog.trace("index {}: summary_idx={}", this, summary_idx);

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
            sstlog.trace("index {}: old page index = {}", this, _current_index_idx);
            auto i = std::lower_bound(il.begin() + _current_index_idx, il.end(), pos, index_comparator(*_sstable->_schema));
            if (i == il.end()) {
                sstlog.trace("index {}: not found", this);
                return advance_to_page(summary_idx + 1);
            }
            _current_index_idx = std::distance(il.begin(), i);
            _current_pi_idx = 0;
            _data_file_position = i->position();
            _element = indexable_element::partition;
            sstlog.trace("index {}: new page index = {}, pos={}", this, _current_index_idx, _data_file_position);
            return make_ready_future<>();
        });
    }

    // Returns position in the data file of the cursor.
    // Returns non-decreasing positions.
    // When eof(), returns data_file_end().
    uint64_t data_file_position() const {
        return _data_file_position;
    }

    // Returns the kind of sstable element the cursor is pointing at.
    indexable_element element_kind() const {
        return _element;
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
