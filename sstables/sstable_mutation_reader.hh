/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "mutation.hh"
#include "sstables.hh"
#include "types.hh"
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include "key.hh"
#include "keys.hh"
#include <seastar/core/do_with.hh>
#include "unimplemented.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/byteorder.hh>
#include "index_reader.hh"
#include "counters.hh"
#include "utils/data_input.hh"
#include "clustering_ranges_walker.hh"
#include "binary_search.hh"
#include "../dht/i_partitioner.hh"

namespace sstables {

namespace kl {
    class mp_row_consumer_k_l;
}

namespace mx {
    class mp_row_consumer_m;
}

class mp_row_consumer_reader : public flat_mutation_reader::impl {
    friend class sstables::kl::mp_row_consumer_k_l;
    friend class sstables::mx::mp_row_consumer_m;
protected:
    shared_sstable _sst;

    // Whether index lower bound is in current partition
    bool _index_in_current_partition = false;

    // True iff the consumer finished generating fragments for a partition and hasn't
    // entered the new partition yet.
    // Implies that partition_end was emitted for the last partition.
    // Will cause the reader to skip to the next partition if !_before_partition.
    bool _partition_finished = true;

    // When set, the consumer is positioned right before a partition or at end of the data file.
    // _index_in_current_partition applies to the partition which is about to be read.
    bool _before_partition = true;

    std::optional<dht::decorated_key> _current_partition_key;
public:
    mp_row_consumer_reader(schema_ptr s, reader_permit permit, shared_sstable sst)
        : impl(std::move(s), std::move(permit))
        , _sst(std::move(sst))
    { }

    // Called when all fragments relevant to the query range or fast forwarding window
    // within the current partition have been pushed.
    // If no skipping is required, this method may not be called before transitioning
    // to the next partition.
    virtual void on_out_of_clustering_range() = 0;

    void on_next_partition(dht::decorated_key key, tombstone tomb);
};

inline atomic_cell make_atomic_cell(const abstract_type& type,
                                    api::timestamp_type timestamp,
                                    fragmented_temporary_buffer::view value,
                                    gc_clock::duration ttl,
                                    gc_clock::time_point expiration,
                                    atomic_cell::collection_member cm) {
    if (ttl != gc_clock::duration::zero()) {
        return atomic_cell::make_live(type, timestamp, value, expiration, ttl, cm);
    } else {
        return atomic_cell::make_live(type, timestamp, value, cm);
    }
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, fragmented_temporary_buffer::view value);

position_in_partition_view get_slice_upper_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key);

// data_consume_rows() iterates over rows in the data file from
// a particular range, feeding them into the consumer. The iteration is
// done as efficiently as possible - reading only the data file (not the
// summary or index files) and reading data in batches.
//
// The consumer object may request the iteration to stop before reaching
// the end of the requested data range (e.g. stop after each sstable row).
// A context object is returned which allows to resume this consumption:
// This context's read() method requests that consumption begins, and
// returns a future which will be resolved when it ends (because the
// consumer asked to stop, or the data range ended). Only after the
// returned future is resolved, may read() be called again to consume
// more.
// The caller must ensure (e.g., using do_with()) that the context object,
// as well as the sstable, remains alive as long as a read() is in
// progress (i.e., returned a future which hasn't completed yet).
//
// The "toread" range specifies the range we want to read initially.
// However, the object returned by the read, a data_consume_context, also
// provides a fast_forward_to(start,end) method which allows resetting
// the reader to a new range. To allow that, we also have a "last_end"
// byte which should be the last end to which fast_forward_to is
// eventually allowed. If last_end==end, fast_forward_to is not allowed
// at all, if last_end==file_size fast_forward_to is allowed until the
// end of the file, and it can be something in between if we know that we
// are planning to skip parts, but eventually read until last_end.
// When last_end==end, we guarantee that the read will only read the
// desired byte range from disk. However, when last_end > end, we may
// read beyond end in anticipation of a small skip via fast_foward_to.
// The amount of this excessive read is controlled by read ahead
// hueristics which learn from the usefulness of previous read aheads.
template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    auto input = sst->data_stream(toread.start, last_end - toread.start, consumer.io_priority(),
            consumer.permit(), consumer.trace_state(), sst->_partition_range_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_single_partition(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread) {
    auto input = sst->data_stream(toread.start, toread.end - toread.start, consumer.io_priority(),
            consumer.permit(), consumer.trace_state(), sst->_single_partition_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

// Like data_consume_rows() with bounds, but iterates over whole range
template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer) {
        auto data_size = sst->data_size();
        return data_consume_rows<DataConsumeRowsContext>(s, std::move(sst), consumer, {0, data_size}, data_size);
}

template<typename T>
concept RowConsumer =
    requires(T t,
                    const partition_key& pk,
                    position_range cr,
                    db::timeout_clock::time_point timeout) {
        { t.io_priority() } -> std::convertible_to<const io_priority_class&>;
        { t.is_mutation_end() } -> std::same_as<bool>;
        { t.setup_for_partition(pk) } -> std::same_as<void>;
        { t.push_ready_fragments() } -> std::same_as<void>;
        { t.maybe_skip() } -> std::same_as<std::optional<position_in_partition_view>>;
        { t.fast_forward_to(std::move(cr), timeout) } -> std::same_as<std::optional<position_in_partition_view>>;
    };

/*
 * Helper method to set or reset the range tombstone start bound according to the
 * end open marker of a promoted index block.
 *
 * Only applies to consumers that have the following methods:
 *      void reset_range_tombstone_start();
 *      void set_range_tombstone_start(clustering_key_prefix, bound_kind, tombstone);
 *
 * For other consumers, it is a no-op.
 */
template <typename Consumer>
void set_range_tombstone_start_from_end_open_marker(Consumer& c, const schema& s, const index_reader& idx) {
    if constexpr (Consumer::is_setting_range_tombstone_start_supported) {
        auto open_end_marker = idx.end_open_marker();
        if (open_end_marker) {
            auto[pos, tomb] = *open_end_marker;
            if (pos.is_clustering_row()) {
                auto ck = pos.key();
                bool was_non_full = clustering_key::make_full(s, ck);
                c.set_range_tombstone_start(
                        std::move(ck),
                        was_non_full ? bound_kind::incl_start : bound_kind::excl_start,
                        tomb);
            } else {
                auto view = position_in_partition_view(pos).as_start_bound_view();
                c.set_range_tombstone_start(view.prefix(), view.kind(), tomb);
            }
        } else {
            c.reset_range_tombstone_start();
        }
    }
}

template <typename DataConsumeRowsContext, typename Consumer>
requires RowConsumer<Consumer>
class sstable_mutation_reader : public mp_row_consumer_reader {
    Consumer _consumer;
    bool _will_likely_slice = false;
    bool _read_enabled = true;
    std::unique_ptr<DataConsumeRowsContext> _context;
    std::unique_ptr<index_reader> _index_reader;
    // We avoid unnecessary lookup for single partition reads thanks to this flag
    bool _single_partition_read = false;
    const dht::partition_range& _pr;
    const query::partition_slice& _slice;
    const io_priority_class& _pc;
    streamed_mutation::forwarding _fwd;
    mutation_reader::forwarding _fwd_mr;
    read_monitor& _monitor;
public:
    sstable_mutation_reader(shared_sstable sst,
         schema_ptr schema,
         reader_permit permit,
         const dht::partition_range& pr,
         const query::partition_slice& slice,
         const io_priority_class& pc,
         tracing::trace_state_ptr trace_state,
         streamed_mutation::forwarding fwd,
         mutation_reader::forwarding fwd_mr,
         read_monitor& mon)
        : mp_row_consumer_reader(std::move(schema), permit, std::move(sst))
        , _consumer(this, _schema, std::move(permit), slice, pc, std::move(trace_state), fwd, _sst)
        // FIXME: I want to add `&& fwd_mr == mutation_reader::forwarding::no` below
        // but can't because many call sites use the default value for
        // `mutation_reader::forwarding` which is `yes`.
        , _single_partition_read(pr.is_singular())
        , _pr(pr)
        , _slice(slice)
        , _pc(pc)
        , _fwd(fwd)
        , _fwd_mr(fwd_mr)
        , _monitor(mon) { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    sstable_mutation_reader(sstable_mutation_reader&&) = delete;
    sstable_mutation_reader(const sstable_mutation_reader&) = delete;
    ~sstable_mutation_reader() {
        if (_context || _index_reader) {
            sstlog.warn("sstable_mutation_reader was not closed. Closing in the background. Backtrace: {}", current_backtrace());
            // FIXME: discarded future.
            (void)close();
        }
    }
private:
    static bool will_likely_slice(const query::partition_slice& slice) {
        return (!slice.default_row_ranges().empty() && !slice.default_row_ranges()[0].is_full())
               || slice.get_specific_ranges();
    }
    index_reader& get_index_reader() {
        if (!_index_reader) {
            _index_reader = std::make_unique<index_reader>(_sst, _consumer.permit(), _consumer.io_priority(), _consumer.trace_state());
        }
        return *_index_reader;
    }
    future<> advance_to_next_partition() {
        sstlog.trace("reader {}: advance_to_next_partition()", fmt::ptr(this));
        _before_partition = true;
        auto& consumer = _consumer;
        if (consumer.is_mutation_end()) {
            sstlog.trace("reader {}: already at partition boundary", fmt::ptr(this));
            _index_in_current_partition = false;
            return make_ready_future<>();
        }
        return (_index_in_current_partition
                ? _index_reader->advance_to_next_partition()
                : get_index_reader().advance_to(dht::ring_position_view::for_after_key(*_current_partition_key))).then([this] {
            _index_in_current_partition = true;
            auto [start, end] = _index_reader->data_file_positions();
            if (end && start > *end) {
                _read_enabled = false;
                return make_ready_future<>();
            }
            assert(_index_reader->element_kind() == indexable_element::partition);
            return skip_to(_index_reader->element_kind(), start).then([this] {
                _sst->get_stats().on_partition_seek();
            });
        });
    }
    future<> read_from_index() {
        sstlog.trace("reader {}: read from index", fmt::ptr(this));
        auto tomb = _index_reader->partition_tombstone();
        if (!tomb) {
            sstlog.trace("reader {}: no tombstone", fmt::ptr(this));
            return read_from_datafile();
        }
        auto pk = _index_reader->partition_key().to_partition_key(*_schema);
        auto key = dht::decorate_key(*_schema, std::move(pk));
        _consumer.setup_for_partition(key.key());
        on_next_partition(std::move(key), tombstone(*tomb));
        return make_ready_future<>();
    }
    future<> read_from_datafile() {
        sstlog.trace("reader {}: read from data file", fmt::ptr(this));
        return _context->consume_input();
    }
    // Assumes that we're currently positioned at partition boundary.
    future<> read_partition() {
        sstlog.trace("reader {}: reading partition", fmt::ptr(this));

        _end_of_stream = true; // on_next_partition() will set it to true
        if (!_read_enabled) {
            sstlog.trace("reader {}: eof", fmt::ptr(this));
            return make_ready_future<>();
        }

        if (!_consumer.is_mutation_end()) {
            throw malformed_sstable_exception(format("consumer not at partition boundary, position: {}",
                    position_in_partition_view::printer(*_schema, _consumer.position())), _sst->get_filename());
        }

        // It's better to obtain partition information from the index if we already have it.
        // We can save on IO if the user will skip past the front of partition immediately.
        //
        // It is also better to pay the cost of reading the index if we know that we will
        // need to use the index anyway soon.
        //
        if (_index_in_current_partition) {
            if (_context->eof()) {
                sstlog.trace("reader {}: eof", fmt::ptr(this));
                return make_ready_future<>();
            }
            if (_index_reader->partition_data_ready()) {
                return read_from_index();
            }
            if (_will_likely_slice) {
                return _index_reader->read_partition_data().then([this] {
                    return read_from_index();
                });
            }
        }

        // FIXME: advance index to current partition if _will_likely_slice
        return read_from_datafile();
    }
    // Can be called from any position.
    future<> read_next_partition() {
        sstlog.trace("reader {}: read next partition", fmt::ptr(this));
        // If next partition exists then on_next_partition will be called
        // and _end_of_stream will be set to false again.
        _end_of_stream = true;
        if (!_read_enabled || _single_partition_read) {
            sstlog.trace("reader {}: eof", fmt::ptr(this));
            return make_ready_future<>();
        }
        return advance_to_next_partition().then([this] {
            return read_partition();
        });
    }
    future<> advance_context(std::optional<position_in_partition_view> pos) {
        if (!pos || pos->is_before_all_fragments(*_schema)) {
            return make_ready_future<>();
        }
        assert (_current_partition_key);
        return [this] {
            if (!_index_in_current_partition) {
                _index_in_current_partition = true;
                return get_index_reader().advance_to(*_current_partition_key);
            }
            return make_ready_future();
        }().then([this, pos] {
            return get_index_reader().advance_to(*pos).then([this] {
                index_reader& idx = *_index_reader;
                auto index_position = idx.data_file_positions();
                if (index_position.start <= _context->position()) {
                    return make_ready_future<>();
                }
                return skip_to(idx.element_kind(), index_position.start).then([this, &idx] {
                    _sst->get_stats().on_partition_seek();
                    set_range_tombstone_start_from_end_open_marker(_consumer, *_schema, idx);
                });
            });
        });
    }
    bool is_initialized() const {
        return bool(_context);
    }
    future<> initialize() {
        if (_single_partition_read) {
            _sst->get_stats().on_single_partition_read();
            const auto& key = dht::ring_position_view(_pr.start()->value());
            position_in_partition_view pos = get_slice_upper_bound(*_schema, _slice, key);
            const auto present = co_await get_index_reader().advance_lower_and_check_if_present(key, pos);

            if (!present) {
                _sst->get_filter_tracker().add_false_positive();
                co_return;
            }

            _sst->get_filter_tracker().add_true_positive();
        } else {
            _sst->get_stats().on_range_partition_read();
            co_await get_index_reader().advance_to(_pr);
        }

        auto [begin, end] = _index_reader->data_file_positions();
        assert(end);

        if (_single_partition_read) {
            _read_enabled = (begin != *end);
            _context = data_consume_single_partition<DataConsumeRowsContext>(*_schema, _sst, _consumer, { begin, *end });
        } else {
            sstable::disk_read_range drr{begin, *end};
            auto last_end = _fwd_mr ? _sst->data_size() : drr.end;
            _read_enabled = bool(drr);
            _context = data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer, std::move(drr), last_end);
        }

        _monitor.on_read_started(_context->reader_position());
        _index_in_current_partition = true;
        _will_likely_slice = will_likely_slice(_slice);
    }
    future<> ensure_initialized() {
        if (is_initialized()) {
            return make_ready_future<>();
        }
        return initialize();
    }
    future<> skip_to(indexable_element el, uint64_t begin) {
        sstlog.trace("sstable_reader: {}: skip_to({} -> {}, el={})", fmt::ptr(_context.get()), _context->position(), begin, static_cast<int>(el));
        if (begin <= _context->position()) {
            return make_ready_future<>();
        }
        _context->reset(el);
        return _context->skip_to(begin);
    }
public:
    void on_out_of_clustering_range() override {
        if (_fwd == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            this->push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end()));
            _partition_finished = true;
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        return ensure_initialized().then([this, &pr] {
            if (!is_initialized()) {
                _end_of_stream = true;
                return make_ready_future<>();
            } else {
                clear_buffer();
                _partition_finished = true;
                _before_partition = true;
                _end_of_stream = false;
                assert(_index_reader);
                auto f1 = _index_reader->advance_to(pr);
                return f1.then([this] {
                    auto [start, end] = _index_reader->data_file_positions();
                    assert(end);
                    if (start != *end) {
                        _read_enabled = true;
                        _index_in_current_partition = true;
                        _context->reset(indexable_element::partition);
                        return _context->fast_forward_to(start, *end);
                    }
                    _index_in_current_partition = false;
                    _read_enabled = false;
                    return make_ready_future<>();
                });
            }
        });
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        if (_end_of_stream) {
            return make_ready_future<>();
        }
        if (!is_initialized()) {
            return initialize().then([this, timeout] {
                if (!is_initialized()) {
                    _end_of_stream = true;
                    return make_ready_future<>();
                } else {
                    return fill_buffer(timeout);
                }
            });
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (_partition_finished) {
                if (_before_partition) {
                    return read_partition();
                } else {
                    return read_next_partition();
                }
            } else {
                return do_until([this] { return is_buffer_full() || _partition_finished || _end_of_stream; }, [this] {
                    _consumer.push_ready_fragments();
                    if (is_buffer_full() || _partition_finished || _end_of_stream) {
                        return make_ready_future<>();
                    }
                    return advance_context(_consumer.maybe_skip()).then([this] {
                        return _context->consume_input();
                    });
                });
            }
        });
    }
    virtual future<> next_partition() override {
        if (is_initialized()) {
            if (_fwd == streamed_mutation::forwarding::yes) {
                clear_buffer();
                _partition_finished = true;
                _end_of_stream = false;
            } else {
                clear_buffer_to_next_partition();
                if (!_partition_finished && is_buffer_empty()) {
                    _partition_finished = true;
                }
            }
        }
        return make_ready_future<>();
        // If _ds is not created then next_partition() has no effect because there was no partition_start emitted yet.
    }
    virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override {
        forward_buffer_to(cr.start());
        if (!_partition_finished) {
            _end_of_stream = false;
            return advance_context(_consumer.fast_forward_to(std::move(cr), timeout));
        } else {
            _end_of_stream = true;
            return make_ready_future<>();
        }
    }
    virtual future<> close() noexcept override {
        auto close_context = make_ready_future<>();
        if (_context) {
            _monitor.on_read_completed();
            // move _context to prevent double-close from destructor.
            close_context = _context->close().finally([_ = std::move(_context)] {});
        }

        auto close_index_reader = make_ready_future<>();
        if (_index_reader) {
            // move _index_reader to prevent double-close from destructor.
            close_index_reader = _index_reader->close().finally([_ = std::move(_index_reader)] {});
        }

        return when_all_succeed(std::move(close_context), std::move(close_index_reader)).discard_result().handle_exception([] (std::exception_ptr ep) {
            // close can not fail as it is called either from the destructor or from flat_mutation_reader::close
            sstlog.warn("Failed closing of sstable_mutation_reader: {}. Ignored since the reader is already done.", ep);
        });
    }
};

}
