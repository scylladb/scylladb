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
#include "mutation.hh"
#include "sstables.hh"
#include "types.hh"
#include <seastar/core/future-util.hh>
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
#include "data_consume_context.hh"
#include "mp_row_consumer.hh"

namespace sstables {

template
data_consume_context<data_consume_rows_context>
data_consume_rows<data_consume_rows_context>(const schema& s, shared_sstable, data_consume_rows_context::consumer&, sstable::disk_read_range, uint64_t);

template
data_consume_context<data_consume_rows_context>
data_consume_single_partition<data_consume_rows_context>(const schema& s, shared_sstable, data_consume_rows_context::consumer&, sstable::disk_read_range);

template
data_consume_context<data_consume_rows_context>
data_consume_rows<data_consume_rows_context>(const schema& s, shared_sstable, data_consume_rows_context::consumer&);

template
data_consume_context<data_consume_rows_context_m>
data_consume_rows<data_consume_rows_context_m>(const schema& s, shared_sstable, data_consume_rows_context_m::consumer&, sstable::disk_read_range, uint64_t);

template
data_consume_context<data_consume_rows_context_m>
data_consume_single_partition<data_consume_rows_context_m>(const schema& s, shared_sstable, data_consume_rows_context_m::consumer&, sstable::disk_read_range);

template
data_consume_context<data_consume_rows_context_m>
data_consume_rows<data_consume_rows_context_m>(const schema& s, shared_sstable, data_consume_rows_context_m::consumer&);

static
position_in_partition_view get_slice_upper_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key) {
    const auto& ranges = slice.row_ranges(s, *key.key());
    if (ranges.empty()) {
        return position_in_partition_view::for_static_row();
    }
    if (slice.options.contains(query::partition_slice::option::reversed)) {
        return position_in_partition_view::for_range_end(ranges.front());
    }
    return position_in_partition_view::for_range_end(ranges.back());
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

template <typename DataConsumeRowsContext = data_consume_rows_context, typename Consumer = mp_row_consumer_k_l>
requires RowConsumer<Consumer>
class sstable_mutation_reader : public mp_row_consumer_reader {
    Consumer _consumer;
    bool _will_likely_slice = false;
    bool _read_enabled = true;
    data_consume_context_opt<DataConsumeRowsContext> _context;
    std::unique_ptr<index_reader> _index_reader;
    // We avoid unnecessary lookup for single partition reads thanks to this flag
    bool _single_partition_read = false;
    std::function<future<> ()> _initialize;
    streamed_mutation::forwarding _fwd;
    read_monitor& _monitor;
public:
    sstable_mutation_reader(shared_sstable sst, schema_ptr schema,
         reader_permit permit,
         const io_priority_class &pc,
         tracing::trace_state_ptr trace_state,
         streamed_mutation::forwarding fwd,
         read_monitor& mon)
        : mp_row_consumer_reader(std::move(schema), permit, std::move(sst))
        , _consumer(this, _schema, std::move(permit), _schema->full_slice(), pc, std::move(trace_state), fwd, _sst)
        , _initialize([this] {
            _context = data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer);
            _monitor.on_read_started(_context->reader_position());
            return make_ready_future<>();
        })
        , _fwd(fwd)
        , _monitor(mon) { }
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
        , _initialize([this, pr, &pc, &slice, fwd_mr] () mutable {
            auto f = get_index_reader().advance_to(pr);
            return f.then([this, &pc, &slice, fwd_mr] () mutable {
                auto [begin, end] = _index_reader->data_file_positions();
                assert(end);
                sstable::disk_read_range drr{begin, *end};
                auto last_end = fwd_mr ? _sst->data_size() : drr.end;
                _read_enabled = bool(drr);
                _context = data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer, std::move(drr), last_end);
                _monitor.on_read_started(_context->reader_position());
                _index_in_current_partition = true;
                _will_likely_slice = will_likely_slice(slice);
            });
        })
        , _fwd(fwd)
        , _monitor(mon) { }
    sstable_mutation_reader(shared_sstable sst,
                            schema_ptr schema,
                            reader_permit permit,
                            dht::ring_position_view key,
                            const query::partition_slice& slice,
                            const io_priority_class& pc,
                            tracing::trace_state_ptr trace_state,
                            streamed_mutation::forwarding fwd,
                            mutation_reader::forwarding fwd_mr,
                            read_monitor& mon)
        : mp_row_consumer_reader(std::move(schema), permit, std::move(sst))
        , _consumer(this, _schema, std::move(permit), slice, pc, std::move(trace_state), fwd, _sst)
        , _single_partition_read(true)
        , _initialize([this, key = std::move(key), &pc, &slice, fwd_mr] () mutable {
            position_in_partition_view pos = get_slice_upper_bound(*_schema, slice, key);
            auto f = get_index_reader().advance_lower_and_check_if_present(key, pos);
            return f.then([this, &slice, &pc, key] (bool present) mutable {
                if (!present) {
                    _sst->get_filter_tracker().add_false_positive();
                    return make_ready_future<>();
                }

                _sst->get_filter_tracker().add_true_positive();

                auto [start, end] = _index_reader->data_file_positions();
                assert(end);
                _read_enabled = (start != *end);
                _context = data_consume_single_partition<DataConsumeRowsContext>(*_schema, _sst, _consumer,
                        { start, *end });
                _monitor.on_read_started(_context->reader_position());
                _will_likely_slice = will_likely_slice(slice);
                _index_in_current_partition = true;
                return make_ready_future<>();
            });
        })
        , _fwd(fwd)
        , _monitor(mon) { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    sstable_mutation_reader(sstable_mutation_reader&&) = delete;
    sstable_mutation_reader(const sstable_mutation_reader&) = delete;
    ~sstable_mutation_reader() {
        _monitor.on_read_completed();
        auto close = [this] (std::unique_ptr<index_reader>& ptr) {
            if (ptr) {
                auto f = ptr->close();
                // FIXME: discarded future.
                (void)f.handle_exception([index = std::move(ptr)] (auto&&) { });
            }
        };
        close(_index_reader);
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
            return _context->skip_to(_index_reader->element_kind(), start).then([this] {
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
        return _context->read();
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
                if (!_context->need_skip(index_position.start)) {
                    return make_ready_future<>();
                }
                return _context->skip_to(idx.element_kind(), index_position.start).then([this, &idx] {
                    _sst->get_stats().on_partition_seek();
                    set_range_tombstone_start_from_end_open_marker(_consumer, *_schema, idx);
                });
            });
        });
    }
    bool is_initialized() const {
        return bool(_context);
    }
    future<> ensure_initialized() {
        if (is_initialized()) {
            return make_ready_future<>();
        }
        return _initialize();
    }
public:
    void on_out_of_clustering_range() override {
        if (_fwd == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            this->push_mutation_fragment(mutation_fragment(partition_end()));
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
            return _initialize().then([this, timeout] {
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
                        return _context->read();
                    });
                });
            }
        });
    }
    virtual void next_partition() override {
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
};

void mp_row_consumer_reader::on_next_partition(dht::decorated_key key, tombstone tomb) {
    _partition_finished = false;
    _before_partition = false;
    _end_of_stream = false;
    _current_partition_key = std::move(key);
    push_mutation_fragment(
        mutation_fragment(partition_start(*_current_partition_key, tomb)));
    _sst->get_stats().on_partition_read();
}

flat_mutation_reader sstable::read_rows_flat(schema_ptr schema, reader_permit permit, const io_priority_class& pc,
        streamed_mutation::forwarding fwd) {
    get_stats().on_sstable_partition_read();
    if (_version >= version_types::mc) {
        return make_flat_mutation_reader<sstable_mutation_reader<data_consume_rows_context_m, mp_row_consumer_m>>(
            shared_from_this(), std::move(schema), std::move(permit), pc, tracing::trace_state_ptr(), fwd, default_read_monitor());
    }
    return make_flat_mutation_reader<sstable_mutation_reader<>>(shared_from_this(), std::move(schema), std::move(permit), pc,
            tracing::trace_state_ptr(), fwd, default_read_monitor());
}

flat_mutation_reader
sstables::sstable::read_row_flat(schema_ptr schema,
                                 reader_permit permit,
                                 dht::ring_position_view key,
                                 const query::partition_slice& slice,
                                 const io_priority_class& pc,
                                 tracing::trace_state_ptr trace_state,
                                 streamed_mutation::forwarding fwd,
                                 read_monitor& mon)
{
    get_stats().on_single_partition_read();
    if (_version >= version_types::mc) {
        return make_flat_mutation_reader<sstable_mutation_reader<data_consume_rows_context_m, mp_row_consumer_m>>(
            shared_from_this(), std::move(schema), std::move(permit), std::move(key), slice, pc,
            std::move(trace_state), fwd, mutation_reader::forwarding::no, mon);
    }
    return make_flat_mutation_reader<sstable_mutation_reader<>>(shared_from_this(), std::move(schema), std::move(permit), std::move(key), slice, pc,
            std::move(trace_state), fwd, mutation_reader::forwarding::no, mon);
}

flat_mutation_reader
sstable::read_range_rows_flat(schema_ptr schema,
                         reader_permit permit,
                         const dht::partition_range& range,
                         const query::partition_slice& slice,
                         const io_priority_class& pc,
                         tracing::trace_state_ptr trace_state,
                         streamed_mutation::forwarding fwd,
                         mutation_reader::forwarding fwd_mr,
                         read_monitor& mon) {
    get_stats().on_range_partition_read();
    if (_version >= version_types::mc) {
        return make_flat_mutation_reader<sstable_mutation_reader<data_consume_rows_context_m, mp_row_consumer_m>>(
            shared_from_this(), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, mon);
    }
    return make_flat_mutation_reader<sstable_mutation_reader<>>(
        shared_from_this(), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, mon);
}

}
