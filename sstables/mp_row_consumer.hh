/*
 * Copyright (C) 2018 ScyllaDB
 *
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


#include <variant>
#include "flat_mutation_reader.hh"
#include "timestamp.hh"
#include "gc_clock.hh"
#include "mutation_fragment.hh"
#include "schema_fwd.hh"
#include "row.hh"
#include "clustering_ranges_walker.hh"
#include "utils/data_input.hh"
#include "utils/overloaded_functor.hh"
#include "liveness_info.hh"
#include "mutation_fragment_filter.hh"
#include "types.hh"
#include "keys.hh"
#include "clustering_bounds_comparator.hh"
#include "range_tombstone.hh"
#include "types/user.hh"
#include "concrete_types.hh"

namespace sstables {

namespace kl {
    class mp_row_consumer_k_l;
}

class mp_row_consumer_reader : public flat_mutation_reader::impl {
    friend class sstables::kl::mp_row_consumer_k_l;
    friend class mp_row_consumer_m;
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

struct new_mutation {
    partition_key key;
    tombstone tomb;
};

inline atomic_cell make_atomic_cell(const abstract_type& type,
                                    api::timestamp_type timestamp,
                                    bytes_view value,
                                    gc_clock::duration ttl,
                                    gc_clock::time_point expiration,
                                    atomic_cell::collection_member cm) {
    if (ttl != gc_clock::duration::zero()) {
        return atomic_cell::make_live(type, timestamp, value, expiration, ttl, cm);
    } else {
        return atomic_cell::make_live(type, timestamp, value, cm);
    }
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, bytes_view value);

class mp_row_consumer_m : public consumer_m {
    mp_row_consumer_reader* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    std::optional<mutation_fragment_filter> _mf_filter;

    bool _is_mutation_end = true;
    streamed_mutation::forwarding _fwd;
    // For static-compact tables C* stores the only row in the static row but in our representation they're regular rows.
    const bool _treat_static_row_as_regular;

    std::optional<clustering_row> _in_progress_row;
    std::optional<range_tombstone> _stored_tombstone;
    static_row _in_progress_static_row;
    bool _inside_static_row = false;

    struct cell {
        column_id id;
        atomic_cell_or_collection val;
    };
    std::vector<cell> _cells;
    collection_mutation_description _cm;

    struct range_tombstone_start {
        clustering_key_prefix ck;
        bound_kind kind;
        tombstone tomb;

        position_in_partition_view position() const {
            return position_in_partition_view(position_in_partition_view::range_tag_t{}, bound_view(ck, kind));
        }
    };

    inline friend std::ostream& operator<<(std::ostream& o, const sstables::mp_row_consumer_m::range_tombstone_start& rt_start) {
        o << "{ clustering: " << rt_start.ck
          << ", kind: " << rt_start.kind
          << ", tombstone: " << rt_start.tomb << " }";
        return o;
    }

    std::optional<range_tombstone_start> _opened_range_tombstone;

    void consume_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_start(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}",
                           *_opened_range_tombstone, t));
        }
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    proceed consume_range_tombstone_end(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_end(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (!_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Closing range tombstone that wasn't opened: clustering {}, kind {}, tombstone {}",
                           ck, k, t));
        }
        if (_opened_range_tombstone->tomb != t) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstone with ck {} and two different tombstones at ends: {}, {}",
                           ck, _opened_range_tombstone->tomb, t));
        }


        auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                            _opened_range_tombstone->kind,
                            std::move(ck),
                            k,
                            std::move(t)};
        _opened_range_tombstone.reset();
        return maybe_push_range_tombstone(std::move(rt));
    }

    const column_definition& get_column_definition(std::optional<column_id> column_id) const {
        auto column_type = _inside_static_row ? column_kind::static_column : column_kind::regular_column;
        return _schema->column_at(column_type, *column_id);
    }

    inline proceed maybe_push_range_tombstone(range_tombstone&& rt) {
        const auto action = _mf_filter->apply(rt);
        switch (action) {
        case mutation_fragment_filter::result::emit:
            _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(rt)));
            break;
        case mutation_fragment_filter::result::ignore:
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                return proceed::no;
            }
            if (_mf_filter->is_current_range_changed()) {
                return proceed::no;
            }
            break;
        case mutation_fragment_filter::result::store_and_finish:
            _stored_tombstone = std::move(rt);
            _reader->on_out_of_clustering_range();
            return proceed::no;
        }

        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    inline void reset_for_new_partition() {
        _is_mutation_end = true;
        _in_progress_row.reset();
        _stored_tombstone.reset();
        _mf_filter.reset();
        _opened_range_tombstone.reset();
    }

    void check_schema_mismatch(const column_translation::column_info& column_info, const column_definition& column_def) const {
        if (column_info.schema_mismatch) {
            throw malformed_sstable_exception(
                    format("{} definition in serialization header does not match schema. Expected {} but got {}",
                        column_def.name_as_text(),
                        column_def.type->name(),
                        column_info.type->name()));
        }
    }

    void check_column_missing_in_current_schema(const column_translation::column_info& column_info,
                                                api::timestamp_type timestamp) const {
        if (!column_info.id) {
            sstring name = sstring(to_sstring_view(*column_info.name));
            auto it = _schema->dropped_columns().find(name);
            if (it == _schema->dropped_columns().end() || timestamp > it->second.timestamp) {
                throw malformed_sstable_exception(format("Column {} missing in current schema", name));
            }
        }
    }

public:

    /*
     * In m format, RTs are represented as separate start and end bounds,
     * so setting/resetting RT start is needed so that we could skip using index.
     * For this, the following methods need to be defined:
     *
     * void set_range_tombstone_start(clustering_key_prefix, bound_kind, tombstone);
     * void reset_range_tombstone_start();
     */
    constexpr static bool is_setting_range_tombstone_start_supported = true;

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : consumer_m(std::move(permit), std::move(trace_state), pc)
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _treat_static_row_as_regular(_schema->is_static_compact_table()
            && (!sst->has_scylla_component() || sst->features().is_enabled(sstable_feature::CorrectStaticCompact))) // See #4139
    {
        _cells.reserve(std::max(_schema->static_columns_count(), _schema->regular_columns_count()));
    }

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
    : mp_row_consumer_m(reader, schema, std::move(permit), schema->full_slice(), pc, std::move(trace_state), fwd, sst)
    { }

    virtual ~mp_row_consumer_m() {}

    // See the RowConsumer concept
    void push_ready_fragments() {
        auto maybe_push = [this] (auto&& mfopt) {
            if (mfopt) {
                assert(_mf_filter);
                switch (_mf_filter->apply(*mfopt)) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), *std::exchange(mfopt, {})));
                    break;
                case mutation_fragment_filter::result::ignore:
                    mfopt.reset();
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    _reader->on_out_of_clustering_range();
                    break;
                }
            }
        };

        maybe_push(_stored_tombstone);
    }

    std::optional<position_in_partition_view> maybe_skip() {
        if (!_mf_filter) {
            return {};
        }
        return _mf_filter->maybe_skip();
    }

    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    void setup_for_partition(const partition_key& pk) {
        sstlog.trace("mp_row_consumer_m {}: setup_for_partition({})", fmt::ptr(this), pk);
        _is_mutation_end = false;
        _mf_filter.emplace(*_schema, _slice, pk, _fwd);
    }

    std::optional<position_in_partition_view> fast_forward_to(position_range r, db::timeout_clock::time_point) {
        if (!_mf_filter) {
            _reader->on_out_of_clustering_range();
            return {};
        }
        auto skip = _mf_filter->fast_forward_to(std::move(r));
        if (skip) {
            position_in_partition::less_compare less(*_schema);
            // No need to skip using index if stored fragments are after the start of the range
            if (_in_progress_row && !less(_in_progress_row->position(), *skip)) {
                return {};
            }
            if (_stored_tombstone && !less(_stored_tombstone->position(), *skip)) {
                return {};
            }
        }
        if (_mf_filter->out_of_range()) {
            _reader->on_out_of_clustering_range();
        }
        return skip;
    }

    /*
     * Sets the range tombstone start. Overwrites the currently set RT start if any.
     * Used for skipping through wide partitions using index when the data block
     * skipped to starts in the middle of an opened range tombstone.
     */
    void set_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    /*
     * Resets the previously set range tombstone start if any.
     */
    void reset_range_tombstone_start() {
        _opened_range_tombstone.reset();
    }

    virtual proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_start(deltime=({}, {})), _is_mutation_end={}", fmt::ptr(this),
            deltime.local_deletion_time, deltime.marked_for_delete_at, _is_mutation_end);
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        auto pk = partition_key::from_exploded(key.explode(*_schema));
        setup_for_partition(pk);
        auto dk = dht::decorate_key(*_schema, pk);
        _reader->on_next_partition(std::move(dk), tombstone(deltime));
        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual consumer_m::row_processing_result consume_row_start(const std::vector<temporary_buffer<char>>& ecp) override {
        auto key = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));

        sstlog.trace("mp_row_consumer_m {}: consume_row_start({})", fmt::ptr(this), key);

        // enagaged _in_progress_row means we have already split around this key.
        if (_opened_range_tombstone && !_in_progress_row) {
            // We have an opened range tombstone which means that the current row is spanned by that RT.
            auto ck = key;
            bool was_non_full_key = clustering_key::make_full(*_schema, ck);
            auto end_kind = was_non_full_key ? bound_kind::excl_end : bound_kind::incl_end;
            assert(!_stored_tombstone);
            auto rt = range_tombstone(std::move(_opened_range_tombstone->ck),
                _opened_range_tombstone->kind,
                ck,
                end_kind,
                _opened_range_tombstone->tomb);
            sstlog.trace("mp_row_consumer_m {}: push({})", fmt::ptr(this), rt);
            _opened_range_tombstone->ck = std::move(ck);
            _opened_range_tombstone->kind = was_non_full_key ? bound_kind::incl_start : bound_kind::excl_start;

            if (maybe_push_range_tombstone(std::move(rt)) == proceed::no) {
                _in_progress_row.emplace(std::move(key));
                return consumer_m::row_processing_result::retry_later;
            }
        }

        _in_progress_row.emplace(std::move(key));

        switch (_mf_filter->apply(_in_progress_row->position())) {
        case mutation_fragment_filter::result::emit:
            sstlog.trace("mp_row_consumer_m {}: emit", fmt::ptr(this));
            return consumer_m::row_processing_result::do_proceed;
        case mutation_fragment_filter::result::ignore:
            sstlog.trace("mp_row_consumer_m {}: ignore", fmt::ptr(this));
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                // We actually want skip_later, which doesn't exist, but retry_later
                // is ok because signalling out-of-range on the reader will cause it
                // to either stop reading or skip to the next partition using index,
                // not by ignoring fragments.
                return consumer_m::row_processing_result::retry_later;
            }
            if (_mf_filter->is_current_range_changed()) {
                return consumer_m::row_processing_result::retry_later;
            } else {
                _in_progress_row.reset();
                return consumer_m::row_processing_result::skip_row;
            }
        case mutation_fragment_filter::result::store_and_finish:
            sstlog.trace("mp_row_consumer_m {}: store_and_finish", fmt::ptr(this));
            _reader->on_out_of_clustering_range();
            return consumer_m::row_processing_result::retry_later;
        }
        abort();
    }

    virtual proceed consume_row_marker_and_tombstone(
            const liveness_info& info, tombstone tomb, tombstone shadowable_tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_row_marker_and_tombstone({}, {}, {}), key={}",
            fmt::ptr(this), info.to_row_marker(), tomb, shadowable_tomb, _in_progress_row->position());
        _in_progress_row->apply(info.to_row_marker());
        _in_progress_row->apply(tomb);
        if (shadowable_tomb) {
            _in_progress_row->apply(shadowable_tombstone{shadowable_tomb});
        }
        return proceed::yes;
    }

    virtual consumer_m::row_processing_result consume_static_row_start() override {
        sstlog.trace("mp_row_consumer_m {}: consume_static_row_start()", fmt::ptr(this));
        if (_treat_static_row_as_regular) {
            return consume_row_start({});
        }
        _inside_static_row = true;
        _in_progress_static_row = static_row();
        return consumer_m::row_processing_result::do_proceed;
    }

    virtual proceed consume_column(const column_translation::column_info& column_info,
                                   bytes_view cell_path,
                                   bytes_view value,
                                   api::timestamp_type timestamp,
                                   gc_clock::duration ttl,
                                   gc_clock::time_point local_deletion_time,
                                   bool is_deleted) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_column(id={}, path={}, value={}, ts={}, ttl={}, del_time={}, deleted={})", fmt::ptr(this),
            column_id, fmt_hex(cell_path), fmt_hex(value), timestamp, ttl.count(), local_deletion_time.time_since_epoch().count(), is_deleted);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        if (column_def.is_multi_cell()) {
            auto& value_type = visit(*column_def.type, make_visitor(
                [] (const collection_type_impl& ctype) -> const abstract_type& { return *ctype.value_comparator(); },
                [&] (const user_type_impl& utype) -> const abstract_type& {
                    if (cell_path.size() != sizeof(int16_t)) {
                        throw malformed_sstable_exception(format("wrong size of field index while reading UDT column: expected {}, got {}",
                                    sizeof(int16_t), cell_path.size()));
                    }

                    auto field_idx = deserialize_field_index(cell_path);
                    if (field_idx >= utype.size()) {
                        throw malformed_sstable_exception(format("field index too big while reading UDT column: type has {} fields, got {}",
                                    utype.size(), field_idx));
                    }

                    return *utype.type(field_idx);
                },
                [] (const abstract_type& o) -> const abstract_type& {
                    throw malformed_sstable_exception(format("attempted to read multi-cell column, but expected type was {}", o.name()));
                }
            ));
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(value_type,
                                                    timestamp,
                                                    value,
                                                    ttl,
                                                    local_deletion_time,
                                                    atomic_cell::collection_member::yes);
            _cm.cells.emplace_back(to_bytes(cell_path), std::move(ac));
        } else {
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(*column_def.type, timestamp, value, ttl, local_deletion_time,
                                       atomic_cell::collection_member::no);
            _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        }
        return proceed::yes;
    }

    virtual proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info,
                                                 tombstone tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_start({}, {})", fmt::ptr(this), column_info.id, tomb);
        _cm.tomb = tomb;
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_end({})", fmt::ptr(this), column_id);
        if (_cm.tomb) {
            check_column_missing_in_current_schema(column_info, _cm.tomb.timestamp);
        }
        if (column_id) {
            const column_definition& column_def = get_column_definition(column_id);
            if (!_cm.cells.empty() || (_cm.tomb && _cm.tomb.timestamp > column_def.dropped_at())) {
                check_schema_mismatch(column_info, column_def);
                _cells.push_back({column_def.id, _cm.serialize(*column_def.type)});
            }
        }
        _cm.tomb = {};
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_counter_column(const column_translation::column_info& column_info,
                                           bytes_view value,
                                           api::timestamp_type timestamp) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_counter_column({}, {}, {})", fmt::ptr(this), column_id, fmt_hex(value), timestamp);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        auto ac = make_counter_cell(timestamp, value);
        _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        return proceed::yes;
    }

    virtual proceed consume_range_tombstone(const std::vector<temporary_buffer<char>>& ecp,
                                            bound_kind kind,
                                            tombstone tomb) override {
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));
        if (kind == bound_kind::incl_start || kind == bound_kind::excl_start) {
            consume_range_tombstone_start(std::move(ck), kind, std::move(tomb));
            return proceed(!_reader->is_buffer_full() && !need_preempt());
        } else { // *_end kind
            return consume_range_tombstone_end(std::move(ck), kind, std::move(tomb));
        }
    }

    virtual proceed consume_range_tombstone(const std::vector<temporary_buffer<char>>& ecp,
                                            sstables::bound_kind_m kind,
                                            tombstone end_tombstone,
                                            tombstone start_tombstone) override {
        auto result = proceed::yes;
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));
        switch (kind) {
        case bound_kind_m::incl_end_excl_start:
            result = consume_range_tombstone_end(ck, bound_kind::incl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::excl_start, std::move(start_tombstone));
            break;
        case bound_kind_m::excl_end_incl_start:
            result = consume_range_tombstone_end(ck, bound_kind::excl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::incl_start, std::move(start_tombstone));
            break;
        default:
            assert(false && "Invalid boundary type");
        }

        return result;
    }

    virtual proceed consume_row_end() override {
        auto fill_cells = [this] (column_kind kind, row& cells) {
            for (auto &&c : _cells) {
                cells.apply(_schema->column_at(kind, c.id), std::move(c.val));
            }
            _cells.clear();
        };

        if (_inside_static_row) {
            fill_cells(column_kind::static_column, _in_progress_static_row.cells());
            sstlog.trace("mp_row_consumer_m {}: consume_row_end(_in_progress_static_row={})", fmt::ptr(this), static_row::printer(*_schema, _in_progress_static_row));
            _inside_static_row = false;
            if (!_in_progress_static_row.empty()) {
                auto action = _mf_filter->apply(_in_progress_static_row);
                switch (action) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(_in_progress_static_row)));
                    break;
                case mutation_fragment_filter::result::ignore:
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    // static row is always either emited or ignored.
                    throw runtime_exception("We should never need to store static row");
                }
            }
        } else {
            if (!_cells.empty()) {
                fill_cells(column_kind::regular_column, _in_progress_row->cells());
            }
            _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), *std::exchange(_in_progress_row, {})));
        }

        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual void on_end_of_stream() override {
        sstlog.trace("mp_row_consumer_m {}: on_end_of_stream()", fmt::ptr(this));
        if (_opened_range_tombstone) {
            if (!_mf_filter || _mf_filter->out_of_range()) {
                throw sstables::malformed_sstable_exception("Unclosed range tombstone.");
            }
            auto range_end = _mf_filter->uppermost_bound();
            position_in_partition::less_compare less(*_schema);
            auto start_pos = position_in_partition_view(position_in_partition_view::range_tag_t{},
                                                        bound_view(_opened_range_tombstone->ck, _opened_range_tombstone->kind));
            if (less(start_pos, range_end)) {
                auto end_bound = range_end.is_clustering_row()
                    ? position_in_partition_view::after_key(range_end.key()).as_end_bound_view()
                    : range_end.as_end_bound_view();
                auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                                           _opened_range_tombstone->kind,
                                           end_bound.prefix(),
                                           end_bound.kind(),
                                           _opened_range_tombstone->tomb};
                sstlog.trace("mp_row_consumer_m {}: on_end_of_stream(), emitting last tombstone: {}", fmt::ptr(this), rt);
                _opened_range_tombstone.reset();
                _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(rt)));
            }
        }
        if (!_reader->_partition_finished) {
            consume_partition_end();
        }
        _reader->_end_of_stream = true;
    }

    virtual proceed consume_partition_end() override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_end()", fmt::ptr(this));
        reset_for_new_partition();

        if (_fwd == streamed_mutation::forwarding::yes) {
            _reader->_end_of_stream = true;
            return proceed::no;
        }

        _reader->_index_in_current_partition = false;
        _reader->_partition_finished = true;
        _reader->_before_partition = true;
        _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), partition_end()));
        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual void reset(sstables::indexable_element el) override {
        sstlog.trace("mp_row_consumer_m {}: reset({})", fmt::ptr(this), static_cast<int>(el));
        if (el == indexable_element::partition) {
            reset_for_new_partition();
        } else {
            _in_progress_row.reset();
            _is_mutation_end = false;
        }
    }

    virtual position_in_partition_view position() override {
        if (_inside_static_row) {
            return position_in_partition_view(position_in_partition_view::static_row_tag_t{});
        }
        if (_stored_tombstone) {
            return _stored_tombstone->position();
        }
        if (_in_progress_row) {
            return _in_progress_row->position();
        }
        if (_is_mutation_end) {
            return position_in_partition_view(position_in_partition_view::end_of_partition_tag_t{});
        }
        return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
    }
};

}
