/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "reader.hh"
#include "concrete_types.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "sstables/liveness_info.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "sstables/m_format_read_helpers.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "sstables/processing_result_generator.hh"
#include "utils/assert.hh"
#include "utils/to_string.hh"
#include "utils/value_or_reference.hh"

namespace sstables {
namespace mx {

class mp_row_consumer_reader_mx : public mp_row_consumer_reader_base, public mutation_reader::impl {
    friend class sstables::mx::mp_row_consumer_m;
public:
    mp_row_consumer_reader_mx(schema_ptr s, reader_permit permit, shared_sstable sst)
        : mp_row_consumer_reader_base(std::move(sst))
        , impl(std::move(s), std::move(permit))
    {
        _permit.on_start_sstable_read();
    }
    virtual ~mp_row_consumer_reader_mx() {
        _permit.on_finish_sstable_read();
    }

    void on_next_partition(dht::decorated_key, tombstone);
};

enum class row_processing_result {
    // Causes the parser to return the control to the caller without advancing.
    // Next time when the parser is called, the same consumer method will be called.
    retry_later,

    // Causes the parser to proceed to the next element.
    do_proceed,

    // Causes the parser to skip the whole row. consume_row_end() will not be called for the current row.
    skip_row
};

class mp_row_consumer_m {
    reader_permit _permit;
    const shared_sstable& _sst;
    tracing::trace_state_ptr _trace_state;
public:

    mp_row_consumer_reader_mx* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    std::optional<mutation_fragment_filter> _mf_filter;

    bool _is_mutation_end = true;
    streamed_mutation::forwarding _fwd;
    // For static-compact tables C* stores the only row in the static row but in our representation they're regular rows.
    const bool _treat_static_row_as_regular;

    std::optional<clustering_row> _in_progress_row;
    std::optional<range_tombstone_change> _stored_tombstone;
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

    inline friend std::ostream& operator<<(std::ostream& o, const mp_row_consumer_m::range_tombstone_start& rt_start) {
        fmt::print(o, "{{ clustering: {}, kind: {}, tombstone: {}}}",
                   rt_start.ck, rt_start.kind, rt_start.tomb);
        return o;
    }

    data_consumer::proceed consume_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_start(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (_mf_filter->current_tombstone()) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}",
                           _mf_filter->current_tombstone(), t));
        }
        auto pos = position_in_partition(position_in_partition::range_tag_t(), k, std::move(ck));
        return on_range_tombstone_change(std::move(pos), t);
    }

    data_consumer::proceed consume_range_tombstone_end(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_end(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (!_mf_filter->current_tombstone()) {
            throw sstables::malformed_sstable_exception(
                    format("Closing range tombstone that wasn't opened: clustering {}, kind {}, tombstone {}",
                           ck, k, t));
        }
        if (_mf_filter->current_tombstone() != t) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstone with ck {} and two different tombstones at ends: {}, {}",
                           ck, _mf_filter->current_tombstone(), t));
        }
        auto pos = position_in_partition(position_in_partition::range_tag_t(), k, std::move(ck));
        return on_range_tombstone_change(std::move(pos), {});
    }

    data_consumer::proceed consume_range_tombstone_boundary(position_in_partition pos, tombstone left, tombstone right) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_boundary(pos={}, left={}, right={})", fmt::ptr(this), pos, left, right);
        if (!_mf_filter->current_tombstone()) {
            throw sstables::malformed_sstable_exception(
                    format("Closing range tombstone that wasn't opened: pos {}, tombstone {}", pos, left));
        }
        if (_mf_filter->current_tombstone() != left) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstone at {} and two different tombstones at ends: {}, {}",
                           pos, _mf_filter->current_tombstone(), left));
        }
        return on_range_tombstone_change(std::move(pos), right);
    }

    const column_definition& get_column_definition(std::optional<column_id> column_id) const {
        auto column_type = _inside_static_row ? column_kind::static_column : column_kind::regular_column;
        return _schema->column_at(column_type, *column_id);
    }

    inline data_consumer::proceed on_range_tombstone_change(position_in_partition pos, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: on_range_tombstone_change({}, {}->{})", fmt::ptr(this), pos,
                     _mf_filter->current_tombstone(), t);

        mutation_fragment_filter::clustering_result result = _mf_filter->apply(pos, t);

        for (auto&& rt : result.rts) {
            sstlog.trace("mp_row_consumer_m {}: push({})", fmt::ptr(this), rt);
            _reader->push_mutation_fragment(mutation_fragment_v2(*_schema, permit(), std::move(rt)));
        }

        switch (result.action) {
        case mutation_fragment_filter::result::emit:
            sstlog.trace("mp_row_consumer_m {}: emit", fmt::ptr(this));
            break;
        case mutation_fragment_filter::result::ignore:
            sstlog.trace("mp_row_consumer_m {}: ignore", fmt::ptr(this));
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                return data_consumer::proceed::no;
            }
            if (_mf_filter->is_current_range_changed()) {
                return data_consumer::proceed::no;
            }
            break;
        case mutation_fragment_filter::result::store_and_finish:
            sstlog.trace("mp_row_consumer_m {}: store", fmt::ptr(this));
            _stored_tombstone = range_tombstone_change(pos, t);
            _reader->on_out_of_clustering_range();
            return data_consumer::proceed::no;
        }

        return data_consumer::proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    inline void reset_for_new_partition() {
        _is_mutation_end = true;
        _in_progress_row.reset();
        _stored_tombstone.reset();
        _mf_filter.reset();
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
    mp_row_consumer_m(mp_row_consumer_reader_mx* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const query::partition_slice& slice,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : _permit(std::move(permit))
        , _sst(sst)
        , _trace_state(std::move(trace_state))
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _treat_static_row_as_regular(_schema->is_static_compact_table()
            && (!sst->has_scylla_component() || sst->features().is_enabled(sstable_feature::CorrectStaticCompact))) // See #4139
    {
        _cells.reserve(std::max(_schema->static_columns_count(), _schema->regular_columns_count()));
    }

    mp_row_consumer_m(mp_row_consumer_reader_mx* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
    : mp_row_consumer_m(reader, schema, std::move(permit), schema->full_slice(), std::move(trace_state), fwd, sst)
    { }

    ~mp_row_consumer_m() {}

    // See the RowConsumer concept
    void push_ready_fragments() {
        if (auto rto = std::move(_stored_tombstone)) {
            _stored_tombstone = std::nullopt;
            on_range_tombstone_change(rto->position(), rto->tombstone());
        }
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
        _mf_filter.emplace(*_schema, query::clustering_key_filter_ranges(_slice.row_ranges(*_schema, pk)), _fwd);
    }

    std::optional<position_in_partition_view> fast_forward_to(position_range r) {
        if (!_mf_filter) {
            _reader->on_out_of_clustering_range();
            return {};
        }
        // r is used to trim range tombstones and range_tombstone:s can be trimmed only to positions
        // which are !is_clustering_row(). Replace with equivalent ranges.
        // Long-term we should guarantee this on position_range.
        if (r.start().is_clustering_row()) {
            r.set_start(position_in_partition::before_key(r.start().key()));
        }
        if (r.end().is_clustering_row()) {
            r.set_end(position_in_partition::before_key(r.end().key()));
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
    void set_range_tombstone(tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: set_range_tombstone({})", fmt::ptr(this), t);
        _mf_filter->set_tombstone(t);
    }

    // Consume the row's key and deletion_time. The latter determines if the
    // row is a tombstone, and if so, when it has been deleted.
    // Note that the key is in serialized form, and should be deserialized
    // (according to the schema) before use.
    // As explained above, the key object is only valid during this call, and
    // if the implementation wishes to save it, it must copy the *contents*.
    data_consumer::proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_start(deltime=({}, {})), _is_mutation_end={}", fmt::ptr(this),
            deltime.local_deletion_time, deltime.marked_for_delete_at, _is_mutation_end);
        if (!_is_mutation_end) {
            return data_consumer::proceed::yes;
        }
        auto pk = key.to_partition_key(*_schema);
        setup_for_partition(pk);
        auto dk = dht::decorate_key(*_schema, pk);
        _reader->on_next_partition(std::move(dk), tombstone(deltime));
        return data_consumer::proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    row_processing_result consume_row_start(const std::vector<fragmented_temporary_buffer>& ecp) {
        auto key = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));

        _sst->get_stats().on_row_read();
        sstlog.trace("mp_row_consumer_m {}: consume_row_start({})", fmt::ptr(this), key);

        _in_progress_row.emplace(std::move(key));

        mutation_fragment_filter::clustering_result res = _mf_filter->apply(_in_progress_row->position());

        for (auto&& rt : res.rts) {
            sstlog.trace("mp_row_consumer_m {}: push({})", fmt::ptr(this), rt);
            _reader->push_mutation_fragment(mutation_fragment_v2(*_schema, permit(), std::move(rt)));
        }

        switch (res.action) {
        case mutation_fragment_filter::result::emit:
            sstlog.trace("mp_row_consumer_m {}: emit", fmt::ptr(this));
            return row_processing_result::do_proceed;
        case mutation_fragment_filter::result::ignore:
            sstlog.trace("mp_row_consumer_m {}: ignore", fmt::ptr(this));
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                // We actually want skip_later, which doesn't exist, but retry_later
                // is ok because signalling out-of-range on the reader will cause it
                // to either stop reading or skip to the next partition using index,
                // not by ignoring fragments.
                return row_processing_result::retry_later;
            }
            if (_mf_filter->is_current_range_changed()) {
                return row_processing_result::retry_later;
            } else {
                _in_progress_row.reset();
                return row_processing_result::skip_row;
            }
        case mutation_fragment_filter::result::store_and_finish:
            sstlog.trace("mp_row_consumer_m {}: store_and_finish", fmt::ptr(this));
            _reader->on_out_of_clustering_range();
            return row_processing_result::retry_later;
        }
        abort();
    }

    data_consumer::proceed consume_row_marker_and_tombstone(
            const liveness_info& info, tombstone tomb, tombstone shadowable_tomb) {
        sstlog.trace("mp_row_consumer_m {}: consume_row_marker_and_tombstone({}, {}, {}), key={}",
            fmt::ptr(this), info.to_row_marker(), tomb, shadowable_tomb, _in_progress_row->position());
        _in_progress_row->apply(info.to_row_marker());
        _in_progress_row->apply(tomb);
        if (shadowable_tomb) {
            _in_progress_row->apply(shadowable_tombstone{shadowable_tomb});
        }
        if (_in_progress_row->tomb()) {
            _sst->get_stats().on_row_tombstone_read();
        }
        return data_consumer::proceed::yes;
    }

    row_processing_result consume_static_row_start() {
        sstlog.trace("mp_row_consumer_m {}: consume_static_row_start()", fmt::ptr(this));
        if (_treat_static_row_as_regular) {
            return consume_row_start({});
        }
        _inside_static_row = true;
        _in_progress_static_row = static_row();
        return row_processing_result::do_proceed;
    }

    data_consumer::proceed consume_column(const column_translation::column_info& column_info,
                                   bytes_view cell_path,
                                   fragmented_temporary_buffer::view value,
                                   api::timestamp_type timestamp,
                                   gc_clock::duration ttl,
                                   gc_clock::time_point local_deletion_time,
                                   bool is_deleted) {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_column(id={}, path={}, value={}, ts={}, ttl={}, del_time={}, deleted={})", fmt::ptr(this),
            column_id, fmt_hex(cell_path), value, timestamp, ttl.count(), local_deletion_time.time_since_epoch().count(), is_deleted);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return data_consumer::proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return data_consumer::proceed::yes;
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
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info,
                                                 tombstone tomb) {
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_start({}, {})", fmt::ptr(this), column_info.id, tomb);
        _cm.tomb = tomb;
        _cm.cells.clear();
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) {
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
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_counter_column(const column_translation::column_info& column_info,
                                           fragmented_temporary_buffer::view value,
                                           api::timestamp_type timestamp) {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_counter_column({}, {}, {})", fmt::ptr(this), column_id, value, timestamp);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return data_consumer::proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return data_consumer::proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        auto ac = make_counter_cell(timestamp, value);
        _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            bound_kind kind,
                                            tombstone tomb) {
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));
        if (kind == bound_kind::incl_start || kind == bound_kind::excl_start) {
            return consume_range_tombstone_start(std::move(ck), kind, std::move(tomb));
        } else { // *_end kind
            return consume_range_tombstone_end(std::move(ck), kind, std::move(tomb));
        }
    }

    data_consumer::proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            sstables::bound_kind_m kind,
                                            tombstone end_tombstone,
                                            tombstone start_tombstone) {
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));
        switch (kind) {
        case bound_kind_m::incl_end_excl_start: {
            auto pos = position_in_partition(position_in_partition::range_tag_t(), bound_kind::incl_end, std::move(ck));
            return consume_range_tombstone_boundary(std::move(pos), end_tombstone, start_tombstone);
        }
        case bound_kind_m::excl_end_incl_start: {
            auto pos = position_in_partition(position_in_partition::range_tag_t(), bound_kind::excl_end, std::move(ck));
            return consume_range_tombstone_boundary(std::move(pos), end_tombstone, start_tombstone);
        }
        default:
            SCYLLA_ASSERT(false && "Invalid boundary type");
        }
    }

    data_consumer::proceed consume_row_end() {
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
                    _reader->push_mutation_fragment(mutation_fragment_v2(*_schema, permit(), std::move(_in_progress_static_row)));
                    break;
                case mutation_fragment_filter::result::ignore:
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    // static row is always either emitted or ignored.
                    throw runtime_exception("We should never need to store static row");
                }
            }
        } else {
            if (!_cells.empty()) {
                fill_cells(column_kind::regular_column, _in_progress_row->cells());
            }
            if (_slice.is_reversed() &&
                    // we always consume whole rows (i.e. `consume_row_end` is always called) when reading in reverse,
                    // even when `consume_row_start` requested to ignore the row. This happens because for reversed reads
                    // skipping is performed in the intermediary reversing data source (not in the reader) and the source
                    // always returns whole rows.
                    // Hence we must again check what the filtering result for this row was, even though we already
                    // checked it in `consume_row_start`; otherwise we would incorrectly emit rows that were filtered out.
                    _mf_filter->apply(_in_progress_row->position()).action != mutation_fragment_filter::result::emit) {
                return data_consumer::proceed(!_reader->is_buffer_full() && !need_preempt());
            }
            _reader->push_mutation_fragment(mutation_fragment_v2(
                    *_schema, permit(), *std::exchange(_in_progress_row, {})));
        }

        return data_consumer::proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    void on_end_of_stream() {
        sstlog.trace("mp_row_consumer_m {}: on_end_of_stream()", fmt::ptr(this));
        if (_mf_filter && _mf_filter->current_tombstone()) {
            if (_mf_filter->out_of_range()) {
                throw sstables::malformed_sstable_exception("Unclosed range tombstone.");
            }
            auto result = _mf_filter->apply(position_in_partition_view::after_all_clustered_rows(), {});
            for (auto&& rt : result.rts) {
                sstlog.trace("mp_row_consumer_m {}: on_end_of_stream(), emitting last tombstone: {}", fmt::ptr(this), rt);
                _reader->push_mutation_fragment(mutation_fragment_v2(*_schema, permit(), std::move(rt)));
            }
        }
        if (!_reader->_partition_finished) {
            consume_partition_end();
        }
        _reader->_end_of_stream = true;
    }

    // Called at the end of the row, after all cells.
    // Returns a flag saying whether the sstable consumer should stop now, or
    // proceed consuming more data.
    data_consumer::proceed consume_partition_end() {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_end()", fmt::ptr(this));
        reset_for_new_partition();

        if (_fwd == streamed_mutation::forwarding::yes) {
            _reader->_end_of_stream = true;
            return data_consumer::proceed::no;
        }

        _reader->_index_in_current_partition = false;
        _reader->_partition_finished = true;
        _reader->_before_partition = true;
        _reader->push_mutation_fragment(mutation_fragment_v2(*_schema, permit(), partition_end()));
        return data_consumer::proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    // Called when the reader is fast forwarded to given element.
    void reset(sstables::indexable_element el) {
        sstlog.trace("mp_row_consumer_m {}: reset({})", fmt::ptr(this), static_cast<int>(el));
        if (el == indexable_element::partition) {
            reset_for_new_partition();
        } else {
            _in_progress_row.reset();
            _stored_tombstone.reset();
            _is_mutation_end = false;
        }
    }

    // Call after a reverse index skip is performed during reversed reads.
    void reset_after_reversed_read_skip() {
        // We must not reset `_in_progress_row` since rows are always consumed fully
        // during reversed reads. We also don't need to reset any state that may change
        // when moving between partitions as reversed skips are only performed within
        // a partition.
        // We must only reset the stored tombstone. A range tombstone may be stored in forwarding
        // mode, when the parser gets ahead of the currently forwarded-to range and provides
        // us (the consumer) a tombstone positioned after the range; we store it so we can
        // process it again when (if) the read gets forwarded to a range containing this
        // tombstone. But a successful index skip means that the source jumped to a later
        // position, so to a position past the stored tombstone's (if there is one) position.
        // The stored tombstone may no longer be relevant for the position we're at. The correct
        // active tombstone, if any, is obtained from the index and will be set using
        // `set_range_tombstone`.
        _stored_tombstone.reset();
    }

    position_in_partition_view position() {
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
            return position_in_partition_view::for_partition_end();
        }
        return position_in_partition_view::for_partition_start();
    }

    // The permit for this read
    reader_permit& permit() {
        return _permit;
    }

    tracing::trace_state_ptr trace_state() const {
        return _trace_state;
    }
};

// data_consume_rows_context_m remembers the context that an ongoing
// data_consume_rows() future is in for SSTable in 3_x format.
template <typename Consumer>
requires requires(
        Consumer& c,
        sstables::key_view pk_view,
        sstables::deletion_time deltime,
        const std::vector<fragmented_temporary_buffer>& ck_view,
        const liveness_info& l_info,
        tombstone tomb,
        const column_translation::column_info& column_info,
        bytes_view cell_path,
        fragmented_temporary_buffer::view value,
        api::timestamp_type timestamp,
        gc_clock::duration ttl,
        gc_clock::time_point local_deletion_time,
        bool is_deleted,
        bound_kind kind,
        sstables::bound_kind_m kind_m) {
    { c.permit() } -> std::convertible_to<reader_permit>;
    { c.trace_state() } -> std::same_as<tracing::trace_state_ptr>;
    { c.consume_partition_start(pk_view, deltime) } -> std::same_as<data_consumer::proceed>;
    { c.consume_static_row_start() } -> std::same_as<row_processing_result>;
    { c.consume_row_start(ck_view) } -> std::same_as<row_processing_result>;
    { c.consume_row_marker_and_tombstone(l_info, tomb, tomb) } -> std::same_as<data_consumer::proceed>;
    { c.consume_column(column_info, cell_path, value, timestamp, ttl, local_deletion_time, is_deleted) } -> std::same_as<data_consumer::proceed>;
    { c.consume_complex_column_start(column_info, tomb) } -> std::same_as<data_consumer::proceed>;
    { c.consume_complex_column_end(column_info) } -> std::same_as<data_consumer::proceed>;
    { c.consume_counter_column(column_info, value, timestamp) } -> std::same_as<data_consumer::proceed>;
    { c.consume_range_tombstone(ck_view, kind, tomb) } -> std::same_as<data_consumer::proceed>;
    { c.consume_range_tombstone(ck_view, kind_m, tomb, tomb) } -> std::same_as<data_consumer::proceed>;
    { c.consume_row_end() } -> std::same_as<data_consumer::proceed>;
    { c.consume_partition_end() } -> std::same_as<data_consumer::proceed>;
    c.on_end_of_stream();
}
class data_consume_rows_context_m : public data_consumer::continuous_data_consumer<data_consume_rows_context_m<Consumer>> {
    using parent = data_consumer::continuous_data_consumer<data_consume_rows_context_m<Consumer>>;
    using read_status = typename parent::read_status;
private:
    enum class state {
        PARTITION_START,
        DELETION_TIME,
        FLAGS,
        OTHER,
    } _state = state::PARTITION_START;

    // becomes false when we yield in the main coroutine, although we don't need to consume
    // more data buffers to continue, switch back to true afterwards
    bool _consuming = true;
    Consumer& _consumer;
    shared_sstable _sst;
    const serialization_header& _header;
    column_translation _column_translation;
    const bool _has_shadowable_tombstones;

    temporary_buffer<char> _pk;

    unfiltered_flags_m _flags{0};
    unfiltered_extended_flags_m _extended_flags{0};
    uint64_t _next_row_offset;
    liveness_info _liveness;
    bool _is_first_unfiltered = true;

    std::vector<fragmented_temporary_buffer> _row_key;

    struct row_schema {
        using column_range = boost::iterator_range<std::vector<column_translation::column_info>::const_iterator>;

        // All columns for this kind of row inside column_translation of the current sstable
        column_range _all_columns;

        // Subrange of _all_columns which is yet to be processed for current row
        column_range _columns;

        // Represents the subset of _all_columns present in current row
        boost::dynamic_bitset<uint64_t> _columns_selector; // size() == _columns.size()
    };

    row_schema _regular_row;
    row_schema _static_row;
    row_schema* _row;

    uint64_t _missing_columns_to_read;

    boost::iterator_range<std::vector<std::optional<uint32_t>>::const_iterator> _ck_column_value_fix_lengths;

    tombstone _row_tombstone;
    tombstone _row_shadowable_tombstone;

    column_flags_m _column_flags{0};
    api::timestamp_type _column_timestamp;
    gc_clock::time_point _column_local_deletion_time;
    gc_clock::duration _column_ttl;
    fragmented_temporary_buffer _column_value;
    temporary_buffer<char> _cell_path;
    uint64_t _ck_blocks_header;
    uint32_t _ck_blocks_header_offset;
    bool _null_component_occured;
    uint64_t _subcolumns_to_read = 0;
    api::timestamp_type _complex_column_marked_for_delete;
    tombstone _complex_column_tombstone;
    bool _reading_range_tombstone_ck = false;
    bound_kind_m _range_tombstone_kind;
    uint16_t _ck_size;
    /*
     * We need two range tombstones because range tombstone marker can be either a single bound
     * or a double bound that represents end of one range tombstone and start of another at the same time.
     * If range tombstone marker is a single bound then only _left_range_tombstone is used.
     * Otherwise, _left_range_tombstone represents tombstone for a range tombstone that's being closed
     * and _right_range_tombstone represents a tombstone for a range tombstone that's being opened.
     */
    tombstone _left_range_tombstone;
    tombstone _right_range_tombstone;

    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;
    void start_row(row_schema& rs) {
        _row = &rs;
        _row->_columns = _row->_all_columns;
    }
    void setup_columns(row_schema& rs, const std::vector<column_translation::column_info>& columns) {
        rs._all_columns = boost::make_iterator_range(columns);
        rs._columns_selector = boost::dynamic_bitset<uint64_t>(columns.size());
    }
    void skip_absent_columns() {
        size_t pos = _row->_columns_selector.find_first();
        if (pos == boost::dynamic_bitset<uint64_t>::npos) {
            pos = _row->_columns.size();
        }
        _row->_columns.advance_begin(pos);
    }
    bool no_more_columns() const { return _row->_columns.empty(); }
    void move_to_next_column() {
        size_t current_pos = _row->_columns_selector.size() - _row->_columns.size();
        size_t next_pos = _row->_columns_selector.find_next(current_pos);
        size_t jump_to_next = (next_pos == boost::dynamic_bitset<uint64_t>::npos) ? _row->_columns.size()
                                                                                  : next_pos - current_pos;
        _row->_columns.advance_begin(jump_to_next);
    }
    bool is_column_simple() const { return !_row->_columns.front().is_collection; }
    bool is_column_counter() const { return _row->_columns.front().is_counter; }
    const column_translation::column_info& get_column_info() const {
        return _row->_columns.front();
    }
    std::optional<uint32_t> get_column_value_length() const {
        return _row->_columns.front().value_length;
    }
    void setup_ck(const std::vector<std::optional<uint32_t>>& column_value_fix_lengths) {
        _row_key.clear();
        _row_key.reserve(column_value_fix_lengths.size());
        if (column_value_fix_lengths.empty()) {
            _ck_column_value_fix_lengths = boost::make_iterator_range(column_value_fix_lengths);
        } else {
            _ck_column_value_fix_lengths = boost::make_iterator_range(std::begin(column_value_fix_lengths),
                                                                      std::begin(column_value_fix_lengths) + _ck_size);
        }
        _ck_blocks_header_offset = 0u;
    }
    bool no_more_ck_blocks() const { return _ck_column_value_fix_lengths.empty(); }
    void move_to_next_ck_block() {
        _ck_column_value_fix_lengths.advance_begin(1);
        ++_ck_blocks_header_offset;
        if (_ck_blocks_header_offset == 32u) {
            _ck_blocks_header_offset = 0u;
        }
    }
    std::optional<uint32_t> get_ck_block_value_length() const {
        return _ck_column_value_fix_lengths.front();
    }
    bool is_block_empty() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset))) != 0;
    }
    bool is_block_null() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset + 1))) != 0;
    }
    bool should_read_block_header() const {
        return _ck_blocks_header_offset == 0u;
    }
public:
    using consumer = Consumer;
    // assumes !primitive_consumer::active()
    bool non_consuming() const {
        return !_consuming;
    }

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        return _gen.generate();
    }
private:
    processing_result_generator do_process_state() {
        if (_state != state::PARTITION_START) {
            goto flags_label;
        }
        partition_start_label: {
            _is_first_unfiltered = true;
            _state = state::DELETION_TIME;
            co_yield this->read_short_length_bytes(*_processing_data, _pk);
            _state = state::OTHER;
            co_yield this->read_32(*_processing_data);
            co_yield this->read_64(*_processing_data);
            deletion_time del;
            del.local_deletion_time = this->_u32;
            del.marked_for_delete_at = this->_u64;
            auto ret = _consumer.consume_partition_start(key_view(to_bytes_view(_pk)), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _pk.release();
            _state = state::FLAGS;
            if (ret == data_consumer::proceed::no) {
                co_yield data_consumer::proceed::no;
            }
        }
        flags_label:
            _liveness = {};
            _row_tombstone = {};
            _row_shadowable_tombstone = {};
            co_yield this->read_8(*_processing_data);
            _flags = unfiltered_flags_m(this->_u8);
            _state = state::OTHER;
            if (_flags.is_end_of_partition()) {
                _state = state::PARTITION_START;
                if (_consumer.consume_partition_end() == data_consumer::proceed::no) {
                    co_yield data_consumer::proceed::no;
                }
                goto partition_start_label;
            } else if (_flags.is_range_tombstone()) {
                _is_first_unfiltered = false;
                co_yield this->read_8(*_processing_data);
                _range_tombstone_kind = bound_kind_m(this->_u8);
                co_yield this->read_16(*_processing_data);
                _ck_size = this->_u16;
                if (_ck_size == 0) {
                    _row_key.clear();
                    _range_tombstone_kind = is_start(_range_tombstone_kind)
                            ? bound_kind_m::incl_start : bound_kind_m::incl_end;
                    goto range_tombstone_body_label;
                } else {
                    _reading_range_tombstone_ck = true;
                }
            } else if (!_flags.has_extended_flags()) {
                _extended_flags = unfiltered_extended_flags_m(uint8_t{0u});
                start_row(_regular_row);
                _ck_size = _column_translation.clustering_column_value_fix_legths().size();
            } else {
                co_yield this->read_8(*_processing_data);
                _extended_flags = unfiltered_extended_flags_m(this->_u8);
                if (_extended_flags.has_cassandra_shadowable_deletion()) {
                    throw std::runtime_error("SSTables with Cassandra-style shadowable deletion cannot be read by Scylla");
                }
                if (_extended_flags.is_static()) {
                    if (_is_first_unfiltered) {
                        start_row(_static_row);
                        _is_first_unfiltered = false;
                        goto row_body_label;
                    } else {
                        throw malformed_sstable_exception("static row should be a first unfiltered in a partition");
                    }
                }
                start_row(_regular_row);
                _ck_size = _column_translation.clustering_column_value_fix_legths().size();
            }
            _is_first_unfiltered = false;
            _null_component_occured = false;
            setup_ck(_column_translation.clustering_column_value_fix_legths());
            while (!no_more_ck_blocks()) {
                if (should_read_block_header()) {
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _ck_blocks_header = this->_u64;
                }
                if (is_block_null()) {
                    _null_component_occured = true;
                    move_to_next_ck_block();
                    continue;
                }
                if (_null_component_occured) {
                    throw malformed_sstable_exception("non-null component after null component");
                }
                if (is_block_empty()) {
                    _row_key.push_back({});
                    move_to_next_ck_block();
                    continue;
                }
                read_status status = read_status::waiting;
                if (auto len = get_ck_block_value_length()) {
                    status = this->read_bytes(*_processing_data, *len, _column_value);
                } else {
                    status = this->read_unsigned_vint_length_bytes(*_processing_data, _column_value);
                }
                co_yield status;
                _row_key.push_back(std::move(_column_value));
                move_to_next_ck_block();
            }
            if (_reading_range_tombstone_ck) {
                _reading_range_tombstone_ck = false;
                goto range_tombstone_body_label;
            }
        row_body_label: {
            co_yield this->read_unsigned_vint(*_processing_data);
            _next_row_offset = this->position() - _processing_data->size() + this->_u64;
            co_yield this->read_unsigned_vint(*_processing_data);
            // Ignore the result
            row_processing_result ret = _extended_flags.is_static()
                ? _consumer.consume_static_row_start()
                : _consumer.consume_row_start(_row_key);

            while (ret == row_processing_result::retry_later) {
                co_yield data_consumer::proceed::no;
                ret = _extended_flags.is_static()
                    ? _consumer.consume_static_row_start()
                    : _consumer.consume_row_start(_row_key);
            }
            if (ret == row_processing_result::skip_row) {
                _state = state::FLAGS;
                auto current_pos = this->position() - _processing_data->size();
                auto maybe_skip_bytes = this->skip(*_processing_data, _next_row_offset - current_pos);
                if (std::holds_alternative<skip_bytes>(maybe_skip_bytes)) {
                    co_yield maybe_skip_bytes;
                }
                goto flags_label;
            }
            if (_extended_flags.is_static()) {
                if (_flags.has_timestamp() || _flags.has_ttl() || _flags.has_deletion()) {
                    throw malformed_sstable_exception(format("Static row has unexpected flags: timestamp={}, ttl={}, deletion={}",
                        _flags.has_timestamp(), _flags.has_ttl(), _flags.has_deletion()));
                }
            } else {
                if (_flags.has_timestamp()) {
                    co_yield this->read_unsigned_vint(*_processing_data);

                    _liveness.set_timestamp(parse_timestamp(_header, this->_u64));
                    if (_flags.has_ttl()) {
                        co_yield this->read_unsigned_vint(*_processing_data);
                        _liveness.set_ttl(parse_ttl(_header, this->_u64));
                        co_yield this->read_unsigned_vint(*_processing_data);
                        _liveness.set_local_deletion_time(parse_expiry(_header, this->_u64));
                    }
                }
                if (_flags.has_deletion()) {
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _row_tombstone.timestamp = parse_timestamp(_header, this->_u64);
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _row_tombstone.deletion_time = parse_expiry(_header, this->_u64);
                }
                if (_extended_flags.has_scylla_shadowable_deletion()) {
                    if (!_has_shadowable_tombstones) {
                        throw malformed_sstable_exception("Scylla shadowable tombstone flag is set but not supported on this SSTables");
                    }
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _row_shadowable_tombstone.timestamp = parse_timestamp(_header, this->_u64);
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _row_shadowable_tombstone.deletion_time = parse_expiry(_header, this->_u64);
                }
                _consumer.consume_row_marker_and_tombstone(
                        _liveness, std::move(_row_tombstone), std::move(_row_shadowable_tombstone));
            }
            if (!_flags.has_all_columns()) {
                co_yield this->read_unsigned_vint(*_processing_data);
                uint64_t missing_column_bitmap_or_count = this->_u64;
                if (_row->_columns.size() < 64) {
                    _row->_columns_selector.clear();
                    _row->_columns_selector.append(missing_column_bitmap_or_count);
                    _row->_columns_selector.flip();
                    _row->_columns_selector.resize(_row->_columns.size());
                    skip_absent_columns();
                    goto column_label;
                }
                _row->_columns_selector.resize(_row->_columns.size());
                if (_row->_columns.size() - missing_column_bitmap_or_count < _row->_columns.size() / 2) {
                    _missing_columns_to_read = _row->_columns.size() - missing_column_bitmap_or_count;
                    _row->_columns_selector.reset();
                } else {
                    _missing_columns_to_read = missing_column_bitmap_or_count;
                    _row->_columns_selector.set();
                }
                while (_missing_columns_to_read > 0) {
                    --_missing_columns_to_read;
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _row->_columns_selector.flip(this->_u64);
                }
                skip_absent_columns();
            } else {
                _row->_columns_selector.set();
            }
        }
        column_label:
            if (_subcolumns_to_read == 0) {
                if (no_more_columns()) {
                    _state = state::FLAGS;
                    if (_consumer.consume_row_end() == data_consumer::proceed::no) {
                        co_yield data_consumer::proceed::no;
                    }
                    goto flags_label;
                }
                if (!is_column_simple()) {
                    if (!_flags.has_complex_deletion()) {
                        _complex_column_tombstone = {};
                    } else {
                        co_yield this->read_unsigned_vint(*_processing_data);
                        _complex_column_marked_for_delete = parse_timestamp(_header, this->_u64);
                        co_yield this->read_unsigned_vint(*_processing_data);
                        _complex_column_tombstone = {_complex_column_marked_for_delete, parse_expiry(_header, this->_u64)};
                    }
                    if (_consumer.consume_complex_column_start(get_column_info(), _complex_column_tombstone) == data_consumer::proceed::no) {
                        co_yield data_consumer::proceed::no;
                    }
                    co_yield this->read_unsigned_vint(*_processing_data);
                    _subcolumns_to_read = this->_u64;
                    if (_subcolumns_to_read == 0) {
                        const sstables::column_translation::column_info& column_info = get_column_info();
                        move_to_next_column();
                        if (_consumer.consume_complex_column_end(column_info) == data_consumer::proceed::no) {
                            _consuming = false;
                            co_yield data_consumer::proceed::no;
                            _consuming = true;
                        }
                    }
                    goto column_label;
                }
                _subcolumns_to_read = 0;
            }
            co_yield this->read_8(*_processing_data);
            _column_flags = column_flags_m(this->_u8);

            if (_column_flags.use_row_timestamp()) {
                _column_timestamp = _liveness.timestamp();
            } else {
                co_yield this->read_unsigned_vint(*_processing_data);
                _column_timestamp = parse_timestamp(_header, this->_u64);
            }
            if (_column_flags.use_row_ttl()) {
                _column_local_deletion_time = _liveness.local_deletion_time();
            } else if (!_column_flags.is_deleted() && ! _column_flags.is_expiring()) {
                _column_local_deletion_time = gc_clock::time_point::max();
            } else {
                co_yield this->read_unsigned_vint(*_processing_data);
                _column_local_deletion_time = parse_expiry(_header, this->_u64);
            }
            if (_column_flags.use_row_ttl()) {
                _column_ttl = _liveness.ttl();
            } else if (!_column_flags.is_expiring()) {
                _column_ttl = gc_clock::duration::zero();
            } else {
                co_yield this->read_unsigned_vint(*_processing_data);
                _column_ttl = parse_ttl(_header, this->_u64);
            }
            if (!is_column_simple()) {
                co_yield this->read_unsigned_vint_length_bytes_contiguous(*_processing_data, _cell_path);
            } else {
                _cell_path = temporary_buffer<char>(0);
            }
            if (!_column_flags.has_value()) {
                _column_value = fragmented_temporary_buffer();
            } else {
                read_status status = read_status::waiting;
                if (auto len = get_column_value_length()) {
                    status = this->read_bytes(*_processing_data, *len, _column_value);
                } else {
                    status = this->read_unsigned_vint_length_bytes(*_processing_data, _column_value);
                }
                co_yield status;
            }
            _consuming = false;
            if (is_column_counter() && !_column_flags.is_deleted()) {
                if (_consumer.consume_counter_column(get_column_info(),
                                                     fragmented_temporary_buffer::view(_column_value),
                                                     _column_timestamp) == data_consumer::proceed::no) {
                    co_yield data_consumer::proceed::no;
                }
            } else {
                if (_consumer.consume_column(get_column_info(),
                                             to_bytes_view(_cell_path),
                                             fragmented_temporary_buffer::view(_column_value),
                                             _column_timestamp,
                                             _column_ttl,
                                             _column_local_deletion_time,
                                             _column_flags.is_deleted()) == data_consumer::proceed::no) {
                    co_yield data_consumer::proceed::no;
                }
            }
            if (!is_column_simple()) {
                --_subcolumns_to_read;
                if (_subcolumns_to_read == 0) {
                    const sstables::column_translation::column_info& column_info = get_column_info();
                    move_to_next_column();
                    if (_consumer.consume_complex_column_end(column_info) == data_consumer::proceed::no) {
                        co_yield data_consumer::proceed::no;
                    }
                }
            } else {
                move_to_next_column();
            }
            _consuming = true;
            goto column_label;
        range_tombstone_body_label:
            co_yield this->read_unsigned_vint(*_processing_data);
            // Ignore result (marker_body_size or row_body_size)
            co_yield this->read_unsigned_vint(*_processing_data);
            // Ignore result (prev_unfiltered_size)
            co_yield this->read_unsigned_vint(*_processing_data);
            _left_range_tombstone.timestamp = parse_timestamp(_header, this->_u64);
            co_yield this->read_unsigned_vint(*_processing_data);
            _left_range_tombstone.deletion_time = parse_expiry(_header, this->_u64);
            if (!is_boundary_between_adjacent_intervals(_range_tombstone_kind)) {
                if (!is_bound_kind(_range_tombstone_kind)) {
                    throw sstables::malformed_sstable_exception(
                        format("Corrupted range tombstone: invalid boundary type {}", _range_tombstone_kind));
                }
                _sst->get_stats().on_range_tombstone_read();
                _state = state::FLAGS;
                if (_consumer.consume_range_tombstone(_row_key,
                                                      to_bound_kind(_range_tombstone_kind),
                                                      _left_range_tombstone) == data_consumer::proceed::no) {
                    _row_key.clear();
                    co_yield data_consumer::proceed::no;
                }
                _row_key.clear();
                goto flags_label;
            }
            co_yield this->read_unsigned_vint(*_processing_data);
            _right_range_tombstone.timestamp = parse_timestamp(_header, this->_u64);
            co_yield this->read_unsigned_vint(*_processing_data);
            _sst->get_stats().on_range_tombstone_read();
            _right_range_tombstone.deletion_time = parse_expiry(_header, this->_u64);
            _state = state::FLAGS;
            if (_consumer.consume_range_tombstone(_row_key,
                                                  _range_tombstone_kind,
                                                  _left_range_tombstone,
                                                  _right_range_tombstone) == data_consumer::proceed::no) {
                _row_key.clear();
                co_yield data_consumer::proceed::no;
            }
            _row_key.clear();
            goto flags_label;
    }
public:

    data_consume_rows_context_m(const schema& s,
                                const shared_sstable& sst,
                                Consumer& consumer,
                                input_stream<char> && input,
                                uint64_t start,
                                uint64_t maxlen)
        : data_consumer::continuous_data_consumer<data_consume_rows_context_m<Consumer>>(consumer.permit(), std::move(input), start, maxlen)
        , _consumer(consumer)
        , _sst(sst)
        , _header(sst->get_serialization_header())
        , _column_translation(sst->get_column_translation(s, _header, sst->features()))
        , _has_shadowable_tombstones(sst->has_shadowable_tombstones())
        , _gen(do_process_state())
    {
        setup_columns(_regular_row, _column_translation.regular_columns());
        setup_columns(_static_row, _column_translation.static_columns());
    }

    void verify_end_state() {
        // If reading a partial row (i.e., when we have a clustering row
        // filter and using a promoted index), we may be in FLAGS
        // state instead of PARTITION_START.
        if (_state == state::FLAGS) {
            _consumer.on_end_of_stream();
            return;
        }

        // We may end up in state::DELETION_TIME after consuming last partition's end marker
        // and proceeding to attempt to parse the next partition, since state::DELETION_TIME
        // is the first state corresponding to the contents of a new partition.
        if (_state != state::DELETION_TIME
                && (_state != state::PARTITION_START || data_consumer::primitive_consumer::active())) {
            throw malformed_sstable_exception("end of input, but not end of partition");
        }
    }

    void reset(indexable_element el) {
        auto reset_to_state = [this, el] (state s) {
            _state = s;
            _consumer.reset(el);
            _gen = do_process_state();
        };
        switch (el) {
            case indexable_element::partition:
                return reset_to_state(state::PARTITION_START);
            case indexable_element::cell:
                return reset_to_state(state::FLAGS);
        }
        // We should not get here unless some enum member is not handled by the switch
        throw std::logic_error(format("Unable to reset - unknown indexable element: {}", el));
    }

    // Call after a reverse index skip is performed during reversed reads.
    void reset_after_reversed_read_skip() {
        // During reversed reads the source is always returning whole rows
        // even when we perform an index skip in the middle of a row.
        // Thus we must not reset the parser state as we do in regular reset.
        // We need only to inform the consumer.
        _consumer.reset_after_reversed_read_skip();
    }

    reader_permit& permit() {
        return _consumer.permit();
    }
};

class mx_sstable_mutation_reader : public mp_row_consumer_reader_mx {
    using DataConsumeRowsContext = data_consume_rows_context_m<mp_row_consumer_m>;
    using Consumer = mp_row_consumer_m;
    static_assert(RowConsumer<Consumer>);
    value_or_reference<query::partition_slice> _slice_holder;
    const query::partition_slice& _slice;
    Consumer _consumer;
    bool _will_likely_slice = false;
    bool _read_enabled = true;
    std::unique_ptr<DataConsumeRowsContext> _context;
    std::unique_ptr<index_reader> _index_reader;
    // We avoid unnecessary lookup for single partition reads thanks to this flag
    bool _single_partition_read = false;
    const dht::partition_range& _pr;
    streamed_mutation::forwarding _fwd;
    mutation_reader::forwarding _fwd_mr;
    read_monitor& _monitor;

    // For reversed (single partition) reads, points to the current position in the sstable
    // of the reversing data source used underneath (see `partition_reversing_data_source`).
    // Engaged after `_context` is engaged, i.e. after `initialize()`.
    const uint64_t* _reversed_read_sstable_position;
public:
    mx_sstable_mutation_reader(shared_sstable sst,
                            schema_ptr schema,
                            reader_permit permit,
                            const dht::partition_range& pr,
                            value_or_reference<query::partition_slice> slice,
                            tracing::trace_state_ptr trace_state,
                            streamed_mutation::forwarding fwd,
                            mutation_reader::forwarding fwd_mr,
                            read_monitor& mon)
            : mp_row_consumer_reader_mx(std::move(schema), permit, std::move(sst))
            , _slice_holder(std::move(slice))
            , _slice(_slice_holder.get())
            , _consumer(this, _schema, std::move(permit), _slice, std::move(trace_state), fwd, _sst)
            // FIXME: I want to add `&& fwd_mr == mutation_reader::forwarding::no` below
            // but can't because many call sites use the default value for
            // `mutation_reader::forwarding` which is `yes`.
            , _single_partition_read(pr.is_singular())
            , _pr(pr)
            , _fwd(fwd)
            , _fwd_mr(fwd_mr)
            , _monitor(mon) {
        if (reversed()) {
            if (!_single_partition_read) {
                on_internal_error(sstlog, format(
                        // Not only in the reader, they are disabled in CQL.
                        "mx reader: multi-partition reversed queries are not supported yet;"
                        " partition range: {}", pr));
            }
            // FIXME: if only the defaults were better...
            //SCYLLA_ASSERT(fwd_mr == mutation_reader::forwarding::no);
        }
    }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    mx_sstable_mutation_reader(mx_sstable_mutation_reader&&) = delete;
    mx_sstable_mutation_reader(const mx_sstable_mutation_reader&) = delete;
    ~mx_sstable_mutation_reader() {
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
            auto caching = use_caching(global_cache_index_pages && !_slice.options.contains(query::partition_slice::option::bypass_cache));
            _index_reader = std::make_unique<index_reader>(_sst, _consumer.permit(),
                                                           _consumer.trace_state(), caching, _single_partition_read);
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
            SCYLLA_ASSERT(_index_reader->element_kind() == indexable_element::partition);
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
        auto pk = _index_reader->get_partition_key();
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
        SCYLLA_ASSERT (_current_partition_key);
        return [this] {
            if (!_index_in_current_partition) {
                _index_in_current_partition = true;
                // FIXME reversed multi partition reads
                return get_index_reader().advance_to(*_current_partition_key);
            }
            return make_ready_future();
        }().then([this, pos = *pos] {
            if (reversed()) {
                // The position `pos` conforms to the query schema (it is the start of a reversed range),
                // which is reversed w.r.t. the table schema. We use the table schema in index_reader,
                // so we need to unreverse `pos` before passing it into index_reader.
                auto rev_pos = pos.reversed();
                return get_index_reader().advance_reverse(std::move(rev_pos)).then([this] {
                    // The reversing data source will notice the skip and update the data ranges
                    // from which it prepares the data given to us.

                    SCYLLA_ASSERT(_reversed_read_sstable_position);
                    auto ip = _index_reader->data_file_positions();
                    if (ip.end >= *_reversed_read_sstable_position) {
                        // The reversing data source was already ahead (in reverse - its position was smaller)
                        // than the index. We must not update the current range tombstone in this case
                        // or reset the context since all fragments up to the new position of the index
                        // will be (or already have been) provided to the context by the source.
                        return;
                    }

                    _context->reset_after_reversed_read_skip();

                    _sst->get_stats().on_partition_seek();
                    auto open_end_marker = _index_reader->reverse_end_open_marker();
                    if (open_end_marker) {
                        _consumer.set_range_tombstone(open_end_marker->tomb);
                    } else {
                        _consumer.set_range_tombstone({});
                    }
                });
            } else {
                return get_index_reader().advance_to(pos).then([this] {
                    index_reader& idx = *_index_reader;
                    auto index_position = idx.data_file_positions();
                    if (index_position.start <= _context->position()) {
                        return make_ready_future<>();
                    }
                    return skip_to(idx.element_kind(), index_position.start).then([this, &idx] {
                        _sst->get_stats().on_partition_seek();
                        auto open_end_marker = idx.end_open_marker();
                        if (open_end_marker) {
                            _consumer.set_range_tombstone(open_end_marker->tomb);
                        } else {
                            _consumer.set_range_tombstone({});
                        }
                    });
                });
            }
        });
    }
    bool is_initialized() const {
        return bool(_context);
    }
    // Returns true if reader is initialized, by either a previous or current request
    future<bool> maybe_initialize() {
        if (is_initialized()) {
            co_return true;
        }
        if (_single_partition_read) {
            _sst->get_stats().on_single_partition_read();
            const auto& key = dht::ring_position_view(_pr.start()->value());
            position_in_partition_view pos = get_slice_upper_bound(*_schema, _slice, key);
            const auto present = co_await get_index_reader().advance_lower_and_check_if_present(key, pos);

            if (!present) {
                _sst->get_filter_tracker().add_false_positive();
                co_return false;
            }

            _sst->get_filter_tracker().add_true_positive();
            if (reversed()) {
                co_await _index_reader->advance_reverse_to_next_partition();
            }
        } else {
            _sst->get_stats().on_range_partition_read();
            co_await get_index_reader().advance_to(_pr);
        }

        auto [begin, end] = _index_reader->data_file_positions();
        SCYLLA_ASSERT(end);

        if (_single_partition_read) {
            _read_enabled = (begin != *end);
            if (reversed()) {
                auto reversed_context = data_consume_reversed_partition<DataConsumeRowsContext>(
                        *_schema, _sst, *_index_reader, _consumer, { begin, *end });
                _context = std::move(reversed_context.the_context);
                _reversed_read_sstable_position = &reversed_context.current_position_in_sstable;
            } else {
                _context = data_consume_single_partition<DataConsumeRowsContext>(*_schema, _sst, _consumer, { begin, *end });
            }
        } else {
            sstable::disk_read_range drr{begin, *end};
            auto last_end = _fwd_mr ? _sst->data_size() : drr.end;
            _read_enabled = bool(drr);
            _context = data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer, std::move(drr), last_end);
        }

        _monitor.on_read_started(_context->reader_position());
        _index_in_current_partition = true;
        _will_likely_slice = will_likely_slice(_slice);
        co_return true;
    }
    future<> skip_to(indexable_element el, uint64_t begin) {
        sstlog.trace("sstable_reader: {}: skip_to({} -> {}, el={})", fmt::ptr(_context.get()), _context->position(), begin, static_cast<int>(el));
        if (begin <= _context->position()) {
            return make_ready_future<>();
        }
        _context->reset(el);
        return _context->skip_to(begin);
    }
    bool reversed() const {
        return _slice.is_reversed();
    }
public:
    void on_out_of_clustering_range() override {
        if (_fwd == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            this->push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
            _partition_finished = true;
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        if (reversed()) {
            // FIXME
            on_internal_error(sstlog, "mx reader: fast_forward_to(partition_range) not supported for reversed queries");
        }

        return maybe_initialize().then([this, &pr] (bool initialized) {
            if (!initialized) {
                _end_of_stream = true;
                return make_ready_future<>();
            } else {
                clear_buffer();
                _partition_finished = true;
                _before_partition = true;
                _end_of_stream = false;
                SCYLLA_ASSERT(_index_reader);
                auto f1 = _index_reader->advance_to(pr);
                return f1.then([this] {
                    auto [start, end] = _index_reader->data_file_positions();
                    SCYLLA_ASSERT(end);
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
    virtual future<> fill_buffer() override {
        if (_end_of_stream) {
            return make_ready_future<>();
        }
        if (!is_initialized()) {
            return maybe_initialize().then([this] (bool initialized) {
                if (!initialized) {
                    _end_of_stream = true;
                    return make_ready_future<>();
                } else {
                    return fill_buffer();
                }
            });
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (_partition_finished) {
                check_abort();
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
                    check_abort();
                    return advance_context(_consumer.maybe_skip()).then([this] {
                        return _context->consume_input();
                    });
                });
            }
        }).then_wrapped([this] (future<> f) {
            try {
                f.get();
            } catch(sstables::malformed_sstable_exception& e) {
                throw sstables::malformed_sstable_exception(format("Failed to read partition from SSTable {} due to {}", _sst->get_filename(), e.what()));
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
    virtual future<> fast_forward_to(position_range cr) override {
        clear_buffer();
        if (!_partition_finished) {
            _end_of_stream = false;
            return advance_context(_consumer.fast_forward_to(std::move(cr)));
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
            // close can not fail as it is called either from the destructor or from mutation_reader::close
            sstlog.warn("Failed closing of sstable_mutation_reader: {}. Ignored since the reader is already done.", ep);
        });
    }
};

static mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        value_or_reference<query::partition_slice> slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_mutation_reader<mx_sstable_mutation_reader>(
        std::move(sstable), std::move(schema), std::move(permit), range,
        std::move(slice), std::move(trace_state), fwd, fwd_mr, monitor);
}

mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_reader(std::move(sstable), std::move(schema), std::move(permit), range,
            value_or_reference(slice), std::move(trace_state), fwd, fwd_mr, monitor);
}

mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        query::partition_slice&& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_reader(std::move(sstable), std::move(schema), std::move(permit), range,
            value_or_reference(std::move(slice)), std::move(trace_state), fwd, fwd_mr, monitor);
}

class mx_crawling_sstable_mutation_reader : public mp_row_consumer_reader_mx {
    using DataConsumeRowsContext = data_consume_rows_context_m<mp_row_consumer_m>;
    using Consumer = mp_row_consumer_m;
    static_assert(RowConsumer<Consumer>);
    Consumer _consumer;
    std::unique_ptr<DataConsumeRowsContext> _context;
    read_monitor& _monitor;
public:
    mx_crawling_sstable_mutation_reader(shared_sstable sst, schema_ptr schema,
             reader_permit permit,
             tracing::trace_state_ptr trace_state,
             read_monitor& mon)
        : mp_row_consumer_reader_mx(std::move(schema), permit, std::move(sst))
        , _consumer(this, _schema, std::move(permit), _schema->full_slice(), std::move(trace_state), streamed_mutation::forwarding::no, _sst)
        , _context(data_consume_rows<DataConsumeRowsContext>(*_schema, _sst, _consumer))
        , _monitor(mon) {
        _monitor.on_read_started(_context->reader_position());
    }
public:
    void on_out_of_clustering_range() override { }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        on_internal_error(sstlog, "mx_crawling_sstable_mutation_reader: doesn't support fast_forward_to(const dht::partition_range&)");
    }
    virtual future<> fast_forward_to(position_range cr) override {
        on_internal_error(sstlog, "mx_crawling_sstable_mutation_reader: doesn't support fast_forward_to(position_range)");
    }
    virtual future<> next_partition() override {
        on_internal_error(sstlog, "mx_crawling_sstable_mutation_reader: doesn't support next_partition()");
    }
    virtual future<> fill_buffer() override {
        if (_end_of_stream) {
            return make_ready_future<>();
        }
        if (_context->eof()) {
            _end_of_stream = true;
            return make_ready_future<>();
        }
        return _context->consume_input();
    }
    virtual future<> close() noexcept override {
        if (!_context) {
            return make_ready_future<>();
        }
        _monitor.on_read_completed();
        return _context->close().handle_exception([_ = std::move(_context)] (std::exception_ptr ep) {
            sstlog.warn("Failed closing of mx_crawling_sstable_mutation_reader: {}. Ignored since the reader is already done.", ep);
        });
    }
};

mutation_reader make_crawling_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor) {
    return make_mutation_reader<mx_crawling_sstable_mutation_reader>(std::move(sstable), std::move(schema), std::move(permit),
            std::move(trace_state), monitor);
}

void mp_row_consumer_reader_mx::on_next_partition(dht::decorated_key key, tombstone tomb) {
    _partition_finished = false;
    _before_partition = false;
    _end_of_stream = false;
    _current_partition_key = std::move(key);
    push_mutation_fragment(
            mutation_fragment_v2(*_schema, _permit, partition_start(*_current_partition_key, tomb)));
    _sst->get_stats().on_partition_read();
}

// A validating consumer implementing the Consumer concept of data_consume_rows_context_m.
//
// It consumes the atoms coming from the parser and validates that the mutation
// fragment stream they form is valid, namely it checks:
// * partition ordering
// * mutation fragment kind ordering
// * clustering element ordering
// * partitions being ended properly (before EOS)
// * range tombstones being ended properly (before end-of-partition)
//
// For this, it relies on the mutation_fragment_stream_validator.
//
// It also allows for checking that partitions and clustering keys are laid out
// as expected by the index and promoted index respectively.
class validating_consumer {
public:
    struct clustering_block {
        position_in_partition start;
        position_in_partition end;
        bool done = false;

        clustering_block(position_in_partition_view start, position_in_partition_view end) : start(start), end(end) {
        }
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    std::function<void(sstring)> _error_handler;
    // For static-compact tables C* stores the only row in the static row but in our representation they're regular rows.
    const bool _treat_static_row_as_regular;
    mutation_fragment_stream_validator _validator;
    uint64_t _error_count = 0;
    std::optional<partition_key> _expected_pkey;
    std::optional<clustering_block> _expected_clustering_block;
    position_in_partition _current_pos;
    bool _stop_after_partition_header = false;

private:
    clustering_key from_fragmented_buffer(const std::vector<fragmented_temporary_buffer>& ecp) {
        return clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
                [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));
    }
    void validate_fragment_order(mutation_fragment_v2::kind kind, std::optional<tombstone> new_current_tombstone) {
        if (auto res = _validator(kind, _current_pos, new_current_tombstone); !res) {
            report_error(res.what());
            _validator.reset(kind, _current_pos, new_current_tombstone);
        }
        if (_current_pos.region() != partition_region::clustered) {
            return;
        }
        if (_expected_clustering_block) {
            auto cmp = position_in_partition::tri_compare(*_schema);
            const auto cmp_start = cmp(_expected_clustering_block->start, _current_pos);
            const auto cmp_end = cmp(_expected_clustering_block->end, _current_pos);
            if (cmp_start > 0 || cmp_end < 0) {
                if (_expected_clustering_block->done) {
                    report_error(format("mismatching index/data: promoted index has no more blocks, but partition {} ({}) has more rows",
                            _validator.previous_partition_key().key().with_schema(*_schema),
                            _validator.previous_partition_key().key()));
                } else {
                    report_error(format("mismatching index/data: clustering element {} is outside of current promoted-index block [{}, {}]",
                            _current_pos,
                            _expected_clustering_block->start,
                            _expected_clustering_block->end));
                }
            }
            if (cmp_end == 0) {
                sstlog.trace("validating_consumer {}: {}() current block is done", fmt::ptr(this), __FUNCTION__);
                _expected_clustering_block->done = true;
            }
        }
    }

public:
    validating_consumer(const schema_ptr schema, reader_permit permit, const shared_sstable& sst, std::function<void(sstring)> error_handler)
        : _schema(schema)
        , _permit(std::move(permit))
        , _error_handler(std::move(error_handler))
        , _treat_static_row_as_regular(_schema->is_static_compact_table()
            && (!sst->has_scylla_component() || sst->features().is_enabled(sstable_feature::CorrectStaticCompact))) // See #4139
        , _validator(*_schema)
        , _current_pos(position_in_partition::end_of_partition_tag_t{})
    {
    }

    const reader_permit& permit() const { return _permit; }
    tracing::trace_state_ptr trace_state() { return {}; }
    uint64_t error_count() const { return _error_count; }
    position_in_partition_view current_position() const { return _current_pos; }

    void set_stop_after_partition_header() {
        _stop_after_partition_header = true;
    }

    void set_index_expected_partition(partition_key pkey) {
        _expected_pkey.emplace(std::move(pkey));
    }
    void reset_index_expected_partition() {
        _expected_pkey.reset();
    }
    void set_index_expected_clustering_block(position_in_partition_view start, position_in_partition_view end) {
        _expected_clustering_block.emplace(start, end);
    }
    bool in_expected_clustering_block() const {
        return _expected_clustering_block && !_expected_clustering_block->done;
    }

    void report_error(sstring what) {
        ++_error_count;
        _error_handler(what);
    }

    data_consumer::proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) {
        auto pk = key.to_partition_key(*_schema);
        auto dk = dht::decorate_key(*_schema, pk);
        _current_pos = position_in_partition(position_in_partition::partition_start_tag_t{});
        sstlog.trace("validating_consumer {}: {}({}) _expected_pkey={}", fmt::ptr(this), __FUNCTION__, pk, _expected_pkey);
        validate_fragment_order(mutation_fragment_v2::kind::partition_start, {});
        if (_expected_pkey && !_expected_pkey->equal(*_schema, dk.key())) {
            report_error(format("mismatching index/data: partition mismatch: index: {}, data: {}", *_expected_pkey, dk.key()));
        }
        if (auto res = _validator(dk); !res) {
            report_error(res.what());
            _validator.reset(dk);
        }
        _expected_clustering_block.reset();
        if (_stop_after_partition_header) {
            _stop_after_partition_header = false;
            return data_consumer::proceed::no;
        }
        return data_consumer::proceed::yes;
    }

    row_processing_result consume_row_start(const std::vector<fragmented_temporary_buffer>& ecp) {
        auto ck = from_fragmented_buffer(ecp);
        _current_pos = position_in_partition::for_key(std::move(ck));
        sstlog.trace("validating_consumer {}: {}({})", fmt::ptr(this), __FUNCTION__, _current_pos);
        validate_fragment_order(mutation_fragment_v2::kind::clustering_row, {});
        return row_processing_result::do_proceed;
    }

    data_consumer::proceed consume_row_marker_and_tombstone(const liveness_info& info, tombstone tomb, tombstone shadowable_tomb) {
        return data_consumer::proceed::yes;
    }

    row_processing_result consume_static_row_start() {
        sstlog.trace("validating_consumer {}: {}()", fmt::ptr(this), __FUNCTION__);
        if (_treat_static_row_as_regular) {
            return consume_row_start({});
        }
        _current_pos = position_in_partition(position_in_partition::static_row_tag_t{});
        validate_fragment_order(mutation_fragment_v2::kind::static_row, {});
        return row_processing_result::do_proceed;
    }

    data_consumer::proceed consume_column(const column_translation::column_info& column_info, bytes_view cell_path, fragmented_temporary_buffer::view value,
            api::timestamp_type timestamp, gc_clock::duration ttl, gc_clock::time_point local_deletion_time, bool is_deleted) {
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info, tombstone tomb) {
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) {
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_counter_column(const column_translation::column_info& column_info, fragmented_temporary_buffer::view value, api::timestamp_type timestamp) {
        return data_consumer::proceed::yes;
    }

    data_consumer::proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp, bound_kind kind, tombstone tomb) {
        auto ck = from_fragmented_buffer(ecp);
        _current_pos = position_in_partition(position_in_partition::range_tag_t(), kind, std::move(ck));
        tombstone new_current_tomb;
        if (kind == bound_kind::incl_start || kind == bound_kind::excl_start) {
            new_current_tomb = tomb;
        }
        sstlog.trace("validating_consumer {}: {}({}, {})", fmt::ptr(this), __FUNCTION__, _current_pos, new_current_tomb);
        validate_fragment_order(mutation_fragment_v2::kind::range_tombstone_change, new_current_tomb);
        if (_expected_clustering_block) {
            return data_consumer::proceed(!_expected_clustering_block->done);
        } else {
            return data_consumer::proceed(!need_preempt());
        }
    }

    data_consumer::proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp, sstables::bound_kind_m kind, tombstone end_tombstone, tombstone start_tombstone) {
        sstlog.trace("validating_consumer {}: {}()", fmt::ptr(this), __FUNCTION__);
        switch (kind) {
        case bound_kind_m::incl_end_excl_start:
            return consume_range_tombstone(ecp, bound_kind::excl_start, start_tombstone);
        case bound_kind_m::excl_end_incl_start:
            return consume_range_tombstone(ecp, bound_kind::incl_start, start_tombstone);
        default:
            SCYLLA_ASSERT(false && "Invalid boundary type");
        }
    }

    data_consumer::proceed consume_row_end() {
        sstlog.trace("validating_consumer {}: {}()", fmt::ptr(this), __FUNCTION__);
        if (_expected_clustering_block) {
            return data_consumer::proceed(!_expected_clustering_block->done);
        } else {
            return data_consumer::proceed(!need_preempt());
        }
    }

    void on_end_of_stream() {
        if (auto res = _validator.on_end_of_stream(); !res) {
            report_error(res.what());
        }
        sstlog.trace("validating_consumer {}: {}()", fmt::ptr(this), __FUNCTION__);
    }

    data_consumer::proceed consume_partition_end() {
        sstlog.trace("validating_consumer {}: {}()", fmt::ptr(this), __FUNCTION__);
        _current_pos = position_in_partition(position_in_partition::end_of_partition_tag_t{});
        validate_fragment_order(mutation_fragment_v2::kind::partition_end, {});
        return data_consumer::proceed::no;
    }
};

future<uint64_t> validate(
        shared_sstable sstable,
        reader_permit permit,
        abort_source& abort,
        std::function<void(sstring)> error_handler,
        sstables::read_monitor& monitor) {
    auto schema = sstable->get_schema();
    validating_consumer consumer(schema, permit, sstable, std::move(error_handler));
    auto context = data_consume_rows<data_consume_rows_context_m<validating_consumer>>(*schema, sstable, consumer);

    std::optional<sstables::index_reader> idx_reader;
    idx_reader.emplace(sstable, permit, tracing::trace_state_ptr{}, sstables::use_caching::no, false);

    try {
        monitor.on_read_started(context->reader_position());
        while (!context->eof() && !abort.abort_requested()) {
            uint64_t current_partition_pos = 0;
            clustered_index_cursor* idx_cursor = nullptr;

            if (idx_reader && idx_reader->eof()) {
                consumer.report_error("mismatching index/data: index is at EOF, but data file has more data");
                co_await idx_reader->close();
                idx_reader.reset();
            }

            if (idx_reader) {
                co_await idx_reader->read_partition_data();

                idx_cursor = idx_reader->current_clustered_cursor();

                const auto index_pos = idx_reader->get_data_file_position();
                const auto data_pos = context->position();
                sstlog.trace("validate(): index-data position check for partition {}: {} == {}", idx_reader->get_partition_key(), data_pos, index_pos);
                if (index_pos != data_pos) {
                    consumer.report_error(format("mismatching index/data: position mismatch: index: {}, data: {}", index_pos, data_pos));
                }
                current_partition_pos = data_pos;

                consumer.set_index_expected_partition(idx_reader->get_partition_key());
            } else {
                consumer.reset_index_expected_partition();
            }

            std::optional<clustered_index_cursor::entry_info> current_pi_block;

            if (idx_cursor) {
                consumer.set_stop_after_partition_header();
                co_await context->consume_input();
            }
            bool first_block = true;

            do {
                if (!consumer.in_expected_clustering_block() && idx_cursor && (current_pi_block = co_await idx_cursor->next_entry())) {
                    // The mx format always has position-in-partition in the variant.
                    const auto start = std::get<position_in_partition_view>(current_pi_block->start);
                    const auto end = std::get<position_in_partition_view>(current_pi_block->end);
                    const auto index_pos = current_partition_pos + current_pi_block->offset;
                    const auto data_pos = context->position();
                    sstlog.trace("validate(): index-data position check for clustering block (first={}) [{}, {}]: {} == {}, partition starts at {}", first_block, start, end, index_pos, data_pos, current_partition_pos);
                    // We cannot reliably position the parser at the start of
                    // the first block, because there is no way to check what
                    // the next element is (static row or clustering row)
                    // without reading its header. And once read, we cannot
                    // rewind the parser.
                    // So we do a best-effort here: we ask the consumer to stop
                    // after consuming the partition-start. For schemas without
                    // static row this should result in the exact position, but
                    // even then it is not guaranteed if the schema has a dropped
                    // static column.
                    if (first_block) {
                        first_block = false;
                        if (data_pos > index_pos) {
                            consumer.report_error(format("mismatching index/data: position mismatch: first promoted index block: {}, data: {}", index_pos, data_pos));
                        }
                    } else {
                        if (data_pos != index_pos) {
                            consumer.report_error(format("mismatching index/data: position mismatch: promoted index: {}, data: {}", index_pos, data_pos));
                        }
                    }

                    consumer.set_index_expected_clustering_block(start, end);
                }
                co_await context->consume_input();

                co_await coroutine::maybe_yield();
            } while (consumer.current_position().region() != partition_region::partition_end && !abort.abort_requested());

            if (abort.abort_requested()) {
                // Prevent fall-through to the post-checks below if the the loop above was broken due to abort.
                break;
            }

            // Check if promoted index still has more entries.
            if (idx_cursor && (current_pi_block = co_await idx_cursor->next_entry())) {
                consumer.report_error(format("mismatching index/data: promoted index has more blocks, but it is end of partition {} ({})",
                        idx_reader->get_partition_key().with_schema(*schema),
                        idx_reader->get_partition_key()));
            }

            if (idx_reader) {
                co_await idx_reader->advance_to_next_partition();
            }
        }
    } catch (...) {
        consumer.report_error(format("unexpected exception: {}", std::current_exception()));
    }

    monitor.on_read_completed();
    if (idx_reader) {
        co_await idx_reader->close();
    }

    co_await context->close();
    co_return consumer.error_count();
}

} // namespace mx
} // namespace sstables
