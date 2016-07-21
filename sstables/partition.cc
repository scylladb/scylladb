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
#include "core/future-util.hh"
#include "key.hh"
#include "keys.hh"
#include "core/do_with.hh"
#include "unimplemented.hh"
#include "utils/move.hh"
#include "dht/i_partitioner.hh"

namespace sstables {

/**
 * @returns: >= 0, if key is found. That is the index where the key is found.
 *             -1, if key is not found, and is smaller than the first key in the list.
 *          <= -2, if key is not found, but is greater than one of the keys. By adding 2 and
 *                 negating, one can determine the index before which the key would have to
 *                 be inserted.
 *
 * Origin uses this slightly modified binary search for the Summary, that will
 * indicate in which bucket the element would be in case it is not a match.
 *
 * For the Index entries, it uses a "normal", java.lang binary search. Because
 * we have made the explicit decision to open code the comparator for
 * efficiency, using a separate binary search would be possible, but very
 * messy.
 *
 * It's easier to reuse the same code for both binary searches, and just ignore
 * the extra information when not needed.
 *
 * This code should work in all kinds of vectors in whose's elements is possible to aquire
 * a key view via get_key().
 */
template <typename T>
int sstable::binary_search(const T& entries, const key& sk, const dht::token& token) {
    int low = 0, mid = entries.size(), high = mid - 1, result = -1;

    auto& partitioner = dht::global_partitioner();

    while (low <= high) {
        // The token comparison should yield the right result most of the time.
        // So we avoid expensive copying operations that happens at key
        // creation by keeping only a key view, and then manually carrying out
        // both parts of the comparison ourselves.
        mid = low + ((high - low) >> 1);
        key_view mid_key = entries[mid].get_key();
        auto mid_token = partitioner.get_token(mid_key);

        if (token == mid_token) {
            result = sk.tri_compare(mid_key);
        } else {
            result = token < mid_token ? -1 : 1;
        }

        if (result > 0) {
            low = mid + 1;
        } else if (result < 0) {
            high = mid - 1;
        } else {
            return mid;
        }
    }

    return -mid - (result < 0 ? 1 : 2);
}

// Force generation, so we make it available outside this compilation unit without moving that
// much code to .hh
template int sstable::binary_search<>(const std::vector<summary_entry>& entries, const key& sk);
template int sstable::binary_search<>(const std::vector<index_entry>& entries, const key& sk);

static inline bytes pop_back(std::vector<bytes>& vec) {
    auto b = std::move(vec.back());
    vec.pop_back();
    return std::move(b);
}

class mp_row_consumer : public row_consumer {
public:
    struct new_mutation {
        partition_key key;
        tombstone tomb;
    };
private:
    schema_ptr _schema;
    key_view _key;
    const io_priority_class* _pc = nullptr;
    query::clustering_key_filtering_context _ck_filtering;
    query::clustering_key_filter _filter;

    bool _skip_partition;
    bool _skip_clustering_row;

    // We don't have "end of clustering row" markers. So we know that the current
    // row has ended once we get something (e.g. a live cell) that belongs to another
    // one. If that happens sstable reader is interrupted (proceed::no) but we
    // already have the whole row that just ended and a part of the new row.
    // The finished row is moved to _ready so that upper layer can retrieve it and
    // the part of the new row goes to _in_progress and this is were we will continue
    // accumulating data once sstable reader is continued.
    mutation_fragment_opt _in_progress;
    mutation_fragment_opt _ready;

    stdx::optional<new_mutation> _mutation;
    bool _is_mutation_end;

    struct column {
        bool is_static;
        bytes_view col_name;
        std::vector<bytes> clustering;
        // see is_collection. collections have an extra element aside from the name.
        // This will be non-zero size if this is a collection, and zero size othersize.
        bytes collection_extra_data;
        bytes cell;
        const column_definition *cdef;

        static constexpr size_t static_size = 2;

        // For every normal column, we expect the clustering key, followed by the
        // extra element for the column name.
        //
        // For a collection, some auxiliary data will be embedded into the
        // column_name as seen by the row consumer. This means that if our
        // exploded clustering keys has more rows than expected, we are dealing
        // with a collection.
        bool is_collection(const schema& s) {
            auto expected_normal = s.clustering_key_size() + 1;
            // Note that we can have less than the expected. That is the case for
            // incomplete prefixes, for instance.
            if (clustering.size() <= expected_normal) {
                return false;
            } else if (clustering.size() == (expected_normal + 1)) {
                return true;
            }
            throw malformed_sstable_exception(sprint("Found %d clustering elements in column name. Was not expecting that!", clustering.size()));
        }

        bool is_present(api::timestamp_type timestamp) {
            return cdef && timestamp > cdef->dropped_at();
        }

        static bool check_static(bytes_view col) {
            static bytes static_row(static_size, 0xff);
            return col.compare(0, static_size, static_row) == 0;
        }

        static bytes_view fix_static_name(bytes_view col) {
            if (check_static(col)) {
                col.remove_prefix(static_size);
            }
            return col;
        }

        std::vector<bytes> extract_clustering_key(const schema& schema) {
            if (!schema.is_compound()) {
                return { to_bytes(col_name) };
            } else {
                return composite_view(col_name).explode();
            }
        }
        column(const schema& schema, bytes_view col)
            : is_static(check_static(col))
            , col_name(fix_static_name(col))
            , clustering(extract_clustering_key(schema))
            , collection_extra_data(is_collection(schema) ? pop_back(clustering) : bytes()) // collections are not supported with COMPACT STORAGE, so this is fine
            , cell(!schema.is_dense() ? pop_back(clustering) : (*(schema.regular_begin())).name()) // dense: cell name is not provided. It is the only regular column
            , cdef(schema.get_column_definition(cell))
        {

            if (is_static) {
                for (auto& e: clustering) {
                    if (e.size() != 0) {
                        throw malformed_sstable_exception("Static row has clustering key information. I didn't expect that!");
                    }
                }
            }
        }
    };

    // Notes for collection mutation:
    //
    // While we could in theory generate the mutation for the elements as they
    // appear, that would be costly.  We would need to keep deserializing and
    // serializing them, either explicitly or through a merge.
    //
    // The best way forward is to accumulate the collection data into a data
    // structure, and later on serialize it fully when this (sstable) row ends.
    class collection_mutation {
        const column_definition *_cdef;
    public:
        collection_type_impl::mutation cm;

        // We need to get a copy of the prefix here, because the outer object may be short lived.
        collection_mutation(const column_definition *cdef)
            : _cdef(cdef) { }

        collection_mutation() : _cdef(nullptr) {}

        bool is_new_collection(const column_definition *c) {
            if (!_cdef || ((_cdef->id != c->id) || (_cdef->kind != c->kind))) {
                return true;
            }
            return false;
        };

        void flush(const schema& s, mutation_fragment& mf) {
            if (!_cdef) {
                return;
            }
            auto ctype = static_pointer_cast<const collection_type_impl>(_cdef->type);
            auto ac = atomic_cell_or_collection::from_collection_mutation(ctype->serialize_mutation_form(cm));
            if (_cdef->is_static()) {
                mf.as_static_row().set_cell(*_cdef, std::move(ac));
            } else {
                mf.as_clustering_row().set_cell(*_cdef, std::move(ac));
            }
        }
    };
    std::experimental::optional<collection_mutation> _pending_collection = {};

    collection_mutation& pending_collection(const column_definition *cdef) {
        if (!_pending_collection || _pending_collection->is_new_collection(cdef)) {
            flush_pending_collection(*_schema);

            if (!cdef->type->is_multi_cell()) {
                throw malformed_sstable_exception("frozen set should behave like a cell\n");
            }
            _pending_collection = collection_mutation(cdef);
        }
        return *_pending_collection;
    }

    void update_pending_collection(const column_definition *cdef, bytes&& col, atomic_cell&& ac) {
        pending_collection(cdef).cm.cells.emplace_back(std::move(col), std::move(ac));
    }

    void update_pending_collection(const column_definition *cdef, tombstone&& t) {
        pending_collection(cdef).cm.tomb = std::move(t);
    }

    void flush_pending_collection(const schema& s) {
        if (_pending_collection) {
            _pending_collection->flush(s, *_in_progress);
            _pending_collection = {};
        }
    }

public:
    mutation_opt mut;

    mp_row_consumer(const key& key,
                    const schema_ptr schema,
                    query::clustering_key_filtering_context ck_filtering,
                    const io_priority_class& pc)
            : _schema(schema)
            , _key(key_view(key))
            , _pc(&pc)
            , _ck_filtering(ck_filtering)
            , _filter(_ck_filtering.get_filter_for_sorted(partition_key::from_exploded(*_schema, key.explode(*_schema))))
    { }

    mp_row_consumer(const key& key,
                    const schema_ptr schema,
                    const io_priority_class& pc)
            : mp_row_consumer(key, schema, query::no_clustering_key_filtering, pc) { }

    mp_row_consumer(const schema_ptr schema,
                    query::clustering_key_filtering_context ck_filtering,
                    const io_priority_class& pc)
            : _schema(schema)
            , _pc(&pc)
            , _ck_filtering(ck_filtering)
    { }

    mp_row_consumer(const schema_ptr schema,
                    const io_priority_class& pc)
            : mp_row_consumer(schema, query::no_clustering_key_filtering, pc) { }

    mp_row_consumer() : _ck_filtering(query::no_clustering_key_filtering) {}

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        if (_key.empty() || key == _key) {
            _mutation = new_mutation { partition_key::from_exploded(key.explode(*_schema)), tombstone(deltime) };
            _is_mutation_end = false;
            _skip_partition = false;
            _skip_clustering_row = false;
            _filter = _ck_filtering.get_filter_for_sorted(_mutation->key);
            return proceed::no;
        } else {
            throw malformed_sstable_exception(sprint("Key mismatch. Got %s while processing %s", to_hex(bytes_view(key)).c_str(), to_hex(bytes_view(_key)).c_str()));
        }
    }

    void flush() {
        flush_pending_collection(*_schema);
        // If _ready is already set we have a bug: get_mutation_fragment()
        // was not called, and below we will lose one clustering row!
        assert(!_ready);
        if (!_skip_clustering_row) {
            _ready = move_and_disengage(_in_progress);
        } else {
            _in_progress = { };
            _ready = { };
        }
        _skip_clustering_row = false;
    }

    proceed flush_if_needed_for_range_tombstone() {
        proceed ret = proceed::yes;
        if (_in_progress) {
            ret = _skip_clustering_row ? proceed::yes : proceed::no;
            flush();
        }
        return ret;
    }


    proceed flush_if_needed(bool is_static, position_in_partition&& pos) {
        position_in_partition::equal_compare eq(*_schema);
        proceed ret = proceed::yes;
        if (_in_progress && !eq(*_in_progress, pos)) {
            ret = _skip_clustering_row ? proceed::yes : proceed::no;
            flush();
        }
        if (!_in_progress) {
            _skip_clustering_row = !is_static && !_filter(pos.key());
            if (is_static) {
                _in_progress = mutation_fragment(static_row());
            } else {
                _in_progress = mutation_fragment(clustering_row(std::move(pos.key())));
            }
        }
        return ret;
    }

    proceed flush_if_needed(bool is_static, const exploded_clustering_prefix& ecp) {
        auto pos = [&] {
            if (is_static) {
                return position_in_partition(position_in_partition::static_row_tag_t());
            } else {
                auto ck = clustering_key_prefix::from_clustering_prefix(*_schema, ecp);
                return position_in_partition(position_in_partition::clustering_row_tag_t(), std::move(ck));
            }
        }();
        return flush_if_needed(is_static, std::move(pos));
    }

    proceed flush_if_needed(clustering_key_prefix&& ck) {
        return flush_if_needed(false, position_in_partition(position_in_partition::clustering_row_tag_t(), std::move(ck)));
    }

    atomic_cell make_atomic_cell(uint64_t timestamp, bytes_view value, uint32_t ttl, uint32_t expiration) {
        if (ttl) {
            return atomic_cell::make_live(timestamp, value,
                gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
        } else {
            return atomic_cell::make_live(timestamp, value);
        }
    }

    virtual proceed consume_cell(bytes_view col_name, bytes_view value, int64_t timestamp, int32_t ttl, int32_t expiration) override {
        if (_skip_partition) {
            return proceed::yes;
        }

        struct column col(*_schema, col_name);

        auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));
        auto ret = flush_if_needed(col.is_static, clustering_prefix);
        if (_skip_clustering_row) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(timestamp, gc_clock::duration(ttl), gc_clock::time_point(gc_clock::duration(expiration)));
            _in_progress->as_clustering_row().apply(std::move(rm));
            return ret;
        }

        if (!col.is_present(timestamp)) {
            return ret;
        }

        auto ac = make_atomic_cell(timestamp, value, ttl, expiration);

        bool is_multi_cell = col.collection_extra_data.size();
        if (is_multi_cell != col.cdef->type->is_multi_cell()) {
            return ret;
        }
        if (is_multi_cell) {
            update_pending_collection(col.cdef, std::move(col.collection_extra_data), std::move(ac));
            return ret;
        }

        if (col.is_static) {
            _in_progress->as_static_row().set_cell(*(col.cdef), std::move(ac));
            return ret;
        }
        _in_progress->as_clustering_row().set_cell(*(col.cdef), atomic_cell_or_collection(std::move(ac)));
        return ret;
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        if (_skip_partition) {
            return proceed::yes;
        }

        struct column col(*_schema, col_name);
        gc_clock::duration secs(deltime.local_deletion_time);

        return consume_deleted_cell(col, deltime.marked_for_delete_at, gc_clock::time_point(secs));
    }

    proceed consume_deleted_cell(column &col, int64_t timestamp, gc_clock::time_point ttl) {
        auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));
        auto ret = flush_if_needed(col.is_static, clustering_prefix);
        if (_skip_clustering_row) {
            return ret;
        }

        if (col.cell.size() == 0) {
            row_marker rm(tombstone(timestamp, ttl));
            _in_progress->as_clustering_row().apply(rm);
            return ret;
        }
        if (!col.is_present(timestamp)) {
            return ret;
        }

        auto ac = atomic_cell::make_dead(timestamp, ttl);

        bool is_multi_cell = col.collection_extra_data.size();
        if (is_multi_cell != col.cdef->type->is_multi_cell()) {
            return ret;
        }

        if (is_multi_cell) {
            update_pending_collection(col.cdef, std::move(col.collection_extra_data), std::move(ac));
        } else if (col.is_static) {
            _in_progress->as_static_row().set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
        } else {
            _in_progress->as_clustering_row().set_cell(*col.cdef, atomic_cell_or_collection(std::move(ac)));
        }
        return ret;
    }
    virtual proceed consume_row_end() override {
        flush();
        _is_mutation_end = true;
        return proceed::no;
    }

    static bound_kind start_marker_to_bound_kind(bytes_view component) {
        auto found = composite::eoc(component.back());
        switch (found) {
        // start_col may have composite_marker::none in sstables
        // from older versions of Cassandra (see CASSANDRA-7593).
        case composite::eoc::none:
            return bound_kind::incl_start;
        case composite::eoc::start:
            return bound_kind::incl_start;
        case composite::eoc::end:
            return bound_kind::excl_start;
        default:
            throw malformed_sstable_exception(sprint("Unexpected start composite marker %d\n", uint16_t(uint8_t(found))));
        }
    }

    static bound_kind end_marker_to_bound_kind(bytes_view component) {
        auto found = composite::eoc(component.back());
        switch (found) {
        // start_col may have composite_marker::none in sstables
        // from older versions of Cassandra (see CASSANDRA-7593).
        case composite::eoc::none:
            return bound_kind::incl_end;
        case composite::eoc::start:
            return bound_kind::excl_end;
        case composite::eoc::end:
            return bound_kind::incl_end;
        default:
            throw malformed_sstable_exception(sprint("Unexpected start composite marker %d\n", uint16_t(uint8_t(found))));
        }
    }

    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {

        if (_skip_partition) {
            return proceed::yes;
        }

        auto start = composite_view(column::fix_static_name(start_col)).explode();

        // Note how this is slightly different from the check in is_collection. Collection tombstones
        // do not have extra data.
        //
        // Still, it is enough to check if we're dealing with a collection, since any other tombstone
        // won't have a full clustering prefix (otherwise it isn't a range)
        if (start.size() <= _schema->clustering_key_size()) {
            auto start_ck = clustering_key_prefix::from_exploded(std::move(start));
            auto start_kind = start_marker_to_bound_kind(start_col);
            auto end = clustering_key_prefix::from_exploded(composite_view(column::fix_static_name(end_col)).explode());
            auto end_kind = end_marker_to_bound_kind(end_col);
            if (range_tombstone::is_single_clustering_row_tombstone(*_schema, start_ck, start_kind, end, end_kind)) {
                auto ret = flush_if_needed(std::move(start_ck));
                if (!_skip_clustering_row) {
                    _in_progress->as_clustering_row().apply(tombstone(deltime));
                }
                return ret;
            } else {
                auto rt = range_tombstone(std::move(start_ck), start_kind, std::move(end), end_kind, tombstone(deltime));
                if (flush_if_needed_for_range_tombstone() == proceed::yes) {
                    _ready = mutation_fragment(std::move(rt));
                } else {
                    _in_progress = mutation_fragment(std::move(rt));
                }
                return proceed::no;
            }
        } else {
            auto&& column = pop_back(start);
            auto cdef = _schema->get_column_definition(column);
            if (cdef && cdef->type->is_multi_cell() && deltime.marked_for_delete_at > cdef->dropped_at()) {
                auto ret = flush_if_needed(cdef->is_static(), exploded_clustering_prefix(std::move(start)));
                if (!_skip_clustering_row) {
                    update_pending_collection(cdef, tombstone(deltime));
                }
                return ret;
            }
        }
        return proceed::yes;
    }
    virtual const io_priority_class& io_priority() override {
        assert (_pc != nullptr);
        return *_pc;
    }

    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    stdx::optional<new_mutation> get_mutation() {
        return move_and_disengage(_mutation);
    }

    mutation_fragment_opt get_mutation_fragment() {
        return move_and_disengage(_ready);
    }

    void skip_partition() {
        _pending_collection = { };
        _in_progress = { };
        _ready = { };

        _skip_partition = true;
    }
};

class sstable_streamed_mutation : public streamed_mutation::impl {
    const schema& _schema;
    data_consume_context& _context;
    mp_row_consumer& _consumer;
    tombstone _t;
    bool _finished = false;
    range_tombstone_stream _range_tombstones;
    mutation_fragment_opt _current_candidate;
    mutation_fragment_opt _next_candidate;
    stdx::optional<position_in_partition> _last_position;
    position_in_partition::less_compare _cmp;
    position_in_partition::equal_compare _eq;
private:
    future<stdx::optional<mutation_fragment_opt>> read_next() {
        // Because of #1203 we may encounter sstables with range tombstones
        // placed earler than expected.
        if (_next_candidate || (_current_candidate && _finished)) {
            assert(_current_candidate);
            auto mf = _range_tombstones.get_next(*_current_candidate);
            if (!mf) {
                mf = move_and_disengage(_current_candidate);
                _current_candidate = move_and_disengage(_next_candidate);
            }
            return make_ready_future<stdx::optional<mutation_fragment_opt>>(std::move(mf));
        }
        if (_finished) {
            // No need to update _last_position here. We've already read everything from the sstable.
            return make_ready_future<stdx::optional<mutation_fragment_opt>>(_range_tombstones.get_next());
        }
        return _context.read().then([this] {
            if (_consumer.is_mutation_end()) {
                _finished = true;
            }
            auto mf = _consumer.get_mutation_fragment();
            if (mf) {
                if (mf->is_range_tombstone()) {
                    // If sstable uses promoted index it will repeat relevant range tombstones in
                    // each block. Do not emit these duplicates as they will break the guarantee
                    // that mutation fragment are produced in ascending order.
                    if (!_last_position || !_cmp(*mf, *_last_position)) {
                        _last_position = mf->position();
                        _range_tombstones.apply(std::move(mf->as_range_tombstone()));
                    }
                } else {
                    // mp_row_consumer may produce mutation_fragments in parts if they are
                    // interrupted by range tombstone duplicate. Make sure they are merged
                    // before emitting them.
                    _last_position = mf->position();
                    if (!_current_candidate) {
                        _current_candidate = std::move(mf);
                    } else if (_current_candidate && _eq(*_current_candidate, *mf)) {
                        _current_candidate->apply(_schema, std::move(*mf));
                    } else {
                        _next_candidate = std::move(mf);
                    }
                }
            }
            return stdx::optional<mutation_fragment_opt>();
        });
    }
public:
    sstable_streamed_mutation(schema_ptr s, dht::decorated_key dk, data_consume_context& context, mp_row_consumer& consumer, tombstone t)
        : streamed_mutation::impl(s, std::move(dk), t), _schema(*s), _context(context), _consumer(consumer), _t(t), _range_tombstones(*s), _cmp(*s), _eq(*s) { }

    virtual future<> fill_buffer() final override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return repeat_until_value([this] {
                return read_next();
            }).then([this] (mutation_fragment_opt&& mfopt) {
                if (!mfopt) {
                    _end_of_stream = true;
                } else {
                    push_mutation_fragment(std::move(*mfopt));
                }
            });
        });
    }
};

class sstable_single_streamed_mutation final : public sstable_streamed_mutation {
    struct data_source {
        mp_row_consumer _consumer;
        data_consume_context _context;

        data_source(schema_ptr s, sstable& sst, const sstables::key& k, const io_priority_class& pc,
                    query::clustering_key_filtering_context ck_filtering, uint64_t start, uint64_t end)
            : _consumer(k, s, ck_filtering, pc)
            , _context(sst.data_consume_rows(_consumer, start, end))
        {
        }
    };

    lw_shared_ptr<data_source> _data_source;
public:
    sstable_single_streamed_mutation(schema_ptr s, dht::decorated_key dk, tombstone t, lw_shared_ptr<data_source> ds)
        : sstable_streamed_mutation(std::move(s), std::move(dk), ds->_context, ds->_consumer, t)
        , _data_source(ds)
    { }

    static future<streamed_mutation> create(schema_ptr s, sstable& sst, const sstables::key& k,
                                            query::clustering_key_filtering_context ck_filtering,
                                            const io_priority_class& pc, uint64_t start, uint64_t end)
    {
        auto ds = make_lw_shared<data_source>(s, sst, k, pc, ck_filtering, start, end);
        return ds->_context.read().then([s, ds] {
            auto mut = ds->_consumer.get_mutation();
            assert(mut);
            auto dk = dht::global_partitioner().decorate_key(*s, std::move(mut->key));
            return make_streamed_mutation<sstable_single_streamed_mutation>(s, std::move(dk), mut->tomb, ds);
        });
    }
};

static int adjust_binary_search_index(int idx) {
    if (idx < 0) {
        // binary search gives us the first index _greater_ than the key searched for,
        // i.e., its insertion position
        auto gt = (idx + 1) * -1;
        idx = gt - 1;
    }
    return idx;
}

future<uint64_t> sstables::sstable::data_end_position(uint64_t summary_idx, uint64_t index_idx, const index_list& il,
                                                      const io_priority_class& pc) {
    if (uint64_t(index_idx + 1) < il.size()) {
        return make_ready_future<uint64_t>(il[index_idx + 1].position());
    }

    return data_end_position(summary_idx, pc);
}

future<uint64_t> sstables::sstable::data_end_position(uint64_t summary_idx, const io_priority_class& pc) {
    // We should only go to the end of the file if we are in the last summary group.
    // Otherwise, we will determine the end position of the current data read by looking
    // at the first index in the next summary group.
    if (size_t(summary_idx + 1) >= _summary.entries.size()) {
        return make_ready_future<uint64_t>(data_size());
    }

    return read_indexes(summary_idx + 1, pc).then([] (auto next_il) {
        return next_il.front().position();
    });
}

future<streamed_mutation_opt>
sstables::sstable::read_row(schema_ptr schema,
                            const sstables::key& key,
                            query::clustering_key_filtering_context ck_filtering,
                            const io_priority_class& pc) {

    assert(schema);

    if (!filter_has_key(key)) {
        return make_ready_future<streamed_mutation_opt>();
    }

    auto& partitioner = dht::global_partitioner();
    auto token = partitioner.get_token(key_view(key));

    auto& summary = _summary;

    if (token < partitioner.get_token(key_view(summary.first_key.value))
            || token > partitioner.get_token(key_view(summary.last_key.value))) {
        _filter_tracker.add_false_positive();
        return make_ready_future<streamed_mutation_opt>();
    }

    auto summary_idx = adjust_binary_search_index(binary_search(summary.entries, key, token));
    if (summary_idx < 0) {
        _filter_tracker.add_false_positive();
        return make_ready_future<streamed_mutation_opt>();
    }

    return read_indexes(summary_idx, pc).then([this, schema, ck_filtering, &key, token, summary_idx, &pc] (auto index_list) {
        auto index_idx = this->binary_search(index_list, key, token);
        if (index_idx < 0) {
            _filter_tracker.add_false_positive();
            return make_ready_future<streamed_mutation_opt>();
        }
        _filter_tracker.add_true_positive();

        auto position = index_list[index_idx].position();
        return this->data_end_position(summary_idx, index_idx, index_list, pc).then([&key, schema, ck_filtering, this, position, &pc] (uint64_t end) {
            return sstable_single_streamed_mutation::create(schema, *this, key, ck_filtering, pc, position, end).then([] (auto sm) {
                return streamed_mutation_opt(std::move(sm));
            });
        });
    });
}

class mutation_reader::impl {
private:
    schema_ptr _schema;
    mp_row_consumer _consumer;
    std::experimental::optional<data_consume_context> _context;
    std::function<future<data_consume_context> ()> _get_context;
public:
    impl(sstable& sst, schema_ptr schema, uint64_t start, uint64_t end,
         const io_priority_class &pc)
        : _schema(schema)
        , _consumer(schema, query::no_clustering_key_filtering, pc)
        , _get_context([&sst, this, start, end] {
            return make_ready_future<data_consume_context>(sst.data_consume_rows(_consumer, start, end));
        }) { }
    impl(sstable& sst, schema_ptr schema,
         const io_priority_class &pc)
        : _schema(schema)
        , _consumer(schema, query::no_clustering_key_filtering, pc)
        , _get_context([this, &sst] {
            return make_ready_future<data_consume_context>(sst.data_consume_rows(_consumer));
        }) { }
    impl(sstable& sst,
         schema_ptr schema,
         std::function<future<uint64_t>()> start,
         std::function<future<uint64_t>()> end,
         query::clustering_key_filtering_context ck_filtering,
         const io_priority_class& pc)
        : _schema(schema)
        , _consumer(schema, ck_filtering, pc)
        , _get_context([this, &sst, start = std::move(start), end = std::move(end)] () {
            return start().then([this, &sst, end = std::move(end)] (uint64_t start) {
                return end().then([this, &sst, start] (uint64_t end) {
                    return make_ready_future<data_consume_context>(sst.data_consume_rows(_consumer, start, end));
                });
            });
        }) { }
    impl() : _consumer(), _get_context() { }

    // Reference to _consumer is passed to data_consume_rows() in the constructor so we must not allow move/copy
    impl(impl&&) = delete;
    impl(const impl&) = delete;

    future<streamed_mutation_opt> read() {
        if (!_get_context) {
            // empty mutation reader returns EOF immediately
            return make_ready_future<streamed_mutation_opt>();
        }

        if (_context) {
            return do_read();
        }
        return (_get_context)().then([this] (data_consume_context context) {
            _context = std::move(context);
            return do_read();
        });
    }
private:
    future<streamed_mutation_opt> do_read() {
        return _context->read().then([this] {
            auto mut = _consumer.get_mutation();
            if (!mut) {
                if (_consumer.get_mutation_fragment()) {
                    // We are still in the middle of the previous mutation.
                    _consumer.skip_partition();
                    return do_read();
                } else {
                    return make_ready_future<streamed_mutation_opt>();
                }
            }
            auto dk = dht::global_partitioner().decorate_key(*_schema, std::move(mut->key));
            auto sm = make_streamed_mutation<sstable_streamed_mutation>(_schema, std::move(dk), *_context, _consumer, mut->tomb);
            return make_ready_future<streamed_mutation_opt>(std::move(sm));
        });
    }
};

mutation_reader::~mutation_reader() = default;
mutation_reader::mutation_reader(mutation_reader&&) = default;
mutation_reader& mutation_reader::operator=(mutation_reader&&) = default;
mutation_reader::mutation_reader(std::unique_ptr<impl> p)
    : _pimpl(std::move(p)) { }
future<streamed_mutation_opt> mutation_reader::read() {
    return _pimpl->read();
}

mutation_reader sstable::read_rows(schema_ptr schema, const io_priority_class& pc) {
    return std::make_unique<mutation_reader::impl>(*this, schema, pc);
}

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

future<uint64_t> sstable::lower_bound(schema_ptr s, const dht::ring_position& pos, const io_priority_class& pc) {
    uint64_t summary_idx = std::distance(std::begin(_summary.entries),
        std::lower_bound(_summary.entries.begin(), _summary.entries.end(), pos, index_comparator(*s)));

    if (summary_idx == 0) {
        return make_ready_future<uint64_t>(0);
    }

    --summary_idx;

    return read_indexes(summary_idx, pc).then([this, s, pos, summary_idx, &pc] (index_list il) {
        auto i = std::lower_bound(il.begin(), il.end(), pos, index_comparator(*s));
        if (i == il.end()) {
            return this->data_end_position(summary_idx, pc);
        }
        return make_ready_future<uint64_t>(i->position());
    });
}

future<uint64_t> sstable::upper_bound(schema_ptr s, const dht::ring_position& pos, const io_priority_class& pc) {
    uint64_t summary_idx = std::distance(std::begin(_summary.entries),
        std::upper_bound(_summary.entries.begin(), _summary.entries.end(), pos, index_comparator(*s)));

    if (summary_idx == 0) {
        return make_ready_future<uint64_t>(0);
    }

    --summary_idx;

    return read_indexes(summary_idx, pc).then([this, s, pos, summary_idx, &pc] (index_list il) {
        auto i = std::upper_bound(il.begin(), il.end(), pos, index_comparator(*s));
        if (i == il.end()) {
            return this->data_end_position(summary_idx, pc);
        }
        return make_ready_future<uint64_t>(i->position());
    });
}

mutation_reader sstable::read_range_rows(schema_ptr schema,
        const dht::token& min_token, const dht::token& max_token, const io_priority_class& pc) {
    if (max_token < min_token) {
        return std::make_unique<mutation_reader::impl>();
    }
    return read_range_rows(std::move(schema),
        query::range<dht::ring_position>::make(
            dht::ring_position::starting_at(min_token),
            dht::ring_position::ending_at(max_token)), query::no_clustering_key_filtering, pc);
}

mutation_reader
sstable::read_range_rows(schema_ptr schema,
                         const query::partition_range& range,
                         query::clustering_key_filtering_context ck_filtering,
                         const io_priority_class& pc) {
    if (query::is_wrap_around(range, *schema)) {
        fail(unimplemented::cause::WRAP_AROUND);
    }

    auto start = [this, range, schema, &pc] {
        return range.start() ? (range.start()->is_inclusive()
                 ? lower_bound(schema, range.start()->value(), pc)
                 : upper_bound(schema, range.start()->value(), pc))
        : make_ready_future<uint64_t>(0);
    };

    auto end = [this, range, schema, &pc] {
        return range.end() ? (range.end()->is_inclusive()
                 ? upper_bound(schema, range.end()->value(), pc)
                 : lower_bound(schema, range.end()->value(), pc))
        : make_ready_future<uint64_t>(data_size());
    };

    return std::make_unique<mutation_reader::impl>(
        *this, std::move(schema), std::move(start), std::move(end), ck_filtering, pc);
}


class key_reader final : public ::key_reader::impl {
    schema_ptr _s;
    shared_sstable _sst;
    index_list _bucket;
    int64_t _current_bucket_id;
    int64_t _end_bucket_id;
    int64_t _begin_bucket_id;
    int64_t _position_in_bucket = 0;
    int64_t _end_of_bucket = 0;
    query::partition_range _range;
    const io_priority_class& _pc;
private:
    dht::decorated_key decorate(const index_entry& ie) {
        auto pk = partition_key::from_exploded(*_s, ie.get_key().explode(*_s));
        return dht::global_partitioner().decorate_key(*_s, std::move(pk));
    }
public:
    key_reader(schema_ptr s, shared_sstable sst, const query::partition_range& range, const io_priority_class& pc)
        : _s(s), _sst(std::move(sst)), _range(range), _pc(pc)
    {
        auto& summary = _sst->_summary;
        using summary_entries_type = std::decay_t<decltype(summary.entries)>;

        _begin_bucket_id = 0;
        if (range.start()) {
            summary_entries_type::iterator pos;
            if (range.start()->is_inclusive()) {
                pos = std::lower_bound(summary.entries.begin(), summary.entries.end(),
                    range.start()->value(), index_comparator(*s));

            } else {
                pos = std::upper_bound(summary.entries.begin(), summary.entries.end(),
                    range.start()->value(), index_comparator(*s));
            }
            _begin_bucket_id = std::distance(summary.entries.begin(), pos);
            if (_begin_bucket_id) {
                _begin_bucket_id--;
            }
        }
        _current_bucket_id = _begin_bucket_id - 1;

        _end_bucket_id = summary.header.size;
        if (range.end()) {
            summary_entries_type::iterator pos;
            if (range.end()->is_inclusive()) {
                pos = std::upper_bound(summary.entries.begin(), summary.entries.end(),
                    range.end()->value(), index_comparator(*s));
            } else {
                pos = std::lower_bound(summary.entries.begin(), summary.entries.end(),
                    range.end()->value(), index_comparator(*s));
            }
            _end_bucket_id = std::distance(summary.entries.begin(), pos);
            if (_end_bucket_id) {
                _end_bucket_id--;
            }
        }
    }
    virtual future<dht::decorated_key_opt> operator()() override;
};

future<dht::decorated_key_opt> key_reader::operator()()
{
    if (_position_in_bucket < _end_of_bucket) {
        auto& ie = _bucket[_position_in_bucket++];
        return make_ready_future<dht::decorated_key_opt>(decorate(ie));
    }
    if (_current_bucket_id == _end_bucket_id) {
        return make_ready_future<dht::decorated_key_opt>();
    }
    return _sst->read_indexes(++_current_bucket_id, _pc).then([this] (index_list il) mutable {
        _bucket = std::move(il);

        if (_range.start() && _current_bucket_id == _begin_bucket_id) {
            index_list::const_iterator pos;
            if (_range.start()->is_inclusive()) {
                pos = std::lower_bound(_bucket.begin(), _bucket.end(), _range.start()->value(), index_comparator(*_s));
            } else {
                pos = std::upper_bound(_bucket.begin(), _bucket.end(), _range.start()->value(), index_comparator(*_s));
            }
            _position_in_bucket = std::distance(_bucket.cbegin(), pos);
        } else {
            _position_in_bucket = 0;
        }

        if (_range.end() && _current_bucket_id == _end_bucket_id) {
            index_list::const_iterator pos;
            if (_range.end()->is_inclusive()) {
                pos = std::upper_bound(_bucket.begin(), _bucket.end(), _range.end()->value(), index_comparator(*_s));
            } else {
                pos = std::lower_bound(_bucket.begin(), _bucket.end(), _range.end()->value(), index_comparator(*_s));
            }
            _end_of_bucket = std::distance(_bucket.cbegin(), pos);
        } else {
            _end_of_bucket = _bucket.size();
        }

        return operator()();
    });
}

::key_reader make_key_reader(schema_ptr s, shared_sstable sst, const query::partition_range& range, const io_priority_class& pc)
{
    return ::make_key_reader<key_reader>(std::move(s), std::move(sst), range, pc);
}

}
