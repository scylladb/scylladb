/*
 * Copyright (C) 2016 ScyllaDB
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

#include "mutation_partition.hh"
#include "position_in_partition.hh"

#include <experimental/optional>
#include <seastar/util/gcc6-concepts.hh>
#include <seastar/util/optimized_optional.hh>

#include "stdx.hh"
#include "seastar/core/future-util.hh"

#include "db/timeout_clock.hh"

// mutation_fragments are the objects that streamed_mutation are going to
// stream. They can represent:
//  - a static row
//  - a clustering row
//  - a range tombstone
//
// There exists an ordering (implemented in position_in_partition class) between
// mutation_fragment objects. It reflects the order in which content of
// partition appears in the sstables.

class clustering_row {
    clustering_key_prefix _ck;
    row_tombstone _t;
    row_marker _marker;
    row _cells;
public:
    explicit clustering_row(clustering_key_prefix ck) : _ck(std::move(ck)) { }
    clustering_row(clustering_key_prefix ck, row_tombstone t, row_marker marker, row cells)
            : _ck(std::move(ck)), _t(t), _marker(std::move(marker)), _cells(std::move(cells)) {
        _t.maybe_shadow(marker);
    }
    clustering_row(const schema& s, const clustering_row& other)
        : clustering_row(other._ck, other._t, other._marker, row(s, column_kind::regular_column, other._cells)) { }
    clustering_row(const schema& s, const rows_entry& re)
            : clustering_row(re.key(), re.row().deleted_at(), re.row().marker(), row(s, column_kind::regular_column, re.row().cells())) { }
    clustering_row(rows_entry&& re)
            : clustering_row(std::move(re.key()), re.row().deleted_at(), re.row().marker(), std::move(re.row().cells())) { }

    clustering_key_prefix& key() { return _ck; }
    const clustering_key_prefix& key() const { return _ck; }

    void remove_tombstone() { _t = {}; }
    row_tombstone tomb() const { return _t; }

    const row_marker& marker() const { return _marker; }
    row_marker& marker() { return _marker; }

    const row& cells() const { return _cells; }
    row& cells() { return _cells; }

    bool empty() const {
        return !_t && _marker.is_missing() && _cells.empty();
    }

    bool is_live(const schema& s, tombstone base_tombstone = tombstone(), gc_clock::time_point now = gc_clock::time_point::min()) const {
        base_tombstone.apply(_t.tomb());
        return _marker.is_live(base_tombstone, now) || _cells.is_live(s, column_kind::regular_column, base_tombstone, now);
    }

    void apply(const schema& s, clustering_row&& cr) {
        _marker.apply(std::move(cr._marker));
        _t.apply(cr._t, _marker);
        _cells.apply(s, column_kind::regular_column, std::move(cr._cells));
    }
    void apply(const schema& s, const clustering_row& cr) {
        _marker.apply(cr._marker);
        _t.apply(cr._t, _marker);
        _cells.apply(s, column_kind::regular_column, cr._cells);
    }
    void set_cell(const column_definition& def, atomic_cell_or_collection&& value) {
        _cells.apply(def, std::move(value));
    }
    void apply(row_marker rm) {
        _marker.apply(std::move(rm));
        _t.maybe_shadow(_marker);
    }
    void apply(tombstone t) {
        _t.apply(t);
    }
    void apply(shadowable_tombstone t) {
        _t.apply(t, _marker);
    }
    void apply(const schema& s, const rows_entry& r) {
        _marker.apply(r.row().marker());
        _t.apply(r.row().deleted_at(), _marker);
        _cells.apply(s, column_kind::regular_column, r.row().cells());
    }

    position_in_partition_view position() const;

    size_t external_memory_usage(const schema& s) const {
        return _ck.external_memory_usage() + _cells.external_memory_usage(s, column_kind::regular_column);
    }

    size_t memory_usage(const schema& s) const {
        return sizeof(clustering_row) + external_memory_usage(s);
    }

    bool equal(const schema& s, const clustering_row& other) const {
        return _ck.equal(s, other._ck)
               && _t == other._t
               && _marker == other._marker
               && _cells.equal(column_kind::regular_column, s, other._cells, s);
    }

    friend std::ostream& operator<<(std::ostream& os, const clustering_row& row);
};

class static_row {
    row _cells;
public:
    static_row() = default;
    static_row(const schema& s, const static_row& other) : static_row(s, other._cells) { }
    explicit static_row(const schema& s, const row& r) : _cells(s, column_kind::static_column, r) { }
    explicit static_row(row&& r) : _cells(std::move(r)) { }

    row& cells() { return _cells; }
    const row& cells() const { return _cells; }

    bool empty() const {
        return _cells.empty();
    }

    bool is_live(const schema& s, gc_clock::time_point now = gc_clock::time_point::min()) const {
        return _cells.is_live(s, column_kind::static_column, tombstone(), now);
    }

    void apply(const schema& s, const row& r) {
        _cells.apply(s, column_kind::static_column, r);
    }
    void apply(const schema& s, static_row&& sr) {
        _cells.apply(s, column_kind::static_column, std::move(sr._cells));
    }
    void set_cell(const column_definition& def, atomic_cell_or_collection&& value) {
        _cells.apply(def, std::move(value));
    }

    position_in_partition_view position() const;

    size_t external_memory_usage(const schema& s) const {
        return _cells.external_memory_usage(s, column_kind::static_column);
    }

    size_t memory_usage(const schema& s) const {
        return sizeof(static_row) + external_memory_usage(s);
    }

    bool equal(const schema& s, const static_row& other) const {
        return _cells.equal(column_kind::static_column, s, other._cells, s);
    }

    friend std::ostream& operator<<(std::ostream& is, const static_row& row);
};

class partition_start final {
    dht::decorated_key _key;
    tombstone _partition_tombstone;
public:
    partition_start(dht::decorated_key pk, tombstone pt)
        : _key(std::move(pk))
        , _partition_tombstone(std::move(pt))
    { }

    dht::decorated_key& key() { return _key; }
    const dht::decorated_key& key() const { return _key; }
    const tombstone& partition_tombstone() const { return _partition_tombstone; }
    tombstone& partition_tombstone() { return _partition_tombstone; }

    position_in_partition_view position() const;

    size_t external_memory_usage(const schema&) const {
        return _key.external_memory_usage();
    }

    size_t memory_usage(const schema& s) const {
        return sizeof(partition_start) + external_memory_usage(s);
    }

    bool equal(const schema& s, const partition_start& other) const {
        return _key.equal(s, other._key) && _partition_tombstone == other._partition_tombstone;
    }

    friend std::ostream& operator<<(std::ostream& is, const partition_start& row);
};

class partition_end final {
public:
    position_in_partition_view position() const;

    size_t external_memory_usage(const schema&) const {
        return 0;
    }

    size_t memory_usage(const schema& s) const {
        return sizeof(partition_end) + external_memory_usage(s);
    }

    bool equal(const schema& s, const partition_end& other) const {
        return true;
    }

    friend std::ostream& operator<<(std::ostream& is, const partition_end& row);
};

GCC6_CONCEPT(
template<typename T, typename ReturnType>
concept bool MutationFragmentConsumer() {
    return requires(T t, static_row sr, clustering_row cr, range_tombstone rt, partition_start ph, partition_end pe) {
        { t.consume(std::move(sr)) } -> ReturnType;
        { t.consume(std::move(cr)) } -> ReturnType;
        { t.consume(std::move(rt)) } -> ReturnType;
        { t.consume(std::move(ph)) } -> ReturnType;
        { t.consume(std::move(pe)) } -> ReturnType;
    };
}
)

GCC6_CONCEPT(
template<typename T, typename ReturnType>
concept bool FragmentConsumerReturning() {
    return requires(T t, static_row sr, clustering_row cr, range_tombstone rt, tombstone tomb) {
        { t.consume(std::move(sr)) } -> ReturnType;
        { t.consume(std::move(cr)) } -> ReturnType;
        { t.consume(std::move(rt)) } -> ReturnType;
    };
}
)

GCC6_CONCEPT(
template<typename T>
concept bool FragmentConsumer() {
    return FragmentConsumerReturning<T, stop_iteration >() || FragmentConsumerReturning<T, future<stop_iteration>>();
}
)

GCC6_CONCEPT(
template<typename T>
concept bool StreamedMutationConsumer() {
    return FragmentConsumer<T>() && requires(T t, static_row sr, clustering_row cr, range_tombstone rt, tombstone tomb) {
        t.consume(tomb);
        t.consume_end_of_stream();
    };
}
)

GCC6_CONCEPT(
template<typename T, typename ReturnType>
concept bool MutationFragmentVisitor() {
    return requires(T t, const static_row& sr, const clustering_row& cr, const range_tombstone& rt, const partition_start& ph, const partition_end& eop) {
        { t(sr) } -> ReturnType;
        { t(cr) } -> ReturnType;
        { t(rt) } -> ReturnType;
        { t(ph) } -> ReturnType;
        { t(eop) } -> ReturnType;
    };
}
)

class mutation_fragment {
public:
    enum class kind {
        static_row,
        clustering_row,
        range_tombstone,
        partition_start,
        partition_end,
    };
private:
    struct data {
        data() { }
        ~data() { }

        stdx::optional<size_t> _size_in_bytes;
        union {
            static_row _static_row;
            clustering_row _clustering_row;
            range_tombstone _range_tombstone;
            partition_start _partition_start;
            partition_end _partition_end;
        };
    };
private:
    kind _kind;
    std::unique_ptr<data> _data;

    mutation_fragment() = default;
    explicit operator bool() const noexcept { return bool(_data); }
    void destroy_data() noexcept;
    friend class optimized_optional<mutation_fragment>;

    friend class position_in_partition;
public:
    struct clustering_row_tag_t { };
    
    template<typename... Args>
    mutation_fragment(clustering_row_tag_t, Args&&... args)
        : _kind(kind::clustering_row)
        , _data(std::make_unique<data>())
    {
        new (&_data->_clustering_row) clustering_row(std::forward<Args>(args)...);
    }

    mutation_fragment(static_row&& r);
    mutation_fragment(clustering_row&& r);
    mutation_fragment(range_tombstone&& r);
    mutation_fragment(partition_start&& r);
    mutation_fragment(partition_end&& r);

    mutation_fragment(const schema& s, const mutation_fragment& o)
        : _kind(o._kind), _data(std::make_unique<data>()) {
        switch(_kind) {
            case kind::static_row:
                new (&_data->_static_row) static_row(s, o._data->_static_row);
                break;
            case kind::clustering_row:
                new (&_data->_clustering_row) clustering_row(s, o._data->_clustering_row);
                break;
            case kind::range_tombstone:
                new (&_data->_range_tombstone) range_tombstone(o._data->_range_tombstone);
                break;
            case kind::partition_start:
                new (&_data->_partition_start) partition_start(o._data->_partition_start);
                break;
            case kind::partition_end:
                new (&_data->_partition_end) partition_end(o._data->_partition_end);
                break;
        }
    }
    mutation_fragment(mutation_fragment&& other) = default;
    mutation_fragment& operator=(mutation_fragment&& other) noexcept {
        if (this != &other) {
            this->~mutation_fragment();
            new (this) mutation_fragment(std::move(other));
        }
        return *this;
    }
    [[gnu::always_inline]]
    ~mutation_fragment() {
        if (_data) {
            destroy_data();
        }
    }

    position_in_partition_view position() const;

    // Returns the range of positions for which this fragment holds relevant information.
    position_range range() const;

    // Checks if this fragment may be relevant for any range starting at given position.
    bool relevant_for_range(const schema& s, position_in_partition_view pos) const;

    // Like relevant_for_range() but makes use of assumption that pos is greater
    // than the starting position of this fragment.
    bool relevant_for_range_assuming_after(const schema& s, position_in_partition_view pos) const;

    bool has_key() const { return is_clustering_row() || is_range_tombstone(); }
    // Requirements: has_key() == true
    const clustering_key_prefix& key() const;

    kind mutation_fragment_kind() const { return _kind; }

    bool is_static_row() const { return _kind == kind::static_row; }
    bool is_clustering_row() const { return _kind == kind::clustering_row; }
    bool is_range_tombstone() const { return _kind == kind::range_tombstone; }
    bool is_partition_start() const { return _kind == kind::partition_start; }
    bool is_end_of_partition() const { return _kind == kind::partition_end; }

    static_row& as_mutable_static_row() {
        _data->_size_in_bytes = stdx::nullopt;
        return _data->_static_row;
    }
    clustering_row& as_mutable_clustering_row() {
        _data->_size_in_bytes = stdx::nullopt;
        return _data->_clustering_row;
    }
    range_tombstone& as_mutable_range_tombstone() {
        _data->_size_in_bytes = stdx::nullopt;
        return _data->_range_tombstone;
    }
    partition_start& as_mutable_partition_start() {
        _data->_size_in_bytes = stdx::nullopt;
        return _data->_partition_start;
    }
    partition_end& as_mutable_end_of_partition() {
        _data->_size_in_bytes = stdx::nullopt;
        return _data->_partition_end;
    }

    static_row&& as_static_row() && { return std::move(_data->_static_row); }
    clustering_row&& as_clustering_row() && { return std::move(_data->_clustering_row); }
    range_tombstone&& as_range_tombstone() && { return std::move(_data->_range_tombstone); }
    partition_start&& as_partition_start() && { return std::move(_data->_partition_start); }
    partition_end&& as_end_of_partition() && { return std::move(_data->_partition_end); }

    const static_row& as_static_row() const & { return _data->_static_row; }
    const clustering_row& as_clustering_row() const & { return _data->_clustering_row; }
    const range_tombstone& as_range_tombstone() const & { return _data->_range_tombstone; }
    const partition_start& as_partition_start() const & { return _data->_partition_start; }
    const partition_end& as_end_of_partition() const & { return _data->_partition_end; }

    // Requirements: mergeable_with(mf)
    void apply(const schema& s, mutation_fragment&& mf);

    template<typename Consumer>
    GCC6_CONCEPT(
        requires MutationFragmentConsumer<Consumer, decltype(std::declval<Consumer>().consume(std::declval<range_tombstone>()))>()
    )
    decltype(auto) consume(Consumer& consumer) && {
        switch (_kind) {
        case kind::static_row:
            return consumer.consume(std::move(_data->_static_row));
        case kind::clustering_row:
            return consumer.consume(std::move(_data->_clustering_row));
        case kind::range_tombstone:
            return consumer.consume(std::move(_data->_range_tombstone));
        case kind::partition_start:
            return consumer.consume(std::move(_data->_partition_start));
        case kind::partition_end:
            return consumer.consume(std::move(_data->_partition_end));
        }
        abort();
    }

    template<typename Visitor>
    GCC6_CONCEPT(
        requires MutationFragmentVisitor<Visitor, decltype(std::declval<Visitor>()(std::declval<static_row&>()))>()
    )
    decltype(auto) visit(Visitor&& visitor) const {
        switch (_kind) {
        case kind::static_row:
            return visitor(as_static_row());
        case kind::clustering_row:
            return visitor(as_clustering_row());
        case kind::range_tombstone:
            return visitor(as_range_tombstone());
        case kind::partition_start:
            return visitor(as_partition_start());
        case kind::partition_end:
            return visitor(as_end_of_partition());
        }
        abort();
    }

    size_t memory_usage(const schema& s) const {
        if (!_data->_size_in_bytes) {
            _data->_size_in_bytes = sizeof(data) + visit([&s] (auto& mf) -> size_t { return mf.external_memory_usage(s); });
        }
        return *_data->_size_in_bytes;
    }

    bool equal(const schema& s, const mutation_fragment& other) const {
        if (other._kind != _kind) {
            return false;
        }
        switch(_kind) {
        case kind::static_row:
            return as_static_row().equal(s, other.as_static_row());
        case kind::clustering_row:
            return as_clustering_row().equal(s, other.as_clustering_row());
        case kind::range_tombstone:
            return as_range_tombstone().equal(s, other.as_range_tombstone());
        case kind::partition_start:
            return as_partition_start().equal(s, other.as_partition_start());
        case kind::partition_end:
            return as_end_of_partition().equal(s, other.as_end_of_partition());
        }
        abort();
    }

    // Fragments which have the same position() and are mergeable can be
    // merged into one fragment with apply() which represents the sum of
    // writes represented by each of the fragments.
    // Fragments which have the same position() but are not mergeable
    // can be emitted one after the other in the stream.
    bool mergeable_with(const mutation_fragment& mf) const {
        return _kind == mf._kind && _kind != kind::range_tombstone;
    }

    friend std::ostream& operator<<(std::ostream&, const mutation_fragment& mf);
};

inline position_in_partition_view static_row::position() const
{
    return position_in_partition_view(position_in_partition_view::static_row_tag_t());
}

inline position_in_partition_view clustering_row::position() const
{
    return position_in_partition_view(position_in_partition_view::clustering_row_tag_t(), _ck);
}

inline position_in_partition_view partition_start::position() const
{
    return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
}

inline position_in_partition_view partition_end::position() const
{
    return position_in_partition_view(position_in_partition_view::end_of_partition_tag_t());
}

std::ostream& operator<<(std::ostream&, mutation_fragment::kind);

std::ostream& operator<<(std::ostream&, const mutation_fragment& mf);

using mutation_fragment_opt = optimized_optional<mutation_fragment>;

namespace streamed_mutation {
    // Determines whether streamed_mutation is in forwarding mode or not.
    //
    // In forwarding mode the stream does not return all fragments right away,
    // but only those belonging to the current clustering range. Initially
    // current range only covers the static row. The stream can be forwarded
    // (even before end-of- stream) to a later range with fast_forward_to().
    // Forwarding doesn't change initial restrictions of the stream, it can
    // only be used to skip over data.
    //
    // Monotonicity of positions is preserved by forwarding. That is fragments
    // emitted after forwarding will have greater positions than any fragments
    // emitted before forwarding.
    //
    // For any range, all range tombstones relevant for that range which are
    // present in the original stream will be emitted. Range tombstones
    // emitted before forwarding which overlap with the new range are not
    // necessarily re-emitted.
    //
    // When streamed_mutation is not in forwarding mode, fast_forward_to()
    // cannot be used.
    class forwarding_tag;
    using forwarding = bool_class<forwarding_tag>;
}

// range_tombstone_stream is a helper object that simplifies producing a stream
// of range tombstones and merging it with a stream of clustering rows.
// Tombstones are added using apply() and retrieved using get_next().
//
// get_next(const rows_entry&) and get_next(const mutation_fragment&) allow
// merging the stream of tombstones with a stream of clustering rows. If these
// overloads return disengaged optional it means that there is no tombstone
// in the stream that should be emitted before the object given as an argument.
// (And, consequently, if the optional is engaged that tombstone should be
// emitted first). After calling any of these overloads with a mutation_fragment
// which is at some position in partition P no range tombstone can be added to
// the stream which start bound is before that position.
//
// get_next() overload which doesn't take any arguments is used to return the
// remaining tombstones. After it was called no new tombstones can be added
// to the stream.
class range_tombstone_stream {
    const schema& _schema;
    position_in_partition::less_compare _cmp;
    range_tombstone_list _list;
private:
    mutation_fragment_opt do_get_next();
public:
    range_tombstone_stream(const schema& s) : _schema(s), _cmp(s), _list(s) { }
    mutation_fragment_opt get_next(const rows_entry&);
    mutation_fragment_opt get_next(const mutation_fragment&);
    // Returns next fragment with position before upper_bound or disengaged optional if no such fragments are left.
    mutation_fragment_opt get_next(position_in_partition_view upper_bound);
    mutation_fragment_opt get_next();
    // Forgets all tombstones which are not relevant for any range starting at given position.
    void forward_to(position_in_partition_view);

    void apply(range_tombstone&& rt) {
        _list.apply(_schema, std::move(rt));
    }
    void apply(const range_tombstone_list& list) {
        _list.apply(_schema, list);
    }
    void apply(const range_tombstone_list&, const query::clustering_range&);
    void reset();
    bool empty() const;
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone_stream&);
};

GCC6_CONCEPT(
    // F gets a stream element as an argument and returns the new value which replaces that element
    // in the transformed stream.
    template<typename F>
    concept bool StreamedMutationTranformer() {
        return requires(F f, mutation_fragment mf, schema_ptr s) {
            { f(std::move(mf)) } -> mutation_fragment
            { f(s) } -> schema_ptr
        };
    }
)