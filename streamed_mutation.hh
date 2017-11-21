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
#include "utils/optimized_optional.hh"
#include "position_in_partition.hh"

#include <experimental/optional>
#include <seastar/util/gcc6-concepts.hh>

#include "stdx.hh"
#include "seastar/core/future-util.hh"

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
    clustering_row(const rows_entry& re)
            : clustering_row(re.key(), re.row().deleted_at(), re.row().marker(), re.row().cells()) { }
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

    size_t external_memory_usage() const {
        return _ck.external_memory_usage() + _cells.external_memory_usage();
    }

    size_t memory_usage() const {
        return sizeof(clustering_row) + external_memory_usage();
    }

    bool equal(const schema& s, const clustering_row& other) const {
        return _ck.equal(s, other._ck)
               && _t == other._t
               && _marker == other._marker
               && _cells.equal(column_kind::static_column, s, other._cells, s);
    }

    friend std::ostream& operator<<(std::ostream& os, const clustering_row& row);
};

class static_row {
    row _cells;
public:
    static_row() = default;
    explicit static_row(const row& r) : _cells(r) { }
    explicit static_row(row&& r) : _cells(std::move(r)) { }

    row& cells() { return _cells; }
    const row& cells() const { return _cells; }

    bool empty() const {
        return _cells.empty();
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

    size_t external_memory_usage() const {
        return _cells.external_memory_usage();
    }

    size_t memory_usage() const {
        return sizeof(static_row) + external_memory_usage();
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
    tombstone partition_tombstone() const { return _partition_tombstone; }

    position_in_partition_view position() const;

    size_t external_memory_usage() const {
        return _key.external_memory_usage();
    }

    size_t memory_usage() const {
        return sizeof(partition_start) + external_memory_usage();
    }

    bool equal(const schema& s, const partition_start& other) const {
        return _key.equal(s, other._key) && _partition_tombstone == other._partition_tombstone;
    }

    friend std::ostream& operator<<(std::ostream& is, const partition_start& row);
};

class partition_end final {
public:
    position_in_partition_view position() const;

    size_t external_memory_usage() const {
        return 0;
    }

    size_t memory_usage() const {
        return sizeof(partition_end) + external_memory_usage();
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
    mutation_fragment(static_row&& r);
    mutation_fragment(clustering_row&& r);
    mutation_fragment(range_tombstone&& r);
    mutation_fragment(partition_start&& r);
    mutation_fragment(partition_end&& r);

    mutation_fragment(const mutation_fragment& o)
        : _kind(o._kind), _data(std::make_unique<data>()) {
        switch(_kind) {
            case kind::static_row:
                new (&_data->_static_row) static_row(o._data->_static_row);
                break;
            case kind::clustering_row:
                new (&_data->_clustering_row) clustering_row(o._data->_clustering_row);
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
    mutation_fragment& operator=(const mutation_fragment& other) {
        if (this != &other) {
            mutation_fragment copy(other);
            this->~mutation_fragment();
            new (this) mutation_fragment(std::move(copy));
        }
        return *this;
    }
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

    // Requirements: mutation_fragment_kind() == mf.mutation_fragment_kind() && !is_range_tombstone()
    void apply(const schema& s, mutation_fragment&& mf);

    template<typename Consumer>
    GCC6_CONCEPT(
        requires MutationFragmentConsumer<Consumer, decltype(auto)>()
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

    template<typename Consumer>
    GCC6_CONCEPT(
        requires StreamedMutationConsumer<Consumer>()
    )
    decltype(auto) consume_streamed_mutation(Consumer& consumer) && {
        switch (_kind) {
            case kind::static_row:
                return consumer.consume(std::move(_data->_static_row));
            case kind::clustering_row:
                return consumer.consume(std::move(_data->_clustering_row));
            case kind::range_tombstone:
                return consumer.consume(std::move(_data->_range_tombstone));
            case kind::partition_start:
                abort();
            case kind::partition_end:
                abort();
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

    size_t memory_usage() const {
        if (!_data->_size_in_bytes) {
            _data->_size_in_bytes = sizeof(data) + visit([] (auto& mf) -> size_t { return mf.external_memory_usage(); });
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

template<>
struct move_constructor_disengages<mutation_fragment> {
    enum { value = true };
};
using mutation_fragment_opt = optimized_optional<mutation_fragment>;

// streamed_mutation represents a mutation in a form of a stream of
// mutation_fragments. streamed_mutation emits mutation fragments in the order
// they should appear in the sstables, i.e. static row is always the first one,
// then clustering rows and range tombstones are emitted according to the
// lexicographical ordering of their clustering keys and bounds of the range
// tombstones.
//
// The ordering of mutation_fragments also guarantees that by the time the
// consumer sees a clustering row it has already received all relevant tombstones.
//
// Partition key and partition tombstone are not streamed and is part of the
// streamed_mutation itself.
class streamed_mutation {
public:
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

    // streamed_mutation uses batching. The mutation implementations are
    // supposed to fill a buffer with mutation fragments until is_buffer_full()
    // or end of stream is encountered.
    class impl {
        circular_buffer<mutation_fragment> _buffer;
        size_t _buffer_size = 0;
    protected:
        size_t max_buffer_size_in_bytes = 8 * 1024;

        schema_ptr _schema;
        dht::decorated_key _key;
        tombstone _partition_tombstone;

        bool _end_of_stream = false;

        friend class streamed_mutation;
    protected:
        template<typename... Args>
        void push_mutation_fragment(Args&&... args) {
            _buffer.emplace_back(std::forward<Args>(args)...);
            _buffer_size += _buffer.back().memory_usage();
        }
    public:
        // When succeeds, makes sure that the next push_mutation_fragment() will not fail.
        void reserve_one() {
            if (_buffer.capacity() == _buffer.size()) {
                _buffer.reserve(_buffer.size() * 2 + 1);
            }
        }
        explicit impl(schema_ptr s, dht::decorated_key dk, tombstone pt)
            : _schema(std::move(s)), _key(std::move(dk)), _partition_tombstone(pt)
        { }

        virtual ~impl() { }
        virtual future<> fill_buffer() = 0;

        // See streamed_mutation::fast_forward_to().
        virtual future<> fast_forward_to(position_range) {
            throw std::bad_function_call(); // FIXME: make pure virtual after implementing everywhere.
        }

        bool is_end_of_stream() const { return _end_of_stream; }
        bool is_buffer_empty() const { return _buffer.empty(); }
        bool is_buffer_full() const { return _buffer_size >= max_buffer_size_in_bytes; }

        mutation_fragment pop_mutation_fragment() {
            auto mf = std::move(_buffer.front());
            _buffer.pop_front();
            _buffer_size -= mf.memory_usage();
            return mf;
        }

        future<mutation_fragment_opt> operator()() {
            if (is_buffer_empty()) {
                if (is_end_of_stream()) {
                    return make_ready_future<mutation_fragment_opt>();
                }
                return fill_buffer().then([this] { return operator()(); });
            }
            return make_ready_future<mutation_fragment_opt>(pop_mutation_fragment());
        }

        // Removes all fragments from the buffer which are not relevant for any range starting at given position.
        // It is assumed that pos is greater than positions of fragments already in the buffer.
        void forward_buffer_to(const position_in_partition& pos);
    };
private:
    std::unique_ptr<impl> _impl;

    streamed_mutation() = default;
    explicit operator bool() const { return bool(_impl); }
    friend class optimized_optional<streamed_mutation>;
public:
    explicit streamed_mutation(std::unique_ptr<impl> i)
        : _impl(std::move(i)) { }

    const partition_key& key() const { return _impl->_key.key(); }
    const dht::decorated_key& decorated_key() const { return _impl->_key; }

    const schema_ptr& schema() const { return _impl->_schema; }

    tombstone partition_tombstone() const { return _impl->_partition_tombstone; }

    bool is_end_of_stream() const { return _impl->is_end_of_stream(); }
    bool is_buffer_empty() const { return _impl->is_buffer_empty(); }
    bool is_buffer_full() const { return _impl->is_buffer_full(); }

    mutation_fragment pop_mutation_fragment() { return _impl->pop_mutation_fragment(); }

    future<> fill_buffer() { return _impl->fill_buffer(); }

    // Skips to a later range of rows.
    // The new range must not overlap with the current range.
    //
    // See docs of streamed_mutation::forwarding for semantics.
    future<> fast_forward_to(position_range pr) {
        return _impl->fast_forward_to(std::move(pr));
    }

    future<mutation_fragment_opt> operator()() {
        return _impl->operator()();
    }

    void set_max_buffer_size(size_t size) {
        _impl->max_buffer_size_in_bytes = size;
    }
};

// Adapts streamed_mutation to a streamed_mutation which is in forwarding mode.
streamed_mutation make_forwardable(streamed_mutation);

std::ostream& operator<<(std::ostream& os, const streamed_mutation& sm);

template<typename Impl, typename... Args>
streamed_mutation make_streamed_mutation(Args&&... args) {
    return streamed_mutation(std::make_unique<Impl>(std::forward<Args>(args)...));
}

template<>
struct move_constructor_disengages<streamed_mutation> {
    enum { value = true };
};
using streamed_mutation_opt = optimized_optional<streamed_mutation>;

template<typename Consumer>
GCC6_CONCEPT(
    requires StreamedMutationConsumer<Consumer>()
)
auto consume(streamed_mutation& m, Consumer consumer) {
    return do_with(std::move(consumer), [&m] (Consumer& c) {
        if (c.consume(m.partition_tombstone()) == stop_iteration::yes) {
            return make_ready_future().then([&] { return c.consume_end_of_stream(); });
        }
        return repeat([&m, &c] {
            if (m.is_buffer_empty()) {
                if (m.is_end_of_stream()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return m.fill_buffer().then([] { return stop_iteration::no; });
            }
            return make_ready_future<stop_iteration>(m.pop_mutation_fragment().consume_streamed_mutation(c));
        }).then([&c] {
            return c.consume_end_of_stream();
        });
    });
}

class mutation;

streamed_mutation streamed_mutation_from_mutation(mutation, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);
streamed_mutation streamed_mutation_returning(schema_ptr, dht::decorated_key, std::vector<mutation_fragment>, tombstone t = {});
streamed_mutation streamed_mutation_from_forwarding_streamed_mutation(streamed_mutation&&);

//Requires all streamed_mutations to have the same schema.
streamed_mutation merge_mutations(std::vector<streamed_mutation>);

streamed_mutation make_empty_streamed_mutation(schema_ptr, dht::decorated_key, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

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
    bool _inside_range_tombstone = false;
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
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone_stream&);
};

// Consumes mutation fragments until StopCondition is true.
// The consumer will stop iff StopCondition returns true, in particular
// reaching the end of stream alone won't stop the reader.
template<typename StopCondition, typename ConsumeMutationFragment, typename ConsumeEndOfStream>
GCC6_CONCEPT(requires requires(StopCondition stop, ConsumeMutationFragment consume_mf, ConsumeEndOfStream consume_eos, mutation_fragment mf) {
    { stop() } -> bool;
    { consume_mf(std::move(mf)) } -> void;
    { consume_eos() } -> future<>;
})
future<> consume_mutation_fragments_until(streamed_mutation& sm, StopCondition&& stop,
                                          ConsumeMutationFragment&& consume_mf, ConsumeEndOfStream&& consume_eos) {
    return do_until([stop] { return stop(); }, [&sm, stop, consume_mf, consume_eos] {
        while (!sm.is_buffer_empty()) {
            consume_mf(sm.pop_mutation_fragment());
            if (stop()) {
                return make_ready_future<>();
            }
        }
        if (sm.is_end_of_stream()) {
            return consume_eos();
        }
        return sm.fill_buffer();
    });
}

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

// Creates a stream which is like sm but with transformation applied to the elements.
template<typename T>
GCC6_CONCEPT(
    requires StreamedMutationTranformer<T>()
)
streamed_mutation transform(streamed_mutation sm, T t) {
    class reader : public streamed_mutation::impl {
        streamed_mutation _sm;
        T _t;
    public:
        explicit reader(streamed_mutation sm, T&& t)
            : impl(t(sm.schema()), sm.decorated_key(), sm.partition_tombstone())
            , _sm(std::move(sm))
            , _t(std::move(t))
        { }

        virtual future<> fill_buffer() override {
            return _sm.fill_buffer().then([this] {
                while (!_sm.is_buffer_empty()) {
                    push_mutation_fragment(_t(_sm.pop_mutation_fragment()));
                }
                _end_of_stream = _sm.is_end_of_stream();
            });
        }

        virtual future<> fast_forward_to(position_range pr) override {
            _end_of_stream = false;
            forward_buffer_to(pr.start());
            return _sm.fast_forward_to(std::move(pr));
        }
    };
    return make_streamed_mutation<reader>(std::move(sm), std::move(t));
}
