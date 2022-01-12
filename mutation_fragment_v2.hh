/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation_fragment.hh"
#include "position_in_partition.hh"

#include <optional>
#include <seastar/util/optimized_optional.hh>

#include "seastar/core/future-util.hh"

#include "db/timeout_clock.hh"
#include "reader_permit.hh"

// Mutation fragment which represents a range tombstone boundary.
//
// The range_tombstone_change::tombstone() method returns the tombstone which takes effect
// for positions >= range_tombstone_change::position() in the stream, until the next
// range_tombstone_change is encountered.
//
// Note, a range_tombstone_change with an empty tombstone() ends the range tombstone.
// An empty tombstone naturally does not cover any timestamp.
class range_tombstone_change {
    position_in_partition _pos;
    ::tombstone _tomb;
public:
    range_tombstone_change(position_in_partition pos, tombstone tomb)
        : _pos(std::move(pos))
        , _tomb(tomb)
    { }
    range_tombstone_change(position_in_partition_view pos, tombstone tomb)
        : _pos(pos)
        , _tomb(tomb)
    { }
    const position_in_partition& position() const & {
        return _pos;
    }
    position_in_partition position() && {
        return std::move(_pos);
    }
    void set_position(position_in_partition pos) {
        _pos = std::move(pos);
    }
    ::tombstone tombstone() const {
        return _tomb;
    }
    size_t external_memory_usage(const schema& s) const {
        return _pos.external_memory_usage();
    }
    bool equal(const schema& s, const range_tombstone_change& other) const {
        position_in_partition::equal_compare eq(s);
        return _tomb == other._tomb && eq(_pos, other._pos);
    }
    friend std::ostream& operator<<(std::ostream& out, const range_tombstone_change&);
};

template<typename T, typename ReturnType>
concept MutationFragmentConsumerV2 =
    requires(T& t,
            static_row sr,
            clustering_row cr,
            range_tombstone_change rt_chg,
            partition_start ph,
            partition_end pe) {
        { t.consume(std::move(sr)) } -> std::same_as<ReturnType>;
        { t.consume(std::move(cr)) } -> std::same_as<ReturnType>;
        { t.consume(std::move(rt_chg)) } -> std::same_as<ReturnType>;
        { t.consume(std::move(ph)) } -> std::same_as<ReturnType>;
        { t.consume(std::move(pe)) } -> std::same_as<ReturnType>;
    };

template<typename T, typename ReturnType>
concept MutationFragmentVisitorV2 =
    requires(T t,
            const static_row& sr,
            const clustering_row& cr,
            const range_tombstone_change& rt,
            const partition_start& ph,
            const partition_end& eop) {
        { t(sr) } -> std::same_as<ReturnType>;
        { t(cr) } -> std::same_as<ReturnType>;
        { t(rt) } -> std::same_as<ReturnType>;
        { t(ph) } -> std::same_as<ReturnType>;
        { t(eop) } -> std::same_as<ReturnType>;
    };

template<typename T, typename ReturnType>
concept FragmentConsumerReturningV2 =
requires(T t, static_row sr, clustering_row cr, range_tombstone_change rt, tombstone tomb) {
    { t.consume(std::move(sr)) } -> std::same_as<ReturnType>;
    { t.consume(std::move(cr)) } -> std::same_as<ReturnType>;
    { t.consume(std::move(rt)) } -> std::same_as<ReturnType>;
};

template<typename T>
concept FragmentConsumerV2 =
FragmentConsumerReturningV2<T, stop_iteration> || FragmentConsumerReturningV2<T, future<stop_iteration>>;

template<typename T>
concept StreamedMutationConsumerV2 =
FragmentConsumerV2<T> && requires(T t, tombstone tomb) {
    t.consume(tomb);
    t.consume_end_of_stream();
};

class mutation_fragment_v2 {
public:
    enum class kind {
        static_row,
        clustering_row,
        range_tombstone_change,
        partition_start,
        partition_end,
    };
private:
    struct data {
        data(reader_permit permit) :  _memory(permit.consume_memory()) { }
        ~data() { }

        reader_permit::resource_units _memory;
        union {
            static_row _static_row;
            clustering_row _clustering_row;
            range_tombstone_change _range_tombstone_chg;
            partition_start _partition_start;
            partition_end _partition_end;
        };
    };
private:
    kind _kind;
    std::unique_ptr<data> _data;

    mutation_fragment_v2() = default;
    explicit operator bool() const noexcept { return bool(_data); }
    void destroy_data() noexcept;
    friend class optimized_optional<mutation_fragment_v2>;

    friend class position_in_partition;
public:
    struct clustering_row_tag_t { };

    template<typename... Args>
    mutation_fragment_v2(clustering_row_tag_t, const schema& s, reader_permit permit, Args&&... args)
        : _kind(kind::clustering_row)
        , _data(std::make_unique<data>(std::move(permit)))
    {
        new (&_data->_clustering_row) clustering_row(std::forward<Args>(args)...);
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
    }

    mutation_fragment_v2(const schema& s, reader_permit permit, static_row&& r);
    mutation_fragment_v2(const schema& s, reader_permit permit, clustering_row&& r);
    mutation_fragment_v2(const schema& s, reader_permit permit, range_tombstone_change&& r);
    mutation_fragment_v2(const schema& s, reader_permit permit, partition_start&& r);
    mutation_fragment_v2(const schema& s, reader_permit permit, partition_end&& r);

    mutation_fragment_v2(const schema& s, reader_permit permit, const mutation_fragment_v2& o)
        : _kind(o._kind), _data(std::make_unique<data>(std::move(permit))) {
        switch (_kind) {
            case kind::static_row:
                new (&_data->_static_row) static_row(s, o._data->_static_row);
                break;
            case kind::clustering_row:
                new (&_data->_clustering_row) clustering_row(s, o._data->_clustering_row);
                break;
            case kind::range_tombstone_change:
                new (&_data->_range_tombstone_chg) range_tombstone_change(o._data->_range_tombstone_chg);
                break;
            case kind::partition_start:
                new (&_data->_partition_start) partition_start(o._data->_partition_start);
                break;
            case kind::partition_end:
                new (&_data->_partition_end) partition_end(o._data->_partition_end);
                break;
        }
        _data->_memory.reset(o._data->_memory.resources());
    }
    mutation_fragment_v2(mutation_fragment_v2&& other) = default;
    mutation_fragment_v2& operator=(mutation_fragment_v2&& other) noexcept {
        if (this != &other) {
            this->~mutation_fragment_v2();
            new (this) mutation_fragment_v2(std::move(other));
        }
        return *this;
    }
    [[gnu::always_inline]]
    ~mutation_fragment_v2() {
        if (_data) {
            destroy_data();
        }
    }

    position_in_partition_view position() const;

    // Checks if this fragment may be relevant for any range starting at given position.
    bool relevant_for_range(const schema& s, position_in_partition_view pos) const;

    bool has_key() const { return is_clustering_row() || is_range_tombstone_change(); }

    // Requirements: has_key() == true
    const clustering_key_prefix& key() const;

    kind mutation_fragment_kind() const { return _kind; }

    bool is_static_row() const { return _kind == kind::static_row; }
    bool is_clustering_row() const { return _kind == kind::clustering_row; }
    bool is_range_tombstone_change() const { return _kind == kind::range_tombstone_change; }
    bool is_partition_start() const { return _kind == kind::partition_start; }
    bool is_end_of_partition() const { return _kind == kind::partition_end; }

    void mutate_as_static_row(const schema& s, std::invocable<static_row&> auto&& fn) {
        fn(_data->_static_row);
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
    }
    void mutate_as_clustering_row(const schema& s, std::invocable<clustering_row&> auto&& fn) {
        fn(_data->_clustering_row);
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
    }
    void mutate_as_range_tombstone_change(const schema& s, std::invocable<range_tombstone_change&> auto&& fn) {
        fn(_data->_range_tombstone_chg);
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
    }
    void mutate_as_partition_start(const schema& s, std::invocable<partition_start&> auto&& fn) {
        fn(_data->_partition_start);
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
    }

    static_row&& as_static_row() && { return std::move(_data->_static_row); }
    clustering_row&& as_clustering_row() && { return std::move(_data->_clustering_row); }
    range_tombstone_change&& as_range_tombstone_change() && { return std::move(_data->_range_tombstone_chg); }
    partition_start&& as_partition_start() && { return std::move(_data->_partition_start); }
    partition_end&& as_end_of_partition() && { return std::move(_data->_partition_end); }

    const static_row& as_static_row() const & { return _data->_static_row; }
    const clustering_row& as_clustering_row() const & { return _data->_clustering_row; }
    const range_tombstone_change& as_range_tombstone_change() const & { return _data->_range_tombstone_chg; }
    const partition_start& as_partition_start() const & { return _data->_partition_start; }
    const partition_end& as_end_of_partition() const & { return _data->_partition_end; }

    // Requirements: mergeable_with(mf)
    void apply(const schema& s, mutation_fragment_v2&& mf);

    template<typename Consumer>
    requires MutationFragmentConsumerV2<Consumer, decltype(std::declval<Consumer>().consume(std::declval<range_tombstone_change>()))>
    decltype(auto) consume(Consumer& consumer) && {
        _data->_memory.reset();
        switch (_kind) {
        case kind::static_row:
            return consumer.consume(std::move(_data->_static_row));
        case kind::clustering_row:
            return consumer.consume(std::move(_data->_clustering_row));
        case kind::range_tombstone_change:
            return consumer.consume(std::move(_data->_range_tombstone_chg));
        case kind::partition_start:
            return consumer.consume(std::move(_data->_partition_start));
        case kind::partition_end:
            return consumer.consume(std::move(_data->_partition_end));
        }
        abort();
    }

    template<typename Visitor>
    requires MutationFragmentVisitorV2<Visitor, decltype(std::declval<Visitor>()(std::declval<static_row&>()))>
    decltype(auto) visit(Visitor&& visitor) const {
        switch (_kind) {
        case kind::static_row:
            return visitor(as_static_row());
        case kind::clustering_row:
            return visitor(as_clustering_row());
        case kind::range_tombstone_change:
            return visitor(as_range_tombstone_change());
        case kind::partition_start:
            return visitor(as_partition_start());
        case kind::partition_end:
            return visitor(as_end_of_partition());
        }
        abort();
    }

    size_t memory_usage() const {
        return _data->_memory.resources().memory;
    }

    reader_permit permit() const {
        return _data->_memory.permit();
    }

    bool equal(const schema& s, const mutation_fragment_v2& other) const {
        if (other._kind != _kind) {
            return false;
        }
        switch (_kind) {
        case kind::static_row:
            return as_static_row().equal(s, other.as_static_row());
        case kind::clustering_row:
            return as_clustering_row().equal(s, other.as_clustering_row());
        case kind::range_tombstone_change:
            return as_range_tombstone_change().equal(s, other.as_range_tombstone_change());
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
    // and at least one of them is not a range_tombstone_change can be emitted one after the other in the stream.
    //
    // Undefined for range_tombstone_change.
    // Merging range tombstones requires a more complicated handling
    // because range_tombstone_change doesn't represent a write on its own, only
    // with a matching change for the end bound. It's not enough to chose one fragment over another,
    // the upper bound of the winning tombstone needs to be taken into account when merging
    // later range_tombstone_change fragments in the stream.
    bool mergeable_with(const mutation_fragment_v2& mf) const {
        return _kind == mf._kind && _kind != kind::range_tombstone_change;
    }

    class printer {
        const schema& _schema;
        const mutation_fragment_v2& _mutation_fragment;
    public:
        printer(const schema& s, const mutation_fragment_v2& mf) : _schema(s), _mutation_fragment(mf) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);

private:
    size_t calculate_memory_usage(const schema& s) const {
        return sizeof(data) + visit([&s] (auto& mf) -> size_t { return mf.external_memory_usage(s); });
    }
};

std::ostream& operator<<(std::ostream&, mutation_fragment_v2::kind);

using mutation_fragment_v2_opt = optimized_optional<mutation_fragment_v2>;


// F gets a stream element as an argument and returns the new value which replaces that element
// in the transformed stream.
template<typename F>
concept StreamedMutationTranformerV2 =
requires(F f, mutation_fragment_v2 mf, schema_ptr s) {
    { f(std::move(mf)) } -> std::same_as<mutation_fragment_v2>;
    { f(s) } -> std::same_as<schema_ptr>;
};
