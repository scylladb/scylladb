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

#include <boost/range/algorithm/find_if.hpp>

#include "atomic_cell_or_collection.hh"
#include "types.hh"

#include "stdx.hh"

class mutation;

class mutation;

class counter_id {
    int64_t _least_significant;
    int64_t _most_significant;
public:
    static_assert(std::is_same<decltype(std::declval<utils::UUID>().get_least_significant_bits()), int64_t>::value
            &&  std::is_same<decltype(std::declval<utils::UUID>().get_most_significant_bits()), int64_t>::value,
        "utils::UUID is expected to work with two signed 64-bit integers");

    counter_id() = default;
    explicit counter_id(utils::UUID uuid) noexcept
        : _least_significant(uuid.get_least_significant_bits())
        , _most_significant(uuid.get_most_significant_bits())
    { }

    utils::UUID to_uuid() const {
        return utils::UUID(_most_significant, _least_significant);
    }

    bool operator<(const counter_id& other) const {
        return to_uuid() < other.to_uuid();
    }
    bool operator>(const counter_id& other) const {
        return other.to_uuid() < to_uuid();
    }
    bool operator==(const counter_id& other) const {
        return to_uuid() == other.to_uuid();
    }
    bool operator!=(const counter_id& other) const {
        return !(*this == other);
    }
public:
    // (Wrong) Counter ID ordering used by Scylla 1.7.4 and earlier.
    struct less_compare_1_7_4 {
        bool operator()(const counter_id& a, const counter_id& b) const;
    };
public:
    static counter_id local();

    // For tests.
    static counter_id generate_random() {
        return counter_id(utils::make_random_uuid());
    }
};
static_assert(std::is_pod<counter_id>::value, "counter_id should be a POD type");

std::ostream& operator<<(std::ostream& os, const counter_id& id);

template<mutable_view is_mutable>
class basic_counter_shard_view {
    enum class offset : unsigned {
        id = 0u,
        value = unsigned(id) + sizeof(counter_id),
        logical_clock = unsigned(value) + sizeof(int64_t),
        total_size = unsigned(logical_clock) + sizeof(int64_t),
    };
private:
    using pointer_type = std::conditional_t<is_mutable == mutable_view::no, const signed char*, signed char*>;
    pointer_type _base;
private:
    template<typename T>
    T read(offset off) const {
        T value;
        std::copy_n(_base + static_cast<unsigned>(off), sizeof(T), reinterpret_cast<signed char*>(&value));
        return value;
    }
public:
    static constexpr auto size = size_t(offset::total_size);
public:
    basic_counter_shard_view() = default;
    explicit basic_counter_shard_view(pointer_type ptr) noexcept
        : _base(ptr) { }

    counter_id id() const { return read<counter_id>(offset::id); }
    int64_t value() const { return read<int64_t>(offset::value); }
    int64_t logical_clock() const { return read<int64_t>(offset::logical_clock); }

    void swap_value_and_clock(basic_counter_shard_view& other) noexcept {
        static constexpr size_t off = size_t(offset::value);
        static constexpr size_t size = size_t(offset::total_size) - off;

        signed char tmp[size];
        std::copy_n(_base + off, size, tmp);
        std::copy_n(other._base + off, size, _base + off);
        std::copy_n(tmp, size, other._base + off);
    }

    void set_value_and_clock(const basic_counter_shard_view& other) noexcept {
        static constexpr size_t off = size_t(offset::value);
        static constexpr size_t size = size_t(offset::total_size) - off;
        std::copy_n(other._base + off, size, _base + off);
    }

    bool operator==(const basic_counter_shard_view& other) const {
        return id() == other.id() && value() == other.value()
               && logical_clock() == other.logical_clock();
    }
    bool operator!=(const basic_counter_shard_view& other) const {
        return !(*this == other);
    }

    struct less_compare_by_id {
        bool operator()(const basic_counter_shard_view& x, const basic_counter_shard_view& y) const {
            return x.id() < y.id();
        }
    };
};

using counter_shard_view = basic_counter_shard_view<mutable_view::no>;

std::ostream& operator<<(std::ostream& os, counter_shard_view csv);

class counter_shard {
    counter_id _id;
    int64_t _value;
    int64_t _logical_clock;
private:
    template<typename T>
    static void write(const T& value, bytes::iterator& out) {
        out = std::copy_n(reinterpret_cast<const signed char*>(&value), sizeof(T), out);
    }
private:
    // Shared logic for applying counter_shards and counter_shard_views.
    // T is either counter_shard or basic_counter_shard_view<U>.
    template<typename T>
    GCC6_CONCEPT(requires requires(T shard) {
        { shard.value() } -> int64_t;
        { shard.logical_clock() } -> int64_t;
    })
    counter_shard& do_apply(T&& other) noexcept {
        auto other_clock = other.logical_clock();
        if (_logical_clock < other_clock) {
            _logical_clock = other_clock;
            _value = other.value();
        }
        return *this;
    }
public:
    counter_shard(counter_id id, int64_t value, int64_t logical_clock) noexcept
        : _id(id)
        , _value(value)
        , _logical_clock(logical_clock)
    { }

    explicit counter_shard(counter_shard_view csv) noexcept
        : _id(csv.id())
        , _value(csv.value())
        , _logical_clock(csv.logical_clock())
    { }

    counter_id id() const { return _id; }
    int64_t value() const { return _value; }
    int64_t logical_clock() const { return _logical_clock; }

    counter_shard& update(int64_t value_delta, int64_t clock_increment) noexcept {
        _value += value_delta;
        _logical_clock += clock_increment;
        return *this;
    }

    counter_shard& apply(counter_shard_view other) noexcept {
        return do_apply(other);
    }

    counter_shard& apply(const counter_shard& other) noexcept {
        return do_apply(other);
    }

    static constexpr size_t serialized_size() {
        return counter_shard_view::size;
    }
    void serialize(bytes::iterator& out) const {
        write(_id, out);
        write(_value, out);
        write(_logical_clock, out);
    }
};

class counter_cell_builder {
    std::vector<counter_shard> _shards;
    bool _sorted = true;
private:
    void do_sort_and_remove_duplicates();
public:
    counter_cell_builder() = default;
    counter_cell_builder(size_t shard_count) {
        _shards.reserve(shard_count);
    }

    void add_shard(const counter_shard& cs) {
        _shards.emplace_back(cs);
    }

    void add_maybe_unsorted_shard(const counter_shard& cs) {
        add_shard(cs);
        if (_sorted && _shards.size() > 1) {
            auto current = _shards.rbegin();
            auto previous = std::next(current);
            _sorted = current->id() > previous->id();
        }
    }

    void sort_and_remove_duplicates() {
        if (!_sorted) {
            do_sort_and_remove_duplicates();
        }
    }

    size_t serialized_size() const {
        return _shards.size() * counter_shard::serialized_size();
    }
    void serialize(bytes::iterator& out) const {
        for (auto&& cs : _shards) {
            cs.serialize(out);
        }
    }

    bool empty() const {
        return _shards.empty();
    }

    atomic_cell build(api::timestamp_type timestamp) const {
        // If we can assume that the counter shards never cross fragment boundaries
        // the serialisation code gets much simpler.
        static_assert(data::cell::maximum_external_chunk_length % counter_shard::serialized_size() == 0);

        auto ac = atomic_cell::make_live_uninitialized(*counter_type, timestamp, serialized_size());

        auto dst_it = ac.value().begin();
        auto dst_current = *dst_it++;
        for (auto&& cs : _shards) {
            if (dst_current.empty()) {
                dst_current = *dst_it++;
            }
            assert(!dst_current.empty());
            auto value_dst = dst_current.data();
            cs.serialize(value_dst);
            dst_current.remove_prefix(counter_shard::serialized_size());
        }
        return ac;
    }

    static atomic_cell from_single_shard(api::timestamp_type timestamp, const counter_shard& cs) {
        // We don't really need to bother with fragmentation here.
        static_assert(data::cell::maximum_external_chunk_length >= counter_shard::serialized_size());
        auto ac = atomic_cell::make_live_uninitialized(*counter_type, timestamp, counter_shard::serialized_size());
        auto dst = ac.value().first_fragment().begin();
        cs.serialize(dst);
        return ac;
    }

    class inserter_iterator : public std::iterator<std::output_iterator_tag, counter_shard> {
        counter_cell_builder* _builder;
    public:
        explicit inserter_iterator(counter_cell_builder& b) : _builder(&b) { }
        inserter_iterator& operator=(const counter_shard& cs) {
            _builder->add_shard(cs);
            return *this;
        }
        inserter_iterator& operator=(const counter_shard_view& csv) {
            return operator=(counter_shard(csv));
        }
        inserter_iterator& operator++() { return *this; }
        inserter_iterator& operator++(int) { return *this; }
        inserter_iterator& operator*() { return *this; };
    };

    inserter_iterator inserter() {
        return inserter_iterator(*this);
    }
};

// <counter_id>   := <int64_t><int64_t>
// <shard>        := <counter_id><int64_t:value><int64_t:logical_clock>
// <counter_cell> := <shard>*
template<mutable_view is_mutable>
class basic_counter_cell_view {
protected:
    using linearized_value_view = std::conditional_t<is_mutable == mutable_view::no,
                                                     bytes_view, bytes_mutable_view>;
    using pointer_type = typename linearized_value_view::pointer;
    basic_atomic_cell_view<is_mutable> _cell;
    linearized_value_view _value;
private:
    class shard_iterator : public std::iterator<std::input_iterator_tag, basic_counter_shard_view<is_mutable>> {
        pointer_type _current;
        basic_counter_shard_view<is_mutable> _current_view;
    public:
        shard_iterator() = default;
        shard_iterator(pointer_type ptr) noexcept
            : _current(ptr), _current_view(ptr) { }

        basic_counter_shard_view<is_mutable>& operator*() noexcept {
            return _current_view;
        }
        basic_counter_shard_view<is_mutable>* operator->() noexcept {
            return &_current_view;
        }
        shard_iterator& operator++() noexcept {
            _current += counter_shard_view::size;
            _current_view = basic_counter_shard_view<is_mutable>(_current);
            return *this;
        }
        shard_iterator operator++(int) noexcept {
            auto it = *this;
            operator++();
            return it;
        }
        shard_iterator& operator--() noexcept {
            _current -= counter_shard_view::size;
            _current_view = basic_counter_shard_view<is_mutable>(_current);
            return *this;
        }
        shard_iterator operator--(int) noexcept {
            auto it = *this;
            operator--();
            return it;
        }
        bool operator==(const shard_iterator& other) const noexcept {
            return _current == other._current;
        }
        bool operator!=(const shard_iterator& other) const noexcept {
            return !(*this == other);
        }
    };
public:
    boost::iterator_range<shard_iterator> shards() const {
        auto begin = shard_iterator(_value.data());
        auto end = shard_iterator(_value.data() + _value.size());
        return boost::make_iterator_range(begin, end);
    }

    size_t shard_count() const {
        return _cell.value().size_bytes() / counter_shard_view::size;
    }
protected:
    // ac must be a live counter cell
    explicit basic_counter_cell_view(basic_atomic_cell_view<is_mutable> ac, linearized_value_view vv) noexcept
        : _cell(ac), _value(vv)
    {
        assert(_cell.is_live());
        assert(!_cell.is_counter_update());
    }
public:
    api::timestamp_type timestamp() const { return _cell.timestamp(); }

    static data_type total_value_type() { return long_type; }

    int64_t total_value() const {
        return boost::accumulate(shards(), int64_t(0), [] (int64_t v, counter_shard_view cs) {
            return v + cs.value();
        });
    }

    stdx::optional<counter_shard_view> get_shard(const counter_id& id) const {
        auto it = boost::range::find_if(shards(), [&id] (counter_shard_view csv) {
            return csv.id() == id;
        });
        if (it == shards().end()) {
            return { };
        }
        return *it;
    }

    stdx::optional<counter_shard_view> local_shard() const {
        // TODO: consider caching local shard position
        return get_shard(counter_id::local());
    }

    bool operator==(const basic_counter_cell_view& other) const {
        return timestamp() == other.timestamp() && boost::equal(shards(), other.shards());
    }
};

struct counter_cell_view : basic_counter_cell_view<mutable_view::no> {
    using basic_counter_cell_view::basic_counter_cell_view;

    template<typename Function>
    static decltype(auto) with_linearized(basic_atomic_cell_view<mutable_view::no> ac, Function&& fn) {
        return ac.value().with_linearized([&] (bytes_view value_view) {
            counter_cell_view ccv(ac, value_view);
            return fn(ccv);
        });
    }

    // Returns counter shards in an order that is compatible with Scylla 1.7.4.
    std::vector<counter_shard> shards_compatible_with_1_7_4() const;

    // Reversibly applies two counter cells, at least one of them must be live.
    static void apply(const column_definition& cdef, atomic_cell_or_collection& dst, atomic_cell_or_collection& src);

    // Computes a counter cell containing minimal amount of data which, when
    // applied to 'b' returns the same cell as 'a' and 'b' applied together.
    static stdx::optional<atomic_cell> difference(atomic_cell_view a, atomic_cell_view b);

    friend std::ostream& operator<<(std::ostream& os, counter_cell_view ccv);
};

struct counter_cell_mutable_view : basic_counter_cell_view<mutable_view::yes> {
    using basic_counter_cell_view::basic_counter_cell_view;

    explicit counter_cell_mutable_view(atomic_cell_mutable_view ac) noexcept
        : basic_counter_cell_view<mutable_view::yes>(ac, ac.value().first_fragment())
    {
        assert(!ac.value().is_fragmented());
    }

    void set_timestamp(api::timestamp_type ts) { _cell.set_timestamp(ts); }
};

// Transforms mutation dst from counter updates to counter shards using state
// stored in current_state.
// If current_state is present it has to be in the same schema as dst.
void transform_counter_updates_to_shards(mutation& dst, const mutation* current_state, uint64_t clock_offset);

template<>
struct appending_hash<counter_shard_view> {
    template<typename Hasher>
    void operator()(Hasher& h, const counter_shard_view& cshard) const {
        ::feed_hash(h, cshard.id().to_uuid());
        ::feed_hash(h, cshard.value());
        ::feed_hash(h, cshard.logical_clock());
    }
};

template<>
struct appending_hash<counter_cell_view> {
    template<typename Hasher>
    void operator()(Hasher& h, const counter_cell_view& cell) const {
        ::feed_hash(h, true); // is_live
        ::feed_hash(h, cell.timestamp());
        for (auto&& csv : cell.shards()) {
            ::feed_hash(h, csv);
        }
    }
};
