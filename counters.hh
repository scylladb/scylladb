/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"

#include "mutation/atomic_cell.hh"
#include "types/types.hh"
#include "locator/host_id.hh"

class mutation;
class atomic_cell_or_collection;

using counter_id = utils::tagged_uuid<struct counter_id_tag>;

template<mutable_view is_mutable>
class basic_counter_shard_view {
    enum class offset : unsigned {
        id = 0u,
        value = unsigned(id) + sizeof(counter_id),
        logical_clock = unsigned(value) + sizeof(int64_t),
        total_size = unsigned(logical_clock) + sizeof(int64_t),
    };
private:
    managed_bytes_basic_view<is_mutable> _base;
private:
    template<typename T>
    T read(offset off) const {
        auto v = _base;
        v.remove_prefix(size_t(off));
        return read_simple_native<T>(v);
    }
public:
    static constexpr auto size = size_t(offset::total_size);
public:
    basic_counter_shard_view() = default;
    explicit basic_counter_shard_view(managed_bytes_basic_view<is_mutable> v) noexcept
        : _base(v) { }

    counter_id id() const { return read<counter_id>(offset::id); }
    int64_t value() const { return read<int64_t>(offset::value); }
    int64_t logical_clock() const { return read<int64_t>(offset::logical_clock); }

    void swap_value_and_clock(basic_counter_shard_view other) noexcept {
        static constexpr size_t off = size_t(offset::value);
        static constexpr size_t size = size_t(offset::total_size) - off;

        signed char tmp[size];
        auto tmp_view = single_fragmented_mutable_view(bytes_mutable_view(std::data(tmp), std::size(tmp)));

        managed_bytes_mutable_view this_view = _base.substr(off, size);
        managed_bytes_mutable_view other_view = other._base.substr(off, size);

        copy_fragmented_view(tmp_view, this_view);
        copy_fragmented_view(this_view, other_view);
        copy_fragmented_view(other_view, tmp_view);
    }

    void set_value_and_clock(const basic_counter_shard_view& other) noexcept {
        static constexpr size_t off = size_t(offset::value);
        static constexpr size_t size = size_t(offset::total_size) - off;

        managed_bytes_mutable_view this_view = _base.substr(off, size);
        managed_bytes_mutable_view other_view = other._base.substr(off, size);

        copy_fragmented_view(this_view, other_view);
    }

    bool operator==(const basic_counter_shard_view& other) const {
        return id() == other.id() && value() == other.value()
               && logical_clock() == other.logical_clock();
    }

    struct less_compare_by_id {
        bool operator()(const basic_counter_shard_view& x, const basic_counter_shard_view& y) const {
            return x.id() < y.id();
        }
    };
};

using counter_shard_view = basic_counter_shard_view<mutable_view::no>;

template <>
struct fmt::formatter<counter_shard_view> : fmt::formatter<string_view> {
    auto format(const counter_shard_view&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

class counter_shard {
    counter_id _id;
    int64_t _value;
    int64_t _logical_clock;
private:
    // Shared logic for applying counter_shards and counter_shard_views.
    // T is either counter_shard or basic_counter_shard_view<U>.
    template<typename T>
    requires requires(T shard) {
        { shard.value() } -> std::same_as<int64_t>;
        { shard.logical_clock() } -> std::same_as<int64_t>;
    }
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
        _value = uint64_t(_value) + uint64_t(value_delta); // signed int overflow is undefined hence the cast
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
    void serialize(atomic_cell_value_mutable_view& out) const {
        write_native<counter_id>(out, _id);
        write_native<int64_t>(out, _value);
        write_native<int64_t>(out, _logical_clock);
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
    void serialize(atomic_cell_value_mutable_view& out) const {
        for (auto&& cs : _shards) {
            cs.serialize(out);
        }
    }

    bool empty() const {
        return _shards.empty();
    }

    atomic_cell build(api::timestamp_type timestamp) const {
        auto ac = atomic_cell::make_live_uninitialized(*counter_type, timestamp, serialized_size());

        auto dst = ac.value();
        for (auto&& cs : _shards) {
            cs.serialize(dst);
        }
        return ac;
    }

    static atomic_cell from_single_shard(api::timestamp_type timestamp, const counter_shard& cs) {
        auto ac = atomic_cell::make_live_uninitialized(*counter_type, timestamp, counter_shard::serialized_size());
        auto dst = ac.value();
        cs.serialize(dst);
        return ac;
    }

    class inserter_iterator {
    public:
        using iterator_category = std::output_iterator_tag;
        using value_type = counter_shard;
        using difference_type = std::ptrdiff_t;
        using pointer = counter_shard*;
        using reference = counter_shard&;
    private:
        counter_cell_builder* _builder;
    public:
        explicit inserter_iterator(counter_cell_builder& b) : _builder(&b) { }
        inserter_iterator& operator=(const counter_shard& cs) {
            _builder->add_shard(cs);
            return *this;
        }
        inserter_iterator& operator=(const counter_shard_view& csv) {
            return this->operator=(counter_shard(csv));
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
    basic_atomic_cell_view<is_mutable> _cell;
private:
    class shard_iterator {
    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using iterator_concept = std::bidirectional_iterator_tag;
        using value_type = basic_counter_shard_view<is_mutable>;
        using difference_type = std::ptrdiff_t;
        using pointer = basic_counter_shard_view<is_mutable>*;
    private:
        managed_bytes_basic_view<is_mutable> _current;
        basic_counter_shard_view<is_mutable> _current_view;
        size_t _pos = 0;
    public:
        shard_iterator() noexcept = default;
        shard_iterator(managed_bytes_basic_view<is_mutable> v, size_t offset) noexcept
            : _current(v), _current_view(_current), _pos(offset) { }

        value_type operator*() const noexcept {
            return _current_view;
        }

        pointer operator->() noexcept {
            return &_current_view;
        }
        shard_iterator& operator++() noexcept {
            _pos += counter_shard_view::size;
            _current_view = basic_counter_shard_view<is_mutable>(_current.substr(_pos, counter_shard_view::size));
            return *this;
        }
        shard_iterator operator++(int) noexcept {
            auto it = *this;
            operator++();
            return it;
        }
        shard_iterator& operator--() noexcept {
            _pos -= counter_shard_view::size;
            _current_view = basic_counter_shard_view<is_mutable>(_current.substr(_pos, counter_shard_view::size));
            return *this;
        }
        shard_iterator operator--(int) noexcept {
            auto it = *this;
            operator--();
            return it;
        }
        bool operator==(const shard_iterator& other) const noexcept {
            return _pos == other._pos;
        }
    };
public:
    std::ranges::subrange<shard_iterator> shards() const {
        auto value = _cell.value();
        auto begin = shard_iterator(value, 0);
        auto end = shard_iterator(value, value.size());
        return {begin, end};
    }

    size_t shard_count() const {
        return _cell.value().size() / counter_shard_view::size;
    }
public:
    // ac must be a live counter cell
    explicit basic_counter_cell_view(basic_atomic_cell_view<is_mutable> ac) noexcept
        : _cell(ac)
    {
        SCYLLA_ASSERT(_cell.is_live());
        SCYLLA_ASSERT(!_cell.is_counter_update());
    }

    api::timestamp_type timestamp() const { return _cell.timestamp(); }

    static data_type total_value_type() { return long_type; }

    int64_t total_value() const {
        return std::ranges::fold_left(shards(), int64_t(0), [] (int64_t v, counter_shard_view cs) {
            return v + cs.value();
        });
    }

    std::optional<counter_shard_view> get_shard(const counter_id& id) const {
        auto it = std::ranges::find_if(shards(), [&id] (counter_shard_view csv) {
            return csv.id() == id;
        });
        if (it == shards().end()) {
            return { };
        }
        return *it;
    }

    bool operator==(const basic_counter_cell_view& other) const {
        return timestamp() == other.timestamp() && std::ranges::equal(shards(), other.shards());
    }
};

struct counter_cell_view : basic_counter_cell_view<mutable_view::no> {
    using basic_counter_cell_view::basic_counter_cell_view;

    // Reversibly applies two counter cells, at least one of them must be live.
    static void apply(const column_definition& cdef, atomic_cell_or_collection& dst, atomic_cell_or_collection& src);

    // Computes a counter cell containing minimal amount of data which, when
    // applied to 'b' returns the same cell as 'a' and 'b' applied together.
    static std::optional<atomic_cell> difference(atomic_cell_view a, atomic_cell_view b);
};

template <>
struct fmt::formatter<counter_cell_view> : fmt::formatter<string_view> {
    auto format(const counter_cell_view&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

struct counter_cell_mutable_view : basic_counter_cell_view<mutable_view::yes> {
    using basic_counter_cell_view::basic_counter_cell_view;

    explicit counter_cell_mutable_view(atomic_cell_mutable_view ac) noexcept
        : basic_counter_cell_view<mutable_view::yes>(ac)
    {
    }

    void set_timestamp(api::timestamp_type ts) { _cell.set_timestamp(ts); }
};

// Transforms mutation dst from counter updates to counter shards using state
// stored in current_state.
// If current_state is present it has to be in the same schema as dst.
void transform_counter_updates_to_shards(mutation& dst, const mutation* current_state, uint64_t clock_offset, locator::host_id local_id);

template<>
struct appending_hash<counter_shard_view> {
    template<typename Hasher>
    void operator()(Hasher& h, const counter_shard_view& cshard) const {
        ::feed_hash(h, cshard.id());
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
