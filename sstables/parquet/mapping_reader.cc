/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/mapping_reader.hh"

#include <algorithm>
#include <array>
#include <cstring>
#include <unordered_map>

#include <seastar/core/coroutine.hh>

#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "schema/schema.hh"
#include "sstables/exceptions.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/types.hh"
#include "types/user.hh"
#include "utils/fragment_range.hh"

namespace sstables::parquet {

namespace {

[[noreturn]] void malformed(std::string_view what) {
    throw_malformed_sstable_exception(seastar::format("Parquet Data file: {}", what));
}

// The index after the subtree that starts at element `i`.
size_t skip_subtree(const utils::chunked_vector<schema_element>& elements, size_t i) {
    size_t pending = 1;
    while (pending) {
        if (i >= elements.size()) {
            malformed("the schema ends inside a group");
        }
        const int32_t children = elements[i].num_children.value_or(0);
        if (children < 0) {
            malformed("a group has a negative number of children");
        }
        pending += size_t(children);
        --pending;
        ++i;
    }
    return i;
}

} // anonymous namespace

opened_mapping open_mapping(const serialization_header& header, const file_summary& summary) {
    const auto& kv = summary.key_value_metadata();
    const auto layout = std::ranges::find_if(kv, [] (const key_value& e) { return std::string_view(e.key) == layout_key; });
    if (layout == kv.end() || !layout->value || std::string_view(*layout->value) != layout_v1) {
        malformed(seastar::format("the cell metadata layout is {}, expected {}",
                layout == kv.end() ? sstring("missing") : layout->value.value_or("empty"), layout_v1));
    }

    // The key names are the names of the first top-level fields.
    size_t keys = 0;
    try {
        keys = partition_key_types(header).size() + header.clustering_key_types_names.elements.size();
    } catch (const std::invalid_argument& e) {
        malformed(e.what());
    }
    const auto& elements = summary.elements();
    if (elements.empty() || size_t(elements[0].num_children.value_or(0)) < keys) {
        malformed(seastar::format("the schema has fewer top-level fields than the {} key columns", keys));
    }
    std::vector<sstring> key_names;
    key_names.reserve(keys);
    size_t i = 1;
    for (size_t k = 0; k < keys; ++k) {
        if (i >= elements.size()) {
            malformed("the schema ends before the key columns");
        }
        key_names.push_back(elements[i].name);
        i = skip_subtree(elements, i);
    }

    std::optional<table_mapping> m;
    try {
        m.emplace(table_mapping::from_header(header, key_names));
    } catch (const std::invalid_argument& e) {
        malformed(e.what());
    }

    // The file's leaves are the mapping's leaves that the file has, in the
    // same order, so one pass matches them.
    const auto& s = m->get_schema();
    std::vector<std::optional<leaf_index>> leaves(s.columns());
    std::vector<bool> included(s.columns(), false);
    leaf_index next = 0;
    for (const column_id c : s.columns_in_schema_order()) {
        if (next == summary.columns().size()) {
            break;
        }
        if (std::ranges::equal(s.path(c), summary.column(next).path)) {
            leaves[c] = next++;
            included[c] = true;
        }
    }
    if (next != summary.columns().size()) {
        malformed(seastar::format("the leaf {} is not a column of the serialization header", fmt::join(summary.column(next).path, ".")));
    }
    if (s.to_elements(included) != elements) {
        malformed("the schema does not match the serialization header");
    }
    for (const auto* key : {&m->partition_key(), &m->clustering_key()}) {
        for (const auto& k : *key) {
            if (!leaves[k.value]) {
                malformed(seastar::format("the key column {} is missing", k.parquet_name));
            }
        }
    }
    return opened_mapping{std::move(*m), std::move(leaves)};
}

// read_plan

read_plan::read_plan(const opened_mapping& om, const file_summary& summary, const ::schema& s, const column_translation& ct)
    : _mapping(om.mapping)
    , _version(s.version())
    , _slot_of(om.mapping.get_schema().columns(), -1)
{
    auto add = [&] (std::optional<column_id> c, int32_t owner, bool parkable) {
        if (!c || !om.leaves[*c]) {
            return;
        }
        const leaf_index l = *om.leaves[*c];
        const auto& col = summary.column(l);
        _slot_of[*c] = int32_t(_slots.size());
        _slots.push_back(slot{
            .leaf = l,
            .max_def = col.max_def,
            .element_def = col.max_rep ? col.repeated_def[0] : uint8_t(0),
            .parkable = parkable,
            .owner = owner,
        });
    };

    const auto& r = _mapping.row();
    for (const column_id c : {r.kind, r.partition_tomb_ts, r.partition_tomb_deletion_time, r.ts, r.ttl, r.expiry,
            r.marker_deletion_time, r.tomb_ts, r.tomb_deletion_time, r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time}) {
        add(c, -1, false);
    }
    for (const auto* keys : {&_mapping.partition_key(), &_mapping.clustering_key()}) {
        for (const auto& k : *keys) {
            add(k.value, -1, false);
            add(k.empty, -1, false);
        }
    }

    // column_translation matches the header's columns to the table schema
    // by name. It reorders them, so they are found by name here.
    auto collect = [&] (const utils::chunked_vector<cell_columns>& columns, const std::vector<column_translation::column_info>& infos, column_kind kind) {
        std::unordered_map<bytes_view, const column_translation::column_info*> by_name;
        by_name.reserve(infos.size());
        for (const auto& info : infos) {
            by_name.emplace(bytes_view(*info.name), &info);
        }
        const size_t first = _cells.size();
        for (const auto& c : columns) {
            const auto it = by_name.find(bytes_view(c.name));
            if (it == by_name.end() || !it->second->id) {
                continue;
            }
            _cells.push_back(cell_plan{.columns = &c, .kind = kind, .id = *it->second->id, .schema_mismatch = it->second->schema_mismatch});
        }
        std::sort(_cells.begin() + first, _cells.end(), [] (const cell_plan& a, const cell_plan& b) { return a.id < b.id; });
    };
    collect(_mapping.static_columns(), ct.static_columns(), column_kind::static_column);
    collect(_mapping.regular_columns(), ct.regular_columns(), column_kind::regular_column);

    for (size_t i = 0; i < _cells.size(); ++i) {
        const auto& c = *_cells[i].columns;
        const int32_t owner = int32_t(i);
        auto meta = [&] (const cell_metadata_columns& m) {
            add(m.ts, owner, true);
            add(m.ttl, owner, true);
            add(m.expiry, owner, true);
            add(m.deletion_time, owner, true);
            add(m.empty, owner, true);
        };
        if (c.shape == column_shape::udt) {
            for (const auto& f : c.fields) {
                add(f.value, owner, true);
                meta(f.meta);
            }
        } else {
            // A live counter without shards has empty lists, which the
            // cursor counts as null rows. So its lists are never parked.
            const bool counter = c.shape == column_shape::counter;
            add(c.value, owner, !counter);
            add(c.map_key, owner, true);
            meta(c.meta);
            add(c.key_empty, owner, true);
            add(c.list_key, owner, true);
            add(c.shard_id, owner, !counter);
            add(c.clock, owner, !counter);
        }
        add(c.tomb_ts, owner, true);
        add(c.tomb_deletion_time, owner, true);
    }
}

std::vector<std::optional<leaf_index>> read_plan::projection() const {
    std::vector<std::optional<leaf_index>> r;
    r.reserve(_slots.size());
    for (const auto& s : _slots) {
        r.push_back(s.leaf);
    }
    return r;
}

// Values

namespace {

// The CQL serialized form of a stored value: a view of the page for byte
// arrays, a copy otherwise. A fixed-width value takes at most 8 bytes, so
// neither form allocates. A value is built in place by read_value() and is
// not copied: the decoder builds one for every cell.
class cql_value {
    std::array<bytes::value_type, 8> _fixed;
    uint8_t _size = 0;
    bool _is_view = false;
    byte_array_view _view;
public:
    cql_value() = default;
    cql_value(const cql_value&) = delete;

    // The empty value.
    void set_empty() noexcept {
        _is_view = false;
        _size = 0;
    }
    // `b` is the serialized form of a fixed-width value.
    void set_fixed(const bytes& b) {
        _is_view = false;
        // Each width is copied with one load of that width.
        switch (b.size()) {
        case 1: std::memcpy(_fixed.data(), b.data(), 1); break;
        case 2: std::memcpy(_fixed.data(), b.data(), 2); break;
        case 4: std::memcpy(_fixed.data(), b.data(), 4); break;
        case 8: std::memcpy(_fixed.data(), b.data(), 8); break;
        default: malformed(seastar::format("a fixed-width value has {} bytes", b.size()));
        }
        _size = b.size();
    }
    void set_view(byte_array_view v) noexcept {
        _is_view = true;
        _view = v;
    }

    template <typename Fn>
    decltype(auto) visit(Fn&& fn) const {
        return _is_view ? fn(_view) : fn(bytes_view(_fixed.data(), _size));
    }
    bytes to_bytes() const {
        return _is_view ? linearized(_view) : bytes(_fixed.data(), _size);
    }
};

// Reads into `out` the value with index `index` of cursor `c`, stored with
// type `vt`. An empty value (`empty`) is not read.
void read_value(cql_value& out, const column_cursor& c, uint32_t index, value_type vt, bool empty) {
    if (empty) {
        out.set_empty();
        return;
    }
    switch (physical_type_of(vt)) {
    case physical_type::boolean: out.set_fixed(to_cql_value(vt, c.value_at<bool>(index))); return;
    case physical_type::int32: out.set_fixed(to_cql_value(vt, c.value_at<int32_t>(index))); return;
    case physical_type::int64: out.set_fixed(to_cql_value(vt, c.value_at<int64_t>(index))); return;
    case physical_type::float_: out.set_fixed(to_cql_value(vt, c.value_at<float>(index))); return;
    case physical_type::double_: out.set_fixed(to_cql_value(vt, c.value_at<double>(index))); return;
    case physical_type::fixed_len_byte_array: {
        const auto& v = c.value_at<byte_array_view>(index);
        if (v.size_bytes() != 16) {
            malformed(seastar::format("a UUID has {} bytes", v.size_bytes()));
        }
        out.set_view(v);
        return;
    }
    case physical_type::byte_array:
        out.set_view(c.value_at<byte_array_view>(index));
        return;
    default:
        malformed(seastar::format("value type {} has physical type {}", int(vt), int(physical_type_of(vt))));
    }
}

// The value as read_value() reads it, as bytes.
bytes read_bytes(const column_cursor& c, uint32_t index, value_type vt, bool empty) {
    cql_value v;
    read_value(v, c, index, vt, empty);
    return v.to_bytes();
}

api::timestamp_type add_difference(api::timestamp_type base, int64_t diff) {
    return api::timestamp_type(uint64_t(base) + uint64_t(diff));
}

gc_clock::time_point time_point_of(int64_t seconds) {
    return gc_clock::time_point(gc_clock::duration(seconds));
}

// The metadata of a cell, a UDT field, or a collection element, as stored.
struct cell_meta {
    std::optional<int64_t> ts;
    std::optional<int32_t> ttl;
    std::optional<int64_t> expiry;
    std::optional<int64_t> deletion_time;
    bool empty = false;

    bool any() const noexcept {
        return ts || ttl || expiry || deletion_time || empty;
    }
};

// Builds a cell from its metadata and its value, or its absence. See
// "Reader, per cell" in mapping.hh. `value` is the cell's value, or null
// if the value leaf is null.
atomic_cell make_cell(const abstract_type& type, const cell_meta& m, const cql_value* value, api::timestamp_type row_ts,
        const std::optional<expiration>& row_exp, atomic_cell::collection_member cm) {
    const api::timestamp_type ts = m.ts ? add_difference(row_ts, *m.ts) : row_ts;
    if (m.deletion_time) {
        if (m.ttl || m.expiry || value || m.empty) {
            malformed("a dead cell has a TTL, an expiry, or a value");
        }
        return atomic_cell::make_dead(ts, time_point_of(*m.deletion_time));
    }
    if (!value) {
        malformed("a live cell has no value");
    }
    const std::optional<int32_t> ttl = m.ttl ? m.ttl : (row_exp ? std::optional<int32_t>(row_exp->ttl) : std::nullopt);
    if (!ttl || *ttl == 0) {
        if (m.expiry) {
            malformed("a cell that does not expire has an expiry");
        }
        return value->visit([&] (const auto& v) { return atomic_cell::make_live(type, ts, v, cm); });
    }
    const std::optional<int64_t> expiry = m.expiry ? m.expiry : (row_exp ? std::optional<int64_t>(row_exp->expiry) : std::nullopt);
    if (!expiry) {
        malformed("an expiring cell has no expiry");
    }
    return value->visit([&] (const auto& v) {
        return atomic_cell::make_live(type, ts, v, time_point_of(*expiry), gc_clock::duration(*ttl), cm);
    });
}

// The entries of a list leaf in the current row, with the structure checks
// of the format. A leaf that is not read, or parked, is a null list.
class list_leaf {
    const column_cursor* _c = nullptr;
    uint8_t _max_def = 0;
    uint8_t _element_def = 0;
    column_cursor::row_entries _entries;
    std::optional<column_cursor::entry> _next;
    bool _first = true;
public:
    list_leaf() = default;
    list_leaf(const column_cursor* c, const read_plan::slot* s) : _c(c) {
        if (_c) {
            _max_def = s->max_def;
            _element_def = s->element_def;
            _entries = _c->entries();
            _next = _entries.next();
            if (_next->def == 0) {
                _next.reset();
            } else if (_next->def < _element_def) {
                malformed("a list is empty");
            }
        }
    }
    bool is_null() const noexcept { return !_c || (_first && !_next); }
    /// The next element, or nothing after the last one.
    std::optional<column_cursor::entry> next() {
        if (!_next) {
            return std::nullopt;
        }
        auto e = *_next;
        _next = _entries.next();
        if (_next && (_next->rep == 0 || _next->def < _element_def)) {
            malformed("a list has an entry that is not an element");
        }
        _first = false;
        return e;
    }
    bool has_value(const column_cursor::entry& e) const noexcept { return e.def == _max_def; }
    const column_cursor& cursor() const noexcept { return *_c; }
};

// The entries of a counter's list leaves: like list_leaf, but an empty
// list is a live counter without shards.
struct counter_list {
    const column_cursor* c = nullptr;
    column_cursor::row_entries entries;
    std::optional<column_cursor::entry> next;
    // 0: null, 1: empty, 2: has elements.
    int state = 0;

    counter_list(const column_cursor* cur, const read_plan::slot* s) : c(cur) {
        if (!c) {
            return;
        }
        entries = c->entries();
        next = entries.next();
        if (next->def == 0) {
            next.reset();
        } else if (next->def < s->element_def) {
            next.reset();
            state = 1;
        } else {
            state = 2;
        }
    }
    std::optional<column_cursor::entry> take() {
        auto e = next;
        if (next) {
            next = entries.next();
            if (next && next->rep == 0) {
                malformed("a counter list has an entry that is not an element");
            }
        }
        return e;
    }
};

} // anonymous namespace

// row_decoder

row_decoder::row_decoder(const ::schema& s, const read_plan& plan, reader& r, read_direction direction)
    : _schema(s)
    , _plan(plan)
    , _reader(r)
    , _direction(direction)
    , _rows(r.summary().rows())
    , _parked(plan.slots().size())
    , _owner_epoch(plan.cells().size(), 0)
{
    for (uint32_t i = 0; i < plan.slots().size(); ++i) {
        if (!plan.slots()[i].parkable) {
            _fixed.push_back(i);
        }
    }
    _components.reserve(std::max(s.partition_key_size(), s.clustering_key_size()));
}

size_t row_decoder::fixed_memory(const read_plan& plan) noexcept {
    // Per slot: _parked, and one element in each of _fixed, _active,
    // _pending, and _wake. Per cell column: _owners and _owner_epoch.
    return sizeof(row_decoder)
            + plan.slots().size() * (sizeof(parked) + 3 * sizeof(uint32_t) + sizeof(std::pair<uint64_t, uint32_t>))
            + plan.cells().size() * (sizeof(uint32_t) + sizeof(uint64_t));
}

bool row_decoder::wakes_before(const std::pair<uint64_t, uint32_t>& a, const std::pair<uint64_t, uint32_t>& b) const noexcept {
    return _direction == read_direction::forward ? a.first < b.first : a.first > b.first;
}

void row_decoder::push_wake(uint64_t wake, uint32_t slot) {
    _wake.emplace_back(wake, slot);
    // std::push_heap keeps the largest element on top, so the comparison
    // is reversed to keep the earliest wake there.
    std::push_heap(_wake.begin(), _wake.end(), [this] (const auto& a, const auto& b) { return wakes_before(b, a); });
}

void row_decoder::try_park(uint32_t slot) {
    const auto& c = _reader.cursor(slot);
    if (!c.is_null()) {
        return;
    }
    auto& p = _parked[slot];
    if (_direction == read_direction::forward) {
        const uint64_t n = c.null_rows_forward();
        if (n < 2) {
            return;
        }
        p = parked{.is = true, .lo = _row, .hi = _row + n};
        push_wake(p.hi, slot);
    } else {
        const uint64_t n = c.null_rows_backward();
        if (n < 2) {
            return;
        }
        p = parked{.is = true, .lo = _row + 1 - n, .hi = _row + 1};
        // A run that reaches the first row never ends in this direction.
        if (p.lo > 0) {
            push_wake(p.lo - 1, slot);
        }
    }
}

void row_decoder::park_nulls() {
    for (size_t i = 0; i < _active.size();) {
        try_park(_active[i]);
        if (_parked[_active[i]].is) {
            _active[i] = _active.back();
            _active.pop_back();
        } else {
            ++i;
        }
    }
}

bool row_decoder::step(uint64_t row) {
    for (const uint32_t s : _fixed) {
        if (!_reader.cursor(s).try_seek(row)) {
            _pending.push_back(s);
        }
    }
    for (const uint32_t s : _active) {
        if (!_reader.cursor(s).try_seek(row)) {
            _pending.push_back(s);
        }
    }
    auto later = [this] (const auto& a, const auto& b) { return wakes_before(b, a); };
    while (!_wake.empty()) {
        const auto [wake, s] = _wake.front();
        if (_direction == read_direction::forward ? wake > row : wake < row) {
            break;
        }
        std::pop_heap(_wake.begin(), _wake.end(), later);
        _wake.pop_back();
        _parked[s].is = false;
        _active.push_back(s);
        if (!_reader.cursor(s).try_seek(row)) {
            _pending.push_back(s);
        }
    }
    return _pending.empty();
}

bool row_decoder::jump(uint64_t row) {
    for (const uint32_t s : _fixed) {
        if (!_reader.cursor(s).try_seek(row)) {
            _pending.push_back(s);
        }
    }
    _active.clear();
    _wake.clear();
    for (uint32_t s = 0; s < _plan.slots().size(); ++s) {
        if (!_plan.slots()[s].parkable) {
            continue;
        }
        auto& p = _parked[s];
        if (p.is && p.lo <= row && row < p.hi) {
            if (_direction == read_direction::forward) {
                push_wake(p.hi, s);
            } else if (p.lo > 0) {
                push_wake(p.lo - 1, s);
            }
            continue;
        }
        p.is = false;
        _active.push_back(s);
        if (!_reader.cursor(s).try_seek(row)) {
            _pending.push_back(s);
        }
    }
    return _pending.empty();
}

bool row_decoder::try_position(uint64_t row) {
    if (_positioned && row == _row) {
        return true;
    }
    const bool is_step = _positioned && (_direction == read_direction::forward ? row == _row + 1 : row + 1 == _row);
    _positioned = false;
    _pending.clear();
    _row = row;
    if (!(is_step ? step(row) : jump(row))) {
        return false;
    }
    park_nulls();
    _positioned = true;
    return true;
}

future<> row_decoder::finish_position() {
    // The first cursor that misses fetches the read-ahead window for all
    // columns, so the others find their pages fetched or in flight.
    for (const uint32_t s : _pending) {
        co_await _reader.cursor(s).seek(_row);
    }
    _pending.clear();
    park_nulls();
    _positioned = true;
}

future<> row_decoder::position(uint64_t row) {
    if (!try_position(row)) {
        co_await finish_position();
    }
}

void row_decoder::release_pages() noexcept {
    // Parked cursors stay parked: their null runs are a property of the
    // file, and a woken cursor seeks, which loads its page again.
    _positioned = false;
    _pending.clear();
    _reader.release_pages();
}

// Decoding

const column_cursor* row_decoder::cursor(column_id c) const noexcept {
    const int32_t s = _plan.slot_of(c);
    if (s < 0 || _parked[s].is) {
        return nullptr;
    }
    return &_reader.cursor(s);
}

const read_plan::slot* row_decoder::slot_of_column(column_id c) const noexcept {
    const int32_t s = _plan.slot_of(c);
    return s < 0 ? nullptr : &_plan.slots()[s];
}

namespace {

template <typename T>
std::optional<T> scalar(const column_cursor* c) {
    if (!c || !c->has_value()) {
        return std::nullopt;
    }
    return c->value<T>();
}

bool is_present(const column_cursor* c) {
    return c && c->def() > 0;
}

} // anonymous namespace

void row_decoder::check_null(column_id c, std::string_view what) const {
    if (is_present(cursor(c))) {
        malformed(seastar::format("row {}: {} is set on a row of kind {}", _row, what, int(kind())));
    }
}

fragment_kind row_decoder::kind() const {
    const auto k = scalar<int32_t>(cursor(_plan.mapping().row().kind));
    if (!k || *k < int32_t(fragment_kind::none) || *k > int32_t(fragment_kind::excl_end)) {
        malformed(seastar::format("row {} has no valid kind", _row));
    }
    return fragment_kind(*k);
}

bool row_decoder::starts_partition() const {
    return is_present(cursor(_plan.mapping().partition_key().front().value));
}

partition_key row_decoder::decode_partition_key() {
    _components.clear();
    for (const auto& k : _plan.mapping().partition_key()) {
        const column_cursor* c = cursor(k.value);
        if (!c || !c->has_value()) {
            malformed(seastar::format("row {} has a partial partition key", _row));
        }
        const bool empty = scalar<bool>(k.empty ? cursor(*k.empty) : nullptr).value_or(false);
        _components.push_back(read_bytes(*c, c->entries().next()->value_index, k.values, empty));
    }
    return partition_key::from_exploded(_schema, _components);
}

tombstone row_decoder::decode_partition_tombstone() const {
    const auto& r = _plan.mapping().row();
    const auto ts = scalar<int64_t>(cursor(r.partition_tomb_ts));
    const auto dt = scalar<int64_t>(cursor(r.partition_tomb_deletion_time));
    if (bool(ts) != bool(dt)) {
        malformed(seastar::format("row {} has half of a partition tombstone", _row));
    }
    return dt ? tombstone(*ts, time_point_of(*dt)) : tombstone();
}

void row_decoder::check_no_partition_tombstone() const {
    if (!starts_partition()) {
        const auto& r = _plan.mapping().row();
        if (is_present(cursor(r.partition_tomb_ts)) || is_present(cursor(r.partition_tomb_deletion_time))) {
            malformed(seastar::format("row {} has a partition tombstone but does not start a partition", _row));
        }
    }
}

void row_decoder::check_no_cells() {
    for (const uint32_t s : _active) {
        if (_reader.cursor(s).def() > 0) {
            malformed(seastar::format("row {} of kind {} has cells", _row, int(kind())));
        }
    }
    for (const uint32_t s : _fixed) {
        if (_plan.slots()[s].owner >= 0 && _reader.cursor(s).def() > 0) {
            malformed(seastar::format("row {} of kind {} has cells", _row, int(kind())));
        }
    }
}

api::timestamp_type row_decoder::default_timestamp() const {
    const auto ts = scalar<int64_t>(cursor(_plan.mapping().row().ts));
    if (!ts) {
        malformed(seastar::format("row {} of kind {} has no timestamp", _row, int(kind())));
    }
    return *ts;
}

std::optional<expiration> row_decoder::default_expiration() const {
    const auto& r = _plan.mapping().row();
    const auto ttl = scalar<int32_t>(cursor(r.ttl));
    const auto expiry = scalar<int64_t>(cursor(r.expiry));
    if (bool(ttl) != bool(expiry)) {
        malformed(seastar::format("row {} has a TTL without an expiry, or the reverse", _row));
    }
    if (!ttl) {
        return std::nullopt;
    }
    if (*ttl == 0) {
        malformed(seastar::format("row {} has a TTL of zero", _row));
    }
    return expiration{.ttl = *ttl, .expiry = *expiry};
}

void row_decoder::decode_clustering_components(bool full) {
    _components.clear();
    bool ended = false;
    for (const auto& k : _plan.mapping().clustering_key()) {
        const column_cursor* c = cursor(k.value);
        const bool empty = scalar<bool>(k.empty ? cursor(*k.empty) : nullptr).value_or(false);
        if (!c || !c->has_value()) {
            if (empty) {
                malformed(seastar::format("row {} has an empty marker on a null clustering key component", _row));
            }
            ended = true;
            continue;
        }
        if (ended) {
            malformed(seastar::format("row {} has a clustering key component after a null one", _row));
        }
        _components.push_back(read_bytes(*c, c->entries().next()->value_index, k.values, empty));
    }
    if (full && _components.size() != _plan.mapping().clustering_key().size()) {
        malformed(seastar::format("row {} of kind {} has a partial clustering key", _row, int(kind())));
    }
}

void row_decoder::check_none_row() {
    const auto& r = _plan.mapping().row();
    for (const column_id c : {r.ts, r.ttl, r.expiry, r.marker_deletion_time, r.tomb_ts, r.tomb_deletion_time,
            r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time}) {
        check_null(c, "a row-level column");
    }
    decode_clustering_components(false);
    if (!_components.empty()) {
        malformed(seastar::format("row {} of kind none has a clustering key", _row));
    }
    check_no_cells();
}

static_row row_decoder::decode_static_row() {
    const auto& r = _plan.mapping().row();
    for (const column_id c : {r.marker_deletion_time, r.tomb_ts, r.tomb_deletion_time, r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time}) {
        check_null(c, "a row-level column");
    }
    decode_clustering_components(false);
    if (!_components.empty()) {
        malformed(seastar::format("row {} is a static row with a clustering key", _row));
    }
    static_row sr;
    decode_cells(sr.cells(), column_kind::static_column, default_timestamp(), default_expiration());
    return sr;
}

clustering_key row_decoder::decode_clustering_key() {
    decode_clustering_components(true);
    return clustering_key::from_exploded(_schema, _components);
}

void row_decoder::decode_clustering_row(clustering_row& cr) {
    check_no_partition_tombstone();
    const auto& r = _plan.mapping().row();
    const fragment_kind k = kind();
    const api::timestamp_type ts = default_timestamp();
    const auto exp = default_expiration();
    const auto marker_deletion_time = scalar<int64_t>(cursor(r.marker_deletion_time));
    if (k == fragment_kind::clustering_row_with_marker) {
        if (marker_deletion_time) {
            cr.apply(row_marker(tombstone(ts, time_point_of(*marker_deletion_time))));
        } else if (exp) {
            cr.apply(row_marker(ts, gc_clock::duration(exp->ttl), time_point_of(exp->expiry)));
        } else {
            cr.apply(row_marker(ts));
        }
    } else if (marker_deletion_time) {
        malformed(seastar::format("row {} has a marker deletion time but no marker", _row));
    }
    auto tomb = [&] (column_id ts_leaf, column_id dt_leaf) {
        const auto diff = scalar<int64_t>(cursor(ts_leaf));
        const auto dt = scalar<int64_t>(cursor(dt_leaf));
        if (!dt) {
            if (diff) {
                malformed(seastar::format("row {} has a tombstone timestamp without a deletion time", _row));
            }
            return tombstone();
        }
        return tombstone(diff ? add_difference(ts, *diff) : ts, time_point_of(*dt));
    };
    if (const auto t = tomb(r.tomb_ts, r.tomb_deletion_time)) {
        cr.apply(t);
    }
    if (const auto t = tomb(r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time)) {
        cr.apply(shadowable_tombstone(t));
    }
    decode_cells(cr.cells(), column_kind::regular_column, ts, exp);
}

decoded_bound row_decoder::decode_bound() {
    check_no_partition_tombstone();
    const auto& r = _plan.mapping().row();
    for (const column_id c : {r.ttl, r.expiry, r.marker_deletion_time, r.tomb_ts, r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time}) {
        check_null(c, "a row-level column");
    }
    check_no_cells();
    const auto dt = scalar<int64_t>(cursor(r.tomb_deletion_time));
    if (!dt) {
        malformed(seastar::format("range tombstone bound row {} has no deletion time", _row));
    }
    bound_kind bk;
    switch (kind()) {
    case fragment_kind::incl_start: bk = bound_kind::incl_start; break;
    case fragment_kind::excl_start: bk = bound_kind::excl_start; break;
    case fragment_kind::incl_end: bk = bound_kind::incl_end; break;
    case fragment_kind::excl_end: bk = bound_kind::excl_end; break;
    default: malformed(seastar::format("row {} is not a range tombstone bound", _row));
    }
    decode_clustering_components(false);
    return decoded_bound{
        .prefix = clustering_key_prefix::from_exploded(_schema, _components),
        .kind = bk,
        .tomb = tombstone(default_timestamp(), time_point_of(*dt)),
    };
}

void row_decoder::decode_cells(::row& cells, column_kind kind, api::timestamp_type ts, std::optional<expiration> exp) {
    ++_epoch;
    _owners.clear();
    auto mark = [&] (uint32_t s) {
        const int32_t owner = _plan.slots()[s].owner;
        if (owner >= 0 && _owner_epoch[owner] != _epoch && _reader.cursor(s).def() > 0) {
            _owner_epoch[owner] = _epoch;
            _owners.push_back(uint32_t(owner));
        }
    };
    for (const uint32_t s : _active) {
        mark(s);
    }
    for (const uint32_t s : _fixed) {
        mark(s);
    }
    // The plan orders the columns by kind and id, so the cells are
    // appended in the order of their ids.
    std::ranges::sort(_owners);
    for (const uint32_t owner : _owners) {
        const auto& p = _plan.cells()[owner];
        if (p.kind != kind) {
            malformed(seastar::format("row {} has a cell of {} column {}", _row, to_sstring(p.kind), p.columns->parquet_name));
        }
        if (auto cell = decode_cell(p, ts, exp)) {
            cells.append_cell(p.id, std::move(*cell));
        }
    }
}

static void check_schema_mismatch(const read_plan::cell_plan& p, const column_definition& cdef) {
    if (p.schema_mismatch) {
        throw_malformed_sstable_exception(seastar::format("{} definition in serialization header does not match schema. Expected {} but got {}",
                cdef.name_as_text(), cdef.type->name(), p.columns->type->name()));
    }
}

std::optional<atomic_cell_or_collection> row_decoder::decode_cell(const read_plan::cell_plan& p, api::timestamp_type ts, std::optional<expiration> exp) {
    const auto& c = *p.columns;
    const column_definition& cdef = _schema.column_at(p.kind, p.id);
    switch (c.shape) {
    case column_shape::atomic:
        break;
    case column_shape::counter:
        return decode_counter(p, cdef, ts);
    case column_shape::udt:
        return decode_udt(p, cdef, ts, exp);
    case column_shape::map:
    case column_shape::set:
    case column_shape::list:
        return decode_collection(p, cdef, ts, exp);
    }
    const column_cursor* v = cursor(c.value);
    cell_meta m{
        .ts = scalar<int64_t>(cursor(c.meta.ts)),
        .ttl = scalar<int32_t>(c.meta.ttl ? cursor(*c.meta.ttl) : nullptr),
        .expiry = scalar<int64_t>(c.meta.expiry ? cursor(*c.meta.expiry) : nullptr),
        .deletion_time = scalar<int64_t>(cursor(c.meta.deletion_time)),
        .empty = scalar<bool>(c.meta.empty ? cursor(*c.meta.empty) : nullptr).value_or(false),
    };
    const bool has_value = v && v->has_value();
    if (!has_value && !m.deletion_time) {
        malformed(seastar::format("row {} has metadata of column {} but no cell", _row, c.parquet_name));
    }
    const api::timestamp_type cell_ts = m.ts ? add_difference(ts, *m.ts) : ts;
    if (cell_ts <= cdef.dropped_at()) {
        return std::nullopt;
    }
    check_schema_mismatch(p, cdef);
    cql_value value;
    if (has_value) {
        read_value(value, *v, v->entries().next()->value_index, c.values, m.empty);
    }
    return atomic_cell_or_collection(make_cell(*c.type, m, has_value ? &value : nullptr, ts, exp, atomic_cell::collection_member::no));
}

std::optional<atomic_cell_or_collection> row_decoder::decode_counter(const read_plan::cell_plan& p, const column_definition& cdef, api::timestamp_type ts) {
    const auto& c = *p.columns;
    const auto diff = scalar<int64_t>(cursor(c.meta.ts));
    const auto dt = scalar<int64_t>(cursor(c.meta.deletion_time));
    const api::timestamp_type cell_ts = diff ? add_difference(ts, *diff) : ts;
    counter_list values(cursor(c.value), slot_of_column(c.value));
    counter_list ids(c.shard_id ? cursor(*c.shard_id) : nullptr, c.shard_id ? slot_of_column(*c.shard_id) : nullptr);
    counter_list clocks(c.clock ? cursor(*c.clock) : nullptr, c.clock ? slot_of_column(*c.clock) : nullptr);
    if (values.state != ids.state || values.state != clocks.state) {
        malformed(seastar::format("row {}: the lists of counter column {} disagree", _row, c.parquet_name));
    }
    if (dt) {
        if (values.state) {
            malformed(seastar::format("row {}: a dead counter cell of column {} has shards", _row, c.parquet_name));
        }
    } else if (!values.state) {
        malformed(seastar::format("row {} has metadata of counter column {} but no cell", _row, c.parquet_name));
    }
    if (cell_ts <= cdef.dropped_at()) {
        return std::nullopt;
    }
    check_schema_mismatch(p, cdef);
    if (dt) {
        return atomic_cell_or_collection(atomic_cell::make_dead(cell_ts, time_point_of(*dt)));
    }
    counter_cell_builder ccb;
    while (auto v = values.take()) {
        const auto id = ids.take();
        const auto clock = clocks.take();
        if (!id || !clock) {
            malformed(seastar::format("row {}: the lists of counter column {} have different lengths", _row, c.parquet_name));
        }
        const auto& id_bytes = ids.c->value_at<byte_array_view>(id->value_index);
        if (id_bytes.size_bytes() != 16) {
            malformed(seastar::format("row {}: a counter shard id has {} bytes", _row, id_bytes.size_bytes()));
        }
        const bytes b = linearized(id_bytes);
        const auto uuid = utils::UUID(read_be<int64_t>(reinterpret_cast<const char*>(b.data())), read_be<int64_t>(reinterpret_cast<const char*>(b.data()) + 8));
        ccb.add_maybe_unsorted_shard(counter_shard(counter_id(uuid), values.c->value_at<int64_t>(v->value_index), clocks.c->value_at<int64_t>(clock->value_index)));
    }
    if (ids.take() || clocks.take()) {
        malformed(seastar::format("row {}: the lists of counter column {} have different lengths", _row, c.parquet_name));
    }
    ccb.sort_and_remove_duplicates();
    return atomic_cell_or_collection(ccb.build(cell_ts));
}

std::optional<atomic_cell_or_collection> row_decoder::decode_udt(const read_plan::cell_plan& p, const column_definition& cdef,
        api::timestamp_type ts, std::optional<expiration> exp) {
    const auto& c = *p.columns;
    const auto tomb_diff = scalar<int64_t>(cursor(*c.tomb_ts));
    const auto tomb_dt = scalar<int64_t>(cursor(*c.tomb_deletion_time));
    if (tomb_diff && !tomb_dt) {
        malformed(seastar::format("row {}: column {} has a tombstone timestamp without a deletion time", _row, c.parquet_name));
    }
    const tombstone tomb = tomb_dt ? tombstone(tomb_diff ? add_difference(ts, *tomb_diff) : ts, time_point_of(*tomb_dt)) : tombstone();
    collection_mutation_writer w(tomb);
    const auto* current_udt = dynamic_cast<const user_type_impl*>(&*cdef.type);
    for (size_t i = 0; i < c.fields.size(); ++i) {
        const auto& f = c.fields[i];
        const column_cursor* v = cursor(f.value);
        cell_meta m{
            .ts = scalar<int64_t>(cursor(f.meta.ts)),
            .ttl = scalar<int32_t>(cursor(*f.meta.ttl)),
            .expiry = scalar<int64_t>(cursor(*f.meta.expiry)),
            .deletion_time = scalar<int64_t>(cursor(f.meta.deletion_time)),
            .empty = scalar<bool>(f.meta.empty ? cursor(*f.meta.empty) : nullptr).value_or(false),
        };
        const bool has_value = v && v->has_value();
        if (!has_value && !m.deletion_time) {
            if (m.any()) {
                malformed(seastar::format("row {} has metadata of field {} of column {} but no cell", _row, f.parquet_name, c.parquet_name));
            }
            continue;
        }
        const api::timestamp_type cell_ts = m.ts ? add_difference(ts, *m.ts) : ts;
        if (cell_ts <= cdef.dropped_at()) {
            continue;
        }
        check_schema_mismatch(p, cdef);
        if (current_udt && i >= current_udt->size()) {
            malformed(seastar::format("field index too big while reading UDT column: type has {} fields, got {}", current_udt->size(), i));
        }
        cql_value value;
        if (has_value) {
            read_value(value, *v, v->entries().next()->value_index, f.type, m.empty);
        }
        const bytes path = serialize_field_index(i);
        w.push_back(managed_bytes_view(bytes_view(path)),
                make_cell(*c.type, m, has_value ? &value : nullptr, ts, exp, atomic_cell::collection_member::yes));
    }
    if (w.empty()) {
        return std::nullopt;
    }
    check_schema_mismatch(p, cdef);
    return atomic_cell_or_collection(std::move(w).finish());
}

std::optional<atomic_cell_or_collection> row_decoder::decode_collection(const read_plan::cell_plan& p, const column_definition& cdef,
        api::timestamp_type ts, std::optional<expiration> exp) {
    const auto& c = *p.columns;
    const auto tomb_diff = scalar<int64_t>(cursor(*c.tomb_ts));
    const auto tomb_dt = scalar<int64_t>(cursor(*c.tomb_deletion_time));
    if (tomb_diff && !tomb_dt) {
        malformed(seastar::format("row {}: column {} has a tombstone timestamp without a deletion time", _row, c.parquet_name));
    }
    const tombstone tomb = tomb_dt ? tombstone(tomb_diff ? add_difference(ts, *tomb_diff) : ts, time_point_of(*tomb_dt)) : tombstone();

    auto list = [&] (std::optional<column_id> leaf) {
        return leaf ? list_leaf(cursor(*leaf), slot_of_column(*leaf)) : list_leaf();
    };
    list_leaf values = list(c.value);
    list_leaf keys = list(c.map_key);
    list_leaf list_keys = list(c.list_key);
    list_leaf ts_list = list(c.meta.ts);
    list_leaf ttl_list = list(c.meta.ttl);
    list_leaf expiry_list = list(c.meta.expiry);
    list_leaf dt_list = list(c.meta.deletion_time);
    list_leaf empty_list = list(c.meta.empty);
    list_leaf key_empty_list = list(c.key_empty);

    if (values.is_null()) {
        if (!keys.is_null() || !list_keys.is_null() || !ts_list.is_null() || !ttl_list.is_null() || !expiry_list.is_null()
                || !dt_list.is_null() || !empty_list.is_null() || !key_empty_list.is_null()) {
            malformed(seastar::format("row {} has element metadata of column {} but no elements", _row, c.parquet_name));
        }
        if (!tomb) {
            malformed(seastar::format("row {} has metadata of column {} but no cell", _row, c.parquet_name));
        }
    } else if (c.list_key && list_keys.is_null()) {
        malformed(seastar::format("row {}: list column {} has no keys", _row, c.parquet_name));
    }

    // One entry of a metadata list, for the current element.
    auto entry = [&] <typename T> (list_leaf& l) -> std::optional<T> {
        if (l.is_null()) {
            return std::nullopt;
        }
        const auto e = l.next();
        if (!e) {
            malformed(seastar::format("row {}: a metadata list of column {} is shorter than its elements", _row, c.parquet_name));
        }
        return l.has_value(*e) ? std::optional<T>(l.cursor().value_at<T>(e->value_index)) : std::nullopt;
    };

    collection_mutation_writer w(tomb);
    while (auto ve = values.next()) {
        cell_meta m{
            .ts = entry.template operator()<int64_t>(ts_list),
            .ttl = entry.template operator()<int32_t>(ttl_list),
            .expiry = entry.template operator()<int64_t>(expiry_list),
            .deletion_time = entry.template operator()<int64_t>(dt_list),
            .empty = entry.template operator()<bool>(empty_list).value_or(false),
        };
        const bool key_empty = entry.template operator()<bool>(key_empty_list).value_or(false);
        // The cell path, and the cell's value.
        bytes path;
        cql_value value;
        bool has_value = false;
        switch (c.shape) {
        case column_shape::map: {
            const auto ke = keys.next();
            if (!ke || !keys.has_value(*ke)) {
                malformed(seastar::format("row {}: a map entry of column {} has no key", _row, c.parquet_name));
            }
            path = read_bytes(keys.cursor(), ke->value_index, c.keys, key_empty);
            if (values.has_value(*ve)) {
                read_value(value, values.cursor(), ve->value_index, c.values, m.empty);
                has_value = true;
            }
            break;
        }
        case column_shape::set:
            if (!values.has_value(*ve)) {
                malformed(seastar::format("row {}: a set element of column {} is null", _row, c.parquet_name));
            }
            path = read_bytes(values.cursor(), ve->value_index, c.values, m.empty);
            // The element is the cell path. The cell's value is empty.
            if (!m.deletion_time) {
                value.set_empty();
                has_value = true;
            }
            m.empty = false;
            break;
        case column_shape::list: {
            const auto ke = list_keys.next();
            if (!ke || !list_keys.has_value(*ke)) {
                malformed(seastar::format("row {}: a list element of column {} has no key", _row, c.parquet_name));
            }
            path = read_bytes(list_keys.cursor(), ke->value_index, value_type::uuid, false);
            if (values.has_value(*ve)) {
                read_value(value, values.cursor(), ve->value_index, c.values, m.empty);
                has_value = true;
            }
            break;
        }
        default:
            std::abort();
        }
        if (key_empty && c.shape != column_shape::map) {
            malformed(seastar::format("row {}: an element of column {} has a key empty marker", _row, c.parquet_name));
        }
        const api::timestamp_type cell_ts = m.ts ? add_difference(ts, *m.ts) : ts;
        auto cell = make_cell(*c.type, m, has_value ? &value : nullptr, ts, exp, atomic_cell::collection_member::yes);
        if (cell_ts <= cdef.dropped_at()) {
            continue;
        }
        w.push_back(managed_bytes_view(bytes_view(path)), std::move(cell));
    }
    for (auto* l : {&keys, &list_keys, &ts_list, &ttl_list, &expiry_list, &dt_list, &empty_list, &key_empty_list}) {
        if (!l->is_null() && l->next()) {
            malformed(seastar::format("row {}: a list of column {} is longer than its elements", _row, c.parquet_name));
        }
    }
    if (w.empty()) {
        return std::nullopt;
    }
    check_schema_mismatch(p, cdef);
    return atomic_cell_or_collection(std::move(w).finish());
}

} // namespace sstables::parquet
