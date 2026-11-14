/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/mapping_writer.hh"

#include <limits>
#include <random>
#include <stdexcept>

#include <seastar/core/byteorder.hh>
#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>

#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "schema/schema.hh"
#include "sstables/types.hh"
#include "types/user.hh"
#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger mapping_writer_logger("parquet_mapping_writer");

[[noreturn]] static void fail(std::string_view what) {
    on_internal_error(mapping_writer_logger, seastar::format("Parquet mapping writer: {}", what));
}

// Policies

namespace {

// The Boyer-Moore majority vote. It finds the majority element if there is
// one, and some element otherwise.
template <typename T>
std::optional<T> majority(const utils::chunked_vector<T>& candidates) {
    std::optional<T> current;
    size_t count = 0;
    for (const T& c : candidates) {
        if (count == 0) {
            current = c;
            count = 1;
        } else if (c == *current) {
            ++count;
        } else {
            --count;
        }
    }
    return current;
}

class default_policy final : public row_defaults_policy {
    api::timestamp_type _previous = 0;
public:
    api::timestamp_type pick_timestamp(const utils::chunked_vector<api::timestamp_type>& candidates) override {
        if (auto ts = majority(candidates)) {
            _previous = *ts;
        }
        return _previous;
    }
    std::optional<expiration> pick_expiration(const utils::chunked_vector<std::optional<expiration>>& candidates) override {
        auto e = majority(candidates);
        return e ? *e : std::nullopt;
    }
};

class random_policy final : public row_defaults_policy {
    std::mt19937_64 _rng;

    bool coin() { return _rng() & 1; }
public:
    explicit random_policy(uint64_t seed) : _rng(seed) {}
    api::timestamp_type pick_timestamp(const utils::chunked_vector<api::timestamp_type>& candidates) override {
        if (!candidates.empty() && coin()) {
            return candidates[_rng() % candidates.size()];
        }
        return api::timestamp_type(_rng());
    }
    std::optional<expiration> pick_expiration(const utils::chunked_vector<std::optional<expiration>>& candidates) override {
        switch (_rng() % 3) {
        case 0:
            if (!candidates.empty()) {
                return candidates[_rng() % candidates.size()];
            }
            return std::nullopt;
        case 1:
            return std::nullopt;
        default: {
            int32_t ttl = int32_t(_rng());
            return expiration{.ttl = ttl ? ttl : 1, .expiry = int64_t(_rng())};
        }
        }
    }
};

} // anonymous namespace

std::unique_ptr<row_defaults_policy> make_default_row_defaults_policy() {
    return std::make_unique<default_policy>();
}

std::unique_ptr<row_defaults_policy> make_random_row_defaults_policy(uint64_t seed) {
    return std::make_unique<random_policy>(seed);
}

utils::small_vector<range_tombstone_bound, 2> range_tombstone_bounds(tombstone open, const range_tombstone_change& rtc) {
    utils::small_vector<range_tombstone_bound, 2> r;
    const auto pos = position_in_partition_view(rtc.position());
    const tombstone next = rtc.tombstone();
    if (open) {
        r.push_back({pos.as_end_bound_view().kind(), open});
    }
    if (next) {
        r.push_back({pos.as_start_bound_view().kind(), next});
    }
    return r;
}

// The writer

namespace {

int64_t timestamp_difference(api::timestamp_type ts, api::timestamp_type base) {
    return int64_t(uint64_t(ts) - uint64_t(base));
}

int64_t seconds(gc_clock::time_point tp) {
    return tp.time_since_epoch().count();
}

int32_t ttl_seconds(gc_clock::duration ttl) {
    const auto n = ttl.count();
    if (n == 0 || n < std::numeric_limits<int32_t>::min() || n > std::numeric_limits<int32_t>::max()) {
        fail(seastar::format("cannot store a TTL of {} seconds", n));
    }
    return int32_t(n);
}

std::optional<expiration> expiration_of(atomic_cell_view cell) {
    if (!cell.is_live_and_has_ttl()) {
        return std::nullopt;
    }
    return expiration{.ttl = ttl_seconds(cell.ttl()), .expiry = seconds(cell.expiry())};
}

} // anonymous namespace

// The metadata of a cell, a UDT field, or a collection element, encoded
// against the row's defaults. See the truth table in mapping.hh.
struct mapping_writer::cell_metadata {
    std::optional<int64_t> ts;
    std::optional<int32_t> ttl;
    std::optional<int64_t> expiry;
    std::optional<int64_t> deletion_time;

    static cell_metadata of(atomic_cell_view cell, const defaults& d) {
        cell_metadata m;
        if (const auto diff = timestamp_difference(cell.timestamp(), d.ts)) {
            m.ts = diff;
        }
        if (!cell.is_live()) {
            m.deletion_time = seconds(cell.deletion_time());
            return m;
        }
        const auto e = expiration_of(cell);
        if (!d.exp) {
            if (e) {
                m.ttl = e->ttl;
                m.expiry = e->expiry;
            }
        } else if (!e) {
            m.ttl = 0;
        } else {
            if (e->ttl != d.exp->ttl) {
                m.ttl = e->ttl;
            }
            if (e->expiry != d.exp->expiry) {
                m.expiry = e->expiry;
            }
        }
        return m;
    }
};

mapping_writer::mapping_writer(const ::schema& s, const serialization_header& header, sstables::file_writer& out, writer_config cfg,
        std::unique_ptr<writer_memory_handle> memory, std::unique_ptr<row_defaults_policy> policy)
    : _schema(s)
    , _mapping([&] {
        if (s.is_compact_table()) {
            throw std::invalid_argument(seastar::format("Parquet sstables do not support compact storage table {}.{}", s.ks_name(), s.cf_name()));
        }
        const auto names = key_parquet_names(s);
        return table_mapping::from_header(header, names);
    }())
    , _writer(out, _mapping.get_schema(), std::move(cfg), std::move(memory))
    , _policy(policy ? std::move(policy) : make_default_row_defaults_policy())
    , _static_by_id(s.static_columns_count(), nullptr)
    , _regular_by_id(s.regular_columns_count(), nullptr)
{
    auto index = [&] (const utils::chunked_vector<cell_columns>& columns, std::vector<const cell_columns*>& by_id, column_kind kind) {
        for (const auto& c : columns) {
            const column_definition* cdef = s.get_column_definition(c.name);
            if (!cdef || cdef->kind != kind) {
                throw std::invalid_argument(seastar::format("Parquet mapping writer: the serialization header has column {}, which is not a {} column of the schema",
                        c.parquet_name, to_sstring(kind)));
            }
            by_id[cdef->id] = &c;
        }
    };
    index(_mapping.static_columns(), _static_by_id, column_kind::static_column);
    index(_mapping.regular_columns(), _regular_by_id, column_kind::regular_column);
}

const cell_columns& mapping_writer::column(column_kind kind, column_id id) const {
    const auto& by_id = kind == column_kind::static_column ? _static_by_id : _regular_by_id;
    if (id >= by_id.size() || !by_id[id]) {
        fail(seastar::format("{} column {} is not in the serialization header", to_sstring(kind), id));
    }
    return *by_id[id];
}

levels mapping_writer::value_levels(column_id c) const {
    return levels{.rep = 0, .def = _writer.get_schema().get_column(c).max_def};
}

void mapping_writer::check_in_partition() const {
    if (!_in_partition) {
        fail("a row outside a partition");
    }
}

void mapping_writer::append_key(const key_columns& k, managed_bytes_view v) {
    if (append_value(_writer, k.value, value_levels(k.value), k.values, v)) {
        _writer.append(*k.empty, true);
    }
}

void mapping_writer::start_partition(const partition_key& key) {
    if (_in_partition) {
        fail("a partition starts before the previous one ended");
    }
    _in_partition = true;
    _partition_has_row = false;
    const auto& columns = _mapping.partition_key();
    size_t i = 0;
    for (managed_bytes_view component : key.components(_schema)) {
        if (i == columns.size()) {
            fail("the partition key has too many components");
        }
        append_key(columns[i++], component);
    }
    if (i != columns.size()) {
        fail("the partition key has too few components");
    }
}

void mapping_writer::partition_tombstone(tombstone t) {
    check_in_partition();
    if (_partition_has_row) {
        fail("a partition tombstone after the first row of the partition");
    }
    if (t) {
        _writer.append(_mapping.row().partition_tomb_ts, int64_t(t.timestamp));
        _writer.append(_mapping.row().partition_tomb_deletion_time, seconds(t.deletion_time));
    }
}

void mapping_writer::write_clustering_prefix(const clustering_key_prefix& prefix) {
    const auto& columns = _mapping.clustering_key();
    size_t i = 0;
    for (managed_bytes_view component : prefix.components(_schema)) {
        if (i == columns.size()) {
            fail("the clustering key has too many components");
        }
        append_key(columns[i++], component);
    }
    // The trailing components of a prefix are null.
}

row_info mapping_writer::end_row() {
    const uint64_t ordinal = _writer.rows();
    const uint64_t raw_size = _writer.current_row_raw_size();
    _partition_has_row = true;
    return row_info{.ordinal = ordinal, .raw_size = raw_size, .page_set_ended = _writer.end_row()};
}

void mapping_writer::write_tombstone(column_id ts, column_id deletion_time, tombstone t, api::timestamp_type base) {
    if (const auto diff = timestamp_difference(t.timestamp, base)) {
        _writer.append(ts, diff);
    }
    _writer.append(deletion_time, seconds(t.deletion_time));
}

void mapping_writer::collect_candidates(const row& cells, column_kind kind, bool timestamps) {
    _timestamps.clear();
    _expirations.clear();
    auto add_cell = [&] (atomic_cell_view cell) {
        if (timestamps) {
            _timestamps.push_back(cell.timestamp());
        }
        if (cell.is_live()) {
            _expirations.push_back(expiration_of(cell));
        }
    };
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac) {
        const cell_columns& c = column(kind, id);
        switch (c.shape) {
        case column_shape::atomic:
            add_cell(ac.as_atomic_cell(_schema.column_at(kind, id)));
            break;
        case column_shape::counter:
            if (timestamps) {
                _timestamps.push_back(ac.as_atomic_cell(_schema.column_at(kind, id)).timestamp());
            }
            break;
        case column_shape::map:
        case column_shape::set:
        case column_shape::list:
        case column_shape::udt: {
            const auto cmv = ac.as_collection_mutation();
            if (const auto t = cmv.tomb(); t && timestamps) {
                _timestamps.push_back(t.timestamp);
            }
            for (const auto& [path, cell] : cmv) {
                add_cell(cell);
            }
            break;
        }
        }
    });
}

mapping_writer::defaults mapping_writer::pick_defaults(const row& cells, column_kind kind, const row_marker* marker, const row_tombstone* tomb) {
    if (marker && !marker->is_missing()) {
        defaults d{.ts = marker->timestamp()};
        if (marker->is_live()) {
            if (marker->is_expiring()) {
                d.exp = expiration{.ttl = ttl_seconds(marker->ttl()), .expiry = seconds(marker->expiry())};
            }
        } else {
            collect_candidates(cells, kind, false);
            d.exp = _policy->pick_expiration(_expirations);
        }
        return d;
    }
    collect_candidates(cells, kind, true);
    if (tomb) {
        if (tomb->regular()) {
            _timestamps.push_back(tomb->regular().timestamp);
        }
        if (tomb->is_shadowable()) {
            _timestamps.push_back(tomb->tomb().timestamp);
        }
    }
    defaults d{.ts = _policy->pick_timestamp(_timestamps), .exp = _policy->pick_expiration(_expirations)};
    return d;
}

void mapping_writer::write_row_level(fragment_kind kind, const defaults& d) {
    const auto& r = _mapping.row();
    _writer.append(r.kind, int32_t(kind));
    _writer.append(r.ts, int64_t(d.ts));
    if (d.exp) {
        if (d.exp->ttl == 0) {
            fail("the default TTL of a row is zero");
        }
        _writer.append(r.ttl, d.exp->ttl);
        _writer.append(r.expiry, d.exp->expiry);
    }
}

void mapping_writer::write_atomic_cell(const cell_metadata_columns& meta, column_id value, value_type vt, atomic_cell_view cell, const defaults& d) {
    const auto m = cell_metadata::of(cell, d);
    if (m.ts) {
        _writer.append(meta.ts, *m.ts);
    }
    if (m.ttl) {
        _writer.append(*meta.ttl, *m.ttl);
    }
    if (m.expiry) {
        _writer.append(*meta.expiry, *m.expiry);
    }
    if (m.deletion_time) {
        _writer.append(meta.deletion_time, *m.deletion_time);
        return;
    }
    if (append_value(_writer, value, value_levels(value), vt, cell.value())) {
        _writer.append(*meta.empty, true);
    }
}

void mapping_writer::write_counter_cell(const cell_columns& c, atomic_cell_view cell, const defaults& d) {
    if (const auto diff = timestamp_difference(cell.timestamp(), d.ts)) {
        _writer.append(c.meta.ts, diff);
    }
    if (!cell.is_live()) {
        _writer.append(c.meta.deletion_time, seconds(cell.deletion_time()));
        return;
    }
    if (cell.is_counter_update()) {
        fail("a counter update cannot be written to an sstable");
    }
    if (cell.is_live_and_has_ttl()) {
        fail("a counter cell cannot expire");
    }
    const auto& schema = _writer.get_schema();
    const column_id lists[] = {c.value, *c.shard_id, *c.clock};
    const counter_cell_view ccv(cell);
    if (ccv.shard_count() == 0) {
        // A live counter without shards has an empty list, to tell it
        // apart from an absent cell.
        for (const column_id l : lists) {
            _writer.append_null(l, levels{.rep = 0, .def = uint16_t(schema.get_column(l).max_def - 1)});
        }
        return;
    }
    uint16_t rep = 0;
    for (const auto shard : ccv.shards()) {
        const auto id = shard.id().uuid();
        std::array<int8_t, 16> id_bytes;
        write_be<int64_t>(reinterpret_cast<char*>(id_bytes.data()), id.get_most_significant_bits());
        write_be<int64_t>(reinterpret_cast<char*>(id_bytes.data()) + 8, id.get_least_significant_bits());
        _writer.append(c.value, levels{.rep = rep, .def = schema.get_column(c.value).max_def}, shard.value());
        _writer.append(*c.shard_id, levels{.rep = rep, .def = schema.get_column(*c.shard_id).max_def}, bytes_view(id_bytes.data(), id_bytes.size()));
        _writer.append(*c.clock, levels{.rep = rep, .def = schema.get_column(*c.clock).max_def}, shard.logical_clock());
        rep = 1;
    }
}

void mapping_writer::write_udt(const cell_columns& c, collection_mutation_view cmv, const defaults& d) {
    for (const auto& [path, cell] : cmv) {
        const size_t index = deserialize_field_index(path);
        if (index >= c.fields.size()) {
            fail(seastar::format("field {} of a UDT with {} fields", index, c.fields.size()));
        }
        const auto& f = c.fields[index];
        write_atomic_cell(f.meta, f.value, f.type, cell, d);
    }
}

void mapping_writer::write_collection(const cell_columns& c, collection_mutation_view cmv, const defaults& d) {
    if (const auto t = cmv.tomb()) {
        write_tombstone(*c.tomb_ts, *c.tomb_deletion_time, t, d.ts);
    }
    if (c.shape == column_shape::udt) {
        write_udt(c, cmv, d);
        return;
    }

    // The value of an element, and whether its value and its key are empty
    // values with a marker.
    struct element {
        std::optional<managed_bytes_view> value;
        bool empty = false;
        bool key_empty = false;
    };
    auto element_of = [&] (managed_bytes_view path, atomic_cell_view cell) {
        element e;
        if (c.shape == column_shape::set) {
            e.value = path;
        } else if (cell.is_live()) {
            e.value = cell.value();
        }
        e.empty = e.value && e.value->empty() && c.meta.empty;
        e.key_empty = c.shape == column_shape::map && path.empty() && c.key_empty;
        return e;
    };

    // A metadata list is written only if some element has an entry in it.
    size_t count = 0;
    bool any_ts = false, any_ttl = false, any_expiry = false, any_deletion_time = false, any_empty = false, any_key_empty = false;
    for (const auto& [path, cell] : cmv) {
        const auto m = cell_metadata::of(cell, d);
        const auto e = element_of(path, cell);
        any_ts |= bool(m.ts);
        any_ttl |= bool(m.ttl);
        any_expiry |= bool(m.expiry);
        any_deletion_time |= bool(m.deletion_time);
        any_empty |= e.empty;
        any_key_empty |= e.key_empty;
        ++count;
    }
    if (count == 0) {
        return;
    }

    const auto& schema = _writer.get_schema();
    uint16_t rep = 0;
    // Appends an entry of a list leaf: a value, or a null element.
    auto entry = [&] <typename T> (column_id l, const std::optional<T>& v) {
        const uint16_t max_def = schema.get_column(l).max_def;
        if (v) {
            _writer.append(l, levels{.rep = rep, .def = max_def}, *v);
        } else {
            _writer.append_null(l, levels{.rep = rep, .def = uint16_t(max_def - 1)});
        }
    };
    for (const auto& [path, cell] : cmv) {
        const auto m = cell_metadata::of(cell, d);
        const auto e = element_of(path, cell);
        if (c.map_key) {
            append_value(_writer, *c.map_key, levels{.rep = rep, .def = schema.get_column(*c.map_key).max_def}, c.keys, path);
        }
        if (c.list_key) {
            if (path.size_bytes() != 16) {
                fail(seastar::format("a list key has {} bytes", path.size_bytes()));
            }
            _writer.append(*c.list_key, levels{.rep = rep, .def = schema.get_column(*c.list_key).max_def}, path);
        }
        if (e.value) {
            append_value(_writer, c.value, levels{.rep = rep, .def = schema.get_column(c.value).max_def}, c.values, *e.value);
        } else {
            _writer.append_null(c.value, levels{.rep = rep, .def = uint16_t(schema.get_column(c.value).max_def - 1)});
        }
        if (any_ts) {
            entry(c.meta.ts, m.ts);
        }
        if (any_ttl) {
            entry(*c.meta.ttl, m.ttl);
        }
        if (any_expiry) {
            entry(*c.meta.expiry, m.expiry);
        }
        if (any_deletion_time) {
            entry(c.meta.deletion_time, m.deletion_time);
        }
        if (any_empty) {
            entry(*c.meta.empty, e.empty ? std::optional<bool>(true) : std::nullopt);
        }
        if (any_key_empty) {
            entry(*c.key_empty, e.key_empty ? std::optional<bool>(true) : std::nullopt);
        }
        rep = 1;
    }
}

void mapping_writer::write_cells(const row& cells, column_kind kind, const defaults& d) {
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac) {
        const cell_columns& c = column(kind, id);
        switch (c.shape) {
        case column_shape::atomic:
            write_atomic_cell(c.meta, c.value, c.values, ac.as_atomic_cell(_schema.column_at(kind, id)), d);
            break;
        case column_shape::counter:
            write_counter_cell(c, ac.as_atomic_cell(_schema.column_at(kind, id)), d);
            break;
        case column_shape::map:
        case column_shape::set:
        case column_shape::list:
        case column_shape::udt:
            write_collection(c, ac.as_collection_mutation(), d);
            break;
        }
    });
}

std::optional<row_info> mapping_writer::write_static_row(const static_row& sr) {
    check_in_partition();
    if (_partition_has_row) {
        fail("a static row after the first row of the partition");
    }
    if (sr.cells().empty()) {
        return std::nullopt;
    }
    const auto d = pick_defaults(sr.cells(), column_kind::static_column, nullptr, nullptr);
    write_row_level(fragment_kind::static_row, d);
    write_cells(sr.cells(), column_kind::static_column, d);
    return end_row();
}

row_info mapping_writer::write_clustering_row(const clustering_row& cr) {
    check_in_partition();
    if (!cr.key().is_full(_schema)) {
        fail("a clustering row without a full key");
    }
    write_clustering_prefix(cr.key());
    const row_marker& marker = cr.marker();
    const row_tombstone tomb = cr.tomb();
    const auto d = pick_defaults(cr.cells(), column_kind::regular_column, &marker, &tomb);
    const auto& r = _mapping.row();
    write_row_level(marker.is_missing() ? fragment_kind::clustering_row : fragment_kind::clustering_row_with_marker, d);
    if (!marker.is_missing() && !marker.is_live()) {
        _writer.append(r.marker_deletion_time, seconds(marker.deletion_time()));
    }
    if (tomb.regular()) {
        write_tombstone(r.tomb_ts, r.tomb_deletion_time, tomb.regular(), d.ts);
    }
    if (tomb.is_shadowable()) {
        write_tombstone(r.shadowable_tomb_ts, r.shadowable_tomb_deletion_time, tomb.tomb(), d.ts);
    }
    write_cells(cr.cells(), column_kind::regular_column, d);
    return end_row();
}

row_info mapping_writer::write_range_tombstone_bound(const clustering_key_prefix& prefix, bound_kind kind, tombstone t) {
    check_in_partition();
    if (!t) {
        fail("a range tombstone bound without a tombstone");
    }
    fragment_kind fk;
    switch (kind) {
    case bound_kind::incl_start: fk = fragment_kind::incl_start; break;
    case bound_kind::excl_start: fk = fragment_kind::excl_start; break;
    case bound_kind::incl_end: fk = fragment_kind::incl_end; break;
    case bound_kind::excl_end: fk = fragment_kind::excl_end; break;
    default: fail(seastar::format("bound kind {}", static_cast<int>(kind)));
    }
    write_clustering_prefix(prefix);
    const auto& r = _mapping.row();
    _writer.append(r.kind, int32_t(fk));
    _writer.append(r.ts, int64_t(t.timestamp));
    _writer.append(r.tomb_deletion_time, seconds(t.deletion_time));
    return end_row();
}

std::optional<row_info> mapping_writer::end_partition() {
    check_in_partition();
    std::optional<row_info> r;
    if (!_partition_has_row) {
        _writer.append(_mapping.row().kind, int32_t(fragment_kind::none));
        r = end_row();
    }
    _in_partition = false;
    return r;
}

writer_result mapping_writer::finish(std::vector<key_value> extra) {
    if (_in_partition) {
        fail("the file ends inside a partition");
    }
    auto kv = file_key_value_metadata();
    kv.insert(kv.end(), std::make_move_iterator(extra.begin()), std::make_move_iterator(extra.end()));
    return _writer.finish(std::move(kv));
}

} // namespace sstables::parquet
