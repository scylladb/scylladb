/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/mapping.hh"

#include <bit>
#include <stdexcept>

#include <seastar/core/byteorder.hh>
#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>

#include "db/marshal/type_parser.hh"
#include "schema/schema.hh"
#include "sstables/parquet/writer.hh"
#include "sstables/types.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "utils/fragment_range.hh"
#include "utils/log.hh"
#include "utils/utf8.hh"

namespace sstables::parquet {

static logging::logger mapping_logger("parquet_mapping");

std::vector<key_value> file_key_value_metadata() {
    return {key_value{.key = sstring(layout_key), .value = sstring(layout_v1)}};
}

static bool needs_escape(bytes_view cql_name) {
    const auto chars = std::string_view(reinterpret_cast<const char*>(cql_name.data()), cql_name.size());
    return cql_name.empty() || chars.starts_with(reserved_name) || !utils::utf8::validate(cql_name);
}

sstring parquet_name(bytes_view cql_name) {
    if (needs_escape(cql_name)) {
        return seastar::format("{}{}", escaped_name_prefix, to_hex(cql_name));
    }
    return sstring(reinterpret_cast<const char*>(cql_name.data()), cql_name.size());
}

bool parquet_names_fit(const ::schema& s) noexcept {
    auto fits = [] (bytes_view name) {
        const size_t size = needs_escape(name) ? escaped_name_prefix.size() + 2 * name.size() : name.size();
        return size <= max_name_size;
    };
    for (const auto& c : s.all_columns()) {
        if (!fits(c.name())) {
            return false;
        }
        if (c.type->is_user_type() && c.type->is_multi_cell()) {
            for (const auto& f : static_cast<const user_type_impl&>(*c.type).field_names()) {
                if (!fits(f)) {
                    return false;
                }
            }
        }
    }
    return true;
}

value_type value_type_of(const abstract_type& type) {
    const abstract_type& t = type.without_reversed();
    if (!t.is_atomic()) {
        throw std::invalid_argument(seastar::format("Parquet mapping: type {} is not atomic", t.name()));
    }
    using kind = abstract_type::kind;
    switch (t.get_kind()) {
    case kind::boolean: return value_type::boolean;
    case kind::byte: return value_type::tinyint;
    case kind::short_kind: return value_type::smallint;
    case kind::int32: return value_type::int32;
    case kind::simple_date: return value_type::date;
    case kind::long_kind: return value_type::bigint;
    case kind::timestamp:
    case kind::date: // The legacy DateType has the serialized form of a timestamp.
        return value_type::timestamp;
    case kind::time: return value_type::time;
    case kind::float_kind: return value_type::float_;
    case kind::double_kind: return value_type::double_;
    case kind::uuid:
    case kind::timeuuid:
        return value_type::uuid;
    case kind::ascii:
    case kind::utf8:
        return value_type::string;
    default:
        return value_type::bytes;
    }
}

physical_type physical_type_of(value_type t) noexcept {
    switch (t) {
    case value_type::boolean: return physical_type::boolean;
    case value_type::tinyint:
    case value_type::smallint:
    case value_type::int32:
    case value_type::date:
        return physical_type::int32;
    case value_type::bigint:
    case value_type::timestamp:
    case value_type::time:
        return physical_type::int64;
    case value_type::float_: return physical_type::float_;
    case value_type::double_: return physical_type::double_;
    case value_type::uuid: return physical_type::fixed_len_byte_array;
    case value_type::string:
    case value_type::bytes:
        return physical_type::byte_array;
    }
    std::abort();
}

static logical_type int_type(int8_t bits) {
    return logical_type{.k = logical_type::kind::integer, .bit_width = bits, .is_signed = true};
}

column_options column_options_of(value_type t) {
    switch (t) {
    case value_type::boolean:
    case value_type::float_:
    case value_type::double_:
    case value_type::bytes:
        return {};
    case value_type::tinyint: return {.converted = converted_type::int_8, .logical = int_type(8)};
    case value_type::smallint: return {.converted = converted_type::int_16, .logical = int_type(16)};
    case value_type::int32: return {.converted = converted_type::int_32, .logical = int_type(32)};
    case value_type::bigint: return {.converted = converted_type::int_64, .logical = int_type(64)};
    case value_type::date:
        return {.converted = converted_type::date, .logical = logical_type{.k = logical_type::kind::date}};
    case value_type::timestamp:
        return {.converted = converted_type::timestamp_millis,
                .logical = logical_type{.k = logical_type::kind::timestamp, .is_adjusted_to_utc = true, .unit = time_unit::millis}};
    case value_type::time:
        // A CQL time is a time of day without a time zone. TIME with
        // nanoseconds has no converted type.
        return {.logical = logical_type{.k = logical_type::kind::time, .is_adjusted_to_utc = false, .unit = time_unit::nanos}};
    case value_type::uuid:
        return {.type_length = 16, .logical = logical_type{.k = logical_type::kind::uuid}};
    case value_type::string:
        return {.converted = converted_type::utf8, .logical = logical_type{.k = logical_type::kind::string}};
    }
    std::abort();
}

std::optional<uint32_t> cql_fixed_width(value_type t) noexcept {
    switch (t) {
    case value_type::boolean:
    case value_type::tinyint:
        return 1;
    case value_type::smallint: return 2;
    case value_type::int32:
    case value_type::date:
    case value_type::float_:
        return 4;
    case value_type::bigint:
    case value_type::timestamp:
    case value_type::time:
    case value_type::double_:
        return 8;
    case value_type::uuid: return 16;
    case value_type::string:
    case value_type::bytes:
        return std::nullopt;
    }
    std::abort();
}

// The date's serialized form is an unsigned day number with the epoch at
// 2^31. Flipping the top bit gives the signed day relative to the epoch.
static constexpr uint32_t date_bias = uint32_t(1) << 31;

template <typename T>
static T read_exactly(managed_bytes_view v, value_type t) {
    if (v.size_bytes() != sizeof(T)) {
        on_internal_error(mapping_logger, seastar::format("Parquet mapping: a value of type {} has {} bytes, expected {}",
                static_cast<int>(t), v.size_bytes(), sizeof(T)));
    }
    return read_simple_exactly<T>(v);
}

bool append_value(writer& w, column_id c, levels l, value_type t, managed_bytes_view v) {
    const bool empty = v.empty() && has_empty_marker(t);
    switch (t) {
    case value_type::boolean:
        w.append(c, l, !empty && read_exactly<int8_t>(v, t) != 0);
        break;
    case value_type::tinyint:
        w.append(c, l, empty ? int32_t(0) : int32_t(read_exactly<int8_t>(v, t)));
        break;
    case value_type::smallint:
        w.append(c, l, empty ? int32_t(0) : int32_t(read_exactly<int16_t>(v, t)));
        break;
    case value_type::int32:
        w.append(c, l, empty ? int32_t(0) : read_exactly<int32_t>(v, t));
        break;
    case value_type::date:
        w.append(c, l, empty ? int32_t(0) : int32_t(read_exactly<uint32_t>(v, t) ^ date_bias));
        break;
    case value_type::bigint:
    case value_type::timestamp:
    case value_type::time:
        w.append(c, l, empty ? int64_t(0) : read_exactly<int64_t>(v, t));
        break;
    case value_type::float_:
        w.append(c, l, empty ? 0.0f : std::bit_cast<float>(read_exactly<uint32_t>(v, t)));
        break;
    case value_type::double_:
        w.append(c, l, empty ? 0.0 : std::bit_cast<double>(read_exactly<uint64_t>(v, t)));
        break;
    case value_type::uuid:
        if (empty) {
            static constexpr std::array<int8_t, 16> zero{};
            w.append(c, l, bytes_view(zero.data(), zero.size()));
        } else {
            if (v.size_bytes() != 16) {
                on_internal_error(mapping_logger, seastar::format("Parquet mapping: a UUID has {} bytes", v.size_bytes()));
            }
            w.append(c, l, v);
        }
        break;
    case value_type::string:
    case value_type::bytes:
        w.append(c, l, v);
        break;
    }
    return empty;
}

template <typename T>
static bytes serialize_be(T v) {
    bytes b(bytes::initialized_later(), sizeof(T));
    write_be(reinterpret_cast<char*>(b.data()), v);
    return b;
}

static void check_physical(value_type t, physical_type expected) {
    if (physical_type_of(t) != expected) {
        on_internal_error(mapping_logger, seastar::format("Parquet mapping: value type {} is not stored as physical type {}",
                static_cast<int>(t), static_cast<int>(expected)));
    }
}

bytes to_cql_value(value_type t, bool v) {
    check_physical(t, physical_type::boolean);
    return serialize_be<int8_t>(v ? 1 : 0);
}

bytes to_cql_value(value_type t, int32_t v) {
    check_physical(t, physical_type::int32);
    switch (t) {
    case value_type::tinyint: return serialize_be<int8_t>(v);
    case value_type::smallint: return serialize_be<int16_t>(v);
    case value_type::date: return serialize_be<uint32_t>(uint32_t(v) ^ date_bias);
    default: return serialize_be<int32_t>(v);
    }
}

bytes to_cql_value(value_type t, int64_t v) {
    check_physical(t, physical_type::int64);
    return serialize_be<int64_t>(v);
}

bytes to_cql_value(value_type t, float v) {
    check_physical(t, physical_type::float_);
    return serialize_be<uint32_t>(std::bit_cast<uint32_t>(v));
}

bytes to_cql_value(value_type t, double v) {
    check_physical(t, physical_type::double_);
    return serialize_be<uint64_t>(std::bit_cast<uint64_t>(v));
}

// table_mapping

static constexpr auto optional_rep = repetition_type::optional;
static constexpr auto required_rep = repetition_type::required;

key_columns table_mapping::add_key_value(const sstring& name, data_type type) {
    type = type->without_reversed().shared_from_this();
    const auto vt = value_type_of(*type);
    auto opts = column_options_of(vt);
    // The reader matches key columns by position, so they stay in the
    // file's schema even when they are null in the whole file.
    opts.always_present = true;
    const column_id value = _schema.add_column(schema::root, name, optional_rep, physical_type_of(vt), std::move(opts));
    return key_columns{.parquet_name = name, .type = std::move(type), .values = vt, .value = value};
}

void table_mapping::add_key_metadata(key_columns& k) {
    if (has_empty_marker(k.values)) {
        const node_id g = _schema.add_group(_cells, k.parquet_name, required_rep);
        k.empty = _schema.add_column(g, "empty", optional_rep, physical_type::boolean);
    }
}

cell_columns table_mapping::make_cell_columns(bytes_view name, data_type type) {
    cell_columns c{.name = bytes(name), .parquet_name = parquet_name(name), .type = type, .shape = column_shape::atomic};
    if (type->is_counter()) {
        c.shape = column_shape::counter;
    } else if (type->is_multi_cell()) {
        if (type->is_map()) {
            const auto& m = static_cast<const map_type_impl&>(*type);
            c.shape = column_shape::map;
            c.keys = value_type_of(*m.get_keys_type());
            c.values = value_type_of(*m.get_values_type());
        } else if (type->is_set()) {
            c.shape = column_shape::set;
            c.values = value_type_of(*static_cast<const set_type_impl&>(*type).get_elements_type());
        } else if (type->is_list()) {
            c.shape = column_shape::list;
            c.values = value_type_of(*static_cast<const list_type_impl&>(*type).get_elements_type());
        } else if (type->is_user_type()) {
            c.shape = column_shape::udt;
        } else {
            throw std::invalid_argument(seastar::format("Parquet mapping: unsupported multi-cell type {}", type->name()));
        }
    } else {
        c.values = value_type_of(*type);
    }
    return c;
}

void table_mapping::add_value(cell_columns& c) {
    const sstring& name = c.parquet_name;
    switch (c.shape) {
    case column_shape::atomic:
        c.value = _schema.add_column(schema::root, name, optional_rep, physical_type_of(c.values), column_options_of(c.values));
        break;
    case column_shape::map: {
        auto m = _schema.add_map(schema::root, name, physical_type_of(c.keys), column_options_of(c.keys),
                physical_type_of(c.values), column_options_of(c.values));
        c.map_key = m.key;
        c.value = m.value;
        break;
    }
    case column_shape::set:
        // Dead elements stay in the list, so elements are never null.
        c.value = _schema.add_list(schema::root, name, physical_type_of(c.values), column_options_of(c.values), required_rep);
        break;
    case column_shape::list:
        // A dead element is a null.
        c.value = _schema.add_list(schema::root, name, physical_type_of(c.values), column_options_of(c.values), optional_rep);
        break;
    case column_shape::udt: {
        const auto& ut = static_cast<const user_type_impl&>(*c.type);
        // The group is always there, so it adds no definition level.
        const node_id g = _schema.add_group(schema::root, name, required_rep);
        for (size_t i = 0; i < ut.size(); ++i) {
            field_columns f{.name = bytes(ut.field_name(i)), .parquet_name = parquet_name(ut.field_name(i)),
                    .type = value_type_of(*ut.type(i))};
            f.value = _schema.add_column(g, f.parquet_name, optional_rep, physical_type_of(f.type), column_options_of(f.type));
            c.fields.push_back(std::move(f));
        }
        break;
    }
    case column_shape::counter:
        c.value = _schema.add_list(schema::root, name, physical_type::int64, {}, required_rep);
        break;
    }
}

void table_mapping::add_metadata(cell_columns& c) {
    const node_id g = _schema.add_group(_cells, c.parquet_name, required_rep);
    auto scalar = [&] (node_id parent, const char* leaf, physical_type t, column_options opts = {}) {
        return _schema.add_column(parent, leaf, optional_rep, t, std::move(opts));
    };
    auto list = [&] (const char* leaf, physical_type t, column_options opts = {}, repetition_type element = optional_rep) {
        return _schema.add_list(g, leaf, t, std::move(opts), element);
    };
    auto scalar_metadata = [&] (node_id parent, value_type vt) {
        cell_metadata_columns m{};
        m.ts = scalar(parent, "ts", physical_type::int64);
        m.ttl = scalar(parent, "ttl", physical_type::int32);
        m.expiry = scalar(parent, "expiry", physical_type::int64);
        m.deletion_time = scalar(parent, "deletion_time", physical_type::int64);
        if (has_empty_marker(vt)) {
            m.empty = scalar(parent, "empty", physical_type::boolean);
        }
        return m;
    };
    auto uuid_opts = [] {
        return column_options{.type_length = 16, .logical = logical_type{.k = logical_type::kind::uuid}};
    };
    switch (c.shape) {
    case column_shape::atomic:
        c.meta = scalar_metadata(g, c.values);
        return;
    case column_shape::counter:
        c.meta.ts = scalar(g, "ts", physical_type::int64);
        c.meta.deletion_time = scalar(g, "deletion_time", physical_type::int64);
        c.shard_id = list("shard_id", physical_type::fixed_len_byte_array, uuid_opts(), required_rep);
        c.clock = list("clock", physical_type::int64, {}, required_rep);
        return;
    case column_shape::udt: {
        const node_id fields = _schema.add_group(g, "fields", required_rep);
        for (auto& f : c.fields) {
            f.meta = scalar_metadata(_schema.add_group(fields, f.parquet_name, required_rep), f.type);
        }
        break;
    }
    case column_shape::map:
    case column_shape::set:
    case column_shape::list:
        c.meta.ts = list("ts", physical_type::int64);
        c.meta.ttl = list("ttl", physical_type::int32);
        c.meta.expiry = list("expiry", physical_type::int64);
        c.meta.deletion_time = list("deletion_time", physical_type::int64);
        if (has_empty_marker(c.values)) {
            c.meta.empty = list("empty", physical_type::boolean);
        }
        if (c.shape == column_shape::map && has_empty_marker(c.keys)) {
            c.key_empty = list("key_empty", physical_type::boolean);
        }
        if (c.shape == column_shape::list) {
            c.list_key = list("key", physical_type::fixed_len_byte_array, uuid_opts(), required_rep);
        }
        break;
    }
    c.tomb_ts = scalar(g, "tomb_ts", physical_type::int64);
    c.tomb_deletion_time = scalar(g, "tomb_deletion_time", physical_type::int64);
}

static data_type parse_type(bytes_view name) {
    try {
        return db::marshal::type_parser::parse(std::string_view(reinterpret_cast<const char*>(name.data()), name.size()));
    } catch (...) {
        throw std::invalid_argument(seastar::format("Parquet mapping: cannot parse type {}: {}",
                std::string_view(reinterpret_cast<const char*>(name.data()), name.size()), std::current_exception()));
    }
}

std::vector<data_type> partition_key_types(const serialization_header& h) {
    static constexpr std::string_view composite = "org.apache.cassandra.db.marshal.CompositeType";
    const auto& raw = h.pk_type_name.value;
    const auto name = std::string_view(reinterpret_cast<const char*>(raw.data()), raw.size());
    if (!name.starts_with(composite) || name.size() == composite.size() || name[composite.size()] != '(') {
        return {parse_type(raw)};
    }
    try {
        db::marshal::type_parser parser(name.substr(composite.size()));
        auto types = parser.get_type_parameters();
        if (types.empty()) {
            throw std::invalid_argument("no components");
        }
        return types;
    } catch (...) {
        throw std::invalid_argument(seastar::format("Parquet mapping: cannot parse partition key type {}: {}", name, std::current_exception()));
    }
}

table_mapping table_mapping::from_header(const serialization_header& h, std::span<const sstring> key_names) {
    table_mapping m;
    const auto pk_types = partition_key_types(h);
    const auto& ck_names = h.clustering_key_types_names.elements;
    if (key_names.size() != pk_types.size() + ck_names.size()) {
        throw std::invalid_argument(seastar::format("Parquet mapping: {} key names for {} partition key and {} clustering key columns",
                key_names.size(), pk_types.size(), ck_names.size()));
    }

    // Key columns come first, in key order.
    size_t key = 0;
    for (const auto& t : pk_types) {
        m._partition_key.push_back(m.add_key_value(key_names[key++], t));
    }
    for (const auto& t : ck_names) {
        m._clustering_key.push_back(m.add_key_value(key_names[key++], parse_type(t.value)));
    }

    // Then the values of the static and the regular columns.
    for (const auto& c : h.static_columns.elements) {
        m._static.push_back(m.make_cell_columns(c.name.value, parse_type(c.type_name.value)));
        m.add_value(m._static.back());
    }
    for (const auto& c : h.regular_columns.elements) {
        m._regular.push_back(m.make_cell_columns(c.name.value, parse_type(c.type_name.value)));
        m.add_value(m._regular.back());
    }

    // Then the reserved group.
    m._reserved = m._schema.add_group(schema::root, sstring(reserved_name), required_rep);
    auto row_leaf = [&] (const char* name, physical_type t) {
        return m._schema.add_column(m._reserved, name, optional_rep, t);
    };
    m._row = row_columns{
        .kind = row_leaf("kind", physical_type::int32),
        .partition_tomb_ts = row_leaf("partition_tomb_ts", physical_type::int64),
        .partition_tomb_deletion_time = row_leaf("partition_tomb_deletion_time", physical_type::int64),
        .ts = row_leaf("ts", physical_type::int64),
        .ttl = row_leaf("ttl", physical_type::int32),
        .expiry = row_leaf("expiry", physical_type::int64),
        .marker_deletion_time = row_leaf("marker_deletion_time", physical_type::int64),
        .tomb_ts = row_leaf("tomb_ts", physical_type::int64),
        .tomb_deletion_time = row_leaf("tomb_deletion_time", physical_type::int64),
        .shadowable_tomb_ts = row_leaf("shadowable_tomb_ts", physical_type::int64),
        .shadowable_tomb_deletion_time = row_leaf("shadowable_tomb_deletion_time", physical_type::int64),
    };
    m._cells = m._schema.add_group(m._reserved, "cells", required_rep);
    for (auto* keys : {&m._partition_key, &m._clustering_key}) {
        for (auto& k : *keys) {
            m.add_key_metadata(k);
        }
    }
    for (auto* columns : {&m._static, &m._regular}) {
        for (auto& c : *columns) {
            m.add_metadata(c);
        }
    }
    return m;
}

std::vector<sstring> key_parquet_names(const ::schema& s) {
    std::vector<sstring> r;
    r.reserve(s.partition_key_size() + s.clustering_key_size());
    for (const auto& c : s.partition_key_columns()) {
        r.push_back(parquet_name(c.name()));
    }
    for (const auto& c : s.clustering_key_columns()) {
        r.push_back(parquet_name(c.name()));
    }
    return r;
}

} // namespace sstables::parquet
