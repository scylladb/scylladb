/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <cstdint>
#include "utils/assert.hh"
#include "utils/hashers.hh"
#include "utils/managed_bytes.hh"
#include "cql3/result_set.hh"
#include "cql3/column_identifier.hh"
#include "db/marshal/type_parser.hh"

namespace cql3 {

metadata::metadata(std::vector<lw_shared_ptr<column_specification>> names_)
        : _flags(flag_enum_set())
        , _column_info(make_lw_shared<column_info>(std::move(names_)))
{
    if (!_column_info->_names.empty() && column_specification::all_in_same_table(_column_info->_names)) {
        _flags.set<flag::GLOBAL_TABLES_SPEC>();
    }
}

metadata::metadata(flag_enum_set flags, std::vector<lw_shared_ptr<column_specification>> names_, uint32_t column_count,
        lw_shared_ptr<const service::pager::paging_state> paging_state)
    : _flags(flags)
    , _column_info(make_lw_shared<column_info>(std::move(names_), column_count))
    , _paging_state(std::move(paging_state))
{
    if (!_column_info->_names.empty() && column_specification::all_in_same_table(_column_info->_names)) {
        _flags.set<flag::GLOBAL_TABLES_SPEC>();
    }
}

// The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
uint32_t metadata::value_count() const {
    return _flags.contains<flag::NO_METADATA>() ? _column_info->_column_count : _column_info->_names.size();
}

void metadata::add_non_serialized_column(lw_shared_ptr<column_specification> name) {
    // See comment above. Because columnCount doesn't account the newly added name, it
    // won't be serialized.
    _column_info->_names.emplace_back(std::move(name));
}

void metadata::set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state) {
    _flags.set<flag::HAS_MORE_PAGES>();
    _paging_state = std::move(paging_state);
}

void metadata::maybe_set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state) {
    SCYLLA_ASSERT(paging_state);
    if (paging_state->get_remaining() > 0) {
        set_paging_state(std::move(paging_state));
    } else {
        _flags.remove<flag::HAS_MORE_PAGES>();
        _paging_state = nullptr;
    }
}

void metadata::set_skip_metadata() {
    _flags.set<flag::NO_METADATA>();
}

metadata::flag_enum_set metadata::flags() const {
    return _flags;
}

lw_shared_ptr<const service::pager::paging_state> metadata::paging_state() const {
    return _paging_state;
}

// Metadata_id is a checksum computed from given metadata to track schema changes in prepared statements.
// Originally introduced in CQLv5.
cql3::cql_metadata_id_type metadata::calculate_metadata_id() const {
    auto h = sha256_hasher();
    for (uint32_t i = 0; i < _column_info->_column_count; ++i) {
        feed_hash(h, _column_info->_names[i]->name->name());
        feed_hash(h, _column_info->_names[i]->type->name());
    }
    // Return first 16 bytes to have the same length as Cassandra's MD5
    return cql_metadata_id_type(h.finalize().substr(0, 16));
}

prepared_metadata::prepared_metadata(const std::vector<lw_shared_ptr<column_specification>>& names,
                                     const std::vector<uint16_t>& partition_key_bind_indices,
                                     bool is_conditional)
    : _names{names}
    , _partition_key_bind_indices{partition_key_bind_indices}
{
    if (!names.empty() && column_specification::all_in_same_table(_names)) {
        _flags.set<flag::GLOBAL_TABLES_SPEC>();
    }

    if (is_conditional) {
        _flags.set<flag::LWT>();
    }
}

prepared_metadata::flag_enum_set prepared_metadata::flags() const {
    return _flags;
}

const std::vector<lw_shared_ptr<column_specification>>& prepared_metadata::names() const {
    return _names;
}

const std::vector<uint16_t>& prepared_metadata::partition_key_bind_indices() const {
    return _partition_key_bind_indices;
}

result_set::result_set(std::vector<lw_shared_ptr<column_specification>> metadata_)
    : _metadata(::make_shared<metadata>(std::move(metadata_)))
{ }

result_set::result_set(::shared_ptr<metadata> metadata)
    : _metadata(std::move(metadata))
{ }

size_t result_set::size() const {
    return _rows.size();
}

bool result_set::empty() const {
    return _rows.empty();
}

void result_set::add_row(std::vector<managed_bytes_opt> row) {
    SCYLLA_ASSERT(row.size() == _metadata->value_count());
    _rows.emplace_back(std::move(row));
}

void result_set::add_row(std::vector<bytes_opt> row) {
    row_type new_row;
    new_row.reserve(row.size());
    for (auto& bo : row) {
        new_row.emplace_back(bo ? managed_bytes_opt(*bo) : managed_bytes_opt());
    }
    add_row(std::move(new_row));
}

void result_set::add_column_value(managed_bytes_opt value) {
    if (_rows.empty() || _rows.back().size() == _metadata->value_count()) {
        std::vector<managed_bytes_opt> row;
        row.reserve(_metadata->value_count());
        _rows.emplace_back(std::move(row));
    }

    _rows.back().emplace_back(std::move(value));
}

void result_set::add_column_value(bytes_opt value) {
    add_column_value(to_managed_bytes_opt(value));
}

void result_set::reverse() {
    std::reverse(_rows.begin(), _rows.end());
}

void result_set::trim(size_t limit) {
    if (_rows.size() > limit) {
        _rows.resize(limit);
    }
}

metadata& result_set::get_metadata() {
    return *_metadata;
}

const metadata& result_set::get_metadata() const {
    return *_metadata;
}

const utils::chunked_vector<std::vector<managed_bytes_opt>>& result_set::rows() const {
    return _rows;
}

shared_ptr<const cql3::metadata>
make_empty_metadata() {
    static thread_local shared_ptr<const metadata> empty_metadata_cache = [] {
        auto result = ::make_shared<metadata>(std::vector<lw_shared_ptr<cql3::column_specification>>{});
        result->set_skip_metadata();
        return result;
    }();
    return empty_metadata_cache;
}

result_set_serialized serialize_result_set(const result_set& rs) {
    const auto& md = rs.get_metadata();

    std::vector<column_specification_serialized> col_specs;
    col_specs.reserve(md.get_names().size());
    for (const auto& col : md.get_names()) {
        col_specs.push_back(column_specification_serialized{
            .ks_name = col->ks_name,
            .cf_name = col->cf_name,
            .column_name = col->name->text(),
            .type_name = col->type->name()
        });
    }

    std::optional<service::pager::paging_state> paging_state_copy;
    if (auto ps = md.paging_state()) {
        paging_state_copy = *ps;
    }

    // Convert rows from managed_bytes_opt to bytes_opt
    utils::chunked_vector<std::vector<bytes_opt>> rows;
    rows.reserve(rs.rows().size());
    for (const auto& row : rs.rows()) {
        std::vector<bytes_opt> converted_row;
        converted_row.reserve(row.size());
        for (const auto& cell : row) {
            converted_row.push_back(to_bytes_opt(cell));
        }
        rows.push_back(std::move(converted_row));
    }

    return result_set_serialized{
        .metadata = metadata_serialized{
            .flags = md.flags().mask(),
            .column_specs = std::move(col_specs),
            .column_count = md.column_count(),
            .paging_state = std::move(paging_state_copy)
        },
        .rows = std::move(rows)
    };
}

result_set deserialize_result_set(result_set_serialized&& serialized) {
    std::vector<lw_shared_ptr<column_specification>> names;
    names.reserve(serialized.metadata.column_specs.size());
    for (auto& col : serialized.metadata.column_specs) {
        auto type = db::marshal::type_parser::parse(col.type_name);
        auto col_id = ::make_shared<column_identifier>(col.column_name, true);
        names.push_back(make_lw_shared<column_specification>(
            std::move(col.ks_name),
            std::move(col.cf_name),
            std::move(col_id),
            std::move(type)
        ));
    }

    lw_shared_ptr<const service::pager::paging_state> paging_state;
    if (serialized.metadata.paging_state) {
        paging_state = make_lw_shared<const service::pager::paging_state>(
            std::move(*serialized.metadata.paging_state));
    }

    auto md = ::make_shared<metadata>(
        metadata::flag_enum_set::from_mask(serialized.metadata.flags),
        std::move(names),
        serialized.metadata.column_count,
        std::move(paging_state)
    );

    result_set rs(std::move(md));
    for (auto& row : serialized.rows) {
        rs.add_row(std::move(row));
    }
    return rs;
}

}
