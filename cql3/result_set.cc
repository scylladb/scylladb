/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <cstdint>
#include "types/json_utils.hh"
#include "utils/assert.hh"
#include "utils/hashers.hh"
#include "utils/rjson.hh"
#include "cql3/result_set.hh"

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

template <typename OStream>
future<> do_print_query_results_text(OStream os, const cql3::result& result) {
    const auto& metadata = result.get_metadata();
    const auto& column_metadata = metadata.get_names();

    struct column_values {
        size_t max_size{0};
        sstring header_format;
        sstring row_format;
        std::vector<sstring> values;

        void add(sstring value) {
            max_size = std::max(max_size, value.size());
            values.push_back(std::move(value));
        }
    };

    std::vector<column_values> columns;
    columns.resize(column_metadata.size());

    for (size_t i = 0; i < column_metadata.size(); ++i) {
        columns[i].add(column_metadata[i]->name->text());
    }

    for (const auto& row : result.result_set().rows()) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (row[i]) {
                columns[i].add(column_metadata[i]->type->to_string(linearized(managed_bytes_view(*row[i]))));
            } else {
                columns[i].add("");
            }
        }
    }

    std::vector<sstring> separators(columns.size(), sstring());
    for (size_t i = 0; i < columns.size(); ++i) {
        auto& col_values = columns[i];
        col_values.header_format = seastar::format(" {{:<{}}} ", col_values.max_size);
        col_values.row_format = seastar::format(" {{:>{}}} ", col_values.max_size);
        for (size_t c = 0; c < col_values.max_size; ++c) {
            separators[i] += "-";
        }
    }

    for (size_t r = 0; r < result.result_set().rows().size() + 1; ++r) {
        std::vector<sstring> row;
        row.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& format = r == 0 ? columns[i].header_format : columns[i].row_format;
            row.push_back(fmt::format(fmt::runtime(std::string_view(format)), columns[i].values[r]));
        }
        co_await os("{}\n", fmt::join(row, "|"));
        if (!r) {
            co_await os("-{}-\n", fmt::join(separators, "-+-"));
        }
    }
}

template <typename OStream>
future<> do_print_query_results_json(OStream os, const cql3::result& result) {
    const auto& metadata = result.get_metadata();
    const auto& column_metadata = metadata.get_names();

    co_await os("[");
    bool first = true;
    for (const auto& row : result.result_set().rows()) {
        if (first) {
            first = false;
        } else {
            co_await os(",");
        }
        co_await os("{");
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) {
                co_await os(",");
            }
            co_await os("{}:", rjson::quote_json_string(column_metadata[i]->name->text()));
            if (!row[i] || row[i]->empty()) {
                co_await os("null");
                continue;
            }
            const auto value = to_json_string(*column_metadata[i]->type, *row[i]);
            co_await os("{}", value);
        }
        co_await os("}");
    }
    co_await os("]");
}

struct std_ostream_wrapper {
    std::ostream& os;

    future<> operator()(auto&& raw) {
        os << raw;
        return make_ready_future<>();
    }
    future<> operator()(std::string_view fmt, auto&& arg1, auto&&... args) {
        fmt::print(os, fmt::runtime(fmt), std::forward<decltype(arg1)>(arg1), std::forward<decltype(args)>(args)...);
        return make_ready_future<>();
    }
};

future<> print_query_results_text(std::ostream& os, const result& result) {
    return do_print_query_results_text(std_ostream_wrapper{os}, result);
}

future<> print_query_results_json(std::ostream& os, const result& result) {
    return do_print_query_results_json(std_ostream_wrapper{os}, result);
}

struct seastar_outputs_stream_wrapper {
    seastar::output_stream<char>& os;

    future<> operator()(std::string_view raw) {
        co_await os.write(raw.data(), raw.size());
    }
    future<> operator()(std::string_view fmt, auto&& arg1, auto&&... args) {
        auto str = fmt::format(fmt::runtime(fmt), std::forward<decltype(arg1)>(arg1), std::forward<decltype(args)>(args)...);
        co_await os.write(str.data(), str.size());
    }
};

future<> print_query_results_text(seastar::output_stream<char>& os, const result& result) {
    return do_print_query_results_text(seastar_outputs_stream_wrapper{os}, result);
}

future<> print_query_results_json(seastar::output_stream<char>& os, const result& result) {
    return do_print_query_results_json(seastar_outputs_stream_wrapper{os}, result);
}

}
