/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cdc/cdc_options.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "index/external_index.hh"
#include "index/fulltext_index.hh"
#include "index/pattern_index.hh"
#include "index/vector_index.hh"
#include "utils/UUID_gen.hh"
#include "utils/rjson.hh"

#include <boost/algorithm/string.hpp>
#include <ranges>

namespace secondary_index {

using cql3::statements::index_target;

static constexpr auto TC_TARGET_KEY = "tc";
static constexpr auto PK_TARGET_KEY = "pk";
static constexpr auto FC_TARGET_KEY = "fc";

namespace {

using external_index_types = std::tuple<fulltext_index, vector_index, pattern_index>;

template <typename Fn>
void for_each_external_index_type(Fn&& fn) {
    [&]<typename... Ts>(std::tuple<Ts...>*) {
        (fn.template operator()<Ts>(), ...);
    }(static_cast<external_index_types*>(nullptr));
}

} // anonymous namespace

bool external_index::view_should_exist() const {
    return false;
}

/// Returns a timeuuid representing the time at which the index was created.
/// This is used to determine if the index needs to be rebuilt, and to enable
/// routing by creation time when multiple vector indexes exist on the same column.
utils::UUID external_index::index_version(const schema& schema) {
    return utils::UUID_gen::get_time_UUID();
}

void external_index::check_cdc_options(const schema& schema, std::string_view search_type_name, bool index_already_exists) {
    auto cdc_options = schema.cdc_options();
    auto ttl = cdc_options.ttl();
    auto delta_mode = cdc_options.get_delta_mode();
    auto postimage = cdc_options.postimage();
    if ((ttl && ttl < VS_TTL_SECONDS) ||
        (delta_mode != cdc::delta_mode::full && !postimage)) {
        throw exceptions::invalid_request_exception(
            index_already_exists ?
            format("{} is enabled on this table.\n"
            "The CDC log must meet the minimal requirements for external indexes.\n"
            "This means that the CDC's TTL must be at least {} seconds (24 hours), "
            "and the CDC's delta mode must be set to 'full' or postimage must be enabled.",
            search_type_name, VS_TTL_SECONDS) :
            format("To enable {} on this table, "
            "the CDC log must meet the minimal requirements for external indexes.\n"
            "CDC's TTL must be at least {} seconds (24 hours), "
            "and the CDC's delta mode must be set to 'full' or postimage must be enabled.",
            search_type_name, VS_TTL_SECONDS));
    }
}

void external_index::check_uses_tablets(const schema& schema, const data_dictionary::database& db) const {
    const auto& keyspace = db.find_keyspace(schema.ks_name());
    if (!keyspace.uses_tablets()) {
        throw exceptions::invalid_request_exception(
            format("Creating a {} index requires the base table's keyspace to use tablets.\n"
            "Please alter the keyspace to use tablets and try again.", index_type_name()));
    }
}

void external_index::check_regular_column(const column_definition& column) const {
    // Only a regular column can be served:
    // - partition and clustering key columns: the Vector Store's full scan reads the column's
    //   `writetime()`, which ScyllaDB refuses for a key column,
    // - static columns: the Vector Store fetches each row by its full primary key, which
    //   ScyllaDB refuses when only static columns are selected.
    if (!column.is_regular()) {
        throw exceptions::invalid_request_exception(
            fmt::format("Creating a {} index requires a regular column, but column {} is a {} column",
            index_type_name(), column.name_as_text(), column_kind_name(column.kind)));
    }
}

void external_index::check_cdc_options_if_present(const schema& s) {
    for_each_external_index_type([&]<typename T>() {
        if (T::has_index(s)) {
            T::check_cdc_options(s);
        }
    });
}

void external_index::validate_cdc_not_disabled_if_present(const schema& s) {
    for_each_external_index_type([&]<typename T>() {
        if (T::has_index(s)) {
            throw exceptions::invalid_request_exception(
                format("Cannot disable CDC when {} is enabled on the table.\n"
                "Please drop the {} index first, then disable CDC.",
                T::SEARCH_TYPE_NAME, T::INDEX_TYPE_NAME));
        }
    });
}


// Convert a serialized targets string (as produced by serialize_targets())
// back into the CQL column list used inside CREATE INDEX ... ON table(<here>).
//
// JSON examples:
//   {"tc":"v","fc":["f1","f2"]}               -> "v, f1, f2"
//   {"tc":"v","pk":["p1","p2"]}               -> "(p1, p2), v"
//   {"tc":"v","pk":["p1","p2"],"fc":["f1"]}   -> "(p1, p2), v, f1"
sstring external_index::targets_to_cql(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return cql3::util::maybe_quote(index_target::column_name_from_target_string(targets));
    }

    sstring result;

    const rjson::value* pk = rjson::find(*json_value, PK_TARGET_KEY);
    if (pk && pk->IsArray() && !pk->Empty()) {
        result += "(";
        auto pk_cols = std::views::all(pk->GetArray()) | std::views::transform([&](const rjson::value& col) {
            return cql3::util::maybe_quote(sstring(rjson::to_string_view(col)));
        }) | std::ranges::to<std::vector<sstring>>();
        result += boost::algorithm::join(pk_cols, ", ");
        result += "), ";
    }

    const rjson::value* tc = rjson::find(*json_value, TC_TARGET_KEY);
    if (tc && tc->IsString()) {
        result += cql3::util::maybe_quote(sstring(rjson::to_string_view(*tc)));
    }

    const rjson::value* fc = rjson::find(*json_value, FC_TARGET_KEY);
    if (fc && fc->IsArray()) {
        for (rapidjson::SizeType i = 0; i < fc->Size(); ++i) {
            result += ", ";
            result += cql3::util::maybe_quote(sstring(rjson::to_string_view((*fc)[i])));
        }
    }

    return result;
}

// Serialize external index targets into a format using:
//   "tc" for the target column,
//   "pk" for partition key columns (local index),
//   "fc" for filtering columns.
// For a simple single-column index, returns just the column name.
// Examples:
//   (v)                          -> "v"
//   (v, f1, f2)                  -> {"tc":"v","fc":["f1","f2"]}
//   ((p1, p2), v)                -> {"tc":"v","pk":["p1","p2"]}
//   ((p1, p2), v, f1, f2)        -> {"tc":"v","pk":["p1","p2"],"fc":["f1","f2"]}
sstring external_index::serialize_targets(const std::vector<::shared_ptr<index_target>>& targets) {
    if (targets.size() == 0) {
        throw exceptions::invalid_request_exception("External index must have at least one target column");
    }

    if (targets.size() == 1) {
        auto tc = targets[0]->value;
        if (!std::holds_alternative<index_target::single_column>(tc)) {
            throw exceptions::invalid_request_exception("Missing target column for local external index");
        }
        return index_target::escape_target_column(*std::get<index_target::single_column>(tc));
    }

    const bool has_pk = std::holds_alternative<index_target::multiple_columns>(targets.front()->value);
    const size_t tc_idx = has_pk ? 1 : 0;
    const size_t fc_count = targets.size() - tc_idx - 1;

    if (!std::holds_alternative<index_target::single_column>(targets[tc_idx]->value)) {
        throw exceptions::invalid_request_exception("External index target column must be a single column");
    }

    rjson::value json_map = rjson::empty_object();
    rjson::add_with_string_name(json_map, TC_TARGET_KEY, rjson::from_string(std::get<index_target::single_column>(targets[tc_idx]->value)->text()));

    if (has_pk) {
        rjson::value pk_json = rjson::empty_array();
        for (const auto& col : std::get<index_target::multiple_columns>(targets.front()->value)) {
            rjson::push_back(pk_json, rjson::from_string(col->text()));
        }
        rjson::add_with_string_name(json_map, PK_TARGET_KEY, std::move(pk_json));
    }

    if (fc_count > 0) {
        rjson::value fc_json = rjson::empty_array();
        for (size_t i = tc_idx + 1; i < targets.size(); ++i) {
            if (!std::holds_alternative<index_target::single_column>(targets[i]->value)) {
                throw exceptions::invalid_request_exception("External index filtering column must be a single column");
            }
            rjson::push_back(fc_json, rjson::from_string(std::get<index_target::single_column>(targets[i]->value)->text()));
        }
        rjson::add_with_string_name(json_map, FC_TARGET_KEY, std::move(fc_json));
    }

    return rjson::print(json_map);
}

sstring external_index::get_target_column(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return index_target::column_name_from_target_string(targets);
    }

    rjson::value* tc = rjson::find(*json_value, TC_TARGET_KEY);
    if (tc && tc->IsString()) {
        return sstring(rjson::to_string_view(*tc));
    }
    return index_target::column_name_from_target_string(targets);
}

std::vector<sstring> external_index::get_all_target_columns(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return {index_target::column_name_from_target_string(targets)};
    }

    std::vector<sstring> columns;
    const rjson::value* tc = rjson::find(*json_value, TC_TARGET_KEY);
    if (tc && tc->IsString()) {
        columns.emplace_back(rjson::to_string_view(*tc));
    }
    for (auto key : {PK_TARGET_KEY, FC_TARGET_KEY}) {
        const rjson::value* array = rjson::find(*json_value, key);
        if (array && array->IsArray()) {
            for (const rjson::value& column : array->GetArray()) {
                columns.emplace_back(rjson::to_string_view(column));
            }
        }
    }
    return columns;
}

bool external_index::is_local(const sstring& target_string) {
    std::optional<rjson::value> json_value = rjson::try_parse(target_string);
    if (!json_value || !json_value->IsObject()) {
        return false;
    }
    rjson::value* pk = rjson::find(*json_value, PK_TARGET_KEY);
    if (!pk || !pk->IsArray() || pk->Empty()) {
        return false;
    }
    rjson::value* tc = rjson::find(*json_value, TC_TARGET_KEY);
    return tc && tc->IsString();
}

} // namespace secondary_index
