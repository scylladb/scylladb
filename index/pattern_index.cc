/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/pattern_index.hh"
#include "index/index_option_utils.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/sstring.hh>

namespace secondary_index {

namespace {

const std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> pattern_index_options = {
        // 'case_sensitive' set to false lowercases both the indexed values and the patterns.
        {"case_sensitive", std::bind_front(util::validate_enumerated_option, util::boolean_values)},
};

} // anonymous namespace

std::optional<cql3::description> pattern_index::describe(const index_metadata& im, const schema& base_schema) const {
    auto target = im.options().at(cql3::statements::index_target::target_option_name);
    auto target_column = cql3::statements::index_target::column_name_from_target_string(target);
    return describe_with_target(im, base_schema, cql3::util::maybe_quote(target_column));
}

void pattern_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const {
    using cql3::statements::index_target;

    if (targets.size() != 1) {
        throw exceptions::invalid_request_exception("Pattern index must have exactly one target");
    }

    auto& target = targets[0];
    if (!std::holds_alternative<index_target::single_column>(target->value)) {
        throw exceptions::invalid_request_exception("Pattern index target must be a single column");
    }

    auto& column = std::get<index_target::single_column>(target->value);
    auto c_name = column->to_string();
    auto const* c_def = schema.get_column_definition(column->name());
    if (c_def == nullptr) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", c_name));
    }

    // Only a regular column can be served:
    // - partition and clustering key columns: ScyllaDB keeps every WHERE condition on a key
    //   column, `LIKE` included, as a key restriction, and a `LIKE` served by a pattern index rejects key
    //   restrictions, so a `LIKE` on a key column could never reach the index,
    // - static columns: the Vector Store fetches each row by its full primary key, which
    //   ScyllaDB refuses when the only selected column is static.
    if (!c_def->is_regular()) {
        throw exceptions::invalid_request_exception(
                format("Pattern index is only supported on regular columns, but column {} is a {} column", c_name, to_sstring(c_def->kind)));
    }

    auto kind = c_def->type->get_kind();
    if (kind != abstract_type::kind::utf8 && kind != abstract_type::kind::ascii) {
        throw exceptions::invalid_request_exception(
                format("Pattern index is only supported on text, varchar, or ascii columns, but column {} has an incompatible type", c_name));
    }
}

void pattern_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    for (const auto& option : properties.get_raw_options()) {
        auto it = pattern_index_options.find(option.first);
        if (it == pattern_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for pattern index", option.first));
        }
        it->second(index_type_name(), option.first, option.second);
    }
}

void pattern_index::validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service&, const data_dictionary::database& db) const {
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_options(schema);
    check_index_options(properties);
}

bool pattern_index::has_index_on_column(const schema& s, const sstring& column) {
    return std::ranges::any_of(s.indices(), [&] (const index_metadata& im) {
        auto target_it = im.options().find(cql3::statements::index_target::target_option_name);
        return target_it != im.options().end() && secondary_index_manager::is_custom_index<pattern_index>(im)
                && cql3::statements::index_target::column_name_from_target_string(target_it->second) == column;
    });
}

std::unique_ptr<secondary_index::custom_index> pattern_index_factory() {
    return std::make_unique<pattern_index>();
}

} // namespace secondary_index
