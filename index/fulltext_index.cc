/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/index_prop_defs.hh"
#include "cql3/statements/index_target.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/fulltext_index.hh"
#include "index/index_option_utils.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/sstring.hh>
#include <boost/algorithm/string.hpp>

namespace secondary_index {

// Supported text analyzers for fulltext indexing.
// This list corresponds to analyzers expected to be provided
// by the backend search engine (Tantivy).
static const std::vector<sstring> analyzer_values = {
        "standard", "english", "german", "french", "spanish", "italian", "portuguese", "russian", "simple", "whitespace"};

const static std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> fulltext_index_options = {
        // 'analyzer' specifies the built-in text analyzer to use for tokenization.
        {"analyzer", std::bind_front(util::validate_enumerated_option, analyzer_values)},
        // 'positions' controls whether token positions are stored in the index.
        // Required for phrase queries. Set to false to save space.
        {"positions", std::bind_front(util::validate_enumerated_option, util::boolean_values)},
};

std::optional<cql3::description> fulltext_index::describe(const index_metadata& im, const schema& base_schema) const {
    return describe_with_target(im, base_schema, targets_to_cql(im.options().at(cql3::statements::index_target::target_option_name)));
}

void fulltext_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const {
    using cql3::statements::index_target;

    const bool is_local = !targets.empty() && std::holds_alternative<index_target::multiple_columns>(targets.front()->value);
    const size_t target_idx = is_local ? 1 : 0;
    if (targets.size() != target_idx + 1) {
        throw exceptions::invalid_request_exception("Fulltext index must have exactly one target column");
    }

    auto& target = targets[target_idx];
    if (!std::holds_alternative<index_target::single_column>(target->value)) {
        throw exceptions::invalid_request_exception("Fulltext index target must be a single column");
    }

    auto& column = std::get<index_target::single_column>(target->value);
    auto c_name = column->to_string();
    auto const* c_def = schema.get_column_definition(column->name());
    if (c_def == nullptr) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", c_name));
    }

    check_regular_column(*c_def);

    auto kind = c_def->type->get_kind();
    if (kind != abstract_type::kind::utf8 && kind != abstract_type::kind::ascii) {
        throw exceptions::invalid_request_exception(
                format("Fulltext index is only supported on text, varchar, or ascii columns, but column {} has an incompatible type", c_name));
    }

    if (is_local) {
        for (const auto& key : std::get<index_target::multiple_columns>(targets.front()->value)) {
            if (key->name() == column->name()) {
                throw exceptions::invalid_request_exception(
                        format("Local fulltext index cannot be partitioned by its indexed column {}", c_name));
            }
            if (!is_supported_filtering_column(*schema.get_column_definition(key->name())->type)) {
                throw exceptions::invalid_request_exception(
                        format("Unsupported local fulltext index partition column {} type", key->to_string()));
            }
        }
    }
}

void fulltext_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    for (auto option : properties.get_raw_options()) {
        auto it = fulltext_index_options.find(option.first);
        if (it == fulltext_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for fulltext index", option.first));
        }
        it->second(index_type_name(), option.first, option.second);
    }
}

void fulltext_index::validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service&, const data_dictionary::database& db) const {
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_options(schema);
    check_index_options(properties);
}

std::unique_ptr<secondary_index::custom_index> fulltext_index_factory() {
    return std::make_unique<fulltext_index>();
}

} // namespace secondary_index
