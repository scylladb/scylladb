/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "cql3/statements/index_target.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/vector_index.hh"
#include "concrete_types.hh"
#include <seastar/core/sstring.hh>


namespace secondary_index {

template <int MAX>
static void validate_unsigned_option(const sstring& value) {
    int num_value;
    size_t len;
    try {
        num_value = std::stoi(value, &len);
    } catch (...) {
        throw exceptions::invalid_request_exception(format("Numeric option {} is not a valid number", value));
    }
    if (len != value.size()) {
        throw exceptions::invalid_request_exception(format("Numeric option {} is not a valid number", value));
    }

    if (num_value < 0 || num_value > MAX) {
        throw exceptions::invalid_request_exception(format("Numeric option {} out of valid range [0 - {}]", value, MAX));
    }
}

static void validate_similarity_function(const sstring& value) {
    if (value != "COSINE" && value != "EUCLIDEAN" && value != "DOT_PRODUCT") {
        throw exceptions::invalid_request_exception(format("Unsupported similarity function: {}", value));
    }
}

const static std::unordered_map<sstring, std::function<void(const sstring&)>> supported_options = {
        {"similarity_function", validate_similarity_function},
        {"maximum_node_connections", validate_unsigned_option<512>},
        {"construction_beam_width", validate_unsigned_option<4096>},
        {"search_beam_width", validate_unsigned_option<4096>},
    };

void vector_index::validate(const schema &schema, cql3::statements::index_prop_defs &properties, const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs) {
    if (targets.size() != 1) {
        throw exceptions::invalid_request_exception("Vector index can only be created on a single column");
    }

    auto target = targets[0];
    auto c_def = schema.get_column_definition(to_bytes(target->column_name()));
    if (!c_def) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", target->column_name()));
    }
    auto type = c_def->type;
    if (!type->is_vector() || static_cast<const vector_type_impl*>(type.get())->get_elements_type()->get_kind() != abstract_type::kind::float_kind) {
        throw exceptions::invalid_request_exception(format("Vector indexes are only supported on columns of vectors of floats", target->column_name()));
    }

    for (auto option: properties.get_raw_options()) {
        auto it = supported_options.find(option.first);
        if (it == supported_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for vector index", option.first));
        }
        it->second(option.second);
    }
}

std::unique_ptr<secondary_index::custom_index> vector_index_factory() {
    return std::make_unique<vector_index>();
}

}