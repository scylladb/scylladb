/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/vector_index.hh"
#include "index/secondary_index.hh"
#include "index/secondary_index_manager.hh"
#include "types/concrete_types.hh"
#include "utils/managed_string.hh"
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
    sstring similarity_function = value;
    std::transform(similarity_function.begin(), similarity_function.end(), similarity_function.begin(), ::tolower);
    if (similarity_function != "cosine" && similarity_function != "euclidean" && similarity_function != "dot_product") {
        throw exceptions::invalid_request_exception(format("Unsupported similarity function: {}", value));
    }
}

const static std::unordered_map<sstring, std::function<void(const sstring&)>> supported_options = {
        {"similarity_function", validate_similarity_function},
        {"maximum_node_connections", validate_unsigned_option<512>},
        {"construction_beam_width", validate_unsigned_option<4096>},
        {"search_beam_width", validate_unsigned_option<4096>},
    };

bool vector_index::view_should_exist() const {
    return false;
}

std::optional<cql3::description> vector_index::describe(const index_metadata& im, const schema& base_schema) const {
    fragmented_ostringstream os;
    os << "CREATE CUSTOM INDEX " << cql3::util::maybe_quote(im.name()) << " ON "
       << cql3::util::maybe_quote(base_schema.ks_name()) << "." << cql3::util::maybe_quote(base_schema.cf_name())
       << "(" << cql3::util::maybe_quote(im.options().at(cql3::statements::index_target::target_option_name)) << ")"
       << " USING 'vector_index'";

    return cql3::description{
        .keyspace = base_schema.ks_name(),
        .type = "index",
        .name = im.name(),
        .create_statement = std::move(os).to_managed_string(),
    };
}

void vector_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) {
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
}

void vector_index::check_cdc_not_explicitly_disabled(const schema& schema) {
    auto cdc_options = schema.cdc_options();
    if (cdc_options.is_enabled_set() && !cdc_options.enabled()) {
        // If CDC is explicitly disabled by the user, we cannot create the vector index.
        throw exceptions::invalid_request_exception(format(
            "Cannot create the vector index when CDC is explicitly disabled.\n"
                "Please enable CDC with the required parameters first.\n"
                "CDC's TTL must be at least {} seconds (24 hours), "
                "and the CDC's delta mode must be set to 'full' or postimage must be enabled "
                "to enable Vector Search.\n"
                "Check documentation on how to setup CDC's parameters - "
                "https://docs.scylladb.com/manual/branch-2025.2/features/cdc/cdc-intro.html#cdc-parameters",
                VS_TTL_SECONDS));
    }
}

void vector_index::check_cdc_options(const schema& schema) {
    auto cdc_options = schema.cdc_options();
    if (cdc_options.enabled()) {
        auto ttl = cdc_options.ttl();
        auto delta_mode = cdc_options.get_delta_mode();
        auto postimage = cdc_options.postimage();
        if ((ttl && ttl < VS_TTL_SECONDS) ||
            (delta_mode != cdc::delta_mode::full && !postimage)) {
            throw exceptions::invalid_request_exception(
                secondary_index::vector_index::has_vector_index(schema) ?
                format("Vector Search is enabled on this table.\n"
                "The CDC log must meet the minimal requirements of Vector Search.\n"
                "This means that the CDC's TTL must be at least {} seconds (24 hours), "
                "and the CDC's delta mode must be set to 'full' or postimage must be enabled.\n",
                VS_TTL_SECONDS) :
                format("To enable Vector Search on this table, "
                "the CDC log must meet the minimal requirements of Vector Search.\n"
                "CDC's TTL must be at least {} seconds (24 hours), "
                "and the CDC's delta mode must be set to 'full' or postimage must be enabled "
                "to enable Vector Search.\n"
                "Check documentation on how to setup CDC's parameters - "
                "https://docs.scylladb.com/manual/branch-2025.2/features/cdc/cdc-intro.html#cdc-parameters",
                VS_TTL_SECONDS));
        }
    }
}

void vector_index::check_index_options(cql3::statements::index_prop_defs& properties) {
    for (auto option: properties.get_raw_options()) {
        auto it = supported_options.find(option.first);
        if (it == supported_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for vector index", option.first));
        }
        it->second(option.second);
    }
}

void vector_index::validate(const schema &schema, cql3::statements::index_prop_defs &properties, const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs) {
    check_target(schema, targets);
    check_cdc_not_explicitly_disabled(schema);
    check_cdc_options(schema);
    check_index_options(properties);
}

bool vector_index::has_vector_index(const schema& s) {
    auto i = s.indices();
    return std::any_of(i.begin(), i.end(), [](const auto& index) {
        auto it = index.options().find(db::index::secondary_index::custom_index_option_name);
        if (it != index.options().end()) {
            auto custom_class = secondary_index_manager::get_custom_class_factory(it->second);
            return (custom_class && dynamic_cast<vector_index*>((*custom_class)().get()));
        }
        return false;
    });
}

std::unique_ptr<secondary_index::custom_index> vector_index_factory() {
    return std::make_unique<vector_index>();
}

}
