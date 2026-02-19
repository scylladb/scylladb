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
#include "index/target_parser.hh"
#include "types/concrete_types.hh"
#include "types/types.hh"
#include "utils/managed_string.hh"
#include <seastar/core/sstring.hh>
#include <boost/algorithm/string.hpp>

namespace secondary_index {

static void validate_positive_option(int max, const sstring& value_name, const sstring& value) {
    int num_value;
    size_t len;
    try {
        num_value = std::stoi(value, &len);
    } catch (...) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is not an integer", value_name, value));
    }
    if (len != value.size()) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is not an integer", value_name, value));
    }

    if (num_value <= 0 || num_value > max) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is out of valid range [1 - {}]", value_name, value, max));
    }
}

static void validate_factor_option(float min, float max, const sstring& value_name, const sstring& value) {
    float num_value;
    size_t len;
    try {
        num_value = std::stof(value, &len);
    } catch (...) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is not a float", value_name, value));
    }
    if (len != value.size()) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is not a float", value_name, value));
    }

    if (!(num_value >= min && num_value <= max)) {
        throw exceptions::invalid_request_exception(format("Invalid value in option '{}' for vector index: '{}' is out of valid range [{} - {}]", value_name, value, min, max));
    }
}

static void validate_enumerated_option(const std::vector<sstring>& supported_values, const sstring& value_name, const sstring& value) {    
    bool is_valid = std::any_of(supported_values.begin(), supported_values.end(),
        [&](const std::string& func) { return boost::iequals(value, func); });
    
    if (!is_valid) {
        throw exceptions::invalid_request_exception(
            seastar::format("Invalid value in option '{}' for vector index: '{}'. Supported are case-insensitive: {}", 
                   value_name,
                   value,
                   fmt::join(supported_values, ", ")));
    }
}

static const std::vector<sstring> similarity_function_values = {
    "cosine", "euclidean", "dot_product"
};

static const std::vector<sstring> quantization_values = {
    "f32", "f16", "bf16", "i8", "b1"
};

static const std::vector<sstring> boolean_values = {
    "false", "true"
};

const static std::unordered_map<sstring, std::function<void(const sstring&, const sstring&)>> vector_index_options = {
        // `similarity_function` defines method of calculating similarity between vectors
        // Used internally by vector store during both indexing and querying
        // CQL implements corresponding functions in cql3/functions/similarity_functions.hh
        {"similarity_function", std::bind_front(validate_enumerated_option, similarity_function_values)},
        // 'maximum_node_connections', 'construction_beam_width', 'search_beam_width' define HNSW index parameters
        // Used internally by vector store.
        {"maximum_node_connections", std::bind_front(validate_positive_option, 512)},
        {"construction_beam_width", std::bind_front(validate_positive_option, 4096)},
        {"search_beam_width", std::bind_front(validate_positive_option, 4096)},
        // 'quantization' enables compression of vectors in vector store (not in base table!)
        // Used internally by vector store. Scylla only checks it to enable rescoring.
        {"quantization", std::bind_front(validate_enumerated_option, quantization_values)},
        // 'oversampling' defines factor by which number of candidates retrieved from vector store is multiplied.
        // It can improve accuracy of ANN queries, especially for quantized vectors when combined with rescoring.
        // Used by Scylla during query processing to increase query limit sent to vector store.
        {"oversampling", std::bind_front(validate_factor_option, 1.0f, 100.0f)},
        // 'rescoring' enables recalculating of similarity scores of candidates retrieved from vector store when quantization is used.
        {"rescoring", std::bind_front(validate_enumerated_option, boolean_values)},
    };

sstring get_vector_index_target_column(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return target_parser::get_target_column_name_from_string(targets);
    }

    rjson::value* pk = rjson::find(*json_value, "pk");
    if (pk && pk->IsArray() && !pk->Empty()) {
        return sstring(rjson::to_string_view(pk->GetArray()[0]));
    }
    return target_parser::get_target_column_name_from_string(targets);
}

bool vector_index::is_rescoring_enabled(const index_options_map& properties) {
    auto q = properties.find("quantization");
    auto r = properties.find("rescoring");
    return q != properties.end() && !boost::iequals(q->second, "f32")
        && r != properties.end() && boost::iequals(r->second, "true");
}

float vector_index::get_oversampling(const index_options_map& properties) {
    auto it = properties.find("oversampling");
    if (it != properties.end()) {
        return std::stof(it->second);
    }
    return 1.0f;
}

sstring vector_index::get_cql_similarity_function_name(const index_options_map& properties) {
    auto it = properties.find("similarity_function");
    if (it != properties.end()) {
        return "similarity_" + boost::to_lower_copy(it->second);
    }
    return "similarity_cosine";
}

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

void vector_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const {

    struct validate_visitor {
        const class schema& schema;
        bool& is_vector;

        /// Vector indexes support filtering on native types that can be used as primary key columns.
        /// There is no counter (it cannot be used with vector columns)
        /// and no duration (it cannot be used as a primary key or in secondary indexes).
        static bool is_supported_filtering_column(abstract_type const & kind_type) {
            switch (kind_type.get_kind()) {
                case abstract_type::kind::ascii:
                case abstract_type::kind::boolean:
                case abstract_type::kind::byte:
                case abstract_type::kind::bytes:
                case abstract_type::kind::date:
                case abstract_type::kind::decimal:
                case abstract_type::kind::double_kind:
                case abstract_type::kind::float_kind:
                case abstract_type::kind::inet:
                case abstract_type::kind::int32:
                case abstract_type::kind::long_kind:
                case abstract_type::kind::short_kind:
                case abstract_type::kind::simple_date:
                case abstract_type::kind::time:
                case abstract_type::kind::timestamp:
                case abstract_type::kind::timeuuid:
                case abstract_type::kind::utf8:
                case abstract_type::kind::uuid:
                case abstract_type::kind::varint:
                    return true;
                default:
                    break;
            }
            return false;
        }

        void validate(cql3::column_identifier const& column, bool is_vector) const {
            auto const& c_name = column.to_string();
            auto const* c_def = schema.get_column_definition(column.name());
            if (c_def == nullptr) {
                throw exceptions::invalid_request_exception(format("Column {} not found in schema", c_name));
            }

            auto type = c_def->type;

            if (is_vector) {
                auto const* vector_type = dynamic_cast<const vector_type_impl*>(type.get());
                if (vector_type == nullptr) {
                    throw exceptions::invalid_request_exception("Vector indexes are only supported on columns of vectors of floats");
                }

                auto elements_type = vector_type->get_elements_type();
                if (elements_type->get_kind() != abstract_type::kind::float_kind) {
                    throw exceptions::invalid_request_exception("Vector indexes are only supported on columns of vectors of floats");
                }
                return;
            }

            if (!is_supported_filtering_column(*type)) {
                throw exceptions::invalid_request_exception(format("Unsupported vector index filtering column {} type", c_name));
            }
        }

        void operator()(const std::vector<::shared_ptr<cql3::column_identifier>>& columns) const {
            for (const auto& column : columns) {
                // CQL restricts the secondary local index to have multiple columns with partition key only.
                // Vectors shouldn't be partition key columns and they aren't supported as a filtering column,
                // so we can assume here that these are non-vectors filtering columns.
                validate(*column, false);
            }
        }

        void operator()(const ::shared_ptr<cql3::column_identifier>& column) {
            validate(*column, is_vector);
            // The first column is the vector column, the rest mustn't be vectors.
            is_vector = false;
        }
    };

    bool is_vector = true;
    for (const auto& target : targets) {
        std::visit(validate_visitor{.schema = schema, .is_vector = is_vector}, target->value);
    }
}

void vector_index::check_cdc_not_explicitly_disabled(const schema& schema) const {
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

void vector_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    for (auto option: properties.get_raw_options()) {
        auto it = vector_index_options.find(option.first);
        if (it == vector_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for vector index", option.first));
        }
        it->second(option.first, option.second);
    }
}

void vector_index::check_uses_tablets(const schema& schema, const data_dictionary::database& db) const {
    const auto& keyspace = db.find_keyspace(schema.ks_name());
    if (!keyspace.uses_tablets()) {
        throw exceptions::invalid_request_exception(
            "Vector index requires the base table's keyspace to use tablets.\n"
            "Please alter the keyspace to use tablets and try again.");
    }
}

void vector_index::validate(const schema &schema, const cql3::statements::index_specific_prop_defs &properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>> &targets,
        const gms::feature_service& fs,
        const data_dictionary::database& db) const
{
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_not_explicitly_disabled(schema);
    check_cdc_options(schema);
    check_index_options(properties);
}

bool vector_index::has_vector_index(const schema& s) {
    auto i = s.indices();
    return std::any_of(i.begin(), i.end(), [](const auto& index) {
        auto it = index.options().find(db::index::secondary_index::custom_class_option_name);
        if (it != index.options().end()) {
            auto custom_class = secondary_index_manager::get_custom_class_factory(it->second);
            return (custom_class && dynamic_cast<vector_index*>((*custom_class)().get()));
        }
        return false;
    });
}

bool vector_index::has_vector_index_on_column(const schema& s, const sstring& target_name) {
    for (const auto& index : s.indices()) {
        if (is_vector_index_on_column(index, target_name)) {
            return true;
        }
    }
    return false;
}

bool vector_index::is_vector_index_on_column(const index_metadata& im, const sstring& target_name) {
    auto class_it = im.options().find(db::index::secondary_index::custom_class_option_name);
    auto target_it = im.options().find(cql3_parser::index_target::target_option_name);
    if (class_it != im.options().end() && target_it != im.options().end()) {
        auto custom_class = secondary_index_manager::get_custom_class_factory(class_it->second);
        return custom_class && dynamic_cast<vector_index*>((*custom_class)().get()) && get_vector_index_target_column(target_it->second) == target_name;
    }
    return false;
}

/// Returns the schema version of the base table at which the index was created.
/// This is used to determine if the index needs to be rebuilt after a schema change.
/// The CREATE INDEX and DROP INDEX statements does change the schema version.
table_schema_version vector_index::index_version(const schema& schema) {
    return schema.version();
}

std::unique_ptr<secondary_index::custom_index> vector_index_factory() {
    return std::make_unique<vector_index>();
}

}
