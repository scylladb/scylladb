/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/vector_index.hh"
#include "index/index_option_utils.hh"
#include "index/secondary_index.hh"
#include "index/secondary_index_manager.hh"
#include "index/target_parser.hh"
#include "types/concrete_types.hh"
#include "types/types.hh"
#include <ranges>
#include <seastar/core/sstring.hh>
#include <boost/algorithm/string.hpp>

namespace secondary_index {

static const std::vector<sstring> similarity_function_values = {
    "cosine", "euclidean", "dot_product"
};

static const std::vector<sstring> quantization_values = {
    "f32", "f16", "bf16", "i8", "b1"
};

const static std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> vector_index_options = {
        // `similarity_function` defines method of calculating similarity between vectors
        // Used internally by vector store during both indexing and querying
        // CQL implements corresponding functions in cql3/functions/similarity_functions.hh
        {"similarity_function", std::bind_front(util::validate_enumerated_option, similarity_function_values)},
        // 'maximum_node_connections', 'construction_beam_width', 'search_beam_width' define HNSW index parameters
        // Used internally by vector store.
        {"maximum_node_connections", std::bind_front(util::validate_positive_option, 512)},
        {"construction_beam_width", std::bind_front(util::validate_positive_option, 4096)},
        {"search_beam_width", std::bind_front(util::validate_positive_option, 4096)},
        // 'quantization' enables compression of vectors in vector store (not in base table!)
        // Used internally by vector store. Scylla only checks it to enable rescoring.
        {"quantization", std::bind_front(util::validate_enumerated_option, quantization_values)},
        // 'oversampling' defines factor by which number of candidates retrieved from vector store is multiplied.
        // It can improve accuracy of ANN queries, especially for quantized vectors when combined with rescoring.
        // Used by Scylla during query processing to increase query limit sent to vector store.
        {"oversampling", std::bind_front(util::validate_factor_option, 1.0f, 100.0f)},
        // 'rescoring' enables recalculating of similarity scores of candidates retrieved from vector store when quantization is used.
        {"rescoring", std::bind_front(util::validate_enumerated_option, util::boolean_values)},
        // 'source_model' is a Cassandra SAI option specifying the embedding model name.
        // Used by Cassandra libraries (e.g., CassIO) to tag indexes with the model that produced the vectors.
        // Accepted for compatibility but not used by ScyllaDB.
        {"source_model", [](std::string_view, const sstring&, const sstring&) { /* accepted for Cassandra compatibility */ }},
    };

static constexpr auto TC_TARGET_KEY = "tc";
static constexpr auto PK_TARGET_KEY = "pk";
static constexpr auto FC_TARGET_KEY = "fc";

// Convert a serialized targets string (as produced by serialize_targets())
// back into the CQL column list used inside CREATE INDEX ... ON table(<here>).
//
// JSON examples:
//   {"tc":"v","fc":["f1","f2"]}               -> "v, f1, f2"
//   {"tc":"v","pk":["p1","p2"]}               -> "(p1, p2), v"
//   {"tc":"v","pk":["p1","p2"],"fc":["f1"]}   -> "(p1, p2), v, f1"
static sstring targets_to_cql(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return cql3::util::maybe_quote(cql3::statements::index_target::column_name_from_target_string(targets));
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

// Serialize vector index targets into a format using:
//   "tc" for the target (vector) column,
//   "pk" for partition key columns (local index),
//   "fc" for filtering columns.
// For a simple single-column vector index, returns just the column name.
// Examples:
//   (v)                          -> "v"
//   (v, f1, f2)                  -> {"tc":"v","fc":["f1","f2"]}
//   ((p1, p2), v)                -> {"tc":"v","pk":["p1","p2"]}
//   ((p1, p2), v, f1, f2)        -> {"tc":"v","pk":["p1","p2"],"fc":["f1","f2"]}
sstring vector_index::serialize_targets(const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) {
    using cql3::statements::index_target;

    if (targets.size() == 0) {
        throw exceptions::invalid_request_exception("Vector index must have at least one target column");
    }

    if (targets.size() == 1) {
        auto tc = targets[0]->value;
        if (!std::holds_alternative<index_target::single_column>(tc)) {
            throw exceptions::invalid_request_exception("Missing vector column target for local vector index");
        }
        return index_target::escape_target_column(*std::get<index_target::single_column>(tc));
    }

    const bool has_pk = std::holds_alternative<index_target::multiple_columns>(targets.front()->value);
    const size_t tc_idx = has_pk ? 1 : 0;
    const size_t fc_count = targets.size() - tc_idx - 1;

    if (!std::holds_alternative<index_target::single_column>(targets[tc_idx]->value)) {
        throw exceptions::invalid_request_exception("Vector index target column must be a single column");
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
                throw exceptions::invalid_request_exception("Vector index filtering column must be a single column");
            }
            rjson::push_back(fc_json, rjson::from_string(std::get<index_target::single_column>(targets[i]->value)->text()));
        }
        rjson::add_with_string_name(json_map, FC_TARGET_KEY, std::move(fc_json));
    }

    return rjson::print(json_map);
}

sstring vector_index::get_target_column(const sstring& targets) {
    std::optional<rjson::value> json_value = rjson::try_parse(targets);
    if (!json_value || !json_value->IsObject()) {
        return cql3::statements::index_target::column_name_from_target_string(targets);
    }

    rjson::value* tc = rjson::find(*json_value, TC_TARGET_KEY);
    if (tc && tc->IsString()) {
        return sstring(rjson::to_string_view(*tc));
    }
    return cql3::statements::index_target::column_name_from_target_string(targets);
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

std::optional<cql3::description> vector_index::describe(const index_metadata& im, const schema& base_schema) const {
    return describe_with_target(im, base_schema,
            targets_to_cql(im.options().at(cql3::statements::index_target::target_option_name)));
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

void vector_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    for (auto option: properties.get_raw_options()) {
        auto it = vector_index_options.find(option.first);
        if (it == vector_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for vector index", option.first));
        }
        it->second(index_type_name(), option.first, option.second);
    }
}

void vector_index::check_key_column_count(const schema& schema) const {
    // The vector-store InvariantKey supports at most 255 key columns (VECTOR-553).
    static constexpr column_count_type max_key_columns = 255;
    auto key_column_count = schema.partition_key_size() + schema.clustering_key_size();
    if (key_column_count > max_key_columns) {
        throw exceptions::invalid_request_exception(
            format("Vector index requires at most {:d} primary key columns, but {:d} were found", max_key_columns, key_column_count));
    }
}

void vector_index::validate(const schema &schema, const cql3::statements::index_specific_prop_defs &properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>> &targets,
        const gms::feature_service& fs,
        const data_dictionary::database& db) const
{
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_key_column_count(schema);
    check_cdc_not_explicitly_disabled(schema);
    check_cdc_options(schema);
    check_index_options(properties);
}

bool vector_index::is_local(const sstring& target_string) {
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

bool vector_index::is_vector_index_on_column(const index_metadata& im, const sstring& target_name) {
    auto class_it = im.options().find(db::index::secondary_index::custom_class_option_name);
    auto target_it = im.options().find(cql3_parser::index_target::target_option_name);
    if (class_it != im.options().end() && target_it != im.options().end()) {
        auto custom_class = secondary_index_manager::get_custom_class_factory(class_it->second);
        return custom_class && dynamic_cast<vector_index*>((*custom_class)().get()) && get_target_column(target_it->second) == target_name;
    }
    return false;
}

std::unique_ptr<secondary_index::custom_index> vector_index_factory() {
    return std::make_unique<vector_index>();
}

}
