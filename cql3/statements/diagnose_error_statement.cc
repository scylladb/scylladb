/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/diagnose_error_statement.hh"
#include "cql3/error_history.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/column_identifier.hh"
#include "cql3/result_set.hh"
#include "transport/messages/result_message.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"

#include <fmt/format.h>

// External C functions from Go shared library (when enabled)
#ifdef SCYLLA_ENABLE_ERROR_DIAGNOSIS
extern "C" {
    typedef struct {
        char* response;
        char* error;
        int success;
    } AnthropicResult;
    
    extern int anthropic_init(char* key);
    extern AnthropicResult anthropic_diagnose_error(char* error_msg, char* query, char* model);
    extern void anthropic_free_result(AnthropicResult result);
    extern int anthropic_is_initialized();
}
#endif

namespace cql3 {
namespace statements {

diagnose_error_statement::diagnose_error_statement(std::optional<int32_t> error_index)
    : cql_statement_no_metadata(&timeout_config::other_timeout)
    , _error_index(error_index)
{}

future<::shared_ptr<cql_transport::messages::result_message>>
diagnose_error_statement::execute(query_processor& qp, service::query_state& state,
                                  const query_options& options,
                                  std::optional<service::group0_guard> guard) const {
    auto& client_state = state.get_client_state();
    auto* error_hist = client_state.get_error_history();

    // Validate we have error history
    if (!error_hist || error_hist->empty()) {
        throw exceptions::invalid_request_exception(
            "No errors recorded in current session. Use LIST ERRORS to see recorded errors.");
    }

    // Determine which error to diagnose
    size_t target_index = _error_index.value_or(1);  // Default to most recent
    
    if (target_index < 1 || target_index > error_hist->size()) {
        throw exceptions::invalid_request_exception(
            fmt::format("Error index {} out of range. Valid range: 1-{}. Use LIST ERRORS to see available errors.",
                       target_index, error_hist->size()));
    }

    // Get the error record
    auto error_opt = error_hist->get_error(target_index);
    if (!error_opt) {
        throw exceptions::invalid_request_exception(
            fmt::format("Error at index {} not found", target_index));
    }
    const auto& error = *error_opt;

    // Generate diagnosis
    sstring diagnosis;
    
#ifdef SCYLLA_ENABLE_ERROR_DIAGNOSIS
    // Get API key from config
    auto api_key = qp.db().get_config().anthropic_api_key();
    
    if (api_key.empty()) {
        diagnosis = "ERROR: Anthropic API key not configured.\n\n"
                   "To enable AI-powered error diagnosis:\n"
                   "1. Add 'anthropic_api_key: YOUR_KEY' to scylla.yaml\n"
                   "2. Restart ScyllaDB\n\n"
                   "Get an API key at: https://console.anthropic.com/";
    } else {
        // Initialize client if needed
        if (!anthropic_is_initialized()) {
            std::vector<char> key_buf(api_key.begin(), api_key.end());
            key_buf.push_back('\0');
            if (!anthropic_init(key_buf.data())) {
                diagnosis = "ERROR: Failed to initialize Anthropic client. Check API key validity.";
            }
        }
        
        if (diagnosis.empty()) {  // Initialization succeeded
            auto model = qp.db().get_config().anthropic_model();
            
            // Prepare null-terminated C strings
            std::vector<char> err_buf(error.error_message.begin(), error.error_message.end());
            std::vector<char> qry_buf(error.query.begin(), error.query.end());
            std::vector<char> mdl_buf(model.begin(), model.end());
            err_buf.push_back('\0');
            qry_buf.push_back('\0');
            mdl_buf.push_back('\0');

            // Call the AI diagnosis
            auto result = anthropic_diagnose_error(err_buf.data(), qry_buf.data(), mdl_buf.data());
            
            if (result.success) {
                diagnosis = sstring(result.response);
            } else {
                diagnosis = fmt::format("Diagnosis failed: {}", result.error ? result.error : "Unknown error");
            }
            
            anthropic_free_result(result);
        }
    }
#else
    diagnosis = "ERROR: AI error diagnosis is not enabled in this build.\n\n"
               "To enable:\n"
               "1. Install Go 1.21+ and build the anthropic_bridge library\n"
               "2. Rebuild ScyllaDB with -DSCYLLA_ENABLE_ERROR_DIAGNOSIS=1\n"
               "3. Configure anthropic_api_key in scylla.yaml";
#endif

    // Build result set
    auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return make_lw_shared<column_specification>(
            "system",
            "diagnosis",
            ::make_shared<column_identifier>(name, true),
            ty);
    };

    auto metadata = ::make_shared<cql3::metadata>(
        std::vector<lw_shared_ptr<column_specification>>{
            make_column_spec("index", int32_type),
            make_column_spec("error", utf8_type),
            make_column_spec("query", utf8_type),
            make_column_spec("diagnosis", utf8_type)
        });

    auto results = std::make_unique<result_set>(std::move(metadata));
    
    results->add_column_value(int32_type->decompose(static_cast<int32_t>(error.index)));
    results->add_column_value(utf8_type->decompose(error.error_message));
    results->add_column_value(utf8_type->decompose(error.query));
    results->add_column_value(utf8_type->decompose(diagnosis));

    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(
        ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(results))));
}

namespace raw {

diagnose_error_statement::diagnose_error_statement(std::optional<int32_t> error_index)
    : _error_index(error_index)
{}

std::unique_ptr<prepared_statement> diagnose_error_statement::prepare(
        data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(
        ::make_shared<cql3::statements::diagnose_error_statement>(_error_index));
}

} // namespace raw

} // namespace statements
} // namespace cql3
