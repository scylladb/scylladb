/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/list_errors_statement.hh"
#include "cql3/error_history.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/column_identifier.hh"
#include "cql3/result_set.hh"
#include "transport/messages/result_message.hh"
#include "types/types.hh"

namespace cql3 {
namespace statements {

list_errors_statement::list_errors_statement()
    : cql_statement_no_metadata(&timeout_config::other_timeout)
{}

future<::shared_ptr<cql_transport::messages::result_message>>
list_errors_statement::execute(query_processor& qp, service::query_state& state,
                               const query_options& options,
                               std::optional<service::group0_guard> guard) const {
    auto& client_state = state.get_client_state();
    auto* error_hist = client_state.get_error_history();

    // Helper to create column specifications
    auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return make_lw_shared<column_specification>(
            "system",
            "errors",
            ::make_shared<column_identifier>(name, true),
            ty);
    };

    // Define result metadata
    auto metadata = ::make_shared<cql3::metadata>(
        std::vector<lw_shared_ptr<column_specification>>{
            make_column_spec("index", int32_type),
            make_column_spec("timestamp", timestamp_type),
            make_column_spec("error", utf8_type),
            make_column_spec("query", utf8_type)
        });

    auto results = std::make_unique<result_set>(std::move(metadata));

    // Populate with error history if available
    if (error_hist && !error_hist->empty()) {
        auto errors = error_hist->get_all_errors();
        for (const auto& err : errors) {
            // Index column
            results->add_column_value(int32_type->decompose(static_cast<int32_t>(err.index)));
            
            // Timestamp column (milliseconds since epoch)
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                err.timestamp.time_since_epoch()).count();
            results->add_column_value(timestamp_type->decompose(millis));
            
            // Error message column
            results->add_column_value(utf8_type->decompose(err.error_message));
            
            // Query column
            results->add_column_value(utf8_type->decompose(err.query));
        }
    }

    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(
        ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(results))));
}

namespace raw {

std::unique_ptr<prepared_statement> list_errors_statement::prepare(
        data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(
        ::make_shared<cql3::statements::list_errors_statement>());
}

} // namespace raw

} // namespace statements
} // namespace cql3
