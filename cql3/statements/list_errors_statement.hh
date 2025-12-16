/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/prepared_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

/**
 * LIST ERRORS statement - displays recent errors from the current session.
 * 
 * Syntax:
 *   LIST ERRORS;
 * 
 * Returns a result set with columns:
 *   - index (int): Error number (1 = most recent)
 *   - timestamp (timestamp): When the error occurred
 *   - error (text): The error message
 *   - query (text): The query that caused the error
 */
class list_errors_statement : public cql_statement_no_metadata {
public:
    list_errors_statement();

    uint32_t get_bound_terms() const override { return 0; }
    
    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override { 
        return false; 
    }

    future<> check_access(query_processor& qp, const service::client_state& state) const override {
        // No special permissions needed - accessing own session data
        return make_ready_future<>();
    }

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options,
            std::optional<service::group0_guard> guard) const override;

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override {
        return std::make_unique<prepared_statement>(::make_shared<list_errors_statement>(*this));
    }
};

namespace raw {

/**
 * Raw (unprepared) form of LIST ERRORS statement.
 */
class list_errors_statement : public parsed_statement {
public:
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

} // namespace raw

} // namespace statements
} // namespace cql3
