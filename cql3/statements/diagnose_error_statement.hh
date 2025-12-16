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
#include <optional>

namespace cql3 {

class query_processor;

namespace statements {

/**
 * DIAGNOSE ERROR statement - uses AI to diagnose CQL errors.
 * 
 * Syntax:
 *   DIAGNOSE ERROR;       -- Diagnose most recent error
 *   DIAGNOSE ERROR N;     -- Diagnose error at index N
 * 
 * Requires:
 *   - anthropic_api_key configured in scylla.yaml
 *   - SCYLLA_ENABLE_ERROR_DIAGNOSIS build flag
 * 
 * Returns a result set with columns:
 *   - index (int): Error number that was diagnosed
 *   - error (text): The original error message
 *   - query (text): The original query
 *   - diagnosis (text): AI-generated diagnosis and fix
 */
class diagnose_error_statement : public cql_statement_no_metadata {
public:
    explicit diagnose_error_statement(std::optional<int32_t> error_index = std::nullopt);

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
        return std::make_unique<prepared_statement>(::make_shared<diagnose_error_statement>(*this));
    }

private:
    std::optional<int32_t> _error_index;  // If set, diagnose this specific error; else diagnose most recent
};

namespace raw {

/**
 * Raw (unprepared) form of DIAGNOSE ERROR statement.
 */
class diagnose_error_statement : public parsed_statement {
    std::optional<int32_t> _error_index;
public:
    explicit diagnose_error_statement(std::optional<int32_t> error_index = std::nullopt);
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

} // namespace raw

} // namespace statements
} // namespace cql3
