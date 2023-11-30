/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/statements/service_level_statement.hh"

namespace cql3 {
namespace statements {

class list_effective_service_level_statement final : public service_level_statement {
    sstring _role_name;

public:
    list_effective_service_level_statement(sstring role_name);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard>) const override;
};

}
}