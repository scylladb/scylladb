/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/statements/service_level_statement.hh"
#include "cql3/statements/sl_prop_defs.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {
namespace statements {

class alter_service_level_statement final : public service_level_statement {
    sstring _service_level;
    qos::service_level_options _slo;

public:
    alter_service_level_statement(sstring service_level, shared_ptr<sl_prop_defs> attrs);
    std::unique_ptr<cql3::statements::prepared_statement> prepare(data_dictionary::database db, cql_stats &stats) override;
    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard> guard) const override;
};

}

}
