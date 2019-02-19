/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/statements/service_level_statement.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {
namespace statements {

class list_service_level_attachments_statement final : public service_level_statement {
    sstring _role_name;
    bool _describe_all;
public:
    list_service_level_attachments_statement(sstring role_name);
    list_service_level_attachments_statement();
    std::unique_ptr<cql3::statements::prepared_statement> prepare(database &db, cql_stats &stats) override;
    void validate(service::storage_proxy&, const service::client_state&) const override;
    virtual future<> check_access(service::storage_proxy& sp, const service::client_state&) const override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(service::storage_proxy&, service::query_state&, const query_options&) const override;
};

}

}
