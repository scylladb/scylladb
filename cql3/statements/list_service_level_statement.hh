/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/statements/service_level_statement.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {
namespace statements {

class list_service_level_statement final : public service_level_statement {
    sstring _service_level;
    bool _describe_all;
public:
    list_service_level_statement(sstring service_level, bool describe_all);
    std::unique_ptr<cql3::statements::prepared_statement> prepare(database &db, cql_stats &stats) override;
    void validate(service::storage_proxy&, const service::client_state&) const override;
    virtual future<> check_access(service::storage_proxy& sp, const service::client_state&) const override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&) const override;
};

}

}
