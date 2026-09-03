/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include <seastar/core/sstring.hh>

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class alter_cluster_config_statement final : public schema_altering_statement {
public:
    enum class scope {
        cluster,
        datacenter,
        rack,
        node,
    };

private:
    scope _scope;
    std::optional<sstring> _dc_name;
    std::optional<sstring> _rack_name;
    std::optional<sstring> _node_uuid;
    sstring _config_name;
    std::optional<sstring> _value;

public:
    alter_cluster_config_statement(scope statement_scope, std::optional<sstring> dc_name, std::optional<sstring> rack_name, std::optional<sstring> node_uuid, sstring config_name, std::optional<sstring> value);

    static std::unique_ptr<alter_cluster_config_statement> for_cluster(sstring config_name, std::optional<sstring> value);
    static std::unique_ptr<alter_cluster_config_statement> for_datacenter(sstring dc_name, sstring config_name, std::optional<sstring> value);
    static std::unique_ptr<alter_cluster_config_statement> for_rack(sstring dc_name, sstring rack_name, sstring config_name, std::optional<sstring> value);
    static std::unique_ptr<alter_cluster_config_statement> for_node(sstring node_uuid, sstring config_name, std::optional<sstring> value);

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    void validate(query_processor& qp, const service::client_state& state) const override;
    future<std::tuple<::shared_ptr<event_t>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const override;
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), sstring(), sstring());
    }
};

}

}
