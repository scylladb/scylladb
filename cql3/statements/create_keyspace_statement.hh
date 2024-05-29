/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "service/raft/raft_group0_client.hh"
#include "transport/event.hh"

#include <seastar/core/shared_ptr.hh>

namespace locator {

class token_metadata;
};

namespace gms { class feature_service; }

namespace data_dictionary {
class keyspace_metadata;
}

namespace cql3 {

class query_processor;

namespace statements {

class ks_prop_defs;

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
class create_keyspace_statement : public schema_altering_statement {
private:
    sstring _name;
    shared_ptr<ks_prop_defs> _attrs;
    bool _if_not_exists;

public:
    /**
     * Creates a new <code>CreateKeyspaceStatement</code> instance for a given
     * keyspace name and keyword arguments.
     *
     * @param name the name of the keyspace to create
     * @param attrs map of the raw keyword arguments that followed the <code>WITH</code> keyword.
     */
    create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists);

    virtual bool has_keyspace() const override {
        return true;
    }
    virtual const sstring& keyspace() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    virtual void validate(query_processor&, const service::client_state& state) const override;


    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<> grant_permissions_to_creator(const service::client_state&, service::group0_batch&) const override;

    virtual future<::shared_ptr<messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

    lw_shared_ptr<data_dictionary::keyspace_metadata> get_keyspace_metadata(const locator::token_metadata& tm, const gms::feature_service& feat);

private:
    ::shared_ptr<event_t> created_event() const;
};

std::vector<sstring> check_against_restricted_replication_strategies(
    query_processor& qp,
    const sstring& keyspace,
    const ks_prop_defs& attrs,
    cql_stats& stats);

}

}
