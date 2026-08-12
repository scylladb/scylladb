/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/update_statement.hh"
#include "cql3/statements/delete_statement.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"

namespace cql3::statements::strong_consistency {

/*
 * Turns a modification statement into a strongly consistent one, by committing
 * its mutation through Raft instead of storage_proxy. Everything else - parsing
 * state, mutation building, access control - is inherited unchanged.
 *
 * This is a mixin rather than a single class because the base modification
 * statement is abstract: building the mutation for a given row is implemented
 * per statement kind (update, delete, insert-json).
 *
 * Note that only do_execute() is replaced. A statement executed as part of a
 * batch goes through get_mutations() instead, and so still takes the eventually
 * consistent path, which is what batches did before this was introduced.
 */
template <typename Base>
class strongly_consistent final : public Base {
public:
    using Base::Base;

    bool is_strongly_consistent() const override {
        return true;
    }

    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const override;
};

// Builds either Stmt or its strongly consistent counterpart, depending on the
// keyspace the statement targets.
template <typename Stmt, typename... Args>
::shared_ptr<cql3::statements::modification_statement>
make_modification(data_dictionary::database db, const schema_ptr& s, Args&&... args) {
    if (is_strongly_consistent(db, s->ks_name())) {
        return ::make_shared<strongly_consistent<Stmt>>(std::forward<Args>(args)...);
    }
    return ::make_shared<Stmt>(std::forward<Args>(args)...);
}

}
