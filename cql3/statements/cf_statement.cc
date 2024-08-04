/*
 * Copyright 2014-present-2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "raw/cf_statement.hh"
#include "service/client_state.hh"

namespace cql3 {

namespace statements {

namespace raw {

cf_statement::cf_statement(std::optional<cf_name> cf_name)
    : _cf_name(std::move(cf_name))
{
}

void cf_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_cf_name->has_keyspace()) {
        // XXX: We explicitly only want to call state.getKeyspace() in this case, as we don't want to throw
        // if not logged in any keyspace but a keyspace is explicitly set on the statement. So don't move
        // the call outside the 'if' or replace the method by 'prepareKeyspace(state.getKeyspace())'
        _cf_name->set_keyspace(state.get_keyspace(), true);
    }
}

void cf_statement::prepare_keyspace(std::string_view keyspace)
{
    if (!_cf_name->has_keyspace()) {
        _cf_name->set_keyspace(keyspace, true);
    }
}

bool cf_statement::has_keyspace() const {
    SCYLLA_ASSERT(_cf_name.has_value());
    return _cf_name->has_keyspace();
}

const sstring& cf_statement::keyspace() const
{
    SCYLLA_ASSERT(_cf_name->has_keyspace()); // "The statement hasn't be prepared correctly";
    return _cf_name->get_keyspace();
}

const sstring& cf_statement::column_family() const
{
    return _cf_name->get_column_family();
}

}

}

}
