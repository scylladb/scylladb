/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/checked_ptr.hh>
#include <vector>

#include "exceptions/exceptions.hh"

namespace cql3 {

class prepare_context;
class column_specification;
class cql_statement;

namespace statements {

struct invalidated_prepared_usage_attempt {
    void operator()() const {
        throw exceptions::invalidated_prepared_usage_attempt_exception();
    }
};

class prepared_statement : public seastar::weakly_referencable<prepared_statement> {
public:
    typedef seastar::checked_ptr<seastar::weak_ptr<const prepared_statement>> checked_weak_ptr;

public:
    const seastar::shared_ptr<cql_statement> statement;
    const std::vector<seastar::lw_shared_ptr<column_specification>> bound_names;
    const std::vector<uint16_t> partition_key_bind_indices;
    const std::vector<sstring> warnings;

    prepared_statement(seastar::shared_ptr<cql_statement> statement_, std::vector<seastar::lw_shared_ptr<column_specification>> bound_names_,
                       std::vector<uint16_t> partition_key_bind_indices, std::vector<sstring> warnings = {});

    prepared_statement(seastar::shared_ptr<cql_statement> statement_, const prepare_context& ctx, const std::vector<uint16_t>& partition_key_bind_indices,
                       std::vector<sstring> warnings = {});

    prepared_statement(seastar::shared_ptr<cql_statement> statement_, prepare_context&& ctx, std::vector<uint16_t>&& partition_key_bind_indices);

    prepared_statement(seastar::shared_ptr<cql_statement>&& statement_);

    checked_weak_ptr checked_weak_from_this() const {
        return checked_weak_ptr(this->weak_from_this());
    }
};

}

}
