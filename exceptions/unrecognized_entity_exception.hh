/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"

namespace exceptions {

/**
 * Exception thrown when an entity is not recognized within a relation.
 */
class unrecognized_entity_exception : public invalid_request_exception {
public:
    /**
     * The unrecognized entity.
     */
    cql3::column_identifier entity;

    /**
     * The entity relation in a stringified form.
     */
    sstring relation_str;

    /**
     * Creates a new <code>UnrecognizedEntityException</code>.
     * @param entity the unrecognized entity
     * @param relation_str the entity relation string
     */
    unrecognized_entity_exception(cql3::column_identifier entity, sstring relation_str)
        : invalid_request_exception(format("Undefined name {} in where clause ('{}')", entity, relation_str))
        , entity(std::move(entity))
        , relation_str(std::move(relation_str))
    { }
};

}
