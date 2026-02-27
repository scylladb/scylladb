/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "exceptions.hh"
#include "cql3/column_identifier.hh"

namespace exceptions {

/**
 * Exception thrown when an entity is not recognized.
 */
class unrecognized_entity_exception : public invalid_request_exception {
public:
    /**
     * The unrecognized entity.
     */
    cql3::column_identifier entity;

    /**
     * Creates a new <code>UnrecognizedEntityException</code>.
     * @param entity the unrecognized entity
     */
    unrecognized_entity_exception(cql3::column_identifier entity)
        : invalid_request_exception(format("Unrecognized name {}", entity))
        , entity(std::move(entity))
    { }
};

}
