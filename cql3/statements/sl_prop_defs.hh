/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/property_definitions.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <optional>

class keyspace_metadata;

namespace cql3 {

namespace statements {

class sl_prop_defs : public property_definitions {
public:
    void validate();
};

}

}
