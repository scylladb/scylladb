/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string>
#include <stdexcept>

#include "expressions_types.hh"

namespace alternator {

class expressions_syntax_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

parsed::update_expression parse_update_expression(std::string query);


} /* namespace alternator */
