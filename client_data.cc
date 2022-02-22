/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "client_data.hh"
#include <stdexcept>

sstring to_string(client_type ct) {
    switch (ct) {
        case client_type::cql: return "cql";
        case client_type::thrift: return "thrift";
        case client_type::alternator: return "alternator";
    }
    throw std::runtime_error("Invalid client_type");
}

sstring to_string(client_connection_stage ccs) {
    switch (ccs) {
        case client_connection_stage::established: return "ESTABLISHED";
        case client_connection_stage::authenticating: return "AUTHENTICATING";
        case client_connection_stage::ready: return "READY";
    }
    throw std::runtime_error("Invalid client_connection_stage");
}
