/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "connection_notifier.hh"
#include "cql3/constants.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"

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
        case client_connection_stage::established: return connection_stage_literal<client_connection_stage::established>;
        case client_connection_stage::authenticating: return connection_stage_literal<client_connection_stage::authenticating>;
        case client_connection_stage::ready: return connection_stage_literal<client_connection_stage::ready>;
    }
    throw std::runtime_error("Invalid client_connection_stage");
}
