/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/frozen_schema.idl.hh"
#include "idl/token.idl.hh"

namespace replica {
namespace logstor {

struct primary_index_key {
    dht::decorated_key dk;
};

class log_record_header {
    replica::logstor::primary_index_key key;
    replica::logstor::record_generation generation;
    table_id table;
};

}
}
