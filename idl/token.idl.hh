/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dht/token.hh"

#include "idl/keys.idl.hh"

namespace dht {
class token {
    enum class kind : int {
        before_all_keys,
        key,
        after_all_keys,
    };
    dht::token::kind _kind;
    bytes data();
};

class decorated_key {
    dht::token token();
    partition_key key();
};

}
