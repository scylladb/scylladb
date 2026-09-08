/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/prepared_statements_cache.hh"

namespace cql3 {

prepared_statements_cache::prepared_statements_cache(logging::logger& logger, size_t size)
    : _cache(size, entry_expiry, logger)
{}

void prepared_statements_cache::touch(const key_type& key) {
    // loading_cache::find() returns a value_ptr object which constructor does the "thouching".
    _cache.find(key.key());
}

prepared_statements_cache::value_type prepared_statements_cache::find(const key_type& key) {
    cache_value_ptr vp = _cache.find(key.key());
    if (vp) {
        return (*vp)->checked_weak_from_this();
    }
    return value_type();
}

future<> prepared_statements_cache::stop() {
    return _cache.stop();
}

}
