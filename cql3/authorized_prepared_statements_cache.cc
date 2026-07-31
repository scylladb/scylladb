/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/authorized_prepared_statements_cache.hh"

namespace cql3 {

authorized_prepared_statements_cache::authorized_prepared_statements_cache(utils::loading_cache_config c, logging::logger& logger)
    : _cache(std::move(c), logger, [this] (const key_type& k) {
        _cache.remove(k);
        return make_ready_future<value_type>();
    })
{}

future<> authorized_prepared_statements_cache::insert(auth::authenticated_user user, cql3::prepared_cache_key_type prep_cache_key, value_type v) noexcept {
    return _cache.insert(key_type(std::move(user), std::move(prep_cache_key)), [v = std::move(v)] (const cache_key_type&) mutable {
        return make_ready_future<value_type>(std::move(v));
    });
}

authorized_prepared_statements_cache::value_ptr authorized_prepared_statements_cache::find(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
    return _cache.find(key_view_type{user, prep_cache_key}, key_view_hasher(), key_view_equal());
}

void authorized_prepared_statements_cache::remove(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
    _cache.remove(key_view_type{user, prep_cache_key}, key_view_hasher(), key_view_equal());
}

bool authorized_prepared_statements_cache::update_config(utils::loading_cache_config c) {
    return _cache.update_config(std::move(c));
}

void authorized_prepared_statements_cache::reset() {
    _cache.reset();
}

future<> authorized_prepared_statements_cache::stop() {
    return _cache.stop();
}

}
