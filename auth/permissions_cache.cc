/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/permissions_cache.hh"

#include <fmt/ranges.h>
#include "auth/authorizer.hh"
#include "auth/service.hh"

namespace auth {

permissions_cache::permissions_cache(const utils::loading_cache_config& c, service& ser, logging::logger& log)
        : _cache(c, log, [&ser, &log](const key_type& k) {
              log.debug("Refreshing permissions for {}", k.first);
              return ser.get_uncached_permissions(k.first, k.second);
          }) {
}

bool permissions_cache::update_config(utils::loading_cache_config c) {
    return _cache.update_config(std::move(c));
}

void permissions_cache::reset() {
    _cache.reset();
}

future<permission_set> permissions_cache::get(const role_or_anonymous& maybe_role, const resource& r) {
    return do_with(key_type(maybe_role, r), [this](const auto& k) {
        return _cache.get(k);
    });
}

}
