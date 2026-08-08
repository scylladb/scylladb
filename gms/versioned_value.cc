/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */
#include "gms/versioned_value.hh"
#include "message/messaging_service.hh"

// Explicit instantiations matching the `extern template` declarations in
// locator/host_id.hh, schema/schema_fwd.hh and service/state_id.hh. These
// tags are all odr-used via to_sstring() from the inline factory functions
// in gms/versioned_value.hh, a header included (directly or transitively,
// e.g. via gms/gossiper.hh) by dozens of translation units, so this avoids
// re-instantiating tagged_uuid<Tag>::to_sstring() (and the fmt formatting
// machinery it pulls in) in each of them.
template struct utils::tagged_uuid<locator::host_id_tag>;
template struct utils::tagged_uuid<table_schema_version_tag>;
template struct utils::tagged_uuid<service::state_id_tag>;

namespace gms {

static_assert(std::is_nothrow_default_constructible_v<versioned_value>);
static_assert(std::is_nothrow_move_constructible_v<versioned_value>);

versioned_value versioned_value::network_version() {
    return versioned_value(format("{}", netw::messaging_service::current_version));
}

sstring versioned_value::make_token_string(const std::unordered_set<dht::token>& tokens) {
    if (tokens.empty()) {
        return "";
    }
    return tokens.begin()->to_sstring();
}

} // namespace gms