/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */
#include "gms/versioned_value.hh"
#include "message/messaging_service.hh"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <charconv>

namespace gms {

static_assert(std::is_nothrow_default_constructible_v<versioned_value>);
static_assert(std::is_nothrow_move_constructible_v<versioned_value>);

versioned_value versioned_value::network_version() {
    return versioned_value(format("{}", netw::messaging_service::current_version));
}

sstring versioned_value::make_full_token_string(const std::unordered_set<dht::token>& tokens) {
    return fmt::to_string(fmt::join(tokens | std::views::transform([] (const dht::token& t) {
        return t.to_sstring(); }), ";"));
}

sstring versioned_value::make_token_string(const std::unordered_set<dht::token>& tokens) {
    if (tokens.empty()) {
        return "";
    }
    return tokens.begin()->to_sstring();
}

sstring versioned_value::make_cdc_generation_id_string(std::optional<cdc::generation_id> gen_id) {
    // We assume that the db_clock epoch is the same on all receiving nodes.
    if (!gen_id) {
        return "";
    }

    return std::visit(make_visitor(
    [] (const cdc::generation_id_v1& id) -> sstring {
        return std::to_string(id.ts.time_since_epoch().count());
    },
    [] (const cdc::generation_id_v2& id) {
        // v2;<timestamp>;<uuid>
        return format("v2;{};{}", id.ts.time_since_epoch().count(), id.id);
    }
    ), *gen_id);
}

std::unordered_set<dht::token> versioned_value::tokens_from_string(const sstring& s) {
    if (s.size() == 0) {
        return {}; // boost::split produces one element for empty string
    }
    std::vector<sstring> tokens;
    boost::split(tokens, s, boost::is_any_of(";"));
    std::unordered_set<dht::token> ret;
    for (auto str : tokens) {
        ret.emplace(dht::token::from_sstring(str));
    }
    return ret;
}
}