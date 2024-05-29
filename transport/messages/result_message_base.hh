
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "locator/tablets.hh"
#include "replica/tablets.hh"

namespace cql_transport {
namespace messages {

class result_message {
    std::vector<sstring> _warnings;
    std::optional<std::unordered_map<sstring, bytes>> _custom_payload;
public:
    class visitor;
    class visitor_base;

    virtual ~result_message() {}

    virtual void accept(visitor&) const = 0;

    void add_warning(sstring w) {
        _warnings.push_back(std::move(w));
    }

    const std::vector<sstring>& warnings() const {
        return _warnings;
    }

    void add_custom_payload(sstring key, bytes value) {
        if (!_custom_payload) {
            _custom_payload = std::optional<std::unordered_map<sstring, bytes>>{std::unordered_map<sstring, bytes>()};
        }
        _custom_payload.value()[key] = value;
    }

    void add_tablet_info(locator::tablet_replica_set tablet_replicas, std::pair<dht::token, dht::token> token_range) {
        if (!tablet_replicas.empty()) {
            auto replicas_values = make_list_value(replica::get_replica_set_type(), replica::replicas_to_data_value(tablet_replicas));
            auto v1 = data_value(dht::token::to_int64(token_range.first));
            auto v2 = data_value(dht::token::to_int64(token_range.second));

            auto tablets_routing = make_tuple_value(replica::get_tablet_info_type(), {v1, v2, replicas_values});
            this->add_custom_payload("tablets-routing-v1", tablets_routing.serialize_nonnull());
        }
    }

    const std::optional<std::unordered_map<sstring, bytes>>& custom_payload() const {
        return _custom_payload;
    }

    virtual std::optional<unsigned> move_to_shard() const {
        return std::nullopt;
    }

    virtual bool is_exception() const {
        return false;
    }

    virtual void throw_if_exception() const {}
    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class prepared;
    class schema_change;
    class rows;
    class bounce_to_shard;
    class exception;
};

std::ostream& operator<<(std::ostream& os, const result_message& msg);

}
}
