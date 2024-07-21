/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/metrics_registration.hh>
#include "utils/rjson.hh"
#include "db/consistency_level_type.hh"

namespace alternator {

/**
 * \brief consumed_capacity_counter holds the bookkeeping to calculate RCU and WCU
 *
 * We prefer to use integer for counting the unit while DynamoDB API uses double.
 *
 * So the internal calculation is done using internal units and the consumed capacity
 * is reported as a double.
 *
 * We use consumed_capacity_counter for calculation of a specific action
 *
 * It is also used to update the response if needed and optionally update a metric with internal units.
 */
class consumed_capacity_counter {
public:
    consumed_capacity_counter(bool should_add, bool rcu, bool is_quorum, bool is_transaction) : _should_add(should_add), _is_rcu(rcu),
        _is_quorum(is_quorum), _is_transaction(is_transaction), _is_dummy(false), _total_bytes(0) {
    }

    consumed_capacity_counter() = default;

    bool operator()() const noexcept {
        return _should_add;
    }

    consumed_capacity_counter& operator +=(uint64_t bytes);
    double get_consumed_capacity_units() const noexcept;
    void add_consumed_capacity_if_needed(rjson::value& response) const noexcept;
    void add_consumed_capacity_if_needed(rjson::value& response, uint64_t& metric) const noexcept;
    bool is_dummy() const noexcept {
        return _is_dummy;
    }
private:
    /**
     * \brief get_internal_units calculate the internal units from the total bytes based on the type of the request
     */
    uint64_t get_internal_units() const noexcept;
    bool _should_add = false;
    bool _is_rcu = false;
    bool _is_quorum = false;
    bool _is_transaction = false;
    bool _is_dummy = true;
    uint64_t _total_bytes = 0;
};

std::unique_ptr<consumed_capacity_counter> get_write_consumed_capacity_ptr(const rjson::value& request);
consumed_capacity_counter get_read_consumed_capacity(const rjson::value& request, db::consistency_level cl);

}

