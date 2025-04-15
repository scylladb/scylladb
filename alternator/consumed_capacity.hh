/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/rjson.hh"

namespace alternator {

/**
 * \brief consumed_capacity_counter is a base class that holds the bookkeeping
 *  to calculate RCU and WCU
 *
 * DynamoDB counts read capacity in half-integers - a short
 * eventually-consistent read is counted as 0.5 unit.
 * Because we want our counter to be an integer, we counts half units in
 * our internal calculations.
 *
 * We use consumed_capacity_counter for calculation of a specific action
 *
 * It is also used to update the response if needed.
 */
class consumed_capacity_counter {
public:
    consumed_capacity_counter() = default;
    consumed_capacity_counter(bool should_add_to_reponse) : _should_add_to_reponse(should_add_to_reponse){}
    bool operator()() const noexcept {
        return _should_add_to_reponse;
    }

    consumed_capacity_counter& operator +=(uint64_t bytes);
    double get_consumed_capacity_units() const noexcept;
    void add_consumed_capacity_to_response_if_needed(rjson::value& response) const noexcept;
    virtual ~consumed_capacity_counter() = default;
    /**
     * \brief get_half_units calculate the half units from the total bytes based on the type of the request
     */
    virtual uint64_t get_half_units() const noexcept = 0;
    uint64_t _total_bytes = 0;
protected:
    bool _should_add_to_reponse = false;
};

class rcu_consumed_capacity_counter : public consumed_capacity_counter {
    virtual uint64_t get_half_units() const noexcept;
    bool _is_quorum = false;
public:
    rcu_consumed_capacity_counter(const rjson::value& request, bool is_quorum);
};

class wcu_consumed_capacity_counter : public consumed_capacity_counter {
    virtual uint64_t get_half_units() const noexcept;
public:
    wcu_consumed_capacity_counter(const rjson::value& request);
};

}
