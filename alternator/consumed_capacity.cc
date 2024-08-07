/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "consumed_capacity.hh"
#include "error.hh"
#include <seastar/core/metrics.hh>
#include "log.hh"

namespace alternator {

/*
 * \brief internally we calculate the consumed capacity as integer, externally
 * the API returns double, to get the consumed capacity unit from the internal units
 * you divide by INTERNAL_UNIT_COEFFICIENT.
 *
 */
static constexpr uint64_t INTERNAL_UNIT_COEFFICIENT = 2ULL;

static constexpr uint64_t KB = 1024ULL;
static constexpr uint64_t RCU_LENGTH = 4*KB;
static constexpr uint64_t WCU_LENGTH = 1*KB;

static bool should_add_capacity(const rjson::value& request) {
    const rjson::value* return_consumed = rjson::find(request, "ReturnConsumedCapacity");
    if (!return_consumed) {
        return false;
    }
    if (!return_consumed->IsString()) {
        throw api_error::validation("Non-string ReturnConsumedCapacity field in request");
    }
    std::string consumed = return_consumed->GetString();
    if (consumed == "INDEXES") {
        throw api_error::validation("INDEXES consumed capacity is not supported");
    }
    return consumed == "TOTAL";
}

void consumed_capacity_counter::add_consumed_capacity_if_needed(rjson::value& response) const noexcept {
    if (_should_add) {
        auto consumption = rjson::empty_object();
        rjson::add(consumption, "CapacityUnits", get_consumed_capacity_units());
        rjson::add(response, "ConsumedCapacity", std::move(consumption));
    }
}

void consumed_capacity_counter::add_consumed_capacity_if_needed(rjson::value& response, uint64_t& metric) const noexcept {
    add_consumed_capacity_if_needed(response);
    metric += get_internal_units();
}

static std::unique_ptr<consumed_capacity_counter> get_consumed_capacity_helper_ptr(bool should_add, bool rcu, bool isQuorum, bool isTransaction) {
    return std::make_unique<consumed_capacity_counter>(should_add, rcu, isQuorum, isTransaction);
}

std::unique_ptr<consumed_capacity_counter> get_write_consumed_capacity_ptr(const rjson::value& request) {
    return get_consumed_capacity_helper_ptr(should_add_capacity(request), false, false, false);
}

static consumed_capacity_counter get_consumed_capacity_helper(bool should_add, bool rcu, bool isQuorum, bool isTransaction) {
    return consumed_capacity_counter(should_add, rcu, isQuorum, isTransaction);
}

consumed_capacity_counter get_read_consumed_capacity(const rjson::value& request, db::consistency_level cl) {
    return get_consumed_capacity_helper(should_add_capacity(request), true, cl == db::consistency_level::LOCAL_ONE, false);
}

uint64_t consumed_capacity_counter::get_internal_units() const noexcept {
    uint64_t length = (_is_rcu)? RCU_LENGTH : WCU_LENGTH;
    uint64_t internal_units = INTERNAL_UNIT_COEFFICIENT*((_total_bytes + length -1)/length); //divide by length and round up

    if (_is_transaction) {
        internal_units *= 2;
    }
    if (_is_quorum) {
        internal_units /= 2;
    }
    return internal_units;
}

consumed_capacity_counter& consumed_capacity_counter::operator +=(uint64_t units) {
    _total_bytes += units;
    return *this;
}

double consumed_capacity_counter::get_consumed_capacity_units() const noexcept {
    return get_internal_units()/static_cast<double>(INTERNAL_UNIT_COEFFICIENT);
}

}
