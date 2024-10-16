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

void consumed_capacity_counter::add_consumed_capacity_to_response_if_needed(rjson::value& response, replica::consumption_unit_counter& metric) const noexcept {
    if (_should_add) {
        auto consumption = rjson::empty_object();
        rjson::add(consumption, "CapacityUnits", get_consumed_capacity_units());
        rjson::add(response, "ConsumedCapacity", std::move(consumption));
    }
    metric.add_units(get_internal_units());
}

std::unique_ptr<wcu_consumed_capacity_counter> get_write_consumed_capacity_ptr(const rjson::value& request) {
    return std::make_unique<wcu_consumed_capacity_counter>(should_add_capacity(request));
}

rcu_consumed_capacity_counter get_read_consumed_capacity(const rjson::value& request, db::consistency_level cl) {
    return rcu_consumed_capacity_counter(should_add_capacity(request), cl != db::consistency_level::LOCAL_ONE);
}

static uint64_t calculate_internal_units(uint64_t length, uint64_t total_bytes, bool is_quorum) {
    uint64_t internal_units = (total_bytes + length -1) / length; //divide by length and round up

    if (is_quorum) {
        internal_units *= 2;
    }
    return internal_units;
}

uint64_t rcu_consumed_capacity_counter::get_internal_units() const noexcept {
    return calculate_internal_units(RCU_LENGTH, _total_bytes, _is_quorum);
}

uint64_t wcu_consumed_capacity_counter::get_internal_units() const noexcept {
    return calculate_internal_units(WCU_LENGTH, _total_bytes, true);
}

consumed_capacity_counter& consumed_capacity_counter::operator +=(uint64_t units) {
    _total_bytes += units;
    return *this;
}

double consumed_capacity_counter::get_consumed_capacity_units() const noexcept {
    return get_internal_units()/static_cast<double>(INTERNAL_UNIT_COEFFICIENT);
}

}
