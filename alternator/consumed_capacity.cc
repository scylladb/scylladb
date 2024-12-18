/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "consumed_capacity.hh"
#include "error.hh"

namespace alternator {

/*
 * \brief DynamoDB counts read capacity in half-integers - a short
 * eventually-consistent read is counted as 0.5 unit.
 * Because we want our counter to be an integer, it counts half units.
 * Both read and write counters count in these half-units, and should be
 * multiply by 0.5 (HALF_UNIT_MULTIPLIER) to get the DynamoDB-compatible RCU or WCU numbers.
 */
static constexpr double HALF_UNIT_MULTIPLIER = 0.5;

static constexpr uint64_t KB = 1024ULL;
static constexpr uint64_t RCU_BLOCK_SIZE_LENGTH = 4*KB;
static constexpr uint64_t WCU_BLOCK_SIZE_LENGTH = 1*KB;

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
    if (consumed != "TOTAL") {
        throw api_error::validation("Unknown consumed capacity "+ consumed);
    }
    return true;
}

void consumed_capacity_counter::add_consumed_capacity_to_response_if_needed(rjson::value& response) const noexcept {
    if (_should_add_to_reponse) {
        auto consumption = rjson::empty_object();
        rjson::add(consumption, "CapacityUnits", get_consumed_capacity_units());
        rjson::add(response, "ConsumedCapacity", std::move(consumption));
    }
}

static uint64_t calculate_half_units(uint64_t unit_block_size, uint64_t total_bytes, bool is_quorum) {
    uint64_t half_units = (total_bytes + unit_block_size -1) / unit_block_size; //divide by unit_block_size and round up

    if (is_quorum) {
        half_units *= 2;
    }
    return half_units;
}

rcu_consumed_capacity_counter::rcu_consumed_capacity_counter(const rjson::value& request, bool is_quorum) :
        consumed_capacity_counter(should_add_capacity(request)),_is_quorum(is_quorum) {
}

uint64_t rcu_consumed_capacity_counter::get_half_units() const noexcept {
    return calculate_half_units(RCU_BLOCK_SIZE_LENGTH, _total_bytes, _is_quorum);
}

uint64_t wcu_consumed_capacity_counter::get_half_units() const noexcept {
    return calculate_half_units(WCU_BLOCK_SIZE_LENGTH, _total_bytes, true);
}

wcu_consumed_capacity_counter::wcu_consumed_capacity_counter(const rjson::value& request) :
        consumed_capacity_counter(should_add_capacity(request)) {
}

consumed_capacity_counter& consumed_capacity_counter::operator +=(uint64_t units) {
    _total_bytes += units;
    return *this;
}

double consumed_capacity_counter::get_consumed_capacity_units() const noexcept {
    return get_half_units() * HALF_UNIT_MULTIPLIER;
}

}
