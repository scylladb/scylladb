/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <unordered_map>
#include <iostream>

#include "schema_fwd.hh"
#include "system_keyspace.hh"
#include "utils/UUID.hh"
#include "gc_clock.hh"

using namespace std::literals::chrono_literals;

namespace db {

constexpr gc_clock::duration NO_TTL = 0s;
constexpr gc_clock::duration ONE_DAY_TTL = 86400s;
constexpr gc_clock::duration ONE_MONTH_TTL = 2628000s;
constexpr gc_clock::duration ONE_YEAR_TTL = 31536000s;
constexpr gc_clock::duration DEFAULT_SYSTEM_EVENTS_TTL = 3 * ONE_YEAR_TTL;

using event_params = std::unordered_map<sstring, sstring>;

struct system_event {
    utils::UUID id = utils::null_uuid();   // started_at
    sstring category;
    sstring type;
    event_params params;
    utils::UUID completed_at;
    sstring completion_status = "";
};

// The following functions update the system.events table.
// They do not report query errors back, but rather log a warning
// and clear the event.id where a system_event is available to the caller.
future<> record_system_event(sstring category, sstring event_type, std::unordered_map<sstring, sstring> params = {}, const sstring& completion_status = "", gc_clock::duration ttl = DEFAULT_SYSTEM_EVENTS_TTL);
future<system_event> start_system_event(sstring category, sstring event_type, std::unordered_map<sstring, sstring> params = {}, gc_clock::duration ttl = DEFAULT_SYSTEM_EVENTS_TTL);

// end_system_event is a noop if event.id is null.
future<> end_system_event(system_event& event, const sstring& completion_status);
future<> end_system_event(system_event& event, std::exception_ptr = nullptr);

} // namespace db

namespace std {

std::ostream& operator<<(std::ostream& out, const db::system_event& event);

}