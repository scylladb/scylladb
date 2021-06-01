/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <variant>

#include "db_clock.hh"
#include "utils/UUID.hh"

namespace cdc {

struct generation_id_v1 {
    db_clock::time_point ts;
};

struct generation_id_v2 {
    db_clock::time_point ts;
    utils::UUID id;
};

using generation_id = std::variant<generation_id_v1, generation_id_v2>;

std::ostream& operator<<(std::ostream&, const generation_id&);
bool operator==(const generation_id&, const generation_id&);
db_clock::time_point get_ts(const generation_id&);

} // namespace cdc
