/*
 * Copyright (C) 2018 ScyllaDB
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

#include "gms/inet_address.hh"
#include <cstdint>

namespace netw {

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y);
    friend bool operator<(const msg_addr& x, const msg_addr& y);
    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);
    struct hash {
        size_t operator()(const msg_addr& id) const;
    };
};

}
