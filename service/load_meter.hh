/*
 * Copyright (C) 2020-present ScyllaDB
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

#pragma  once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "service/load_broadcaster.hh"

using namespace seastar;

class database;
namespace gms { class gossiper; }

namespace service {

class load_meter {
private:
    shared_ptr<load_broadcaster> _lb;

    /** raw load value */
    double get_load() const;
    sstring get_load_string() const;

public:
    future<std::map<sstring, double>> get_load_map();

    future<> init(distributed<database>& db, gms::gossiper& gossiper);
    future<> exit();
};

}
