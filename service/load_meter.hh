/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "service/load_broadcaster.hh"

using namespace seastar;

namespace replica {
class database;
}

namespace gms { class gossiper; }

namespace service {

class load_meter {
private:
    shared_ptr<load_broadcaster> _lb;

    /** raw load value */
    double get_load() const;

public:
    future<std::map<sstring, double>> get_load_map();

    future<> init(sharded<replica::database>& db, gms::gossiper& gossiper);
    future<> exit();
};

}
